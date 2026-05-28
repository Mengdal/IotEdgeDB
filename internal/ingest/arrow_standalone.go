package ingest

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// microPerHour is the number of microseconds in one hour (3600 * 1,000,000)
const microPerHour = int64(3600_000_000)

// hourBucket represents a collection of row indices belonging to a specific hour
// Used for hash-based grouping that doesn't require globally sorted data
type hourBucket struct {
	hourID  int64 // Hour identifier (microseconds / microPerHour)
	indices []int // Row indices belonging to this hour
	minTime int64 // Minimum timestamp in this hour (microseconds)
	maxTime int64 // Maximum timestamp in this hour (microseconds)
}

// convertToDecimal128Slice converts a []interface{} column to []decimal128.Num.
// Accepts float64, float32, int64, int*, uint*, and string values.
// Returns the typed array, optional validity bitmap (nil if no nulls), and error.
func convertToDecimal128Slice(col []interface{}, precision, scale int32) ([]decimal128.Num, []bool, error) {
	arr := make([]decimal128.Num, len(col))
	var valid []bool

	for i, v := range col {
		if v == nil {
			if valid == nil {
				valid = make([]bool, len(col))
				for j := 0; j < i; j++ {
					valid[j] = true
				}
			}
			continue
		}
		if valid != nil {
			valid[i] = true
		}

		var num decimal128.Num
		var err error

		switch val := v.(type) {
		case float64:
			num, err = decimal128.FromFloat64(val, precision, scale)
		case float32:
			num, err = decimal128.FromFloat64(float64(val), precision, scale)
		case int64:
			num, err = decimal128.FromString(strconv.FormatInt(val, 10), precision, scale)
		case int:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int32:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int16:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int8:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case uint64:
			num, err = decimal128.FromString(strconv.FormatUint(val, 10), precision, scale)
		case uint:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint32:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint16:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint8:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case string:
			num, err = decimal128.FromString(val, precision, scale)
		default:
			return nil, nil, fmt.Errorf("cannot convert %T to decimal128 at row %d", v, i)
		}

		if err != nil {
			return nil, nil, fmt.Errorf("row %d: %w", i, err)
		}
		arr[i] = num
	}

	return arr, valid, nil
}

// mergeTypedColumnBatches merges multiple column batches into a single TypedColumnBatch.
// OPTIMIZATION: Pre-allocate merged arrays to avoid O(n^2) append reallocations
// Handles sparse columns (schema evolution) by marking missing positions as null via validity bitmaps.
func mergeTypedColumnBatches(batches []interface{}) (*TypedColumnBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to merge")
	}

	// If only one batch, return it directly
	if len(batches) == 1 {
		if tcb, ok := batches[0].(*TypedColumnBatch); ok {
			return tcb, nil
		}
		// Legacy: bare map without validity (e.g. from WAL replay)
		if cols, ok := batches[0].(map[string]interface{}); ok {
			return &TypedColumnBatch{Data: cols, Validity: nil}, nil
		}
		return nil, fmt.Errorf("invalid batch type: %T", batches[0])
	}

	// PHASE 1: Calculate total rows from time column and collect column type info
	type colInfo struct {
		colType string // "int64", "float64", "string", "bool"
	}
	colTypes := make(map[string]*colInfo)
	totalRows := 0

	// Track which columns have validity bitmaps and which batches have which columns
	hasAnyValidity := false

	// Union of tag columns across all batches (for Parquet metadata)
	tagColumnSet := make(map[string]struct{})

	// First pass: count total rows using time column
	for _, batch := range batches {
		var cols map[string]interface{}
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
			if len(b.Validity) > 0 {
				hasAnyValidity = true
			}
			for _, tag := range b.TagColumns {
				tagColumnSet[tag] = struct{}{}
			}
		case map[string]interface{}:
			cols = b
		default:
			return nil, fmt.Errorf("invalid batch type: %T", batch)
		}

		// Count rows from time column (always present)
		if timeCol, ok := cols["time"].([]int64); ok {
			totalRows += len(timeCol)
		}

		// Collect column types
		for name, col := range cols {
			if colTypes[name] == nil {
				info := &colInfo{}
				switch col.(type) {
				case []int64:
					info.colType = "int64"
				case []float64:
					info.colType = "float64"
				case []string:
					info.colType = "string"
				case []bool:
					info.colType = "bool"
				case []decimal128.Num:
					info.colType = "decimal128"
				default:
					return nil, fmt.Errorf("unsupported column type: %T", col)
				}
				colTypes[name] = info
			}
		}
	}

	// Determine if we need validity tracking.
	// Needed if: any batch already has validity, OR columns are sparse across batches.
	// Check sparsity: if any batch doesn't have all columns, those positions are null.
	hasSparseColumns := false
	for _, batch := range batches {
		var cols map[string]interface{}
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
		case map[string]interface{}:
			cols = b
		}
		if len(cols) < len(colTypes) {
			hasSparseColumns = true
			break
		}
	}
	needsValidity := hasAnyValidity || hasSparseColumns

	// PHASE 2: Pre-allocate ALL columns to totalRows (handles sparse columns)
	merged := make(map[string]interface{}, len(colTypes))
	var mergedValidity map[string][]bool
	if needsValidity {
		mergedValidity = make(map[string][]bool, len(colTypes))
	}

	for name, info := range colTypes {
		switch info.colType {
		case "int64":
			merged[name] = make([]int64, totalRows)
		case "float64":
			merged[name] = make([]float64, totalRows)
		case "string":
			merged[name] = make([]string, totalRows)
		case "bool":
			merged[name] = make([]bool, totalRows)
		case "decimal128":
			merged[name] = make([]decimal128.Num, totalRows)
		}
		if needsValidity {
			// Initialize all positions as invalid (null). Positions with data get set to true below.
			mergedValidity[name] = make([]bool, totalRows)
		}
	}

	// PHASE 3: Copy data at correct row offsets (not per-column offsets)
	rowOffset := 0
	for _, batch := range batches {
		var cols map[string]interface{}
		var batchValidity map[string][]bool
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
			batchValidity = b.Validity
		case map[string]interface{}:
			cols = b
		}

		// Determine batch size from time column
		batchRows := 0
		if timeCol, ok := cols["time"].([]int64); ok {
			batchRows = len(timeCol)
		}

		// Copy each column's data at the current row offset
		for name, col := range cols {
			switch v := col.(type) {
			case []int64:
				copy(merged[name].([]int64)[rowOffset:], v)
			case []float64:
				copy(merged[name].([]float64)[rowOffset:], v)
			case []string:
				copy(merged[name].([]string)[rowOffset:], v)
			case []bool:
				copy(merged[name].([]bool)[rowOffset:], v)
			case []decimal128.Num:
				copy(merged[name].([]decimal128.Num)[rowOffset:], v)
			}

			// Copy validity bitmap for this column
			if needsValidity {
				dest := mergedValidity[name][rowOffset : rowOffset+batchRows]
				if batchValidity != nil {
					if srcValid, ok := batchValidity[name]; ok {
						// Batch has explicit validity for this column — copy it
						copy(dest, srcValid)
					} else {
						// Batch has validity tracking but not for this column → all valid
						for i := range dest {
							dest[i] = true
						}
					}
				} else {
					// Batch has no validity tracking at all → all valid
					for i := range dest {
						dest[i] = true
					}
				}
			}
		}
		// Sparse columns that don't exist in this batch keep validity=false (null)
		// at positions [rowOffset : rowOffset+batchRows] — no action needed.

		rowOffset += batchRows
	}

	// Optimization: strip validity entries that are all-true (no nulls)
	if mergedValidity != nil {
		for name, valid := range mergedValidity {
			allValid := true
			for _, v := range valid {
				if !v {
					allValid = false
					break
				}
			}
			if allValid {
				delete(mergedValidity, name)
			}
		}
		if len(mergedValidity) == 0 {
			mergedValidity = nil
		}
	}

	// Collect merged tag columns
	var mergedTagColumns []string
	if len(tagColumnSet) > 0 {
		mergedTagColumns = make([]string, 0, len(tagColumnSet))
		for tag := range tagColumnSet {
			mergedTagColumns = append(mergedTagColumns, tag)
		}
	}

	return &TypedColumnBatch{Data: merged, Validity: mergedValidity, TagColumns: mergedTagColumns}, nil
}

// sortColumnsByTime sorts all columns by the time column in-place
// Returns the sorted columns and any error encountered
func sortColumnsByTime(columns map[string]interface{}) (map[string]interface{}, error) {
	// Delegate to multi-key sort with just "time" key
	return sortColumnsByKeys(columns, []string{"time"})
}

// sortColumnsByKeys sorts columns by multiple keys (e.g., sensor_id, then time)
// Returns the sorted columns and any error encountered
func sortColumnsByKeys(columns map[string]interface{}, sortKeys []string) (map[string]interface{}, error) {
	if len(sortKeys) == 0 {
		return nil, fmt.Errorf("no sort keys provided")
	}

	// FAST PATH: Time-only sort (most common case) - avoid multi-key overhead
	if len(sortKeys) == 1 && sortKeys[0] == "time" {
		return sortColumnsByTimeOnly(columns)
	}

	// Validate all sort keys exist and cache column pointers
	cachedCols := make([]interface{}, len(sortKeys))
	for i, key := range sortKeys {
		col, exists := columns[key]
		if !exists {
			return nil, fmt.Errorf("sort key column not found: %s", key)
		}
		cachedCols[i] = col
	}

	// Get first column to determine row count
	var n int
	for _, col := range columns {
		switch c := col.(type) {
		case []int64:
			n = len(c)
		case []float64:
			n = len(c)
		case []string:
			n = len(c)
		case []bool:
			n = len(c)
		case []decimal128.Num:
			n = len(c)
		}
		if n > 0 {
			break
		}
	}

	if n == 0 {
		return columns, nil
	}

	// Create permutation indices [0, 1, 2, ..., n-1]
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Multi-key sort with cached columns (no map lookups in comparator)
	sort.Slice(indices, func(i, j int) bool {
		return compareMultiKeyCached(cachedCols, indices[i], indices[j])
	})

	// Apply permutation to all columns
	result := make(map[string]interface{}, len(columns))
	for colName, colData := range columns {
		result[colName] = applyPermutation(colData, indices)
	}

	return result, nil
}

// sortColumnsByTimeOnly is an optimized path for time-only sorting
// Avoids the multi-key comparator overhead for the common case
func sortColumnsByTimeOnly(columns map[string]interface{}) (map[string]interface{}, error) {
	timeCol, exists := columns["time"]
	if !exists {
		return nil, fmt.Errorf("time column not found")
	}

	times, ok := timeCol.([]int64)
	if !ok {
		return nil, fmt.Errorf("time column is not []int64")
	}

	n := len(times)
	if n == 0 {
		return columns, nil
	}

	// FAST PATH: Check if already sorted (common case for time-series producers)
	// This is O(n) but much cheaper than sorting + permutation when data is in order
	alreadySorted := true
	for i := 1; i < n; i++ {
		if times[i] < times[i-1] {
			alreadySorted = false
			break
		}
	}
	if alreadySorted {
		return columns, nil // No work needed - data is already in time order
	}

	// Create permutation indices
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Sort by time directly (no function call overhead per comparison)
	sort.Slice(indices, func(i, j int) bool {
		return times[indices[i]] < times[indices[j]]
	})

	// Apply permutation to all columns
	result := make(map[string]interface{}, len(columns))
	for colName, colData := range columns {
		result[colName] = applyPermutation(colData, indices)
	}

	return result, nil
}

// compareMultiKeyCached compares two rows by multiple sort keys using cached column pointers
// This avoids map lookups on every comparison (called O(n log n) times)
func compareMultiKeyCached(cachedCols []interface{}, i, j int) bool {
	for _, col := range cachedCols {
		switch c := col.(type) {
		case []int64:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}
			// Equal, continue to next key

		case []float64:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}

		case []string:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}

		case []bool:
			if !c[i] && c[j] { // false < true
				return true
			}
			if c[i] && !c[j] {
				return false
			}

		case []decimal128.Num:
			if c[i].Less(c[j]) {
				return true
			}
			if c[i].Greater(c[j]) {
				return false
			}
		}
	}

	// All keys equal
	return false
}

// applyPermutation reorders a column according to permutation indices
func applyPermutation(colData interface{}, indices []int) interface{} {
	switch col := colData.(type) {
	case []int64:
		result := make([]int64, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []float64:
		result := make([]float64, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []string:
		result := make([]string, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []bool:
		result := make([]bool, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []decimal128.Num:
		result := make([]decimal128.Num, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	default:
		return colData // Unknown type, return as-is
	}
}

// sortTypedColumnBatchByKeys sorts a TypedColumnBatch by the given keys,
// keeping validity bitmaps aligned with the reordered data.
func sortTypedColumnBatchByKeys(batch *TypedColumnBatch, sortKeys []string) *TypedColumnBatch {
	sorted, err := sortColumnsByKeys(batch.Data, sortKeys)
	if err != nil {
		// sortColumnsByKeys only errors on missing sort key or empty keys,
		// which shouldn't happen at this point. Return unsorted on error.
		return batch
	}

	if batch.Validity == nil {
		return &TypedColumnBatch{Data: sorted, Validity: nil, TagColumns: batch.TagColumns}
	}

	// The sort produced a permutation — we need to apply the same permutation to validity.
	// sortColumnsByKeys uses applyPermutation internally via indices.
	// We need to derive the same permutation to reorder validity.
	// Since sortColumnsByKeys returns already-permuted data, we recover the permutation
	// by comparing the time column order.
	//
	// Optimization: extract the permutation directly by sorting indices ourselves.
	// This duplicates the sort but avoids modifying sortColumnsByKeys's signature.

	// Get row count
	var n int
	for _, col := range batch.Data {
		switch c := col.(type) {
		case []int64:
			n = len(c)
		case []float64:
			n = len(c)
		case []string:
			n = len(c)
		case []bool:
			n = len(c)
		case []decimal128.Num:
			n = len(c)
		}
		if n > 0 {
			break
		}
	}

	if n == 0 {
		return &TypedColumnBatch{Data: sorted, Validity: batch.Validity, TagColumns: batch.TagColumns}
	}

	// Build permutation indices (same logic as sortColumnsByKeys)
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices by the same keys
	if len(sortKeys) == 1 && sortKeys[0] == "time" {
		if times, ok := batch.Data["time"].([]int64); ok {
			// Check if already sorted
			alreadySorted := true
			for i := 1; i < n; i++ {
				if times[i] < times[i-1] {
					alreadySorted = false
					break
				}
			}
			if alreadySorted {
				return &TypedColumnBatch{Data: sorted, Validity: batch.Validity, TagColumns: batch.TagColumns}
			}
			sort.Slice(indices, func(i, j int) bool {
				return times[indices[i]] < times[indices[j]]
			})
		}
	} else {
		cachedCols := make([]interface{}, 0, len(sortKeys))
		for _, key := range sortKeys {
			if col, exists := batch.Data[key]; exists {
				cachedCols = append(cachedCols, col)
			}
		}
		sort.Slice(indices, func(i, j int) bool {
			return compareMultiKeyCached(cachedCols, indices[i], indices[j])
		})
	}

	// Apply permutation to validity bitmaps
	sortedValidity := make(map[string][]bool, len(batch.Validity))
	for name, valid := range batch.Validity {
		newValid := make([]bool, n)
		for i, idx := range indices {
			newValid[i] = valid[idx]
		}
		sortedValidity[name] = newValid
	}

	return &TypedColumnBatch{Data: sorted, Validity: sortedValidity, TagColumns: batch.TagColumns}
}

// sliceTypedColumnBatchByIndices extracts rows from a TypedColumnBatch by index list,
// keeping validity bitmaps aligned.
func sliceTypedColumnBatchByIndices(batch *TypedColumnBatch, indices []int) *TypedColumnBatch {
	slicedData := sliceColumnsByIndices(batch.Data, indices)

	if batch.Validity == nil {
		return &TypedColumnBatch{Data: slicedData, Validity: nil, TagColumns: batch.TagColumns}
	}

	slicedValidity := make(map[string][]bool, len(batch.Validity))
	for name, valid := range batch.Validity {
		newValid := make([]bool, len(indices))
		validLen := len(valid)
		for i, idx := range indices {
			if idx < validLen {
				newValid[i] = valid[idx]
			}
			// else: out-of-bounds stays false (null)
		}
		slicedValidity[name] = newValid
	}

	return &TypedColumnBatch{Data: slicedData, Validity: slicedValidity, TagColumns: batch.TagColumns}
}

// hourIDToTime converts an hourID back to a time.Time for path generation
func hourIDToTime(hourID int64) time.Time {
	return time.UnixMicro(hourID * microPerHour).UTC()
}

// groupByHour groups row indices by hour and tracks min/max times
// Works correctly regardless of whether data is globally sorted by time
// Returns: map of hourID -> bucket, global min time, global max time
// Uses integer division for fast hour extraction (no time.Time allocations)
func groupByHour(times []int64) (map[int64]*hourBucket, int64, int64, error) {
	if len(times) == 0 {
		return nil, 0, 0, fmt.Errorf("empty time column")
	}

	buckets := make(map[int64]*hourBucket)
	globalMin := times[0]
	globalMax := times[0]

	// Single pass: group by hour and track min/max
	for i, t := range times {
		// Update global min/max
		if t < globalMin {
			globalMin = t
		}
		if t > globalMax {
			globalMax = t
		}

		// Fast hour extraction using integer division (no time.Time allocation)
		hourID := t / microPerHour

		// Get or create bucket
		bucket, exists := buckets[hourID]
		if !exists {
			bucket = &hourBucket{
				hourID:  hourID,
				indices: make([]int, 0, 100), // Pre-allocate some capacity
				minTime: t,
				maxTime: t,
			}
			buckets[hourID] = bucket
		} else {
			// Update bucket min/max
			if t < bucket.minTime {
				bucket.minTime = t
			}
			if t > bucket.maxTime {
				bucket.maxTime = t
			}
		}

		// Add row index to bucket
		bucket.indices = append(bucket.indices, i)
	}

	return buckets, globalMin, globalMax, nil
}

// sliceColumnsByIndices extracts rows from all columns based on a list of indices
// Returns a new column map with only the selected rows
// Handles sparse columns (columns shorter than indices) by using zero values for out-of-bounds access
func sliceColumnsByIndices(columns map[string]interface{}, indices []int) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))

	for colName, colData := range columns {
		switch col := colData.(type) {
		case []int64:
			newCol := make([]int64, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as zero value (sparse column handling)
			}
			result[colName] = newCol

		case []float64:
			newCol := make([]float64, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as zero value (sparse column handling)
			}
			result[colName] = newCol

		case []string:
			newCol := make([]string, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as empty string (sparse column handling)
			}
			result[colName] = newCol

		case []bool:
			newCol := make([]bool, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as false (sparse column handling)
			}
			result[colName] = newCol

		case []decimal128.Num:
			newCol := make([]decimal128.Num, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
			}
			result[colName] = newCol

		default:
			// Unknown type, copy as-is
			result[colName] = colData
		}
	}

	return result
}

// buildSingleArrowArrayStandalone constructs an Arrow array and field from a single typed column.
// Handles int64, float64, string, bool, and decimal128 types. The "time" column is
// always converted to Arrow Timestamp type. The validity bitmap tracks null values
// (nil = all valid, []bool with false entries = null).
func buildSingleArrowArrayStandalone(colName string, colData interface{}, validity []bool) (arrow.Field, arrow.Array, error) {
	mem := sharedArrowAllocator

	// Handle time column specially — stored as []int64, but Arrow type is Timestamp
	if colName == "time" {
		intCol, ok := colData.([]int64)
		if !ok {
			return arrow.Field{}, nil, fmt.Errorf("time column expected []int64, got %T", colData)
		}
		builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
		tsValues := int64SliceToTimestamps(intCol)
		builder.AppendValues(tsValues, validity)
		arr := builder.NewArray()
		builder.Release()
		return arrow.Field{Name: colName, Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true}, arr, nil
	}

	switch data := colData.(type) {
	case []int64:
		builder := array.NewInt64Builder(mem)
		builder.AppendValues(data, validity)
		arr := builder.NewArray()
		builder.Release()
		return arrow.Field{Name: colName, Type: arrow.PrimitiveTypes.Int64, Nullable: true}, arr, nil

	case []float64:
		builder := array.NewFloat64Builder(mem)
		builder.AppendValues(data, validity)
		arr := builder.NewArray()
		builder.Release()
		return arrow.Field{Name: colName, Type: arrow.PrimitiveTypes.Float64, Nullable: true}, arr, nil

	case []string:
		builder := array.NewStringBuilder(mem)
		builder.AppendValues(data, validity)
		arr := builder.NewArray()
		builder.Release()
		return arrow.Field{Name: colName, Type: arrow.BinaryTypes.String, Nullable: true}, arr, nil

	case []bool:
		builder := array.NewBooleanBuilder(mem)
		builder.AppendValues(data, validity)
		arr := builder.NewArray()
		builder.Release()
		return arrow.Field{Name: colName, Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, arr, nil

	case []decimal128.Num:
		// Use default precision/scale for buffer views; DuckDB UNION ALL will
		// handle type coercion with the Parquet-side decimal columns.
		dt := &arrow.Decimal128Type{Precision: 38, Scale: 18}
		builder := array.NewDecimal128Builder(mem, dt)
		builder.AppendValues(data, validity)
		arr := builder.NewArray()
		builder.Release()
		return arrow.Field{Name: colName, Type: dt, Nullable: true}, arr, nil

	default:
		return arrow.Field{}, nil, fmt.Errorf("unsupported type for column %s: %T", colName, colData)
	}
}
