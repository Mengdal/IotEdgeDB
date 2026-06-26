//go:build duckdb_arrow

package database

import (
	"database/sql/driver"
	"fmt"
	"sort"
	"unsafe"

	"iedb/internal/ingest"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// sharedArrowAllocatorSync is the allocator for query-time VIEW construction.
// Reuses the same pattern as the ingest package's shared allocator.
var sharedArrowAllocatorSync = memory.NewGoAllocator()

// buildArrowRecordBatch converts a bufferEntry (Go typed slices) to an Arrow RecordBatch.
// Reuses the vectorized Builder.AppendValues pattern from WriteParquetColumnar
// but WITHOUT the Parquet serialization step.
//
// The caller is responsible for calling Release() on the returned RecordBatch.
func BuildArrowRecordBatch(entry *ingest.Entry, schema *arrow.Schema) (arrow.RecordBatch, error) {
	mem := sharedArrowAllocatorSync
	builders := make([]array.Builder, len(schema.Fields()))
	arrays := make([]arrow.Array, len(schema.Fields()))

	// CRITICAL: Release both builders and arrays to prevent memory leak.
	// Builders are released after NewArray() transfers ownership; arrays
	// are released only on error since they are owned by the RecordBatch on success.
	defer func() {
		for _, builder := range builders {
			if builder != nil {
				builder.Release()
			}
		}
		for _, arr := range arrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()

	columns := entry.GetColumns()
	if columns == nil {
		return nil, fmt.Errorf("entry has no columns")
	}

	for i, field := range schema.Fields() {
		cd, ok := columns[field.Name]
		if !ok {
			return nil, fmt.Errorf("column %q not found in data", field.Name)
		}
		col := cd.Data
		colValidity := cd.Validity

		switch field.Type.ID() {
		case arrow.INT64:
			builder := array.NewInt64Builder(mem)
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				builder.AppendValues(intCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %q: expected []int64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.TIMESTAMP:
			builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				// Zero-copy conversion from []int64 to []arrow.Timestamp
				tsValues := int64SliceToTimestamps(intCol)
				builder.AppendValues(tsValues, colValidity)
			} else {
				return nil, fmt.Errorf("column %q: expected []int64 for timestamp, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.FLOAT64:
			builder := array.NewFloat64Builder(mem)
			builders[i] = builder
			if floatCol, ok := col.([]float64); ok {
				builder.AppendValues(floatCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %q: expected []float64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.STRING:
			builder := array.NewStringBuilder(mem)
			builders[i] = builder
			if strCol, ok := col.([]string); ok {
				builder.AppendValues(strCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %q: expected []string, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.BOOL:
			builder := array.NewBooleanBuilder(mem)
			builders[i] = builder
			if boolCol, ok := col.([]bool); ok {
				builder.AppendValues(boolCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %q: expected []bool, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.DECIMAL128:
			dt, ok := field.Type.(*arrow.Decimal128Type)
			if !ok {
				return nil, fmt.Errorf("column %q: expected Decimal128Type, got %T", field.Name, field.Type)
			}
			builder := array.NewDecimal128Builder(mem, dt)
			builders[i] = builder
			if decCol, ok := col.([]decimal128.Num); ok {
				builder.AppendValues(decCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %q: expected []decimal128.Num, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		default:
			return nil, fmt.Errorf("unsupported Arrow type for column %q: %s", field.Name, field.Type.Name())
		}
	}

	// Build the record batch. Arrays are transferred to the RecordBatch;
	// the builder-release defer will clean up any error leftovers.
	record := array.NewRecord(schema, arrays, -1)

	// Clear arrays slice so the deferred release loop sees nil
	// and does not double-release arrays now owned by the record.
	for i := range arrays {
		arrays[i] = nil
	}

	return record, nil
}

// BuildArrowSchema infers an Arrow schema from the Entry's column data types.
// This is used when the entry's arrowSchema field has not been populated yet
// (i.e., data is still in-memory and has not been flushed to Parquet).
func BuildArrowSchema(entry *ingest.Entry) *arrow.Schema {
	columns := entry.GetColumns()
	if columns == nil {
		return nil
	}
	fields := make([]arrow.Field, 0, len(columns))
	for name, cd := range columns {
		var dt arrow.DataType
		switch cd.Data.(type) {
		case []string:
			dt = arrow.BinaryTypes.String
		case []int64:
			if name == "time" || name == "_time" {
				dt = arrow.FixedWidthTypes.Timestamp_us
			} else {
				dt = arrow.PrimitiveTypes.Int64
			}
		case []float64:
			dt = arrow.PrimitiveTypes.Float64
		case []bool:
			dt = arrow.FixedWidthTypes.Boolean
		case []decimal128.Num:
			dt = &arrow.Decimal128Type{Precision: 38, Scale: 19}
		default:
			continue // unsupported type, skip column
		}
		fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: true})
	}
	if len(fields) == 0 {
		return nil
	}
	// Ensure "time" column is first for consistent ordering.
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].Name == "time" {
			return true
		}
		if fields[j].Name == "time" {
			return false
		}
		return fields[i].Name < fields[j].Name
	})
	return arrow.NewSchema(fields, nil)
}

// registerBufferView wraps an Arrow RecordBatch in a RecordReader and registers
// it as a DuckDB VIEW on the given connection via the Arrow API.
//
// Returns a release function that the caller MUST defer until after the query
// referencing this VIEW has completed. The release function frees the VIEW's
// underlying Arrow memory.
func RegisterBufferView(driverConn driver.Conn, viewName string, rec arrow.RecordBatch) (release func(), err error) {
	reader, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		return nil, fmt.Errorf("failed to create RecordReader: %w", err)
	}

	arrowAPI, err := duckdb.NewArrowFromConn(driverConn)
	if err != nil {
		reader.Release()
		return nil, fmt.Errorf("failed to create Arrow interface: %w", err)
	}

	// Register the view. The returned release function handles cleanup of
	// the C-level ArrowArrayStream and the RecordReader.
	viewRelease, err := arrowAPI.RegisterView(reader, viewName)
	if err != nil {
		reader.Release()
		return nil, fmt.Errorf("failed to register VIEW %q: %w", viewName, err)
	}

	// Return a combined release that closes the view AND releases the reader.
	return func() {
		viewRelease()
		reader.Release()
		rec.Release()
	}, nil
}

// int64SliceToTimestamps reinterprets a []int64 as []arrow.Timestamp without copying.
// This is identical to the function in the ingest package and is duplicated here
// to avoid a circular dependency (ingest → database → ingest is not allowed via import).
// Safe because arrow.Timestamp is defined as `type Timestamp int64` (identical layout).
func int64SliceToTimestamps(src []int64) []arrow.Timestamp {
	return *(*[]arrow.Timestamp)(unsafe.Pointer(&src))
}
