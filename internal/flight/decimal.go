//go:build duckdb_arrow

package flight

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// decimalCastInfo describes how to convert decimal columns to simpler types
// for Flight streaming. DuckDB returns decimal types (e.g., DECIMAL(18,0)
// from SUM/COUNT on integers, DECIMAL(38,10) from AVG). Flight clients
// handle int64/float64 more reliably.
type decimalCastInfo struct {
	schema  *arrow.Schema
	targets []arrow.DataType // nil = no cast needed for that column
}

// normalizeDecimalSchema inspects a schema for decimal columns.
// Returns nil if no decimal columns are found (zero overhead on the common path).
//   - decimal(x, 0) → int64  (SUM/COUNT of integers)
//   - decimal(x, y) → float64 (AVG or user-configured decimals)
func normalizeDecimalSchema(schema *arrow.Schema) *decimalCastInfo {
	hasDecimal := false
	for i := 0; i < schema.NumFields(); i++ {
		if _, ok := schema.Field(i).Type.(*arrow.Decimal128Type); ok {
			hasDecimal = true
			break
		}
	}
	if !hasDecimal {
		return nil
	}

	targets := make([]arrow.DataType, schema.NumFields())
	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		if dt, ok := f.Type.(*arrow.Decimal128Type); ok {
			if dt.Scale == 0 {
				targets[i] = arrow.PrimitiveTypes.Int64
			} else {
				targets[i] = arrow.PrimitiveTypes.Float64
			}
			fields[i] = arrow.Field{Name: f.Name, Type: targets[i], Nullable: f.Nullable, Metadata: f.Metadata}
		} else {
			fields[i] = f
		}
	}

	md := schema.Metadata()
	return &decimalCastInfo{
		schema:  arrow.NewSchema(fields, &md),
		targets: targets,
	}
}

// castDecimalBatch replaces decimal columns in the batch with int64/float64
// using arrow-go's compute.CastArray (SIMD-optimized, null-aware).
// The returned record must be Released by the caller.
func castDecimalBatch(batch arrow.RecordBatch, info *decimalCastInfo) (arrow.RecordBatch, error) {
	cols := make([]arrow.Array, batch.NumCols())
	toRelease := make([]arrow.Array, 0, batch.NumCols())

	ctx := compute.WithAllocator(context.Background(), memory.DefaultAllocator)

	for i, target := range info.targets {
		if target == nil {
			cols[i] = batch.Column(i)
			continue
		}
		casted, err := compute.CastArray(ctx, batch.Column(i), compute.SafeCastOptions(target))
		if err != nil {
			for _, a := range toRelease {
				a.Release()
			}
			return nil, err
		}
		cols[i] = casted
		toRelease = append(toRelease, casted)
	}

	rec := array.NewRecord(info.schema, cols, batch.NumRows())
	for _, a := range toRelease {
		a.Release()
	}
	return rec, nil
}
