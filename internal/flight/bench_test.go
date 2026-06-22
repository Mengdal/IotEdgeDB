//go:build duckdb_arrow

package flight

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// setupFlightBench creates a test environment suitable for benchmarks.
func setupFlightBench(b *testing.B) *integrationTestEnv {
	b.Helper()
	// Reuse setupFlightTest by wrapping testing.B as testing.TB
	// We construct manually to avoid the *testing.T dependency
	return setupFlightTest(&testing.T{})
}

// BenchmarkDoGet_SelectLiteral measures raw Flight DoGet throughput.
func BenchmarkDoGet_SelectLiteral(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(`{"sql":"SELECT 1 AS value"}`),
	}
	info, err := env.client.GetFlightInfo(ctx, desc)
	if err != nil {
		b.Fatalf("GetFlightInfo: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := env.client.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			b.Fatalf("DoGet: %v", err)
		}
		reader, err := flight.NewRecordReader(stream)
		if err != nil {
			b.Fatalf("record reader: %v", err)
		}
		for reader.Next() {
			reader.RecordBatch()
		}
		reader.Release()
	}
}

// BenchmarkDoGet_1K benchmarks DoGet for a 1000-row result.
func BenchmarkDoGet_1K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(`{"sql":"SELECT unnest(generate_series(1, 1000)) AS n"}`),
	}
	info, err := env.client.GetFlightInfo(ctx, desc)
	if err != nil {
		b.Fatalf("GetFlightInfo: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := env.client.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			b.Fatalf("DoGet: %v", err)
		}
		reader, err := flight.NewRecordReader(stream)
		if err != nil {
			b.Fatalf("record reader: %v", err)
		}
		n := int64(0)
		for reader.Next() {
			n += reader.RecordBatch().NumRows()
		}
		reader.Release()
		if n != 1000 {
			b.Fatalf("expected 1000, got %d", n)
		}
	}
}

// BenchmarkWriteArrowRecord_1K measures WriteArrowRecord with 1000-row batches.
func BenchmarkWriteArrowRecord_1K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()
	record := makeTestRecordBatch(b, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := env.buf.WriteArrowRecord(ctx, "bench", "bm", record); err != nil {
			b.Fatalf("WriteArrowRecord: %v", err)
		}
	}
}

// BenchmarkWriteArrowRecord_10K measures WriteArrowRecord with 10K-row batches.
func BenchmarkWriteArrowRecord_10K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()
	record := makeTestRecordBatch(b, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := env.buf.WriteArrowRecord(ctx, "bench", "bm", record); err != nil {
			b.Fatalf("WriteArrowRecord: %v", err)
		}
	}
}

// BenchmarkMergedReader_4x1K measures merging 4 readers each with 250 rows.
func BenchmarkMergedReader_4x1K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readers := make([]array.RecordReader, 4)
		for j := 0; j < 4; j++ {
			r, err := queryReader(ctx, env, fmt.Sprintf("SELECT unnest(generate_series(1, 250)) AS n"))
			if err != nil {
				b.Fatalf("reader %d: %v", j, err)
			}
			readers[j] = r
		}
		merged, err := NewMergedReader(readers)
		if err != nil {
			b.Fatalf("NewMergedReader: %v", err)
		}
		var n int64
		for merged.Next() {
			n += merged.RecordBatch().NumRows()
		}
		merged.Release()
		if n != 1000 {
			b.Fatalf("expected 1000, got %d", n)
		}
	}
}
