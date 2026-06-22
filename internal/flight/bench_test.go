//go:build duckdb_arrow

package flight

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// benchResultRows prevents compiler from optimizing away results in benchmarks.
var benchResultRows int64

// setupFlightBench creates a test environment suitable for benchmarks.
func setupFlightBench(b *testing.B) *integrationTestEnv {
	b.Helper()
	return setupFlightTest(&testing.T{})
}

// startHTTPArrowServer starts a minimal HTTP server that exposes Arrow IPC streaming
// on POST /query. Accepts JSON {"sql": "..."}, returns application/vnd.apache.arrow.stream.
// Returns the URL and a cleanup function.
func startHTTPArrowServer(b *testing.B, env *integrationTestEnv) (string, func()) {
	b.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		// Quick JSON parse: {"sql":"..."}
		sql := ""
		if len(body) > 8 {
			// Extract sql value between quotes after "sql":
			start := bytes.Index(body, []byte(`"sql":"`))
			if start >= 0 {
				start += 7
				end := bytes.IndexByte(body[start:], '"')
				if end >= 0 {
					sql = string(body[start : start+end])
				}
			}
		}
		if sql == "" {
			http.Error(w, "missing sql", http.StatusBadRequest)
			return
		}

		reader, conn, err := env.db.ArrowQueryContext(r.Context(), sql)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer reader.Release()
		defer conn.Close()

		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		w.WriteHeader(http.StatusOK)

		wr := ipc.NewWriter(w, ipc.WithSchema(reader.Schema()))
		defer wr.Close()

		for reader.Next() {
			if err := wr.Write(reader.RecordBatch()); err != nil {
				return
			}
		}
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("HTTP listen: %v", err)
	}

	srv := &http.Server{Handler: mux}
	go srv.Serve(listener)

	url := "http://" + listener.Addr().String() + "/query"
	return url, func() {
		srv.Close()
		listener.Close()
	}
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

// BenchmarkRawDuckDB_SelectLiteral measures a raw DuckDB Arrow query without any
// transport overhead — this is the baseline for Flight overhead comparison.
func BenchmarkRawDuckDB_SelectLiteral(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, conn, err := env.db.ArrowQueryContext(ctx, "SELECT 1 AS value")
		if err != nil {
			b.Fatalf("ArrowQueryContext: %v", err)
		}
		for reader.Next() {
			reader.RecordBatch()
		}
		reader.Release()
		conn.Close()
	}
}

// BenchmarkRawDuckDB_1K measures raw DuckDB Arrow query for 1K rows — baseline.
func BenchmarkRawDuckDB_1K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, conn, err := env.db.ArrowQueryContext(ctx, "SELECT unnest(generate_series(1, 1000)) AS n")
		if err != nil {
			b.Fatalf("ArrowQueryContext: %v", err)
		}
		for reader.Next() {
			reader.RecordBatch()
		}
		reader.Release()
		conn.Close()
	}
}

// BenchmarkRawDuckDB_100K measures raw DuckDB Arrow query for 100K rows — baseline.
func BenchmarkRawDuckDB_100K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, conn, err := env.db.ArrowQueryContext(ctx, "SELECT unnest(generate_series(1, 100000)) AS n")
		if err != nil {
			b.Fatalf("ArrowQueryContext: %v", err)
		}
		n := int64(0)
		for reader.Next() {
			n += reader.RecordBatch().NumRows()
		}
		reader.Release()
		conn.Close()
		benchResultRows = n
	}
}

// --- Flight vs HTTP Arrow IPC benchmarks ---

// BenchmarkFlight_vs_HTTP_1K compares Flight DoGet against HTTP Arrow IPC for a 1K-row query.
func BenchmarkFlight_vs_HTTP_1K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	// Start HTTP Arrow IPC server
	httpURL, cleanup := startHTTPArrowServer(b, env)
	defer cleanup()

	sql := "SELECT unnest(generate_series(1, 1000)) AS n"

	b.Run("Flight", func(b *testing.B) {
		desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte(`{"sql":"` + sql + `"}`)}
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
			reader, _ := flight.NewRecordReader(stream)
			n := int64(0)
			for reader.Next() {
				n += reader.RecordBatch().NumRows()
			}
			reader.Release()
			if n != 1000 {
				b.Fatalf("expected 1000, got %d", n)
			}
		}
	})

	b.Run("HTTP", func(b *testing.B) {
		body := []byte(`{"sql":"` + sql + `"}`)
		client := &http.Client{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.Post(httpURL, "application/json", bytes.NewReader(body))
			if err != nil {
				b.Fatalf("HTTP POST: %v", err)
			}
			reader, err := ipc.NewReader(resp.Body)
			if err != nil {
				resp.Body.Close()
				b.Fatalf("ipc.NewReader: %v", err)
			}
			n := int64(0)
			for reader.Next() {
				n += reader.RecordBatch().NumRows()
			}
			reader.Release()
			resp.Body.Close()
			if n != 1000 {
				b.Fatalf("expected 1000, got %d", n)
			}
		}
	})
}

// BenchmarkFlight_vs_HTTP_100K compares Flight vs HTTP Arrow IPC for 100K rows.
func BenchmarkFlight_vs_HTTP_100K(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	httpURL, cleanup := startHTTPArrowServer(b, env)
	defer cleanup()

	sql := "SELECT unnest(generate_series(1, 100000)) AS n"

	b.Run("Flight", func(b *testing.B) {
		desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte(`{"sql":"` + sql + `"}`)}
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
			reader, _ := flight.NewRecordReader(stream)
			n := int64(0)
			for reader.Next() {
				n += reader.RecordBatch().NumRows()
			}
			reader.Release()
			benchResultRows = n
		}
	})

	b.Run("HTTP", func(b *testing.B) {
		body := []byte(`{"sql":"` + sql + `"}`)
		client := &http.Client{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.Post(httpURL, "application/json", bytes.NewReader(body))
			if err != nil {
				b.Fatalf("HTTP POST: %v", err)
			}
			reader, err := ipc.NewReader(resp.Body)
			if err != nil {
				resp.Body.Close()
				b.Fatalf("ipc.NewReader: %v", err)
			}
			n := int64(0)
			for reader.Next() {
				n += reader.RecordBatch().NumRows()
			}
			reader.Release()
			resp.Body.Close()
			benchResultRows = n
		}
	})
}

// BenchmarkFlight_vs_HTTP_SelectLiteral compares overhead for a trivial query.
func BenchmarkFlight_vs_HTTP_SelectLiteral(b *testing.B) {
	env := setupFlightBench(b)
	ctx := context.Background()

	httpURL, cleanup := startHTTPArrowServer(b, env)
	defer cleanup()

	sql := "SELECT 1 AS value"

	b.Run("Flight", func(b *testing.B) {
		desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte(`{"sql":"` + sql + `"}`)}
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
			reader, _ := flight.NewRecordReader(stream)
			for reader.Next() {
				reader.RecordBatch()
			}
			reader.Release()
		}
	})

	b.Run("HTTP", func(b *testing.B) {
		body := []byte(`{"sql":"` + sql + `"}`)
		client := &http.Client{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.Post(httpURL, "application/json", bytes.NewReader(body))
			if err != nil {
				b.Fatalf("HTTP POST: %v", err)
			}
			reader, err := ipc.NewReader(resp.Body)
			if err != nil {
				resp.Body.Close()
				b.Fatalf("ipc.NewReader: %v", err)
			}
			for reader.Next() {
				reader.RecordBatch()
			}
			reader.Release()
			resp.Body.Close()
		}
	})
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
