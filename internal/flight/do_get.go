package flight

import (
	"context"
	"encoding/json"
	"time"

	"iedb/internal/metrics"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetFlightInfo returns schema information for a query without executing it.
// The client sends a FlightDescriptor with SQL; we run LIMIT 0 to get the schema.
func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Parse the descriptor command
	fd := FlightDescriptor{}
	if len(desc.Cmd) > 0 {
		if err := json.Unmarshal(desc.Cmd, &fd); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid flight descriptor: %v", err)
		}
	}

	if fd.SQL == "" {
		return nil, status.Error(codes.InvalidArgument, "missing sql in flight descriptor")
	}

	// Run LIMIT 0 to get schema without data
	reader, conn, err := s.db.ArrowQueryContext(ctx, "SELECT * FROM ("+fd.SQL+") LIMIT 0")
	if err != nil {
		s.logger.Error().Err(err).Str("sql", fd.SQL).Msg("GetFlightInfo query failed")
		return nil, status.Errorf(codes.Internal, "schema query failed: %v", err)
	}
	schema := reader.Schema()
	reader.Release()
	conn.Close() //nolint:errcheck

	// Serialize the ticket
	ticket := QueryTicket{
		SQL:         fd.SQL,
		Database:    fd.Database,
		Measurement: fd.Measurement,
	}
	ticketBytes, err := SerializeTicket(ticket)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ticket serialization: %v", err)
	}

	s.logger.Debug().Str("sql", fd.SQL).Msg("GetFlightInfo OK")

	// Normalize decimal schema for the Flight descriptor
	flightSchema := schema
	if ci := normalizeDecimalSchema(schema); ci != nil {
		flightSchema = ci.schema
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Schema:           SerializeSchema(flightSchema),
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1, // unknown for streaming queries
		Ordered:      false,
	}, nil
}

// DoGet executes a query and streams results as Arrow RecordBatches.
func (s *Server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	startTime := time.Now()

	// 1. Authenticate
	_, err := s.verifyToken(stream.Context())
	if err != nil {
		return err
	}

	// 2. Deserialize the ticket
	qt, err := DeserializeTicket(ticket.Ticket)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}

	if qt.SQL == "" {
		return status.Error(codes.InvalidArgument, "ticket contains empty sql")
	}

	s.logger.Debug().Str("sql", qt.SQL).Msg("DoGet request")

	// 3. Execute query via DuckDB Arrow API
	reader, conn, err := s.db.ArrowQueryContext(stream.Context(), qt.SQL)
	if err != nil {
		s.logger.Error().Err(err).Str("sql", qt.SQL).Dur("elapsed", time.Since(startTime)).Msg("DoGet query failed")
		return status.Errorf(codes.Internal, "query failed: %v", err)
	}
	defer reader.Release()
	defer conn.Close() //nolint:errcheck

	// 4. Stream results with decimal normalization
	// DuckDB may return Decimal128 columns (e.g., from SUM/AVG). Convert them to
	// int64 (scale=0) or float64 (scale>0) for better Flight client compatibility.
	var castInfo *decimalCastInfo
	wr := flight.NewRecordWriter(stream)
	defer wr.Close()

	var rowCount int64
	for reader.Next() {
		record := reader.RecordBatch()

		// Lazily check schema for decimal columns on the first batch
		if castInfo == nil {
			castInfo = normalizeDecimalSchema(record.Schema())
		}

		// Cast decimal columns if needed
		writeRecord := record
		if castInfo != nil {
			casted, err := castDecimalBatch(record, castInfo)
			if err != nil {
				return status.Errorf(codes.Internal, "decimal cast: %v", err)
			}
			writeRecord = casted
			defer writeRecord.Release()
		}

		rowCount += writeRecord.NumRows()
		if err := wr.Write(writeRecord); err != nil {
			return status.Errorf(codes.Internal, "write record batch: %v", err)
		}
	}

	if err := reader.Err(); err != nil {
		s.logger.Error().Err(err).Str("sql", qt.SQL).Int64("rows", rowCount).Msg("DoGet reader error")
		return status.Errorf(codes.Internal, "record reader error: %v", err)
	}

	duration := time.Since(startTime)
	s.logger.Info().
		Str("sql", qt.SQL).
		Int64("rows", rowCount).
		Dur("duration", duration).
		Msg("DoGet complete")

	metrics.Get().RecordFlightDoGet(rowCount, duration.Microseconds())
	return nil
}

// ListFlights lists all available measurements as FlightInfo entries.
func (s *Server) ListFlights(req *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	_ = req
	_ = stream
	return nil
}
