package flight

import (
	"context"
	"encoding/json"
	"time"

	"iedb/internal/metrics"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetFlightInfo returns schema information for a query without executing it.
// Accessed via unifiedHandler.GetFlightInfo.
func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
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

	ticket := QueryTicket{SQL: fd.SQL, Database: fd.Database, Measurement: fd.Measurement}
	ticketBytes, err := SerializeTicket(ticket)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ticket serialization: %v", err)
	}

	// Normalize decimal schema for the Flight descriptor
	flightSchema := schema
	if ci := normalizeDecimalSchema(schema); ci != nil {
		flightSchema = ci.schema
	}

	s.logger.Debug().Str("sql", fd.SQL).Msg("GetFlightInfo OK")

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Schema:           SerializeSchema(flightSchema),
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticketBytes}}},
		TotalRecords:     -1,
		Ordered:          false,
	}, nil
}

// DoGet executes a query and streams results as Arrow RecordBatches.
// Accessed via unifiedHandler.DoGet.
func (s *Server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	startTime := time.Now()

	// 1. Authenticate + RBAC
	tokenInfo, err := s.verifyToken(stream.Context())
	if err != nil {
		return err
	}

	qt, err := DeserializeTicket(ticket.Ticket)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}
	if qt.SQL == "" {
		return status.Error(codes.InvalidArgument, "ticket contains empty sql")
	}

	// RBAC: check read permission if database/measurement specified in ticket
	if qt.Database != "" && qt.Measurement != "" {
		if err := s.checkPermission(tokenInfo, qt.Database, qt.Measurement, "read"); err != nil {
			return err
		}
	}

	s.logger.Debug().Str("sql", qt.SQL).Msg("DoGet request")

	// 2. Execute query
	reader, conn, err := s.db.ArrowQueryContext(stream.Context(), qt.SQL)
	if err != nil {
		s.logger.Error().Err(err).Str("sql", qt.SQL).Dur("elapsed", time.Since(startTime)).Msg("DoGet query failed")
		return status.Errorf(codes.Internal, "query failed: %v", err)
	}
	defer reader.Release()
	defer conn.Close() //nolint:errcheck

	// 3. Stream results with decimal normalization
	wr := flight.NewRecordWriter(stream)
	defer wr.Close()

	var castInfo *decimalCastInfo
	var rowCount int64
	for reader.Next() {
		record := reader.RecordBatch()

		if castInfo == nil {
			castInfo = normalizeDecimalSchema(record.Schema())
		}

		writeRecord := record
		var casted arrow.RecordBatch
		if castInfo != nil {
			var cerr error
			casted, cerr = castDecimalBatch(record, castInfo)
			if cerr != nil {
				return status.Errorf(codes.Internal, "decimal cast: %v", cerr)
			}
			writeRecord = casted
		}

		rowCount += writeRecord.NumRows()
		if err := wr.Write(writeRecord); err != nil {
			return status.Errorf(codes.Internal, "write record batch: %v", err)
		}

		// Release casted batch immediately after write (not deferred to function end)
		if casted != nil {
			casted.Release()
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

// GetSchema returns the schema for a FlightDescriptor.
func (s *Server) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	fd := FlightDescriptor{}
	if len(desc.Cmd) > 0 {
		if err := json.Unmarshal(desc.Cmd, &fd); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid flight descriptor: %v", err)
		}
	}
	if fd.SQL == "" {
		return nil, status.Error(codes.InvalidArgument, "missing sql in flight descriptor")
	}
	reader, conn, err := s.db.ArrowQueryContext(ctx, "SELECT * FROM ("+fd.SQL+") LIMIT 0")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "schema query: %v", err)
	}
	schema := reader.Schema()
	reader.Release()
	conn.Close() //nolint:errcheck

	if ci := normalizeDecimalSchema(schema); ci != nil {
		return &flight.SchemaResult{Schema: SerializeSchema(ci.schema)}, nil
	}
	return &flight.SchemaResult{Schema: SerializeSchema(schema)}, nil
}

// ListFlights lists all available measurements as FlightInfo entries.
func (s *Server) ListFlights(req *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	// Query DuckDB system catalog for all tables in all databases
	reader, conn, err := s.db.ArrowQueryContext(stream.Context(),
		"SELECT DISTINCT database_name, schema_name, table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY database_name, table_name")
	if err != nil {
		s.logger.Debug().Err(err).Msg("ListFlights system query failed, returning empty")
		return nil // soft-fail: return empty list on query failure
	}
	defer reader.Release()
	defer conn.Close() //nolint:errcheck

	for reader.Next() {
		rec := reader.RecordBatch()
		for i := int64(0); i < rec.NumRows(); i++ {
			fd := FlightDescriptor{}
			if col0, ok := rec.Column(0).(*array.String); ok {
				fd.Database = col0.Value(int(i))
			}
			if col2, ok := rec.Column(2).(*array.String); ok {
				fd.Measurement = col2.Value(int(i))
			}
			fdJSON, _ := json.Marshal(fd)

			info := &flight.FlightInfo{
				FlightDescriptor: &flight.FlightDescriptor{
					Type: flight.DescriptorCMD,
					Cmd:  fdJSON,
				},
				TotalRecords: -1,
			}
			if err := stream.Send(info); err != nil {
				return nil // client disconnected
			}
		}
	}
	return reader.Err()
}
