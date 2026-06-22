package flight

import (
	"encoding/json"

	"iedb/internal/metrics"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DoPut accepts an Arrow RecordBatch stream and writes it directly to the ingest buffer.
// The first message carries the FlightDescriptor (database + measurement).
// Each subsequent message is a RecordBatch written via ArrowBuffer.WriteArrowRecord.
func (s *Server) DoPut(stream flight.FlightService_DoPutServer) error {
	ctx := stream.Context()

	// 1. Authenticate
	_, err := s.verifyToken(ctx)
	if err != nil {
		metrics.Get().RecordFlightDoPutError()
		return err
	}

	// 2. Parse the descriptor from the first message
	fd, err := stream.Recv()
	if err != nil {
		metrics.Get().RecordFlightDoPutError()
		return status.Errorf(codes.InvalidArgument, "failed to receive descriptor: %v", err)
	}

	var desc IngestDescriptor
	if err := json.Unmarshal(fd.FlightDescriptor.Cmd, &desc); err != nil {
		metrics.Get().RecordFlightDoPutError()
		return status.Errorf(codes.InvalidArgument, "invalid ingest descriptor: %v", err)
	}
	if desc.Database == "" || desc.Measurement == "" {
		metrics.Get().RecordFlightDoPutError()
		return status.Errorf(codes.InvalidArgument, "database and measurement are required")
	}

	// 3. Check ingest buffer is available
	if s.ingest == nil {
		metrics.Get().RecordFlightDoPutError()
		return status.Error(codes.Unavailable, "ingest buffer not configured")
	}

	// 4. Create RecordReader from the stream
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		metrics.Get().RecordFlightDoPutError()
		return status.Errorf(codes.Internal, "failed to create record reader: %v", err)
	}
	defer reader.Release()

	// 5. Write each RecordBatch directly into the ArrowBuffer
	for reader.Next() {
		record := reader.RecordBatch()
		if err := s.ingest.WriteArrowRecord(ctx, desc.Database, desc.Measurement, record); err != nil {
			metrics.Get().RecordFlightDoPutError()
			return status.Errorf(codes.Internal, "write arrow record: %v", err)
		}

		// Send acknowledgment
		ack := &flight.PutResult{
			AppMetadata: []byte(`{"status":"ok"}`),
		}
		if err := stream.Send(ack); err != nil {
			return status.Errorf(codes.Internal, "send ack: %v", err)
		}
	}

	if err := reader.Err(); err != nil {
		metrics.Get().RecordFlightDoPutError()
		return err
	}

	metrics.Get().RecordFlightDoPut()
	return nil
}
