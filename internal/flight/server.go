package flight

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"iedb/internal/auth"
	"iedb/internal/database"
	"iedb/internal/ingest"
)

// Server implements the Arrow Flight protocol for IotEdgeDB.
// It embeds BaseFlightServer and only overrides the methods needed.
// The Flight server runs on a separate port from the HTTP API, in the same process.
type Server struct {
	flight.BaseFlightServer

	db      *database.DuckDB
	ingest  *ingest.ArrowBuffer
	authMgr *auth.AuthManager
	rbacMgr *auth.RBACManager
	logger  zerolog.Logger

	mu       sync.RWMutex
	grpcSrv  *grpc.Server
	listener net.Listener
}

// NewServer creates a new Flight Server.
// The ingest buffer is optional (nil until DoPut is implemented in Step 1.2).
func NewServer(
	db *database.DuckDB,
	ingestBuf *ingest.ArrowBuffer,
	authMgr *auth.AuthManager,
	rbacMgr *auth.RBACManager,
	logger zerolog.Logger,
) *Server {
	s := &Server{
		db:      db,
		ingest:  ingestBuf,
		authMgr: authMgr,
		rbacMgr: rbacMgr,
		logger:  logger.With().Str("component", "flight-server").Logger(),
	}

	// gRPC server with 64MB message limits, keepalive enforcement
	s.grpcSrv = grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024), // 64MB
		grpc.MaxSendMsgSize(64*1024*1024), // 64MB
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	flight.RegisterFlightServiceServer(s.grpcSrv, s)
	// Flight SQL methods are not registered yet in v1 — they return Unimplemented
	// via BaseFlightServer's default handlers.
	return s
}

// Start begins listening and serving on the given address.
func (s *Server) Start(addr string) error {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("flight server listen on %s: %w", addr, err)
	}

	s.logger.Info().Str("addr", addr).Msg("Flight server starting")
	return s.grpcSrv.Serve(s.listener)
}

// Stop gracefully shuts down the Flight server, waiting for in-flight RPCs to complete.
func (s *Server) Stop() {
	s.logger.Info().Msg("Stopping Flight server...")
	s.grpcSrv.GracefulStop()
	s.logger.Info().Msg("Flight server stopped")
}

// Close implements shutdown.Shutdownable so the Flight server can be
// registered with the shutdown coordinator.
func (s *Server) Close() error {
	s.Stop()
	return nil
}

// SerializeSchema serializes an Arrow schema to wire format via Flight.
func SerializeSchema(schema *arrow.Schema) []byte {
	data := flight.SerializeSchema(schema, memory.DefaultAllocator)
	return append([]byte{}, data...)
}

// SerializeTicket marshals a QueryTicket to JSON bytes.
func SerializeTicket(t QueryTicket) ([]byte, error) {
	return json.Marshal(t)
}

// DeserializeTicket unmarshals a QueryTicket from JSON bytes.
func DeserializeTicket(data []byte) (*QueryTicket, error) {
	var t QueryTicket
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}
