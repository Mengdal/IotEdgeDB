package flight

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"iedb/internal/auth"
	"iedb/internal/database"
	"iedb/internal/ingest"
)

// ShardQueryExecutor is the interface for cluster-aware query execution.
// When set on the Server, DoGet delegates shard-aware queries to this executor.
// Returns (nil, nil) when all shards are local — the caller should execute locally.
type ShardQueryExecutor interface {
	ExecuteFlight(ctx context.Context, ticket QueryTicket) (array.RecordReader, error)
}

// Server implements the Arrow Flight protocol for IotEdgeDB.
// The Flight server runs on a separate port from the HTTP API, in the same process.
type Server struct {
	db      *database.DuckDB
	ingest  *ingest.ArrowBuffer
	authMgr *auth.AuthManager
	rbacMgr *auth.RBACManager
	logger  zerolog.Logger

	shardExecutor ShardQueryExecutor // optional cluster scatter-gather

	mu       sync.RWMutex
	grpcSrv  *grpc.Server
	listener net.Listener
}

// NewServer creates a new Flight Server.
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

	s.grpcSrv = grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024),
		grpc.MaxSendMsgSize(64*1024*1024),
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

	// Register a unified handler that routes:
	//   - JSON descriptors  → our base Server (DoGet, GetFlightInfo, ListFlights)
	//   - Protobuf descriptors → Flight SQL (catalogs, schemas, tables, SqlInfo, statements)
	handler := newUnifiedHandler(s)
	flight.RegisterFlightServiceServer(s.grpcSrv, handler)
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

// SetShardExecutor configures a cluster scatter-gather executor.
// When set, DoGet delegates multi-shard queries to this executor.
// When nil (default), all queries execute locally.
func (s *Server) SetShardExecutor(exec ShardQueryExecutor) {
	s.shardExecutor = exec
}

// Close implements shutdown.Shutdownable.
func (s *Server) Close() error {
	s.Stop()
	return nil
}

// unifiedHandler implements flight.FlightServer. It routes:
// - JSON descriptors → base handlers on our Server
// - Protobuf-encoded descriptors → Flight SQL via flightsql wrapper
// All unimplemented methods fall back to BaseFlightServer defaults.
type unifiedHandler struct {
	flight.BaseFlightServer
	base    *Server
	sqlWrap flight.FlightServer // flightsql.NewFlightServer result
}

func newUnifiedHandler(s *Server) flight.FlightServer {
	sqlSrv := &flightSQLServer{srv: s}
	return &unifiedHandler{
		BaseFlightServer: flight.BaseFlightServer{},
		base:             s,
		sqlWrap:          flightsql.NewFlightServer(sqlSrv),
	}
}

// isJSONDescriptor returns true if the Cmd bytes look like JSON (starts with '{').
func isJSONDescriptor(cmd []byte) bool {
	return len(cmd) > 0 && cmd[0] == '{'
}

func (h *unifiedHandler) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if isJSONDescriptor(desc.Cmd) {
		return h.base.GetFlightInfo(ctx, desc)
	}
	return h.sqlWrap.GetFlightInfo(ctx, desc)
}

func (h *unifiedHandler) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	if isJSONDescriptor(ticket.Ticket) {
		return h.base.DoGet(ticket, stream)
	}
	return h.sqlWrap.DoGet(ticket, stream)
}

func (h *unifiedHandler) ListFlights(req *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	return h.base.ListFlights(req, stream)
}

func (h *unifiedHandler) DoPut(stream flight.FlightService_DoPutServer) error {
	return h.base.DoPut(stream)
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
