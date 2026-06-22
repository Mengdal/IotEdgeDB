// Package flight — Arrow Flight gRPC server for IotEdgeDB.
//
// DoExchange (bidirectional stream) is not yet implemented.
// Cluster scatter-gather uses parallel DoGet via ShardQueryExecutor instead.
// See internal/flight/server.go (ShardQueryExecutor interface) and
// internal/cluster/sharding/scatter_gather_flight.go for the implementation.
package flight
