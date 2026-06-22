package sharding

import (
	"context"
	"fmt"
	"sync"
	"time"

	"iedb/internal/cluster"
	"iedb/internal/flight"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/rs/zerolog"
)

// FlightScatterGatherConfig holds configuration for Flight-based scatter-gather.
type FlightScatterGatherConfig struct {
	// ShardMap provides shard-to-node mappings
	ShardMap *ShardMap

	// LocalNode is this node
	LocalNode *cluster.Node

	// FlightPool provides reusable Flight client connections to peer nodes
	FlightPool *flight.ClientPool

	// Timeout for individual shard queries
	Timeout time.Duration

	// MaxConcurrentShards limits parallel shard queries (0 = unlimited)
	MaxConcurrentShards int

	// Logger for scatter-gather events
	Logger zerolog.Logger
}

// FlightScatterGather coordinates queries across multiple shards using Arrow Flight.
// Replaces HTTP+JSON scatter-gather with zero-copy RecordBatch merging.
type FlightScatterGather struct {
	cfg    *FlightScatterGatherConfig
	logger zerolog.Logger
}

// NewFlightScatterGather creates a new Flight-based scatter-gather coordinator.
func NewFlightScatterGather(cfg *FlightScatterGatherConfig) *FlightScatterGather {
	return &FlightScatterGather{
		cfg:    cfg,
		logger: cfg.Logger.With().Str("component", "flight-scatter-gather").Logger(),
	}
}

// Execute queries all relevant shards via Flight DoGet and merges the results
// into a single RecordReader. If all shards are on the local node, it returns
// nil with no error — the caller should execute locally instead.
func (fsg *FlightScatterGather) Execute(ctx context.Context, ticket flight.QueryTicket) (array.RecordReader, error) {
	numShards := fsg.cfg.ShardMap.NumShards()
	if numShards <= 1 {
		// Single shard — execute locally
		return nil, nil
	}

	// Determine target nodes for each shard
	type shardTarget struct {
		shardID int
		node    *cluster.Node
	}

	targets := make([]shardTarget, 0, numShards)
	for shardID := 0; shardID < numShards; shardID++ {
		node := fsg.cfg.ShardMap.SelectNode(shardID)
		if node == nil {
			continue
		}
		// Skip local node — handled by caller
		if node.ID == fsg.cfg.LocalNode.ID {
			continue
		}
		targets = append(targets, shardTarget{shardID, node})
	}

	if len(targets) == 0 {
		// All shards on local node
		return nil, nil
	}

	// Parallel query with concurrency limit
	sem := make(chan struct{}, fsg.cfg.MaxConcurrentShards)
	if fsg.cfg.MaxConcurrentShards <= 0 {
		sem = make(chan struct{}, len(targets)) // unlimited
	}

	type result struct {
		index  int
		reader array.RecordReader
		err    error
	}

	results := make([]result, len(targets))
	var wg sync.WaitGroup

	for i, t := range targets {
		wg.Add(1)
		go func(idx int, target shardTarget) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			flightAddr := flightAddrFromAPI(target.node.APIAddress)
			client, err := fsg.cfg.FlightPool.Get(flightAddr)
			if err != nil {
				results[idx] = result{index: idx, err: fmt.Errorf("connect to %s: %w", target.node.ID, err)}
				return
			}

			ctx, cancel := context.WithTimeout(ctx, fsg.cfg.Timeout)
			defer cancel()

			reader, err := client.Query(ctx, ticket.SQL)
			results[idx] = result{index: idx, reader: reader, err: err}
		}(i, t)
	}
	wg.Wait()

	// Collect successful readers
	readers := make([]array.RecordReader, 0, len(results))
	var firstErr error
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			fsg.logger.Warn().Err(r.err).Int("shard", targets[r.index].shardID).Msg("shard query failed")
			continue
		}
		if r.reader != nil {
			readers = append(readers, r.reader)
		}
	}

	if len(readers) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fmt.Errorf("all shard queries returned no data")
	}

	merged, err := flight.NewMergedReader(readers)
	if err != nil {
		// Release readers on merge failure
		for _, r := range readers {
			r.Release()
		}
		return nil, err
	}

	return merged, nil
}

// flightAddrFromAPI derives the Flight server address from the HTTP API address
// by replacing the port. The default Flight port is 9090; the HTTP API default is 8080.
// Example: "10.0.0.1:8080" → "10.0.0.1:9090"
func flightAddrFromAPI(apiAddr string) string {
	// Strip trailing port and append Flight port
	for i := len(apiAddr) - 1; i >= 0; i-- {
		if apiAddr[i] == ':' {
			return apiAddr[:i] + ":9090"
		}
	}
	// No port in address — assume default
	return apiAddr + ":9090"
}
