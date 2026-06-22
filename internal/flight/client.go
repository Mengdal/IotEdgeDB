package flight

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps a Flight gRPC connection and exposes a simplified query interface.
// It abstracts the GetFlightInfo → DoGet flow so callers don't need to know
// whether data comes from a local or remote node.
type Client struct {
	conn   *grpc.ClientConn
	client flight.Client
}

// ClientOption configures a Client connection.
type ClientOption func(*clientOpts)

type clientOpts struct {
	addr           string
	bearerToken    string
	maxRecvMsgSize int
}

func defaultClientOpts() *clientOpts {
	return &clientOpts{
		maxRecvMsgSize: 64 * 1024 * 1024, // 64MB
	}
}

// WithBearerToken sets a Bearer token for authentication.
func WithBearerToken(token string) ClientOption {
	return func(o *clientOpts) {
		o.bearerToken = token
	}
}

// WithMaxRecvMsgSize overrides the max gRPC receive message size.
func WithMaxRecvMsgSize(size int) ClientOption {
	return func(o *clientOpts) {
		o.maxRecvMsgSize = size
	}
}

// NewClient creates a new Flight Client connected to the given address.
func NewClient(addr string, opts ...ClientOption) (*Client, error) {
	o := defaultClientOpts()
	for _, opt := range opts {
		opt(o)
	}

	grpcOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(o.maxRecvMsgSize)),
	}

	conn, err := grpc.NewClient(addr, grpcOpts...)
	if err != nil {
		return nil, fmt.Errorf("flight client dial %s: %w", addr, err)
	}

	c := &Client{
		conn:   conn,
		client: flight.NewClientFromConn(conn, nil),
	}
	return c, nil
}

// Query executes a SQL query and returns a RecordReader of results.
// It encapsulates the full GetFlightInfo → DoGet flow.
func (c *Client) Query(ctx context.Context, sql string) (array.RecordReader, error) {
	// 1. GetFlightInfo
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  mustMarshal(FlightDescriptor{SQL: sql}),
	}

	info, err := c.client.GetFlightInfo(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("get flight info: %w", err)
	}

	// 2. DoGet using the first endpoint's ticket
	if len(info.Endpoint) == 0 {
		return nil, fmt.Errorf("no endpoints in FlightInfo")
	}

	stream, err := c.client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, fmt.Errorf("do get: %w", err)
	}

	// 3. Wrap the stream in a RecordReader for zero-copy consumption
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, fmt.Errorf("record reader: %w", err)
	}

	return reader, nil
}

// ListMeasurements returns available measurements via ListFlights.
// Returns a simplified list of MeasurementInfo; full schema requires GetFlightInfo per measurement.
func (c *Client) ListMeasurements(ctx context.Context) ([]MeasurementInfo, error) {
	stream, err := c.client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		return nil, fmt.Errorf("list flights: %w", err)
	}

	var results []MeasurementInfo
	for {
		info, err := stream.Recv()
		if err != nil {
			break // end of stream
		}
		// Parse the FlightInfo for measurement metadata
		mi := MeasurementInfo{}
		if info.FlightDescriptor != nil {
			var fd FlightDescriptor
			if json.Unmarshal(info.FlightDescriptor.Cmd, &fd) == nil {
				mi.Database = fd.Database
				mi.Measurement = fd.Measurement
			}
		}
		results = append(results, mi)
	}
	return results, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
