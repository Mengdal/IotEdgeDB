package flight

import (
	"context"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// flightSQLServer wraps our Server to implement the flightsql.Server interface.
// It embeds flightsql.BaseServer to get default (Unimplemented) implementations.
type flightSQLServer struct {
	flightsql.BaseServer
	srv *Server
}

// --- Statement (query) ---

func (f *flightSQLServer) GetFlightInfoStatement(ctx context.Context, stmt flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, err := f.srv.verifyToken(ctx)
	if err != nil {
		return nil, err
	}

	query := stmt.GetQuery()
	reader, conn, err := f.srv.db.ArrowQueryContext(ctx, "SELECT * FROM ("+query+") LIMIT 0")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "schema query: %v", err)
	}
	schema := reader.Schema()
	reader.Release()
	conn.Close()

	ticketBytes, _ := json.Marshal(QueryTicket{SQL: query})
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Schema:           SerializeSchema(schema),
		TotalRecords:     -1,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticketBytes}}},
	}, nil
}

func (f *flightSQLServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if _, err := f.srv.verifyToken(ctx); err != nil {
		return nil, nil, err
	}

	handle := string(ticket.GetStatementHandle())
	var qt QueryTicket
	if err := json.Unmarshal([]byte(handle), &qt); err != nil {
		qt.SQL = handle
	}

	reader, conn, err := f.srv.db.ArrowQueryContext(ctx, qt.SQL)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "query: %v", err)
	}

	// Convert RecordReader to StreamChunk channel
	schema := reader.Schema()
	ch := make(chan flight.StreamChunk)
	go func() {
		defer func() {
			reader.Release()
			conn.Close()
			close(ch)
		}()
		for reader.Next() {
			rec := reader.RecordBatch()
			rec.Retain()
			select {
			case ch <- flight.StreamChunk{Data: rec}:
			case <-ctx.Done():
				return // client disconnected
			}
		}
	}()

	return schema, ch, nil
}

func (f *flightSQLServer) GetSchemaStatement(ctx context.Context, stmt flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if _, err := f.srv.verifyToken(ctx); err != nil {
		return nil, err
	}
	reader, conn, err := f.srv.db.ArrowQueryContext(ctx, "SELECT * FROM ("+stmt.GetQuery()+") LIMIT 0")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "schema query: %v", err)
	}
	schema := SerializeSchema(reader.Schema())
	reader.Release()
	conn.Close()
	return &flight.SchemaResult{Schema: schema}, nil
}

// --- Catalogs ---

func (f *flightSQLServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Ticket must be the protobuf descriptor so flightsql.DoGet dispatches to DoGetCatalogs.
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     1,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
	}, nil
}

func (f *flightSQLServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// Auth deferred to flightsql.DoGet dispatch — verified in base handler via unifiedHandler
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		b.Field(0).(*array.StringBuilder).Append("iedb")
		ch <- flight.StreamChunk{Data: b.NewRecord()}
	}()
	return schema, ch, nil
}

// --- Schemas ---

func (f *flightSQLServer) GetFlightInfoSchemas(ctx context.Context, req flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     -1,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
	}, nil
}

func (f *flightSQLServer) DoGetDBSchemas(ctx context.Context, req flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		// Query DuckDB for actual database names
		reader, conn, err := f.srv.db.ArrowQueryContext(ctx,
			"SELECT DISTINCT database_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY database_name")
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		if err == nil {
			defer reader.Release()
			defer conn.Close()
			for reader.Next() {
				rec := reader.RecordBatch()
				strCol := rec.Column(0).(*array.String)
				for i := int64(0); i < rec.NumRows(); i++ {
					b.Field(0).(*array.StringBuilder).Append("iedb")
					b.Field(1).(*array.StringBuilder).Append(strCol.Value(int(i)))
				}
			}
		}
		if b.Field(0).(*array.StringBuilder).Len() == 0 {
			// Fallback: return "default" if no databases exist yet
			b.Field(0).(*array.StringBuilder).Append("iedb")
			b.Field(1).(*array.StringBuilder).Append("default")
		}
		ch <- flight.StreamChunk{Data: b.NewRecord()}
	}()
	return schema, ch, nil
}

// --- Tables ---

func (f *flightSQLServer) GetFlightInfoTables(ctx context.Context, req flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     -1,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
	}, nil
}

func (f *flightSQLServer) DoGetTables(ctx context.Context, req flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		// Query DuckDB for actual tables (measurements in our schema mapping)
		reader, conn, err := f.srv.db.ArrowQueryContext(ctx,
			"SELECT database_name, table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY database_name, table_name")
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		if err == nil {
			defer reader.Release()
			defer conn.Close()
			for reader.Next() {
				rec := reader.RecordBatch()
				dbCol := rec.Column(0).(*array.String)
				tblCol := rec.Column(1).(*array.String)
				for i := int64(0); i < rec.NumRows(); i++ {
					b.Field(0).(*array.StringBuilder).Append("iedb")
					b.Field(1).(*array.StringBuilder).Append(dbCol.Value(int(i)))
					b.Field(2).(*array.StringBuilder).Append(tblCol.Value(int(i)))
					b.Field(3).(*array.StringBuilder).Append("TABLE")
				}
			}
		}
		ch <- flight.StreamChunk{Data: b.NewRecord()}
	}()
	return schema, ch, nil
}

// --- Table Types ---

func (f *flightSQLServer) GetFlightInfoTableTypes(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     1,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
	}, nil
}

func (f *flightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		b.Field(0).(*array.StringBuilder).Append("TABLE")
		ch <- flight.StreamChunk{Data: b.NewRecord()}
	}()
	return schema, ch, nil
}

// --- SqlInfo ---

func (f *flightSQLServer) GetFlightInfoSqlInfo(ctx context.Context, req flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     -1,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
	}, nil
}

func (f *flightSQLServer) DoGetSqlInfo(ctx context.Context, req flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// Delegate to BaseServer's SqlInfo map
	return f.BaseServer.DoGetSqlInfo(ctx, req)
}
