#!/usr/bin/env python3
"""
IotEdgeDB Arrow Flight Client Example

Prerequisites:
    pip install pyarrow

Usage:
    # Start IotEdgeDB with Flight enabled
    iedb --config iedb.toml    # [flight] enabled = true, addr = ":9090"

    # Run this script
    python examples/flight_client.py [--host localhost] [--port 9090] [--token YOUR_TOKEN]
"""

import argparse
import sys

try:
    import pyarrow as pa
    import pyarrow.flight as flight
except ImportError:
    print("Error: pyarrow is required. Install with: pip install pyarrow")
    sys.exit(1)


def connect(host: str, port: int, token: str | None) -> flight.FlightClient:
    """Connect to the IotEdgeDB Flight server."""
    location = f"grpc://{host}:{port}"

    if token:
        # Authenticated connection
        client = flight.FlightClient(location)
        # The token is sent via gRPC metadata in each call
        # PyArrow Flight doesn't auto-attach tokens — we use a custom header
        print(f"Connecting to {location} with token ...")
        # Note: token attachment is done per-call via options
    else:
        client = flight.FlightClient(location)
        print(f"Connecting to {location} (no auth) ...")

    return client


def list_datasets(client: flight.FlightClient):
    """List available datasets via ListFlights."""
    print("\n=== Available Datasets ===")
    try:
        for info in client.list_flights():
            print(f"  {info.descriptor}")
    except Exception as e:
        print(f"  ListFlights not available: {e}")


def query_sql(client: flight.FlightClient, sql: str):
    """Execute a SQL query and display results as an Arrow table."""
    print(f"\n=== Query: {sql} ===")

    # Flight SQL path: use do_get with a Command descriptor
    import json
    descriptor = flight.FlightDescriptor.for_command(json.dumps({"sql": sql}))

    try:
        # Get flight info (schema + endpoints)
        info = client.get_flight_info(descriptor)
        print(f"Schema: {info.schema}")

        # Read all data from the first endpoint
        reader = client.do_get(info.endpoints[0].ticket)
        table = reader.read_all()

        print(f"Rows: {table.num_rows}, Columns: {table.num_columns}")
        print(table.to_pandas() if hasattr(table, 'to_pandas') else table)

    except Exception as e:
        print(f"Query failed: {e}")


def write_data(client: flight.FlightClient):
    """Write a sample RecordBatch via DoPut."""
    print("\n=== Writing Sample Data ===")

    # Build a sample Arrow record batch
    schema = pa.schema([
        ("int_val", pa.int64()),
        ("float_val", pa.float64()),
        ("str_val", pa.string()),
        ("time", pa.timestamp("us")),
    ])

    import datetime
    now = pa.scalar(int(datetime.datetime.now().timestamp() * 1_000_000), type=pa.timestamp("us"))

    data = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            pa.array(["a", "b", "c"], type=pa.string()),
            pa.array([now, now, now], type=pa.timestamp("us")),
        ],
        schema=schema,
    )

    import json
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps({"database": "examples", "measurement": "flight_test"})
    )

    try:
        writer, reader = client.do_put(descriptor, schema)
        writer.write(data)
        writer.close()
        print(f"Wrote {data.num_rows} rows to examples.flight_test")
        # Read ack
        for ack in reader:
            print(f"  Ack: {ack}")
    except Exception as e:
        print(f"Write failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="IotEdgeDB Flight Client Example")
    parser.add_argument("--host", default="localhost", help="Flight server host")
    parser.add_argument("--port", type=int, default=9090, help="Flight server port")
    parser.add_argument("--token", help="Bearer token for authentication")
    args = parser.parse_args()

    client = connect(args.host, args.port, args.token)

    # 1. List available datasets
    list_datasets(client)

    # 2. Execute a simple query
    query_sql(client, "SELECT 1 AS value")

    # 3. Execute a DuckDB-specific query
    query_sql(client, "SELECT 'hello from flight!' AS greeting, 42 AS answer")

    # 4. Write sample data
    write_data(client)

    # 5. Query the written data
    query_sql(client, "SELECT int_val, float_val, str_val FROM read_parquet('examples/flight_test/**/*.parquet', union_by_name=true) LIMIT 5")


if __name__ == "__main__":
    main()
