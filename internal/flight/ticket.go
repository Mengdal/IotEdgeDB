package flight

// QueryTicket is the custom ticket format for DoGet/DoPut requests.
// Serialized as JSON and carried inside the Flight Ticket bytes.
type QueryTicket struct {
	SQL         string `json:"sql"`
	Database    string `json:"database,omitempty"`
	Measurement string `json:"measurement,omitempty"`
}

// FlightDescriptor is the custom command format carried in the FlightDescriptor.
// It is sent by clients in GetFlightInfo, DoPut, etc.
type FlightDescriptor struct {
	SQL         string `json:"sql"`
	Database    string `json:"database,omitempty"`
	Measurement string `json:"measurement,omitempty"`
}

// IngestDescriptor is the command format for DoPut ingestion requests.
type IngestDescriptor struct {
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
}

// MeasurementInfo contains metadata about a single measurement.
type MeasurementInfo struct {
	Database    string   `json:"database"`
	Measurement string   `json:"measurement"`
	Columns     []string `json:"columns"`
}
