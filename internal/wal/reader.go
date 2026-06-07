package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

// Reader reads WAL files for recovery operations
type Reader struct {
	filePath string
	logger   zerolog.Logger

	// Metrics
	TotalEntries     int64
	TotalBytes       int64
	CorruptedEntries int64
}

// NewReader creates a new WAL reader
func NewReader(filePath string, logger zerolog.Logger) *Reader {
	return &Reader{
		filePath: filePath,
		logger:   logger.With().Str("component", "wal-reader").Logger(),
	}
}

// Entry represents a single WAL entry
type Entry struct {
	TimestampUS  uint64                   // Microseconds since epoch
	Records      []map[string]interface{} // Row format (from Append path)
	ColumnarData *ColumnarEntry           // Columnar format (from AppendRaw path)
	Control      *ControlRecord           // Control record (FLUSH_OK / FLUSH_FAIL)
}

// ColumnarEntry represents a columnar WAL entry written via the zero-copy path
type ColumnarEntry struct {
	Database    string // From envelope metadata (empty = "default")
	Measurement string
	Columns     map[string][]interface{}
}

// ReadAll reads all entries from the WAL file
func (r *Reader) ReadAll() ([]Entry, error) {
	var entries []Entry

	f, err := os.Open(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer f.Close()

	// Read and verify header
	header := make([]byte, WALFileHeaderSize)
	n, err := f.Read(header)
	if err != nil || n < WALFileHeaderSize {
		r.logger.Warn().Str("file", r.filePath).Msg("WAL file too short")
		return entries, nil
	}

	// Verify magic bytes
	if !bytes.Equal(header[0:4], WALMagic) {
		return nil, fmt.Errorf("invalid WAL magic bytes")
	}

	// Check version
	version := binary.BigEndian.Uint16(header[4:6])
	if version != WALVersion {
		r.logger.Warn().
			Uint16("file_version", version).
			Uint16("expected_version", WALVersion).
			Msg("WAL version mismatch")
	}

	// Read entries
	for {
		entry, err := r.readEntry(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			r.logger.Warn().Err(err).Msg("Skipping incomplete WAL entry")
			r.CorruptedEntries++
			continue
		}

		entries = append(entries, *entry)
		r.TotalEntries++
	}

	r.logger.Info().
		Str("file", r.filePath).
		Int64("entries", r.TotalEntries).
		Int64("bytes", r.TotalBytes).
		Int64("corrupted", r.CorruptedEntries).
		Msg("WAL read complete")

	return entries, nil
}

// readEntry reads a single entry from the file
func (r *Reader) readEntry(f *os.File) (*Entry, error) {
	// Read entry header
	header := make([]byte, WALEntryHeaderSize)
	n, err := f.Read(header)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil || n < WALEntryHeaderSize {
		return nil, fmt.Errorf("failed to read entry header: %w", err)
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	timestampUS := binary.BigEndian.Uint64(header[4:12])
	expectedChecksum := binary.BigEndian.Uint32(header[12:16])

	// Validate payload length to prevent OOM on corrupt WAL files
	if payloadLen > MaxWALPayloadSize {
		return nil, fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, payloadLen, MaxWALPayloadSize)
	}

	// Read payload
	payload := make([]byte, payloadLen)
	n, err = io.ReadFull(f, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	r.TotalBytes += int64(WALEntryHeaderSize + n)

	// Verify checksum
	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
	}

	// Dispatch on payload marker byte
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty WAL entry payload")
	}

	// Control records: FLUSH_OK (0x00) or FLUSH_FAIL (0x02).
	// NOTE: Rolling back to a pre-FLUSH_OK binary will see these as
	// "corrupted entries" since 0x00 and 0x02 are not valid msgpack.
	// This is harmless — the entries are skipped and the counter is
	// informational. Upgrade path: run the new binary to recover, then
	// the control records are properly parsed.
	if payload[0] == WALFlushOK || payload[0] == WALFlushFail {
		db, meas := parseControlPayload(payload)
		return &Entry{
			TimestampUS: timestampUS,
			Control: &ControlRecord{
				Type:        ControlType(payload[0]),
				Database:    db,
				Measurement: meas,
			},
		}, nil
	}

	// Data entries: try ParseEnvelope (handles 0x01 marker), then msgpack deserialization
	database, msgpackData := ParseEnvelope(payload, "")

	// Try row format first (array of maps from Append path)
	var records []map[string]interface{}
	if err := msgpack.Unmarshal(msgpackData, &records); err == nil {
		return &Entry{
			TimestampUS: timestampUS,
			Records:     records,
		}, nil
	}

	// Try columnar format (map with m + columns from AppendRaw path)
	var rawMap map[string]interface{}
	if err := msgpack.Unmarshal(msgpackData, &rawMap); err == nil {
		if colEntry := parseColumnarEntry(rawMap); colEntry != nil {
			colEntry.Database = database
			return &Entry{
				TimestampUS:  timestampUS,
				ColumnarData: colEntry,
			}, nil
		}
	}

	return nil, fmt.Errorf("failed to deserialize: unrecognized WAL entry format")
}

// parseColumnarEntry extracts measurement and columns from a raw msgpack map
func parseColumnarEntry(rawMap map[string]interface{}) *ColumnarEntry {
	m, ok := rawMap["m"].(string)
	if !ok {
		return nil
	}

	colsRaw, ok := rawMap["columns"]
	if !ok {
		return nil
	}

	colsMap, ok := colsRaw.(map[string]interface{})
	if !ok {
		return nil
	}

	columns := make(map[string][]interface{}, len(colsMap))
	for k, v := range colsMap {
		arr, ok := v.([]interface{})
		if !ok {
			continue
		}
		columns[k] = arr
	}

	return &ColumnarEntry{
		Measurement: m,
		Columns:     columns,
	}
}
