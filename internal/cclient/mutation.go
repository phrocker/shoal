package cclient

import "errors"

// MutationLatestTimestamp is the sentinel value Accumulo uses for "use the
// server-side current time" — matches Java's `Long.MAX_VALUE` and
// sharkbite's `9223372036854775807L` (Mutation.h:54).
const MutationLatestTimestamp int64 = 9223372036854775807

// MutationEntry is one Put or Delete in a mutation. Serialization to the
// Accumulo on-wire mutation format (see TMutation.Data in
// internal/thrift/gen/data) is intentionally NOT implemented here — that
// belongs in a follow-on once the read fleet has a write path.
//
// Reference: sharkbite include/data/constructs/Mutation.h:38-86.
type MutationEntry struct {
	ColFamily     []byte
	ColQualifier  []byte
	ColVisibility []byte
	Timestamp     int64
	Value         []byte
	Deleted       bool
}

// Mutation is a row + ordered list of column entries. Mirrors the
// sharkbite class shape (Mutation.h:41-86). The `Data []byte` payload that
// `TMutation.Data` carries on the wire is computed by `Serialize()` —
// currently a TODO.
type Mutation struct {
	row     []byte
	entries []MutationEntry
}

// NewMutation allocates a Mutation for the given row. row must be
// non-empty (Mutation.h:47 — sharkbite takes a const std::string& row but
// Accumulo rejects empty rows server-side).
func NewMutation(row []byte) (*Mutation, error) {
	if len(row) == 0 {
		return nil, errors.New("cclient: Mutation row must be non-empty")
	}
	rowCopy := make([]byte, len(row))
	copy(rowCopy, row)
	return &Mutation{row: rowCopy}, nil
}

// Row returns the row this mutation targets.
func (m *Mutation) Row() []byte { return m.row }

// Entries returns the ordered list of column entries.
func (m *Mutation) Entries() []MutationEntry { return m.entries }

// Put appends a Put entry. cv may be nil (default visibility). Pass
// MutationLatestTimestamp for "let the server stamp it".
//
// Reference: Mutation.h:57-70 (the put overloads).
func (m *Mutation) Put(cf, cq, cv []byte, timestamp int64, value []byte) {
	m.entries = append(m.entries, MutationEntry{
		ColFamily:     cf,
		ColQualifier:  cq,
		ColVisibility: cv,
		Timestamp:     timestamp,
		Value:         value,
		Deleted:       false,
	})
}

// PutLatest is the convenience form: timestamp = MutationLatestTimestamp.
func (m *Mutation) PutLatest(cf, cq, cv, value []byte) {
	m.Put(cf, cq, cv, MutationLatestTimestamp, value)
}

// Delete appends a Delete entry. A delete is a tombstone — value is
// always empty for deletes (Mutation.h:49-55).
func (m *Mutation) Delete(cf, cq, cv []byte, timestamp int64) {
	m.entries = append(m.entries, MutationEntry{
		ColFamily:     cf,
		ColQualifier:  cq,
		ColVisibility: cv,
		Timestamp:     timestamp,
		Value:         nil,
		Deleted:       true,
	})
}

// DeleteLatest is the convenience form: timestamp = MutationLatestTimestamp.
func (m *Mutation) DeleteLatest(cf, cq, cv []byte) {
	m.Delete(cf, cq, cv, MutationLatestTimestamp)
}

// Size returns the number of column entries — matches sharkbite's
// `Mutation::size()` (Mutation.h:76-78).
func (m *Mutation) Size() int { return len(m.entries) }

// Serialize encodes the mutation into the Accumulo on-wire format and
// returns the bytes ready to populate TMutation.Data. NOT YET IMPLEMENTED
// — the read fleet has no write path in V0. Tracking this as a TODO so
// the type surface is stable when the writer arrives.
//
// The format is documented in:
//   - sharkbite src/data/constructs/Mutation.cpp (the byte layout)
//   - Java org.apache.accumulo.core.data.Mutation#serialize
func (m *Mutation) Serialize() ([]byte, error) {
	return nil, errors.New("cclient: Mutation.Serialize not yet implemented (write path is post-V0)")
}
