// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iterrt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/accumulo/shoal/internal/rfile/wire"
)

// LatentEdgeDiscoveryIterator is the majc iterator on the graph_vidx table —
// a port of org.apache.accumulo.core.graph.LatentEdgeDiscoveryIterator.
//
// For every tessellation cell encountered (row prefix before the first ':'),
// the iterator buffers all `V:_embedding` cells in that cell, runs pairwise
// cosine similarity, and writes bidirectional `link:<otherVertex>` cells
// for every pair at or above the similarity threshold. Original cells pass
// through unchanged. Link cells already present (CF="link") are passed
// through but NOT re-processed.
//
// The output is the union of source cells + emitted link cells, sorted by
// wire.Key — matching the Java TreeMap-merge approach. Buffer-everything-
// on-seek is intentional: a compaction iterator already drains the source
// to write one output RFile, so the memory pressure matches Java.
//
// Options (Java parity):
//
//	similarityThreshold (float, default 0.85)
//	maxPairsPerCell     (int,   default 500)
//	maxCellBuffer       (int,   default 200)
//
// All three are read at Init; later option changes have no effect (matches
// Java).
type LatentEdgeDiscoveryIterator struct {
	source SortedKeyValueIterator

	similarityThreshold float32
	maxPairsPerCell     int
	maxCellBuffer       int

	out      []Cell // merged + sorted output buffer
	outIndex int
	err      error
}

// LatentEdgeDiscoveryIterator option keys.
const (
	LatentEdgeSimilarityThreshold = "similarityThreshold"
	LatentEdgeMaxPairsPerCell     = "maxPairsPerCell"
	LatentEdgeMaxCellBuffer       = "maxCellBuffer"
)

// Cell-stream constants — match VectorIndexWriter/LatentEdgeDiscoveryIterator.
const (
	latentVertexCF      = "V"
	latentEmbeddingCQ   = "_embedding"
	latentLinkCF        = "link" // VectorIndexWriter.LINK_COLFAM
	latentDefaultThresh = float32(0.85)
	latentDefaultPairs  = 500
	latentDefaultBuffer = 200
)

// NewLatentEdgeDiscoveryIterator constructs an un-Init'd iterator.
func NewLatentEdgeDiscoveryIterator() *LatentEdgeDiscoveryIterator {
	return &LatentEdgeDiscoveryIterator{
		similarityThreshold: latentDefaultThresh,
		maxPairsPerCell:     latentDefaultPairs,
		maxCellBuffer:       latentDefaultBuffer,
	}
}

// Init parses the three options from the map (parse errors surface here,
// matching Java's Float.parseFloat / Integer.parseInt failure modes — Java
// would NumberFormatException; we return an error).
func (l *LatentEdgeDiscoveryIterator) Init(source SortedKeyValueIterator, options map[string]string, env IteratorEnvironment) error {
	if source == nil {
		return errors.New("iterrt: LatentEdgeDiscoveryIterator requires a non-nil source")
	}
	l.source = source

	if s, ok := options[LatentEdgeSimilarityThreshold]; ok && s != "" {
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return fmt.Errorf("iterrt: LatentEdgeDiscoveryIterator bad %s=%q", LatentEdgeSimilarityThreshold, s)
		}
		l.similarityThreshold = float32(v)
	}
	if s, ok := options[LatentEdgeMaxPairsPerCell]; ok && s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("iterrt: LatentEdgeDiscoveryIterator bad %s=%q", LatentEdgeMaxPairsPerCell, s)
		}
		l.maxPairsPerCell = v
	}
	if s, ok := options[LatentEdgeMaxCellBuffer]; ok && s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("iterrt: LatentEdgeDiscoveryIterator bad %s=%q", LatentEdgeMaxCellBuffer, s)
		}
		l.maxCellBuffer = v
	}
	return nil
}

// Seek drains the source for the range, buffers embeddings per
// tessellation cell, runs pairwise similarity at each cell boundary, and
// merges the emitted link cells with the original cell stream sorted by
// wire.Key. Subsequent HasTop/GetTopKey/GetTopValue/Next walk the buffer.
func (l *LatentEdgeDiscoveryIterator) Seek(r Range, columnFamilies [][]byte, inclusive bool) error {
	l.out = l.out[:0]
	l.outIndex = 0
	l.err = nil

	if err := l.source.Seek(r, columnFamilies, inclusive); err != nil {
		l.err = err
		return err
	}

	// cellEmbeddings keys are vertex IDs within the current tessellation cell.
	// We retain insertion order via a parallel slice so the deterministic
	// pair-order (i<j) matches Java's `new ArrayList<>(embeddings.keySet())`
	// — Java HashMap iteration order is undefined, but our output buffer is
	// re-sorted anyway, so determinism here is for test reproducibility, not
	// correctness.
	cellVerts := []string{}
	cellEmb := map[string][]float32{}
	var currentCell string
	emittedTs := time.Now().UnixMilli()

	for l.source.HasTop() {
		k := l.source.GetTopKey().Clone()
		v := append([]byte(nil), l.source.GetTopValue()...)
		// Pass through every source cell unchanged.
		l.out = append(l.out, Cell{Key: k, Value: v})

		// Already-emitted link cells from a previous compaction must NOT
		// be re-processed for similarity. Pass-through above already kept
		// them; we just skip the embedding logic.
		if string(k.ColumnFamily) == latentLinkCF {
			if err := l.source.Next(); err != nil {
				l.err = err
				return err
			}
			continue
		}

		// Only embedding cells trigger pairwise discovery.
		if string(k.ColumnFamily) != latentVertexCF || string(k.ColumnQualifier) != latentEmbeddingCQ {
			if err := l.source.Next(); err != nil {
				l.err = err
				return err
			}
			continue
		}

		// Row format is <cellIdHex>:<vertexId>.
		sep := bytes.IndexByte(k.Row, ':')
		if sep < 0 {
			if err := l.source.Next(); err != nil {
				l.err = err
				return err
			}
			continue
		}
		cellID := string(k.Row[:sep])
		vertexID := string(k.Row[sep+1:])

		if currentCell != "" && currentCell != cellID {
			l.processCell(currentCell, cellVerts, cellEmb, emittedTs)
			cellVerts = cellVerts[:0]
			for kk := range cellEmb {
				delete(cellEmb, kk)
			}
		}
		currentCell = cellID

		if emb := parseEmbedding(v); emb != nil && len(cellEmb) < l.maxCellBuffer {
			if _, dup := cellEmb[vertexID]; !dup {
				cellVerts = append(cellVerts, vertexID)
			}
			cellEmb[vertexID] = emb
		}

		if err := l.source.Next(); err != nil {
			l.err = err
			return err
		}
	}

	if currentCell != "" && len(cellEmb) > 0 {
		l.processCell(currentCell, cellVerts, cellEmb, emittedTs)
	}

	// Sort merged stream by wire.Key ordering.
	sort.SliceStable(l.out, func(i, j int) bool {
		return l.out[i].Key.Compare(l.out[j].Key) < 0
	})

	return nil
}

// processCell does the pairwise cosine-similarity sweep over one cell's
// embeddings and appends bidirectional link cells to the output buffer.
// The output buffer is re-sorted in Seek after every cell is processed.
//
// We sort the vertex IDs before enumerating pairs so the
// maxPairsPerCell cap selects a deterministic, cross-implementation-
// stable subset. Without this, the cap interacts with whatever order
// the caller buffered vertices in (Go: source-insertion; Java:
// HashMap.keySet()) and the two implementations disagree on which
// first-500 pairs to check. Shadow oracle's hash compare surfaced
// the divergence at ~14% link-cell delta against Java 4.0's iterator;
// the matching Java fix is in LatentEdgeDiscoveryIterator.java
// (Collections.sort(vertexIds) at the same point).
func (l *LatentEdgeDiscoveryIterator) processCell(cellID string, vertices []string, emb map[string][]float32, ts int64) {
	sort.Strings(vertices)
	pairsChecked := 0
	for i := 0; i < len(vertices) && pairsChecked < l.maxPairsPerCell; i++ {
		for j := i + 1; j < len(vertices) && pairsChecked < l.maxPairsPerCell; j++ {
			a := vertices[i]
			b := vertices[j]
			ea := emb[a]
			eb := emb[b]
			if len(ea) != len(eb) {
				pairsChecked++
				continue
			}
			sim := cosineSimilarity(ea, eb)
			pairsChecked++
			if sim < l.similarityThreshold {
				continue
			}
			// Java formats with %.4f; match exactly so wire-readable
			// content is bit-identical.
			scoreStr := fmt.Sprintf("%.4f", sim)
			val := []byte(scoreStr)

			// Bidirectional link cells. ColumnVisibility is left empty —
			// Java constructs Key(row, cf, cq, ts) which sets visibility
			// to empty. If/when ABAC propagation is wired here, it'd come
			// from the source cell's visibility (out of scope for parity).
			l.out = append(l.out,
				Cell{
					Key: &wire.Key{
						Row:             []byte(cellID + ":" + a),
						ColumnFamily:    []byte(latentLinkCF),
						ColumnQualifier: []byte(b),
						Timestamp:       ts,
					},
					Value: val,
				},
				Cell{
					Key: &wire.Key{
						Row:             []byte(cellID + ":" + b),
						ColumnFamily:    []byte(latentLinkCF),
						ColumnQualifier: []byte(a),
						Timestamp:       ts,
					},
					Value: val,
				},
			)
		}
	}
}

// parseEmbedding decodes a BIG_ENDIAN float32 array — matches
// VectorIndexWriter's value encoding. Returns nil on any length anomaly
// (sub-4 bytes, non-multiple-of-4 length, or empty) so the caller skips
// the cell.
func parseEmbedding(v []byte) []float32 {
	if len(v) < 4 || len(v)%4 != 0 {
		return nil
	}
	out := make([]float32, len(v)/4)
	for i := range out {
		bits := binary.BigEndian.Uint32(v[i*4 : i*4+4])
		out[i] = math.Float32frombits(bits)
	}
	return out
}

// cosineSimilarity is dot(a,b) / (||a|| * ||b||) — direct port. Returns 0
// when either vector has zero norm (matches Java's guard).
func cosineSimilarity(a, b []float32) float32 {
	var dot, na, nb float32
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(na))*math.Sqrt(float64(nb)))
}

// HasTop reports whether a cell is available.
func (l *LatentEdgeDiscoveryIterator) HasTop() bool {
	return l.err == nil && l.outIndex < len(l.out)
}

// GetTopKey returns the current top key, or nil when exhausted.
func (l *LatentEdgeDiscoveryIterator) GetTopKey() *Key {
	if !l.HasTop() {
		return nil
	}
	return l.out[l.outIndex].Key
}

// GetTopValue returns the current top value, or nil when exhausted.
func (l *LatentEdgeDiscoveryIterator) GetTopValue() []byte {
	if !l.HasTop() {
		return nil
	}
	return l.out[l.outIndex].Value
}

// Next advances past the current top.
func (l *LatentEdgeDiscoveryIterator) Next() error {
	if l.err != nil {
		return l.err
	}
	if !l.HasTop() {
		return errors.New("iterrt: LatentEdgeDiscoveryIterator.Next called without a top")
	}
	l.outIndex++
	return nil
}

// DeepCopy returns an un-Seeked iterator over a DeepCopy'd source, with
// the same options carried forward (matches Java's deepCopy behaviour).
func (l *LatentEdgeDiscoveryIterator) DeepCopy(env IteratorEnvironment) SortedKeyValueIterator {
	return &LatentEdgeDiscoveryIterator{
		source:              l.source.DeepCopy(env),
		similarityThreshold: l.similarityThreshold,
		maxPairsPerCell:     l.maxPairsPerCell,
		maxCellBuffer:       l.maxCellBuffer,
	}
}
