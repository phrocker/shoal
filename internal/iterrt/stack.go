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

import "fmt"

// IterSpec names one iterator to compose into a stack, plus its
// per-iterator options. The parity harness, the compaction-stack
// composer (Phase C3), and the WAL merge stack (Bet 2) all describe a
// stack as an ordered []IterSpec.
type IterSpec struct {
	// Name is the iterator's registered identifier. See BuildStack for
	// the C1 set.
	Name string
	// Options is the per-iterator option map handed to Init.
	Options map[string]string
}

// Stack identifiers recognized by BuildStack. The set grows as iterator
// ports land (the design doc's iterator router allowlist).
const (
	// IterVersioning is VersioningIterator — newest-N versions per
	// coordinate. Option "maxVersions" (default 1).
	IterVersioning = "versioning"
	// IterVisibility is VisibilityFilter — drops cells the env's
	// Authorizations cannot satisfy. Active only at ScopeScan.
	IterVisibility = "visibility"
	// IterDeleting is DeletingIterator — applies tombstone suppression.
	// Options "propagateDeletes" (bool) and "behavior" ("process"|"fail")
	// override env-derived defaults; default propagateDeletes is false
	// only at ScopeMajc with FullMajorCompaction, true otherwise.
	IterDeleting = "deleting"
	// IterLatentEdgeDiscovery is the graph_vidx majc iterator that emits
	// bidirectional `link:<otherVertex>` cells for embedding pairs above
	// a similarity threshold, scoped to the row's tessellation cell.
	// Options: similarityThreshold, maxPairsPerCell, maxCellBuffer.
	IterLatentEdgeDiscovery = "latentEdgeDiscovery"
)

// BuildStack composes an iterator stack on top of leaf, in order: specs[0]
// sits directly above leaf, specs[1] above specs[0], and so on. The
// returned iterator is the top of the stack — Seek/Next it directly.
//
// Every iterator in the stack is Init'd against env. leaf is assumed
// already Init'd by the caller (it is a leaf — RFileSource or
// SliceSource — with construction-time inputs).
//
// An empty specs list returns leaf unchanged: "identity compaction"
// (cells pass through untouched), which is the C0 harness behaviour.
func BuildStack(leaf SortedKeyValueIterator, specs []IterSpec, env IteratorEnvironment) (SortedKeyValueIterator, error) {
	cur := leaf
	for i, spec := range specs {
		next, err := newIterator(spec.Name)
		if err != nil {
			return nil, fmt.Errorf("iterrt: stack position %d: %w", i, err)
		}
		if err := next.Init(cur, spec.Options, env); err != nil {
			return nil, fmt.Errorf("iterrt: stack position %d (%s): %w", i, spec.Name, err)
		}
		cur = next
	}
	return cur, nil
}

// newIterator constructs an un-Init'd iterator by name.
func newIterator(name string) (SortedKeyValueIterator, error) {
	switch name {
	case IterVersioning:
		return NewVersioningIterator(), nil
	case IterVisibility:
		return NewVisibilityFilter(), nil
	case IterDeleting:
		return NewDeletingIterator(), nil
	case IterLatentEdgeDiscovery:
		return NewLatentEdgeDiscoveryIterator(), nil
	default:
		return nil, fmt.Errorf("unknown iterator %q", name)
	}
}
