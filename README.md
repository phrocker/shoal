# shoal

Go-based read-fleet for [Apache Accumulo](https://accumulo.apache.org).
Replaces JVM scan-servers for narrow, well-defined read shapes.

A shoal pod opens RFiles directly from object storage (GCS, S3, local),
runs a minimal iterator stack in-process, and serves single-shot Thrift
`scan()` and `multiScan()` calls. A Java-side hedge coordinator sends each
scan to N shoal pods in parallel and returns the first response, cancelling
the rest — clients can also pick `shoal-only`, `shoal-then-stall-fallback`,
or `tserver-only` per call.

## Status

V1 + IVF-PQ iterator port shipped.

| Capability | Status |
|---|---|
| `startScan` (single tablet, single range) | shipped |
| `startMultiScan` (BatchScanner shape) | shipped — auto-bins ranges across tablets |
| Multi-locality-group merge + CF pushdown (LG-skip) | shipped |
| RFile reader (block-level CRC, prefetch, snappy/gz/zlib) | shipped |
| `RFile.blockmeta` zone-map block-skip extension | shipped — forward-compatible with Java `RFile.Reader` |
| VisibilityFilter pushdown | shipped — alloc-free warm cache |
| File / locator / block caches | shipped |
| Startup pre-warm (`-prewarm-tables=auto`) | shipped |
| Custom server-side iterator: `IvfPqDistanceIterator` | shipped — wire-compatible with Java `VectorPQ.toBytes()` |
| Java SDK hedge coordinator | shipped — `scanRow*` + `scanBatch*` overloads |

## Architecture

Shoal mirrors the Apache Accumulo `TabletScanClientService` Thrift surface but
is **not** a tablet server — it doesn't write, doesn't participate in
tablet hosting, doesn't coordinate with the manager. It's strictly a
read fleet:

```
       ┌──────────────────┐    Thrift scan()  ┌─────────────┐
       │ Java SDK         │ ─── HEDGE ──────▶ │ shoal pod   │── ◀ GCS/S3 RFiles
       │ ShoalScanRouter  │                   │  (Go)       │── ◀ ZK metadata cache
       │ HedgedScan       │ ──────────────▶   └─────────────┘
       │ Coordinator      │   (parallel)      ┌─────────────┐
       │                  │ ─────────────────▶│ tserver     │── ◀ memtables
       └──────────────────┘    Thrift scan()  │ (Java)      │── ◀ JVM block cache
                                              └─────────────┘
```

A shoal pod's read path:

1. **Bootstrap**: ZK → `/accumulo/<uuid>/root_tablet` → root metadata scan →
   `accumulo.metadata` walk → tablet→RFile map. Exception-driven cache
   invalidation (sharkbite-pattern) instead of TTL.
2. **Per scan**: locator-cache lookup → fan-out to one `fileIter` per
   (RFile, locality-group) → heap-merge by Key → visibility filter (alloc-
   free) → optional CF/iterator pushdown → emit results.
3. **Caches**: file-bytes LRU (default 1 GB), decompressed-block LRU,
   tablet-locator cache. Block-level CRC + zone-map skip when the RFile
   carries the `RFile.blockmeta` extension.

See `ARCHITECTURE.md` for design rationale and `REFERENCES.md` for upstream
Apache Accumulo + sharkbite source pointers consulted during the port.

## Build

```bash
make thrift-gen  # generate Go bindings from Accumulo's .thrift IDL
make build       # go build ./...
make test        # full test suite (race-clean)
```

Requires Apache Thrift compiler **exactly 0.17.0** (matches Accumulo's
`version.thrift` in its root `pom.xml`). Go 1.25+ (transitively from
`cloud.google.com/go/storage`).

Docker image (multi-stage, distroless static):
```bash
docker build -t shoal:dev .
```

## Layout

```
cmd/
  shoal/                main daemon — ZK + metadata + Thrift listener
  shoal-bootstrap/      diagnostic CLI: walks ZK → root → metadata → tablets
  shoal-probe/          one-shot RFile probe (version + LG summary + walk count)
  shoal-rfile-pull/     gs://… → local copy
  shoal-rfile-write/    synthetic RFile writer (test fixtures)
  shoal-scan-client/    Thrift StartScan from CLI
  shoal-count-row/      row-count micro-bench against a tablet

internal/
  protocol/             AccumuloProtocol — magic + version + instance-id header
  zk/                   ZooKeeper client + root-tablet locator
  cred/                 Hadoop-Writable PasswordToken encoding
  metadata/             metadata-table walker — tablet→file map bootstrap
  cclient/              cooked Go types (KeyExtent, Range, Authorizations, …)
  scanclient/           Thrift client wrapper (TSocket → framed → AccumuloProtocol → MUX)
  cache/                LRU caches: tablet locator, decompressed blocks, RFile bytes
  storage/              backend interface + local / memory / gcs implementations
  rfile/                RFile reader (block-level seek, multi-LG, multi-level index)
    bcfile/             BCFile container (footer, meta-index, block layout)
      block/            decompressor + sharkbite-style async prefetcher
    relkey/             relative-key decoder (cursor-based, zero-copy views)
    index/              RFile.index parsing + multi-level walker
    blockmeta/          RFile.blockmeta optional meta-block — zone-map + skip predicate
    wire/               Java DataInput primitives (UTF, varint, key codec)
  visfilter/            CV expression parser + Authorizations + alloc-free evaluator
  ivfpq/                IvfPqDistanceIterator Go port (V1)
  scanserver/           Thrift TabletScanClientService implementation
  thrift/gen/           generated Thrift bindings (gitignored, run thrift-gen)
```

## Custom iterators

The hedge coordinator can route through shoal whenever the underlying scan
is iterator-free OR uses one of shoal's natively-recognized iterators.
Currently recognized:

- **`org.apache.accumulo.core.graph.ann.IvfPqDistanceIterator`** — full
  ADC-distance + top-K + threshold replicated in `internal/ivfpq/`.
  Wire-compatible with the Java side's `VectorPQ.toBytes()` and
  `IvfPqDistanceIterator.encodeQuery`. When this iterator appears in the
  `ssiList` of a multi-scan, shoal runs it natively and returns the same
  top-K output a tserver-side iterator would.

Anything else in `ssiList` errors out server-side rather than silently
producing wrong answers — callers fall back to tserver in that case.

## Operational notes

- ZK watch lookup → `/accumulo/<uuid>/root_tablet` + metadata table walk;
  exception-driven cache invalidation (sharkbite pattern) instead of TTL.
- Block-level CRC check via the `RFile.blockmeta` extension when present;
  zone-map skip predicate avoids decompressing blocks that can't match.
- Visibility filtering pushed down into the relkey decoder; reject path
  doesn't allocate or copy values.
- One Server instance per pod; goroutine-safe across concurrent scans.
- Default file cache 1 GB, decompressed-block cache configurable.
- Pre-warm walks user-table tablets at startup; first scan is warm-fast.

## License

Apache License, Version 2.0.
