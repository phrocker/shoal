<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Shoal architecture

## Goal
Lightweight read-only Accumulo replica. JVM-free pods that open RFiles
directly from object storage, run a minimal iterator stack, and serve
hedged single-shot `scan()` calls.

## V0 boundary

**In V0:**
- ZK + metadata-table bootstrap (Option A: from-scratch Go client, no JVM).
- RFile reader port from `core/.../file/rfile/RFile.java`, with two
  sharkbite-cribbed optimizations: async block readahead, in-block visibility
  filtering (skip cells before decompression).
- VisibilityFilter only — no other iterators.
- Thrift `TabletScanClientService.startScan` only — no `startMultiScan`,
  `continueMultiScan`, or session state. Each scan is self-contained.
- Per-replica LRU block cache. Per-replica is non-negotiable; shared cache
  defeats hedged reads.
- Java-side hedge coordinator in `platform/provisioner/` (out of this repo
  subtree, but called out for completeness).

**Out of V0:** all other iterators (V1–V4 in the staging plan), FRESH-tier
reads, server-side BatchScanner (range fan-out happens client-side in the
SDK), HTTP frontend, full Prometheus integration, sharkbite's Python
lambda-iterator path.

## Bootstrap chain (Option A)

1. ZK → `/accumulo/instances/<name>` → instance UUID.
2. ZK → `/accumulo/<uuid>/root_tablet` → JSON `RootTabletMetadata` →
   tserver host:port hosting the root tablet.
3. Thrift `scan()` against root tablet → metadata-tablet locations + their
   files.
4. Thrift `scan()` against metadata tablets → user-table tablet→file map.
5. Cache; refresh by exception (sharkbite-style), not TTL.

## Wire protocol

Apache Thrift, IDL pinned to **0.17.0** (matches Accumulo's
`version.thrift`). Compiler at
`/mnt/ExtraDrive/repos/tools/thrift-0.17.0/bin/thrift`.

Transport: `TFramedTransport`. Protocol: `TCompactProtocol` wrapped in a
custom `AccumuloProtocol` that prepends/validates a header on every
message. Header (encoded through TCompactProtocol's writers, not raw
bytes): `i32` magic `0x41434355` ("ACCU") + `byte` protocol version (1) +
`string` Accumulo version (major.minor must match server) + `string`
instance ID (must match server's). Server validates all four fields and
errors on any mismatch. See `core/.../rpc/AccumuloProtocolFactory.java`.
Sharkbite predates this header and does not implement it.

## Caching strategy

Tablet→location and tablet→file maps cached in-process. Invalidate on
Thrift errors that indicate stale state (NotServingTabletException,
transport-level failures) — same approach as sharkbite's `LocatorCache` +
`CachedTransport`. No TTL.

## Locked decisions (no re-litigation)

- Language: Go (qwal precedent).
- Wire: existing Accumulo Thrift, not new gRPC.
- Tablet→RFile source: metadata table (pulled, with exception-driven cache
  invalidation), NOT a ZK watch — corrected from the original v0 spec.
- Hedge coordinator: client SDK in Java provisioner.
- Cache: per-replica LRU.
- No FRESH tier.
