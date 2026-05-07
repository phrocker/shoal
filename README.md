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
# shoal

Go-based read-fleet for Apache Accumulo. Replaces JVM scan-servers for narrow,
well-defined read shapes.

A shoal pod opens RFiles directly from object storage, runs a minimal iterator
stack in-process, and serves single-shot Thrift `scan()` calls. A Java-side
hedge coordinator (in `platform/provisioner/`) sends each scan to N shoal pods
in parallel and returns the first response, cancelling the rest.

## Status

V0 in progress. See `ARCHITECTURE.md` for the design and `REFERENCES.md` for
upstream + sharkbite source pointers used during the port.

## Build

```bash
make thrift-gen  # generate Go bindings from Accumulo's .thrift IDL
make build       # go build ./...
make test
```

Requires Apache Thrift compiler **exactly 0.17.0** at
`/mnt/ExtraDrive/repos/tools/thrift-0.17.0/bin/thrift` (matches Accumulo's
`version.thrift` in the root `pom.xml`).

## Layout

```
cmd/shoal/             entrypoint
internal/
  protocol/            AccumuloProtocol wrapper (5-byte ACCU magic header)
  zk/                  ZooKeeper client + root-tablet locator
  metadata/            metadata-table client; tablet→file map bootstrap
  cache/               per-replica LRU block cache + tablet-location cache
  rfile/               RFile reader (Java→Go port, sharkbite-inspired)
  visfilter/           VisibilityFilter port (in-block, not iterator-wrapped)
  scanserver/          Thrift TabletScanClientService server impl
  thrift/gen/          generated Thrift bindings (gitignored, run thrift-gen)
deploy/                k8s manifests / chart additions
```
