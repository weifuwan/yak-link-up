# Link-Up

Link-Up is a local, batch-oriented data synchronization engine. It currently provides JDBC source and sink connectors,
HOCON job parsing, parallel source/sink execution, execution metrics, and a standalone Worker API.

## Architecture

The project is organized around explicit dependency boundaries instead of technical catch-all modules:

```text
                    link-up-launcher / link-up-server
                       /                     \
                      v                       v
             link-up-framework        connector implementations
                      \                       /
                       v                     v
                           link-up-api
```

The rules are intentionally strict:

- `link-up-api` is the stable extension-facing contract. It must not depend on framework, server, launcher, or concrete connectors.
- `link-up-framework` implements planning and local runtime behavior and depends only on API contracts, never on concrete connector modules.
- connector modules depend on `link-up-api`, not on framework internals.
- `link-up-launcher` and `link-up-server` are composition roots that assemble the framework with concrete connectors.

The runtime architecture is Flink-inspired at the role level (`Source`, `SourceReader`, `SourceSplit`, planner, task runtime),
without copying Flink's distributed complexity. See [architecture](docs/architecture.md),
[ADR-0001](docs/adr/0001-flink-inspired-runtime-roles.md), and the
[connector development guide](docs/connector-development.md).

## Quick start

Build the complete project and its deployable archives with JDK 8+ and Maven 3.8.1+:

```bash
mvn --batch-mode clean verify
```

Run directly from source:

```bash
mvn -pl link-up-launcher -am compile exec:java \
  -Dexec.mainClass=com.link.up.launcher.LocalSyncLauncher \
  -Dexec.args=link-up-launcher/examples/jdbc-single-table.conf
```

Or extract `link-up-dist/target/link-up-1.0.0.tar.gz`, copy and edit `config/link-up.yaml`, then run:

```bash
bin/link-up.sh --config config/link-up.yaml
```

The configuration file has a `.yaml` deployment-friendly name but uses HOCON syntax. Do not commit real JDBC
credentials. See the [deployment and operations guide](docs/deployment.md) for packaging, CI, Docker, JVM options,
Log4j2, security, rollback, and production guidance.

## Offline Worker protocol

`link-up-server` is a single-node, offline-only execution Worker. Its lifecycle is:

```text
CREATED -> SUBMITTED -> QUEUED -> RUNNING
                                  -> SUCCEEDED / FAILED / CANCELED / LOST
```

The JSON submit protocol supports control-plane `externalExecutionId`, `idempotencyKey`, definition versioning,
auditable state transitions, worker instance identity and deterministic duplicate submission handling. The previous
HOCON body submission remains available for CLI and compatibility use.

See [the single-node offline Worker protocol](docs/worker-protocol.md) for the complete API and state ownership contract.

## Connector Schema

Link-Up exports Connector options, types, defaults, validation rules, semantic metadata and capabilities through a
stable machine-readable schema:

```http
GET /api/v1/connectors
GET /api/v1/connectors/{connectorId}/schema?role=SOURCE
GET /api/v1/connectors/{connectorId}/schema?role=SINK
```

The schema is execution-focused and frontend-framework neutral. Yak Ops can cache it and combine it with its own
Presentation Profile to produce a product-oriented form without duplicating Connector validation rules.

See [the Connector Schema protocol](docs/connector-schema.md) for the complete contract and phase-one boundaries.
