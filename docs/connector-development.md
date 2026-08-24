# Connector Development Guide

This guide defines the package and role conventions for Link-Up connectors. New connectors should follow this layout;
existing connectors can migrate incrementally when touched.

## Dependency rule

A connector is an implementation of `link-up-api` contracts. It must not depend on or import `link-up-framework`.
Framework internals are free to evolve without breaking connector implementations.

## Recommended layout

```text
com.link.up.connector.<name>
├── <Name>SourceFactory.java
├── <Name>SinkFactory.java
├── source
│   ├── <Name>Source.java
│   ├── <Name>SourceSplit.java
│   ├── <Name>SourceSplitEnumerator.java
│   └── <Name>SourceReader.java
├── sink
│   ├── <Name>SinkWriter.java
│   └── <Name>SinkPreparer.java
├── catalog
│   ├── <Name>Catalog.java
│   └── <Name>CatalogFactory.java
├── config
│   ├── <Name>SourceOptions.java
│   ├── <Name>SourceConfig.java
│   ├── <Name>SinkOptions.java
│   └── <Name>SinkConfig.java
├── client
├── converter
└── internal
```

Only create packages that the connector actually needs. A source-only connector should not add empty sink packages.

## Role rules

### Factory

Factories are the stable discovery entry point. They should:

- expose a stable `factoryIdentifier()`;
- declare and validate connector options;
- construct connector API components;
- avoid opening long-lived network/database resources during discovery.

Factory classes must not schedule tasks or create framework runtime objects.

### Source

A Source is the immutable connector-level description used to create two per-execution roles:

- `SourceSplitEnumerator`: discovers/calculates bounded units of work;
- `SourceReader`: reads already-assigned splits and owns reader-side resources.

New connectors should implement:

```text
Source#createEnumerator(preparedTables, SourceEnumeratorContext)
Source#createReader(preparedTables, batchSize)
```

Use these concepts consistently:

- `SourceSplit`: one independently readable unit of work;
- `SourceSplitEnumerator`: owns split discovery/planning for one framework planning attempt;
- `SourceEnumeratorContext`: supplies framework planning inputs such as source parallelism;
- `SourceReader`: owns the resource used to read assigned splits and is not shared between task threads.

The framework creates and closes the Enumerator through its `SourceCoordinator`. Connector code must not create
framework coordinators, task plans, split queues, or executor services.

#### Legacy `createSplits(...)` compatibility

The `Source#createSplits(...)` overloads are deprecated compatibility hooks. Existing connectors may keep them while they
migrate: the default `Source#createEnumerator(...)` adapts those methods automatically.

New connectors should not implement split discovery only through `createSplits(...)`. Override `createEnumerator(...)`
instead so split lifecycle, classloader scope, and validation flow through the canonical framework path.

JDBC is the reference native implementation: `JdbcSource` creates `JdbcSourceSplitEnumerator`, while its old
`createSplits(...)` methods delegate back through that Enumerator path.

#### Split contract

An Enumerator may return an empty list for an empty bounded source. Non-empty results must contain:

- no null splits;
- a non-blank `splitId`;
- a non-blank `dataSetId`;
- no duplicate `splitId` within the same `dataSetId`.

Keep split objects immutable/serializable and do not attach open database/network resources to them.

### Sink

`SinkWriter` owns data writes. `SinkPreparer` may resolve or prepare target metadata/DDL before task execution. Keep write
batching, retry semantics, serialization, and client calls inside sink-specific roles rather than framework classes.

### Catalog

Catalog code discovers databases/tables/schema metadata and performs catalog operations. It must not be used as a generic
SQL/client utility package.

### Config and options

`*Options` declares external option contracts. `*Config` is the validated/normalized connector configuration used at
runtime. Do not pass an unvalidated map deep into reader/writer code when a typed configuration object is appropriate.

### Client

Client packages wrap protocol/database client interaction. They should not know about framework planners, tasks, or
server DTOs.

### Converter / serializer

Conversion packages translate between Link-Up row/schema types and external-system representations. Keep these
transformations deterministic and independently testable.

### Internal

Use `internal` for connector implementation details that do not form an extension contract. Prefer a precise role package
before using `internal`.

## Packages to avoid

Do not add broad dumping-ground packages such as:

```text
common
core
helper
misc
utils
```

A narrowly named utility tied to one role is acceptable, but a growing generic utility package is a signal that domain
roles need to be extracted. Existing JDBC `core`/`utils` packages are transitional and should be migrated when related
code is substantially changed; do not move them solely for cosmetics.

## ServiceLoader registration

Connector factories are discovered from API types through `ServiceLoader`. The framework's canonical registry is
`com.link.up.framework.connector.FactoryRegistry`; connectors must not reference that class directly.

When adding a factory:

1. implement the API factory contract;
2. register it using the existing ServiceLoader/AutoService convention;
3. add a discovery test that proves the stable identifier is visible;
4. keep identifiers lowercase and stable after release.

## Test expectations

At minimum, a connector should test the responsibilities it owns:

- option/config validation;
- schema/type conversion for supported types;
- Enumerator split planning, including parallelism-sensitive behavior when applicable;
- reader/writer lifecycle and error handling;
- factory discovery/schema export;
- database-specific behavior using a lightweight integration fixture when practical.

Framework Source coordination, task scheduling, and channel behavior belong to framework tests, not connector tests.

## Review checklist

Before merging a connector change, verify:

- no `com.link.up.framework.*` import exists in the connector;
- each new class has one obvious runtime role;
- new Sources implement `createEnumerator(...)` instead of introducing another split-planning entry point;
- Enumerator results follow the split identity contract;
- source and sink resources are owned by enumerator/reader/writer/preparer roles;
- no second factory registry, coordinator, executor, thread pool, or job lifecycle is introduced;
- configuration validation stays close to connector option contracts;
- new packages follow role names rather than generic technical buckets.
