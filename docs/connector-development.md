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
│   ├── <Name>SplitEnumerator.java
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

A source describes how readers and splits are created. Keep external I/O in reader/enumerator/client roles rather than in
configuration objects.

Use these concepts consistently:

- `SourceSplit`: one independently readable unit of work;
- `SourceReader`: owns the resource used to read assigned splits and is not shared between task threads;
- `SplitEnumerator`: discovers or calculates splits when the connector needs a dedicated planning role.

The current runtime still supports `Source#createSplits(...)`; do not invent a connector-local execution framework around
that compatibility method.

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
code is substantially changed; Phase 1 does not move them solely for cosmetics.

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
- split planning when applicable;
- reader/writer lifecycle and error handling;
- factory discovery/schema export;
- database-specific behavior using a lightweight integration fixture when practical.

Framework task scheduling and channel behavior belong to framework tests, not connector tests.

## Review checklist

Before merging a connector change, verify:

- no `com.link.up.framework.*` import exists in the connector;
- each new class has one obvious runtime role;
- source and sink resources are owned by reader/writer/preparer roles;
- no second factory registry, executor, thread pool, or job lifecycle is introduced;
- configuration validation stays close to connector option contracts;
- new packages follow role names rather than generic technical buckets.
