# ADR-0005: Standardize connector package roles

- Status: Accepted
- Date: 2026-08-24

## Context

Phases 1-4 established framework role vocabulary, physical/runtime boundaries, runtime coordination roles, and the
canonical Source Enumerator path. Connector implementations still expressed the same responsibilities with different
package languages:

- HTTP used `parser` for response-to-row conversion;
- Doris used `serializer` for row-to-payload conversion;
- JDBC kept catalog/preflight helpers in a generic `utils` package;
- JDBC also has a historical `core` wrapper around three already-specific subdomains: `converter`, `dialect`, and `split`.

These names do not break runtime behavior, but they make connector structure drift over time and encourage new catch-all
packages instead of role-owned code.

## Decision

Connector package structure is standardized around explicit roles. A connector creates only the roles it needs, using
these names where applicable:

```text
com.link.up.connector.<name>
├── source
├── sink
├── catalog
├── client
├── config
├── converter
├── internal
└── <connector-specific role>
```

Connector-specific packages such as JDBC `dialect` or `split` are valid because they name a real domain role. Generic
root packages such as `common`, `core`, `helper`, `misc`, and `utils` are not valid destinations for new code.

### Conversion role

`converter` is the canonical package for deterministic translation between Link-Up rows/schemas and external
representations. A class may still use a precise class name such as `HttpResponseParser` or `DorisRowSerializer`; the
package communicates the architectural role while the class name communicates the concrete operation.

Phase 5 therefore moves:

```text
http/parser/HttpResponseParser
    -> http/converter/HttpResponseParser

doris/serializer/DorisRowSerializer
    -> doris/converter/DorisRowSerializer
```

### JDBC utility cleanup

The two JDBC `utils` classes already have clear ownership and are moved without changing their algorithms:

```text
jdbc/utils/JdbcCatalogUtils
    -> jdbc/catalog/JdbcCatalogUtils

jdbc/utils/JdbcConnectionPreflight
    -> jdbc/client/JdbcConnectionPreflight
```

The preflight test moves with the client role.

### JDBC legacy `core`

Moving the complete JDBC `core` subtree in one architecture PR would touch more than twenty mature converter, dialect,
and split-planning classes plus cross-package imports and tests. That would turn a package-standardization change into a
large regression surface with little immediate behavioral value.

Therefore Phase 5 freezes JDBC `core` as an explicit legacy allowlist. Its only permitted direct subdomains are:

```text
core/converter
core/dialect
core/split
```

No fourth `core` subdomain may be added. New JDBC code must use a top-level role package. Existing classes move out of
`core` incrementally when their domain is materially changed and can be verified in a focused PR.

This is a migration constraint, not an endorsement of `core` as a permanent package.

### HTTP Source completion

HTTP is migrated from the Phase 4 compatibility adapter to a native `HttpSourceSplitEnumerator`. Pagination remains
reader-owned, so the Enumerator returns one logical split exactly as the legacy `createSplits()` implementation did.
The legacy method remains only as a compatibility bridge.

### Architecture guards

Each built-in connector receives package-boundary tests:

- HTTP and Doris fail if generic/obsolete root packages are reintroduced;
- JDBC fails if `utils/common/helper/misc` reappear;
- JDBC fails if the legacy `core` directory gains any direct subdomain other than `converter`, `dialect`, or `split`;
- role-sensitive classes are asserted to live in their canonical packages;
- HTTP is asserted to implement the native Enumerator extension point.

## Consequences

Positive:

- built-in connectors use the same package vocabulary;
- converter/client/catalog ownership is visible from the directory tree;
- generic utility packages stop accumulating unrelated responsibilities;
- HTTP no longer depends on the deprecated split-discovery adapter;
- future connector reviews can enforce package roles mechanically;
- JDBC can be migrated safely without a high-risk all-at-once move.

Trade-offs:

- JDBC temporarily retains `core/converter`, `core/dialect`, and `core/split`;
- class names such as `Parser`, `Serializer`, or `Utils` are not automatically forbidden when the class has a precise
  role; the package boundary is the primary architectural signal;
- this phase intentionally does not rewrite connector algorithms or merge distinct connector-specific roles.

## Compatibility

This decision does not change:

- Connector identifiers or ServiceLoader registration;
- connector option/schema contracts;
- Source/Sink execution semantics;
- JDBC split planning, dialect, catalog, preflight, or write behavior;
- HTTP response parsing or pagination behavior;
- Doris Stream Load serialization or commit behavior.

All production changes in this phase are package/import moves or the HTTP legacy-to-native Enumerator bridge.

## Follow-up

When JDBC converter/dialect/split code is materially modified, move the touched domain from `core` to a top-level
role package in a focused change. Once the last subdomain moves, delete `jdbc/core` and remove its temporary allowlist
from the architecture guard.
