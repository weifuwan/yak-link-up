# Link-Up StarRocks Connector

`link-up-connector-starrocks` 是独立的 StarRocks Connector，不依赖 `link-up-connector-jdbc`。

## Stage 1: Native Source

当前实现只包含 bounded Native Source：

```text
StarRocksSource
  -> FE POST /api/{database}/{table}/_query_plan
  -> opaqued query plan + tablet routings
  -> deterministic tablet/BE splits
  -> BE Thrift open_scanner/get_next/close_scanner
  -> Apache Arrow
  -> FluxRow
```

Stage 1 支持：

- 单表和多表 bounded read
- 显式 `schema.fields`
- 列投影
- `scan_filter` 下推
- Tablet Split 和 BE 并行读取
- FE Query Plan failover/retry
- BE Scanner batch / timeout / keep-alive / memory 参数
- 标量类型 Arrow -> FluxRow 转换

Stage 1 不包含：

- JDBC fallback
- Stream Load Sink（Stage 2）
- CDC / streaming
- 运行时 schema evolution
- Native Catalog 自动 Schema 发现
- ARRAY / MAP 的 Arrow 转换

## Single table example

```hocon
source {
  StarRocks {
    node_urls = ["127.0.0.1:8030"]
    username = "root"
    password = ""
    database = "demo"
    table = "orders"

    scan_filter = "id >= 100"
    request_tablet_size = 8
    scan_batch_rows = 4096

    schema {
      fields {
        id = BIGINT
        order_no = STRING
        amount = "DECIMAL(18,2)"
        created_at = DATETIME
      }
    }
  }
}
```

## Multiple tables example

```hocon
source {
  StarRocks {
    node_urls = ["fe-1:8030", "fe-2:8030"]
    username = "root"
    password = ""
    database = "demo"

    table_list = [
      {
        table = "orders"
        scan_filter = "status = 'PAID'"
        schema = {
          fields = {
            id = BIGINT
            amount = "DECIMAL(18,2)"
          }
        }
      },
      {
        table = "customers"
        schema = {
          fields = {
            id = BIGINT
            name = STRING
          }
        }
      }
    ]
  }
}
```

## Type boundary

Stage 1 intentionally keeps the Native Source type boundary conservative. Core scalar StarRocks types are mapped to Flux types, including `BOOLEAN`, integer types, `LARGEINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, string types, `JSON`, `DATE` and `DATETIME`.

`ARRAY` and `MAP` fail during configuration instead of being silently converted to an unsafe representation. They can be added later with dedicated Arrow conversion tests.
