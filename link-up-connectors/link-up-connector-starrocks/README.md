# Link-Up StarRocks Connector

`link-up-connector-starrocks` 是独立的 StarRocks Connector，不依赖 `link-up-connector-jdbc`。

## Stage 1: Native Source

Native Source 数据链路：

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

## Stage 2: Stream Load Sink

Stage 2 新增 bounded Stream Load Sink：

```text
RecordBatch<FluxRow>
  -> schema-aware JSON / CSV serialization
  -> row / byte threshold buffering
  -> FE PUT /api/{database}/{table}/_stream_load
  -> 307/308 redirect to BE
  -> label-based idempotent retry
  -> Label Already Exists -> GET /api/{database}/get_load_state
```

Stage 2 支持：

- 单目标表 bounded write
- JSON Stream Load（默认）
- CSV Stream Load
- `batch_max_rows` / `batch_max_bytes` 双阈值 flush
- 多 FE 轮转和 307/308 redirect
- 同一批次失败重试复用同一个 label
- `Label Already Exists` 后查询最终状态：`VISIBLE/COMMITTED` 视为成功，只有明确 `ABORTED` 才创建新 label
- `Publish Timeout` 按 StarRocks Stream Load 语义视为已接受成功结果
- `stream_load.params` 透传高级 Stream Load header 参数
- JSON 日期/时间/Decimal/Bytes 的稳定序列化
- CSV schema 顺序 `columns` header

Stage 2 明确不包含：

- JDBC fallback / MySQL Driver
- 自动建库、自动建表或 JDBC Catalog
- 2PC / Job-level exactly once
- CDC / DELETE / RowKind 语义
- 运行时 Schema Evolution
- 多目标表 Sink

Stream Load 每次成功 flush 都已经在 StarRocks 侧形成独立提交，所以 Link-Up `commit()` 不能把它描述成可回滚事务。`abort()` 只能丢弃还没发送的本地 buffer，不能撤销已经成功的 Stream Load。

Connector 在一次 flush 的内部重试会复用相同 label，避免网络超时造成重复导入；但如果整个 Link-Up SinkTask 重新执行，会创建新的 label，因此任务级 Retry 仍应结合目标表主键/去重语义判断。

## Native Source single table example

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

## Native Source multiple tables example

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

## Stream Load Sink example

```hocon
sink {
  StarRocks {
    node_urls = ["fe-1:8030", "fe-2:8030"]
    username = "root"
    password = ""
    database = "warehouse"
    table = "orders"

    load_format = JSON
    batch_max_rows = 5000
    batch_max_bytes = 5242880
    max_retries = 3
    retry_backoff_ms = 1000
    max_retry_backoff_ms = 5000

    stream_load.params = {
      strict_mode = "true"
      timeout = "600"
    }
  }
}
```

CSV 示例：

```hocon
sink {
  StarRocks {
    node_urls = ["127.0.0.1:8030"]
    username = "root"
    database = "warehouse"
    table = "orders"

    load_format = CSV
    column_separator = "|"
    row_delimiter = "\n"
  }
}
```

CSV Stage 2 不会猜测 quoting/enclose 规则。如果字段值包含已配置的列分隔符或行分隔符，Connector 会直接失败并建议改用 JSON，避免生成 StarRocks 解析语义不确定的数据。

## Type boundary

Native Source 的类型边界保持保守。核心标量 StarRocks 类型会映射为 Flux 类型，包括 `BOOLEAN`、整数、`LARGEINT`、`FLOAT`、`DOUBLE`、`DECIMAL`、字符串、`JSON`、`DATE` 和 `DATETIME`。

`LARGEINT` 在 Source 中以精确文本承载，避免完整 signed 128-bit 范围被错误压缩到 `DECIMAL(38,0)`。

Source 的 `ARRAY` / `MAP` 在没有专门 Arrow 转换验证前仍会配置阶段 fail-fast；Sink JSON 序列化可以保留已有 Flux `ARRAY/MAP/ROW` Java 值的 JSON 结构，但这不等于运行时 Schema Evolution 或复杂类型自动发现。
