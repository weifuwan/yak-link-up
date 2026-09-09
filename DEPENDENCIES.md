# Dependencies

## 模块方向

允许的主依赖：

```text
launcher -> framework + connectors
server   -> framework + connectors
framework -> api
connectors -> api
```

禁止：

- `api -> framework/server/launcher/connectors`
- `framework -> concrete connector`
- `connector -> framework`
- HTTP/REST 直接依赖 infrastructure 实现
- domain 依赖线程、Future、Executor 或 framework `JobExecution`

## Connector

Connector 通过 `ServiceLoader` / factory contract 被发现。

Connector 包优先使用明确角色：

```text
source
sink
catalog
client
config
converter
internal
```

不要新增 `common`、`helper`、`misc`、`utils` 这类垃圾桶包。

JDBC 历史 `core/converter`、`core/dialect`、`core/split` 暂时保留，但不得新增新的 `core/*` 子域。

### 多 Major Version Connector Family

同一个外部系统存在不兼容的 Major Version SDK 时，可以拆成一个 family common 模块和多个版本叶子模块，例如 Elasticsearch：

```text
elasticsearch-common -> api
elasticsearch7       -> elasticsearch-common + ES7 SDK
elasticsearch8       -> elasticsearch-common + ES8 SDK
```

边界规则：

- family common 模块只放 Link-Up / 产品公共语义，不得依赖任一版本的 vendor SDK。
- 版本 SDK 只能存在于对应的叶子模块，版本叶子之间不得互相依赖。
- 对外 connector identifier 必须体现不兼容的 Major Version，例如 `elasticsearch7` / `elasticsearch8`。
- 如果多个叶子模块依赖同一个 Maven GAV 的不兼容版本，在 classloader / relocation 等隔离机制完成前，禁止同时平铺进 `launcher` / `server` runtime classpath。
- “拆 Maven module”不是 runtime dependency isolation；是否能进入发行包必须单独验证。

## 存储家族 Connector

同一数据源存在多种传输方式（如文件的本地/S3/SFTP）时，允许拆成一个引擎 base 模块加多个存储叶模块：

```text
file-base → api（引擎：格式解析/拆分/Reader 链路 + FileStorage 接口，无厂商 SDK）
file-local/s3/sftp → file-base（各持存储实现、专属配置与 identifier）
```

边界规则：厂商 SDK 只能出现在对应叶模块；叶之间不得互相依赖；每个叶必须有独立 identifier；base 通过工厂注入获得存储实例，不得依赖任何叶。

## 第三方依赖原则

- 能用 JDK 解决的简单问题，不额外引库。
- 依赖版本由根 POM / BOM 统一管理；多 Major Version family 使用独立的版本属性显式 pin。
- Connector 专用 SDK 放在 Connector 模块，不泄漏到 API。
- Server 的 HTTP、JSON、日志依赖不能进入 `link-up-api`。
- 测试依赖使用 test scope。

## 新增依赖前要回答

1. 哪个模块真正需要它？
2. 是否会破坏模块方向？
3. 是否把实现细节暴露到公共 API？
4. 是否已有同类依赖？
5. 能否在单模块内隔离？

答不清楚，就先不要加。
