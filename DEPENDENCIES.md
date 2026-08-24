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

## 第三方依赖原则

- 能用 JDK 解决的简单问题，不额外引库。
- 依赖版本由根 POM / BOM 统一管理。
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
