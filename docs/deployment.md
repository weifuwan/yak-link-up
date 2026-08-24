# 部署与运维指南

Link-Up 有两种本地运行方式：

- `link-up-launcher`：一个进程执行一个离线同步作业，适合 CLI / Kubernetes Job / 外部调度器；
- `link-up-server`：常驻的单节点 Offline Worker，通过 REST 接收、排队、查询和取消作业，并向控制面动态注册。

## 构建与安装

要求 JDK 8+、Maven 3.8.1+。在仓库根目录执行：

```bash
mvn --batch-mode clean verify
```

产物为 `link-up-dist/target/link-up-1.0.0.tar.gz` 和 `.zip`。Launcher 模式示例：

```bash
tar -xzf link-up-dist/target/link-up-1.0.0.tar.gz
cd link-up-1.0.0
cp config/link-up.yaml config/orders-prod.yaml
# Edit URLs, table names, user and password. Do not commit the resulting file.
bin/link-up.sh --config config/orders-prod.yaml
```

配置文件扩展名可为 `.yaml`，但内容是 HOCON，不是 YAML。

## Offline Worker 状态目录

Phase 7 起，Standalone Worker 默认把控制面 checkpoint 写入：

```text
data/worker-state
```

可以用启动参数指定持久目录：

```bash
java ... com.link.up.server.FluxServer \
  --state-dir /var/lib/link-up/worker-state
```

生产环境必须把该目录放在**稳定本地磁盘或持久卷**上。容器重建时如果目录跟着消失，Worker 无法恢复上一实例的幂等索引和非终态 Job checkpoint。

状态文件只包含恢复所需的控制面信息：Job 生命周期、幂等标识、状态转换、错误、run/log 身份和 Attempt 历史。它不持久化 Connector 密码/Token，也不把 Framework Thread、JobExecution 或持续变化的 Metrics 对象序列化到磁盘。

Worker 启动时会扫描状态目录：历史终态继续保留；上一次进程留下的非终态记录会被恢复为 `LOST`，不会自动重新执行。是否重试必须结合 Sink 提交语义与 `retryAdvice` 判断。

建议：

- 目录只允许 Link-Up 运行账户读写；
- 不要由多个 Worker 实例共享同一个 `state-dir`；
- 将状态目录纳入磁盘容量和 inode 监控；
- `--history-limit` 只裁剪终态历史，不会主动删除非终态 checkpoint；
- 升级前备份状态目录；遇到无法识别/损坏的状态文件时 Worker 会 fail-fast，而不是静默丢弃恢复信息。

## 配置与机密

`config/link-up.yaml` 包含 source/sink JDBC 示例、并行度和通道容量。复制后替换 `change-me`，并将配置设为仅运行账户可读，例如：

```bash
chmod 600 config/orders-prod.yaml
```

推荐在 CI/CD 中由 Secret 渲染临时配置文件，或把只读 Secret 挂载到容器；不要把密码放在命令行或提交到 Git。可通过 `LINK_UP_CONF_DIR`、`LINK_UP_LOG_DIR`、`LINK_UP_HOME` 覆盖目录。

## JVM 与日志

默认 JVM 参数建议至少包括：

```text
-Xms256m -Xmx1024m -XX:+ExitOnOutOfMemoryError -Dfile.encoding=UTF-8
```

使用 `LINK_UP_JAVA_OPTS` 追加容器内存比例、GC 或诊断参数，例如：

```bash
LINK_UP_JAVA_OPTS='-XX:+UseG1GC -XX:MaxRAMPercentage=70.0' bin/link-up.sh -c config/orders-prod.yaml
```

Server/Launcher 日志通过 Log4j2 输出。每次 Framework Run 还会绑定独立 `runId` 和 Job 日志文件，控制面可通过 Job Logs API 增量读取。容器运行优先采集 stdout；需要长期日志审计时挂载日志目录或发送到集中日志系统。

## Docker

构建示例：

```bash
mvn -pl link-up-dist -am package
docker build -f deploy/docker/Dockerfile -t link-up:1.0.0 .
```

Launcher 模式：

```bash
docker run --rm \
  -v /secure/orders-prod.yaml:/opt/link-up/config/job.yaml:ro \
  link-up:1.0.0 --config /opt/link-up/config/job.yaml
```

Worker 模式部署时，还应挂载状态与日志目录，例如：

```text
/var/lib/link-up/worker-state  -> Worker --state-dir
/var/log/link-up               -> LINK_UP_LOG_DIR
```

不要让两个同时运行的 Worker 使用同一状态目录。

## 上线与回滚

上线前应执行小表验证、确认目标端写入策略和重复运行行为、设置数据库连接/查询超时，并监控：

- Worker 存活和 `instanceId` 变化；
- `RUNNING / QUEUED / LOST` 数量；
- 状态目录磁盘空间；
- Job Attempt 失败原因和 `retryAdvice`；
- Framework 任务指标与错误日志。

对于非幂等 `APPEND_DATA` 或可能部分提交的任务，不要因为 Worker 重启/LOST 就自动重跑。先确认提交范围或使用 Connector 提供的安全事务语义。

回滚使用上一版本的不可变压缩包/镜像，并保留 Worker 状态目录。若未来持久化格式发生不兼容升级，应按照对应版本的迁移说明处理，不能直接删除状态文件来规避升级问题。

## CI

PR 合并前建议执行：

```bash
mvn --batch-mode clean verify
```

发布流水线还应保存 tar.gz/zip、生成 SHA-512 校验和、对发布工件签名，并在部署前验证校验和。
