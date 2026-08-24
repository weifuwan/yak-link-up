# Link-Up 单节点离线 Worker 执行协议

Link-Up Server 是一个只执行离线批量同步任务的单节点 Worker。Yak Ops 等控制面负责任务定义、调度、执行历史、重试和告警；Link-Up 负责接收一次执行命令、排队、运行、取消、持久化控制面 checkpoint 并返回最终结果。

## Worker 身份

```http
GET /api/v1/node
```

响应包含稳定的 `nodeId`、每次启动变化的 `instanceId`、容量、负载和完整生命周期。控制面必须保存提交时的 `workerInstanceId`。如果同一 `nodeId` 返回新的 `instanceId`，控制面仍应将其视为 Worker 重启事件；Phase 7 起 Worker 自身也会在启动时把本地持久化的非终态 checkpoint 恢复为 `LOST`。

## 状态机

```text
CREATED
   ↓
SUBMITTED
   ↓
QUEUED
   ↓
RUNNING
   ├── SUCCEEDED
   ├── FAILED
   ├── CANCELED
   └── LOST
```

终态不可再次转换。每次状态转换都会增加 `stateVersion`，并记录在 `transitions` 中。取消请求不会引入额外的 `CANCELLING` 业务状态；`cancellationRequested=true` 表示取消意图已被接收，实际状态仍保持 `QUEUED` 或 `RUNNING`，直到进入终态。

## Job 与 Attempt

`jobId` 是一次 Worker 执行资源的稳定身份，`Attempt` 表示该 Job 的一次具体执行尝试。Phase 7 每个新 Job 只创建 Attempt #1，但协议使用数组，为后续安全 Retry 预留模型空间。

```json
{
  "jobId": "flux-1787540000000-1",
  "status": "FAILED",
  "attemptCount": 1,
  "attempts": [
    {
      "attemptNumber": 1,
      "attemptId": "flux-1787540000000-1-attempt-1",
      "status": "FAILED",
      "createTimeMillis": 1787540000000,
      "queuedTimeMillis": 1787540000010,
      "startTimeMillis": 1787540000020,
      "endTimeMillis": 1787540009123,
      "runId": "MYSQL_MYSQL_-1787540000030",
      "jobLogFile": "...",
      "failureType": "SQLException",
      "failureMessage": "...",
      "retryAdvice": "verify already committed data before retrying"
    }
  ]
}
```

Attempt 状态为 `CREATED / QUEUED / RUNNING / SUCCEEDED / FAILED / CANCELED / LOST`。`retryAdvice` 是证据，不是自动重试指令；控制面不能仅凭 `FAILED` 或 `LOST` 自动重跑。

## 结构化 JSON 提交协议

```http
POST /api/v1/jobs
Content-Type: application/json
```

```json
{
  "externalExecutionId": "yak-execution-10086",
  "idempotencyKey": "60af452d-813c-4e51-87d9-4b00b5e2b53f",
  "definitionVersion": 3,
  "jobSpec": {
    "apiVersion": "link-up/v1",
    "kind": "BatchSyncJob",
    "name": "orders-sync",
    "source": {
      "connectorId": "jdbc",
      "options": {
        "url": "jdbc:mysql://source:3306/demo",
        "table_path": "orders"
      }
    },
    "sink": {
      "connectorId": "jdbc",
      "options": {
        "url": "jdbc:mysql://sink:3306/demo",
        "table_path": "orders_copy"
      }
    },
    "runtime": {
      "batchSize": 1000,
      "sourceParallelism": 1,
      "sinkParallelism": 1,
      "pipelineParallelism": 1,
      "maxBufferedBatches": 64
    }
  }
}
```

Worker 会对规范化后的 `jobSpec` 计算配置摘要。相同 `externalExecutionId`、`idempotencyKey`、`definitionVersion` 和配置摘要的重复请求返回同一个 `jobId`；复用相同标识但提交不同内容时返回 HTTP `409` 和错误码 `FLUX-JOB-IDEMPOTENCY-CONFLICT`。

Phase 7 起幂等索引从本地 checkpoint 恢复，因此 Worker 正常重启后，同一幂等请求仍返回原 `jobId`，不会因为进程重启而静默创建第二个执行实例。

JSON 请求必须且只能包含 `jobSpec` 或 `hocon` 其中一个。`hocon`、`application/hocon` 和 `text/plain` 仍保留给 CLI 与迁移场景，控制面必须使用结构化 `jobSpec`。完整字段说明见 `docs/job-spec.md`。

提交入口只记录 `externalExecutionId`、定义版本、任务名称和 Connector 类型等摘要，不记录完整 JobSpec、用户名、密码、Token 或其他 Connector options。

## Worker checkpoint 与重启恢复

Standalone Worker 默认把控制面 checkpoint 写入：

```text
data/worker-state
```

可以通过启动参数覆盖：

```bash
--state-dir /var/lib/link-up/worker-state
```

checkpoint 在 `SUBMITTED / QUEUED / RUNNING / 日志身份绑定 / 取消请求 / 终态` 等关键生命周期节点 upsert。每个 Job 使用一个带 `formatVersion` 的 JSON 文件；写入采用临时文件 + flush/fsync + 原子替换（文件系统支持时）。

启动时：

1. 读取本地 checkpoint；
2. 恢复 `externalExecutionId` 和 `idempotencyKey` 索引；
3. 终态记录保持原终态；
4. 任何非终态记录追加 `worker-restart-recovery` 转换并进入 `LOST`；
5. 当前非终态 Attempt 同步进入 `LOST`，错误类型为 `WorkerRestartRecovery`；
6. **不会自动重新执行**。

持久化范围是控制面恢复状态。Phase 7 不持久化上一个进程里的完整 Pipeline/Task/Channel metrics 或 Table-DDL 详情；这些详细视图在同一进程内仍完整，重启后的历史记录保留生命周期、错误、幂等信息和 Attempt 历史。

## 查询与取消

```http
GET /api/v1/jobs/{jobId}
GET /api/v1/jobs/external/{externalExecutionId}
GET /api/v1/jobs?externalExecutionId={externalExecutionId}
GET /api/v1/jobs?status=RUNNING&page=1&pageSize=20
GET /api/v1/jobs/{jobId}/pipelines
GET /api/v1/jobs/{jobId}/tasks
GET /api/v1/jobs/{jobId}/metrics
GET /api/v1/jobs/{jobId}/logs?cursor=0&limit=500
DELETE /api/v1/jobs/{jobId}
```

提交请求超时后，控制面应优先使用 `externalExecutionId` 查询，不能直接生成新执行实例重复提交。

## 增量运行日志

每个 Framework Run 生成一个文件系统安全的 `runId` 和独立日志文件。Connector 准备、自动建表、Split 规划、Pipeline 和 Task 执行共享同一日志上下文。

日志接口使用不透明的字节偏移游标：

```json
{
  "jobId": "flux-1785977590967-1",
  "externalExecutionId": "yak-offline-21d1f420-c9e7-4d3b-a5c7-87e52899958c",
  "runId": "MYSQL_MYSQL_-1785977593842",
  "items": [],
  "nextCursor": 226,
  "completed": false
}
```

- 首次读取使用 `cursor=0`；后续请求原样传回 `nextCursor`。
- `limit` 范围为 1～1000，按日志事件计数；多行 SQL 和异常堆栈会归入同一条事件。
- `completed=true` 表示 Job 已进入终态，并且游标已经读取到当前日志文件末尾。
- 接口只允许读取 Worker 自己记录的文件名，不接受任意文件路径。
- Job 日志仍受 Log4j RollingFile 的保留和归档策略约束；控制面需要长期审计时，应在执行完成后主动归档或采集。

## 离线建表结果

JDBC Sink 在准备每个数据集时会生成最终目标表的 `CREATE TABLE` 语句。单表任务包含一个 Pipeline，多表任务按数据集包含多个 Pipeline；两种模式使用相同结构。`tableDdl` 只描述离线任务准备阶段生成的建表语句，不表示 CDC 或实时 Schema Change。

## LOST 处理

`LOST` 表示最终结果未知，不等价于失败或取消：

1. Worker 优雅关闭时先请求取消。
2. 在关闭超时内完成的任务进入实际终态。
3. 关闭超时后仍未完成的任务进入 `LOST`。
4. 进程异常退出后，下一次启动会根据本地 checkpoint 把原非终态任务恢复为 `LOST`；控制面仍可用 `workerInstanceId` 变化作为交叉校验。
5. `LOST` 不会由 Worker 自动重跑。
6. 是否可以创建新的 Attempt/执行实例，必须结合 Sink 提交语义、CommitSummary 和 `retryAdvice` 判断。

## 控制面轮询建议

- `SUBMITTED`、`QUEUED`：每 2～3 秒查询。
- `RUNNING`：每 3～5 秒查询，长任务可退避到 15～30 秒。
- 运行日志使用 `nextCursor` 增量读取，不要每次从 0 重放完整文件。
- 终态：完成最后一次日志读取后停止轮询。
- 更新前比较 `stateVersion`，只接受更高版本的状态。
