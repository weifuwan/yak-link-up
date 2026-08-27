# Architecture

## 目标

Link-Up 是本地优先的离线数据同步引擎。架构只解决现在需要的问题：清晰 Connector 契约、可测试本地运行时、稳定 Worker 控制面和安全失败处理。

明确边界：**不做实时同步，不做 Flink 式数据 checkpoint/savepoint，不从 Split/offset 自动恢复，不建设通用分布式计算平台。**

## 模块依赖

```text
                  launcher / server
                   /           \
                  v             v
             framework      connectors
                  \             /
                   v           v
                        api
```

硬规则：

- `api` 不依赖 framework/server/launcher/具体 connector。
- `framework` 不依赖具体 connector。
- connector 只实现 `api` 契约，不引用 framework 内部。
- `server` 和 `launcher` 是 composition root。

## Data Plane

```text
JobSpec
  -> JobDefinition
  -> LogicalJobPlan
  -> CapabilityNegotiator
  -> ConnectorPreparer
  -> PreparedJob
  -> JobPlanner
  -> JobGraph
  -> ExecutionGraph
  -> JobExecution
  -> Pipeline / Task
```

`JobGraph` 是不可变物理计划；`ExecutionGraph` 是一次运行状态。Planner 不创建 Reader、线程、Channel、Split Queue 或 CancellationToken。

## Capability Check

Capability 不是规则引擎，只做有限枚举集合检查：

```text
Connector.supported
        vs
Job.required + topology-required
Job.preferred
```

结果：

```text
required missing  -> REJECTED
preferred missing -> DEGRADED + warning
otherwise         -> SATISFIED
```

真实拓扑目前只派生一个强约束：多表 Source 会让 Source/Sink 都要求 `MULTI_TABLE`。该检查发生在 Sink Preparation 之前，避免先产生目标端副作用再失败。

不维护：

```text
observed capability graph
undeclared-observed warnings
capability dependency graph
fallback/cost/priority rules
capability DSL
```

## Plan / Explain

```text
JobDefinition
  -> LogicalJobPlan
  -> CapabilityNegotiator
  -> ConnectorPreparer
  -> JobPlanner
  -> JobGraph
  -> PhysicalJobPlan
  -> JobPlanResult
```

`validate` 不创建 Source/Sink，不访问外部系统。

`explain` 可以创建 Source、发现 Source Schema、枚举 Split 并生成真实 `JobGraph`；不调用 SinkPreparer，不执行目标端 DDL，不创建 Writer，不写数据。

## Structured Error

结构化错误核心字段：

```text
code
category
phase
retryable
retryScope
```

错误可重试不等于数据可重放。最终 Retry 仍必须通过 Commit Evidence。

## Control Plane

```text
HTTP / registration
  -> JobApplicationService
     -> JobExecutionState
     -> JobRuntimeScheduler
     -> JobRepository
     -> JobExecutor
```

状态角色固定为：

```text
JobExecutionState
  = 内存中的唯一可变控制状态

JobSnapshot + JobExecutionMetadata
  = 持久化与查询状态投影

Runtime Event
  = Job/Attempt 生命周期时间线事实
```

REST DTO 是查询投影，不是新的 Job state 模型。不再新增 `JobReplayState`、`JobHistoryState`、`JobCheckpointState` 等平行状态。

## Worker State Persistence

```text
JobRuntimeLifecycle.persistState
  -> JobRepository.save(snapshot, metadata)
  -> FileJobRepository
  -> JobStateFileStore
```

```text
Worker State Persistence
!= Data Checkpoint
!= Resume Point
!= Savepoint
```

- `stateVersion`：业务状态转换版本。
- `stateRevision`：所有需要持久化的控制面变化的单调修订号。
- Worker 重启后非终态 Job 转 `LOST`，不从数据位置继续执行。
- v4 新格式写 `stateRevision`；v1-v3 `checkpointVersion` 只兼容读取。

## Runtime Event Journal

```text
persist Worker state
  -> derive one Job lifecycle fact
  -> JobEventBus
  -> JsonLineJobEventStore
```

Event Journal 的边界固定为：

```text
Job / Attempt lifecycle only
```

不存 Pipeline/Task/Metrics 快照，不增加 Task/Split/Batch 事件，不反向驱动状态机，不参与 Retry/Commit。

Event schema v2 移除了新事件里的 `execution` 快照。读取端仍接受带该字段的 v1 历史 JSONL，并忽略旧字段。

## Job Read APIs

任务详情直接来自当前 read model，不再建立独立 History 聚合层：

```text
/jobs/{id}
/jobs/{id}/pipelines
/jobs/{id}/tasks
/jobs/{id}/metrics
/jobs/{id}/events
/jobs/{id}/logs
```

Worker state 只承诺恢复控制面历史；详细 Pipeline/Task/Metrics 不做耐久化历史服务器。

## Retry

Retry 是新 Attempt，不是复活旧 Attempt：

```text
FAILED Attempt
  -> structured error permits retry
  -> commit evidence == SAFE
  -> next Attempt
```

`LOST` 和 `CANCELED` 默认不重试。

## Connector 结构

推荐角色包：

```text
source / sink / catalog / client / config / converter / internal
```

JDBC `core/{converter,dialect,split}` 是历史兼容区，不继续扩张。

## 一条判断标准

新增设计先问：**它是否直接解决离线同步现在的问题？**

如果答案只是“以后做实时/分布式可能用到”，就不要加。
