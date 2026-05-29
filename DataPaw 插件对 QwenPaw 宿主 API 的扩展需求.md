| 序号 | TODO | 责任方 | 责任人 | 依赖 | 预期完成时间 |
| --- | --- | --- | --- | --- | --- |
| 1 | 实现插件侧 `DAGStore` / `DAGBroadcaster` / `GET /api/tasks/{sid}/dag/events`，并把 DAG backing store 从 session JSON 切到 `dag.json` | plugin | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 无 | 05-27 |
| 2 | 给 host SSE event / message schema 增加 `metadata` 字段，并保证 serializer 原样输出 | host | [@阿瑞](https://aliyuque.antfin.com/weirui.kwr) | 无 | 06-03 |
| 3 | 提供 outbound metadata hook，让插件在 `Msg` 上写 `graph_id` / `node_id` 等路由信息 | host | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 无 | 06-05 |
| 4 | DataPaw 注册 outbound metadata writer，并删除对 `ConsoleChannel.stream_one` 的 frame rewrite | plugin |  | 2, 3 |  |
| 5 | 提供构造期 hook，让插件把 host `PlanNotebook` 替换为 `RuntimeStateManager` | host |  | 无 |  |
| 6 | 提供 prompt section hook，让插件插入 `MASTER.md`、mode prompt、planner 规则和环境提示 | host | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 无 | 06-05 |
| 7 | DataPaw 注册 prompt sections，并删除 `_build_sys_prompt` override | plugin | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 7 | 06-05 |
| 8 | 提供 `register_uninstall_hook`，替换 `PluginLoader.unload_plugin` monkey-patch | host | [@阿瑞](https://aliyuque.antfin.com/weirui.kwr) | 无 | 06-03 |
| 9 | DataPaw 注册 uninstall cleanup hook，并删除 `PluginLoader.unload_plugin` monkey-patch | plugin | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 9 | 06-05 |
| 10 | 修复 plugin validator 的相对 import / `submodule_search_locations` | host | [@阿瑞](https://aliyuque.antfin.com/weirui.kwr) | 无 | 06-03 |
| 11 | DataPaw 改回标准相对 import，并删除 `sys.path.insert` / 双路径 import workaround | plugin | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 11 | 06-05 |
| 12 | 抽出 `PlanModeFSM`，让 `RuntimeStateManager` 直接使用它，避免复制 `_plan_*` 私有 flag | host |  | 无 |  |
| 13 | DataPaw 使用 `PlanModeFSM`，删除 `_plan_*` 私有 flag 复制逻辑 | plugin |  | 13 |  |
| 14 | 提供 ReAct lifecycle hook，让插件在 reason / act / summarize 后 append trace，删除对应 method override | host | [@辰观](https://aliyuque.antfin.com/qianbingchen.qbc) | 无 | 06-03 |
| 15 | DataPaw 注册 trace append callback，并删除 `_reasoning` / `_acting` / `_summarizing` overrides | plugin | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 15 | 06-05 |
| 16 | 对齐现有 skill system 是否已满足 typed `register_skill_provider` | host | [@阿瑞](https://aliyuque.antfin.com/weirui.kwr) | 无 | 06-03 |
| 17 | 如果 host 暴露 typed skill provider，DataPaw 改用该 API，并删除 copytree / manifest patch / mtime cache 逻辑 | plugin | [@逸男](https://aliyuque.antfin.com/meiyinan.myn) | 17 | 06-05 |
| 18 | sub-agent 方案整体挂起；不先落 `register_reply_signal` 这类半套 hook | host/plugin |  | 无 | 挂起 |
| 19 | sandbox provider 方向等 host / Eric 决策，插件侧保持 frozen | host/plugin |  | 无 | 挂起 |
| 20 | heredoc handling 作为 host shell tool bug 单独提 issue | host | [@辰观](https://aliyuque.antfin.com/qianbingchen.qbc) | 无 | 06-03 |


---

## 1. Agent capability customization hooks
> _Reviewer (2026-05-26):_ §1 的主线不应是鼓励插件注册 / 替换整个 agent class，而应先把 DataPaw 对 agent 的定制拆成一组 host hooks。`register_agent_class` 这类 class registry 本轮不提，先看 hooks 能不能覆盖。
>

### 场景
DataPaw 今天看起来需要一个 `DataPawAgent` class，但它真正想定制的不是「agent 这个类型」本身，而是一组横切能力：

+ prompt 组成：插入 DataPaw 的 `MASTER.md`、mode prompt、planner 规则和环境提示。
+ runtime state：挂载 `RuntimeStateManager`，并让它参与持久化 / prompt hint / REST 读写。
+ lifecycle：在 ReAct reason / act / summarize 后追加 per-node trace。
+ outbound metadata：给 `Msg` 写入 `graph_id` / `node_id`。
+ sandbox：按 session 懒启动容器，并把 sandbox 视角路径暴露给工具。
+ mode：按 session 级 `agent` / `plan` 模式重建工具集。

这些都更适合通过 host 公开 hook / registry 来表达。直接注册一个完整 agent class 只是把所有定制点重新塞进继承树里，会让插件继续承担 host agent 内部演进的兼容成本。

### 当前 plugin 实现
插件在启动期改写 `runner` 模块顶层的 `QwenPawAgent` 名字，让原本构造宿主 agent 的代码间接调用 smart factory，最终创建 `DataPawAgent`：

```python
factory = _SmartAgentFactory(_runner_module.QwenPawAgent)
_runner_module.QwenPawAgent = factory
```

这个实现依赖三个隐式条件：

+ runner 必须以顶层名字引用 `QwenPawAgent`。
+ 没有第二个插件改写同一个名字。
+ host 不重构 import / 构造路径。

这样做的问题不是「缺少一个更正式的 class registry」这么简单，而是 DataPaw 为了拿到多个细粒度扩展点，被迫替换了整个 agent class。结果是：

+ prompt、state、lifecycle、metadata 等互不相同的需求都被捆到一个 subclass 上。
+ host 只要改 `QwenPawAgent` 构造参数、私有属性或 ReAct 方法形状，插件就可能需要跟着适配。
+ 多个插件如果都想定制 agent 能力，只能互斥地替换 class，而不是组合多个 hook。

### 期望 host API
把 DataPaw 现在塞进 `DataPawAgent` 的定制拆到已有或更具体的 hooks。工具注册不列在这里：host 已有 `register_tool`，DataPaw 的 plan tools、sandbox tools、DataPaw tools 继续走既有 tool registry。`RuntimeStateManager` 挂载也不需要独立 hook：host 提供 §2 的构造期 hook 后，plugin 在该 hook 里完成 notebook replacement、hint / restore / pending edits 等运行态接入。

```python
def _on_startup(api):
    api.register_request_hook("after_agent_construct", install_runtime_state)
    api.register_prompt_section(...)
    api.register_message_metadata_enricher(...)
    api.register_react_lifecycle_hook(...)
```

具体能力对应本文后续章节的扩展点 / 改造项：

| 能力 | 对应扩展点 / 改造项 |
| --- | --- |
| 构造前后接入 | §2 `register_request_hook` |
| plan-mode 状态机 | §6 `PlanModeFSM` |
| prompt 拼装 | §7 `register_prompt_section` |
| ReAct phase 观察 | §8 `register_react_lifecycle_hook` |
| outbound metadata | §9 `register_message_metadata_enricher` |
| sub-agent hand-off | §C 整体挂起，本轮不拆半套 hook |


本轮不提 `register_agent_class` 这类 class registry：先验证 hooks 是否足够覆盖 DataPaw 的定制点；只有出现 hooks 显然无法表达的 agent 行为差异时，再单独讨论 class registry。

### 落地考虑
+ 优先把 DataPaw 现在 `DataPawAgent` 里的 override / post-init 装配拆到 typed hooks。
+ 目标是让默认 `QwenPawAgent` 通过 hooks 被组合出 DataPaw 行为，而不是把 `DataPawAgent` 作为第二套 agent 基类长期存在。

---

## 2. PlanNotebook replacement hook
> _Reviewer (2026-05-26):_ §2 的主要场景不是给 §3 SSE queue 找 request，而是在 agent 构造边界把 host 默认 `PlanNotebook` 替换 / 升级为 DataPaw 的 `RuntimeStateManager`。request lifecycle hook 可以承载这个需求，但文档主语应落在 notebook replacement 上。
>

### 场景
host runner 默认会构造或传入一个 `PlanNotebook`，用于 `/plan` 命令、plan-mode flag 和基础 plan 状态。DataPaw 需要把这个 notebook 替换为 `RuntimeStateManager`，因为 DataPaw 的任务状态不是线性 plan，而是带节点、边、artifacts、trace、历史图的 `TaskGraph`。

这个替换必须发生在稳定的构造期边界，并且要保留 host 已经挂在原 `PlanNotebook` 上的语义：

+ `/plan` 命令使用的 plan-mode 状态（详见 §6 `PlanModeFSM`）。
+ runner / session 保存 hook 需要的上下文。
+ host 传给 agent 的 plan 工具 gate 与自动继续控制。
+ DataPaw 自己的 DAG state、pending edits、artifacts 和 trace。

因此 §2 需要的不是任意时刻拿 `request` 的能力，而是一个明确的「agent / notebook 构造期替换点」。

### 当前 plugin 实现
插件包装 `AgentRunner.query_handler`，用 `contextvars` 暂存当前 `request` 和 `runner`，再让 smart factory / agent post-init 在构造过程中回读这些上下文，创建并接入 `RuntimeStateManager`：

```python
AgentRunner.query_handler = _wrap_query_handler(
    AgentRunner.query_handler,
)


runtime_state = RuntimeStateManager(...)
runtime_state.plan_mode = host_plan_notebook.plan_mode
agent.plan_notebook = runtime_state
```

这条链路的问题是：

+ notebook 替换依赖 smart factory 和 contextvars 的隐式组合。
+ host 不知道自己的 `PlanNotebook` 被插件换成了另一个对象。
+ §6 中的 `_plan_*` 私有 flag 只能靠复制 / 迁移维持。
+ runner 保存 session、恢复 state、注入 hint 的时机都依赖插件对 host 内部构造顺序的理解。

### 期望 host API
沿用通用 request lifecycle hook，但把 notebook replacement 作为明确用例：

```python
api.register_request_hook("before_agent_construct", cb)
api.register_request_hook("after_agent_construct", cb)
```

回调签名按阶段区分：

```python
async def before_agent_construct(request, runner, agent_config): ...
async def after_agent_construct(agent, request, runner): ...
```

`after_agent_construct` 里允许插件替换 `agent.plan_notebook`，host 之后所有引用都以替换后的 notebook 为准。

### 协作示意
使用通用 hook 时：

```python
def _on_startup(api):
    api.register_request_hook(
        "after_agent_construct",
        _datapaw_install_runtime_state,
    )


async def _datapaw_install_runtime_state(agent, request, runner):
    if agent.agent_id != BUILTIN_DATAPAW_AGENT_ID:
        return
    host_notebook = agent.plan_notebook
    runtime_state = RuntimeStateManager.from_host_notebook(
        host_notebook,
        session_id=runner.session_id,
        workspace_dir=runner.workspace_dir,
    )
    agent.plan_notebook = runtime_state
```

### 落地考虑
旧文档里 §2 容易被理解为服务 §3 的 request-bound SSE queue。新方案下 §3 sub-B 已改成独立 DAG SSE endpoint，`DAGBroadcaster` 是 plugin-internal server singleton，不需要从 chat request 里拿 queue。

§2 的真正依赖关系是 §6：如果 host 抽出了 `PlanModeFSM`，`RuntimeStateManager` 就能共享同一个 FSM 引用，而不是复制 `_plan_*` 私有 flag。两节可以独立讨论，但落地时最好一起看，避免 notebook 替换 API 继续携带私有 flag 迁移逻辑。

结论：

+ §2 是 DataPaw 从 subclass / smart factory 迁出时的关键构造期 hook。
+ §2 不再是 §3 DAG SSE 的前置条件。
+ 当前不额外要求 notebook factory 专门 API，避免一次性扩展太多 host surface。

---

## 3. SSE / DAG stream split
> _Reviewer (2026-05-26):_ 同意 DAG 流和 Chat 流需要拆开。长连接是必要的，但双向通信不需要；协议层选 SSE。Chat 流只需要保留 metadata，让前端能把消息路由到 DAG 节点。
>

### 场景
DataPaw 前端需要同时看到两类信息：

1. Chat / token 流：agent 的自然语言输出、tool call、content delta。
2. DAG 状态流：节点开始、完成、失败、图归档、图替换等 TaskGraph snapshot。

旧方案把 DAG 事件插入 `/console/chat` 的 SSE 流，同时在 channel 层改写每帧 metadata。这样会让 Chat wire 承担两个职责，也把插件绑在 `ConsoleChannel.stream_one` 的具体实现上。

新方案把它拆成两条流：

+ Chat 流：仍走 host 现有 `/console/chat`，但 event / message 上需要有 `metadata` 字段。
+ DAG 流：插件自己通过 `GET /api/tasks/{sid}/dag/events` 提供独立 SSE endpoint，每次推 full `TaskGraph` snapshot。

### 与 host 现有 `/plan/stream` 的关系
host 已经有 `/plan/stream` SSE，但它和 DataPaw 的 DAG 流不是同一个抽象。

| 维度 | host `/plan/stream` | DataPaw `/api/tasks/{sid}/dag/events` |
| --- | --- | --- |
| 数据结构 | `agentscope.plan.Plan` 线性计划 | `TaskGraph` DAG |
| 订阅维度 | `agent_id` | `session_id` |
| 缓存模型 | `_live_plan_cache` 内存缓存 | `dag.json` backing store + 当前 snapshot |
| payload | plan update | full DAG snapshot |
| sequence | 无明确 `sequence_number` 语义 | SSE `id:` 使用 `sequence_number` |


因此新方案是并存，不是迁移或替代。底层 broadcast 模式可以复用 host 的设计思路：`{key -> set[Queue]}`、`put_nowait`、queue 满时丢弃慢连接，不阻塞主流程。

### Sub-API A：host 给 `Event` 增加 `metadata`
#### 当前问题
`agentscope_runtime.engine.schemas.agent_schemas.Event` 实际字段是：

```latex
sequence_number / object / status / error
```

没有 `metadata`。这导致 DataPaw 想把 `graph_id` / `node_id` 附在 chat event 上时，只能在 plugin channel wrapper 里 post-process SSE 字节。

#### 期望 host API
在 host / upstream event schema 上增加：

```python
metadata: dict[str, Any] = Field(default_factory=dict)
```

host SSE serializer 已经使用 `model_dump_json()` 输出事件。字段存在后，serializer 不需要知道 DataPaw，metadata 会自然进入 SSE payload。

`metadata` 字段本身只是承载位置；字段值由 §9 的 outbound metadata hook 写入。host 在出站 `Msg` 被打印 / 转成 SSE event 之前调用插件注册的 enricher，把返回的 dict merge 到 `msg.metadata`：

#### 协作示意
```python
for enricher in api.message_metadata_enrichers_for(msg, ctx):
    msg.metadata.update(enricher(msg, ctx))

event = Event(
    object="message",
    metadata=msg.metadata,
    ...
)
```

前端收到 chat event 后直接读：

```typescript
const graphId = event.metadata?.graph_id
const nodeId = event.metadata?.node_id
```

### Sub-API B：插件侧独立 DAG SSE endpoint
#### 核心组件
插件内部新增三个组件：

+ `DAGStore`：`dag.json` 的唯一读写入口。
+ `DAGBroadcaster`：进程内连接表 `{sid -> set[asyncio.Queue]}`。
+ `GET /api/tasks/{sid}/dag/events`：SSE endpoint，每条连接一个 queue。

核心关系放在同一个 sketch 里看：

```python
class DAGBroadcaster:
    _queues_by_sid: dict[str, set[asyncio.Queue]] = {}

    @classmethod
    def subscribe(cls, sid: str) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=32)
        cls._queues_by_sid.setdefault(sid, set()).add(queue)
        return queue

    @classmethod
    def unsubscribe(cls, sid: str, queue: asyncio.Queue) -> None:
        cls._queues_by_sid.get(sid, set()).discard(queue)

    @classmethod
    def push(cls, sid: str, snapshot: dict) -> None:
        for queue in cls._queues_by_sid.get(sid, set()):
            try:
                queue.put_nowait(snapshot)
            except asyncio.QueueFull:
                pass  # 慢连接丢弃本次 snapshot，下次 full snapshot 会覆盖


class DAGStore:
    _on_write: list[Callable[[str, dict], None]] = []

    @classmethod
    def on_write(cls, callback: Callable[[str, dict], None]) -> None:
        cls._on_write.append(callback)

    @classmethod
    async def read(cls, sid: str) -> dict | None:
        return await read_json_or_none(dag_json_path(sid))

    @classmethod
    async def write(cls, sid: str, snapshot: dict) -> None:
        for callback in cls._on_write:
            callback(sid, snapshot)  # fan-out 到 queue，约微秒级
        await write_json_atomic(dag_json_path(sid), snapshot)  # 落盘，约毫秒级


DAGStore.on_write(DAGBroadcaster.push)
```

`RuntimeStateManager._notify_graph_change(event_type)` 是所有图变更入口的统一收口：`create_plan` / `update_subtask_state` / `finish_subtask` / `revise_current_plan` / `load_graph` / `finish_plan` 等在改完内存里的 `TaskGraph` 后都会调用它。它不再写 session JSON / request queue，只把当前 snapshot 交给 `DAGStore.write`。涉及 plan 切换或归档的入口（如 `create_plan`、`finish_plan`）会顺带更新 `plan_mode`（§6），普通节点状态变更通常只调 `_notify_graph_change`。

示例（`update_subtask_state`）：

```python
class RuntimeStateManager:
    async def update_subtask_state(self, node_id: str, state: str) -> ToolResponse:
        node = self.current_plan.nodes[node_id]
        node.state = state
        self.current_plan.refresh_state()
        await self._notify_graph_change(TaskEventType.GRAPH_UPDATED)
        return _text(f"Node '{node_id}' marked as '{state}'.")

    async def _notify_graph_change(self, event_type: str) -> None:
        snapshot = self.current_plan_snapshot(
            event_type=event_type,
            sequence_number=self.next_sequence_number(),
        )
        await DAGStore.write(self.session_id, snapshot)
```

`PlanModeFSM` 管下一轮 agent 行为（确认、续跑、tool gate）；`_notify_graph_change` 管 snapshot 持久化与 DAG SSE。两者可在同一 mutation 里相邻调用，职责不合并。

#### `DAGStore.write` 顺序
`DAGStore.write` 采用「先 fan-out，后落盘」。顺序已经体现在上面的类级 sketch 中：`write()` 先逐个调用 `_on_write` callback，再 `write_json_atomic(...)`。

这意味着前端可能比落盘早几毫秒看到更新。这个窗口可接受：

+ 新连接 / 重连会走 `DAGStore.read()` 读取持久化最新态。
+ 下一次结构变更会再次写完整 snapshot。
+ 不为了几毫秒一致性引入额外复杂度。

#### SSE endpoint
```python
@router.get("/api/tasks/{sid}/dag/events")
async def stream_dag_events(sid: str):
    queue = DAGBroadcaster.subscribe(sid)

    async def gen():
        try:
            snapshot = await DAGStore.read(sid)
            if snapshot is not None:
                yield format_sse(snapshot)
            while True:
                snapshot = await queue.get()
                yield format_sse(snapshot)
        finally:
            DAGBroadcaster.unsubscribe(sid, queue)

    return StreamingResponse(gen(), media_type="text/event-stream")
```

`GET/PUT /api/tasks/{sid}/dag` 是已有 REST 端点，不属于本节新增 API；但它们的 backing store 应随迁移一起切到 `dag.json`。

### 持久化迁移
当前 DataPaw DAG 状态存放在 session JSON 的 `agent.runtime_state`。新方案改为：

```latex
sessions/<sid>/dag.json
```

迁移规则：

1. session 加载时检查旧 session JSON 中是否存在 `agent.runtime_state`。
2. 若存在且 `dag.json` 不存在，写出 `dag.json`。
3. 从 session JSON 清空旧 `runtime_state` 字段，避免两份状态分叉。
4. 后续 session load 由 `DAGStore.read()` 旁路恢复 DAG 状态。

`RuntimeStateManager` 不再作为 host `StateModule._module_dict` 自动跟踪对象；它仍然是 DataPaw plan tools 的 runtime state holder，但持久化边界转移到 `DAGStore`。

### 为什么不用 file watcher
reviewer 对「agent 只负责 persist，通知由别的组件管」的单一职责判断是对的；但 file watcher 不是当前最合适的实现。

| 方案 | 好处 | 问题 |
| --- | --- | --- |
| file watcher | 进程外写文件也能被发现 | macOS + Docker volume 事件不稳定；atomic write 可能触发多次；调试链路长；同进程 REST PUT 容易重复推 |
| `DAGStore.write -> on_write` | 同进程语义清晰；无额外依赖；push 与 persist 在同一个写入口 | 只能覆盖同进程写入 |


DataPaw 当前 REST / agent / SSE 都在同一进程内。只要规定所有写入都走 `DAGStore.write`，in-process write-hook 就能达到等价分工。未来若真的出现跨进程写 `dag.json`，再升级 file watcher。

### “消息领先 DAG” 的真实原因
用户看到「消息说正在执行节点 N，但 DAG 还显示 idle」时，主因不是 backend 两条 SSE 流抢发，而是 ReAct 固有节奏：

1. agent 先输出 reasoning 文本，例如「我开始执行节点 N」。
2. agent 随后调用 `update_subtask_state(N, "in_progress")`。
3. DAG 状态变更才触发 `DAGStore.write` 和 DAG SSE。

reasoning 帧物理上就早于 plan update。backend cross-stream race 也可能存在，但通常是小于 10ms 的量级，相比 ReAct narrative-before-action 的差距可忽略。

解法不是前端 buffer。那条消息本身就应该立即展示。正确做法是 chat 帧带：

```json
{
  "metadata": {
    "graph_id": "...",
    "node_id": "..."
  }
}
```

前端拿到 content 帧后可以：

+ 把消息渲染进对应 node drawer。
+ 或者把节点标成「即将开始」占位态。

`sequence_number` 的真正用途是写到 SSE `id:` 字段，让 `EventSource` 重连时通过 `Last-Event-ID` 回传。它解决的是重连续传和丢包检测，不是同流内排序；同一条 SSE 流天然有序。

### 落地考虑
本节拆成两个 landing unit：

+ **§3 sub-A**：host 增 `Event.metadata`，让 Chat 流无需 plugin frame transformer。
+ **§3 sub-B**：插件侧完成 `DAGStore`、`DAGBroadcaster`、`dag/events` endpoint 和 `dag.json` 迁移。它不需要 host 新 API，复用现有 `register_http_router` 即可。

---

## 4. Plugin uninstall hook
> _Reviewer (2026-05-26):_ 当前单插件场景下 monkey-patch 尚可工作；但卸载生命周期是 host plugin system 的自然扩展点，多插件时代不应让插件互相包同一个 `unload_plugin`。
>

### 场景
DataPaw 插件被卸载时需要清理它创建的宿主侧工件：

+ agent profile。
+ `agent.json`。
+ workspace / skills 相关目录。

如果应用进程不重启而只卸载插件，下一个用户不应看到指向已不存在插件的残留 agent。

### 当前 plugin 实现
插件改写 `PluginLoader.unload_plugin`，在 host 自己的卸载逻辑前先调用 DataPaw cleanup：

```python
PluginLoader.unload_plugin = _wrap_unload_plugin(
    PluginLoader.unload_plugin,
)
```

### 期望 host API
注册和 host 调用关系放在同一个 sketch 中：

```python
def _on_startup(api):
    api.register_uninstall_hook(
        plugin_id="datapaw",
        hook_name="datapaw_cleanup",
        callback=uninstall_builtin_agents,
        priority=50,
    )


async def unload_plugin(plugin_id: str):
    for hook in api.uninstall_hooks_for(plugin_id):
        await maybe_await(hook.callback())
    await _unload_plugin_bookkeeping(plugin_id)
```

host 的 `PluginLoader.unload_plugin` 按优先级调用已注册 hook，再执行自己的 bookkeeping。

### 落地考虑
`register_shutdown_hook` 覆盖进程生命周期；`register_uninstall_hook` 覆盖插件生命周期。两者不互相替代。

这个 API 不影响 DataPaw 核心运行路径，可以作为小而清晰的 host plugin API polish 独立落地。

## 6. Plan-mode FSM
> _Reviewer (2026-05-26):_ 同意把宿主 `PlanNotebook` 上散落的 `_plan_*` 私有 flag 抽成 `PlanModeFSM`，避免插件子类复制 host 私有状态。
>

### 场景
宿主 `/plan` 命令在 `PlanNotebook` 上维护多个 `_plan_*` 私有 flag，用于控制 plan 工具 gate、等待用户确认、刚变更后的 text-only 行为等。DataPaw 的 `RuntimeStateManager` 也需要同一套 plan-mode 语义，但不应该通过复制 host 私有 flag 来获得。

### 当前 plugin 实现
DataPaw 子类初始化时复制宿主 notebook 的私有 flag：

```python
for attr in (
    "_plan_tool_gate",
    "_plan_awaiting_user_confirm",
    "_plan_just_mutated",
    "_plan_recently_finished",
    "_plan_text_only_after_mutation",
):
    if hasattr(plan_notebook, attr):
        setattr(runtime_state, attr, getattr(plan_notebook, attr))
```

这让插件对 host 私有字段数量和语义产生隐式依赖。host 新增第 6 个 flag 时，插件不会自动跟上。

### 期望 host API
host 抽出独立对象；`PlanNotebook` 和 `RuntimeStateManager` 都直接持有一个 `PlanModeFSM` 实例，而不是在 notebook 替换时复制 `_plan_*` 私有字段：

```python
class PlanModeFSM:
    tool_gate: bool = False
    awaiting_user_confirm: bool = False
    just_mutated: bool = False
    recently_finished: bool = False
    text_only_after_mutation: bool = False

    def reset_per_turn_flags(self): ...
    def should_block_tool(self, tool_name: str, has_plan: bool) -> str | None: ...
    def should_skip_auto_continue(self, has_plan: bool) -> bool: ...


class PlanNotebook:
    def __init__(self):
        self.plan_mode = PlanModeFSM()


class RuntimeStateManager(PlanNotebook):
    def __init__(self):
        super().__init__()
        self.plan_mode = PlanModeFSM()
```

### 落地考虑
插件不应覆写 FSM 方法来定制语义。更稳的方式是让 host FSM 读取工具元数据；工具注册与 FSM 判断放在一起看：

```python
def _on_startup(api):
    api.register_tool(
        run_ipython_cell,
        metadata={"survives_plan_mode": True},
    )
    api.register_tool(
        create_plan,
        metadata={"is_plan_tool": True},
    )


class PlanModeFSM:
    def should_block_tool(self, tool, has_plan: bool) -> str | None:
        if tool.metadata.get("survives_plan_mode"):
            return None
        if tool.metadata.get("is_plan_tool"):
            return None
        return "当前 plan-mode 不允许调用该工具"
```

这样 plan-mode 语义仍由 host 拥有，插件只声明自己的工具属性。

---

## 7. Prompt section registry
> _Reviewer (2026-05-26):_ 接受 ordered prompt section registry；多个插件都需要向系统提示词贡献片段时，不应通过继承 `_build_sys_prompt` 互相覆盖。
>

### 场景
DataPaw 的系统提示需要在宿主基础 prompt 之后插入：

+ `MASTER.md`。
+ `AGENT_MODE.md` 或 `PLAN_MODE.md`。
+ `PLANNER.md`。
+ analysis environment hint。

今天通过重写 `_build_sys_prompt` 完成拼接。

### 当前 plugin 实现
```python
def _build_sys_prompt(self):
    base = super()._build_sys_prompt()
    master = read_prompt("MASTER.md")
    mode = read_prompt("AGENT_MODE.md" if self.mode == "agent" else "PLAN_MODE.md")
    planner = read_prompt("PLANNER.md")
    env_hint = self._analysis_environment_hint()
    return "\n".join([base, master, mode, planner, env_hint])
```

多个插件如果都想插入 prompt section，只能靠继承顺序或互相包方法。

### 期望 host API
```python
api.register_prompt_section(
    name="datapaw.master",
    after="profile",
    agent_id="datapaw",
    provider=lambda agent: read_master_md(agent.lang),
)

api.register_prompt_section(
    name="datapaw.env_hint",
    after="datapaw.planner",
    agent_id="datapaw",
    provider=lambda agent: analysis_environment_hint(agent),
)
```

host 把自己的核心段也命名，例如 `agents` / `soul` / `profile` / `env_context`，然后按 `after=` 拓扑排序。

### 落地考虑
`provider` 建议接收完整 agent 或受限 `PromptContext`。DataPaw 需要读取 `mode`、语言、沙箱配置和 workspace 信息，仅传 `agent_id` 不够。

条件包含不需要额外 API，`provider` 返回空串即可。

---

## 8. ReAct lifecycle events
> _Reviewer (2026-05-26):_ 同意 agent 的 reasoning / acting / summarizing 不应因为 DataPaw trace side effect 被子类重写；host 应提供 ReAct lifecycle hook，让 agent 只 emit 事件，插件在 callback 中观察。
>

### 场景
DataPaw 需要把每次 reason / act / summarize 的输出 `Msg` 追加到当前 in-progress node 的 trace 中，用于刷新、重连和历史回看。

今天 trace 已经是 node 的一部分：`TaskNode._raw_trace` 通过 `model_dump` 输出到 `nodes[i].trace`。问题不是 trace 的落点，而是写 trace 的触发点混进了 agent 推理方法体。

### 当前 plugin 实现
DataPaw 重写三个方法：

```python
async def _reasoning(self, *args, **kwargs):
    msg = await super()._reasoning(*args, **kwargs)
    self.plan_notebook.append_to_trace(msg)
    return msg
```

`_acting` / `_summarizing` 同理。每个 override 只有一行 DataPaw side effect。

### §8.1 Host 侧改动
host 需要在三个 ReAct phase 退出前 emit，并提供统一 dispatcher / registration API：

```python
_lifecycle_registry: dict[tuple[str, str], list[Callable]] = {}


def register_react_lifecycle_hook(agent_id: str, phase: str, callback):
    _lifecycle_registry.setdefault((agent_id, phase), []).append(callback)


async def _emit_lifecycle(self, phase: str, msg):
    callbacks = _lifecycle_registry.get((self.agent_id, phase), [])
    ctx = {
        "phase": phase,
        "session_id": self.session_id,
        "request_id": getattr(self, "request_id", None),
    }
    for cb in callbacks:
        try:
            result = cb(self, msg, ctx)
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception("react lifecycle callback failed")


class ReActAgent:
    async def _reasoning(self, *args, **kwargs):
        msg = await self._run_reasoning(*args, **kwargs)
        await self._emit_lifecycle("after_reason", msg)
        return msg

    async def _acting(self, *args, **kwargs):
        msg = await self._run_acting(*args, **kwargs)
        await self._emit_lifecycle("after_act", msg)
        return msg

    async def _summarizing(self, *args, **kwargs):
        msg = await self._run_summarizing(*args, **kwargs)
        await self._emit_lifecycle("after_summarize", msg)
        return msg
```

callback 协议：

```python
callback(agent, msg, ctx) -> None | Awaitable[None]
```

callback 错误必须隔离，不能影响主 ReAct 流程。

### §8.2 Plugin 侧改动
插件启动期注册三个 phase，并把 callback 实现放在同一处：

```python
def _datapaw_append_to_trace(agent, msg, ctx):
    agent.plan_notebook.append_to_trace(msg)


for phase in ("after_reason", "after_act", "after_summarize"):
    api.register_react_lifecycle_hook(
        agent_id="datapaw",
        phase=phase,
        callback=_datapaw_append_to_trace,
    )
```

然后删除 `DataPawAgent._reasoning` / `_acting` / `_summarizing` 三个 override。

### §8.3 Trace 落点与推送频次
`append_to_trace` 只改内存：

```python
nodes[i]._raw_trace.append(msg)
```

它不做三件事：

+ 不调 `_notify_graph_change`。
+ 不写 `dag.json`。
+ 不推 DAG SSE。

trace 数据在下一次结构变更触发 `_notify_graph_change` 时，随着整个 `TaskGraph` snapshot 一起由 `DAGStore.write` 落盘 + push。也就是说，trace 是搭车落盘和搭车推送，不是自己触发。

这个取舍的含义：

+ live 渲染靠 §9 / §3 sub-A 的 chat metadata。
+ recovery / history 靠 node 上持久化的 `trace`。
+ 如果最近一次结构变更之后 agent 还在长推理，用户刷新页面，可能丢失这段窗口内尚未搭车落盘的 trace。初版接受这个风险，不预先引入周期性 flush 或独立 `trace.jsonl`。

### §8.4 回应单一职责批评
reviewer 的批评对象是 in-tree DataPaw 的 override：agent 推理方法体里直接写 trace。§8 hook 正是这条批评的标准答案：

+ host agent base class 只 emit lifecycle event。
+ plugin callback 观察事件并写 trace。
+ `plan_notebook` / `RuntimeStateManager` 承担 per-node trace 持久化，这是它作为 DAG runtime state holder 的职责。

不建议抽 `TraceStore`。trace-on-node 与 §9 chat metadata 是互补关系：

+ trace-on-node 解决 persistence / recovery。
+ chat metadata 解决 live routing。

把 trace 抽到另一个 store 反而会破坏「DAG snapshot 一次恢复出节点状态和历史 trace」这个有用属性。

---

## 9. Outbound message metadata enricher
> _Reviewer (2026-05-26):_ metadata 应该在消息对象上成为一等字段，由 host SSE 正常序列化；不要让插件在 channel 层对 SSE 字节做 JSON decode / encode round-trip。
>

### 场景
DataPaw 需要给出站 `Msg` 标注：

```json
{
  "graph_id": "...",
  "node_id": "..."
}
```

前端据此把 token 流路由到对应 DAG node drawer。

### 当前 plugin 实现
DataPaw 同时做了两层 workaround：

+ agent 侧重写 `print()`，在 `super().print()` 前写 `msg.metadata`。
+ channel 侧包装 `ConsoleChannel.stream_one`，对已经序列化的 SSE 帧做 JSON round-trip，把 metadata 从 message 帧补到 content delta 帧。

### 期望 host 行为
§3 sub-A land 后，host event schema 有 `metadata` 字段，SSE serializer 原样输出：

```python
event.model_dump_json()
```

插件只需要负责 writer；注册、enricher 实现，以及 host 出站调用点放在一起：

```python
def _datapaw_node_enrich(msg, ctx) -> dict:
    metadata = {}
    if ctx.current_graph_id:
        metadata["graph_id"] = ctx.current_graph_id
    if ctx.current_node_id:
        metadata["node_id"] = ctx.current_node_id
    return metadata


def _on_startup(api):
    api.register_message_metadata_enricher(
        name="datapaw_node_tag",
        when="outbound",
        agent_id="datapaw",
        enrich=_datapaw_node_enrich,
    )


async def print(self, msg):
    ctx = self.build_outbound_message_context()
    for enricher in api.message_metadata_enrichers_for(
        agent_id=self.agent_id,
        when="outbound",
    ):
        msg.metadata.update(enricher(msg, ctx))
    await super().print(msg)
```

### 修订后的简化点
旧文档把 §3 sub-B 设计成 frame transformer：message 帧填表，content 帧按 `msg_id` 反查 metadata。修订版不再需要插件 reader 侧代码：

+ host 保证 `Msg.metadata` 能进入每个需要的 SSE event。
+ 插件只写 metadata，不读 / 改 wire frame。
+ 前端直接读 event payload。

如果 host 当前 content delta 协议确实不携带 metadata，优先应在 host serializer 层补齐，而不是开放 plugin frame transformer。

---

## 10. Typed SkillProvider registration
> _Reviewer (2026-05-26):_ reviewer 备注「宿主已支持 skill provider」；代码核查显示 host 已有 skill system 基础设施，但 plugin API 尚未暴露 typed `register_skill_provider`，需要 host 团队对齐口径。
>

### 场景
DataPaw 自带 skills，需要在 workspace 中物化，并写入 host skill manifest：

+ `enabled=True`。
+ `source="plugin:datapaw"`。
+ channels / 默认启用策略。

### 当前 plugin 实现
插件直接做文件和 manifest 操作：

1. `shutil.copytree` 把 plugin skills 拷到 workspace。
2. 调 host 内部 `reconcile_workspace_manifest`。
3. 直接读写 `skill.json`，patch `enabled` / `channels` / `source`。
4. 写 `.datapaw_versions.json` 做 mtime 缓存。

这让插件耦合到 host manifest 文件结构和字段名。

### 期望 host API
```python
api.register_skill_provider(
    SkillProvider(
        plugin_id="datapaw",
        source="plugin:datapaw",
        skills_dir=PLUGIN_DIR / "skills",
        enabled_by_default=True,
        channels=["all"],
    )
)
```

host 负责：

+ 拷贝或挂载 skill 目录。
+ reconcile manifest。
+ 应用默认启用策略。
+ 卸载时按 `source` 清理 manifest 和目录。

### 代码核查备注
host `skill_system` 已经有：

+ `reconcile_workspace_manifest`。
+ `get_workspace_skills_dir`。
+ `get_builtin_skills_dir`。

但 plugin API 中没有看到 typed `register_skill_provider`。因此本节不是断言「host 没有 skill system」，而是要求把现有 skill system 以 plugin API 的形式暴露，并明确 provider 契约。

---

## 11. Plugin import resolution
> _Reviewer (2026-05-26):_ 接受修复 plugin import resolution；validator 与 runtime loader 应使用一致的 package loading 语义，插件不应通过 `sys.path.insert` 规避相对 import 失败。
>

### 场景
插件包内部模块应该能使用标准相对 import：

```python
from .constants import PLUGIN_DIR
from .agents_setup import ensure_builtin_agents
```

但安装 validator 如果用 `spec_from_file_location` 加载入口文件时没有传 `submodule_search_locations`，相对 import 会失败。

### 当前 plugin 实现
DataPaw 在 `constants.py` 顶层改 `sys.path`，`plugin.py` 再用双路径兼容：

```python
# constants.py
sys.path.insert(0, str(PLUGIN_DIR))


# plugin.py
if __package__:
    from .constants import PLUGIN_DIR
else:
    from constants import PLUGIN_DIR
```

多插件并存时，`constants.py` / `utils.py` 这类自然命名模块可能按 `sys.path` 顺序撞车。

### 期望 host 修复
validator 与 runtime loader 都以 package 形态加载插件：

```python
spec = importlib.util.spec_from_file_location(
    module_name,
    backend_entry_file,
    submodule_search_locations=[str(plugin_dir)],
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
```

### 落地考虑
这不是新增 plugin API，而是 host loader bugfix。修复后，DataPaw 可以删除 `sys.path.insert` 和 `if __package__` 双分支，统一使用标准相对 import。

---

## A. Host-provided sandbox runtime
> _Reviewer (2026-05-26):_ 沙箱方向需要与 Eric 单独对齐；本轮不要求插件继续抽象沙箱 provider。DataPaw 插件侧保持当前 bundled sandbox 形态，等待 host 决策。
>

### 场景
DataPaw 自带基于 Docker / agentscope-runtime 的沙箱栈：

+ `SessionSandboxPool`：按 session_id 复用容器。
+ `SandboxManager`：单容器生命周期。
+ `PathContext`：沙箱视角与宿主路径翻译。
+ 沙箱内 IPython / shell / 文件工具。

长期看，沙箱是 host 级基础设施。多个插件如果都需要沙箱，不应各自 vendor Docker 集成、池化、挂载、越界保护。

### 当前 plugin 实现
DataPaw 自带整套沙箱实现，并把工具注册到自己的 toolkit。它对其他插件是否也需要沙箱没有感知。

### 可能的 host API
如果 host 未来决定提供 sandbox provider，可以是：

```python
def _on_startup(api):
    api.register_sandbox_image(
        image_id="datapaw",
        image_tag="ghcr.io/qwenpaw/datapaw-sandbox:v1.2.3",
        spec=SandboxSpec(
            mem_limit="2g",
            cpus=2.0,
            env={"PYTHONPATH": "/workspace"},
            extra_mounts={},
        ),
    )


def install_datapaw_sandbox(agent, ctx):
    agent._sandbox_manager = api.runtime.get_sandbox_manager(
        session_id=ctx.session_id,
        image_id="datapaw",
        agent_id=agent.agent_id,
        workspace_dir=ctx.workspace_dir,
    )
```

### 落地考虑
本节 frozen：

+ 插件侧不扩展现有 bundled `SessionSandboxPool`。
+ 插件侧不为未来 provider 预留转接层。
+ 当前实现保留为 host 决策前的过渡形态。

只有当 host 明确要提供共享 sandbox runtime，或出现第二个需要沙箱的插件时，本节才重新打开。

---

## B. `execute_shell_command` heredoc handling
> _Reviewer (2026-05-26):_ 这是 host shell tool bug，不是 DataPaw plugin API 主线；建议单独提 issue 修复。
>

### 场景
ReAct agent 很自然会生成 heredoc：

```bash
python3 << 'PY'
import pandas as pd
print(pd.__version__)
PY
```

DataPaw 在数据加载、清洗、绘图时高频触发。

### 当前问题
上游 `_collapse_newlines_outside_quotes` 会把 heredoc body 折叠成单行：

```bash
python3 << 'PY' import pandas as pd print(pd.__version__) PY
```

`sh -c` 于是把 `import` 当成参数或脚本路径，heredoc body 为空，命令 100% 失败。

### 期望 host 修复
在 `_collapse_newlines_outside_quotes` 中增加 heredoc 状态机：

1. 扫到 `<<` 或 `<<-` 时解析 delimiter。
2. 进入 heredoc 状态后保留所有换行。
3. 直到遇到单独一行 delimiter 才退出 heredoc 状态。

调用方不需要新 API。

### 落地考虑
这影响所有使用 `execute_shell_command` 的 agent，不止 DataPaw。建议作为 host bug 单独提 issue，不放进 plugin API landing sequence。

---

## C. First-class in-process sub-agent dispatch
> _Reviewer (2026-05-26):_ 需求成立，但完整 first-class sub-agent runtime 改动面大，本轮整体 shelved；不要先落 `register_reply_signal` 这类半套 hook。
>

### 场景
DataPaw 希望 master agent 能把独立子任务交给 isolated-context sub-agent，例如数据查询、上下文收集、深度分析。要求：

+ sub-agent 跑在同一 session 内。
+ sub-agent 有独立 context window。
+ sub-agent 的输出进入 master 当前 chat 页面。
+ sub-agent 完成后结果回传 master，master 续跑。
+ sub-agent 如需追问用户，下一条 user message 能临时路由给 sub。

QwenPaw 现有 multi-agent 更偏 HTTP 跨 session / 多 profile 协作，不解决「同一 chat 页面内的 in-process sub-agent」。

### DataPaw 当前设计
CLAUDE.md 中的 DataPaw hand-off 方案：

1. master 调 `dispatch_context_subagent`。
2. 工具体内暂存 `_pending_dispatch_signal`，抛 `HandoffSignal` 中断 reply。
3. runner 捕获信号，写 session JSON 顶层 `_pending_subagent_dispatch`，拉起 `ContextSubAgent`。
4. sub 调 `finish_query`，同样抛 `HandoffSignal`。
5. runner 把 sub result 注入 master memory，master 续跑。

之所以用 sentinel exception，是因为上游 `ReActAgent` 没有「任意工具触发 reply 循环退出」的机制。

### 挂起原因
不要把当前 `HandoffSignal` workaround 半正式化成 `register_reply_signal`。这个 hook 只能解决“工具触发 reply 退出”这一小段，却不解决 sub-agent 真正需要的一整组 host 责任：

+ master / sub 的调度与恢复。
+ 同一 chat 页面内的 SSE 输出归并。
+ sub-agent 中途向用户提问时的用户输入路由。
+ master / sub 状态的一致持久化。
+ 前端 nested / inline sub-agent UI 协议。

如果先落 `register_reply_signal`，后续仍然要重做 runner 调度和 session 模型，还会把 DataPaw 的 sentinel 异常设计固化进 host API。更好的处理是：本轮不做 sub-agent API；等 host 准备讨论完整 in-process sub-agent runtime 时，再一起设计。

### 当前结论
本节只记录需求和现有 workaround，不提出本轮落地项：

+ 不新增 `register_reply_signal`。
+ 不新增 `register_subagent_class`。
+ 不新增 `api.runtime.dispatch_subagent(...)`。
+ 不要求插件继续推进 hand-off 迁移。

DataPaw 当前实现保持 frozen；完整 sub-agent 方案作为单独设计议题处理。

---

## Followups
### 第一组：DAG / Chat 流拆分
+ plugin 先落 `DAGStore` / `DAGBroadcaster` / `GET /api/tasks/{sid}/dag/events`，把 DAG backing store 切到 `dag.json`。
+ host 增 `metadata` 字段并原样序列化，再提供 outbound metadata hook。
+ plugin 注册 outbound metadata writer，删除 `ConsoleChannel.stream_one` frame rewrite。

### 第二组：替换 agent / runner monkey-patch
+ host 提供构造期 hook，plugin 用它接入 `RuntimeStateManager` 并删除 `query_handler` wrapper / `contextvars` / smart factory 里的 notebook 接入逻辑。
+ host 提供 `register_uninstall_hook`，plugin 注册 cleanup 并删除 `PluginLoader.unload_plugin` monkey-patch。
+ host 修复 plugin validator 的相对 import，plugin 改回标准相对 import。

### 第三组：替换继承型 override
+ host 提供 prompt section hook，plugin 注册 prompt sections 并删除 `_build_sys_prompt` override。
+ host 抽出 `PlanModeFSM`，plugin 使用它并删除 `_plan_*` 私有 flag 复制。
+ host 提供 ReAct lifecycle hook，plugin 注册 trace append callback 并删除 `_reasoning` / `_acting` / `_summarizing` overrides。

### 后续 / 挂起
+ host 对齐现有 skill system 是否已满足 typed `register_skill_provider`；若暴露该 API，plugin 再迁移 skills 安装逻辑。
+ sub-agent 方案整体挂起，不先落 `register_reply_signal`。
+ sandbox provider 等 host / Eric 决策，plugin 侧保持 frozen。
+ heredoc handling 作为 host shell tool bug 单独提 issue。

关键结论：DAG 独立 SSE 不依赖构造期 hook；它只需要 plugin 侧 `register_http_router` 暴露新 endpoint。Chat metadata 的前端可见性依赖 host `metadata` 字段和 serializer 支持。
