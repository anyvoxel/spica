# TODO

> 一项一节的登记册,记录「已识别但尚未落地」的架构/逻辑问题与后续工作。每节说明问题、影响、候选方案。落地的条目移到 git 历史,不再留在这里。

## recover_leader 与 run_follower 的折叠逻辑重叠 & Noop 水位推进不一致

**状态:** 已识别,待设计收敛后再动手(不要未经评审直接改)

### 问题

`StreamProcessor::recover_leader`(有界启动 pass)与 `run_follower`(常驻 tail pass)在结构上几乎逐行重复——都是「读回折叠」:Command 跳过(不重派发)、Event 折入一个 driver-owned 的 held txn、Reject 审计。二者只差三点:

1. **入口源与终止条件**:recover 用位置式 `logstream.read(pos)` 读到 durable tail(`None`)即停、返回最终 W;follower 用 `stream.next()` 一直 tail 到 `cancel`。
2. **Noop 臂的水位推进机制不一致**(核心):
   - recover(Leader 角色)直接 `txn.commit(Some(noop_pos))`——把 W 推进到 Noop 位(crates/engine/src/stream_processor.rs)。
   - follower(Follower 角色)先调角色钩子 `state_machine.commit_at_noop(...)` 拿 `w` 再 `txn.commit(w)`。
3. **角色归属**:recover 是 Leader 的清理路径;follower 是 Follower 的复制路径。

`commit_at_noop` 是 **follower-only** 钩子,Leader 的实现返回 `Ok(None)`(crates/engine/src/leader.rs)。而 `txn.commit(None)` 只落折叠行、**不推进 `last_processed_position`**(crates/storage/src/memory.rs)。因此 recover 无法复用该钩子:一旦走它会拿回 `None`、W 不推进 → 下次启动重读重折同一批,永不收敛。所以 recover 只能内联 `commit(Some(noop))`——同一概念动作「封批、把 W 推到 Noop」在两条路径上被表达成两种写法。

`run_follower` / `Role::Follower` 目前是 `#[allow(dead_code)]` 的 TODO(multi-node)脚手架;今天只有 `recover_leader` 被真实执行。

### 候选方案(已分析,评级:低风险、值得做)

活着的 leader 循环**从不调用** `commit_at_noop`(其 Noop 臂是纯 skip)。因此可以:

1. 把 `Leader::commit_at_noop` 从返回 `None` 改为返回 `Some(noop_pos)`——对在线路径零影响。
2. 让 `recover_leader` 的 Noop 臂也改走 `commit_at_noop` 再 `txn.commit(w)`,与 follower 完全一致。
3. 抽出一个共享的「读回折叠 + Noop 封批」核心,供两循环复用,仅保留角色/终止差异。

预期效果:消除逐行重复;recover 与 follower 的 Noop 语义统一;为 Follower 落地复用 proven 的 recovery fold 铺路,避免另起炉灶导致两套实现漂移。

### 注意

- 合并时须保留 recover 的「**有界**」性质(读到 durable tail 即停,不能 tail 等待),以及它**不触发 `after_commit`**/ack(恢复路径无在途 caller)。
- 若把 follower 循环直接当作 recovery 的驱动,需把 Noop 水位推进参数化或统一到 `commit_at_noop`,改动面更大——明确后再评估。

## (参考)Zeebe 异步复制 + ahead-of-commit + committed-only 快照 —— multi-node 时的吞吐优化方案

**状态:** 设计参考,已消化;**单节点不需要、也不应提前落地**。触发条件 = spica 跨入 multi-node(引入 quorum 复制)时再评估。

### 背景:为什么现在不碰

spica 目前单节点:本地 append、本地 `work.commit`、observability 三者水位重合(本地 commit == committed == durable),所以现有 `after_commit` 在本地 commit 后同步调 `on_event_applied` 是**完全正确**的,不需要也遇不到下述任何 ahead-of-commit 问题。Zeebe 那套复杂度的全部成因,是「Raft 复制是异步的、commit 水位可以落后本地处理」——单节点没有 quorum,成因不存在。

### Zeebe 做了什么(已从 camunda/camunda 源码确认)

1. **tryWrite 是异步的(只同步 append 到 leader 本地 journal)**:`Sequencer.tryWrite → logStorage.append → logAppender.appendEntry`(`AtomixLogStorage`)。向 follower 的复制由 `LeaderAppender` 后台流水线扇出;Raft commit 靠 `CommittedPositionListener` 推进,**与本地 append 是两条独立边界**。
2. **投影(`EventApplier`)在 commit 之前就折**(在未提交的 DB 事务里),leader 可以**跑在 commit 前面(ahead-of-commit / "run arbitrarily far ahead of the commit index")**——因为投影只是 log 的确定性导出,崩溃后由「快照 + committed 回放」重建纠正,**ahead 可容忍、可重建**。
3. **一切「不可逆地暴露给外界」的东西必须闸在 committed 水位之后**——因为它不可撤销:ack/响应用 `SideEffectRunner`(有界队列 + 挂 `committedPosition`),快照用「临时 + 等 commit 追上 + 追不上就 abort」(`AsyncSnapshotDirector`:`takeTransientSnapshot → waitUntilLastProcessedAndWrittenPositionIsCommitted → persistSnapshot`)。**投影可以 ahead,**观察/固化**不能 ahead**。这是全设计唯一不可让步的闸门。
4. **快照绝不包含 ahead-of-commit 数据**:快照只在 `commitPosition >= max(lastProcessed, lastWritten)` 时才持久化,否则 abort/丢弃;重启恢复 = 删 runtime → 从最后持久化快照(`openSnapshotOnlyDb`)开库 → committed 回放重建到提交尾。**另一种等价方案:follower-only 拍快照**(follower 从不 ahead,天然 ≤ committed)。
5. **tryWrite 整批 = 一条 raft `ApplicationEntry`,raft 不校验批内 position**:`tryWrite(List<LogAppendEntry>)` 把一次处理产出的全部记录打包成一个 `UnserializedApplicationEntry(lowestPosition, highestPosition, data)` 作为**单条** raft 项(一个 raft index、一次原子提交),`lowest/highestPosition` 只是元数据。因此 **Zeebe 的 position 正确性完全由 `Sequencer` 负责**:它持有一个游标计数器(seq.java:`currentPosition = position; highestPosition = currentPosition+batchSize-1; position += batchSize`),批内各记录的位置由这条游标按顺序分配,并在写入前用 `lock.tryLock` 串行化所有并发 tryWrite,从而保证位置的唯一/连续/单调。raft 层把批当不透明 blob,不校验、不纠正——若 Sequencer 算错(缺口/重叠/乱序),会在下游延迟引爆(reader seek、水位、snapshot 的 index↔position 映射、replay 起跳点)而 raft 拦不住。

### 关键认知(避免踩坑时搞混)

- **延迟 vs 吞吐**:这条 pipeline 优化买的是**吞吐**,不是延迟。单条 ack 延迟无论 sync/async 都 = Raft commit 时间;async 的优势在于 committed 吞吐以「复制带宽 × 在飞窗口 / RTT」计的流水线速率,而非 sync-per-record 的「1/RTT」。
- **前置处理快的意义** = 喂饱 raft 流水线,让 commit 速率不被本地 CPU 拖为次瓶颈(饱和区除外,那时本地再快也无增益)。
- **简单的替代方案**:若未来量级是低速率 / 单区域 / 可接受每命令 = 集群 RTT,**同步 write-through(tryWrite 阻塞到 quorum)是合理且更简单的选择**,能省掉上面全部复杂度(但仍保留「快照 + committed 回放」恢复,因为 committed-未折投影 的崩溃窗口消不掉)。**选 sync 还是 async,由目标负载与部署拓扑决定,不是孤立的延迟数字。**

### 落地到 spica 的候选步骤(仅当跨入 multi-node 时)

1. 让 `Hook`/`after_commit` 这条接缝从「本地 commit 后同步调用」演进为 **「挂 committed 水位的有界异步 side-effect 队列」**(SideEffectRunner 等价物)——现接缝已为此预留,不重写只换驱动源。
2. 为快照引入 **committed-only 不变量**(临时 + 等 commit + abort,或 follower-only 拍)。
3. 恢复路径保持「快照 + committed 回放」,据此选择 snapshot 起点水位。

### 注意

- 这是一条**预留的能力**,不是当前任务;**在加入第二个副本 / 网络 / quorum 之前,不要为它引入任何运行时复杂度**(单节点下它是纯负担)。
- 若最终选择多节点,优先保住「observable/snapshot 必须成立于 committed 水位」这一闸门——它是正确性底线,其余(是否异步、是否批量)都是可在其上调整的性能参数。

## (设计参考)Snapshot / Checkpoint / commitAwait 的正确性模型 —— 单分区「投影 ahead、固化不得 ahead」与跨分区「同 checkpointId 因果一致」

**状态:** 设计参考,已消化(与上节同源,均为 Zeebe 源码走读);**单节点不需要**。触发条件同:跨入 multi-node 时再评估。

### 三个概念的区别(避免混为一谈)

| | Snapshot 快照 | Checkpoint 检查点 | commitAwait(等待机制) |
|---|---|---|---|
| 本质 | 单分区**本地恢复制品**:状态 DB 在 `lastProcessedPosition` 的一致拷贝 | 跨分区备份的**协调锚点**:带 `(id, position, type, timestamp)` 的日志命令/事件 | 快照落盘前等 raft **已提交位置**追上投影位置的闸门 |
| 问题域 | 崩溃免于重放全量日志回恢复 / follower 追赶 | 多分区备份对齐到同一代、因果一致 | 把「投影可 ahead、固化不可 ahead」这句话落地 |
| 锚点 | `lastProcessedPosition`(处理器折到哪) | `checkpointPosition` = CREATE 命令自身日志位置 | `requiredCommitPosition = max(lastProcessed, lastWritten)` |
| 触发 | `AsyncSnapshotDirector` 周期(snapshotRate)或 force | `CheckpointSchedulingService`/显式备份请求 | 快照流程内发起 |
| 特点 | 本地、独立、失败 abort 下周期重试 | 全局、协调、跨分区广播 | leader-only 信号源 |

> 协同但**不相互触发**:备份 = 快照(状态基线) + 到 checkpointPosition 的日志 + 锚定在 checkpoint。checkpoint 处理器调的是 `backupManager.takeBackup`,不触发快照。

### 快照正确性:投影可以 ahead,固化不能 ahead

```
takeTransientSnapshot(lowerBound)   // 含 ahead-of-commit 投影的一致拷贝
  → 记 lastWritten / lastProcessed
  → waitUntil...IsCommitted          // commitAwait:等 commit 追上
  → flushJournal → persistSnapshot   // 之后才落盘、才对外暴露
```

- 投影在提交前就折进(未提交本地事务),leader 可 ahead-of-commit,因它是 log 的确定性导出,崩溃后由「快照 + committed 回放」重建纠正。
- 一切**不可逆暴露**(快照 persist、ack、响应)必须闸在 committed 之后;投影可以 ahead,观察/固化不能 ahead。
- commitAwait 实现:`AsyncSnapshotDirector` 内 `commitAwaiters`(TreeMap)以 `requiredCommitPosition` 为 key 挂 future,`newPositionCommitted` 到来按 `headMap(commitPosition, true)` 一并发回;**追不上则永久悬空、不落盘**——这是设计而非缺陷。

### 跨分区一致:不是原子切面,是"同一 checkpointId"的因果一致

- `CheckpointIdGenerator`:id = 墙钟毫秒 + offset,**全局唯一、单调**;`CheckpointScheduler`(最低 id 的 broker 上)把**同一个 id** 的 `CHECKPOINT:CREATE` 发给所有分区;各分区各自本地折叠。
- 真正的保证在第二通道——**跨分区命令捎带 checkpoint**(`InterPartitionCommandSenderImpl` + `InterPartitionCommandReceiverImpl`,见 `InterPartitionCommandCheckpointTest` 四例 first/update/not-recreate/not-overwrite):
  ```
  A 发跨分区命令给 B,附 A 的 (latestId, type);
  B 先判:B.latestId < A 的 id ? 是 → 先写 CHECKPOINT:CREATE(A 的 id)再写命令;否 → 直接写
  ```
- ✅ 保证:**因果 / epoch 一致**——共享单调 id + 捎带补位,「B 在 checkpoint N 里的跨分区数据,其前因必在 A 的 checkpoint N 里」。
- ❌ 不保证:**同一实时时刻 / 同一全局日志位置**。各分区 `checkpointPosition` 天然不同,非全局原子切面。
- 闭环:折叠 `CheckpointCreatedEventApplier.apply` 的 `checkpointListeners.forEach(onNewCheckpointCreated)` → `InterPartitionCommandSenderImpl` 据此把最新 id 挂到出向命令(listener javadoc 明写 "for InterPartitionCommand Sender/Receiver to get latest checkpointId")。

### 领导权切换下的快照安全(核心正确性论证)

**情景**:L1 拍快照、记录了要等 position 100;100 未复制成功就 step down;新 L2 append 并提交了(可能是复用数字上的)100;L1 若据 commitIndex 判到 100 会存下错误快照。

**Zeebe 的解法 — 不"证明 100 还是那个 100",而是让等待在失位后永久悬空**:

- 供 `AsyncSnapshotDirector.onCommit` 的信号源 = `RaftApplicationEntryCommittedPositionListener`,其调用点**仅一处** `LeaderRole.java:1040`(全 raft 唯一),且带守卫 `if (isRunning() && commitError == null)`;Follower/Candidate/Passive 均无。
- L1 变 follower → raft 不再调 `onCommit` → `commitAwaiters[100]` 永久悬空 → `persistSnapshot` 走不到 → transient snapshot 从未 `.persist()`,被 abort/reset 丢弃。**L2 提交什么都喂不到这个 wait,错误的快照连落盘都做不到。**
- leader 在任时安全:`committedPosition` 报的是 leader **自己那条 entry 的 `highestPosition()`**,与投影同源(own-term 提交规则),导致数字与投影内容一致。
- 第二道防线:position 由 `Sequencer` 从已提交尾继续分配,废弃的 ahead 位置一般不复用(不过即使复用,上面的 leader-only 门仍结构性挡住)。

### 对 spica 的意义

- 单节点下 spica 本地 commit == committed == durable,`after_commit` 同步调 `on_event_applied` **完全正确**,成因(ahead-of-commit)不存在,不需也不应提前落地上述任何机制。
- 若未来 multi-node:重启用「快照 + committed 回放」;快照 persist 用「临时 + 等 committed + 等不到即 abort」(或 follower-only 拍);跨分区备份要用「共享单调 checkpointId + 命令捎带补位」的因果模型,而非试图做全局原子切面。
- 注意单节点同样适用一条真·不变量:**快照/observation 必须成立于已提交水位**——即使单节点它也只是"本地 commit"这一特例,无需额外复杂化。

## (设计参考·备选)链式事务 / 提交门控 apply —— 让「DB 已提交 ≡ raft 已提交」成为结构不变量,替代 Zeebe 的 snapshot+rebuild

**状态:** 参考,未落地;**单节点不需要**。触发条件同:跨入 multi-node 时,作为上方 Zeebe 方案的**更简单替代品**候选评估。

### 动机:为什么它比 Zeebe 更 principled

ahead-of-commit 一切分歧的**根因 = 投影事务的提交先于 raft 复制成功**(`tryWrite` 只本地 append,投影立刻 commit 进 RocksDB,再异步等 raft)。Zeebe 的做法是「投影先进持久 DB + 事后用 snapshot 卫生与 committed 重放修复」。链式事务改走另一条路:**把不变量做进结构**,而非靠卫生守护——让「DB 的已提交状态」和「raft 的已提交状态」恒等。

### 方案

```
内存 delta 链(volatile):提交折叠出的 ahead 状态,可丢弃
  tx1 ⊃ tx2 ⊃ tx3 …  每个基于前一个的未提交写入,tryWrite 后不提交,继续叠
  读路径 = DB(已提交基底) + 链(未提交 ahead)   // 处理器内部读自己的写
                    │
raft commit index X ─► delta 1..X 一次性原子写成 RocksDB batch
                       + 原子记「DB 反映到 raft index X」
   → raft 复制成功到 i 才提交 tx1..txi;继续后提交 tx2..txj …
   → raft 截断(leader 切换,suffix 截断) → 把未提交后缀链级联 abort,DB 剩=已提交前缀
```

成立后:**DB 已提交状态 ≡ raft 已提交状态**:
- Snapshot 随便拍(拍到的永远是已提交位置),commitWait / 临时+abort / leader-only 信号全都不需要。
- 崩溃恢复 = 直接开 DB(本身就是一致已提交态,无需删 runtime + committed 重放)。
- 截断(leader 切换) = 丢弃内存后缀,便宜;raft 截断天然是**连续后缀**,与链的级联 abort 恰好吻合。

### 致命前提(本方案最大短板):DB 无原生支持

**核心结论:** 方案成立的前提 = DB 支持「从 in-flight 事务派生子事务、子事务读到父的未提交变更、且可级联 abort 一段后缀」。**PostgreSQL / MySQL / RocksDB TransactionDB / LMDB 都不提供**这些公开能力(嵌套/堆叠深度也受限)。所以链**只能自建在应用层**(内存 delta 层 + raft 提交时才物化到 DB),DB 只当"已提交"的物化底座。

### 与 Zeebe 取舍对比

| | Zeebe(投影先进 DB + 事后修复) | 链式(投影先进内存,DB 只在已提交时写) |
|---|---|---|
| ahead 状态在哪 | RocksDB(持久,错误成本高) | 内存 delta 链(易丢弃) |
| 快照安全性 | commitWait / 临时+abort / leader-only | **结构保证,随便拍** |
| 崩溃恢复 | 删 runtime → 开快照 → 重放 committed(重但罕见) | 直接开 DB(平凡) |
| 截断(切换 leader) | 整段重建 | 丢内存后缀,便宜 |
| 代价 | recovery 重、机制复杂 | 内存装"热 ahead 态"、读路径多一跳、需自建链层 |

### 注意/代价(落地时须逐一权衡)

- **内存吃热态**:ahead 窗口内(≈raft 在飞窗口)的"热状态"住在内存链,而不在 RocksDB;窗口越深内存越大。吞吐不吃亏(CPU 仍可在内存提前折、喂饱 raft 流水线),但 hot-state 的持久/恢复重心移到了自建层。
- **读路径多一跳**:随机读(如 workflow 查状态)需维护「DB + 链」叠加视图;且须区隔「处理器内部读自己的写(DB+链)」vs「外部一致读(只读 DB)」。
- **DB 提交原子性**:物化到 RocksDB 必须与「DB 反映到 raft index X」的记账原子写,否则崩溃后无法确定 resume 点。
- 若选此方案,上方 Zeebe 条目「落地到 spica 的候选步骤」的 committed-only 快照与恢复步骤可被简化/替代;代价(自建内存层)高于直接复用 Zeebe 方案时再保留 Zeebe 原案。

## (设计参考)外部只读 vs ahead-of-commit —— 三级读模型,以及 Zeebe 方案的取舍与已知问题

**状态:** 参考,已消化(源码走读);**单节点不需要**。触发条件同:multi-node 时,与上方两条 Zeebe 参考联动看。

### 核心事实:leader 的内部读**必然**能看到还没 raft 提交的数据

计算 entry i+1 的折叠必须读 entry i 的(尚未提交)结果——这是 ahead-of-commit 模型 read-your-writes 顺序折叠的**必然代价**,不是 bug。问题不在"读到 ahead",而在"读到之后拿它做了什么"。安全靠三支柱:

1. **折叠是确定性的**:同一 log 前缀怎么折结果都一样,读 ahead = 读一个可复现的推测值,不是读脏。
2. **提交是前缀 → 依赖链原子丢弃**:entry 6 基于 entry 5 的 ahead 结果产生,但 6 只有 5 提交后才能提交(ack 6 必连 5 一起 ack);若 5 被截断,6 必在同一未提交后缀里一起被截断 → **读到幻影 + 产生 echo + 一起丢弃,因果始终封在 log 里,不会半截落地**。
3. **不可逆暴露全闸在 committed**:ack / 响应 / 副作用 / export / 快照,只发生在该位置 committed 之后。

### Zeebe 的三级读(外部一致视图怎么来的)

| 读 | 读哪份 | 能否 ahead | 为什么安全 |
|---|---|---|---|
| 处理器内 read-your-writes | state(含 ahead) | **可以** | 逃逸的每个影响都被追加到 log 的 cause 之后、commit-gated |
| 响应 / 副作用 | state 计算结果 | 只发 **committed** | 前缀提交,幻影随 cause 一起截断 |
| 外部读模型 | **exporter 在 committed 位置构建** | 从不 | 独立读模型,只由已提交事件构成 |

- **Job 激活**(读"有哪些 job"且交给 worker):内部扫描 `JobBatchCollector.forEachActivatableJobs` 读 RocksDB `JobState`(含 ahead),但响应是 JOB:ACTIVATE 命令的 commit-gated side-effect → worker 永远拿不到幻影 job。且 Zeebe 没有"只读 list jobs 不 claim"的公开 API——看 job 只能 activate。
- **流程实例状态 / 外部查询**:不读 leader 的 ahead RocksDB,而是由 **exporter 在 committed 位置**(`ExporterDirector` seek 自 snapshot/committed)→ 灌入独立读模型(ES/OS/RDBMS)→ 外部从那读。

### Zeebe 方案的可能问题 + 对应处理(这条参考要记的重点)

1. **问题:谁对"只读当前状态"负责?** Zeebe 经典模型里根本没有"安全的直接只读查询"——唯一的直接读 API `QueryApiRequestHandler`(读 `QueryService`)在 handler 层**没有 committed 闸门**,读的就是可能 ahead 的 state;且整条 API 被 `@Deprecated(forRemoval=true, since=1.2.0)` 标记待移除。
   - **处理**:Zeebe 的答案是**不暴露这个表面**。外部一致视图改走(①)commit-gated 命令响应 或(③)exporter 建的 committed 读模型。**用"移除 API"而非"修闸门"来应对**,即"不该让活分区状态成为外部查询面"。
2. **问题:内部 ahead 状态会不会漏成外部观察?** 若某条读路径绕过 commit-gating 直接外发 ahead 结果,截断后会留下"已外发的幻影"。
   - **处理**:纪律是「任何 Speculative 读的结果,若要离开处理器,必须先转成一条 log 命令并被 committed-gate」;同时所有 exporter / snapshot / 响应都只读 committed。**实现上给读两个入口**:`Reader::Speculative`(base ⊕ ahead,仅处理链内部)与 `Reader::Committed`(仅已提交,给外部/观察/快照/export),在类型/API 层强制区隔。
3. **问题:exporter 读模型的一致性成本**。因为 state 可能 ahead,外部一致视图必须**另起一套 exporter 读模型**,带来了复杂度 与 端到端滞后(exporter 落后于 committed 再有读模型延迟)。
   - **处理**:接受滞后(观察性查询不需要线性化的新鲜度),换来一致性由 committed 保证。**这也是 spica 链式方案的相对优势**:base DB 按构造 ≡ 已提交,外部只读**直接查 base 即可一致**,无需为"修一致性"再造 exporter 读模型(exporter/读模型仍可为性能/解耦而建,但不再是正确性必需)。

### 对 spica 的意义(链式方案联动)

- 单节点:spica 本地 commit == durable,没有 ahead,三级读自然退化为一级,以上全部不适用。
- 若 multi-node 采用链式方案:**「外部读 = base-only」由结构天然成立**(base ≡ 已提交),只需守住「Speculative 读的结果不得直接外发、必先转成 log 命令并 commit-gated」这一条纪律;这比 Zeebe「靠 exporter 另读模型」更省。
- 若采用 Zeebe 原案(投影先进 DB):则外部一致视图必须复刻这三级读 + exporter committed 读模型,并接受「直接只读 API」这个坑(参照 QueryApi 被弃用)。

## `fail_or!` / `terminate` 把错误通道压平:`InfraError` 被落成持久终止,领域错误绕过 Catch

**状态:** 已识别,待设计收敛后再动手(不要未经评审直接改)。已从 camunda/camunda 源码(`ProcessingStateMachine`)走读确认 Zeebe 的分通道做法。

### 问题

`fail_or!`(crates/engine/src/handlers/mod.rs:5-15)把**一切 `Err` 一律定义为 `terminate`**(TerminateState+TerminateExecution,持久决议)+ `return`;散布在 22 处 `fail_or!` + 21 处 `.terminate()`。这把 Zeebe 用不同通道区分的四类错误塌缩成一个动作,「该重试 / 该 reject / 该 panic / 该终止」无法表达:

| spica 错误 | Zeebe 对应 | 本应走的通道 | 现状 |
|---|---|---|---|
| `ExecutionError::Infra(InfraError::Log)`(瞬时 log/storage 故障) | `RecoverableException` | **重试**,不推进位置 | ❌ 被 `terminate` 落成持久终止 — **真 bug** |
| `Runtime(RuntimeError)`(Jsonata/StateNotFound/StateFailed/TimedOut) | 领域 error-record + `Catch`/`Retry` 拦截 | 拦截不到才终止 | ⚠️ 被 `terminate` 绕过 Catch 直接终止 — **粒度过重** |
| `Rejected(Reject)`(命令前置失败) | `COMMAND_REJECTION` | **reject** | ⚠️ 语义与 terminate 混用 |
| 引擎不变量「不可能发生」 | 自举失败 | **panic** | ⚠️ 部分仍 terminate(已修 3 处 `create==None`) |

- **自相矛盾的证据**:`crates/engine/src/types/error.rs` 对 `InfraError` 的注释自己写明应 "bubble … to the bug-facing config/retry path",而 `fail_or!` 却把它 terminate。
- **Class A(真错,瞬时可重试被落成终止)——已核实会透出 `InfraError` 的位点**:
  - `ctx.machine(...)`/`machine_for_scope(...)`:内部 `get_flow_version(...).await?`(handler.rs:239-243)透传存储读失败的 `InfraError` → `create_execution.rs:65`、`activate_state.rs:60`、`complete_state.rs:168`。
  - `load_scope_ref(...)` 的 `Err(e)` 分支:`activate_state.rs:56`(最初发现处)、`state_handler.rs:207` 等。
- **Class B(方向对、粒度过重)**:`fail_or!(evaluate...)` 返回 `RuntimeError::Jsonata`(pass/wait/task/choice/parallel/map/succeed/fail 等十余处),以及 `resolve_state_*` 的 `StateNotFound`/`InvalidDefinition` —— 这些本应先进 ASL `Retry`/`Catch` 按 `error_name` 拦截,现在被 `fail_or!` 无条件升级成终止。
- **Class C(反了,该 panic)**:`complete_state.rs:124` 的 `InvalidDefinition("activity parent must be a scope")` 是对引擎结构假设的断言,应 `unreachable!` 而非 terminate。

### 候选方案(已分析,未定案;建议分三小步收敛)

核心:**把错误类一路保留到决策点,拆开 `fail_or!`/terminate 通道**,而非逐个改 43 处:

1. **先盘点**:单独列一份「能透出 `InfraError` 的所有位点」清单(Class A),作为最小闭合块。
2. **设计重试通道**:让 handler 层的可重试错误不是 `terminate`,而是「不产出 entries / 不推进位置 / 上交驱动」。驱动侧复用已有不变量——`CommandProcessed { entries, work }` 只在 append 成功后提交、失败 drop 回滚(processing.rs)——即 Zeebe 「提交才推进、失败重放同一条 command」的对应(可参考 `PROCESSING_RETRY_DELAY` 250ms + actor 一次性定时器的节奏)。
3. **Catch 提到 terminate 前(Class B)**:领域错误先按 `error_name` 走 `Retry`/`Catch` 拦截,仅拦截不到才终止;同时把 `complete_state.rs:124` 这类结构断言改 `unreachable!`。

### 注意

- 判定每处到底「重试 / 拦截 / panic / 终止」,**依据是 `Err` 携带的 `ExecutionError` 变体**,不是调用点位置;`evaluate`/`resolve_state_*` 只返回 `RuntimeError`(永久),只有包了存储读取的 `machine*`/`load_scope_ref*` 才会透出 `InfraError`(瞬时)——这一步是清单的关键判据。
- 未评估 `terminate` 现是否会先被某处的 Catch 消费;若引入重试通道,须保证「重试」不破坏 at-least-once(重放条目须幂等,与 `TasksClaimed` 的条件折叠同一原则)。
- TODO 内联注释系统(crates/engine/src/handlers/activate_state.rs:38 的 Scope/reject/retry 批注)与本条目同源,落地时一并收敛。

## (设计参考)Reject / Incident / Resolve / SetVariables —— 引擎「不可遵循的命令」与「可恢复的暂停」如何分工、恢复

**状态:** 设计参考,已从 camunda/camunda 源码走读消化;**spica 目前只有 Reject 半边,Incident/Resolve/SetVariables 未落地**。触发条件 = 需要「外部人工干预后让卡住的执行继续前进」的场景时再评估。

### 三个概念的分工(先厘清,避免混淆)

| | Reject(拒绝) | Incident(事件/钉住) | 外部变量修正 + Resolve(恢复) |
|---|---|---|---|
| 语义 | **命令自身不能被遵循**(前置不满足):被拒命令被丢弃,**引擎不再推进**,不产生事件投影 | **已接受的执行在某个转换处失败**,元素**暂停在原地(停在"ing"态)**,等外部干预 | 操作者**先修正外部条件,再发 `RESOLVE` 重试**那条失败的转换 |
| 与 spica 现状 | ✅ `Reject` + `RejectionType` 已落地(types/reject.rs,COMMAND_REJECTION 的 CCES 对应) | ❌ 无 incident 实体 / `IncidentIntent` | ❌ 无 `Resolve` 命令、无运行期 `SetVariables`(只有定案期 `Assign`) |
| 引擎侧是否重试 | **否**,reject 是终态(客户端收到 `COMMAND_REJECTION` 响应,自己决定怎么办) | **不确定**,等 `RESOLVE` 到来自行触发重放 | 是:`RESOLVE` 处理器按元素当前状态**反推并重发那条失败命令**,用新变量重算 |
| 持久性 | `COMMAND_REJECTION` record,不进 state | `IncidentIntent.CREATED` 事件进 state,绑 processInstanceKey/elementInstanceKey/variableScopeKey/errorType/errorMessage | 失败命令以 `RetryTypedRecord` 重新入队重放 |

### Zeebe 的失败→恢复链路(走读确认)

1. **求值失败落入 incident,且分错误类型**:输入/变量映射失败 = `ErrorType.IO_MAPPING_ERROR`(`BpmnVariableMappingBehavior`);分支条件失败 = `CONDITION_ERROR`。这类可恢复领域错误走 `createIncident`(`BpmnStreamProcessor` 在 `afterActivating`/`onComplete` 的失败臂调 `incidentBehavior.createIncident(...)`,`IncidentIntent.CREATED`),**而不是 reject**。
2. **元素停在"ing"态**:`ACTIVATE_ELEMENT`/`COMPLETE_ELEMENT` 的转换**没提交**,元素悬在 `ELEMENT_ACTIVATING`/`ELEMENT_COMPLETING`,等 `RESOLVE` 重来。
3. **恢复顺序(关键):先改变量,再 RESOLVE**。操作者先通过 `SetVariables` 命令(`BrokerSetVariablesRequest`,写 `VariableIntent.UPDATED`)修正作用域变量;再发 `RESOLVE`。`IncidentResolveProcessor` 按当前状态反推要重发的命令:
   ```java
   case ELEMENT_ACTIVATING  -> ACTIVATE_ELEMENT;
   case ELEMENT_COMPLETING  -> COMPLETE_ELEMENT;  // 多半是这条
   case ELEMENT_TERMINATING -> TERMINATE_ELEMENT;
   ```
   (`IncidentResolveProcessor.java` 约 296–298 行,`RetryTypedRecord`)重发后**用当前(已修正)变量重新执行映射求值**,通过则元素继续走,随后 `IncidentIntent.RESOLVED` 收尾。
4. **顺序颠倒的后果**:不改变量直接 `RESOLVE`,重放仍用旧变量、仍失败 → **又创建一个新 incident**,不会前进。因此 `RESOLVE` 只是"我已经修好了,请重试",引擎侧不做任何补齐。

### 与既有 TODO 的联动

- 与「`fail_or!` / `terminate` 把错误通道压平」条目同源互补:那条讲的是**把错误正确地分类**(重试 / 拦截 / panic / 终止);本条目讲的是**分类成 incident 之后如何恢复**(SetVariables + Resolve)。若落地 incident,`fail_or!` 的第一个分叉点应把「可恢复的领域求值失败(RuntimeError)」优先导向 createIncident(暂停 + 等 Resolve),而不是一律 terminate。

### 注意(落地时须保留)

- 分辨「reject vs incident」的判据是**已接受的执行 vs 尚未接受的命令**:`Reject` 拒绝的是"命令我不会执行";`Incident` 暂停的是"已执行的转换我卡住了"。二者分层不同,互不替代。
- incident 必须持久(进 state),才能跨重启保留、才能让 `RESOLVE` 按元素状态重放;这与 spica 目前「Reject 不进 projection」是**有意为之**的差异。
- 若引入运行期 `SetVariables`,需与定案期 `Assign` 明确作用域语义(SetVariables 用 `variableScopeKey` 定位,Assign 只写当前 activity 的 owner scope),避免两套写变量入口语义漂移。
- `RESOLVE` 重放的是原命令,**必须幂等**(重放条目不得重复入账),与 `TasksClaimed` 的条件折叠同一原则(at-least-once 安全)。

## `TaskTimeout` Timer 的归属:`Activity -> Timer` 还是 `Task -> Timer`

**状态:** 已识别,待评审后再动手(不要未经评审直接改)。倾向**维持现状**(Timer 挂 Activity),另有更小的替代方案;触发条件 = 重试(`TaskTimeout` 按 attempt 重新 arm)真正落地时再评估。

### 问题

一个 `Task` state 今天产出两个子节点,都挂在**同一个 activity** 下:`Activity -> Task`、`Activity -> Timer`(`TimerPurpose::TaskTimeout`,由 `states/task.rs::after_activated` 经 `emit_timer` arm)。因此 timer 不知道自己约束哪一次 task attempt:它 fire 时必须**反过来向 activity 要** in-flight task(`trigger_timer.rs` 的 `TaskTimeout` 臂:`get_children(activity).find(kind == Task)`,找不到就 return)。把 timer 改挂到 Task 下,这两个动作(搜索 + `no in-flight task` 分支)都会消失。问题是代价。

### 现状为什么能工作:归属同时承担了「终结扫荡」职责

- **子边是独立的存储单元**:`get_children` / `add_child` / `remove_child` 维护 `_index/children/<parent_kind>/<parent_name>/<child_kind>/<child_name>` 下的父子边表,任何 kind 都能拥有子节点,行上不再保存 children —— `Task` 与三种容器行一样都是普通行。
- **三条级联扫荡全靠这条子边**:`terminate_execution.rs`、`terminate_thread.rs`、`terminate_state.rs` 都是「遍历 scope 的子边(`get_children`),遇 `ObjectKind::Timer` 就 `CancelTimer`」。Timer 挂 Activity ⇒ 任何 scope 终结路径都自动兜住它,不依赖 Task 那条路径做任何事。
- **命名与 owner 无关**:timer 名来自 `execution.name.base().generated_from_key(seq)`(`handlers/mod.rs::emit_timer`),`Timer::execution` 也是顶层 flat run。所以改挂不影响子边按名匹配,这一点是中立的。

### 改挂 Task 的收益(确实存在)

1. `TaskTimeout` fire 变成直接定位:owner 就是 task,`FailTask` 直出。
2. **重试更干净**:`states/task.rs` 的 `TODO(M2): re-arm TaskTimeout` 一旦落实,attempt ↔ timer 对应天然唯一。今天同一 activity 下可并存多个 attempt 的 timer,靠的是更宽的规则「task 一 settle 就扫掉 activity 的**所有** timer」。

### 改挂 Task 的代价(四条实打实)

1. **行结构不再要改**:子边现在存在独立的 `_index` 边表里,任何 kind 都能拥有子节点,Task 不需要在行上加 children 集合 —— 「Task 从叶子变容器」那部分代价不复存在,剩下的只是第 2 条那条新不变式。
2. **级联扫荡不再覆盖它**(最关键)。今天「`CancelTask` / `TaskCompleted` / 租约过期 / `terminate_state`」任一路径都由 activity 级一次扫荡兜底;改挂后必须成立新不变式:**每条让 task 终结的路径都扫它自己的 timer**。漏一条就会留下活着的 timer,scheduler 到点触发 `TriggerTimer` 去打一个已不存在的 task(即那堆 late-fire 防御分支要处理的情形)。
3. **Activity 的 drain 信号变了**。`trigger_timer` 现在特意在「没找到 in-flight task」时仍跑 `child_settled`,注释明写:那正是 cancel 已扫掉 task、只剩已 fire 的 timer 撑住 activity 的场景。改挂后这个 relay 的收敛对象换成 task 的容器,而此刻 task 自身已 terminal/drained,收敛推理要重做。
4. **容器分发扩容**:Timer 的 owner kind 从 {Execution, Thread, Activity} 变成再加 Task,`cancel_timer` / `trigger_timer` 里的 `if owner.kind != ObjectKind::Activity { return }` 需变成按 kind 分发(`TaskContainer`)。与现有 `Container` 抽象方向一致,但是**新增工作量**,不是改挂的自动结果;且与 `8e0f90f`(把每个 state 的 owner 统一到 Thread)方向相反 —— 那是在收敛「谁拥有子节点」。

### 候选方案:Task 持有自己的 timeout timer 引用(倾向这条)

保留 Timer 挂 Activity,让 **Task 记住自己的 timer**(`Task.timeout_timer: Option<ObjectRef<TimerKind>>`,随 `ActivateTask` 一起带去):

- `trigger_timer` 的 `TaskTimeout` 臂 O(1) 拿到 task,搜索与 no-in-flight 分支都删掉;
- 终结路径按引用**精确**扫那一颗 timer,而非「扫 activity 的所有 timer」;
- 重试时 attempt ↔ timer 也唯一。

代价仅是 Task 多一个字段(域对象 + 事件 + 测试),不动 storage 子边机制与三条 cascade sweep。(更轻的替代:timer 名由 task 名派生、owner 按名反查 —— 隐式,不推荐。)

### 注意

- 判据一句话:**Timer 的归属应跟着「谁负责终结它」走,而不是跟着「语义上它约束谁」走**。在这套引擎里负责终结它的是 Activity 侧的容器与级联扫荡。ASL 层面 `TimeoutSeconds` 确是 Task state 的字段(且 Task 自身的 `deadline` 已镜像它),但这不构成把 timer 挂到 Task 下的充分理由。
- 若最终改挂,必须先补齐「Task 终结路径全覆盖扫自身 timer」的清单并逐条验证,再动 storage。
- 现阶段不做;`Task::claim` / `Task::cancel` / 各 applier 均假定 task 不拥有任何子节点,改动会波及这些不变量。

## 用「从 Storage 重算下一步」取代持久化 follow-up command —— 先做等价性反推盘点

**状态:** 已识别,待评审;**先只做「反推表」盘点,不动实现**。触发条件 = 想简化内部跳转机制(settle 钩子 / `Continue*` 双轨、内部 command 的持久化)时。

### 问题(想简化什么)

CCES 让「决定下一步」与「执行下一步」解耦,并把**决定本身**持久化成 command,换来可恢复 / 可交接 / 可审计。代价是控制流被摊成「一条持久化 command + 下一轮 dispatch」,于是:

- 「子完成后父要推进」这件事有**两套机制**并存:`Container` 的 settle 钩子(`crates/engine/src/elements/mod.rs` 的 `after_child_completed` / `after_child_terminated`)与一跳延迟的 `Command::ContinueComplete` / `ContinueTerminate`(`crates/engine-types/src/types/command.rs`)。读的人得先猜「这次为何不走另一条」。
- 「每条 command 至少一条 entry」不变量(`crates/engine/src/leader.rs` 的 `dispatch_once` `assert!`)对**内部** follow-up command 也生效,而内部 command 根本没有 awaiter(`Command::request_id()` 对它们返回 `None`)。

**想法**:如果恢复时能从 `Storage` 读出最新投影,直接算出「下一个应该执行的点」、执行、把结果写 log、再提交,那么**内部 follow-up command 的持久化可能是多余的**——`settle` 与 `Continue*` 可合并成一条「drive 时重算」,上述不变量可收缩到只对客户端发起的 command 生效。

### 不违反核心不变量(先确立这点)

`Architecture.md §2` 的不变量是「event stream 是唯一真源,`Storage` 必须能由 event 流重建」。上述想法**没动 truth model**,只改「*怎么发现下一步*」:从「读一条持久化 intent」改成「读 state 自己算」。因此这是可接受的演进(k8s reconciler / Temporal 同向),不是推翻 CCES。

### 前提(决定成败的那一条)

那个「下一个应该执行的点」必须是 **`(Storage, 静态定义, clock, ids)` 的纯函数**。今天持久化 command 存在的根本原因,正是有些「下一步」带了 state 里没有的信息:

- 触发它的**具体 `request_id`**(要 ack 给谁);
- 触发它的是**哪个 child** 完成、还剩几个未完成;
- **外部副作用**(给 worker 派活、arm timer、调外部服务)。

分水岭只有一条:**这一步是纯的,还是带外部副作用**。

| 步骤性质 | 可行做法 |
|---|---|
| 纯(state → state+events) | **内存算 → 一次原子 append → 提交**;崩在提交前 = 什么都没发生,重算即可(不需要 WAL) |
| 带外部副作用 | 必须先写意图再动手(否则崩溃重复投递),或要求副作用可从**已提交 event** 重新推导 —— spica 现状正靠 `Hook` 这么做 |

### 代价(诚实记一笔:是「队列」换「扫描」)

持久化 command 让「下一步」是 O(1) pop;reconciler 得从 state 里**找出**该跑的点,于是要长出索引:按 deadline 的 timer 索引、按「容器可否 settle」的索引、按 lease 过期的工作索引。复杂度没蒸发,是从「command log 的语义」搬到「索引维护 + 调度」。另有两个新失效模式:

- **空转**:「下一步」若不单调,naive 循环会反复重算同一件活儿 → 需 state 里能回答「这活儿做过了吗」(每个工作项要 durable 的 done/leased 标记)。
- **谁该跑**:需能回答「哪些对象还没到终态」,又是一个扫描面。

### 候选方案(推荐:先做反推表,不动架构)

**不动实现**:先把「重算」实现为**恢复路径**,让它在稳态之外作为权威;它同时就是「持久化 command 是否多余」的**判据 / 预言机**。

对现存**每一条 follow-up command**逐个回答两问:

1. 它能不能从 `(Storage, 静态定义, clock, ids)` **重新算出来**?
2. 它带没带**外部副作用**?

判读:

- 重算结果 **==** 日志里那条 command → 该 command **冗余**,可去掉持久化,改由 drive 时重算;
- 重算不出 / 算出来**不一致** → 它的决策依赖了 state 外的信息,那就是**必须先补进 state 或 event** 的那份信息(通常意味着给某个 event 补字段,或给某张表补一列)。

这张表做完,「能简化多少」就不是猜的;并顺带回答 `settle` 与 `Continue*` 到底哪个是多余的。

### 注意

- 本条是**盘点 + 设计收敛**,不是直接改;**反推表产出、评审通过前不要动实现**。
- 与本节第一条(`recover_leader` / `run_follower` 折叠)相关:恢复路径正是「重算」的落点,若那条先统一了折回逻辑,重算更容易挂上去。
- 单节点下这不是修 bug(本地 commit == durable,现行为正确),是**简化**;若将来 multi-node,「谁该跑」的索引需与分区 / 领导权切换一起考虑(与上方三条 Zeebe / 链式参考联动)。
- 内联 TODO 与本条同源者,落地时一并收敛。

## (设计参考·备选)ASL → 低层 IR + 自建运行时 —— 「执行半边」的替代终态

**状态:** 设计参考,**未落地**;设计见 `docs/asl-ir-design.md`(已从 `third_party/asl-ir-demo/{asl,scope}` 反推)。触发条件 = 决定为「简化执行半边」付一笔**前置、重写量级**的投入时。

### 与上一条(「用从 Storage 重算下一步」)的关系:不同轴,且互补

- 上一条问「**下一步从哪来**」——读持久化 intent,还是从投影重算。是**驱动**问题。
- 本条问「**状态本身长什么样**」——域对象图,还是 IR 帧(`pc` + 寄存器 + scope + handle)。是**状态模型**问题。

两条不是替代关系:IR 把上一条的前提「下一步必须是 `(Storage, 静态定义, clock, ids)` 的纯函数」从**祈祷**变成**结构性成立**——后继指令就躺在程序里(`pc+1` / `JMP` 目标 / `BR` 分支),不需要*推断*,直接*读出*。

### 它正面命中的复杂度(最强卖点)

- **父子 Terminate/Complete 矩阵**(本册多处痛点的根)被两样通用机制替掉:结构化并发(`docs/asl-ir-design.md` §7.1「a frame may not exit with live children」)+ `SCOPE_CLOSE` 等 ack(§4.3)。cancel 不再需要 runtime 认识 frame 上挂了什么(G3):外部 task / timer / 子 frame 统一为 **handle**,`kinds[kind] = {cleanup actions, NeedAck}` 是**数据表**,取代三段手写级联扫荡(`elements/{execution,thread,activity}` 里的 terminate 扫荡)。
- **顺带解掉上方「`TaskTimeout` Timer 归属」条目**:在 IR 里是伪问题——timer 就是当前 scope 的一个 handle,`SCOPE_CLOSE` 按 `kinds` 统一清理,不需要「谁负责终结它」这个归属决定。
- **G1**:已持久化程序的语义必须能从**指令流**读出(§1.1)。今天 spica 的语义**就是 handler 代码**——升级引擎会静默改变在飞执行的行为;IR 下 ASL 改容差取整是**重新编译**。这是当前引擎给不出的正确性性质。

### 代价:不是「更简单」,是「复杂度换形状 + 前置成本」(三笔账搬家)

1. **编译器成为新的复杂度中心。** 今天散在 `elements/states/*.rs` 的语义(retry backoff、Map 容差算术、error-name 匹配、ItemSelector、timeout/heartbeat)全部搬进 lowering。经典解释器→编译器取舍,但**每个 ASL 特性都要 lowering 对**。`docs/asl-ir-design.md` §9 有 **10 个 OPEN**(tie-break / cleanup landing pad vs scope 走栈 / `pc` 是否持久化 / heartbeat handle 形状 / ItemSelector 逐项还是 hoist / 容差细节 / `MATCH` 住哪……)——575 行、两份参考实现反推过仍留 10 个未决问题,规模可见。
2. **Frame 的持久化 / 重放成为新正确性面。** 跨恢复要持久并还原 `pc`、**带类型标签**的寄存器(§4:不带标签 int 会变 float64)、scope 栈、handle 表、mailbox。§6 的 9 条是新不变量;`Recorder` 要按**调用顺序**记录非确定性函数(`$millis`/`$now`/`$random`/`$uuid`)才能逐字节重放;§8 末尾「in-process 版本藏了五件事」的表就是耐久性坑清单。
3. **查询 / 可观测性失去形状。** 今天 `Storage` 域行**就是对外查询面**(`crates/server/src/query.rs` 把 Execution/Thread/Activity/Timer/Task 行直接转 proto,测试里 ~50 处 fixture 也在断言这些行)。IR 下状态是「frame, pc=37, regs={…}」,`pc` 离开程序不可读 → 要回答「在干什么 / 在等什么 / 目前输出」必须**另建读模型**。而 `docs/asl-ir-design.md` §1.2 明确把 durability / 查询列为 **non-goal** —— 这是**空白**,不是已解决项。

### 危险点(文档自己已标,落地前须先解决)

- **key 在恢复下的唯一与稳定**(§6.7、OPEN-10)。§7.3 直接点名 `scope` 的 `START_TASK` 用 `State/Attempt/task` 做 key,**N 个 item 会碰撞**。拼错的后果 = 重放把两个不同 child 别名到同一行——**正是今天「每对象一个独立 uid」在避免的失效模式**。
- **OPEN-3**(持久化 `pc`,还是从最后挂起点重跑 + 注入取消)与上一条是**同一个决策下沉一格**(持久化 intent vs 重算)——两条讨论在此撞上,应一起定。

### 定义半边不用动(使分期迁移成立)

IR 是**定义(`FlowVersion`)的 lowering**,`Flow`/`FlowVersion` 的存储与语义原封不动,只多一份编译产物(可照今天 `HandlerContext::machine` 的「按需编译 + 每版本缓存」)。即**只替换执行的一半**,不是全盘重写。因此 `docs/asl-ir-design.md` §10 的 S1..S5(Task 起步 → retry/catch → fan-out → 其余 state → ItemBatcher / nested)是一条**真实迁移路线**,而非纸上计划。

### 共享产物:两条路的第一步是同一件

`docs/asl-ir-design.md` §8 的推导规则:

> 对每一行问:**崩在这行之后,必须已经持久化了什么才能继续?** 那个集合就是 IR。

把这条用在**当前的**引擎上,产出的就是上一条要的**反推表**;而表里被标成「**不可推导 / 带了 state 外信息**」的每一项,恰恰就是 **IR 寄存器必须承载的东西**。所以反推表既是「哪些 follow-up command 冗余」的答案,也是「IR 状态模型该装什么」的需求清单——**先做它,两种结局下都不浪费**。

### 结论:复杂度「摊着长」还是「集中长」

| | 现状(域对象图) | IR + 运行时 |
|---|---|---|
| 复杂度形状 | **摊开**(7 个 kind × applier × container × handler × settle/Continue)且**涌现**(要推断) | **集中**(编译器 + 一个解释器 + 一个 frame 模型)且**声明式**(可读出) |
| 成本时机 | 可**增量**改进 | **前置**付清,重写量级 |
| 取消 / 父子 | 手写级联扫荡 + settle/Continue 双轨 | 结构化并发 + handle `kinds` 表(通用) |
| 已持久化程序的语义 | 在 handler 代码里(升级会漂移) | 在指令流里(G1,重新编译) |
| 对外查询 | 域行即查询面 | 需另建读模型(文档未覆盖) |

IR 是收敛更好的**终态**(也正是 durable-execution 领域收敛的方向);代价是前置的、重写量级的投入。**判断标准是「是否愿意付这笔前置成本」,不是技术优劣。**

### 注意

- 本条是**参考 + 决策材料**,不是立即可动的任务;**先产出上一条的反推表,再决定是否立项**。
- 若立项,须先解决 §9 的 10 个 OPEN 与上面两个危险点(尤其 key 唯一性),再进 §10 的 S1。
- 与本册上方三条 Zeebe / 链式参考的关系:那些是 **multi-node 的复制 / 快照**问题,与本条(单节点即可受益的**表示**问题)正交,可独立推进。
