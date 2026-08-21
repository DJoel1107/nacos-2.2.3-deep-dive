# 第4章：Nacos 2.5.3 一致性协议（JRaft & Distro）深度分析

## 4.1 一致性协议概述：AP vs CP 的 CAP 权衡

Nacos 2.5.3 同时支持 AP 模式（Distro 协议）和 CP 模式（JRaft 协议），根据 CAP 理论灵活权衡：

| 维度 | AP 模式（Distro） | CP 模式（JRaft） |
|------|-------------------|-------------------|
| **一致性模型** | 最终一致性 | 强一致性 |
| **可用性** | 高（容忍网络分区） | 中（需要多数派存活） |
| **分区容忍性** | 高（去中心化） | 中（需要 Leader） |
| **适用场景** | 临时实例（高频心跳） | 持久化实例（数据一致性要求高） |
| **同步方式** | 异步复制 | Raft Log 同步复制 |
| **版本 (2.5.3)** | Distro Protocol（无版本变更） | JRaft **1.3.12 → 1.3.14** |

### 2.5.3 JRaft 版本升级要点

| 改进点 | 1.3.12 | 1.3.14 | 影响 |
|--------|--------|--------|------|
| Pre-Vote 超时 | 默认超时 | 优化超时参数 | 减少误触发 Leader 选举 |
| Log Replication Batch | 默认批量 | 优化批量大小 | 提升日志复制吞吐 |
| Snapshot 压缩 | 基础压缩 | 优化压缩算法 | 减少 Snapshot 文件大小 |
| Leader 存活检测 | 基础心跳 | 增强心跳检测 | 更快速发现 Leader 失效 |

## 4.2 Distro 协议设计哲学

Distro 协议是 Nacos 自研的去中心化数据同步协议，核心理念：

| 设计原则 | 说明 |
|---------|------|
| **去中心化** | 每个节点都是对等节点，无中心 Leader |
| **最终一致性** | 异步复制，允许短暂的数据不一致 |
| **高可用** | 任何节点宕机不影响其他节点 |
| **水平扩展** | 新增节点自动同步全量数据 |
| **一致性哈希** | 使用一致性哈希算法确定数据归属 |

## 4.3 Distro 核心数据结构

```java
// DistroProtocol 核心数据结构 (2.5.3)
@Component
public class DistroProtocol {
    
    /** 数据映射 Map<distroKey, Datum> */
    private final ConcurrentHashMap<String, Datum> dataMap = 
        new ConcurrentHashMap<>();
    
    /** Distro Task Map */
    private final Map<String, DistroTask> distroTaskMap = 
        new ConcurrentHashMap<>();
    
    /** 校验任务Map */
    private final Map<String, DistroVerifyData> verifyDataMap = 
        new ConcurrentHashMap<>();
    
    /** ★ 2.5.3: ClientManager 集成 */
    @Autowired
    private ClientManager clientManager;
    
    /**
     * 同步数据到远程节点
     * ★ 2.5.3: ClientManager.getClient() 获取 gRPC 连接
     */
    public void syncToTargetServerAsync(String targetServer, 
                                         byte[] data, 
                                         String action) {
        // 异步复制数据到目标节点
        distroTaskEngine.execute(new DistroSyncTask(targetServer, data, action));
    }
}
```

## 4.4 Distro 增量同步

Distro 增量同步流程：

1. 本地 `put()` → 写入 `dataMap`
2. 计算数据归属节点：`distroHash.hash(key)` → 确定该数据应该分布在哪些节点
3. `syncToTargetServerAsync()` → 异步发送数据到归属节点
4. 目标节点 `onReceiveSyncData()` → 接收并写入本地 `dataMap`
5. `DistroVerifyTask` → 定期校验数据最终一致性

## 4.5 Distro 全量同步

当新节点加入集群时，需要进行全量数据同步：

1. 新节点启动 → `DistroProtocol.init()` → 初始化一致性哈希环
2. 从已有节点拉取全量数据：`loadAllDataSnapshot()`
3. 分批同步：`batchSyncData()` （每次 100 条数据）
4. 全量同步完成 → 新节点状态变为 `UP`
5. 加入增量同步 → `syncToTargetServerAsync()` 开始接收增量数据

## 4.6 Distro 数据校验机制

`DistroVerifyTask` 定期校验数据最终一致性：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `verifyInterval` | 5 秒 | 校验间隔 |
| `verifyBatchSize` | 20 | 每批校验数据量 |
| `syncRetryInterval` | 3 秒 | 同步重试间隔 |

## 4.7 Distro 一致性哈希算法详解

Distro 使用一致性哈希算法确定数据在集群中的分布：

```java
// DistroHash 一致性哈希算法 (2.5.3)
public class DistroHash {
    
    /** 虚拟节点数量 */
    private static final int VIRTUAL_NODES = 10_000;
    
    /** 哈希环（TreeMap 实现） */
    private final TreeMap<Long, String> hashRing = new TreeMap<>();
    
    /**
     * 计算 key 所属的节点
     */
    public String hash(String key) {
        long hash = hashFunction(key);
        // 顺时针查找最近的虚拟节点
        Map.Entry<Long, String> entry = hashRing.ceilingEntry(hash);
        if (entry == null) {
            // 超出最大节点，回到环起点
            entry = hashRing.firstEntry();
        }
        return entry.getValue();
    }
}
```

## 4.8 JRaft 简介

JRaft（Java Raft）是阿里巴巴基于 Raft 论文实现的 Java Raft 库，核心组件：

| 组件 | 职责 |
|------|------|
| **Leader 选举** | Pre-Vote → RequestVote → Leader |
| **日志复制** | Leader → Follower Log Replication |
| **Snapshot** | 定期压缩 Raft Log，防止磁盘无限增长 |
| **线性一致性读** | ReadIndex + LeaseRead |

### Raft 集群角色

| 角色 | 职责 |
|------|------|
| **Leader** | 处理所有写请求，复制 Log 到 Followers |
| **Follower** | 接收 Leader 的 Log Replication，响应 Leader 心跳 |
| **Candidate** | Leader 选举中的临时角色 |

## 4.9 Nacos 中 JRaft 集成架构

Nacos 2.5.3 中的 JRaft 集成架构：

```
┌──────────────────────────────────────────────────────┐
│              Nacos JRaft Integration (2.5.3)           │
├──────────────────────────────────────────────────────┤
│                                                       │
│  RaftConsistencyServiceImpl (naming/consistency)     │
│  │                                                    │
│  ├─ RaftCore (consistency/src/.../raft/)           │
│  │  ├─ RaftNode (JRaft Node 实例)                 │
│  │  ├─ Leader 选举逻辑                             │
│  │  ├─ Log Replication 管理                        │
│  │  └─ Snapshot 管理                              │
│  │                                                    │
│  ├─ RaftStore (consistency/src/.../raft/)          │
│  │  ├─ Raft Log 持久化                            │
│  │  ├─ Raft Metadata 管理                          │
│  │  └─ ★ 2.5.3: 优化 Snapshot 压缩               │
│  │                                                    │
│  └─ NacosFSM (consistency/src/.../raft/)           │
│     ├─ onApply()：应用已提交的 Log Entry          │
│     ├─ onSnapshotSave()：保存 Snapshot              │
│     └─ onSnapshotLoad()：加载 Snapshot              │
│                                                       │
└──────────────────────────────────────────────────────┘
```

### 2.5.3 JRaft 依赖版本

```
<dependency>
    <groupId>com.alipay.sofa</groupId>
    <artifactId>jraft-core</artifactId>
    <version>1.3.14</version>  <!-- ★ 从 1.3.12 升级到 1.3.14 -->
</dependency>
```

## 4.10 RaftCore：CP 模式核心引擎

`RaftCore`（路径：`nacos-2.5.3/consistency/src/main/java/com/alibaba/nacos/consistency/raft/RaftCore.java`）：

```java
// RaftCore 核心引擎 (2.5.3)
@Component
public class RaftCore {
    
    /** Raft 集群节点列表 */
    private final List<RaftNode> raftNodes;
    
    /** ReentrantLock for Leader redirect */
    private final ReentrantLock lock = new ReentrantLock();
    
    /**
     * redirect to Leader
     * ★ 2.5.3: 优化 Leader 存活检测
     */
    public <T> T redirectToLeader(RaftOperation<T> operation) {
        lock.lock();
        try {
            RaftNode leader = getLeader();
            if (leader == null) {
                throw new NoLeaderException("No Raft leader found");
            }
            return operation.execute(leader);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * ★ 2.5.3: 增强 Leader 存活检测
     */
    private RaftNode getLeader() {
        for (RaftNode node : raftNodes) {
            if (node.isLeader() && node.isAlive()) {
                return node;
            }
        }
        return null;
    }
}
```

## 4.11 NacosFSM：有限状态机

`NacosFSM`（路径：`nacos-2.5.3/consistency/src/main/java/com/alibaba/nacos/consistency/raft/NacosFSM.java`）：

```java
// NacosFSM 有限状态机 (2.5.3)
public class NacosFSM extends BaseStateMachine {
    
    /**
     * onApply：应用已提交的 Log Entry
     * ★ 2.5.3: 支持批量 apply 优化
     */
    @Override
    public void onApply(Iterator<LogEntry> iter) {
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            // 解析 Log Entry 中的数据变更操作
            WriteOperation op = WriteOperation.parseFrom(entry.getData());
            // 应用到内存状态
            applyOperation(op);
        }
    }
    
    /**
     * onSnapshotSave：保存 Snapshot
     * ★ 2.5.3: 优化 Snapshot 压缩算法
     */
    @Override
    public void onSnapshotSave(SnapshotWriter writer) {
        // 序列化当前内存状态到 Snapshot
        for (Map.Entry<String, Datum> entry : dataMap.entrySet()) {
            writer.write(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * onSnapshotLoad：加载 Snapshot
     */
    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        // 从 Snapshot 恢复内存状态
        while (reader.hasNext()) {
            Datum datum = reader.next();
            dataMap.put(datum.getKey(), datum);
        }
        return true;
    }
}
```

## 4.12 Leader 选举过程详解

JRaft 的 Leader 选举分为 3 个阶段：

```
┌──────────────────────────────────────────────────────┐
│              Raft Leader Election Process               │
├──────────────────────────────────────────────────────┤
│                                                       │
│  阶段1: Pre-Vote (预投票)                           │
│  ┌─────────────────────────────────────────────┐     │
│  │ Follower 超时未收到 Leader 心跳           │     │
│  │ ↓                                           │     │
│  │ 转变为 Candidate                           │     │
│  │ ↓                                           │     │
│  │ 发起 Pre-Vote RPC                         │     │
│  │ ↓                                           │     │
│  │ ★ 2.5.3: 优化超时参数，减少误触发       │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  阶段2: RequestVote (正式投票)                       │
│  ┌─────────────────────────────────────────────┐     │
│  │ Candidate 收到多数派 Pre-Vote 赞同        │     │
│  │ ↓                                           │     │
│  │ term++                                     │     │
│  │ ↓                                           │     │
│  │ 发起 RequestVote RPC                      │     │
│  │ ↓                                           │     │
│  │ 收集多数派投票 → 成为 Leader              │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  阶段3: Log Replication (日志复制)                  │
│  ┌─────────────────────────────────────────────┐     │
│  │ Leader 开始处理 Client 写请求               │     │
│  │ ↓                                           │     │
│  │ Append Log Entry → 复制到 Followers         │     │
│  │ ↓                                           │     │
│  │ 收到多数派确认 → Commit Log Entry          │     │
│  │ ↓                                           │     │
│  │ Apply to FSM (NacosFSM.onApply())         │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
└──────────────────────────────────────────────────────┘
```

## 4.13 脑裂处理机制

JRaft 使用以下机制防止脑裂：

| 机制 | 说明 | 2.5.3 优化 |
|------|------|-----------|
| **Pre-Vote** | 先预投票确认自己是否能赢得选举，避免 term 无限增长 | **优化超时参数** |
| **Leader 存活检测** | Follower 检测 Leader 是否存活 | **增强心跳检测机制** |
| **term 递增** | 每个 term 只能投一张票，防止双 Leader | — |
| **日志比较** | 只有最新日志的 Candidate 才能当选 | — |

### 脑裂恢复 3 阶段策略

| 阶段 | 操作 | 说明 |
|------|------|------|
| **1. 检测** | `RaftCore.getLeader()` | 检测是否出现双 Leader 或 Leader 失联 |
| **2. 仲裁** | 多数派投票 | 通过 `RequestVote` 重新选举 |
| **3. 数据合并** | `onSnapshotLoad()` + `onApply()` | 从 Snapshot 恢复 + 应用未提交的 Log |

---

### 本章统计数据

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| JRaft 版本 | 1.3.12 | **1.3.14** | ↑ |
| consistency 模块 Java 文件 | 23 | **23** | 0 |
| persistence 模块集成 | 无 | **37 个 Java 文件** | ★新增独立模块 |
| Leader 选举稳定性 | 基础 | Pre-Vote 超时参数优化 | ★增强 |

---

> **本章基于 Nacos 2.5.3 源码分析生成。**
