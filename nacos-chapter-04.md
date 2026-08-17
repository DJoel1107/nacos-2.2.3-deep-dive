# 第4章：一致性协议 (JRaft & Distro) 深度分析

## 4.1 一致性协议概述

Nacos 2.2.3 支持两种一致性模式，通过在注册实例时指定 `ephemeral` 属性来选择：

| 属性 | 模式 | 协议 | 实例类型 | 存储方式 | 适用场景 |
|------|------|------|---------|---------|---------|
| `ephemeral=true` | AP 模式 | Distro | 临时实例 | 内存 + 异步同步 | 服务发现（高可用优先） |
| `ephemeral=false` | CP 模式 | JRaft (SOFAJRaft) | 持久化实例 | Raft Log + Snapshot | DNS/F5 注册（一致性优先） |

### 4.1.1 CAP 权衡策略

```
AP 模式 (Distro):
  - 可用性 > 一致性
  - 节点间异步同步
  - 允许短暂数据不一致
  - 无 Leader 概念，去中心化
  - 适用场景：微服务注册与发现

CP 模式 (JRaft):
  - 一致性 > 可用性
  - Leader-based Replication
  - 多数派确认后才返回成功
  - 保证数据的强一致性
  - 适用场景：关键元数据的持久化存储
```

## 4.2 Distro 协议——AP 模式深度分析

### 4.2.1 Distro 协议的设计哲学

Distro（Distributed Protocol）是 Nacos 自研的去中心化数据同步协议，设计目标：
1. **去中心化**：所有节点平等，没有 Leader 概念
2. **最终一致性**：数据异步同步，允许短暂不一致
3. **高可用**：部分节点故障不影响整体服务
4. **水平扩展**：新节点加入自动同步全量数据

### 4.2.2 Distro 协议的核心数据结构

```java
// DistroConsistencyMapImpl.java - AP 模式存储实现
@Component("distroConsistencyService")
public class DistroConsistencyMapImpl {
    
    // 本地数据存储 Map
    private final ConcurrentHashMap<String, Datum> dataMap = 
        new ConcurrentHashMap<>(1024);
    
    // Distro 数据校验任务
    private final Notifier notifier = new Notifier();
    
    // 全量同步标记
    private volatile boolean isFinishInitial = false;
    
    // Distro 数据同步 Task
    private class DistroSyncTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1. 遍历本地所有数据
                    for (Map.Entry<String, Datum> entry : dataMap.entrySet()) {
                        String key = entry.getKey();
                        Datum datum = entry.getValue();
                        
                        // 2. 根据一致性哈希确定数据归属节点
                        List<Member> targetServers = getTargetServers(key);
                        
                        // 3. 同步到除本地外的目标节点
                        for (Member server : targetServers) {
                            if (server.equals(getCurrentServer())) {
                                continue;
                            }
                            syncToTargetServer(server, key, datum);
                        }
                    }
                    
                    Thread.sleep(DISTRO_SYNC_PERIOD);
                } catch (Exception e) {
                    Loggers.DISTRO.error("Distro sync task error", e);
                }
            }
        }
    }
}
```

### 4.2.3 Distro 数据同步机制

#### 增量同步 (Incremental Sync)

```java
// DistroConsistencyMapImpl.java - 增量同步
public void put(String key, Record value) {
    // 1. 写入本地存储
    Datum datum = new Datum(key, value);
    dataMap.put(key, datum);
    
    // 2. 计算目标节点
    List<Member> targetServers = getTargetServers(key);
    
    // 3. 异步同步到目标节点
    for (Member server : targetServers) {
        if (server.equals(getCurrentServer())) {
            continue;
        }
        syncToTargetServerAsync(server, key, datum);
    }
}

private void syncToTargetServerAsync(Member server, String key, Datum datum) {
    DistroKey distroKey = new DistroKey(key, server.getAddress());
    
    // 构建同步请求
    DistroData distroData = new DistroData(distroKey, datum.getValue());
    DistroDataRequest request = new DistroDataRequest(distroData, 
        DataOperation.ADD);
    
    // 通过 HTTP 异步发送同步请求
    asyncRestTemplate.post(getSyncUrl(server), 
        JacksonUtils.toJson(request),
        new Callback<String>() {
            @Override
            public void onReceive(Result<String> result) {
                if (!result.isOk()) {
                    // 同步失败记录，稍后重试
                    failedSyncQueue.add(new FailedSyncTask(server, key));
                }
            }
        });
}
```

#### 全量同步 (Full Sync)

当新节点加入集群或节点重启后，需要进行全量数据同步：

```java
// DistroConsistencyMapImpl.java - 全量同步
public void fullSyncToNewServer(Member newServer) {
    // 1. 检查节点是否为集群中的活跃成员
    if (!serverMemberManager.isMember(newServer)) {
        return;
    }
    
    // 2. 遍历本地所有数据，打包发送
    List<DistroData> allData = new ArrayList<>();
    for (Map.Entry<String, Datum> entry : dataMap.entrySet()) {
        String key = entry.getKey();
        Datum datum = entry.getValue();
        
        // 只同步归属该新节点的数据
        List<Member> targetServers = getTargetServers(key);
        if (targetServers.contains(newServer)) {
            DistroKey distroKey = new DistroKey(key, newServer.getAddress());
            DistroData distroData = new DistroData(distroKey, datum.getValue());
            allData.add(distroData);
        }
    }
    
    // 3. 分批发送全量数据
    int batchSize = 1000;
    for (int i = 0; i < allData.size(); i += batchSize) {
        List<DistroData> batch = allData.subList(i, 
            Math.min(i + batchSize, allData.size()));
        
        DistroDataRequest request = new DistroDataRequest(batch, 
            DataOperation.FULL_SYNC);
        
        asyncRestTemplate.post(getFullSyncUrl(newServer), 
            JacksonUtils.toJson(request),
            new Callback<String>() {
                @Override
                public void onReceive(Result<String> result) {
                    if (!result.isOk()) {
                        Loggers.DISTRO.error("Full sync to {} failed: {}", 
                            newServer.getAddress(), result.getMessage());
                    }
                }
            });
    }
}
```

### 4.2.4 Distro 数据校验机制

定期校验集群中各节点的数据一致性，确保最终一致性：

```java
// DistroConsistencyMapImpl.Notifier - 数据校验任务
public class Notifier implements Runnable {
    
    // 校验任务周期（默认 2 秒）
    private static final long VERIFY_PERIOD = 2000L;
    
    @Override
    public void run() {
        while (true) {
            try {
                // 1. 获取本地所有数据
                List<String> keys = new ArrayList<>(dataMap.keySet());
                
                for (String key : keys) {
                    // 2. 根据一致性哈希确定数据归属节点
                    List<Member> responsibleServers = getTargetServers(key);
                    
                    for (Member server : responsibleServers) {
                        if (server.equals(getCurrentServer())) {
                            continue;
                        }
                        
                        // 3. 检查目标节点上该 key 的数据版本
                        DistroVerifyData verifyData = getLocalDatum(key);
                        DistroVerifyData remoteVerifyData = fetchVerifyData(server, key);
                        
                        if (remoteVerifyData == null) {
                            // 目标节点缺少该数据，全量同步
                            fullSyncToServer(server, key);
                        } else if (verifyData.getTimestamp() > 
                                remoteVerifyData.getTimestamp()) {
                            // 本地数据更新，增量同步
                            syncToServer(server, key, dataMap.get(key));
                        }
                    }
                }
                
                Thread.sleep(VERIFY_PERIOD);
            } catch (Exception e) {
                Loggers.DISTRO.error("Distro verify task error", e);
            }
        }
    }
}
```

### 4.2.5 Distro 协议的一致性哈希

Distro 协议使用一致性哈希来决定每个 Key 的主要负责节点：

```java
// DistroHash.java - 一致性哈希算法
public class DistroHash {
    
    // 虚拟节点数（用于防止哈希倾斜）
    private static final int VIRTUAL_NODES = 3;
    
    // SortedMap 实现一致性哈希环
    private final SortedMap<Integer, String> hashRing = 
        new TreeMap<>();
    
    public DistroHash(List<Member> serverList) {
        for (Member member : serverList) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                String virtualNodeName = member.getAddress() + "#" + i;
                int hash = hash(virtualNodeName);
                hashRing.put(hash, member.getAddress());
            }
        }
    }
    
    /**
     * 根据 Key 查找负责的服务器
     */
    public String getServer(String key) {
        int hash = hash(key);
        // 顺时针查找第一个大于该 hash 的虚拟节点
        SortedMap<Integer, String> tailMap = hashRing.tailMap(hash);
        if (tailMap.isEmpty()) {
            // 超出哈希环末端，回到环首
            return hashRing.get(hashRing.firstKey());
        }
        return tailMap.get(tailMap.firstKey());
    }
    
    // Java String hashCode
    private int hash(String key) {
        return Math.abs(key.hashCode());
    }
}
```

## 4.3 JRaft 协议——CP 模式深度分析

### 4.3.1 JRaft 简介

Nacos 2.2.3 的 CP 模式基于 SOFAJRaft（蚂蚁金融级 Raft 实现），是阿里巴巴开源的 Java 版 Raft 共识算法实现。

**JRaft 核心特性：**
- **Leader 选举**：基于 Pre-Vote 的 Raft 选举算法，防止网络分区时的反复选举
- **日志复制**：Pipeline 批量复制 + 异步应用，高吞吐低延迟
- **Snapshot**：定期生成快照，压缩 Raft Log 大小
- **线性一致性读**：ReadIndex + LeaseRead 优化读性能
- **Non-Voting Node**：支持 Learner/Follower + Witness 节点类型

### 4.3.2 Nacos 中 JRaft 的集成架构

```
┌─────────────────────────────────────────────────────────┐
│                 PersistentConsistencyService               │
│                    (CP 模式入口)                         │
├─────────────────────────────────────────────────────────┤
│                    RaftConsistencyServiceImpl            │
│  ┌───────────────────────────────────────────────┐    │
│  │               RaftCore                         │    │
│  │  ┌──────────────────────────────────────────┐ │    │
│  │  │         RaftNode (JRaft Node)           │ │    │
│  │  │  ┌────────────────────────────────────┐  │ │    │
│  │  │  │    Leader Election (Pre-Vote)     │  │ │    │
│  │  │  ├────────────────────────────────────┤  │ │    │
│  │  │  │    Log Replication (Pipeline)     │  │ │    │
│  │  │  ├────────────────────────────────────┤  │ │    │
│  │  │  │    Snapshot Generator             │  │ │    │
│  │  │  ├────────────────────────────────────┤  │ │    │
│  │  │  │    StateMachine (FSM)            │  │ │    │
│  │  │  └────────────────────────────────────┘  │ │    │
│  │  └──────────────────────────────────────────┘ │    │
│  └───────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────┤
│                    RaftStore                          │
│    - Raft Log (RocksDB)                             │
│    - Snapshot (本地文件)                             │
└─────────────────────────────────────────────────────────┘
```

### 4.3.3 RaftCore——CP 模式核心引擎

```java
// RaftCore.java - CP 模式核心实现
@DependsOn("ProtocolManager")
@Component
public class RaftCore {
    
    // Raft 集群节点
    private Node raftNode;
    
    // Raft 日志存储
    private final RaftStore raftStore = new RaftStore();
    
    // 全局锁，保证 CP 数据写入串行化
    private final ReentrantLock lock = new ReentrantLock();
    
    @PostConstruct
    public void init() throws Exception {
        // 1. 获取集群成员列表
        List<Member> members = serverMemberManager.getServerList();
        
        // 2. 构建 PeerId 列表
        List<PeerId> peerIds = members.stream()
            .map(m -> new PeerId(m.getIp(), 
                EnvUtil.getPort() + Constants.NACOS_RAFT_PORT_OFFSET))
            .collect(Collectors.toList());
        
        // 3. 创建 Raft 配置
        Configuration conf = new Configuration();
        conf.setPeers(peerIds);
        
        // 4. 获取当前节点的 PeerId
        PeerId selfPeerId = peerIds.stream()
            .filter(p -> p.getEndpoint().getIp().equals(NetUtils.getLocalIP()))
            .findFirst()
            .orElseThrow(() -> new RuntimeException(
                "Cannot find self in cluster members"));
        
        // 5. 创建 Raft 节点
        RaftGroupService raftGroupService = RaftServiceFactory.createRaftGroupService(
            "nacos_cp",
            selfPeerId,
            conf,
            new NacosFSM(raftStore));
        
        raftNode = raftGroupService.getRaftNode();
        
        // 6. 启动 Raft 节点
        raftNode.start();
        
        // 7. 等待 Leader 选举完成
        raftNode.awaitLeaderElection(LEADER_ELECTION_TIMEOUT);
    }
    
    /**
     * 提交数据到 CP 模式
     */
    public Response onApply(WriteRequest request) {
        lock.lock();
        try {
            // 1. 检查当前节点是否为 Leader
            if (!raftNode.isLeader()) {
                // 如果不是 Leader，转发到 Leader 节点
                PeerId leaderId = raftNode.getLeaderId();
                request.setRedirect(true);
                return redirectToLeader(request, leaderId);
            }
            
            // 2. 构建 JRaft Task
            Task task = new Task();
            task.setData(ByteBuffer.wrap(JacksonUtils.toJsonBytes(request)));
            
            // 3. 提交到 Raft 日志
            Clousure closure = new Closure() {
                Response response;
                @Override
                public void run(Status status) {
                    if (status.isOk()) {
                        response = Response.ok(request.getData());
                    } else {
                        response = Response.error(status.getErrorMsg());
                    }
                }
            };
            
            raftNode.apply(task, closure);
            
            // 4. 等待多数派确认
            return closure.await(CP_TIMEOUT, TimeUnit.MILLISECONDS);
            } finally {
            lock.unlock();
        }
    }
}
```

### 4.3.4 NacosFSM——有限状态机

```java
// NacosFSM.java - CP 模式状态机
public class NacosFSM extends StateMachineAdapter {
    
    private final RaftStore raftStore;
    
    public NacosFSM(RaftStore raftStore) {
        this.raftStore = raftStore;
    }
    
    /**
     * 将 Raft Log 应用到状态机
     */
    @Override
    public void onApply(Iterator<Ballot> iter) {
        while (iter.hasNext()) {
            Ballot ballot = iter.next();
            byte[] data = ballot.getData().array();
            WriteRequest request = JacksonUtils.toObj(data, WriteRequest.class);
            
            switch (request.getOperation()) {
                case PUT:
                    // 写入本地存储
                    Datum datum = request.getDatum();
                    raftStore.put(datum.getKey(), datum.getValue());
                    break;
                case REMOVE:
                    // 从本地存储删除
                    raftStore.remove(request.getKey());
                    break;
                case SNAPSHOT:
                    // Snapshot 加载
                    raftStore.loadSnapshot(request.getData());
                    break;
            }
        }
    }
    
    /**
     * 生成 Snapshot
     */
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        // 将当前状态机数据序列化写入 Snapshot
        Map<String, Datum> allData = raftStore.getAllData();
        byte[] snapshotData = JacksonUtils.toJsonBytes(allData);
        writer.addFile(SNAPSHOT_FILE, new ByteArrayInputStream(snapshotData));
        done.run(Status.OK());
    }
    
    /**
     * 加载 Snapshot
     */
    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            byte[] snapshotData = reader.getFile(SNAPSHOT_FILE);
            Map<String, Datum> allData = JacksonUtils.toObj(snapshotData, 
                new TypeReference<Map<String, Datum>>() {});
            raftStore.replaceAll(allData);
            return true;
        } catch (Exception e) {
            Loggers.CORE.error("Failed to load snapshot", e);
            return false;
        }
    }
}
```

### 4.3.5 Leader 选举过程

```
Nacos 集群启动时JRaftLeader选举流程：

Phase 1: Pre-Vote (预投票)
  ┌────────────────────────────────────────────────────┐
  │ Node A (Follower) → RequestPreVote                  │
  │ Node B (Follower) → Grant PreVote                  │
  │ Node C (Follower) → Grant PreVote                  │
  │ => Node A 获得多数派 Pre-Vote → 晋升为 Candidate  │
  └────────────────────────────────────────────────────┘

Phase 2: RequestVote (正式投票)
  ┌────────────────────────────────────────────────────┐
  │ Node A (Candidate) → RequestVote (term=1)           │
  │ Node B (Follower) → Grant Vote (term=1)            │
  │ Node C (Follower) → Grant Vote (term=1)            │
  │ => Node A 获得多数票 → 晋升为 Leader              │
  └────────────────────────────────────────────────────┘

Phase 3: Log Replication
  ┌────────────────────────────────────────────────────┐
  │ Leader (Node A)                                   │
  │    │                                              │
  │    ├→ AppendEntries ( logIndex=1 ) → Node B      │
  │    ├→ AppendEntries ( logIndex=1 ) → Node C      │
  │    │                                              │
  │    ├← Success ACK from Node B                      │
  │    ├← Success ACK from Node C                      │
  │    => commitIndex=1 (majority confirmed)            │
  └────────────────────────────────────────────────────┘
```

### 4.3.6 脑裂处理机制

JRaft 的 Pre-Vote 机制防止网络分区时反复选举：

```java
// RaftNode.java - Pre-Vote 机制
protected void handlePreVoteRequest(PreVoteRequest request) {
    // 1. 检查 term
    if (request.getTerm() <= getCurrentTerm()) {
        // 拒绝低 term 的 Pre-Vote 请求
        respond(new PreVoteResponse(false, "lower term"));
        return;
    }
    
    // 2. 检查 Leader 是否存活
    if (isLeaderAlive()) {
        // 如果 Leader 仍然存活（最近收到了 Leader 的心跳）
        // 拒绝 Pre-Vote，防止反复选举
        respond(new PreVoteResponse(false, "leader alive"));
        return;
    }
    
    // 3. 同意 Pre-Vote
    respond(new PreVoteResponse(true, "granted"));
}
```

## 4.4 AP vs CP 模式选择策略

### 4.4.1 模式选择决策树

```
需要持久化存储？
  ├── Yes → CP 模式 (ephemeral=false)
  │     ├── DNS/F5 注册
  │     ├── 关键业务元数据
  │     └── 低频变更配置
  │
  └── No → AP 模式 (ephemeral=true)
        ├── 微服务注册/发现
        ├── 高频心跳健康检查
        └── 瞬时状态数据
```

### 4.4.2 实例属性对比

| 属性 | AP (临时实例) | CP (持久化实例) |
|------|--------------|----------------|
| 注册速度 | 快（仅内存写入） | 较慢（需 Raft 多数派确认） |
| 数据持久化 | 否（重启丢失） | 是（磁盘持久化） |
| 一致性保证 | 最终一致性（秒级） | 强一致性 |
| 集群最小节点数 | 1 个 | 3 个（保证可用） |
| 故障恢复 | 自动心跳淘汰 | 手动或 Leader 剔除 |
| 适用场景 | 微服务自动注册 | DNS/F5 手动注册 |

---

*（第四章完，约 1.8 万字）*
