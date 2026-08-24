# 第5章：Nacos 2.5.3 集群管理与客户端 SDK 深度分析

### 5.1.1 设计背景

Nacos 集群管理（Core Module）是 Nacos 2.5.3 服务端架构的核心模块之一，负责**集群成员发现、健康状态维护、节点间通信**三大职能。在分布式系统中，每个节点必须知道自己属于哪个集群、集群中有哪些成员、各成员的当前状态（UP/DOWN），才能正确参与一致性协议（参见第 4 章）、服务注册发现（参见第 2 章）和配置管理（参见第 3 章）。

Nacos 2.5.3 的集群管理不同于传统的静态配置方式，它支持**三种集群寻址模式**：
1. **配置文件模式（FileConfigMemberLookup）**：从 `cluster.conf` 文件读取成员列表，适合静态集群部署。
2. **地址服务器模式（AddressServerMemberLookup）**：向外部地址服务器定期 HTTP 查询成员列表，适合动态扩缩容场景。
3. **单机模式（StandaloneMemberLookup）**：仅包含本机 IP + 端口，适合开发与测试环境。

这三种模式通过 `LookupFactory` 工厂类统一创建，上层 `ServerMemberManager` 通过 `MemberLookup` 接口与具体实现解耦。

### 5.1.2 核心类关系图

图 5-1 展示了 Nacos 2.5.3 集群管理模块的核心类关系：

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      ServerMemberManager                                  │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │ memberTable: ConcurrentMap<String, Member>                        │  │
│  │ serverList: ConcurrentSkipListMap<String, Member>                │  │
│  │ nodeReportTasks: Map<String, Task>                              │  │
│  │ memberLookup: MemberLookup   ←────────────────────────┐         │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│  │ init(): 启动流程                                                  │  │
│  │   ├─ memberLookup.start()  ←── 启动成员发现                   │  │
│  │   ├─ GlobalExecutor.scheduleMemberInfoReportTask()                │  │
│  │   └─ NotifyCenter.registerSubscriber(this)                       │  │
│  │ memberChange(members): 处理成员变更                             │  │
│  │ getServerList(): 获取健康成员列表                              │  │
│  │ getSelf(): 获取本机 Member                                     │  │
│  │ allMembers(): 获取全部成员（含非健康）                         │  │
└──────────────────────────────────────────────────────────────────────────┘
        │                                      ▲
        │ MemberLookup                         │ MembersChangeEvent
        ▼                                      │
┌──────────────────────────┐         ┌──────────────────────────┐
│   <<interface>>         │         │   Member                 │
│   MemberLookup          │         │   ├─ ip: String         │
│   ├─ start()           │         │   ├─ port: int         │
│   ├─ destroy()         │         │   ├─ state: NodeState  │
│   ├─ afterLookup()     │         │   ├─ extendInfo: Map  │
│   └─ useAddressServer() │         │   └─ getAddress()    │
└──────────────────────────┘         └──────────────────────────┘
        △                                          △
        │                                          │
┌───────┴────────────────────────────┐    ┌───────┴─────────┐
│        LookupFactory              │    │   NodeState     │
│  ├─ createLookup(ServerEnv)    │    │   UP / DOWN /   │
│  └─ 返回具体 MemberLookup 实现 │    │   SUSPECT       │
└────────────────────────────────┘    └─────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│  具体 MemberLookup 实现                                    │
│  ├─ FileConfigMemberLookup      ← cluster.conf 定期读取  │
│  ├─ AddressServerMemberLookup   ← HTTP API 定期查询     │
│  └─ StandaloneMemberLookup      ← 本机 IP + 端口         │
└────────────────────────────────────────────────────────────┘
```

### 5.1.3 源码走读

#### 5.1.3.1 ServerMemberManager：集群成员管理器

`ServerMemberManager`（`core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java:48-330`）是 Core 模块的 `@Service` 组件，持有集群全部成员信息并提供成员变更通知机制。核心数据结构：

```java
// ServerMemberManager.java:58-65
private final ConcurrentMap<String, Member> memberTable = new ConcurrentHashMap<>();
private final ConcurrentMap<String, Member> serverList = new ConcurrentSkipListMap<>();
private MemberLookup memberLookup;
private volatile Member self;
```

**`init()` 初始化流程（`ServerMemberManager.java:92-115`）**：
```java
protected void init() throws NacosException {
    // 1. 创建 Lookup 实例（根据 nacos.member.lookup.type 配置）
    memberLookup = LookupFactory.createLookUp(serverEnv);
    // 2. 注入 MemberChangeListener 回调——当 Lookup 发现成员变更时通知
    memberLookup.injectMemberChangeListener(this);
    // 3. 启动成员发现
    memberLookup.start();
    // 4. 注册本机 Member（self）
    self = MemberUtil.singleParse(serverEnv);
    serverList.put(self.getAddress(), self);
    // 5. 注册 MembersChangeEvent 事件订阅（NotifyCenter）
    NotifyCenter.registerSubscriber(this);
    // 6. 启动节点信息上报定时任务
    GlobalExecutor.scheduleMemberInfoReportTask(new MemberInfoReportTask());
}
```

**成员变更处理 `memberChange()`（`ServerMemberManager.java:150-207`）**：当 `MemberLookup` 发现成员列表变更时回调此方法。核心逻辑：
1. 计算 `newMembers` 与 `oldMembers` 的差异集（新增/移除）
2. 更新 `memberTable` 和 `serverList`
3. 触发 `MembersChangeEvent` 事件——通过 `NotifyCenter` 发布，通知所有订阅者（包括 `JRaftProtocol`、`DistroProtocol` 等一致性组件）

**健康列表 `getServerList()`（`ServerMemberManager.java:260-268`）**：返回当前所有状态为 `UP` 的成员列表，用于 Distro v2 分布计算（参见 4.7 节）。

#### 5.1.3.2 LookupFactory：三种集群寻址模式工厂

`LookupFactory.createLookUp()`（`core/src/main/java/com/alibaba/nacos/core/cluster/lookup/LookupFactory.java:42-62`）通过工厂方法模式创建具体的 `MemberLookup` 实现：

```java
public static MemberLookup createLookUp(ServerEnv env) throws NacosException {
    // 根据 nacos.member.lookup.type 配置决定实现类
    String lookupType = EnvUtil.getProperty(MEMBER_LOOKUP_TYPE);
    if (StringUtils.isBlank(lookupType)) {
        // 默认：单机模式
        return new StandaloneMemberLookup(env);
    }
    switch (lookupType) {
        case "file":
            return new FileConfigMemberLookup(env);
        case "address-server":
            return new AddressServerMemberLookup(env);
        default:
            return new StandaloneMemberLookup(env);
    }
}
```

### 5.1.4 设计模式分析

1. **工厂方法模式（Factory Method）**：`LookupFactory.createLookUp()` 根据配置参数创建不同的 `MemberLookup` 实现，客户端无需知道具体实现类，符合开闭原则——新增寻址模式只需扩展新的 `MemberLookup` 实现而不修改工厂代码。

2. **观察者模式（Observer）**：`ServerMemberManager` 通过 `NotifyCenter` 发布 `MembersChangeEvent`，`JRaftProtocol`、`DistroProtocol` 等一致性组件订阅此事件。当成员变更时，所有订阅者自动收到通知并执行对应回调（如 JRaft 的 `memberChange()` 触发 peerChange）。

3. **策略模式（Strategy）**：`MemberLookup` 接口定义统一的成员发现契约，三种实现（`FileConfigMemberLookup`、`AddressServerMemberLookup`、`StandaloneMemberLookup`）各自封装不同的发现策略，运行时根据配置动态选择。

### 5.1.5 Trade-off 分析

| 权衡维度 | 配置文件模式 | 地址服务器模式 | 单机模式 |
|---------|------------|--------------|---------|
| **动态扩缩容** | ❌ 需手动修改 cluster.conf + 重启 | ✅ HTTP API 实时推送 | N/A |
| **外部依赖** | 无 | 依赖地址服务器可用性 | 无 |
| **运维复杂度** | 低（静态文件） | 中（需维护地址服务器） | 极低 |
| **故障影响** | 成员变更需重启生效 | 地址服务器不可用时无法感知成员变更 | 单节点故障即整体不可用 |
| **适用场景** | 固定规模集群 | 弹性扩缩容生产集群 | 开发/测试环境 |

### 5.1.6 小结

Nacos 2.5.3 集群管理模块通过 `ServerMemberManager` 统一管理集群成员生命周期，通过 `LookupFactory` 工厂模式支持三种寻址模式。`MemberLookup` 接口解耦了成员发现策略与上层业务逻辑，`MembersChangeEvent` 通过 `NotifyCenter` 事件总线驱动一致性组件（JRaft/Distro）的拓扑感知。集群管理的详细成员发现机制在 5.需要一个 5.4 节展开，通信代理在 5.8 节展开。


### 5.2.1 设计背景

`ServerMemberManager` 需要维护集群全部成员的状态信息，`Member` 模型是集群成员信息的基本载体。一个 `Member` 包含：
- **网络标识**：`ip` + `port`（唯一标识一个节点）
- **健康状态**：`NodeState` 枚举（`UP`/`DOWN`/`SUSPECT`）
- **扩展信息**：`extendInfo`（Map 结构，存储 Raft 元数据、版本号等扩展属性）

`NodeState` 状态机是集群健康管理的核心：节点从 `UP`→`SUSPECT`→`DOWN` 的状态转换，直接影响一致性协议的 Leader 选举（参见 4.12 节）和 Distro v2 的 distribution map 计算（参见 4.7 节）。

### 5.2.2 核心类关系图

图 5-2 展示了 Member 模型与 ServerMemberManager 的协作关系：

```
┌──────────────────────────────────────────────────────────┐
│                 ServerMemberManager                      │
│  ┌────────────────────────────────────────────────────┐  │
│  │ memberTable: ConcurrentMap<String, Member>        │  │
│  │   ├─ "192.168.1.1:8848" → Member(UP)          │  │
│  │   ├─ "192.168.1.2:8848" → Member(UP)          │  │
│  │   └─ "192.168.1.3:8848" → Member(SUSPECT)    │  │
│  │                                                 │  │
│  │ serverList: ConcurrentSkipListMap<>, Member>    │  │
│  │   └─ 仅包含 state=UP 的成员（有序）             │  │
│  └────────────────────────────────────────────────────┘  │
│  getServerList() → List<Member>  ← DistroMapper    │
│  getSelf() → Member                                 │
│  allMembers() → Collection<Member>                   │
│  updateMember(member) → 更新成员状态               │
└──────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────┐
│                   Member                              │
│  ├─ ip: String        ← 节点 IP                     │
│  ├─ port: int         ← 节点端口                   │
│  ├─ state: NodeState  ← UP / DOWN / SUSPECT       │
│  ├─ extendInfo: Map<String, Object>                 │
│  │    ├─ raftMetaData: ProtocolMetaData             │
│  │    └─ version: String                            │
│  ├─ getAddress() → "ip:port"                      │
│  └─ getIp() / getPort()                           │
└────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────┐
│                 NodeState (enum)                      │
│  ├─ UP       → 健康，参与 Distro/JRaft           │
│  ├─ DOWN     → 故障，从 serverList 移除          │
│  └─ SUSPECT  → 疑似故障，保留在 serverList       │
└────────────────────────────────────────────────────────┘
```

### 5.2.3 源码走读

#### 5.2.3.1 Member 数据结构

`Member`（`core/src/main/java/com/alibaba/nacos/core/cluster/Member.java:38-265`）核心字段：

```java
// Member.java:45-56
private String ip;
private int port;
private volatile NodeState state = NodeState.UP;  // 默认 UP
private Map<String, Object> extendInfo = new ConcurrentHashMap<>();
```

**`getAddress()`（`Member.java:108-110`）**：返回 `ip:port` 格式的地址字符串，作为 `memberTable` 和 `serverList` 的 key。

**`setState()`（`Member.java:130-138`）**：线程安全地更新节点状态，仅在状态真正变更时才触发更新。

#### 5.2.3.2 NodeState 状态机

`NodeState`（`core/src/main/java/com/alibaba/nacos/core/cluster/NodeState.java:28-54`）：

```java
public enum NodeState {
    UP,       // 健康状态：参与集群所有操作
    DOWN,     // 故障状态：从健康列表移除
    SUSPECT   // 疑似故障：仍在健康列表中，但标记为可疑
}
```

状态转换规则：
- **UP → SUSPECT**：连续心跳超时但未达到 DOWN 阈值
- **SUSPECT → UP**：心跳恢复
- **SUSPECT → DOWN**：心跳持续超时达到 DOWN 阈值
- **DOWN → UP**：节点重新注册并心跳恢复

#### 5.2.3.3 ServerMemberManager 成员更新

**`memberChange(Collection<Member>)`（`ServerMemberManager.java:150-207`）**：

```java
public synchronized void memberChange(Collection<Member> members) {
    Set<String> newMemberKeys = members.stream().map(Member::getAddress).collect(Collectors.toSet());
    Set<String> oldMemberKeys = allMembers().stream().map(Member::getAddress).collect(Collectors.toSet());
    
    // 计算新增成员
    Set<String> addedKeys = new HashSet<>(newMemberKeys);
    addedKeys.removeAll(oldMemberKeys);
    // 计算移除成员
    Set<String> removedKeys = new HashSet<>(oldMemberKeys);
    removedKeys.removeAll(newMemberKeys);
    
    // 更新 memberTable
    for (Member member : members) {
        memberTable.put(member.getAddress(), member);
    }
    // 移除已离场成员
    for (String key : removedKeys) {
        memberTable.remove(key);
    }
    // 更新健康列表
    serverList.clear();
    serverList.putAll(memberTable.entrySet().stream()
        .filter(e -> e.getValue().getState() == NodeState.UP)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    
    // 发布 MembersChangeEvent 事件
    MembersChangeEvent event = MembersChangeEvent.builder()
        .members(allMembers())
        .addedMembers(addedKeys.stream().map(memberTable::get).collect(Collectors.toList()))
        .removedMembers(removedKeys.stream().collect(Collectors.toList()))
        .build();
    NotifyCenter.publishEvent(event);
}
```

**`updateMember(Member)`（`ServerMemberManager.java:220-240`）**：单个成员状态更新。当心跳检测发现成员状态变更时调用，更新 `memberTable` 中的对应 `Member` 对象并同步 `serverList`。

#### 5.2.3.4 MemberUtil 工具方法

`MemberUtil.singleParse()`（`core/src/main/java/com/alibaba/nacos/core/cluster/MemberUtil.java:42-68`）：从 `ServerEnv` 中提取本机 IP 和端口，构造本机 `Member` 对象。IP 获取优先级：`nacos.inetutils.ip-address` 配置 → `InetAddress.getLocalHost().getHostAddress()`。

### 5.2.4 设计模式分析

1. **观察者模式（Observer）**：`ServerMemberManager.memberChange()` 通过 `NotifyCenter.publishEvent()` 发布 `MembersChangeEvent`。`JRaftProtocol`、`DistroProtocol`、`ClusterRpcClientProxy` 等组件订阅此事件，实现成员变更的自动感知。

2. **不可变快照模式（Immutable Snapshot）**：`getServerList()` 返回 `serverList` 的快照副本（`new ArrayList<>(serverList.values())`），避免并发修改异常，同时保证 DistroMapper 等消费者拿到一致的健康列表视图。

3. **状态模式（State）**：`NodeState` 枚举 + `Member.setState()` 方法实现节点的状态机转换，不同状态下节点的行为不同（UP 参与集群操作、DOWN 从健康列表移除、SUSPECT 保留但标记可疑）。

### 5.2.5 Trade-off 分析

| 权衡维度 | 设计决策 | 收益 | 代价 |
|---------|---------|------|------|
| **ConcurrentMap vs synchronized** | memberTable 使用 ConcurrentMap | 高并发读性能 | 写操作需全量替换 serverList |
| **SUSPECT 中间状态** | 引入 SUSPECT 状态避免误判 DOWN | 减少误下线导致的集群震荡 | 增加状态复杂度 |
| **event vs 直接调用** | 通过 NotifyCenter 事件通知而非直接调用 | 解耦成员变更与业务处理 | 事件异步处理可能延迟 |
| **全量替换 vs 增量更新** | memberChange() 全量替换 serverList | 逻辑简单不易出错 | 成员数多时重建开销大 |

### 5.2.6 小结

`ServerMemberManager` 通过 `memberTable`（全量成员）和 `serverList`（健康成员）双层 Map 结构管理集群成员。`Member` 模型通过 `NodeState` 状态机（UP→SUSPECT→DOWN）驱动成员健康状态转换。`memberChange()` 通过 `NotifyCenter` 事件总线通知一致性组件感知拓扑变更。集群成员的具体发现机制在 5.3-5.4 节展开。
### 5.3.1 设计背景

Nacos 集群需要一种统一的成员发现机制，使每个节点在启动时能自动发现集群中的其他成员。为了实现这一目标，Nacos 2.5.3 通过 `LookupFactory` 工厂类封装三种互斥的寻址模式，运行时根据 `nacos.member.lookup.type` 配置动态选择具体实现。这种设计使得集群部署方式（静态/动态/单机）与成员发现逻辑完全解耦。

### 5.3.2 核心类关系图

图 5-3 展示了 `LookupFactory` 工厂模式与三种 `MemberLookup` 实现的关系：

```
┌────────────────────────────────────────────────────────────────┐
│                    LookupFactory                             │
│  createLookUp(ServerEnv): MemberLookup                    │
│    ├─ lookupType = EnvUtil.getProperty("nacos.member     │
│    │                .lookup.type")                        │
│    ├─ case "file"          → new FileConfigMemberLookup   │
│    ├─ case "address-server" → new AddressServerMemberLookup│
│    └─ default              → new StandaloneMemberLookup   │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│              <<interface>> MemberLookup                    │
│  ├─ start()                    ← 启动成员发现           │
│  ├─ destroy()                 ← 停止成员发现           │
│  ├─ afterLookup(List<Member>) ← 发现成员后回调       │
│  ├─ useAddressServer(): boolean                          │
│  └─ injectMemberChangeListener(listener)                │
└──────────────────────────────────────────────────────────────┘
        △                    △                    △
        │                    │                    │
┌───────┴────────┐ ┌───────┴────────┐ ┌───────┴────────┐
│ FileConfig     │ │ AddressServer  │ │ Standalone     │
│ MemberLookup   │ │ MemberLookup   │ │ MemberLookup   │
│ ├─ watcher    │ │ ├─ restTemplate│ │ ├─ self Member │
│ └─ cluster.conf│ │ └─ domainName  │ │ └─ local IP    │
└────────────────┘ └────────────────┘ └────────────────┘
        △
        │
┌───────┴────────────────────────────────────────────────────┐
│              AbstractMemberLookup                         │
│  ├─ serverMemberManager: ServerMemberManager             │
│  ├─ afterLookup(members) → serverMemberManager         │
│  │       .memberChange(members)                         │
│  └─ MemberUtil.readServerConf(reader) → List<Member>  │
└──────────────────────────────────────────────────────────┘
```

### 5.3.3 源码走读

#### 5.3.3.1 LookupFactory.createLookUp()

`LookupFactory.createLookUp()`（`core/src/main/java/com/alibaba/nacos/core/cluster/lookup/LookupFactory.java:42-62`）是纯工厂方法，根据 `nacos.member.lookup.type` 配置项决定返回哪个 `MemberLookup` 实现：

```java
public static MemberLookup createLookUp(ServerEnv env) throws NacosException {
    String lookupType = EnvUtil.getProperty(
        LookupFactory.MEMBER_LOOKUP_TYPE);  // key = "nacos.member.lookup.type"
    if (StringUtils.isBlank(lookupType)) {
        return new StandaloneMemberLookup(env);  // 默认单机模式
    }
    switch (lookupType) {
        case "file":
            return new FileConfigMemberLookup(env);
        case "address-server":
            return new AddressServerMemberLookup(env);
        default:
            return new StandaloneMemberLookup(env);
    }
}
```

#### 5.3.3.2 AbstractMemberLookup 基类

`AbstractMemberLookup`（`core/src/main/java/com/alibaba/nacos/core/cluster/AbstractMemberLookup.java:35-端）是所有 MemberLookup 实现的抽象基类，持有 `ServerMemberManager` 引用并提供 `afterLookup(List<Member>)` 模板方法：

```java
protected void afterLookup(List<Member> members) {
    this.serverMemberManager.memberChange(members);
}
```

所有具体实现只需调用 `afterLookup(members)` 即可将发现的成员列表通知 `ServerMemberManager`，触发 `memberChange()` → `MembersChangeEvent` 事件链路。

#### 5.3.3.3 MemberLookup 接口定义

`MemberLookup`（`core/src/main/java/com/alibaba/nacos/core/cluster/MemberLookup.java:29-55`）定义统一的成员发现契约：

- `start()`：启动成员发现逻辑
- `destroy()`：停止成员发现，清理资源
- `useAddressServer()`：是否使用地址服务器模式
- `injectMemberChangeListener(listener)`：注入成员变更监听器

### 5.3.4 设计模式分析

1. **工厂方法模式（Factory Method）**：`LookupFactory.createLookUp()` 封装对象创建逻辑，客户端无需知道具体实现类，符合开闭原则——添加新的寻址模式只需新增 `MemberLookup` 实现和对应的 `case` 分支。

2. **模板方法模式（Template Method）**：`AbstractMemberLookup.afterLookup()` 定义统一的 "发现成员→通知 ServerMemberManager" 流程骨架，具体子类实现各自的 `doStart()` 方法完成实际的成员发现逻辑。

3. **策略模式（Strategy）**：`MemberLookup` 接口定义统一契约，三种实现各自封装不同的成员发现算法，运行时通过配置动态切换策略。

### 5.3.5 Trade-off 分析

| 权衡维度 | 工厂模式 | 直接构造 |
|---------|---------|---------|
| **扩展性** | 新增寻址模式只需新增实现类 + case 分支 | 需修改所有构造调用点 |
| **配置灵活性** | 运行时通过配置切换 | 编译时绑定 |
| **代码复杂度** | 增加工厂类 + switch 分支 | 简单直接 |

### 5.3.6 小结

`LookupFactory` 通过工厂方法模式封装三种集群寻址模式的创建逻辑，`MemberLookup` 接口定义统一的成员发现契约，`AbstractMemberLookup` 模板基类封装 "发现→通知" 的标准流程。三种具体实现分别在 5.4（FileConfigMemberLookup）、5.5（AddressServerMemberLookup）、5.6（StandaloneMemberLookup）中展开。


### 5.4.1 设计背景

在静态集群部署场景中，集群成员列表通常配置在一个固定文件中（如 `cluster.conf`），成员变更需要运维手动修改文件。`FileConfigMemberLookup` 通过 `WatchFileCenter` 文件监控机制定期读取 `cluster.conf` 文件内容，当文件内容变更时自动触发成员列表更新。

### 5.4.2 核心类关系图

图 5-4 展示了 `FileConfigMemberLookup` 基于 `WatchFileCenter` 文件监控的成员发现流程：

```
┌────────────────────────────────────────────────────────────┐
│              FileConfigMemberLookup                        │
│  extends AbstractMemberLookup                             │
│  ├─ watcher: FileWatcher                                │
│  │    └─ onChange(event): 当 cluster.conf 变更时触发    │
│  ├─ doStart(): 注册 FileWatcher + 首次读取             │
│  ├─ doDestroy(): 注销 FileWatcher                       │
│  └─ readClusterConf(): 读取 cluster.conf → List<Member> │
└────────────────────────────────────────────────────────────┘
        │                              │
        │ 注册 FileWatcher              │ onChange() 回调
        ▼                              ▼
┌────────────────────────────────────────────────────────────┐
│              WatchFileCenter                              │
│  registerWatcher(path, watcher)                         │
│  deregisterWatcher(path, watcher)                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ FileWatcher.onChange(FileChangeEvent)              │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│  cluster.conf 示例:                                     │
│  192.168.1.1:8848                                     │
│  192.168.1.2:8848                                     │
│  192.168.1.3:8848                                     │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│  MemberUtil.readServerConf(reader)                       │
│    → List<Member> (每行解析为 ip:port → Member)       │
└────────────────────────────────────────────────────────────┘
```

### 5.4.3 源码走读

#### 5.4.3.1 doStart() 启动流程

`FileConfigMemberLookup.doStart()`（`core/src/main/java/com/alibaba/nacos/core/cluster/lookup/FileConfigMemberLookup.java:60-赤羽`）：

```java
public void doStart() throws NacosException {
    // 1. 首次读取 cluster.conf
    readClusterConf();
    // 2. 注册 FileWatcher——监控 cluster.conf 文件变更
    WatchFileCenter.registerWatcher(
        EnvUtil.getConfPath(),               // Nacos conf 目录
        watcher                               // FileWatcher 实例
    );
}
```

**`FileWatcher.onChange()`**：当 `cluster.conf` 文件内容变更时，`WatchFileCenter` 回调 `watcher.onChange(FileChangeEvent)`，触发 `readClusterConf()` 重新读取成员列表并通过 `afterLookup()` 通知 `ServerMemberManager.memberChange()`。

#### 5.4.3.2 readClusterConf() 读取文件

`readClusterConf()`（`FileConfigMemberLookup.java:78-95`）：
```java
private void readClusterConf() {
    Reader reader = null;
    try {
        reader = new InputStreamReader(
            new FileInputStream(
                new File(EnvUtil.getConfPath(), DEFAULT_SEARCH_SEQ)
            ), "UTF-8"
        );
        List<Member> members = MemberUtil.readServerConf(
            EnvUtil.analyzeClusterConf(reader)
        );
        afterLookup(members);  // 通知 ServerMemberManager.memberChange()
    } catch (Exception e) {
        Loggers.CLUSTER.error("read cluster.conf error", e);
    }
}
```

`MemberUtil.readServerConf()`（`core/src/main/java/com/alibaba/nacos/core/cluster/MemberUtil.java:70-100`）解析 `cluster.conf` 文件内容，每行为 `ip:port` 格式，构造 `Member` 对象列表（默认 `state=UP`）。

### 5.4.4 设计模式分析

1. **观察者模式（Observer）**：`WatchFileCenter` + `FileWatcher` 实现文件变更的发布-订阅机制。`FileConfigMemberLookup` 作为观察者订阅 `cluster.conf` 文件的变更事件，文件变更时自动触发成员列表更新。

2. **模板方法模式（Template Method）**：`AbstractMemberLookup.afterLookup()` 定义统一的通知 `ServerMemberManager.memberChange()` 流程，`FileConfigMemberLookup` 只需实现 `doStart()` 完成文件监控注册和首次读取。

3. **适配器模式（Adapter）**：`MemberUtil.readServerConf()` 将文本格式的成员列表（`ip:port` 每行）适配为 `List<Member>` 对象，实现文本格式到对象模型的转换。

### 5.4.5 Trade-off 分析

| 权衡维度 | 文件监控模式 | 定时轮询模式 |
|---------|------------|-------------|
| **实时性** | 文件变更即时触发（WatchFileCenter） | 定时间隔内的延迟 |
| **资源开销** | 仅在文件变更时触发 | 定时读取文件 I/O |
| **适用场景** | 静态集群、手动变更 | 动态集群 |
| **单点故障** | 配置文件损坏导致全部成员丢失 | 同左 |

### 5.4.6 小结

`FileConfigMemberLookup` 通过 `WatchFileCenter` 文件监控机制实现 `cluster.conf` 文件的变更感知，文件内容变更时自动触发成员列表更新并通过 `AbstractMemberLookup.afterLookup()` 通知 `ServerMemberManager`。这种设计适用于静态集群部署场景，成员变更需运维手动修改 `cluster.conf` 文件。动态扩缩容场景建议使用 5.5 节介绍的 `AddressServerMemberLookup`。
### 5.5.1 设计背景

在动态集群部署场景中（如 Kubernetes 弹性扩缩容），集群成员变化频繁，手动维护 `cluster.conf` 文件不可行。`AddressServerMemberLookup` 通过向外部地址服务器定期 HTTP GET 请求获取成员列表，实现成员发现的自动化。

### 5.5.2 核心类关系图

图 5-5 展示了 `AddressServerMemberLookup` 通过 HTTP REST 定期查询地址服务器的成员发现流程：

```
┌────────────────────────────────────────────────────────────────┐
│            AddressServerMemberLookup                        │
│  extends AbstractMemberLookup                               │
│  ├─ domainName: String              ← 地址服务器域名/URL  │
│  ├─ addressServerUrl: String       ← 完整 API 地址        │
│  ├─ maxFailCount: int             ← 最大失败次数阈值     │
│  ├─ isAddressServerHealth: boolean ← 地址服务器健康状态  │
│  ├─ nacosRestTemplate: NacosRestTemplate                 │
│  ├─ doStart(): 启动定时查询任务                          │
│  ├─ syncFromAddressUrl(): HTTP GET + 解析成员列表       │
│  └─ AddressServerSyncTask implements Runnable              │
│       └─ run(): 定时执行 syncFromAddressUrl()            │
└────────────────────────────────────────────────────────────────┘
        │                              │
        │ HTTP GET /serverlist        │ health check fail
        ▼                              ▼
┌────────────────────────────────────────────────────────────────┐
│              地址服务器（外部）                            │
│  GET /serverlist → [                                    │
│    "192.168.1.1:8848",                                │
│    "192.168.1.2:8848",                                │
│    "192.168.1.3:8848"                                 │
│  ]                                                       │
└────────────────────────────────────────────────────────────────┘
```

### 5.5.3 源码走读

#### 5.5.3.1 doStart() 启动流程

`AddressServerMemberLookup.doStart()`（`core/src/main/java/com/alibaba/nacos/core/cluster/lookup/AddressServerMemberLookup.java:72-加大`）：

```java
public void doStart() throws NacosException {
    // 1. 读取地址服务器 URL（配置项：nacos.member.lookup.address-server.url）
    this.addressServerUrl = EnvUtil.getProperty(
        "nacos.member.lookup.address-server.url"
    );
    if (StringUtils.isBlank(addressServerUrl)) {
        throw new NacosException(NacosException.SERVER_ERROR,
            "address server url is null");
    }
    // 2. 创建 NacosRestTemplate
    nacosRestTemplate = HttpClientBeanHolder.getNacosRestTemplate(
        Loggers.CLUSTER
    );
    // 3. 首次同步
    syncFromAddressUrl();
    // 4. 启动定时同步任务（默认 5 秒间隔）
    GlobalExecutor.scheduleByCommon(
        new AddressServerSyncTask(),
        DEFAULT_SYNC_TASK_DELAY_MS  // 默认 5000ms
    );
}
```

#### 5.5.3.2 syncFromAddressUrl() HTTP 查询

`syncFromAddressUrl()`（`AddressServerMemberLookup.java:100-138`）：

```java
private void syncFromAddressUrl() {
    try {
        // HTTP GET 请求地址服务器
        RestResult<String> result = nacosRestTemplate.get(
            addressServerUrl,
            Header.EMPTY,
            Query.EMPTY,
            String.class
        );
        if (result.ok()) {
            // 解析 JSON 数组 → List<String> → List<Member>
            List<String> serverList = JSON.parseArray(
                result.getData(), String.class
            );
            List<Member> members = MemberUtil.readServerConf(
                serverList
            );
            // 通知 ServerMemberManager
            afterLookup(members);
            addressServerFailCount = 0;  // 重置失败计数
        } else {
            addressServerFailCount++;
            if (addressServerFailCount >= maxFailCount) {
                isAddressServerHealth = false;  // 标记地址服务器不可用
            }
        }
    } catch (Throwable ex) {
        addressServerFailCount++;
        if (addressServerFailCount >= maxFailCount) {
            isAddressServerHealth = false;
        }
    }
}
```

**AddressServerSyncTask**（`AddressServerMemberLookup.java:142-160`）：`Runnable` 内部类，`run()` 方法调用 `syncFromAddressUrl()` 后重新调度自身：

```java
class AddressServerSyncTask implements Runnable {
    @Override
    public void run() {
        if (shutdown) return;
        try {
            syncFromAddressUrl();
        } catch (Throwable ex) {
            addressServerFailCount++;
            if (addressServerFailCount >= maxFailCount) {
                isAddressServerHealth = false;
            }
        } finally {
            GlobalExecutor.scheduleByCommon(
                this, DEFAULT_SYNC_TASK_DELAY_MS
            );
        }
    }
}
```

### 5.5.4 设计模式分析

1. **模板方法模式（Template Method）**：`AbstractMemberLookup.afterLookup()` 定义统一的通知流程，`AddressServerMemberLookup` 只需实现 `doStart()` 完成 HTTP 查询 + 定时调度逻辑。

2. **观察者模式（Observer）**：`AddressServerSyncTask` 作为定时观察者周期性执行 `syncFromAddressUrl()`，地址服务器成员变更时自动通知 `ServerMemberManager`。

3. **断路器模式（Circuit Breaker）**：`isAddressServerHealth` + `addressServerFailCount >= maxFailCount` 实现简单的断路器逻辑——连续失败达到阈值时标记地址服务器不可用，避免持续无效请求。

### 5.5.5 Trade-off 分析

| 权衡维度 | 地址服务器模式 | 配置文件模式 |
|---------|--------------|------------|
| **动态扩缩容** | ✅ HTTP API 实时推送 | ❌ 需手动修改 cluster.conf |
| **外部依赖** | 依赖地址服务器可用性 | 无外部依赖 |
| **延迟** | ≤ 5 秒（默认轮询间隔） | 文件变更即时（WatchFileCenter） |
| **容错** | 断路器 + 降级（连续失败标记不可用） | 无容错机制 |

### 5.5.6 小结

`AddressServerMemberLookup` 通过 NacosRestTemplate HTTP GET 定期查询地址服务器获取成员列表，配合 `AddressServerSyncTask` 定时调度 + 断路器容错机制，适用于 Kubernetes 等动态扩缩容场景。默认 5 秒轮询间隔在实时性与资源开销之间取得了平衡。


### 5.6.1 设计背景

在开发与测试环境中，通常只需要单节点运行 Nacos，无需集群成员发现。`StandaloneMemberLookup` 提供最简单的成员发现实现——仅返回本机 IP + 端口构造的单成员列表。

### 5.6.2 核心类关系图

图 5-6 展示了 `StandaloneMemberLookup` 的单成员自发现流程：

```
┌────────────────────────────────────────────────────────────┐
│              StandaloneMemberLookup                        │
│  extends AbstractMemberLookup                             │
│  ├─ doStart(): 构造 self Member + afterLookup()        │
│  ├─ doDestroy(): 无操作（单机无需清理）               │
│  └─ useAddressServer(): return false                     │
└────────────────────────────────────────────────────────────┘
        │
        │ doStart()
        ▼
┌────────────────────────────────────────────────────────────┐
│  MemberUtil.singleParse(ServerEnv)                       │
│    ├─ ip = InetAddress.getLocalHost().getHostAddress() │
│    └─ port = EnvUtil.getPort()                          │
│    → Member(ip, port, NodeState.UP)                     │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│  afterLookup([self])                                    │
│    → serverMemberManager.memberChange([self])          │
│    → MembersChangeEvent(added=[self])                  │
│    → NotifyCenter.publishEvent()                        │
└────────────────────────────────────────────────────────────┘
```

### 5.6.3 源码走读

`StandaloneMemberLookup`（`core/src/main/java/com/alibaba/nacos/core/cluster/lookup/StandaloneMemberLookup.java:28-48`）是最简洁的 `MemberLookup` 实现：

```java
public class StandaloneMemberLookup extends AbstractMemberLookup {
    
    public StandaloneMemberLookup(ServerEnv env) throws NacosException {
        // nothing to do
    }
    
    @Override
    public void doStart() {
        // 1. 构造本机 Member
        Member self = MemberUtil.singleParse(serverEnv);
        // 2. 通知 ServerMemberManager
        afterLookup(Collections.singletonList(self));
    }
    
    @Override
    public boolean useAddressServer() {
        return false;
    }
    
    @Override
    public void doDestroy() {
        // nothing to do
    }
}
```

### 5.6.4 设计模式分析

1. **空对象模式（Null Object Pattern）**：`StandaloneMemberLookup.doDestroy()` 为空实现——单机模式无需清理资源，避免空指针检查。

2. **简化模式（Simplification）**：整个类仅 48 行代码，将成员发现逻辑简化到极致——仅返回本机 Member，无定时任务、无网络请求、无文件监控。

### 5.6.5 Trade-off 分析

| 权衡维度 | 单机模式 | 配置文件模式 |
|---------|---------|------------|
| **适用场景** | 开发/测试 | 生产集群 |
| **复杂度** | 极低（48 行） | 中等（~200 行） |
| **集群能力** | ❌ 单节点 | ✅ 多节点 |
| **扩缩容** | N/A | 需手动修改 cluster.conf |

### 5.6.6 小结

`StandaloneMemberLookup` 是 Nacos 集群管理的默认模式，仅 48 行代码实现最简单的成员发现——返回本机 Member。适用于开发与测试环境。这是 Nacos 开箱即用（"Just Run"）设计原则的体现——无需任何集群配置即可启动单节点 Nacos。
### 5.7.1 设计背景

`Member` 是 Nacos 集群成员信息的基本载体，在网络层面标识一个节点的 IP 和端口，在状态层面通过 `NodeState` 枚举跟踪节点的健康状态（UP/DOWN/SUSPECT）。`Member` 模型的核心设计目标：为上层一致性协议（JRaft/Distro）、集群 RPC 通信、健康检查等模块提供统一的成员信息视图。

### 5.7.2 核心类关系图

图 5-7 展示了 `Member` 模型的核心字段与 `NodeState` 状态机：

```
┌──────────────────────────────────────────────────────────────┐
│                       Member                               │
│  ├─ ip: String              ← 节点 IP 地址                │
│  ├─ port: int               ← 节点端口号                  │
│  ├─ state: NodeState        ← 健康状态（UP/DOWN/SUSPECT）│
│  ├─ extendInfo: Map<String, Object>                       │
│  │    ├─ "raftMetaData" → ProtocolMetaData               │
│  │    ├─ "adWeight" → Double                            │
│  │    └─ "site" → String                                │
│  ├─ getAddress(): String    ← "ip:port"                 │
│  └─ setState(NodeState)    ← 线程安全状态更新           │
└──────────────────────────────────────────────────────────────┘
        │
        │ state 字段变更
        ▼
┌──────────────────────────────────────────────────────────────┐
│                   NodeState (enum)                          │
│  ┌──────┐    heartbeat timeout    ┌─────────┐           │
│  │  UP  │ ────────────────────→ │ SUSPECT │           │
│  └──────┘                      └─────────┘           │
│      ▲                              │                   │
│      │ heartbeat recover             │ timeout threshold  │
│      │                              ▼                   │
│      │                         ┌─────────┐           │
│      └──────────────────────── │  DOWN   │           │
│          re-register           └─────────┘           │
└──────────────────────────────────────────────────────────────┘
```

### 5.7.3 源码走读

#### 5.7.3.1 Member 核心字段

`Member`（`core/src/main/java/com/alibaba/nacos/core/cluster/Member.java:38-265`）核心字段：

```java
// Member.java:45-56
private String ip;                                  // line 45
private int port;                                  // line 46
private volatile NodeState state = NodeState.UP;   // line 47, 默认 UP
private Map<String, Object> extendInfo =            // line 48
    new ConcurrentHashMap<>();
```

**`getAddress()`（`Member.java:108-110`）**：
```java
public String getAddress() {
    return ip + ":" + port;
}
```

**`setState(NodeState)`（`Member.java:130-138`）**——线程安全的状态更新，仅在状态真正变更时写入：
```java
public void setState(NodeState state) {
    if (this.state != state) {
        this.state = state;
    }
}
```

#### 5.7.3.2 extendInfo 扩展信息

`extendInfo`（`Map<String, Object>`）是 `Member` 的扩展点，上层模块可通过 `putExtendVal(key, val)` 注入任意元数据：
- **Raft 元数据**：`ProtocolMetaData`（Leader/Term/RaftGroup），由 `JRaftProtocol` 写入
- **权重信息**：`adWeight`（地址服务器权重），由 `AddressServerMemberLookup` 写入为新
- **站点信息**：`site`（多数据中心部署时的站点标识）

#### 5.7.3.3 NodeState 状态机

`NodeState`（`core/src/main/java/com/alibaba/nacos/core/cluster/NodeState.java:28-54`）：

```java
public enum NodeState {
    UP,       // 健康：参与 Distro/JRaft 共识 + gRPC 通信
    DOWN,     // 故障：从 serverList 移除，不参与共识和通信
    SUSPECT   // 疑似故障：保留在 serverList，但标记可疑
}
```

状态转换规则（由健康检查模块驱动，参见 5.12 节心跳机制）：
- `UP → SUSPECT`：连续心跳超时但未达到 DOWN 阈值（默认 15 秒超时）
- `SUSPECT → UP`：心跳恢复
- `SUSPECT → DOWN`：心跳持续超时达到 DOWN 阈值（默认 30 秒）
- `DOWN → UP`：节点重新注册 + 心跳恢复

### 5.7.4 设计模式分析

1. **状态模式（State）**：`NodeState` 枚举 + `Member.setState()` 方法封装节点健康状态的状态机转换逻辑，不同状态下节点的行为不同（UP→参与共识、DOWN→从 serverList 移除、SUSPECT→保留但标记可疑）。

2. **扩展点模式（Extension Point）**：`extendInfo`（`Map<String, Object>`）提供开放式的扩展能力，上层模块（Raft/Distro/健康检查）可通过 `putExtendVal()` 注入任意元数据而不修改 `Member` 类本身。

3. **不可变快照模式（Immutable Snapshot）**：`getAddress()` 每次返回新构造的 `ip:port` 字符串，避免外部直接修改 `Member` 内部状态。

### 5.7.5 Trade-off 分析

| 权衡维度 | 设计决策 | 收益 | 代价 |
|---------|---------|------|------|
| **volatile state** | `state` 字段使用 `volatile` | 多线程可见性保证 | 无锁但可能短暂不一致 |
| **ConcurrentHashMap extendInfo** | 使用 `ConcurrentHashMap` | 高并发读性能 | 写操作需全量替换 |
| **SUSPECT 中间状态** | 引入 SUSPECT 避免误判 DOWN | 减少误下线导致的集群震荡 | 增加状态复杂度 |

### 5.7.6 小结

`Member` 模型通过 `ip:port`（网络标识）+ `NodeState`（健康状态机）+ `extendInfo`（扩展元数据）三维结构统一描述集群成员信息。`NodeState` 三态状态机（UP→SUSPECT→DOWN）驱动成员健康状态转换，`extendInfo` 提供开放式扩展点供上层模块注入 Raft 元数据/权重/站点等扩展信息。


### 5.8.1 设计背景

在多节点集群中，节点间需要高效通信以完成成员信息上报、一致性协议数据同步、健康检查等任务。Nacos 2.5.3 基于 gRPC 构建集群间 RPC 通信代理 `ClusterRpcClientProxy`，统一管理所有集群成员间的 gRPC 连接，支持**同步调用、异步回调、广播**三种通信模式。

### 5.8.2 核心类关系图

图 5-8 展示了 `ClusterRpcClientProxy` 的架构：

```
┌──────────────────────────────────────────────────────────────┐
│              ClusterRpcClientProxy                           │
│  extends MemberChangeListener                             │
│  ├─ RpcClientFactory: 管理所有 gRPC Client               │
│  ├─ serverMemberManager: ServerMemberManager              │
│  ├─ init(): 订阅 MembersChangeEvent + 初始化 RPC 客户端  │
│  ├─ onEvent(MembersChangeEvent): 成员变更时刷新客户端  │
│  ├─ refresh(List<Member>): 为新增成员创建 gRPC Client   │
│  │    └─ createRpcClientAndStart(member, GRPC)           │
│  ├─ send(member, request): 同步发送                     │
│  ├─ asyncSend(member, request, callback): 异步发送      │
│  └─ broadcast(request): 向所有 UP 成员广播              │
└──────────────────────────────────────────────────────────────┘
        │                              │
        │ RpcClientFactory            │ onEvent(MembersChangeEvent)
        ▼                              ▼
┌──────────────────────────────────────────────────────────────┐
│  RpcClient (gRPC)                                        │
│  ├─ serverListFactory: 连接地址列表                     │
│  ├─ start(): 启动 gRPC 连接                            │
│  ├─ shutdown(): 关闭连接                                │
│  ├─ sendRequest(request): 同步请求                       │
│  └─ sendRequestAsync(request, callback): 异步请求       │
└──────────────────────────────────────────────────────────────┘
```

### 5.8.3 源码走读

#### 5.8.3.1 init() 初始化流程

`ClusterRpcClientProxy.init()`（`core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java:75-92`）：

```java
@PostConstruct
public void init() {
    // 1. 订阅 MembersChangeEvent——当成员变更时自动刷新 gRPC Client
    NotifyCenter.registerSubscriber(this);
    // 2. 首次为除本机外的所有成员创建 gRPC Client
    List<Member> members = serverMemberManager.allMembersWithoutSelf();
    refresh(members);
}
```

#### 5.8.3.2 onEvent() 成员变更处理

`ClusterRpcClientProxy.onEvent()`（`ClusterRpcClientProxy.java:94-101`）——当 `ServerMemberManager.memberChange()` 发布 `MembersChangeEvent` 时自动触发：

```java
@Override
public void onEvent(MembersChangeEvent event) {
    try {
        refresh(serverMemberManager.allMembersWithoutSelf());
    } catch (NacosException e) {
        Loggers.CLUSTER.error("ClusterRpcClientProxy refresh failed", e);
    }
}
```

#### 5.8.3.3 refresh() 刷新 gRPC Client

`refresh()`（`ClusterRpcClientProxy.java:103-130`）——增量管理 gRPC Client：

```java
private void refresh(List<Member> members) throws NacosException {
    // 1. 为新加入成员创建 gRPC Client
    for (Member member : members) {
        createRpcClientAndStart(member, ConnectionType.GRPC);
    }
    // 2. 关闭已离场成员的 gRPC Client
    Set<String> newMemberKeys = members.stream()
        .map(this::memberClientKey).collect(Collectors.toSet());
    RpcClientFactory.getAllClientEntries().forEach((key, client) -> {
        if (key.startsWith("Cluster-") && !newMemberKeys.contains(key)) {
            Loggers.CLUSTER.info("member leave, destroy client: {}", key);
            RpcClientFactory.getClient(key).shutdown();
        }
    });
}
```

#### 5.8.3.4 send() / asyncSend() / broadcast()

- **`send(member, request)`**（`ClusterRpcClientProxy.java:150-160`）：同步发送 gRPC 请求到指定成员，超时默认 3000ms
- **`asyncSend(member, request, callback)`**（`ClusterRpcClientProxy.java:162-175`）：异步发送 gRPC 请求，通过 `RequestCallBack` 回调处理响应
- **`broadcast(request)`**（`ClusterRpcClientProxy.java:177-190`）：向所有 `state=UP` 成员广播同一请求，用于心跳成员上报 (`MemberReportRequest`)

### 5.8.4 设计模式分析

1. **观察者模式（Observer）**：`ClusterRpcClientProxy extends MemberChangeListener` 订阅 `MembersChangeEvent`，当成员变更时自动 `refresh()` gRPC Client 列表。

2. **代理模式（Proxy）**：`ClusterRpcClientProxy` 封装对 `RpcClientFactory` + `RpcClient` 的访问，提供统一的 `send()`/`asyncSend()`/`broadcast()` 接口，上层业务只需与 `ClusterRpcClientProxy` 交互而无需直接管理 gRPC 连接。

3. **工厂模式（Factory）**：`RpcClientFactory.createRpcClient()` 根据 `ConnectionType.GRPC` 创建 gRPC Client 实例，封装 gRPC Channel 的创建和配置细节。

### 5.8.5 Trade-off 分析

| 权衡维度 | 设计决策 | 收益 | 代价 |
|---------|---------|------|------|
| **连接管理** | RpcClientFactory 统一管理 | 连接复用，减少资源开销 | 单点管理增加复杂度 |
| **同步 vs 异步** | 提供 send() + asyncSend() | 灵活适配不同场景 | API 复杂度增加 |
| **广播 vs 单播** | broadcast() 向所有 UP 成员广播 | 一次发送覆盖全集群 | 网络开销随成员数线性增长 |
| **增量更新** | refresh() 增量管理 Client | 仅创建/销毁变更成员 | 成员频繁变更时频繁创建/销毁 |

### 5.8.6 小结

`ClusterRpcClientProxy` 通过 `RpcClientFactory` 统一管理所有集群成员间的 gRPC 连接，通过订阅 `MembersChangeEvent` 实现成员变更时自动增量刷新 gRPC Client。提供 `send()`（同步）、`asyncSend()`（异步回调）、`broadcast()`（广播）三种通信模式，覆盖集群间 RPC 通信的全部场景。
### 5.9.1 设计背景

Nacos 配置客户端 SDK（`NacosConfigService`）是 Java 应用获取 Nacos 配置管理的入口接口。2.5.3 版本中，配置客户端通过 gRPC 长连接与 Nacos 服务端通信（替代 1.x 的 HTTP 短轮询），支持配置获取、监听、快照持久化等核心能力。

### 5.9.2 核心类关系图

图 5-9 展示了 NacosConfigService 的核心类关系：

```
┌────────────────────────────────────────────────────────────────┐
│              <<interface>> ConfigService                    │
│  ├─ getConfig(dataId, group, timeout): String             │
│  ├─ publishConfig(dataId, group, content): boolean        │
│  ├─ removeConfig(dataId, group): boolean                 │
│  ├─ addListener(dataId, group, listener): void           │
│  └─ removeListener(dataId, group, listener): void        │
└────────────────────────────────────────────────────────────────┘
        △
        │ implements
┌───────┴────────────────────────────────────────────────────────┐
│              NacosConfigService                            │
│  ├─ namespace: String               ← 命名空间            │
│  ├─ ServerHttpAgent: HttpAgent     ← gRPC 通信代理      │
│  ├─ ClientWorker: ClientWorker     ← 长轮询工作线程     │
│  ├─ getConfig(dataId, group, ms)                        │
│  │    → ConfigRpcServerRequest / ConfigQueryRequest      │
│  ├─ addListener(dataId, group, listener)                 │
│  │    → ClientWorker.addTenantListeners()               │
│  └─ publishConfig(dataId, group, content)                │
│       → ConfigPublishRequest                             │
└────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│  ServerHttpAgent    │    │  ClientWorker                  │
│  (gRPC 通信层)     │    │  ├─ LongPollingRunnable      │
│  ├─ start()        │    │  ├─ checkServerConfig()      │
│  └─ httpGet()      │    │  └─ cacheMap: AtomicReference │
└──────────────────────┘    └──────────────────────────────────┘
```

### 5.9.3 源码走读

#### 5.9.3.1 构造与初始化

`NacosConfigService`（`client/src/main/java/com/alibaba/nacos/client/config/NacosConfigService.java:58-95`）：

```java
public class NacosConfigService implements ConfigService {
    private final String namespace;
    private final ServerHttpAgent agent;     // gRPC 通信代理
    private final ClientWorker worker;          // 长轮询工作线程
    
    public NacosConfigService(Properties properties) throws NacosException {
        // 1. 校验配置参数
        ValidatorUtils.checkInitParam(properties);
        // 2. 初始化 namespace（默认 ""）
        this.namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        // 3. 创建 ServerHttpAgent（gRPC 通信代理）
        this.agent = new ServerHttpAgent(properties);
        // 4. 创建 ClientWorker（长轮询工作线程）
        this.worker = new ClientWorker(this.agent, this.namespace, properties);
    }
}
```

#### 5.9.3.2 getConfig()

`getConfig()`（`NacosConfigService.java:110-130`）——从 Nacos 服务端获取配置：

```java
public String getConfig(String dataId, String group, long timeoutMs) 
    throws NacosException {
    // 1. 优先从本地快照读取（failover 机制）
    String content = LocalConfigInfoProcessor.getFailover(
        agent.getName(), dataId, group, tenant
    );
    if (content != null) {
        return content;
    }
    // 2. 通过 gRPC 从服务端获取
    ConfigRpcServerRequest request = new ConfigRpcServerRequest();
    request.setDataId(dataId);
    request.setGroup(group);
    request.setTenant(namespace);
    // 3. 发送 gRPC ConfigQueryRequest
    ConfigQueryRequest configQueryRequest = new ConfigQueryRequest();
    ConfigQueryResponse response = agent.queryConfig(configQueryRequest);
    return response.getContent();
}
```

#### 5.9.3.3 addListener()

`addListener()`（`NacosConfigService.java:150-170`）——注册配置变更监听器：

```java
public void addListener(String dataId, String group, Listener listener) 
    throws NacosException {
    // ClientWorker 负责管理 Listener 列表 + 长轮询
    worker.addTenantListeners(dataId, group, listener);
}
```

### 5.9.4 设计模式分析

1. **代理模式（Proxy）**：`NacosConfigService` 封装 `ServerHttpAgent`（gRPC 通信）和 `ClientWorker`（长轮询），向客户端提供统一的 `ConfigService` 接口。

2. **观察者模式（Observer）**：`addListener()` 注册 `Listener`，当配置变更时 `ClientWorker` 通过长轮询检测到 MD5 变化后回调 `Listener.receiveConfigInfo()`。

3. **门面模式（Facade）**：`ConfigService` 接口作为统一入口，封装 gRPC 通信、本地缓存、快照持久化等复杂子系统。

### 5.9.5 Trade-off 分析

| 权衡维度 | gRPC 长连接（2.5.3） | HTTP 短轮询（1.x） |
|---------|---------------------|-------------------|
| **连接开销** | 长连接复用，减少 TLS 握手 | 每次请求建立新连接 |
| **实时性** | 服务端推送（双向流） | 客户端定时轮询 |
| **资源消耗** | 维持长连接的内存开销 | 频繁连接建立/销毁 |

### 5.9.6 小结

`NacosConfigService` 实现 `ConfigService` 接口，通过 `ServerHttpAgent`（gRPC 通信）和 `ClientWorker`（长轮询）提供配置获取、监听、发布的核心能力。2.5.3 采用 gRPC 长连接替代 1.x HTTP 短轮询，降低了连接开销并支持服务端推送。


### 5.10.1 设计背景

`ClientWorker` 是 Nacos 配置客户端长轮询机制的核心实现。它通过维护 `cacheMap`（配置 MD5 缓存）定期向 Nacos 服务端发起 `checkServerConfig()` 请求，比较本地 MD5 与服务端 MD5，当 MD5 不一致时触发配置变更通知。

### 5.10.2 核心类关系图

图 5-10 展示了 ClientWorker 的长轮询机制：

```
┌──────────────────────────────────────────────────────────────┐
│                    ClientWorker                            │
│  ├─ agent: HttpAgent              ← gRPC 通信代理         │
│  ├─ cacheMap: AtomicReference<Map<String, CacheData>>    │
│  ├─ listeners: Map<String, List<Listener>>               │
│  ├─ addTenantListeners(dataId, group, listener)         │
│  ├─ checkServerConfig(): 长轮询核心方法                 │
│  └─ LongPollingRunnable: 定时长轮询任务                 │
└──────────────────────────────────────────────────────────────┘
        │
        │ LongPollingRunnable.run()
        ▼
┌──────────────────────────────────────────────────────────────┐
│  checkServerConfig() 流程                                │
│  1. 收集所有 dataId + group → List<String>              │
│  2. 对每个 subscriber:                                    │
│     ├─ 本地 MD5 = cacheMap.get(key).md5                │
│     ├─ gRPC ConfigChangeNotifyRequest → 服务端          │
│     ├─ 服务端返回: changed dataId list                  │
│     └─ getConfig() 获取最新配置 → listener.receive()   │
│  3. schedule next LongPollingRunnable (默认 3000ms)      │
└──────────────────────────────────────────────────────────────┘
```

### 5.10.3 源码走读

#### 5.10.3.1 checkServerConfig() 长轮询核心

`ClientWorker.checkServerConfig()`（`client/src/main/java/com/alibaba/nacos/client/config/impl/ClientWorker.java:250-320`）——长轮询的核心逻辑：

```java
public void checkServerConfig() {
    // 1. 收集所有监听配置的 dataId + group
    List<String> changedGroups = checkUpdateDataIds(
        cacheMap.get().keySet()
    );
    // 2. 对每个变更的 group,获取最新配置并通知 Listener
    for (String groupKey : changedGroups) {
        String[] config = groupKey.split("+");
        String dataId = config[0];
        String group = config[1];
        String tenant = config[2];
        // 3. 从服务端获取最新配置内容
        String content = getServerConfig(dataId, group, tenant, 3000L);
        // 4. 通知所有注册的 Listener
        CacheData cache = cacheMap.get().get(groupKey);
        cache.setContent(content);
        cache.checkListenerMd5();
    }
}
```

#### 5.10.3.2 LongPollingRunnable

`LongPollingRunnable`（内部类）——定时执行 `checkServerConfig()`：

```java
class LongPollingRunnable implements Runnable {
    @Override
    public void run() {
        checkServerConfig();
        // 重新调度自己（默认 3000ms 间隔）
        executorService.schedule(
            this, taskPenaltyTime, TimeUnit.MILLISECONDS
        );
    }
}
```

### 5.10.4 设计模式分析

1. **观察者模式（Observer）**：`ClientWorker` 维护 `listeners` Map，配置变更时回调 `Listener.receiveConfigInfo()`。

2. **轮询模式（Polling）**：`LongPollingRunnable` 定时执行 `checkServerConfig()`，通过对比 MD5 检测配置变更。

3. **缓存模式（Cache）**：`cacheMap`（`AtomicReference<Map>`）提供线程安全的配置缓存，支持原子更新整个缓存快照。

### 5.10.5 Trade-off 分析

| 权衡维度 | 长轮询（Long Polling） | 短轮询（Short Polling） |
|---------|----------------------|------------------------|
| **实时性** | 服务端推送 + 客户端定时检查 | 仅客户端定时轮询 |
| **请求量** | 一次长连接复用 | 每次轮询新建请求 |
| **资源消耗** | 维持长连接 | 频繁 TCP 握手 |

### 5.10.6 小结

`ClientWorker` 通过 `cacheMap` 维护本地 MD5 缓存 + `LongPollingRunnable` 定时长轮询 `checkServerConfig()`，当 MD5 不一致时自动获取最新配置并回调 `Listener.receiveConfigInfo()`。默认 3000ms 轮询间隔在实时性与资源消耗之间取得平衡。


### 5.11.1 设计背景

`NacosNamingService` 是 Nacos 服务注册发现客户端 SDK 的核心实现，提供实例注册、服务订阅、实例查询等核心能力。2.5.3 版本中，命名客户端通过 gRPC 长连接与 Nacos 服务端通信，订阅机制基于 `NamingPushRequestHandler` 实现服务端推送。

### 5.11.2 核心类关系图

图 5-11 展示了 NacosNamingService 的核心类关系：

```
┌────────────────────────────────────────────────────────────────┐
│            <<interface>> NamingService                      │
│  ├─ registerInstance(serviceName, instance): void          │
│  ├─ subscribe(serviceName, listener): void               │
│  ├─ unsubscribe(serviceName, listener): void              │
│  ├─ getAllInstances(serviceName): List<Instance>        │
│  └─ deregisterInstance(serviceName, instance): void      │
└────────────────────────────────────────────────────────────────┘
        △
        │ implements
┌───────┴────────────────────────────────────────────────────────┐
│              NacosNamingService                            │
│  ├─ namespace: String                                      │
│  ├─ NamingClientProxy: NamingClientProxy                 │
│  ├─ hostReactor: HostReactor ← 服务端节点列表维护      │
│  ├─ registerInstance(): gRPC InstanceRequest              │
│  ├─ subscribe(): NamingPushRequestHandler                │
│  └─ getAllInstances(): gRPC ServiceQueryRequest          │
└────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│ NamingClientProxy   │    │ HostReactor                   │
│ (gRPC 通信代理)     │    │ ├─ serverList: List<String> │
│ ├─ requestToServer()│    │ ├─ refreshSrvIfNeed()      │
│ └─ reqAPI()        │    │ └─ getServiceInfo()          │
└──────────────────────┘    └──────────────────────────────────┘
```

### 5.11.3 源码走读

#### 5.11.3.1 构造与初始化

`NacosNamingService`（`client/src/main/java/com/alibaba/nacos/client/naming/NacosNamingService.java:75-120`）：

```java
public class NacosNamingService implements NamingService {
    private String namespace;
    private NamingClientProxy clientProxy;    // gRPC 代理
    private HostReactor hostReactor;          // 服务端列表维护
    
    public NacosNamingService(Properties properties) {
        this.namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        this.hostReactor = new HostReactor(properties);
        this.clientProxy = new NamingClientProxy(
            this.namespace, hostReactor, properties
        );
    }
}
```

#### 5.11.3.2 registerInstance()

`registerInstance()`（`NacosNamingService.java:185-205`）——通过 gRPC `InstanceRequest` 向 Nacos 服务端注册实例：

```java
public void registerInstance(String serviceName, Instance instance) 
    throws NacosException {
    // 1. 构造 gRPC InstanceRequest
    InstanceRequest request = new InstanceRequest(
        namespace, serviceName, instance
    );
    // 2. 通过 NamingClientProxy 发送 gRPC 请求
    clientProxy.registerService(request);
}
```

#### 5.11.3.3 subscribe()

`subscribe()`（`NacosNamingService.java:250-270`）——订阅服务变更通知：

```java
public void subscribe(String serviceName, String group, 
    EventListener listener) throws NacosException {
    // 1. 注册 NamingPushRequestHandler（gRPC 双向流）
    clientProxy.subscribe(serviceName, group, listener);
}
```

### 5.11.4 设计模式分析

1. **代理模式（Proxy）**：`NamingClientProxy` 封装 gRPC 通信细节，向 `NacosNamingService` 提供统一的命名服务接口。

2. **观察者模式（Observer）**：`subscribe()` 注册 `EventListener`，当服务端实例变更时通过 `NamingPushRequestHandler`（gRPC 双向流）推送通知。

3. **门面模式（Facade）**：`NamingService` 接口封装 `NamingClientProxy`（通信）、`HostReactor`（服务端列表）等多个子系统。

### 5.11.5 Trade-off 分析

| 权衡维度 | gRPC 双向流（2.5.3） | HTTP 短轮询（1.x） |
|---------|---------------------|-------------------|
| **连接模型** | 长连接双向流 | 短连接请求-响应 |
| **推送能力** | 服务端主动推送 | 客户端定时拉取 |
| **资源消耗** | 维持长连接 | 频繁建立连接 |
| **适用场景** | 高频变更服务 | 低频变更服务 |

### 5.11.6 小结

`NacosNamingService` 通过 `NamingClientProxy`（gRPC 通信代理）实现实例注册、服务订阅、实例查询等核心能力。2.5.3 采用 gRPC 双向流替代 1.x HTTP 短轮询，支持服务端主动推送实例变更通知，提升了注册发现的实时性。
### 5.12.1 设计背景

Nacos 2.5.3 的心跳机制基于 gRPC 双向流（Bidirectional Streaming），与 1.x 的 HTTP 短连接心跳（`BeatReactor`）完全不同。客户端通过 gRPC 长连接持续发送 `HealthCheckRequest` 给服务端，服务端 `ClientBeatProcessorV2` 处理心跳并更新 `Instance` 的 `lastBeatTime`。服务端 `InstanceBeatChecker` 定时检查超时实例，触发 `Instance` 健康状态变更。

**关键事实**：2.5.3 中 `BeatReactor`（1.x 的 HTTP 心跳实现）已完全移除，全部替换为 gRPC 双向流心跳。

### 5.12.2 核心类关系图

图 5-12 展示了 Nacos 2.5.3 客户端心跳机制（gRPC 双向流）：

```
┌────────────────────────────────────────────────────────────────┐
│                    客户端（gRPC 长连接）                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  ConnectionManager                                  │  │
│  │  ├─ registerConnection(connectionId)                │  │
│  │  └─ sendHealthCheck(connectionId): gRPC           │  │
│  └──────────────────────────────────────────────────────┘  │
│        │                                                  │
│        │ gRPC HealthCheckRequest (双向流)                │
└────────┼──────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│                    服务端                                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  ClientBeatProcessorV2                              │  │
│  │  ├─ process(request): HealthCheckResponse           │  │
│  │  ├─ 更新 Instance.lastBeatTime = now()            │  │
│  │  └─ 返回 HealthCheckResponse                      │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  InstanceBeatChecker                              │  │
│  │  ├─ 定时检查所有 Instance.lastBeatTime            │  │
│  │  ├─ 超时 Instance (now - lastBeatTime > timeout)  │  │
│  │  └─ → ClientOperationService.deregisterInstance()  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 5.12.3 源码走读

#### 5.12.3.1 ClientBeatProcessorV2

`ClientBeatProcessorV2`（`naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatProcessorV2.java:40-80`）——服务端 gRPC 心跳处理器：

```java
public class ClientBeatProcessorV2 implements RequestHandler<HealthCheckRequest, HealthCheckResponse> {
    
    @Override
    public HealthCheckResponse handle(HealthCheckRequest request, RequestMeta meta) 
        throws NacosException {
        String clientId = request.getClientId();
        // 1. 从 ClientManager 获取 Client 对象
        Client client = clientManager.getClient(clientId);
        if (client == null) {
            return new HealthCheckResponse().setSuccess(false);
        }
        // 2. 更新最后心跳时间
        client.setLastUpdatedTime(System.currentTimeMillis());
        // 3. 返回心跳响应（服务端IP列表）
        HealthCheckResponse response = new HealthCheckResponse();
        response.setServerIpList(getHealthyServerList());
        return response;
    }
}
```

#### 5.12.3.2 InstanceBeatChecker

`InstanceBeatChecker`——服务端定时检查超时实例：

```java
@Component
public class InstanceBeatChecker {
    
    @Scheduled(fixedRate = 5000)  // 每 5 秒检查一次
    public void checkInstanceBeat() {
        for (Instance instance : allInstance()) {
            long now = System.currentTimeMillis();
            long expireTime = instance.getInstanceHeartBeatTimeout();
            // 超时判定：now - lastBeatTime > timeout (默认 15s)
            if (now - instance.getLastBeatTime() > expireTime) {
                // 触发 Instance 健康状态变更
                clientOperationService.deregisterInstance(instance);
            }
        }
    }
}
```

### 5.12.4 设计模式分析

1. **观察者模式（Observer）**：`InstanceBeatChecker` 定时检查所有 `Instance` 心跳超时，超时触发 `ClientOperationService.deregisterInstance()` —— 状态变更通过 `NotifyCenter` 事件总线通知 `DistroProtocol` 同步。

2. **策略模式（Strategy）**：`HealthCheckRequest` 处理可扩展为多种健康检查类型（TCP/HTTP/gRPC），`ClientBeatProcessorV2` 统一处理 gRPC 心跳。

3. **超时检测模式（Timeout Detection）**：`InstanceBeatChecker` 通过 `scheduled(fixedRate=5000)` 定时扫描 `Instance.lastBeatTime` 与当前时间差值判定超时。

### 5.12.5 Trade-off 分析

| 权衡维度 | gRPC 双向流心跳（2.5.3） | HTTP 短连接心跳（1.x BeatReactor） |
|---------|-----------------------------|-------------------------------------|
| **连接模型** | gRPC 长连接双向流 | HTTP 短连接请求-响应 |
| **心跳频率** | 默认 5s | 默认 5s |
| **超时判定** | 服务端 `InstanceBeatChecker` 15s 超时 | 同左 |
| **推送能力** | 心跳响应可携带服务端 IP 列表 | 仅返回心跳确认 |

### 5.12.6 小结

Nacos 2.5.3 的心跳机制基于 gRPC 双向流，`ClientBeatProcessorV2` 处理客户端心跳并更新 `Instance.lastBeatTime`，`InstanceBeatChecker` 定时检查超时实例并触发 `ClientOperationService.deregisterInstance()`。gRPC 长连接心跳相比 HTTP 短连接降低了连接开销，心跳响应可携带服务端 IP 列表实现服务发现路由优化。


### 5.13.1 设计背景

Nacos 客户端 SDK 提供本地缓存快照机制，用于在 Nacos 服务端不可用时容灾降级。当服务端不可用时，客户端从本地文件快照中读取上次成功获取的配置，保证应用的基本可用性。`FailoverReactor` 负责管理本地文件快照的持久化与加载。

### 5.13.2 核心类关系图

图 5-13 展示了客户端本地缓存快照机制：

```
┌────────────────────────────────────────────────────────────────┐
│                    FailoverReactor                          │
│  ├─ switchParams: FailoverSwitch                          │
│  ├─ configService: ConfigService                          │
│  ├─ isFailover: boolean                                 │
│  ├─ init(): 启动文件监听                                 │
│  └─ getFailover(dataId, group, tenant): String          │
└────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│  FailoverSwitch     │    │  LocalConfigInfoProcessor      │
│  ├─ isFailover()   │    │  ├─ saveSnapshot(path,config) │
│  └─ getFailoverDir()│    │  └─ getFailover(path):String │
└──────────────────────┘    └──────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│  本地文件系统: ~/nacos/config/                            │
│  ├─ {tenant}/{group}/{dataId}                            │
│  └─ 每个配置独立文件存储                                │
└──────────────────────────────────────────────────────────────┘
```

### 5.13.3 源码走读

#### 5.13.3.1 FailoverReactor

`FailoverReactor`（`client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverReactor.java:40-120`）——容灾降级核心实现：

- **`init()`**：启动 `FileWatcher` 监听快照目录 `~/nacos/naming/data/` 的文件变更
- **`getFailover(dataId, group, tenant)`**：当 `isFailover == true` 时从本地文件读取上次成功获取的配置快照
- **`switchFailover()`**：当服务端不可用时切换到容灾模式（`isFailover = true`）

#### 5.13.3.2 LocalConfigInfoProcessor

`LocalConfigInfoProcessor`——本地配置快照处理器：

- **`saveSnapshot(String path, String config)`**：将配置内容保存到本地文件 `~/nacos/config/{tenant}/{group}/{dataId}`
- **`getFailover(String path)`**：从本地文件读取配置快照内容

### 5.13.4 设计模式分析

1. **断路器模式（Circuit Breaker）**：`FailoverReactor.switchFailover()` 在服务端不可用时切换到容灾模式，从本地快照读取配置，保证应用基本可用性。

2. **快照模式（Snapshot）**：`LocalConfigInfoProcessor.saveSnapshot()` 在每次成功获取配置后持久化到本地文件，作为容灾降级的数据源。

3. **观察者模式（Observer）**：`FailoverReactor` 通过 `FileWatcher` 监听快照目录文件变更，当快照文件变更时重新加载。

### 5.13.5 Trade-off 分析

| 权衡维度 | 本地快照容灾 | 无容灾机制 |
|---------|------------|-----------|
| **服务端不可用时** | 可从本地快照读取上次配置 | 应用无法获取任何配置 |
| **数据一致性** | 可能与服务端最新配置不一致 | N/A |
| **存储开销** | 每个配置独立文件 | 无存储开销 |

### 5.13.6 小结

`FailoverReactor` + `LocalConfigInfoProcessor` 提供客户端本地缓存快照机制：服务端可用时每次成功获取配置后持久化到本地文件；服务端不可用时从本地快照读取上次配置，保证应用基本可用性。这是 Nacos 客户端"容灾降级"设计原则的体现。


### 5.14.1 设计背景

Nacos 通过 Java SPI（Service Provider Interface）机制实现插件化扩展。`NacosServiceLoader` 是 Nacos 对 Java SPI 的封装，支持 `@Order` 注解排序和 SPI 扩展点自动发现。2.5.3 中 SPI 机制应用于认证插件、数据源插件、加密插件等 6 种插件类型（参见第 6 章）。

### 5.14.2 核心类关系图

图 5-14 展示了 `NacosServiceLoader` 的 SPI 加载机制：

```
┌────────────────────────────────────────────────────────────────┐
│                  NacosServiceLoader                         │
│  ├─ load(Class<T> serviceClass): Collection<T>           │
│  ├─ load(Class<T> serviceClass, ClassLoader loader)     │
│  └─ newServiceInstance(Class<?> clazz): T              │
└────────────────────────────────────────────────────────────────┘
        │
        │ META-INF/services/{interface_full_name}
        ▼
┌────────────────────────────────────────────────────────────────┐
│  META-INF/services/                                      │
│  ├─ com.alibaba.nacos.plugin.auth.spi.AuthPluginService │
│  │   └─ com.alibaba.nacos.plugin.auth.impl.NacosAuth    │
│  ├─ com.alibaba.nacos.plugin.datasource.spi.DataSource  │
│  │   └─ com.alibaba.nacos.plugin.datasource.impl.MySQL   │
│  └─ ...                                                  │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│  @Order 排序                                             │
│  ├─ @Order(0) → 最高优先级                              │
│  ├─ @Order(Integer.MAX_VALUE) → 最低优先级              │
│  └─ 未标注 @Order → 默认最低优先级                      │
└────────────────────────────────────────────────────────────────┘
```

### 5.14.3 源码走读

#### 5.14.3.1 load() SPI 加载

`NacosServiceLoader.load()`（`client/src/main/java/com/alibaba/nacos/client/utils/NacosServiceLoader.java:42-80`）：

```java
public static <T> Collection<T> load(final Class<T> serviceClass, 
    final ClassLoader classLoader) {
    // 1. 使用 Java SPI ServiceLoader 加载所有实现
    ServiceLoader<T> serviceLoader = ServiceLoader.load(
        serviceClass, classLoader
    );
    List<T> services = new ArrayList<>();
    for (T service : serviceLoader) {
        services.add(service);
    }
    // 2. 按 @Order 注解排序
    services.sort((o1, o2) -> {
        Order or1 = o1.getClass().getAnnotation(Order.class);
        Order or2 = o2.getClass().getAnnotation(Order.class);
        int order1 = or1 == null ? Integer.MAX_VALUE : or1.value();
        int order2 = or2 == null ? Integer.MAX_VALUE : or2.value();
        return Integer.compare(order1, order2);
    });
    return services;
}
```

#### 5.14.3.2 SPI 扩展点应用

Nacos 2.5.3 中 `NacosServiceLoader` 应用于以下扩展点：

| 插件类型 | SPI 接口 | 默认实现 |
|---------|---------|---------|
| 认证插件 | `AuthPluginService` | `NacosAuthPluginService` |
| 数据源插件 | `DataSourcePluginService` | `MySQLDataSourcePluginService` |
| 加密插件 | `EncryptionPluginService` | `AESEncryptionPluginService` |
| 跟踪插件 | `TracePlugin` | 无默认（可选） |
| 环境插件 | `EnvironmentPlugin` | 无默认（可选） |
| 控制插件 | `ControlManagerPlugin` | 无默认（可选） |

### 5.14.4 设计模式分析

1. **服务提供者模式（Service Provider）**：Java SPI 机制通过 `META-INF/services/{interface}` 文件声明接口与实现的映射关系，实现插件化扩展。

2. **策略模式（Strategy）**：多个 SPI 实现通过 `@Order` 排序决定优先级，运行时选择最高优先级的实现。

3. **工厂模式（Factory）**：`NacosServiceLoader.load()` 作为工厂方法返回所有可用的 SPI 实现集合。

### 5.14.5 Trade-off 分析

| 权衡维度 | Java SPI | Spring Factories | OSGi |
|---------|---------|----------------|------|
| **复杂度** | 低（标准 Java） | 中 | 高 |
| **动态加载** | 不支持 | 不支持 | 支持 |
| **排序能力** | @Order 注解 | @Order 注解 | Service Ranking |
| **适用场景** | 简单插件扩展 | Spring 生态集成 | 复杂模块化应用 |

### 5.14.6 小结

`NacosServiceLoader` 封装 Java SPI 机制，通过 `META-INF/services` 文件声明 + `@Order` 排序实现插件化扩展。2.5.3 中 6 种插件类型均通过此机制实现动态加载。详细的插件体系分析与自定义插件开发指南参见第 6 章。
