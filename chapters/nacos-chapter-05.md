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

Nacos 配置客户端 SDK（`NacosConfigService`）是 Java 应用接入 Nacos 配置管理的入口接口。2.5.3 版本中，配置客户端通过 gRPC 长连接（替代 1.x HTTP 短轮询）与 Nacos 服务端通信，支持配置获取（`getConfig`）、配置监听（`addListener`）、配置发布（`publishConfig`）、配置删除（`removeConfig`）四大核心能力。本节聚焦 `NacosConfigService` 的接口设计与源码实现，5.10 节展开 `ClientWorker` 长轮询机制的完整链路。

### 5.9.2 核心类关系图

图 5-9 展示了 `NacosConfigService` 的架构——实现 `ConfigService` 接口，内部委托 `ServerHttpAgent`（gRPC 通信层）和 `ClientWorker`（长轮询工作线程）：

```
┌────────────────────────────────────────────────────────────────┐
│              <<interface>> ConfigService                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ + getConfig(dataId, group, timeoutMs): String      │  │
│  │ + publishConfig(dataId, group, content): boolean   │  │
│  │ + removeConfig(dataId, group): boolean             │  │
│  │ + addListener(dataId, group, listener): void       │  │
│  │ + removeListener(dataId, group, listener): void    │  │
│  │ + getServerConfig(dataId, group, timeout): String │  │
│  └──────────────────────────────────────────────────────┘  │
└───────────────────────────────┬────────────────────────────┘
                                △
                                │ implements
┌───────────────────────────────┴────────────────────────────┐
│                NacosConfigService                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - namespace: String                                │  │
│  │ - agent: HttpAgent              ← gRPC 通信代理   │  │
│  │ - worker: ClientWorker          ← 长轮询线程     │  │
│  │ - configFilterChainManager: ConfigFilterChainMgr │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ + NacosConfigService(Properties)                  │  │
│  │   → 校验参数 → 创建 ServerHttpAgent              │  │
│  │   → 创建 ClientWorker → 启动 LongPollingRunnable │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ getConfig(dataId, group, timeoutMs)              │  │
│  │   → 优先读 LocalConfigInfoProcessor.getFailover() │  │
│  │   → gRPC ConfigQueryRequest → 服务端              │  │
│  │   → 返回 config content                          │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ addListener(dataId, group, listener)              │  │
│  │   → ClientWorker.addTenantListeners()            │  │
│  │   → 注册 ConfigChangeListener                     │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│  ServerHttpAgent    │    │  ClientWorker                  │
│  ├─ serverList:     │    │  ├─ agent: HttpAgent          │
│  │   List<String>   │    │  ├─ cacheMap: AtomicRef<Map> │
│  ├─ start()        │    │  ├─ listeners: Map<String,    │
│  ├─ httpGet()      │    │  │          List<Listener>>   │
│  └─ queryConfig()   │    │  ├─ checkServerConfig()      │
└──────────────────────┘    │  └─ LongPollingRunnable      │
                            └──────────────────────────────────┘
```

### 5.9.3 源码走读

#### 5.9.3.1 构造与初始化链路

`NacosConfigService`（`client/src/main/java/com/alibaba/nacos/client/config/NacosConfigService.java:58-105`）构造器完成参数校验→通信代理创建→长轮询线程启动的三段初始化：

```java
public class NacosConfigService implements ConfigService {
    // line 62-65: 核心组件
    private final String namespace;         // 命名空间 (tenant)
    private final ServerHttpAgent agent;     // gRPC 通信代理
    private final ClientWorker worker;       // 长轮询工作线程
    private final ConfigFilterChainManager configFilterChainManager;
    
    public NacosConfigService(Properties properties) throws NacosException {
        // line 72-78: 参数校验——确保 serverAddr 非空
        ValidatorUtils.checkInitParam(properties);
        String encode = properties.getProperty(PropertyKeyConst.ENCODE);
        // line 80: 初始化 namespace（默认空字符串 = public namespace）
        this.namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        // line 83-85: 创建 ConfigFilterChainManager
        this.configFilterChainManager = new ConfigFilterChainManager(properties);
        // line 87: 创建 ServerHttpAgent（内部创建 gRPC RpcClient）
        this.agent = new ServerHttpAgent(properties);
        // line 89: 创建 ClientWorker——启动长轮询线程
        this.worker = new ClientWorker(this.agent, this.namespace, properties);
    }
}
```

#### 5.9.3.2 getConfig()——配置获取全链路

`getConfig()`（`NacosConfigService.java:130-185`）实现三级优先级配置获取：

```java
private String getConfigInner(String tenant, String dataId, String group, long timeoutMs)
    throws NacosException {
    // line 142-156: 第一级——本地容灾快照（Failover 机制）
    String content = LocalConfigInfoProcessor.getFailover(
        agent.getName(), dataId, group, tenant
    );
    if (content != null) {
        NAMING_LOGGER.warn("[{}] [get-config] failover content:{}", agent.getName(), content);
        return content;
    }
    // line 160-175: 第二级——通过 gRPC 从 Nacos 服务端获取
    ConfigRpcServerRequest request = new ConfigRpcServerRequest();
    request.setDataId(dataId);
    request.setGroup(group);
    request.setTenant(tenant);
    try {
        // line 170: 发送 gRPC ConfigQueryRequest
        ConfigRpcClientProxy rpcClientProxy = agent.getRpcClient(tenant);
        ConfigQueryRequest queryRequest = new ConfigQueryRequest();
        queryRequest.setDataId(dataId);
        queryRequest.setGroup(group);
        queryRequest.setTenant(tenant);
        ConfigQueryResponse response = rpcClientProxy.queryConfig(queryRequest);
        content = response.getContent();
    } catch (NacosException ioe) {
        // line 180: 服务端不可用时降级到本地快照
        content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
    }
    return content;
}
```

**三级优先级总结**：
1. **本地容灾快照**（`LocalConfigInfoProcessor.getFailover()`）——服务端不可用时的降级路径
2. **gRPC 远程获取**（`ConfigQueryRequest`）——正常路径，通过 gRPC 双向流从服务端拉取最新配置
3. **gRPC 失败降级**——当 gRPC 请求失败时，回退到本地快照

#### 5.9.3.3 addListener()——配置监听注册

`addListener()`（`NacosConfigService.java:205-230`）——注册配置变更监听器：

```java
public void addListener(String dataId, String group, Listener listener) 
    throws NacosException {
    // line 215-220: 参数校验——dataId/group/Listener 非空
    if (null == dataId || null == group || null == listener) {
        throw new NacosException(NacosException.CLIENT_INVALID_PARAM, 
            "dataId/group/listener cannot be null");
    }
    // line 225: 委托 ClientWorker 管理 Listener 列表 + 长轮询
    worker.addTenantListeners(dataId, group, listener);
}
```

#### 5.9.3.4 publishConfig()——配置发布

`publishConfig()`（`NacosConfigService.java:240-270`）——向 Nacos 服务端发布配置：

```java
public boolean publishConfig(String dataId, String group, String content) 
    throws NacosException {
    // line 250-255: ConfigFilterChain 过滤器链处理
    ConfigRequest configRequest = new ConfigRequest();
    configRequest.setDataId(dataId);
    configRequest.setGroup(group);
    configRequest.setTenant(namespace);
    configRequest.setContent(content);
    configFilterChainManager.doFilter(configRequest, null);
    // line 260-265: gRPC ConfigPublishRequest
    ConfigPublishRequest request = new ConfigPublishRequest();
    request.setDataId(dataId);
    request.setGroup(group);
    request.setTenant(namespace);
    request.setContent(content);
    return agent.publishConfig(request);
}
```

`publishConfig()` 通过 `ConfigFilterChainManager` 过滤器链对配置内容做预处理（如加密），再通过 gRPC `ConfigPublishRequest` 发送到 Nacos 服务端。过滤器链机制参见第 6 章插件体系分析。

### 5.9.4 设计模式分析

1. **门面模式（Facade）**：`ConfigService` 接口作为统一入口，封装 `ServerHttpAgent`（gRPC 通信）、`ClientWorker`（长轮询）、`LocalConfigInfoProcessor`（本地快照）三个子系统，客户端只需调用 `getConfig()` 而无需关心内部实现。

2. **代理模式（Proxy）**：`ServerHttpAgent` 封装 gRPC `RpcClient` 的创建、配置和通信细节，`NacosConfigService` 通过 `agent.queryConfig()` 间接访问 gRPC 服务。

3. **责任链模式（Chain of Responsibility）**：`ConfigFilterChainManager` 管理 `ConfigFilter` 过滤器链，`publishConfig()` 发布配置前先经过过滤器链预处理（加密、校验等），符合开闭原则——新增过滤器无需修改 `NacosConfigService` 代码。

4. **降级模式（Degradation）**：`getConfig()` 三级优先级（本地快照→gRPC→降级回退）保证服务端不可用时客户端仍可从本地快照读取配置，保证基本可用性。

### 5.9.5 Trade-off 分析

| 权衡维度 | 设计决策 | 收益 | 代价 |
|---------|---------|------|------|
| **三级优先级** | 本地快照 → gRPC → 降级回退 | 服务端不可用时仍可读配置 | 本地快照可能与服务端不一致 |
| **gRPC vs HTTP** | gRPC 长连接替代 HTTP 短轮询 | 减少连接开销、支持服务端推送 | 维持长连接的内存开销 |
| **ConfigFilterChain** | 发布前过滤器链预处理 | 可插拔扩展（加密/校验） | 增加调用链深度 |
| **Listener 机制** | addListener() 注册回调 | 配置变更实时通知 | 内存中维护 Listener 列表 |

### 5.9.6 小结

`NacosConfigService` 实现 `ConfigService` 接口，通过 `ServerHttpAgent`（gRPC 通信代理）和 `ClientWorker`（长轮询线程）提供配置获取、监听、发布、删除四大核心能力。`getConfig()` 采用三级优先级策略（本地快照→gRCP→降级回退）保证高可用。配置监听机制在 5.10 节 `ClientWorker` 中展开。


### 5.10.1 设计背景

`ClientWorker` 是 Nacos 配置客户端长轮询机制的核心实现。它通过维护 `cacheMap`（配置 MD5 缓存映射）定期向 Nacos 服务端发起 `checkServerConfig()` 请求，比较本地 MD5 与服务端 MD5，当 MD5 不一致时触发配置变更通知——回调所有注册的 `Listener.receiveConfigInfo()`。

2.5.3 中，`ClientWorker` 通过 gRPC 双向流与服务端通信，替代 1.x 的 HTTP 短轮询。`LongPollingRunnable` 定时任务每隔 `taskPenaltyTime`（默认 3000ms）执行一次 `checkServerConfig()`，形成"定时检查→MD5 对比→变更通知"的闭环。

### 5.10.2 核心类关系图

图 5-10 展示了 `ClientWorker` 的长轮询机制——从 `cacheMap` 维护、`checkServerConfig()` 核心方法、到 `LongPollingRunnable` 定时调度：

```
┌────────────────────────────────────────────────────────────────┐
│                    ClientWorker                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - agent: HttpAgent             ← gRPC 通信代理     │  │
│  │ - cacheMap: AtomicReference<Map<String, CacheData>> │  │
│  │ - listeners: Map<String, List<Listener>>           │  │
│  │ - taskPenaltyTime: long = 3000ms                │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ + ClientWorker(agent, namespace, properties)        │  │
│  │ + addTenantListeners(dataId, group, listener)    │  │
│  │ + checkServerConfig(): void                       │  │
│  │ + getServerConfig(dataId, group, tenant, timeout) │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        │ scheduleWithFixedDelay(taskPenaltyTime)
        ▼
┌────────────────────────────────────────────────────────────────┐
│              LongPollingRunnable (implements Runnable)      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ run():                                           │  │
│  │   1. checkServerConfig()                          │  │
│  │   2. executorService.schedule(this,               │  │
│  │        taskPenaltyTime, TimeUnit.MILLISECONDS)   │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│  checkServerConfig() 详细流程                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ STEP 1: 收集所有监听配置的 dataId + group + tenant│  │
│  │   → Map<String, CacheData> cache = cacheMap.get() │  │
│  │ STEP 2: 对每个 subscriber 发送 gRPC                  │  │
│  │   ConfigChangeNotifyRequest → 服务端返回变更列表    │  │
│  │ STEP 3: 对每个变更配置:                             │  │
│  │   a. getServerConfig(dataId, group, tenant)        │  │
│  │   b. cacheData.setContent(content)                   │  │
│  │   c. cacheData.checkListenerMd5()                   │  │
│  │      → if MD5 changed: listener.receiveConfigInfo()  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│  CacheData（配置缓存单元）                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - content: String          ← 配置内容               │  │
│  │ - md5: String             ← 配置内容 MD5 值        │  │
│  │ - listeners: List<Listener>                        │  │
│  │ - checkListenerMd5(): void                        │  │
│  │   → if (newMd5 != this.md5)                      │  │
│  │       for listener: listener.receiveConfigInfo()   │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 5.10.3 源码走读

#### 5.10.3.1 ClientWorker 构造与初始化

`ClientWorker`（`client/src/main/java/com/alibaba/nacos/client/config/impl/ClientWorker.java:72-110`）：

```java
public class ClientWorker implements Closeable {
    // line 75-82: 核心数据结构
    private final HttpAgent agent;                        // gRPC 通信代理
    private final String tenant;                          // 命名空间
    private final AtomicReference<Map<String, CacheData>> cacheMap = 
        new AtomicReference<>(new HashMap<>());           // MD5 缓存映射
    private final ConfigFilterChainManager configFilterChainManager;
    private ScheduledExecutorService executor;            // 定时任务线程池
    private long taskPenaltyTime = 3000L;               // 默认 3000ms 轮询间隔
    
    public ClientWorker(HttpAgent agent, String tenant, 
        Properties properties) {                          // line 95-110
        this.agent = agent;
        this.tenant = tenant;
        // 创建单线程定时任务执行器
        executor = Executors.newScheduledThreadPool(1,
            new NameThreadFactory("com.alibaba.nacos.client.Worker"));
        // 启动 LongPollingRunnable——首次立即执行，后续每 taskPenaltyTime ms 执行
        executor.scheduleWithFixedDelay(
            new LongPollingRunnable(), 0, taskPenaltyTime, 
            TimeUnit.MILLISECONDS
        );
    }
}
```

#### 5.10.3.2 addTenantListeners()——注册监听器

`addTenantListeners()`（`ClientWorker.java:130-165`）——将 Listener 添加到 `cacheMap` 中对应 `CacheData` 的 Listeners 列表：

```java
public void addTenantListeners(String dataId, String group, Listener listener) {
    // line 135: 构造缓存 key = "dataId+group+tenant"
    String key = GroupKey.getKey(dataId, group, tenant);
    // line 140-150: 更新 cacheMap——原子替换整个 Map
    Map<String, CacheData> newCache = new HashMap<>(cacheMap.get());
    CacheData cacheData = newCache.get(key);
    if (cacheData == null) {
        cacheData = new CacheData();
        newCache.put(key, cacheData);
    }
    cacheData.addListener(listener);
    cacheMap.set(newCache);  // AtomicReference 保证可见性
}
```

#### 5.10.3.3 checkServerConfig()——长轮询核心方法

`ClientWorker.checkServerConfig()`（`ClientWorker.java:200-280`）——长轮询的核心逻辑：

```java
public void checkServerConfig() {
    // STEP 1 (line 210-220): 收集所有监听配置的 dataId + group
    Map<String, CacheData> cache = cacheMap.get();
    List<String> changedGroupKeys = checkUpdateDataIds(
        cache.keySet()              // 所有监听的 key 集合
    );
    // STEP 2 (line 230-260): 对每个变更的 group key,获取最新配置
    for (String groupKey : changedGroupKeys) {
        String[] config = groupKey.split("\\+");
        String dataId = config[0];
        String group = config[1];
        String tenant = config.length > 2 ? config[2] : "";
        // STEP 去打a (line 245): 从服务端获取最新配置
        String content = getServerConfig(dataId, group, tenant, 3000L);
        // STEP 2b (line 250): 更新 CacheData 内容
        CacheData cacheData = cache.get(groupKey);
        cacheData.setContent(content);
        // STEP 三行c (line 255): 检查 MD5 变更并通知 Listener
        cacheData.checkListenerMd5();
    }
}
```

#### 5.10.3.4 checkListenerMd5()——MD5 变更检测与通知

`CacheData.checkListenerMd5()`（`ClientWorker.java:350-380`）——核心通知逻辑：

```java
public void checkListenerMd5() {
    // line 355: 计算当前配置内容的 MD5
    String newMd5 = MD5Utils.md5Hex(content, Constants.ENCODE);
    // line 360: MD5 未变更——跳过通知
    if (Objects.equals(newMd5, this.md5)) {
        return;
    }
    this.md5 = newMd5;  // 更新 MD5 缓存
    // line 370-375: MD5 变更——通知所有注册的 Listener
    for (Listener listener : listeners) {
        listener.receiveConfigInfo(content);
    }
}
```

#### 5.10.3.5 LongPollingRunnable——定时调度

`LongPollingRunnable`（内部类）定时调用 `checkServerConfig()`：

```java
class LongPollingRunnable implements Runnable {
    @Override
    public void run() {
        try {
            checkServerConfig();
        } catch (Throwable ex) {
            NAMING_LOGGER.error("[checkServerConfig] error", ex);
        }
    }
}
```

### 5.10.4 设计模式分析

1. **观察者模式（Observer）**：`CacheData` 维护 `listeners` 列表，当 MD5 变更时 `checkListenerMd5()` 回调所有 `Listener.receiveConfigInfo()`，实现配置变更的自动通知。

2. **缓存模式（Cache）**：`cacheMap`（`AtomicReference<Map>`）提供线程安全的 MD5 缓存，通过 `AtomicReference.compareAndSet()` 原子更新整个缓存快照，避免并发修改异常。

3. **轮询模式（Polling）**：`LongPollingRunnable` 通过 `ScheduledExecutorService.scheduleWithFixedDelay()` 定时执行 `checkServerConfig()`，形成"定时检查→MD5 对比→变更通知"的闭环。

4. **单线程执行器模式（Single-Thread Executor）**：`executor` 是单线程 `ScheduledThreadPoolExecutor(1)`，保证 `checkServerConfig()` 不会并发执行，避免 MD5 缓存并发更新冲突。

### 5.10.5 Trade-off 分析

| 权衡维度 | 设计决策 | 收益 | 代价 |
|---------|---------|------|------|
| **轮询间隔** | `taskPenaltyTime=3000ms` | 配置变更通知延迟 ≤ 3s | 每 3s 一次 gRPC 请求开销 |
| **AtomicReference 缓存** | `cacheMap` 原子替换整个 Map | 无锁读性能最优 | 写操作需全量复制 Map |
| **单线程执行器** | `newScheduledThreadPoolExecutor(1)` | 避免 MD5 并发冲突 | 长任务阻塞后续轮询 |
| **长轮询 vs 推送** | 客户端主动定时检查 MD5 | 不依赖服务端推送通道 | 服务端变更到客感知有延迟 |

### 5.10.6 小结

`ClientWorker` 通过 `cacheMap`（`AtomicReference<Map>`）维护 MD5 缓存映射，`LongPollingRunnable` 每 `taskPenaltyTime=3000ms` 定时执行 `checkServerConfig()`——收集所有监听配置→gRCP 查询变更列表→对比 MD5→回调 `Listener.receiveConfigInfo()`。原子引用 + 单线程执行器保证线程安全，三级优先级 `getConfig()`（本地快照→gRPC→降级回退）保证高可用。


### 5.11.1 设计背景

`NacosNamingService` 是 Nacos 服务注册发现客户端 SDK 的核心实现，提供实例注册（`registerInstance`）、服务订阅（`subscribe`）、实例查询（`getAllInstances`）、实例注销（`deregisterInstance`）四大核心能力。2.5.3 中，命名客户端通过 gRPC 双向流与 Nacos 服务端通信，`NamingPushRequestHandler` 实现服务端主动推送实例变更通知，彻底替代 1.x 的 HTTP 定时轮询拉取模式。

### 5.11.2 核心类关系图

图 5-11 展示了 `NacosNamingService` 的架构——实现 `NamingService` 接口，内部委托 `NamingClientProxy`（gRPC 通信代理）和 `HostReactor`（服务端节点列表维护）：

```
┌────────────────────────────────────────────────────────────────┐
│            <<interface>> NamingService                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ + registerInstance(serviceName, instance): void      │  │
│  │ + deregisterInstance(serviceName, instance): void   │  │
│  │ + getAllInstances(serviceName): List<Instance>     │  │
│  │ + subscribe(serviceName, listener): void            │  │
│  │ + unsubscribe(serviceName, listener): void           │  │
│  │ + getServiceInfo(serviceName): ServiceInfo           │  │
│  └──────────────────────────────────────────────────────┘  │
└───────────────────────────────┬────────────────────────────┘
                                △
                                │ implements
┌───────────────────────────────┴────────────────────────────┐
│                NacosNamingService                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - namespace: String                                │  │
│  │ - clientProxy: NamingClientProxy ← gRPC 通信代理 │  │
│  │ - hostReactor: HostReactor     ← 服务端列表维护  │  │
│  │ - serviceInfoHolder: ServiceInfoHolder             │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ + registerInstance(serviceName, group, instance)    │  │
│  │   → gRPC InstanceRequest → NamingClientProxy      │  │
│  │ + subscribe(serviceName, listener)                  │  │
│  │   → NamingPushRequestHandler ← gRPC 双向流       │  │
│  │ + getAllInstances(serviceName, group)              │  │
│  │   → ServiceQueryRequest → NamingClientProxy        │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│ NamingClientProxy   │    │  HostReactor                  │
│ ├─ reqAPI(request)  │    │  ├─ serverList: List<String> │
│ ├─ registerService()│    │  ├─ refreshSrvIfNeed()      │
│ ├─ subscribe()      │    │  ├─ getServiceInfo()          │
│ └─ requestToServer()│    │  └─ getNamingClientProxy()    │
└──────────────────────┘    └──────────────────────────────────┘
```

### 5.11.3 源码走读

#### 5.11.3.1 构造与初始化

`NacosNamingService`（`client/src/main/java/com/alibaba/nacos/client/naming/NacosNamingService.java:70-130`）：

```java
public class NacosNamingService implements NamingService {
    // line 75-85: 核心组件
    private String namespace;
    private NamingClientProxy clientProxy;        // gRPC 命名代理
    private HostReactor hostReactor;              // 服务端节点列表维护
    private ServiceInfoHolder serviceInfoHolder;    // 本地服务信息缓存
    
    public NacosNamingService(Properties properties) {
        // line 元-100: 初始化 namespace（默认 ""）
        this.namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        // line 105-115: 创建 HostReactor——维护服务端节点列表 + RPC Client 实例
        this.hostReactor = new HostReactor(properties);
        // line 120: 创建 NamingClientProxy——封装 gRPC 通信
        this.serviceInfoHolder = new ServiceInfoHolder(namespace, properties);
        this.clientProxy = new NamingClientProxy(
            namespace, hostReactor, serviceInfoHolder, properties
        );
    }
}
```

#### 5.11.3.2 registerInstance()——实例注册全链路

`registerInstance()`（`NacosNamingService.java:175-220`）——通过 gRPC `InstanceRequest` 向 Nacos 服务端注册实例：

```java
public void registerInstance(String serviceName, String groupName, Instance instance) 
    throws NacosException {
    // line 185-190: 参数校验——serviceName 和 instance 非空
    checkServiceName(serviceName);
    checkInstance(instance);
    // line 195-205: 构造 gRPC InstanceRequest
    InstanceRequest request = new InstanceRequest();
    request.setNamespace(namespace);
    request.setServiceName(serviceName);
    request.setGroupName(groupName);
    request.setInstance(instance);
    // line 210: 通过 NamingClientProxy 发送 gRPC 请求
    clientProxy.registerService(request);
}
```

#### 5.11.3.3 subscribe()——服务订阅全链路

`subscribe()`（`NacosNamingService.java:240-280`）——订阅服务实例变更并通过 `NamingPushRequestHandler` 接收服务端推送：

```java
public void subscribe(String serviceName, String groupName, List<String> clusters,
    EventListener listener) throws NacosException {
    // line 250-255: 构造订阅请求
    SubscribeRequest request = new SubscribeRequest();
    request.setNamespace(namespace);
    request.setServiceName(serviceName);
    request.setGroupName(groupName);
    request.setClusters(clusters);
    // line 260: 通过 NamingClientProxy 注册 NamingPushRequestHandler
    clientProxy.subscribe(request, listener);
}
```

#### 5.11.3.4 getAllInstances()——实例查询

`getAllInstances()`（`NacosNamingService.java:310-370`）——通过 gRPC `ServiceQueryRequest` 获取服务实例列表：

```java
public List<Instance> getAllInstances(String serviceName, String groupName) 
    throws NacosException {
    // line 325-340: 优先从本地 ServiceInfo 缓存获取
    ServiceInfo serviceInfo = serviceInfoHolder.getServiceInfo(
        serviceName, groupName, clusters
    );
    if (serviceInfo != null && !serviceInfo.isExpired()) {
        return serviceInfo.getHosts();  // 本地缓存有效，直接返回
    }
    // line 350: 通过 NamingClientProxy 从服务端查询
    ServiceQueryRequest request = new ServiceQueryRequest();
    request.setNamespace(namespace);
    request.setServiceName(serviceName);
    request.setGroupName(groupName);
    return clientProxy.queryInstancesOfService(request);
}
```

### 5.11.4 设计模式分析

1. **代理模式（Proxy）**：`NamingClientProxy` 封装 gRPC 通信细节，向 `NacosNamingService` 提供统一的 `registerInstance()`/`subscribe()`/`getAllInstances()` 接口。

2. **观察者模式（Observer）**：`subscribe()` 注册 `EventListener`，当服务端实例变更时通过 `NamingPushRequestHandler`（gRPC 双向流）推送通知——响应 `InstancesChangeEvent` 事件。

3. **缓存模式（Cache）**：`ServiceInfoHolder` 维护本地 `ServiceInfo` 缓存，`getAllInstances()` 优先从本地缓存读取，减轻服务端查询压力。

4. **反应式推送模式（Reactive Push）**：`NamingPushRequestHandler` 通过 gRPC 双向流实现服务端主动推送实例变更，替代 1.x 的客户端定时轮询拉取。

### 5.11.5 Trade-off 分析

| 权衡维度 | gRPC 双向流推送（2.5.3） | HTTP 定时轮询（1.x） |
|---------|------------------------|-------------------|
| **实时性** | 服务端即时推送 | 定时轮询延迟（5s） |
| **连接模型** | gRPC 长连接双向流 | HTTP 短连接 |
| **服务端压力** | 仅变更时推送 | 每次轮询都查询 |
| **客户端资源** | 维持长连接 | 频繁建立/关闭连接 |
| **容灾降级** | 本地 ServiceInfo 缓存 | 无本地缓存（每次拉取） |

### 5.11.6 小结

`NacosNamingService` 通过 `NamingClientProxy`（gRPC 通信代理）实现实例注册、服务订阅、实例查询、实例注销四大核心能力。`subscribe()` 通过 `NamingPushRequestHandler`（gRPC 双向流）实现服务端主动推送实例变更通知，`ServiceInfoHolder` 本地缓存减轻服务端查询压力。2.5.3 的 gRPC 双向流替代 1.x HTTP 定时轮询，实例变更通知延迟从 ≤5s 降至 <1s。
### 5.12.1 设计背景

Nacos 2.5.3 的心跳机制基于 gRPC 双向流（Bidirectional Streaming），与 1.x 的 HTTP 短连接心跳（`BeatReactor`）完全不同。客户端通过 gRPC 长连接持续发送 `HealthCheckRequest` 给服务端，服务端 `ClientBeatProcessorV2` 处理心跳请求并更新 `Instance.lastBeatTime`。服务端 `InstanceBeatChecker` 定时检查超时实例，触发 `ClientOperationService.deregisterInstance()` 将超时实例健康状态置为 `DOWN`。

**关键事实**：2.5.3 中 `BeatReactor`（1.x 的 HTTP 心跳实现）已完全移除，全部替换为 gRPC 双向流心跳，心跳响应可携带服务端 IP 列表实现服务发现路由优化。

### 5.12.2 核心类关系图

图 5-12 展示了 Nacos 2.5.3 客户端心跳机制（gRPC 双向流）的完整链路：

```
┌────────────────────────────────────────────────────────────────┐
│                    客户端（gRPC 长连接）                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  RpcClient (gRPC Bidirectional Streaming)           │  │
│  │  ├─ healthCheck(): gRPC HealthCheckRequest         │  │
│  │  └─ connectionId: String                           │  │
│  └──────────────────────────────────────────────────────┘  │
│        │                                                  │
│        │ gRPC HealthCheckRequest (双向流, 默认 5s)       │
│        │ { clientId, instances[], beatInterval }           │
└────────┼──────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│                    服务端                                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │          ClientBeatProcessorV2                       │  │
│  │  ┌────────────────────────────────────────────────┐  │  │
│  │  │ handle(request, meta): HealthCheckResponse    │  │  │
│  │  │  STEP 1: clientManager.getClient(clientId)   │  │  │
│  │  │  STEP 2: client.setLastUpdatedTime(now())    │  │  │
│  │  │  STEP 3: response.setServerIpList(            │  │  │
│  │  │          getHealthyServerList())              │  │  │
│  │  │  STEP 4: return HealthCheckResponse           │  │  │
│  │  └────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │          InstanceBeatChecker                        │  │
│  │  ┌────────────────────────────────────────────────┐  │  │
│  │  │ @Scheduled(fixedRate = 5000)               │  │  │
│  │  │ checkInstanceBeat():                         │  │  │
│  │  │   for each Instance:                          │  │  │
│  │  │     if (now - lastBeatTime > expireTime)     │  │  │
│  │  │       → deregisterInstance(instance)          │  │  │
│  │  │       → changeState(DOWN)                    │  │  │
│  │  └────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        │ deregisterInstance() → Instance.state = DOWN
        ▼
┌────────────────────────────────────────────────────────────────┐
│         ClientOperationService                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ deregisterInstance(Instance): void                  │  │
│  │   → NotifyCenter.publishEvent(                     │  │
│  │       InstanceMetadataEvent)                        │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        │ InstanceMetadataEvent → DistroProtocol.sync()
        ▼
┌────────────────────────────────────────────────────────────────┐
│  DistroProtocol.sync(distroKey, DELETE)                     │
│  → Distro v2 集群同步: 其他节点删除该 Instance         │
└────────────────────────────────────────────────────────────────┘
```

### 5.12.3 源码走读

#### 5.12.3.1 ClientBeatProcessorV2——gRPC 心跳处理

`ClientBeatProcessorV2`（`naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatProcessorV2.java:40-78`）——服务端 gRPC 心跳处理器：

```java
public class ClientBeatProcessorV2 
    implements RequestHandler<HealthCheckRequest, HealthCheckResponse> {
    
    @Override
    public HealthCheckResponse handle(HealthCheckRequest request, RequestMeta meta) 
        throws NacosException {
        // line 50: 从 ClientManager 获取客户端连接对象
        String clientId = request.getClientId();
        Client client = clientManager.getClient(clientId);
        if (client == null) {
            return new HealthCheckResponse(false);
        }
        // line 58-62: 更新最后一次心跳时间
        client.setLastUpdatedTime(System.currentTimeMillis());
        // line 65: 构造心跳响应——携带服务端健康 IP 列表
        HealthCheckResponse response = new HealthCheckResponse();
        List<String> healthyServers = serverMemberManager.getServerList()
            .stream().map(Member::getAddress).collect(Collectors.toList());
        response.setServerIpList(healthyServers);
        response.setSuccess(true);
        return response;
    }
}
```

**关键设计**：心跳响应携带服务端健康 IP 列表 `serverIpList`——客户端可用于 `HostReactor.refreshSrvIfNeed()` 更新本地服务端节点列表（服务发现路由优化）。

#### 5.12.3.2 InstanceBeatChecker——超时实例检查

`InstanceBeatChecker`（`naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceBeatChecker.java:45-90`）——定时检查超时实例并触发 `ClientOperationService.deregisterInstance()`：

```java
@Component
public class InstanceBeatChecker {
    // line 48: 每 5 秒执行一次超时检查
    @Scheduled(fixedRate = 5000)
    public void checkInstanceBeat() {
        // line 55: 遍历 Namespace→Service→Cluster→Instance
        for (Instance instance : allInstance()) {
            long now = System.currentTimeMillis();
            // line 60-70: 超时判定
            long expireTime = instance.getInstanceHeartBeatTimeout(); // 默认 15s
            if (now - instance.getLastBeatTime() > expireTime) {
                // line 75: 触发 deregisterInstance()
                clientOperationService.deregisterInstance(instance);
            }
        }
    }
}
```

**超时参数配置**：
- `nacos.naming.healthCheck.expire.time`: 心跳超时时间（默认 15s = 3 × 心跳间隔 5s）
- `nacos.naming.healthCheck.maxFailCount`: 最大失败次数（默认 3）

#### 5.12.3.3 健康状态变更传播链路

当 `InstanceBeatChecker` 检测到实例超时：
1. `clientOperationService.deregisterInstance(instance)`——将 Instance 状态置为 `DOWN`
2. `NotifyCenter.publishEvent(new InstanceMetadataEvent(instance))`——发布事件
3. `DistroProtocol.sync(distroKey, DELETE)`——Distro v2 集群同步删除该 Instance
4. 其他节点收到同步后执行 `DistroClientDataProcessor.onDelete()`——从本地 `ServiceManager` 移除该 Instance

### 5.12.4 设计模式分析

1. **观察者模式（Observer）**：`InstanceBeatChecker.checkInstanceBeat()` 检测到超时→触发 `clientOperationService.deregisterInstance()`→`NotifyCenter.publishEvent()`→`DistroProtocol.sync()`——完整的"检测→通知→同步"观察者链。

2. **超时检测模式（Timeout Detection）**：`InstanceBeatChecker` 通过 `@Scheduled(fixedRate=5000)` 定时扫描 `Instance.lastBeatTime` 与当前时间差值判定超时，`expireTime=15s`（3 × 心跳间隔 5s）容忍瞬时网络抖动。

3. **双向流模式（Bidirectional Streaming）**：gRPC `HealthCheckRequest/Response` 双向流同时承载心跳上报（客户端→服务端）和服务端 IP 列表推送（服务端→客户端），一连接双用途。

4. **断路器模式（Circuit Breaker）**：心跳超时判定 3 次失败后才触发 `deregisterInstance()`——避免瞬时网络抖动误下线健康实例。

### 5.12.5 Trade-off 分析

| 权衡维度 | gRPC 双向流心跳（2.5.3） | HTTP 短连接心跳（1.x BeatReactor） |
|---------|-----------------------------|-------------------------------------|
| **连接模型** | gRPC 长连接双向流 | HTTP 短连接请求-响应 |
| **心跳频率** | 默认 5s | 默认 5s |
| **超时判定** | `InstanceBeatChecker` 15s（3×心跳间隔） | 同左 |
| **响应能力** | 心跳响应携带服务端 IP 列表 | 仅返回心跳确认 |
| **误下线保护** | 3 次超时才触发 deregister | 同左 |
| **服务发现优化** | `serverIpList` 更新 `HostReactor` | 需额外 `GET /serverlist` 请求 |

### 5.12.6 小结

Nacos 2.5.3 的心跳机制基于 gRPC 双向流：`ClientBeatProcessorV2` 处理客户端心跳并更新 `Instance.lastBeatTime` + 响应携带 `serverIpList` 优化服务发现路由；`InstanceBeatChecker` 定时检查超时实例（15s 默认超时，容忍 3 次失败）触发 `ClientOperationService.deregisterInstance()`→`DistroProtocol.sync()` 集群同步删除超时实例。完整的"心跳上报→超时检测→状态变更→集群同步"闭环保证集群实例健康状态的最终一致性。


### 5.13.1 设计背景

Nacos 客户端 SDK 提供本地缓存快照机制，用于在 Nacos 服务端不可用时进行容灾降级。当服务端不可用时，客户端从本地文件快照中读取上次成功获取的配置/服务实例，保证应用的基本可用性。`FailoverReactor` 负责管理本地文件快照的持久化（服务端可用时每次成功获取后保存）与加载（服务端不可用时从容灾快照读取）。

2.5.3 中，`FailoverReactor` 通过 SPI 机制加载 `FailoverDataSource` 实现（默认本地文件实现或可扩展为其他数据源），定时任务每 `failoverPollIntervalMs`（默认 10s）检查服务端可用性并自动切换容灾模式。

### 5.13.2 核心类关系图

图 5-13 展示了客户端本地缓存快照机制的完整容灾降级链路：

```
┌────────────────────────────────────────────────────────────────┐
│                    FailoverReactor                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - serviceMap: Map<String, ServiceInfo>              │  │
│  │ - failoverSwitchEnable: boolean                     │  │
│  │ - failoverDataSource: FailoverDataSource            │  │
│  │ - executorService: ScheduledExecutorService          │  │
│  │ - serviceInfoHolder: ServiceInfoHolder              │  │
│  │ - instancesDiffer: InstancesDiffer                 │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ + init(): 启动 FileWatcher + FailoverLoadTask     │  │
│  │ + isFailover(): boolean                           │  │
│  │ + onUpdateServiceInfo(serviceName, serviceInfo)    │  │
│  │ + FailoverLoadTask.run(): 定时检查容灾状态      │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│ FailoverDataSource   │    │  FailoverLoadTask              │
│ <<interface>>       │    │  ├─ 每 pollIntervalMs 执行     │
│ ├─ getServiceInfo() │    │  ├─ if !isFailover()         │
│ └─ getAllServiceInfo│    │  │    → serviceInfoHolder     │
│                      │    │  └─ if isFailover()          │
└──────────────────────┘    │    → failoverDataSource.get()  │
        △                  └──────────────────────────────────┘
        │                         │
┌───────┴──────────────┐       ▼
│ DiskFailoverDataSource│  ┌──────────────────────────────┐
│ (默认本地文件实现)   │  │ 本地文件系统                  │
│ ├─ /nacos/naming/   │  │  ~/nacos/naming/data/       │
│ │   failover/data/  │  │  ├─ {serviceName}@          │
│ └────────────────────┘  │  │    @@{group}              │
                         │  └─ 每个服务独立文件存储        │
                         └──────────────────────────────┘
```

### 5.13.3 源码走读

#### 5.13.3.1 FailoverReactor 构造与初始化

`FailoverReactor`（`client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverReactor.java:金→150`）：

```java
public class FailoverReactor implements Closeable {
    // line 58-65: 核心数据结构
    private Map<String, ServiceInfo> serviceMap = new ConcurrentHashMap<>();
    private boolean failoverSwitchEnable;            // 容灾开关
    private final ServiceInfoHolder serviceInfoHolder;  // 本地服务缓存
    private FailoverDataSource failoverDataSource;     // SPI 加载的数据源
    private ScheduledExecutorService executorService;   // 定时任务线程池
    
    public FailoverReactor(ServiceInfoHolder serviceInfoHolder, 
        String notifierEventScope) {
        // line 85-100: SPI 加载 FailoverDataSource
        Collection<FailoverDataSource> dataSources = 
            NacosServiceLoader.load(FailoverDataSource.class);
        for (FailoverDataSource dataSource : dataSources) {
            failoverDataSource = dataSource;
            break;  // 取第一个可用实现
        }
        // line 105: 创建单线程定时任务执行器
        executorService = new ScheduledThreadPoolExecutor(1,
            new NameThreadFactory("com.alibaba.nacos.naming.failover"));
        // line 110: 启动 FailoverSwitchFileWatcher（监听容灾开关文件）
        init();
        // line 115: 启动 FailoverLoadTask——每 10s 检查容灾状态
        executorService.scheduleWithFixedDelay(
            new FailoverLoadTask(), 
            FAILOVER_POLL_INTERVAL_MS,  // 默认 10s
            FAILOVER_POLL_INTERVAL_MS, 
            TimeUnit.MILLISECONDS
        );
    }
}
```

#### 5.13.3.2 FailoverLoadTask——定时容灾检查

`FailoverLoadTask`（内部类）每 `FAILOVER_POLL_INTERVAL_MS=10s` 执行容灾状态检查：

```java
class FailoverLoadTask implements Runnable {
    @Override
    public void run() {
        try {
            if (!failoverSwitchEnable) {
                // line 175-190: 非容灾模式——直接从 Nacos 服务端获取
                List<String> serviceNames = serviceInfoHolder.getAllServiceNames();
                for (String serviceName : serviceNames) {
                    ServiceInfo serviceInfo = serviceInfoHolder
                        .getServiceInfo(serviceName, null, null);
                    if (serviceInfo != null) {
                        onUpdateServiceInfo(serviceName, serviceInfo);
                    }
                }
            } else {
                // line 200-210: 容灾模式——从本地快照读取
                Map<String, ServiceInfo> failoverMap = 
                    failoverDataSource.getAvailableServiceInfos();
                for (Map.Entry<String, ServiceInfo> entry : failoverMap.entrySet()) {
                    // 对比实例差异，触发 InstancesChangeEvent
                    InstancesDiff diff = instancesDiffer.doDiff(
                        serviceMap.get(entry.getKey()), entry.getValue()
                    );
                    if (diff.hasDifferent()) {
                        NotifyCenter.publishEvent(
                            new InstancesChangeEvent(diff.getAdds(), diff.getRemoves(), id)
                        );
                    }
                    serviceMap.put(entry.getKey(), entry.getValue());
                }
            }
        } catch (Throwable ex) {
            NAMING_LOGGER.error("[FailoverLoadTask] error", ex);
        }
    }
}
```

#### 5.13.3.3 FailoverDataSource SPI 加载

`FailoverDataSource`（`client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverDataSource.java`）——SPI 接口定义：

```java
public interface FailoverDataSource {
    // 获取单个服务的容灾备份数据
    ServiceInfo getServiceInfo(String serviceName, String group);
    // 获取所有服务的容灾备份数据
    Map<String, ServiceInfo> getAvailableServiceInfos();
}
```

**默认实现**：`DiskFailoverDataSource`——基于本地文件系统的容灾数据源，文件存储路径 `~/nacos/naming/data/failover/`。

### 5.13.4 设计模式分析

1. **断路器模式（Circuit Breaker）**：`failoverSwitchEnable` 标记服务端不可用，自动切换到容灾模式——从本地文件快照读取服务实例，保证服务基本可用性。

2. **策略模式（Strategy）**：`FailoverDataSource` SPI 接口定义统一契约，`DiskFailoverDataSource`（本地文件）为默认实现，可扩展为 `RedisFailoverDataSource`、`S3FailoverDataSource` 等。

3. **观察者模式（Observer）**：`FailoverLoadTask` 定时检查容灾状态，当服务端恢复时自动切回正常模式，通过 `NotifyCenter.publishEvent(InstancesChangeEvent)` 通知所有订阅者服务实例变更。

4. **快照模式（Snapshot）**：`FailoverReactor` 在服务端可用时每次成功获取 `ServiceInfo` 后持久化到本地文件，服务端不可用时从本地快照读取上次成功的服务实例列表。

### 5.13.5 Trade-off 分析

| 权衡维度 | 本地快照容灾 | 无容灾机制 |
|---------|------------|-----------|
| **服务端不可用时** | 返回上次成功的实例列表 | 应用无法获取任何实例列表 |
| **数据一致性** | 可能与服务端最新实例列表不一致 | N/A |
| **存储开销** | 每个服务独立文件存储（~KB 级） | 无存储开销 |
| **恢复自动性** | 定时 10s 检查自动切回 | N/A |

### 5.13.6 小结

`FailoverReactor` 通过 SPI 机制加载 `FailoverDataSource` 实现（默认 `DiskFailoverDataSource` 本地文件存储），定时 `FailoverLoadTask` 每 10s 检查容灾状态——服务端可用时从 Nacos 获取最新实例并持久化到本地文件，服务端不可用时从本地快照读取上次成功实例列表。断路器模式保证服务端不可用时仍可基本可用，策略模式支持扩展容灾数据源。


### 5.14.1 设计背景

Nacos 通过 Java SPI（Service Provider Interface）机制实现插件化扩展。`NacosServiceLoader` 是 Nacos 对 Java SPI 的封装，支持 `@Order` 注解排序和 SPI 扩展点自动发现。2.5.3 中 SPI 机制应用于认证插件、数据源插件、加密插件等 6 种插件类型（参见第 6 章插件体系深度分析）。

### 5.14.2 核心类关系图

图 5-14 展示了 `NacosServiceLoader` 的 SPI 加载机制——从 `META-INF/services/` 文件声明到 `@Order` 排序的完整流程：

```
┌────────────────────────────────────────────────────────────────┐
│                  NacosServiceLoader                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ static <T> load(Class<T> serviceClass)               │  │
│  │ static <T> load(Class<T> serviceClass,              │  │
│  │               ClassLoader classLoader)               │  │
│  │ static <T> T newServiceInstance(Class<?> clazz)    │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        │ Java SPI ServiceLoader.load(serviceClass, classLoader)
        ▼
┌────────────────────────────────────────────────────────────────┐
│              META-INF/services/                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ com.alibaba.nacos.plugin.auth.spi.                 │  │
│  │   AuthPluginService                                │  │
│  │   └─ com.alibaba.nacos.plugin.auth                 │  │
│  │        .impl.NacosAuthPluginService                 │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ com.alibaba.nacos.plugin.datasource               │  │
│  │   .spi.DataSourcePluginService                     │  │
│  │   └─ com.alibaba.nacos.plugin.datasource          │  │
│  │        .impl.MySQLDataSourcePluginService           │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        │ 排序: @Order 注解值升序
        ▼
┌────────────────────────────────────────────────────────────────┐
│                @Order 排序规则                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ @Order(0)              → 最高优先级（第一个加载）   │  │
│  │ @Order(Integer.MAX_VALUE) → 最低优先级              │  │
│  │ 未标注 @Order         → 默认 Integer.MAX_VALUE     │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│  Nacos 2.5.3 中应用的 6 种 SPI 扩展点                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 1. AuthPluginService       → 认证插件              │  │
│  │ 2. DataSourcePluginService → 数据源插件            │  │
│  │ 3. EncryptionPluginService → 加密插件              │  │
│  │ 4. TracePluginService      → 追踪插件              │  │
│  │ 5. EnvironmentPluginService → 环境插件              │  │
│  │ 6. ControlManagerPluginService → 控制插件          │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 5.14.3 源码走读

#### 5.14.3.1 load()——SPI 加载与 @Order 排序

`NacosServiceLoader.load()`（`common/src/main/java/com/alibaba/nacos/common/spi/NacosServiceLoader.java:42-87`）：

```java
public static <T> Collection<T> load(
    final Class<T> serviceClass, final ClassLoader classLoader) {
    
    // line 48-55: 使用 Java SPI ServiceLoader 加载所有实现
    ServiceLoader<T> serviceLoader = ServiceLoader.load(
        serviceClass, classLoader
    );
    List<T> services = new ArrayList<>();
    for (T service : serviceLoader) {
        services.add(service);
    }
    // line 58-75: 按 @Order 注解排序
    services.sort((o1, o2) -> {
        // 获取 @Order 注解值——未标注默认为 Integer.MAX_VALUE
        Order or1 = o1.getClass().getAnnotation(Order.class);
        Order or2 = o2.getClass().getAnnotation(Order.class);
        int order1 = or1 == null ? Integer.MAX_VALUE : or1.value();
        int order2 = or2 == null ? Integer.MAX_VALUE : or2.value();
        return Integer.compare(order1, order2);
    });
    // line 80: 返回排序后的服务列表
    return services;
}
```

**关键设计**：
- `ServiceLoader.load()` 从 `META-INF/services/{interface_full_name}` 文件读取所有实现类全限定名
- `@Order` 注解排序——值越小优先级越高，未标注 `@Order` 默认最低优先级（`Integer.MAX_VALUE`）

#### 5.14.3.2 NacosServiceLoader 应用实例

**FailoverReactor 加载 FailoverDataSource**（`FailoverReactor.java:90-100`）：

```java
Collection<FailoverDataSource> dataSources = 
    NacosServiceLoader.load(FailoverDataSource.class);
for (FailoverDataSource dataSource : dataSources) {
    this.failoverDataSource = dataSource;
    break;  // 取第一个可用实现（即最高优先级 @Order(0) 的实现）
}
```

#### 5.14.3.3 AuthPluginService SPI 扩展点

`com.alibaba.nacos.plugin.auth.spi.AuthPluginService`——认证插件 SPI 接口：

```java
public interface AuthPluginService {
    // 用户名/密码认证
    boolean login(User user);
    // 获取已认证用户列表
    List<String> getUsers();
    // 获取角色列表
    List<String> getRoles();
    // 获取权限列表
    List<Permission> getPermissions();
    // 检查权限
    boolean hasPermission(String username, String resource, String action);
    // 是否启用认证
    boolean isAuthEnabled();
}
```

默认实现 `NacosAuthPluginService` 通过 BCrypt 密码加密 + JWT Token 生成/验证实现认证。

### 5.14.4 设计模式分析

1. **服务提供者模式（Service Provider）**：Java SPI 机制通过 `META-INF/services/{interface}` 文件声明接口与实现的映射关系，`NacosServiceLoader.load()` 封装 Java SPI `ServiceLoader`。

2. **策略模式（Strategy）**：多个 SPI 实现通过 `@Order` 排序决定优先级，运行时选择最高优先级的实现——如 `FailoverDataSource` 多个实现中取第一个（`@Order(0)` 的最高优先级）。

3. **工厂模式（Factory）**：`NacosServiceLoader.load()` 作为工厂方法返回所有可用的 SPI 实现集合，调用方无需手动管理实例创建。

4. **插件化模式（Plugin）**：6 种插件类型均通过 `NacosServiceLoader` 动态加载，新增插件只需添加 `META-INF/services` 文件和实现类即可，无需修改 Nacos 核心代码。

### 5.14.5 Trade-off 分析

| 权衡维度 | Java SPI | Spring Factories | OSGi |
|---------|---------|----------------|------|
| **复杂度** | 低（标准 Java） | 中 | 高 |
| **动态加载** | 不支持（需重启） | 不支持 | 支持 |
| **排序能力** | `@Order` 注解 | `@Order` 注解 | Service Ranking |
| **依赖注入** | 不支持 | 支持 | 支持 |
| **适用场景** | 简单插件扩展 | Spring 生态集成 | 复杂模块化应用 |

### 5.14.6 小结

`NacosServiceLoader` 封装 Java SPI 机制，通过 `META-INF/services` 文件声明 + `@Order` 排序实现插件化扩展。`load()` 方法通过 `ServiceLoader.load()` 加载所有实现类→按 `@Order` 排序→返回排序后的服务列表。2.5.3 中 6 种插件类型（认证、数据源、加密、追踪、环境、控制管理）均通过此机制实现动态加载。详细的插件体系分析、自定义插件开发指南（5 步从零到部署）与插件热加载机制参见第 6 章插件体系深度分析。
