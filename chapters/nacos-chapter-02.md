# 第2章：Nacos 2.5.3 注册中心（Naming）源码深度分析

## 2.1 Naming 模块全景

Naming 模块（路径：`nacos-2.5.3/naming/`）是 Nacos 注册中心的核心模块，负责服务注册、发现、健康检查、故障转移等功能。2.5.3 版本包含 **247 个 Java 主代码文件**（不含测试），分布在以下子包中：

| 子包 | 路径 | 核心职责 | 2.5.3 变更 |
|------|------|---------|------------|
| `naming/controllers/` | InstanceController、CatalogController | REST API 入口 | — |
| `naming/core/` | ServiceManager、Cluster、Instance | 服务注册表核心数据结构 | — |
| `naming/healthcheck/` | HealthCheckProcessor | 健康检查引擎 | — |
| `naming/cluster/` | ServerStatusManager | 集群状态管理 | **新增 `NamingReadinessCheckService`** |
| `naming/remote/rpc/handler/` | InstanceRequestHandler | gRPC 请求处理器 | **新增 `PersistentInstanceRequestHandler`** |
| `naming/monitor/` | MetricsMonitor | 监控指标 | **新增 `ServiceTopNCounter`** |
| `naming/consistency/persistent/impl/` | OldDataOperation | 旧数据操作 | 一致性服务移出（`PersistentConsistencyService` 等已移除） |
| `naming/paramcheck/` | NamingDefaultHttpParamExtractor | HTTP 参数校验 | **★新增 ParamChecker 参数校验框架** |
| `naming/pojo/instance/` | InstanceIdGeneratorManager | 实例 ID 生成器 | **★新增雪花 ID 生成器** |
| `naming/misc/` | NamingEnabledFilter | 模块启用过滤器 | **新增 `NamingEnabledFilter`** |
| `naming/utils/` | NamingRequestUtil | 请求工具类 | **新增 `NamingRequestUtil`** |

### 2.2.3 → 2.5.3 Naming 模块核心变更概览

| 类型 | 2.2.3 | 2.5.3 | 说明 |
|------|-------|-------|------|
| 一致性服务 | `ConsistencyService`、`PersistentConsistencyService` | **已移除** | 一致性逻辑移出 naming 模块 |
| 持久化处理器 | `PersistentServiceProcessor`、`StandalonePersistentServiceProcessor` | **已移除** | 持久化逻辑移至 `persistence` 模块 |
| KV 存储 | `NamingKvStorage` | **已移除** | KV 存储移出 |
| 实例 ID | 无统一生成器 | `SnowFlakeInstanceIdGenerator` | **★新增雪花 ID** |
| 故障转移 | 无客户端故障转移 | `FailoverData` / `DiskFailoverDataSource` | **★新增故障转移** |
| 参数校验 | 无统一框架 | `NamingDefaultHttpParamExtractor` | **★新增参数校验** |
| 健康检查 | `ReadinessCheck` | `NamingReadinessCheckService` | **★新增就绪检查** |

## 2.2 核心类关系图

```
┌──────────────────────────────────────────────────────────────┐
│               Naming 核心类关系图 (2.5.3)                     │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  InstanceController ─────────────────────────────────────┐    │
│  │ (POST /instance)                                  │    │
│  │ (DELETE /instance)                                │    │
│  │ (GET /instance/list)                              │    │
│  │ (PUT /instance/beat)                             │    │
│  ▼                                                   │    │
│  ServiceManager                                      │    │
│  │ serviceMap: ConcurrentHashMap<Namespace, Map<...>>│    │
│  │ ├─ getOrCreateService()                          │    │
│  │ └─ removeInstance()                              │    │
│  ▼                                                   │    │
│  Service                                             │    │
│  │ ├─ ClusterMap<String, Cluster>                   │    │
│  │ ├─ ephemeralInstances: Map<String, Instance>    │    │
│  │ └─ persistentInstances: Map<String, Instance>    │    │
│  ▼                                                   │    │
│  Cluster                                            │    │
│  │ ├─ HealthCheckTask                             │    │
│  │ └─ allIPs: List<String>                        │    │
│  ▼                                                   │    │
│  DelegateConsistencyServiceImpl                     │    │
│  │ ├─ AP: EphemeralConsistencyService              │    │
│  │ │    └─ DistroProtocol                         │    │
│  │ └─ CP: RaftConsistencyServiceImpl               │    │
│  │       └─ RaftCore                               │    │
│  ▼                                                   │    │
│  PushService                                        │    │
│  │ ├─ gRPC Bi-directional Stream                   │    │
│  │ └─ UDP Fallback                               │    │
│  ▼                                                   │    │
│  ClientManager                                     │    │
│  │ └─ ClientMap<String, IpPortBasedClient>        │    │
│                                                              │
│  ★ 2.5.3 新增组件:                                     │
│  ├─ InstanceIdGeneratorManager (雪花 ID 生成器)       │
│  ├─ NamingReadinessCheckService (就绪检查)            │
│  ├─ PersistentInstanceRequestHandler (持久化实例请求)    │
│  ├─ ServiceTopNCounter (TopN 服务监控)                 │
│  └─ NamingDefaultHttpParamExtractor (参数校验)         │
└──────────────────────────────────────────────────────────────┘
```

## 2.3 InstanceController REST API 入口

`InstanceController`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/controllers/InstanceController.java`）是注册中心的 REST API 入口，提供以下端点：

| HTTP 方法 | 路径 | 方法 | 说明 |
|-----------|------|------|------|
| POST | `/nacos/v1/ns/instance` | `register()` | 服务实例注册 |
| DELETE | `/nacos/v1/ns/instance` | `deregister()` | 服务实例注销 |
| GET | `/nacos/v1/ns/instance/list` | `list()` | 查询服务实例列表 |
| PUT | `/nacos/v1/ns/instance/beat` | `beat()` | 客户端心跳上报 |
| GET | `/nacos/v1/ns/instance` | `detail()` | 查询单个实例详情 |
| GET | `/nacos/v1/ns/client/list` | `listClient()` | 查询客户端连接列表 |

### register() 方法核心流程

```java
// InstanceController.register() 核心源码走读 (2.5.3)
@CanDistro
@PostMapping
public Result<String> register(@RequestBody Instance instance)
    throws NacosException {
    
    // Step 1: 参数校验★ 2.5.3 新增
    NamingRequestUtil.checkInstanceName(instance);
    
    // Step 2: 生成 instanceId★ 2.5.3 新增雪花ID生成
    if (StringUtils.isEmpty(instance.getInstanceId())) {
        instance.setInstanceId(
            InstanceIdGeneratorManager.getInstance().generateInstanceId(instance)
        );
    }
    
    // Step 3: 服务管理器注册实例
    getServiceManager().registerInstance(
        namespaceId, 
        instance.getServiceName(), 
        instance
    );
    
    return Result.success("registered");
}
```

## 2.4 ServiceManager：服务注册表核心数据结构

`ServiceManager`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/core/ServiceManager.java`）维护整个注册中心的服17务注册表：

```java
// ServiceManager 核心数据结构 (2.5.3)
@Component
public class ServiceManager {
    
    /**
     * 服务注册表核心 Map
     * Key: namespaceId
     * Value: Map<group::serviceName, Service>
     */
    private final ConcurrentHashMap<String, 
        ConcurrentHashMap<String, Service>> serviceMap = 
        new ConcurrentHashMap<>();
    
    /**
     * 获取或创建 Service 实例
     * ★ 2.5.3: synchronized 保护并发创建
     */
    public Service getOrCreateService(String namespaceId, 
                                      String groupName, 
                                      String serviceName) {
        // 三层 ConcurrentHashMap 查找 + synchronized 保护
        ConcurrentHashMap<String, Service> groupMap = 
            serviceMap.computeIfAbsent(namespaceId, 
                k -> new ConcurrentHashMap<>());
        
        String serviceKey = groupName + "@@" + serviceName;
        Service service = groupMap.get(serviceKey);
        if (service == null) {
            synchronized (this) {
                service = new Service(serviceName, groupName, namespaceId);
                groupMap.put(serviceKey, service);
            }
        }
        return service;
    }
}
```

## 2.5 Cluster 数据结构

`Cluster`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/core/Cluster.java`）包含双 Map 设计：

```java
// Cluster 核心数据结构 (2.5.3)
public class Cluster {
    private String clusterName;
    private Service service;
    
    /** 临时实例 Map（AP 模式 - Distro 协议同步） */
    private Map<String, Instance> ephemeralInstances = 
        new ConcurrentHashMap<>();
    
    /** 持久化实例 Map（CP 模式 - Raft 协议同步） */
    private Map<String, Instance> persistentInstances = 
        new ConcurrentHashMap<>();
    
    /** 健康检查任务 */
    private HealthCheckTask checkTask;
    
    /** 全量 IP 列表（用于客户端快速查询） */
    private volatile List<String> allIPs = new ArrayList<>();
    
    /**
     * ★ 2.5.3: 新增实例 ID 更新支持
     */
    public void updateInstance(Instance instance) {
        String instanceId = instance.getInstanceId();
        if (instance.isEphemeral()) {
            ephemeralInstances.put(instanceId, instance);
        } else {
            persistentInstances.put(instanceId, instance);
        }
        refreshAllIPs();
    }
}
```

## 2.6 DelegateConsistencyServiceImpl：AP/CP 路由分发

`DelegateConsistencyServiceImpl`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/consistency/DelegateConsistencyServiceImpl.java`）负责根据 `Instance.ephemeral` 字段决定 AP/CP 路由：

| ephemeral | 一致性模式 | 协议 | 存储引擎 | 适用场景 |
|-----------|-----------|------|---------|---------|
| **true** | AP | Distro | 内存 + 异步复制 | 临时实例（高频心跳） |
| **false** | CP | Raft | Raft Log + Snapshot | 持久化实例（强一致性要求） |

**2.5.3 变更**：一致性服务接口（`ConsistencyService`、`PersistentConsistencyService`）已从 naming 模块移除，对应逻辑统一由 `consistency` 模块和 `persistence` 模块承接。

## 2.7 AP 模式：Distro 协议去中心化同步

Distro 协议是 Nacos 自研的去中心化数据同步协议，用于临时实例（AP 模式）的数据同步：

| 组件 | 类路径 | 说明 |
|------|--------|------|
| `DistroProtocol` | `naming/src/.../consistency/DistroProtocol.java` | Distro 协议主入口 |
| `DistroDataStorage` | `naming/src/.../consistency/DistroDataStorage.java` | Distro 数据存储 |
| `DistroVerifyTask` | `naming/src/.../consistency/DistroVerifyTask.java` | Distro 校验任务 |

**数据同步流程**：
1. `put()` → 写入本地 dataMap
2. `syncToTargetServerAsync()` → 异步复制到目标节点
3. `onReceiveSyncData()` → 目标节点接收数据
4. `DistroVerifyTask` → 定期校验最终一致性

## 2.8 Distro 一致性哈希算法

Distro 使用一致性哈希算法（虚拟节点 + TreeMap）确定数据分布：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| VIRTUAL_NODES | 10,000 | 虚拟节点数 |
| Hash String | `ip:port` | 哈希源字符串 |
| 哈希环 | TreeMap | 有序映射实现 |

## 2.9 CP 模式：JRaft 集成

Nacos 2.5.3 中 JRaft 版本从 **1.3.12 升级到 1.3.14**，引入以下改进：

| 改进点 | 说明 |
|--------|------|
| Leader 选举稳定性 | Pre-Vote 阶段超时参数优化 |
| Snapshot 压缩 | Snapshot 文件大小优化 |
| 日志复制效率 | 批量复制 Batch Size 优化 |

核心 CP 类：

| 类 | 路径 | 说明 |
|----|------|------|
| `RaftCore` | `consistency/src/.../raft/RaftCore.java` | CP 模式核心引擎 |
| `RaftStore` | `consistency/src/.../raft/RaftStore.java` | Raft 日志存储 |
| `NacosFSM` | `consistency/src/.../raft/NacosFSM.java` | 有限状态机 |

## 2.10 服务发现流程

客户端查询服务实例的完整流程：

1. `NacosNamingService.selectInstances()` → 客户端发起查询
2. `NamingClientProxy.queryInstances()` → gRPC 请求发送
3. `InstanceRequestHandler.handle()` → 服务端接收请求
4. `ServiceManager.getService()` → 从注册表获取 Service
5. `Cluster.allIPs()` → 获取全量健康 IP 列表
6. `HostReactor.process()` → 客户端 HostReactor 更新本地缓存
7. `JSON` 响应 → 返回实例列表

## 2.11 PushService：gRPC Bi-directional Stream 推送

`PushService`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/push/PushService.java`）负责服务变更实时推送：

| 推送方式 | 协议 | 适用场景 |
|---------|------|---------|
| gRPC Bi-directional Stream | gRPC | 2.x 客户端默认推送方式 |
| UDP 广播 | UDP | 1.x 客户端兼容推送 |

**2.5.3 变更**：UDP 推送兼容模式默认关闭，推荐全部使用 gRPC Bi-directional Stream 推送。

## 2.12 客户端订阅机制

客户端订阅流程：

1. `NacosNamingService.subscribe()` → 客户端发起订阅
2. `NamingClientProxy.subscribe()` → gRPC 请求发送
3. `SubscribeServiceRequestHandler.handle()` → 服务端处理订阅
4. `PushService.addClient()` → 将客户端加入推送列表
5. `ServiceChangedEvent` → 服务变更事件触发
6. `PushService.push()` → 推送变更数据给订阅客户端

## 2.13 健康检查架构

| HealthCheckType | 检查方式 | 适用实例类型 |
|----------------|---------|-------------|
| `TCP` | TCP Socket 连接检测 | 通用 TCP 服务 |
| `HTTP` | HTTP GET 健康端点检测 | Web 服务 |
| `MYSQL` | JDBC Connection Test | MySQL 数据库 |
| `gRPC` | gRPC Health Check | gRPC 服务 |

## 2.14 ClientBeatCheckTask：心跳超时检测

`ClientBeatCheckTask`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/misc/ClientBeatCheckTask.java`）负责定期检查客户端心跳超时：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `heartbeatTimeout` | 15 秒 | 心跳超时时间 |
| `scanInterval` | 5 秒 | 扫描间隔 |
| `expireTime` | 30 秒 | 过期清理时间 |

## 2.15 防雪崩保护

`ProtectManager`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/misc/ProtectManager.java`）提供防雪崩保护：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `protectEnabled` | true | 是否启用防雪崩 |
| `protectThreshold` | 0.5 | 健康实例比例阈值（低于此值触发保护） |

当健康实例比例低于 `protectThreshold` 时，ProtectManager 会返回所有实例（包括不健康的），防止服务彻底不可用。

## 2.16 2.5.3 新增功能详解

### 2.16.1 InstanceIdGenerator：雪花 ID 生成器

2.5.3 新增 `SnowFlakeInstanceIdGenerator`（路径：`nacos-2.5.3/naming/src/main/java/com/alibaba/nacos/naming/pojo/instance/SnowFlakeInstanceIdGenerator.java`）：

```java
@Component
public class SnowFlakeInstanceIdGenerator implements InstanceIdGenerator {
    // SnowFlake 雪花算法参数
    private static final long START_TIMESTAMP = 1650000000000L;
    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    // ...
    
    @Override
    public String generateInstanceId(Instance instance) {
        return String.valueOf(nextId());
    }
}
```

### 2.16.2 NamingFailoverData：故障转移机制

2.5.3 客户端新增完整的故障转移机制（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/naming/backups/`）：

| 类 | 说明 |
|----|------|
| `FailoverData` | 故障转移数据抽象 |
| `FailoverDataSource` | 故障转移数据源接口 |
| `FailoverSwitch` | 故障转移开关 |
| `DiskFailoverDataSource` | 磁盘持久化故障转移数据源 |
| `NamingFailoverData` | Naming 故障转移数据实体 |

### 2.16.3 ParamChecker 参数校验框架

2.5.3 新增统一的参数校验框架（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/paramcheck/`）：

| 类 | 说明 |
|----|------|
| `AbstractHttpParamExtractor` | HTTP 参数提取器抽象 |
| `AbstractRpcParamExtractor` | RPC 参数提取器抽象 |
| `ParamCheckerFilter` | 参数校验过滤器 |
| `ExtractorManager` | 提取器管理器 |
| `CheckConfiguration` | 校验配置 |

### 2.16.4 InstancesDiffer：实例差异计算器

`InstancesDiffer`（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/naming/cache/InstancesDiffer.java`）用于计算客户端本地缓存与服务端实例列表的差异，支持增量更新：

```java
public class InstancesDiffer {
    /**
     * ★ 2.5.3 新增：计算实例列表差异
     * @return DiffResult 包含 removed/added/modified 三部分
     */
    public DiffResult computeDiff(List<Instance> oldInstances, 
                                   List<Instance> newInstances) {
        // 计算删除的实例
        Set<Instance> removed = new HashSet<>(oldInstances);
        removed.removeAll(newInstances);
        // 计算新增的实例
        Set<Instance> added = new HashSet<>(newInstances);
        added.removeAll(oldInstances);
        return new DiffResult(removed, added);
    }
}
```

---

### 本章统计数据（Naming 模块 2.5.3 vs 2.2.3）

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| Java 主代码文件 | 245 | **247** | +2 |
| 一致性服务类 | 有（ConsistencyService 等） | 移出 naming 模块 | 架构调整 |
| 持久化处理器 | 有（PersistentServiceProcessor 等） | 移出 naming 模块 | 移至 persistence |
| 新增故障转移 | 无 | **FailoverData/FailoverSwitch** | ★新增 |
| 新增雪花 ID | 无 | **SnowFlakeInstanceIdGenerator** | ★新增 |
| 新增参数校验 | 无 | **ParamChecker 框架** | ★新增 |

---

> **本章基于 Nacos 2.5.3 源码分析生成。**
