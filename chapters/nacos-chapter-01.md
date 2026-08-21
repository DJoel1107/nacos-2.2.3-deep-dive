# 第1章：Nacos 2.5.3 整体架构概述

## 1.1 Nacos 定位与核心能力矩阵

**【设计背景】**

Nacos（Dynamic Naming and Configuration Service）是阿里巴巴开源的生产级中间件，在云原生微服务体系中承担**服务发现（Service Discovery）**和**配置管理（Configuration Management）**双重核心角色。与 Spring Cloud Alibaba、Dubbo、gRPC 等主流微服务生态深度集成。Nacos 2.5.3 相比 2.2.3，Java 文件总数从 1,925 增至 2,460（+535），新增 `persistence/` 独立持久化模块和 `logger-adapter-impl/` 日志适配器模块。

Nacos 的核心定位可概括为三大能力域：服务发现、配置管理和服务治理。这三者构成了云原生微服务基础设施的完整能力矩阵。

**【核心类关系图】**

```
/* 图 1-1：Nacos 2.5.3 三大核心能力域与外部生态集成全景（基于 Nacos 2.5.3 源码） */
                          ┌────────────────────────────┐
                          │      Spring Cloud Alibaba    │
                          │  (服务发现 + 配置管理)      │
                          └────────────┬───────────────┘
                                       │
                          ┌────────────▼───────────────┐
                          │          Dubbo               │
                          │  (RPC 服务注册与发现)        │
                          └────────────┬───────────────┘
                                       │
┌─────────────────────────────────────┼─────────────────────────────────────┐
│                               Nacos 2.5.3                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐        │
│  │   服务发现       │  │   配置管理       │  │   服务治理       │        │
│  │  (Naming模块)   │  │  (Config模块)    │  │  (Sentinel集成)  │        │
│  │  · 服务注册      │  │  · 配置发布      │  │  · 流量控制      │        │
│  │  · 服务发现      │  │  · 配置订阅      │  │  · 熔断降级      │        │
│  │  · 健康检查      │  │  · 灰度发布      │  │  · 负载均衡      │        │
│  │  · AP(Distro)     │  │  · 历史回滚      │  │  · 热点防护      │        │
│  │  · CP(JRaft)      │  │  · Beta/Tag发布   │  │  · 系统自适应    │        │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘        │
│           │                     │                     │                  │
│  ┌────────▼─────────────────────▼─────────────────────▼──────────┐     │
│  │          底层能力：gRPC双向流 / HTTP REST / DNS-F              │     │
│  │          存储后端：Derby（内置）/ MySQL（生产集群）            │     │
│  │          一致性协议：Distro（AP）/ JRaft（CP）                  │     │
│  └──────────────────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────────────────┘
                                       │
                          ┌────────────▼───────────────┐
                          │         gRPC Ecosystem         │
                          │  (Bi-directional Stream)      │
                          └────────────────────────────┘
```

**【源码走读】**

Nacos 2.5.3 的三大核心能力在源码层面有明确的模块边界：

**一、服务发现（Naming Module）**

服务发现的核心入口为 `InstanceController`（naming/src/main/java/com/alibaba/nacos/naming/controllers/InstanceController.java:1-466），提供 `register()` 方法（第 88-145 行）处理客户端服务注册请求。注册流程如下：

1. `InstanceController.register()`（:88-95）解析请求参数：`namespaceId`、`groupName`、`serviceName`，调用 `parseInstance()` 从请求体中解析出 `Instance` 对象
2. `ServiceManager.getOrCreateService(namespaceId, groupName, serviceName)`（naming/src/main/java/com/alibaba/nacos/naming/core/v2/ServiceManager.java:45-52）在 `serviceMap`（`Map<String, Service>`）中惰性创建或获取 Service 对象
3. `Service.addInstance(clusterName, instance)`（:102）将 Instance 添加到对应 Cluster 的 `ephemeralInstances`（临时实例 Map）或 `persistentInstances`（持久化实例 Map）
4. `DelegateConsistencyServiceImpl.put(key, instances)`（naming/src/main/java/com/alibaba/nacos/naming/consistency/DelegateConsistencyServiceImpl.java:67-78）根据 `ephemeral` 字段路由：`true` → AP（Distro 协议去中心化同步），`false` → CP（JRaft 协议 Raft 日志复制）

服务发现的查询入口为 `InstanceController.list()`，返回过滤健康实例的 JSON 响应。

**二、配置管理（Config Module）**

配置管理的核心入口为 `ConfigController`（config/src/main/java/com/alibaba/nacos/config/server/controller/ConfigController.java:1-1016）。`publishConfig()` 方法（第 156-200 行）处理配置发布：白名单校验 → 参数合法性验证 → `ConfigCacheService.dump()` 持久化到 MySQL（生产）/ Derby（单机）→ `NotifyCenter.publishEvent(ConfigDataChangeEvent)` 异步通知已订阅客户端。

配置订阅基于 `LongPollingService`（config/src/main/java/com/alibaba/nacos/config/server/service/LongPollingService.java）：客户端发起长轮询请求，服务端持有连接最多 29.5 秒，配置发生变更时立即返回新配置内容；若超时无变更，返回空响应，客户端重新发起长轮询。

**三、服务治理（Sentinel 集成）**

Nacos 2.5.3 本身不内置流量控制引擎，而是通过与 Sentinel 集成实现服务治理能力。Sentinel 以 Nacos 作为动态规则数据源，将流量控制规则、熔断降级规则、热点参数规则等持久化在 Nacos 配置中心，Sentinel 客户端通过 Nacos SDK 订阅规则变更，实现动态规则实时生效。

**【设计模式分析】**

1. **分层解耦模式**：Naming 和 Config 模块通过 `ConsistencyService` 接口与引擎层解耦。Naming 模块不关心底层一致性协议的实现细节——只需调用 `ConsistencyService.put()` 方法，`DelegateConsistencyServiceImpl` 根据 `ephemeral` 字段自动路由到 AP 或 CP 实现。这是策略模式（Strategy Pattern）的典型应用：`EphemeralConsistencyService`（Distro）和 `RaftConsistencyServiceImpl`（JRaft）是两种具体的共识策略，`DelegateConsistencyServiceImpl` 充当上下文持有一个策略引用。

2. **观察者模式（Observer Pattern）**：`NotifyCenter`（common/src/main/java/com/alibaba/nacos/common/notify/NotifyCenter.java）是 Nacos 内部事件驱动的核心。`ConfigDataChangeEvent` 发布时，所有订阅了该事件类型的 `Subscriber` 都会收到通知并执行相应的回调（如 `AsyncNotifyService` 向集群其他节点发送 HTTP 通知）。这种设计使得配置发布和通知逻辑完全解耦。

**【小结】**

Nacos 2.5.3 以服务发现、配置管理、服务治理三大核心能力矩阵，通过分层模块设计和服务端推送机制，支撑云原生微服务基础设施。2.5.3 新增的 `persistence/` 和 `logger-adapter-impl/` 模块进一步强化了存储抽象和日志适配能力。


## 1.2 1.x → 2.x 架构演进：HTTP 短连接 → gRPC 长连接

**【设计背景】**

Nacos 1.x 基于 HTTP 短连接 + UDP 广播架构，存在连接开销大（每次请求需 TCP+TLS 握手）、服务端无法主动推送（依赖客户端轮询）、UDP 广播不可靠等问题。Nacos 2.x 进行了革命性的架构升级：核心通信协议从 HTTP 短连接升级为 gRPC 长连接 + Bi-directional Stream，连接模型从无状态变为 ConnectionManager 管理的长连接生命周期，服务端推送从不可靠的 UDP 广播升级为可靠的 gRPC Bi-directional Stream 推送。

**【核心类关系图】**

```
/* 图 1-2：Nacos 1.x vs 2.x 架构对比全景 */
┌─────────────────────────────────────────────────────────────────┐
│                     Nacos 1.x (HTTP 短连接)                      │
│  ┌──────────┐    HTTP Request     ┌──────────────────────┐  │
│  │  Client  │ ─────────────────▶ │  Nacos Server         │  │
│  │  (SDK)   │ ◀────────────────── │  HTTP REST API        │  │
│  │          │    HTTP Response    │  (无连接状态)         │  │
│  └──────────┘                    │  UDP Broadcast ──▶ ?   │  │
│                                  │  (不可靠推送)         │  │
│  每次请求 = TCP握手 + TLS握手   └──────────────────────┘  │
│  客户端轮询配置变更 / 服务列表                             │
├─────────────────────────────────────────────────────────────────┤
│                     Nacos 2.x (gRPC 长连接)                      │
│  ┌──────────┐    gRPC Bi-di Stream   ┌──────────────────┐   │
│  │  Client  │ ◄═══════════════════▶ │  Nacos Server     │   │
│  │  (SDK)   │    一条长连接          │  GrpcSdkServer   │   │
│  │          │    服务端主动推送       │  ConnectionMgr   │   │
│  │          │    客户端能力协商       │  ClientAbilities │   │
│  └──────────┘                        └──────────────────┘   │
│  一次握手, 后续复用, 服务端推送延迟 < 1ms              │
└─────────────────────────────────────────────────────────────────┘
```

**【源码走读】**

Nacos 1.x 到 2.x 的架构演进体现在四个核心维度：

**一、通信层：HTTP 短连接 → gRPC 长连接**

Nacos 1.x 的 HTTP 通信模式中，每次客户端请求都需要经过完整的三次 TCP 握手 + TLS 握手，连接在请求完成后立即释放。这导致两个致命问题：(1) 高并发场景下连接建立/销毁开销巨大；(2) 服务端无法主动向客户端推送数据——客户端必须通过周期性轮询（默认每 5 秒一次）来检测配置变更或服务列表变更，延迟可达数秒且浪费大量网络带宽。

Nacos 2.x 升级为 gRPC 长连接架构。`GrpcSdkServer`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java:1-133）负责处理 SDK 客户端的 gRPC 连接。`GrpcSdkServer.start()`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java:89-120）：

```java
// GrpcSdkServer.start()（core/.../GrpcSdkServer.java:89-120）
@Override
public void start() {
    super.start();                        // BaseGrpcServer: 绑定端口 + 注册拦截器
    grpcCommonRequestAcceptor.start();    // 启动通用请求接收器
    grpcBiStreamRequestAcceptor.start();  // 启动 Bi-directional Stream 接收器
}
```

绑定 gRPC SDK 服务端口（默认 `server.port + 1000`），通过 `GrpcBiStreamRequestAcceptor`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcBiStreamRequestAcceptor.java）接收客户端 Bi-directional Stream 连接。连接建立后，客户端和服务端可以在同一条 TCP 连接上双向独立发送消息：客户端发送服务注册/配置订阅请求，服务端主动推送配置变更和服务实例变更通知。

关键优化：(1) gRPC 基于 HTTP/2 多路复用，单条 TCP 连接可承载多个并发 Stream，彻底消除 1.x 的连接建立开销；(2) Bi-directional Stream 实现真正的服务端推送，推送延迟从秒级降至毫秒级。

**二、连接管理：无状态 → ConnectionManager 生命周期管理**

Nacos 1.x 的 HTTP 连接是无状态的——服务端不维护客户端连接信息，每次请求独立处理。这导致无法实现客户端粒度的推送、无法感知客户端离线。

Nacos 2.x 引入 `ConnectionManager`（core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java:1-356）管理所有客户端 gRPC 长连接的生命周期。每个连接由 `Connection` 对象封装，包含唯一 `connectionId`、`ClientAbilities`（客户端能力标识，如是否支持增量推送）、连接建立时间、最后活跃时间等元数据。`ConnectionManager.register()` 注册新连接，`ConnectionManager.unregister()` 注销断开连接，`ConnectionManager.refresh()` 刷新心跳时间戳。

**三、服务端推送：UDP 广播 → gRPC Bi-directional Stream**

Nacos 1.x 使用 UDP 广播向所有客户端推送服务变更通知。UDP 是不可靠传输协议——推送消息可能在网络传输中丢失，且无法确认客户端是否收到。在跨网段部署中，UDP 广播可能被网络设备拦截。

Nacos 2.x 通过 gRPC Bi-directional Stream 实现可靠的服务端推送。当服务实例发生变更（注册/注销/健康状态变化）时，`PushService`（naming/src/main/java/com/alibaba/nacos/naming/push/v2/PushService.java）通过 `ConnectionManager` 获取所有订阅了该服务的客户端连接，向每个连接的 Bi-directional Stream 发送推送消息。客户端 `ServerPushHandler` 接收推送并触发本地 ServiceInfo 更新回调。推送基于 TCP 可靠传输，保证消息必达。

**四、能力协商：ClientAbilities 机制**

Nacos 2.x 新增客户端能力协商机制。`ClientAbilities`（core/src/main/java/com/alibaba/nacos/core/remote/ClientAbilities.java）定义了客户端支持的能力集合，包括：(1) 是否支持增量推送；(2) 是否支持 gRPC 健康检查；(3) 是否支持 TLS 传输加密。客户端在建立 gRPC 连接时上报自身能力，服务端根据 `ClientAbilities` 动态调整与该客户端的通信策略——例如，不支持增量推送的旧客户端仍使用全量推送模式。

Nacos 2.5.3 相比 2.2.3 进一步升级了能力系统：`ClientAbilities` 从基础布尔字段升级为完整的 `AbilityKey`/`AbilityMode`/`AbilityStatus` 体系，支持更细粒度的能力协商。

| 维度 | Nacos 1.x | Nacos 2.5.3 |
|------|-----------|-------------|
| 通信协议 | HTTP 短连接 | gRPC Bi-directional Stream（HTTP/2 多路复用） |
| 连接管理 | 无状态（每次请求独立） | ConnectionManager 管理长连接生命周期 |
| 服务端推送 | UDP 广播（不可靠） | gRPC Bi-di Stream 推送（TCP 可靠传输） |
| 客户端标识 | 无唯一标识 | connectionId + ClientAbilities 能力协商 |
| 健康检查 | TCP/HTTP/MySQL | 新增 gRPC 健康检查 |
| 增量同步 | 不支持 | 支持增量变更推送 |
| Spring Boot | 1.5.x（1.x） | 2.7.18（2.5.3） |
| gRPC 版本 | — | 1.75.0（2.5.3） |

**【设计模式分析】**

1. **模板方法模式（Template Method Pattern）**：`BaseGrpcServer`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/BaseGrpcServer.java）定义了 gRPC 服务端的通用启动和拦截器注册流程，`GrpcSdkServer` 和 `GrpcClusterServer` 作为子类实现各自的 Service 绑定逻辑——避免了 gRPC 服务端代码的重复。

2. **观察者模式（Observer Pattern）**：`ConnectionManager` 维护了所有客户端连接的注册表。当连接状态变化（注册/注销/超时）时，`ConnectionManager` 通知所有注册的 `ConnectionEventListener`，触发相应的业务逻辑（如 `PushService` 更新推送目标列表）。这种设计使得连接管理和业务逻辑完全解耦。

3. **策略模式（Strategy Pattern）**：`ClientAbilities` 的能力协商本质上是策略模式——服务端根据客户端上报的能力集合，动态选择通信策略（全量推送 vs 增量推送、TCP 健康检查 vs gRPC 健康检查）。这种设计使得同一个 Nacos 集群可以同时服务不同能力的客户端版本。

**【小结】**

Nacos 2.x 的架构演进核心在于通信层从 HTTP 短连接升级为 gRPC 长连接 + Bi-directional Stream，实现真正的服务端推送能力，将推送延迟从秒级降至毫秒级。ConnectionManager 的连接生命周期管理 + ClientAbilities 能力协商机制进一步增强了客户端管理的精细度和可扩展性。

## 1.3 整体架构分层详解

**【设计背景】**

Nacos 2.5.3 采用严格的四层架构设计：接入层（Access Layer）、业务层（Business Layer）、引擎层（Engine Layer）和存储层（Storage Layer）。这种分层实现了关注点分离：接入层负责协议适配与请求路由，业务层承载 Naming（注册中心（Service Discovery））和 Config（配置中心（Configuration Management））两个核心业务域，引擎层提供一致性协议、集群管理和 gRPC 通信等底层能力，存储层支撑数据持久化。每层通过明确的接口契约解耦，可独立演进而不影响其他层。

**【核心类关系图】**

```
/* 图 1-3：Nacos 2.5.3 四层架构全景图 */
┌───────────────────────────────────────────────────────────────────┐
│                         接入层 (Access Layer)                      │
│ ┌──────────────────────┐  ┌──────────────────────┐            │
│ │   GrpcSdkServer     │  │   HTTP REST API     │            │
│ │  (core/remote/grpc) │  │ (naming/controllers │            │
│ │   Bi-directional    │  │  config/controllers)│            │
│ │   Stream 推送       │  │  OpenAPI 兼容      │            │
│ └──────────┬─────────┘  └──────────┬─────────┘            │
├────────────┼───────────────────────┼──────────────────────────┤
│            │       业务层 (Business Layer)                       │
│ ┌──────────▼──────────┐  ┌───────▼────────────────┐        │
│ │   Naming Module     │  │   Config Module        │        │
│ │  (naming/ 247files) │  │  (config/ 217 files)  │        │
│ │  InstanceController │  │  ConfigController     │        │
│ │  ServiceManager     │  │  ConfigCacheService   │        │
│ │  HealthCheck        │  │  LongPollingService  │        │
│ │  ConsistencyService  │  │  AsyncNotifyService  │        │
│ └──────────┬─────────┘  └───────┬────────────────┘        │
├────────────┼───────────────────────┼──────────────────────────┤
│            │       引擎层 (Engine Layer)                        │
│ ┌──────────▼──────────────────────────▼───────────────────┐   │
│ │                  Core Module (core/ 230 files)            │   │
│ │ ┌────────────┐ ┌─────────────┐ ┌───────────────────┐ │   │
│ │ │ Consistency │ │  Cluster   │ │  Remote (gRPC)    │ │   │
│ │ │ Service    │ │  Member    │ │  ConnectionMgr    │ │   │
│ │ │ ┌────────┐│ │  Manager   │ │  GrpcSdkServer   │ │   │
│ │ │ │ Distro ││ │  ServerList│ │  GrpcClusterSrv │ │   │
│ │ │ │ (AP)   ││ │  Finder    │ │  RequestAcceptor │ │   │
│ │ │ ├────────┤│ └─────────────┘ └───────────────────┘ │   │
│ │ │ │ JRaft  ││                                        │   │
│ │ │ │ (CP)   ││  NotifyCenter  +  NamespaceManager      │   │
│ │ │ └────────┘│                                        │   │
│ │ └────────────┘                                        │   │
│ └───────────────────────┬───────────────────────────────┘   │
├─────────────────────────┼──────────────────────────────────────┤
│                         │  存储层 (Storage Layer)               │
│ ┌───────────────────────▼───────────────────────────────┐   │
│ │            persistence/ 模块 (2.5.3 新增, 37 files)      │   │
│ │ ┌──────────────────┐  ┌──────────────────────────────┐ │   │
│ │ │  Embedded Derby  │  │  External MySQL (生产集群)  │ │   │
│ │ │  (单机内置)     │  │  主从复制 + 读写分离       │ │   │
│ │ └──────────────────┘  └──────────────────────────────┘ │   │
│ └──────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────┘
```

**【源码走读】**

Nacos 2.5.3 的四层架构在源码层面通过模块边界和包结构严格实施。以下逐层分析各层的核心组件和交互机制。

**一、接入层：协议适配与请求路由**

接入层负责客户端请求的协议适配和路由分发。Nacos 2.5.3 提供两种接入方式：

1. **gRPC 接入（主要通道）**：`GrpcSdkServer`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java:1-133）负责处理 SDK 客户端的 gRPC 请求。`GrpcSdkServer.start()` 方法绑定 gRPC 服务端口（默认 `server.port + 1000`），并通过 `GrpcBiStreamRequestAcceptor`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcBiStreamRequestAcceptor.java）接收客户端 Bi-directional Stream 连接。每个客户端连接由 `GrpcConnection`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcConnection.java）封装。

2. **HTTP 接入（兼容通道）**：REST API 通过 `InstanceController`（naming/src/main/java/com/alibaba/nacos/naming/controllers/InstanceController.java）和 `ConfigController`（config/src/main/java/com/alibaba/nacos/config/server/controller/ConfigController.java）提供 HTTP 接口，兼容 Nacos 1.x 客户端和非 Java 语言 SDK。

接入层通过 `RequestAcceptor` 接口（core/src/main/java/com/alibaba/nacos/core/remote/RequestAcceptor.java）抽象请求处理，`GrpcRequestAcceptor`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcRequestAcceptor.java）为 gRPC 实现。请求路由通过 `RemoteParamCheckFilter`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/RemoteParamCheckFilter.java）进行参数校验过滤。

**二、业务层：Naming（注册中心）+ Config（配置中心）**

业务层是 Nacos 的核心价值承载层，包含两大业务模块：

**Naming 模块**（naming/，247 个 Java 文件）提供完整的服务注册与发现能力。核心组件：
- `InstanceController.register()`（naming/src/main/java/com/alibaba/nacos/naming/controllers/InstanceController.java:88-145）：接收客户端服务注册请求
- `ServiceManager`（naming/src/main/java/com/alibaba/nacos/naming/core/v2/ServiceManager.java）：服务注册表核心数据结构，维护 `serviceMap`（`Map<String, Service>`），通过 `getOrCreateService()` 方法实现惰性创建
- `ConsistencyService`（naming/src/main/java/com/alibaba/nacos/naming/consistency/ConsistencyService.java）：一致性服务接口，根据 Instance 的 `ephemeral` 字段路由到 AP（DistroConsistencyServiceImpl）或 CP（RaftConsistencyServiceImpl）

**Config 模块**（config/，217 个 Java 文件）提供完整的配置管理能力。核心组件：
- `ConfigController.publishConfig()`（config/src/main/java/com/alibaba/nacos/config/server/controller/ConfigController.java:156-200）：接收客户端配置发布请求
- `ConfigCacheService`（config/src/main/java/com/alibaba/nacos/config/server/service/ConfigCacheService.java）：

```java
// ConfigCacheService.dump()（config/.../ConfigCacheService.java:420-440）
public boolean dump(String dataId, String group, String tenant, String content, long timeoutMs) {
    // 1. 持久化到 MySQL / Derby
    persistService.insertOrUpdate(dataId, group, tenant, content);
    // 2. 更新本地缓存 MD5
    String md5 = MD5.getInstance().getMD5String(content);
    updateMd5(dataId, group, tenant, md5, System.currentTimeMillis());
    return true;
}
```

负责配置的本地缓存、MD5 对比和持久化触发
- `LongPollingService`（config/src/main/java/com/alibaba/nacos/config/server/service/LongPollingService.java）：长轮询核心引擎，客户端订阅配置后，服务端持有请求 29.5 秒，有变更时立即返回

业务层通过接口与引擎层解耦：Naming 模块通过 `ConsistencyService` 接口调用引擎层的一致性协议能力，Config 模块通过 `DataSourceService`（persistence/src/main/java/com/alibaba/nacos/persistence/datasource/DataSourceService.java）接口调用存储层持久化能力。

**三、引擎层：集群、一致性、通信**

引擎层（core/，230 个 Java 文件）提供底层基础能力：

1. **一致性服务**：`DelegateConsistencyServiceImpl`（naming/src/main/java/com/alibaba/nacos/naming/consistency/DelegateConsistencyServiceImpl.java）根据 `ephemeral` 字段路由到 AP（Distro）或 CP（JRaft）。Distro 协议通过一致性哈希算法分发数据，JRaft 通过 Leader 选举和日志复制保证强一致性。

2. **集群管理**：`ServerMemberManager`（core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java）管理集群成员信息，`ServerListManager` 维护服务端地址列表。集群节点间通过 gRPC 集群通道（`GrpcClusterServer`，core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcClusterServer.java）进行节点间数据同步。

3. **连接管理**：`ConnectionManager`（core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java）管理所有客户端 gRPC 长连接的生命周期，包括注册、注销、心跳检测和能力协商（`ClientAbilities`）。

4. **事件驱动**：`NotifyCenter`（common/src/main/java/com/alibaba/nacos/common/notify/NotifyCenter.java）提供模块间的事件发布/订阅机制，实现模块间解耦通信。

**四、存储层：数据持久化抽象**

存储层由 **2.5.3 新增的 `persistence/` 独立模块**（37 个 Java 文件）实现，将原来分散在 `config` 和 `core` 模块中的数据源管理统一抽离。核心设计：

- `DataSourceService`（persistence/src/main/java/com/alibaba/nacos/persistence/datasource/DataSourceService.java）：数据源服务接口，定义数据源初始化、健康检查、关闭等生命周期管理
- `DynamicDataSource`（persistence/src/main/java/com/alibaba/nacos/persistence/datasource/DynamicDataSource.java）：动态数据源实现，通过 `StorageConfiguration`（persistence/src/main/java/com/alibaba/nacos/persistence/configuration/StorageConfiguration.java）条件注解根据配置自动切换 Derby（嵌入式）或 MySQL（外部）
- `ExternalDataSourceServiceImpl` 和 `EmbeddedStoragePersistServiceImpl` 分别实现外部 MySQL 和嵌入式 Derby 的具体持久化逻辑

存储层通过 `DataSourceService` 接口向上暴露数据访问能力，业务层（Config 模块）通过此接口进行配置数据的持久化操作，实现了存储后端的可替换性。

**【设计模式分析】**

1. **分层架构模式（Layered Architecture）**：四层架构严格分离关注点，每层只依赖其直接下层。引擎层通过 `ConsistencyService` 和 `DataSourceService` 接口向上暴露能力，业务层仅依赖接口而非具体实现——这是依赖倒置原则（Dependency Inversion Principle）的典型应用。当存储层从 Derby 切换到 MySQL 时，业务层代码无需任何修改，这验证了分层架构的可替换性。

2. **模板方法模式（Template Method Pattern）**：`BaseGrpcServer`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/BaseGrpcServer.java）定义了 gRPC 服务端的通用启动流程（端口绑定、拦截器注册、Service 注册），`GrpcSdkServer` 和 `GrpcClusterServer` 作为子类实现各自的特定逻辑（SDK 服务 vs 集群同步服务）。这种设计避免了代码重复，并为未来的新 gRPC 服务类型提供了扩展点。

3. **策略模式（Strategy Pattern）**：`DataSourceService` 接口定义了数据访问策略，`ExternalDataSourceServiceImpl`（MySQL 策略）和 `EmbeddedStoragePersistServiceImpl`（Derby 策略）分别实现具体策略。`DynamicDataSource` 作为上下文持有一个 `DataSourceService` 引用，根据 `StorageConfiguration` 条件注解在运行时选择具体策略。这种设计使得添加新的存储后端（如 PostgreSQL）只需增加一个新的策略实现类，符合开闭原则（Open-Closed Principle）。

**【小结】**

Nacos 2.5.3 的四层架构通过严格的模块边界和接口契约实现了关注点分离：接入层处理协议适配，业务层承载核心业务逻辑，引擎层提供底层基础能力，存储层抽象数据持久化。2.5.3 将持久化层独立为 `persistence/` 模块，进一步强化了存储层的可替换性和可测试性。

## 1.4 Maven 模块依赖关系图与模块职责矩阵

**【设计背景】**

Nacos 2.5.3 采用 Maven 多模块工程结构，共 22 个 Maven 模块（相比 2.2.3 的 20 个模块新增 2 个：`persistence/` 独立持久化模块和 `logger-adapter-impl/` 日志适配器模块）。每个模块有明确的职责边界和依赖关系，通过 Maven `<dependency>` 管理模块间依赖。根 POM（`pom.xml`）：

```xml
<!-- nacos-2.5.3/pom.xml（项目根 POM，modules 声明） -->
<groupId>com.alibaba.nacos</groupId>
<artifactId>nacos-all</artifactId>
<version>${revision}</version>
<modules>
    <module>config</module>         <!-- 配置管理模块 -->
    <module>naming</module>         <!-- 服务发现模块 -->
    <module>core</module>           <!-- 核心模块 -->
    <module>console</module>        <!-- 控制台模块 -->
    <module>client</module>         <!-- 客户端 SDK -->
    <module>persistence</module>    <!-- 独立持久化模块（新增于 2.5.3） -->
    <module>plugin</module>         <!-- 插件模块 -->
    <!-- ...其他 15 个子模块 -->
</modules>
```

通过 `<modules>` 声明所有子模块：，统一管理版本号（`<revision>` 属性控制整体版本）。

**【核心类关系图】**

```
/* 图 1-4：Nacos 2.5.3 Maven 模块依赖关系图 */
                        ┌────────────────────────┐
                        │   nacos (根 POM)       │
                        │   revision: 2.5.3      │
                        └───────────┬────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
   ┌────────▼────────┐  ┌───────▼────────┐  ┌────────▼────────┐
   │   api (171 files) │  │ common (210)    │  │  client (136)    │
   │   gRPC Proto定义  │  │  通用工具类     │  │  ConfigService   │
   │   SPI 接口定义   │  │  NotifyCenter   │  │  NamingService   │
   └────────┬────────┘  └───────┬────────┘  └────────┬────────┘
            │                       │                       │
   ┌────────▼────────────────────────▼───────────────────────▼──┐
   │                     业务模块                                  │
   │  ┌────────────────────┐  ┌────────────────────┐            │
   │  │ naming (247)      │  │ config (217)      │            │
   │  │ 服务注册/发现     │  │ 配置发布/订阅     │            │
   │  │ 健康检查/推送    │  │ 长轮询/灰度发布   │            │
   │  └────────┬─────────┘  └────────┬─────────┘            │
   │           │                     │                          │
   │  ┌────────▼─────────────────────▼──────────┐            │
   │  │           core (230)                      │            │
   │  │  集群管理 / gRPC通信 / 连接管理         │            │
   │  │  一致性协议路由 / 命名空间管理           │            │
   │  └────────────────┬──────────────────────────┘            │
   └─────────────────┼──────────────────────────────────────────┘
                     │
   ┌─────────────────┼──────────────────────────────────────────┐
   │                 │         基础设施模块                    │
   │  ┌──────────────▼──────────────┐  ┌────────────────┐ │
   │  │ persistence (37) [2.5.3新增]  │  │ auth (27)      │ │
   │  │ 数据源管理 / Derby/MySQL    │  │ JWT/AK-SK/RBAC│ │
   │  └─────────────────────────────┘  └────────────────┘ │
   │  ┌──────────────┐ ┌──────────────┐ ┌────────────┐  │
   │  │ plugin (7)   │ │ plugin-def-  │ │ address (8) │  │
   │  │ SPI 扩展点   │ │ impl (60)    │ │ 节点寻址    │  │
   │  └──────────────┘ └──────────────┘ └────────────┘  │
   │  ┌──────────────┐ ┌──────────────┐ ┌────────────┐  │
   │  │ istio (30)   │ │ prometheus(7)│ │ sys (19)    │  │
   │  │ MCP over gRPC│ │ Metrics 导出  │ │ 系统过滤器  │  │
   │  └──────────────┘ └──────────────┘ └────────────┘  │
   │  ┌────────────────────────────────────────────────────┐  │
   │  │ logger-adapter-impl (5) [2.5.3新增]           │  │
   │  │ log4j2 日志适配器实现                           │  │
   │  └────────────────────────────────────────────────────┘  │
   └──────────────────────────────────────────────────────────┘
                     │
   ┌─────────────────▼──────────────────────────────────────────┐
   │                   辅助模块                              │
   │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │
   │  │ console (12) │  │ console-ui   │  │distribution │  │
   │  │ 启动入口     │  │ 前端界面     │  │ 打包部署    │  │
   │  │ Nacos.java   │  │              │  │             │  │
   │  └──────────────┘  └──────────────┘  └────────────┘  │
   └──────────────────────────────────────────────────────────┘
```

**【源码走读】**

Nacos 2.5.3 的 22 个 Maven 模块按职责可划分为四个层级：

**一、基础 API 与通用层（3 个模块）**

| 模块 | 文件数 | 职责 | 核心输出 |
|------|--------|------|---------|
| `api` | 171 | gRPC Proto 服务定义 + SPI 扩展接口 | `nacos_grpc_service.proto`、`AuthPluginService` |
| `common` | 210 | 通用工具类、事件总线、异常定义 | `NotifyCenter`、`JacksonUtils`、`NacosException` |
| `client` | 136 | Nacos SDK：`ConfigService` + `NamingService` | `ClientWorker`、`ServerListManager` |

**二、业务模块（2 个模块）**

| 模块 | 文件数 | 职责 | 核心组件 |
|------|--------|------|---------|
| `naming` | 247 | 注册中心：服务注册/发现、健康检查、一致性协议路由 | `InstanceController`、`ServiceManager`、`DelegateConsistencyServiceImpl` |
| `config` | 217 | 配置中心：配置发布/订阅、灰度发布、长轮询 | `ConfigController`、`ConfigCacheService`、`LongPollingService` |

**三、引擎与基础设施层（8 个模块）**

| 模块 | 文件数 | 职责 | 关键类 |
|------|--------|------|--------|
| `core` | 230 | 集群管理、gRPC 通信、连接管理、命名空间 | `ConnectionManager`、`GrpcSdkServer`、`ServerMemberManager` |
| `persistence` | 37 | **[2.5.3 新增]** 独立持久化层 | `DataSourceService`、`DynamicDataSource` |
| `auth` | 27 | 认证与授权：JWT、AK-SK、RBAC | `AuthPluginService`、`JwtTokenManager` |
| `plugin` | 7 | SPI 扩展点接口定义 | `AuthPluginService`、`DataSourcePluginService` |
| `plugin-default-impl` | 60 | 默认插件实现 | `NacosDefaultAuthPluginServiceImpl` |
| `address` | 8 | 节点寻址服务（Address Server） | `AddressServerGeneratorManager` |
| `consistency` | 23 | 一致性协议 API（JRaft/Distro 抽象） | `ConsistencyService` |
| `cmdb` | 9 | CMDB（配置管理数据库）集成 | `LabelEntry` |

**四、辅助与部署层（9 个模块）**

| 模块 | 文件数 | 职责 |
|------|--------|------|
| `console` | 12 | Spring Boot 启动入口 + REST API 控制器注册 |
| `console-ui` | — | 前端 Web UI 资源（HTML/JS/CSS） |
| `distribution` | — | 打包配置（bin/conf/target 目录结构） |
| `istio` | 30 | Istio Service Mesh 集成（MCP over gRPC） |
| `prometheus` | 7 | Prometheus Metrics 数据采集适配 |
| `sys` | 19 | 系统级工具：过滤器、参数校验、JVM 监控 |
| `logger-adapter-impl` | 5 | **[2.5.3 新增]** log4j2 日志适配器 |
| `example` | — | 示例代码 |
| `test` | — | 集成测试 |

### 2.5.3 Maven 依赖版本升级

Nacos 2.5.3 相比 2.2.3，升级了以下关键依赖版本：

| 依赖 | 2.2.3 | 2.5.3 | 变更说明 |
|------|-------|-------|---------|
| Spring Boot | 2.6.14 | **2.7.18** | 升级到 Spring Boot 2.7.x 最新稳定版 |
| gRPC | 1.50.2 | **1.75.0** | 大幅升级，支持 HTTP/2 多路复用优化 |
| Protobuf | 3.21.11 | **3.25.5** | 升级序列化性能 |
| JRaft | 1.3.12 | **1.3.14** | 修复一致性协议边缘问题 |
| Jackson | 2.12.x | **2.18.9** | 统一 JSON 解析版本 BOM |
| MySQL Connector | 8.0.28 | **8.2.0** | 迁移到 `mysql-connector-j` |

新增模块依赖：

```
persistence/
├── configuration/       # 数据源配置 + 存储条件注解
├── datasource/        # DataSourceService 接口 + DynamicDataSource
├── model/            # 分页模型 + Derby 事件
├── monitor/          # 数据源 Metrics 监控
└── repository/       # 嵌入式 Derby 存储实现

logger-adapter-impl/
└── log4j2-adapter/  # Log4j2 日志适配器（实现 common 的 SPI 接口）
```

**【设计模式分析】**

1. **分层架构 + 依赖倒置**：API 层（`api` 模块）定义 SPI 接口，`plugin-default-impl` 提供默认实现。业务模块（`naming`/`config`）只依赖 `api` 层的接口，而非具体实现类——这是依赖倒置原则（Dependency Inversion Principle）的严格实践。当用户自定义鉴权插件时，只需实现 `AuthPluginService` 接口并放入 classpath，Nacos 通过 Java SPI 机制自动加载。

2. **模块化模式（Module Pattern）**：22 个 Maven 模块通过清晰的边界划分实现了物理级模块解耦。每个模块有独立的 `pom.xml` 声明自己的依赖，模块间只能依赖 `api` 或 `common` 等基础模块的接口，不能直接依赖业务模块的内部实现类。这种物理级的模块隔离从根本上杜绝了循环依赖和不正确的依赖方向。

3. **SPI 扩展模式**：`plugin` 模块定义扩展点接口，`plugin-default-impl` 提供默认实现。这种模式使得用户可以在不修改 Nacos 源码的情况下，通过 SPI 机制替换默认实现（如接入外部数据库或自定义认证方式）。

**【小结】**

Nacos 2.5.3 的 22 个 Maven 模块通过清晰的四层物理架构和严格的依赖方向管理，实现了模块间的高度解耦。2.5.3 新增的 `persistence/` 和 `logger-adapter-impl/` 模块进一步完善了持久化抽象和日志适配能力，体现了良好的模块化扩展性。

## 1.5 Spring Boot 启动入口：Nacos.java + @SpringBootApplication 扫描机制

**【设计背景】**

Nacos 2.5.3 服务端基于 Spring Boot 2.7.18 构建。启动入口 `Nacos` 类（console/src/main/java/com/alibaba/nacos/Nacos.java:1-49）使用 `@SpringBootApplication` 注解触发自动配置机制，通过 `spring.factories` 加载各模块的自动配置类，并通过 `@ComponentScan` 自定义过滤器 `NacosTypeExcludeFilter` 控制模块启用/禁用。理解 Nacos 的启动机制需要了解 Spring Boot 的自动配置原理、`@ComponentScan` 的扫描范围控制以及 `NacosTypeExcludeFilter` 的模块过滤机制。

**【核心类关系图】**

```
/* 图 1-5：Nacos 2.5.3 启动类与自动配置机制 */
┌────────────────────────────────────────────────────────────┐
│                  Nacos.java (console/)                       │
│  @SpringBootApplication                                │
│  @ComponentScan(basePackages = "com.alibaba.nacos",   │
│    excludeFilters = {                                   │
│      @Filter(type = CUSTOM,                             │
│        classes = {NacosTypeExcludeFilter.class}),       │
│      @Filter(type = CUSTOM,                             │
│        classes = {TypeExcludeFilter.class}),            │
│      @Filter(type = CUSTOM,                             │
│        classes = {AutoConfigurationExcludeFilter.class}) │
│  })                                                    │
│  @ServletComponentScan                                  │
│  public static void main(String[] args)                 │
│    → SpringApplication.run(Nacos.class, args)          │
└──────────────────────┬─────────────────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │   Spring Boot 启动流程    │
          │   1. 容器初始化           │
          │   2. Environment 准备      │
          │   3. @ComponentScan 扫描   │
          │      └─ NacosTypeExclude  │
          │         Filter 过滤模块    │
          │   4. @EnableAutoConfig    │
          │      └─ spring.factories  │
          │         加载各模块配置     │
          └────────────┬────────────┘
                       │
          ┌────────────▼────────────┐
          │   各模块自动配置类加载     │
          │   ├─ naming/               │
          │   ├─ config/               │
          │   ├─ core/                │
          │   ├─ auth/                │
          │   ├─ persistence/          │
          │   └─ ...                 │
          └──────────────────────────┘
```

**【源码走读】**

Nacos 2.5.3 的启动流程从 `Nacos.main()` 开始，分为以下核心机制：

**一、@SpringBootApplication 复合注解**

`Nacos` 类使用 `@SpringBootApplication` 注解，该注解包含三个核心注解：

1. `@SpringBootConfiguration`：标记当前类为配置类
2. `@EnableAutoConfiguration`：触发 Spring Boot 自动配置机制，通过 `spring.factories` 文件注册各模块的自动配置类。Nacos 在 `resources/META-INF/spring.factories` 中声明了各模块的自动配置类，如 `NamingAutoConfiguration`、`ConfigAutoConfiguration` 等
3. `@ComponentScan`：扫描 `basePackages = "com.alibaba.nacos"` 包及其子包下的所有 Spring 组件（`@Component`、`@Service`、`@Controller` 等）

**二、NacosTypeExcludeFilter：模块启用控制**

`@ComponentScan` 的 `excludeFilters` 使用 `NacosTypeExcludeFilter`（console/src/main/java/com/alibaba/nacos/sys/filter/NacosTypeExcludeFilter.java）来控制哪些模块被加载。该过滤器通过读取模块的 `@NacosModule` 注解来判断模块是否启用。如果一个模块标记为 `enabled = false`，则该模块中的所有 `@Component` 不会被加载——这实现了模块的按需启用/禁用。

**三、@ServletComponentScan：Servlet 组件扫描**

`@ServletComponentScan` 注解自动扫描并注册 `@WebServlet`、`@WebFilter`、`@WebListener` 等 Servlet 组件。Nacos Console UI 模块的静态资源就是通过 Servlet 组件注册的。

**四、SpringApplication.run()：启动 Spring 容器**

`SpringApplication.run(Nacos.class, args)`：

```java
// Nacos.main()（console/src/main/java/com/alibaba/nacos/Nacos.java:42-49）
@SpringBootApplication
@ComponentScan(basePackages = "com.alibaba.nacos", excludeFilters = {
    @Filter(type = FilterType.CUSTOM, classes = {NacosTypeExcludeFilter.class}),
    @Filter(type = FilterType.CUSTOM, classes = {TypeExcludeFilter.class}),
    @Filter(type = FilterType.CUSTOM, classes = {AutoConfigurationExcludeFilter.class})})
public class Nacos {
    public static void main(String[] args) {
        SpringApplication.run(Nacos.class, args);
    }
}
```

触发 Spring Boot 的完整启动流程：创建 `ApplicationContext` → 加载 `Environment` → 执行 `ApplicationRunner`/`CommandLineRunner` → 发布 `ApplicationReadyEvent`。Nacos 各模块通过监听 `ApplicationReadyEvent` 事件来执行各自的初始化逻辑（如 `GrpcSdkServer.start()` 绑定 gRPC 端口）。

**【设计模式分析】**

1. **模板方法模式（Template Method Pattern）**：Spring Boot 的 `SpringApplication.run()` 定义了标准的启动流程模板（容器创建 → 环境准备 → Bean 加载 → 事件发布），各模块通过实现 `ApplicationRunner` 或监听 `ApplicationReadyEvent` 在模板钩子点插入自定义初始化逻辑。

2. **过滤器链模式（Filter Chain Pattern）**：`@ComponentScan` 的 `excludeFilters` 通过多个 `@Filter` 组成过滤器链——`NacosTypeExcludeFilter`（按模块启用状态过滤）、`TypeExcludeFilter`（按类型过滤）、`AutoConfigurationExcludeFilter`（按自动配置过滤）依次过滤扫描到的组件。这种设计实现了灵活的组件加载控制。

3. **观察者模式（Observer Pattern）**：各模块通过监听 `ApplicationReadyEvent` 事件来触发初始化，而非在 `main()` 中硬编码初始化顺序。这种设计使得各模块的初始化逻辑完全解耦，新增模块只需监听事件即可接入启动流程。

**【小结】**

Nacos 2.5.3 通过 `@SpringBootApplication` + `@ComponentScan` + `NacosTypeExcludeFilter` 实现了灵活的模块启用控制，各模块通过 Spring Boot 事件机制在 `ApplicationReadyEvent` 中执行独立初始化，实现了启动流程的高度解耦。

## 1.6 模块独立启动机制：Config.java / NamingApp.java 可独立部署

**【设计背景】**

Nacos 2.5.3 支持各业务模块独立启动——除了启动完整 Nacos 服务（包括 Naming + Config + Console），还支持单独启动 Naming 模块（仅注册中心）或 Config 模块（仅配置中心）。这种独立启动能力使得 Nacos 可以在轻量级场景（如仅需服务发现）中减少资源消耗，或在资源受限环境中按需部署。每个模块有自己的启动类：`NamingApp`（naming/src/main/java/com/alibaba/nacos/NamingApp.java）和 `ConfigApp`（config/src/main/java/com/alibaba/nacos/ConfigApp.java）。

**【核心类关系图】**

```
/* 图 1-6：Nacos 2.5.3 模块独立启动机制 */
┌────────────────────────────────────────────────────────────┐
│                   Nacos.java (完整启动)                    │
│  @SpringBootApplication                                  │
│  @ComponentScan("com.alibaba.nacos")                    │
│  └─ 加载全部模块 (naming + config + core + ...)       │
└────────────────────────────────────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          │            │            │
┌─────────▼──────┐ ┌──▼────────────┐ ┌▼──────────────────┐
│ NamingApp.java │ │ConfigApp.java │ │ (其他独立启动类) │
│ @SpringBootApp │ │@SpringBootApp│ │                    │
│ @ComponentScan │ │@ComponentScan│ │                    │
│ ("com.alibaba │ │("com.alibaba │ │                    │
│  .nacos.naming│ │ .nacos.config│ │                    │
│  .nacos.core") │ │ .nacos.core")│ │                    │
│                │ │              │ │                    │
│ 仅加载:        │ │ 仅加载:      │ │                    │
│ · naming       │ │ · config     │ │                    │
│ · core（部分）  │ │ · core（部分）│ │                    │
│ · common       │ │ · common     │ │                    │
│ · api         │ │ · api       │ │                    │
└────────────────┘ └──────────────┘ └──────────────────────┘
```

**【源码走读】**

Nacos 2.5.3 的模块独立启动机制基于以下核心设计：

**一、独立启动类：缩小 @ComponentScan 扫描范围**

`NamingApp` 使用 `@ComponentScan` 注解将扫描范围限制在 `"com.alibaba.nacos.naming"` 和 `"com.alibaba.nacos.core"` 包（Config 模块及其他不需要的模块被排除），从而实现仅加载 Naming 模块及其最小依赖。同理，`ConfigApp` 将扫描范围限制在 `"com.alibaba.nacos.config"` 和 `"com.alibaba.nacos.core"` 包。

**二、最小依赖裁剪**

独立启动时，Spring Boot 自动配置机制会根据 classpath 中存在的模块自动配置类来决定哪些 Bean 被加载。如果 `config` 模块的 jar 不在 classpath 中，则 `ConfigAutoConfiguration` 不会被加载——这通过 Maven 的 `<optional>` 依赖或独立的 assembly 打包实现。

**三、grpcServer 端口独立绑定**

每个独立启动的模块绑定独立的 gRPC 端口，避免端口冲突：
- 完整 Nacos：`GrpcSdkServer` 绑定 `server.port + 1000`
- Naming 独立：`GrpcSdkServer` 绑定 `server.port + 1000`（仅处理 Naming 请求）
- Config 独立：`GrpcSdkServer` 绑定 `server.port + 1000`（仅处理 Config 请求）

**四、共享 core 模块**

所有独立启动方案都依赖 `core` 模块的基础能力（集群管理、gRPC 通信、连接管理），但非必要的 core 子模块（如 `auth` 认证模块）在独立启动时不会被加载——因为 `auth` 模块的自动配置类不在 classpath 中或 `NacosTypeExcludeFilter` 将其过滤。

**【设计模式分析】**

1. **模块化模式（Module Pattern）**：各模块通过独立的 Maven 模块和启动类实现物理级隔离。`NamingApp` 的 classpath 中不存在 `config` 模块的 jar，从根本上避免了意外的依赖引入。这种物理级模块隔离是 Java 模块化（JPMS）思想的实践。

2. **条件装配模式（Conditional Assembly Pattern）**：Spring Boot 的 `@ConditionalOnClass` / `@ConditionalOnMissingBean` 条件注解使得自动配置类只在必要的类存在于 classpath 时才加载。Nacos 的独立启动利用了这一机制——当 `ConfigController` 不在 classpath 时，Config 模块的自动配置类不会被加载，从而实现了优雅的按需装配。

**【小结】**

Nacos 2.5.3 的模块独立启动机制通过缩小 `@ComponentScan` 范围、Maven 依赖裁剪和 Spring Boot 条件装配，实现了注册中心（Naming）和配置中心（Config）的可独立部署能力，适应不同场景的部署需求。

## 1.7 启动初始化 7 阶段详细流程

**【设计背景】**

Nacos 2.5.3 的启动初始化是一个严格分阶段的过程：从 Spring 容器初始化开始，依次完成环境准备、集群成员加载、一致性协议启动、gRPC 通信层启动、业务模块加载，最后暴露 HTTP 服务。这 7 个阶段有严格的顺序依赖——后续阶段依赖前序阶段的完成（如 gRPC 通信层启动依赖一致性协议已初始化）。这种分阶段设计确保了启动过程的可观测性和失败隔离性。

**【核心类关系图】**

```
/* 图 1-7：Nacos 2.5.3 启动初始化 7 阶段流程 */
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 1: 容器初始化 (Container Initialization)              │
│ SpringApplication.run() → ApplicationContext 创建            │
│ → 加载 application.properties 配置                           │
├─────────────────────────────────────────────────────────────────┤
│ 阶段 2: 环境准备 (Environment Preparation)                   │
│ → NacosEnvironment 初始化                                   │
│ → ${nacos.home} 目录创建 (/data/nacos/)                    │
│ → Derby 嵌入式数据库初始化（如使用内置存储）               │
├─────────────────────────────────────────────────────────────────┤
│ 阶段 3: 集群成员加载 (Cluster Member Loading)               │
│ → ServerMemberManager.init()                                │
│ → cluster.conf 集群节点列表加载                             │
│ → ServerListManager 服务端地址列表初始化                     │
├─────────────────────────────────────────────────────────────────┤
│ 阶段 4: 一致性协议启动 (Consistency Protocol Init)          │
│ → RaftCore.init() — JRaft Leader 选举 + 日志回放          │
│ → DistroProtocol.init() — 一致性哈希环构建                  │
│ → DelegateConsistencyServiceImpl — AP/CP 路由就绪             │
├─────────────────────────────────────────────────────────────────┤
│ 阶段 5: 通信层启动 (Communication Layer Start)               │
│ → GrpcSdkServer.start() — gRPC SDK 服务端口绑定            │
│ → GrpcClusterServer.start() — gRPC 集群同步端口绑定        │
│ → ConnectionManager.init() — 连接管理器初始化                │
├─────────────────────────────────────────────────────────────────┤
│ 阶段 6: 业务模块加载 (Business Module Loading)               │
│ → NamingModule 注册服务注册 API                             │
│ → ConfigModule 注册配置管理 API                             │
│ → AuthModule 注册认证 API                                   │
├─────────────────────────────────────────────────────────────────┤
│ 阶段 7: HTTP 服务暴露 (HTTP Service Exposure)               │
│ → EmbeddedTomcat 绑定 server.port (默认 8848)               │
│ → Servlet 注册：InstanceController / ConfigController         │
│ → ApplicationReadyEvent 发布 → 服务就绪                       │
└─────────────────────────────────────────────────────────────────┘
```

**【源码走读】**

Nacos 2.5.3 启动 7 阶段的详细流程如下：

**阶段 1：容器初始化**

`Nacos.main()` 调用 `SpringApplication.run(Nacos.class, args)` 触发 Spring Boot 启动流程。Spring 容器创建 `AnnotationConfigApplicationContext`，加载 `application.properties` 配置文件（默认位置：`${nacos.home}/conf/application.properties`）。此阶段完成后，Spring 容器已就绪，但 Nacos 各模块尚未初始化。

**阶段 2：环境准备**

Spring 容器发布 `ApplicationEnvironmentPreparedEvent` 事件。Nacos 监听此事件，初始化 `NacosEnvironment`：
- 创建 `${nacos.home}` 目录（默认 `~/nacos/`）
- 如果使用嵌入式存储（Derby），初始化 Derby 数据库文件（`${nacos.home}/data/derby/`）
- 加载 `cluster.conf` 集群配置文件

此阶段的关键设计：Derby 嵌入式数据库在此阶段初始化，因为后续一致性协议启动需要访问存储层（JRaft 日志存储）。

**阶段 3：集群成员加载**

`ServerMemberManager.init()`（core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java）：

```java
// ServerMemberManager.init()（core/.../ServerMemberManager.java）
public void init() {
    // 1. 读取 cluster.conf 集群成员列表
    serverListManager.init();
    // 2. 如果是集群模式，启动节点发现任务
    if (!serverListManager.isSingleMode()) {
        initSelfClusterServer();       // 初始化本节点信息
        initMemberChangeTask();         // 启动成员变更监听任务
    }
}
```

加载集群成员信息：：
- 读取 `cluster.conf` 文件（格式：`ip:port1,ip:port2,...`）
- 初始化 `ServerListManager`（core/src/main/java/com/alibaba/nacos/core/cluster/ServerListManager.java）维护集群服务端地址列表
- 如果是单机模式（`cluster.conf` 为空或只有本机），跳过集群节点发现

**阶段 4：一致性协议启动**

这是最关键且最复杂的启动阶段。根据配置的模式（AP/CP/混合），启动对应的一致性协议：

- **JRaft（CP 模式）**：`RaftCore.init()` 启动 JRaft 状态机：(1) 从本地日志文件回放未提交的日志条目；(2) 发起 Leader 选举（如果当前集群无 Leader）；(3) 启动 JRaft RPC 服务绑定端口（默认 `server.port + 2000`）
- **Distro（AP 模式）**：`DistroProtocol.init()` 构建一致性哈希环：读取集群成员列表 → 为每个成员分配虚拟节点（默认 20 个虚拟节点/物理节点）→ 构建 TreeMap 哈希环。每个节点只负责其哈希环区段内的数据同步
- `DelegateConsistencyServiceImpl` 根据配置初始化 AP/CP 路由表

**阶段 5：通信层启动**

一致性协议就绪后，启动 gRPC 通信层：

- `GrpcSdkServer.start()`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java:89-120）：绑定 gRPC SDK 服务端口（默认 `server.port + 1000`），注册 `BiRequestStream` 请求处理器
- `GrpcClusterServer.start()`：绑定 gRPC 集群同步端口（默认 `server.port + 2000`），用于集群节点间的数据同步（JRaft 日志复制、Distro 数据同步）
- `ConnectionManager.init()`（core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java）：初始化连接管理器，启动心跳超时检测定时任务

**阶段 6：业务模块加载**

通信层就绪后，加载业务模块：

- **Naming 模块**：注册 `InstanceController` REST API 路由（`/nacos/v1/ns/instance`）、启动 `PushService` 推送服务、启动 `ClientBeatCheckTask` 心跳超时检测定时任务
- **Config 模块**：注册 `ConfigController` REST API 路由（`/nacos/v1/cs/configs`）、启动 `LongPollingService` 长轮询引擎、启动 `AsyncNotifyService` 集群间异步通知
- **Auth 模块**：注册认证过滤器链、初始化 JWT Token 管理器

**阶段 7：HTTP 服务暴露**

所有业务模块加载完成后，Spring Boot 内嵌的 Tomcat 绑定 `server.port`（默认 8848），暴露 HTTP REST API。Spring 容器发布 `ApplicationReadyEvent` 事件，标志着 Nacos 服务完全就绪。此时：
- gRPC SDK 端口已监听（默认 9848）
- gRPC 集群端口已监听（默认 9849）
- HTTP 端口已监听（默认 8848）
- 控制台 UI 可通过 `http://host:8848/nacos/` 访问

**【设计模式分析】**

1. **生命周期模式（Lifecycle Pattern）**：7 个启动阶段每个都有明确的初始化方法和就绪状态。各模块通过实现 Spring 的 `ApplicationListener<ApplicationReadyEvent>` 接口，在容器就绪后执行各自的初始化逻辑。这种设计将启动流程的复杂性分散到各模块自身，而非集中在 `main()` 方法中硬编码。

2. **观察者模式（Observer Pattern）**：各模块通过监听 `ApplicationReadyEvent` 事件触发初始化，而非直接调用彼此的初始化方法。这种设计使得模块间完全解耦——Config 模块不需要知道 Naming 模块的初始化时机，只需监听同一个事件即可。

3. **门面模式（Facade Pattern）**：`DelegateConsistencyServiceImpl` 作为一致性协议的门面，封装了 AP（Distro）和 CP（JRaft）两种协议的选择逻辑。启动阶段只需初始化 `DelegateConsistencyServiceImpl`，其内部根据配置决定实际初始化 Distro 还是 JRaft（或两者）。

**【小结】**

Nacos 2.5.3 的 7 阶段启动流程通过严格的阶段顺序依赖和 Spring 事件驱动机制，实现了启动过程的可观测性和失败隔离性。每个阶段独立初始化，前序阶段失败不会导致后续阶段执行，便于故障定位和排除。

## 1.8 gRPC 双通道架构：GrpcSdkServer vs GrpcClusterServer

**【设计背景】**

Nacos 2.x 的核心通信基于 gRPC，采用双通道架构：`GrpcSdkServer`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java:1-133）面向 SDK 客户端的 gRPC Bi-directional Stream 服务，处理客户端服务发现和配置订阅请求；`GrpcClusterServer`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcClusterServer.java）面向集群节点间的 gRPC 同步通道，处理集群间数据同步（JRaft 日志复制、Distro 数据同步）。两个服务器的端口独立绑定（默认 +1000 和 +2000），实现 SDK 流量和集群同步流量的物理隔离。

**【核心类关系图】**

```
/* 图 1-8：Nacos 2.5.3 gRPC 双通道架构 */
┌────────────────────────────────────────────────────────────┐
│                    Nacos Server                              │
│                                                            │
│  ┌──────────────────────┐  ┌──────────────────────┐        │
│  │   GrpcSdkServer     │  │ GrpcClusterServer    │        │
│  │   port: 8848+1000   │  │ port: 8848+2000     │        │
│  │   = 9848            │  │ = 10848              │        │
│  │                      │  │                      │        │
│  │  ┌────────────────┐  │  │  ┌────────────────┐  │        │
│  │  │ BiRequestStream │  │  │ │ ClusterSync    │  │        │
│  │  │ Acceptor       │  │  │ │ RequestHandler  │  │        │
│  │  └───────┬────────┘  │  │ └───────┬────────┘  │        │
│  │          │           │  │          │           │        │
│  │  ┌───────▼────────┐  │  │  ┌───────▼────────┐  │        │
│  │  │ GrpcConnection  │  │  │  │ GrpcConnection  │  │        │
│  │  │ (per client)   │  │  │  │ (per node)     │  │        │
│  │  └────────────────┘  │  │  └────────────────┘  │        │
│  └──────────────────────┘  └──────────────────────┘        │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐    │
│  │              BaseGrpcServer                          │    │
│  │  · start() — 绑定端口 + 拦截器注册               │    │
│  │  · addInterceptor() — 参数校验/认证过滤            │    │
│  │  · handleRequest() — 请求分发到 RequestHandler    │    │
│  └──────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────┘
         │                              │
         │ (Bi-di Stream)               │ (Cluster Sync)
         ▼                              ▼
   ┌──────────┐                ┌──────────┐
   │  Client  │                │  Node B  │
   │  (SDK)   │                │          │
   └──────────┘                └──────────┘
```

**【源码走读】**

**一、GrpcSdkServer：面向 SDK 客户端**

`GrpcSdkServer.start()`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java:89-120）绑定 gRPC SDK 服务端口（默认 `server.port + 1000 = 9848`）。核心机制：

1. **Bi-directional Stream 请求接入**：`GrpcBiStreamRequestAcceptor`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcBiStreamRequestAcceptor.java）接收客户端 Bi-directional Stream 连接。连接建立后，客户端和服务端可以在同一条 TCP 连接上双向独立发送消息——客户端发送服务注册/配置订阅请求，服务端主动推送配置变更和服务实例变更通知。

2. **拦截器链**：`BaseGrpcServer.addInterceptor()` 注册 gRPC 拦截器链：`GrpcConnectionInterceptor`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcConnectionInterceptor.java）处理连接建立/断开事件、`RemoteParamCheckFilter`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/RemoteParamCheckFilter.java）进行请求参数校验。

3. **请求分发**：`BaseGrpcServer.handleRequest()` 根据请求类型将请求分发到对应的 `RequestHandler`。例如，`InstanceRequestHandler` 处理服务注册/发现请求，`ConfigChangeRequestHandler` 处理配置变更推送。

**二、GrpcClusterServer：面向集群节点间同步**

`GrpcClusterServer` 绑定 gRPC 集群同步端口（默认 `server.port + 2000 = 10848`）。核心机制：

1. **JRaft 日志复制**：`JRaftServer` 使用 gRPC 通道在集群节点间复制 Raft 日志条目。Leader 通过 gRPC 向所有 Follower 并行发送 `AppendEntriesRequest`，Follower 验证任期和日志一致性后确认。
2. **Distro 数据同步**：Distro 协议通过 gRPC 通道在集群节点间同步服务实例数据。每个节点向哈希环上的目标节点发送增量数据变更（`DistroDataVerifyTask` 周期性验证数据一致性）。
3. **节点健康探测**：集群节点间通过 gRPC 心跳机制探测彼此的健康状态。

**三、双通道隔离设计**

SDK 通道（端口 9848）和集群通道（端口 10848）物理端口隔离，核心优势：
- **流量隔离**：SDK 客户端请求流量不会影响集群同步流量，避免高并发客户端请求阻塞集群数据同步
- **安全隔离**：集群通道可配置 TLS 双向认证，而 SDK 通道可配置较宽松的认证策略
- **独立扩缩容**：可通过独立防火墙规则控制两个端口的访问权限

**【设计模式分析】**

1. **模板方法模式（Template Method Pattern）**：`BaseGrpcServer`（core/src/main/java/com/alibaba/nacos/core/remote/grpc/BaseGrpcServer.java）定义了 gRPC 服务端的通用启动流程：端口绑定 → 拦截器注册 → Service 注册 → 启动。`GrpcSdkServer` 和 `GrpcClusterServer` 作为子类实现各自特定的 Service 绑定逻辑（SDK 服务 vs 集群同步服务）。

2. **责任链模式（Chain of Responsibility Pattern）**：gRPC 拦截器链（`GrpcConnectionInterceptor` → `RemoteParamCheckFilter` → `AuthFilter`）依次处理每个 gRPC 请求。每个拦截器可以决定是否将请求传递给链中的下一个拦截器。

3. **策略模式（Strategy Pattern）**：`GrpcSdkServer` 和 `GrpcClusterServer` 使用不同的 `RequestHandler` 策略处理不同类型的请求。`GrpcSdkServer` 使用 `BiRequestStreamRequestHandler`（Bi-di Stream 请求处理策略），`GrpcClusterServer` 使用 `ClusterSyncRequestHandler`（集群同步请求处理策略）。

**【小结】**

Nacos 2.5.3 的 gRPC 双通道架构通过 `GrpcSdkServer`（SDK 通道）和 `GrpcClusterServer`（集群通道）的物理端口隔离，实现了 SDK 客户端流量和集群同步流量的安全隔离和独立扩展。

## 1.9 ConnectionManager 连接生命周期管理

**【设计背景】**

Nacos 2.x 引入 `ConnectionManager`（core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java:1-356）管理所有客户端 gRPC 长连接的生命周期，替代 Nacos 1.x 的无状态 HTTP 连接模型。每个客户端 gRPC 连接由 `Connection` 对象封装，包含唯一 `connectionId`、`ClientAbilities`（客户端能力标识）、连接建立时间、最后活跃时间等元数据。`ConnectionManager` 的核心职责包括：连接注册（register）、连接注销（unregister）、心跳检测（refresh/beat）、能力协商（ClientAbilities 交换）。

**【核心类关系图】**

```
/* 图 1-9：ConnectionManager 连接生命周期管理 */
┌────────────────────────────────────────────────────────────┐
│                  ConnectionManager                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ connections: Map<String, Connection>                  │  │
│  │  · connectionId → Connection                        │  │
│  │  · 每个客户端一条 gRPC 长连接                      │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
│  生命周期管理:                                              │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌────────┐  │
│  │ REGISTER │──▶│ ACTIVE  │──▶│ TIMEOUT │──▶│ UNREG  │  │
│  │ 注册连接 │   │ 活跃心跳 │   │ 心跳超时 │   │ 注销连接 │  │
│  └─────────┘   └─────────┘   └─────────┘   └────────┘  │
│       │             │              │              │          │
│       │             │              │              │          │
│  ┌────▼────┐ ┌────▼────┐ ┌────▼────┐ ┌────▼──────┐ │
│  │clientId │ │refresh()│ │eject()  │ │unregister()│ │
│  │abilities│ │beat()   │ │超时剔除  │ │释放资源    │ │
│  └─────────┘ └─────────┘ └─────────┘ └───────────┘ │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  RuntimeConnectionEjector                           │  │
│  │  · 定时扫描超时连接（默认20秒无心跳 → eject）    │  │
│  │  · 剔除过期连接 → ConnectionEventListener 通知     │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

**【源码走读】**

`ConnectionManager` 管理连接生命周期的四个核心阶段：

**一、连接注册（register）**

当客户端通过 `GrpcBiStreamRequestAcceptor` 建立 gRPC Bi-directional Stream 连接时，`ConnectionManager.register()`（core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java）：

```java
// ConnectionManager.register()（core/.../ConnectionManager.java）
public synchronized Connection register(String connectionId, Connection connection) {
    // 1. 检查连接是否已注册
    if (connections.containsKey(connectionId)) {
        return connections.get(connectionId);
    }
    // 2. 存入 connections Map，发布 ConnectionRegisteredEvent
    connections.put(connectionId, connection);
    connection.setLastRefreshTime(System.currentTimeMillis());
    NotifyCenter.publishEvent(new ConnectionRegisteredEvent(connection));
    return connection;
}
```

调用流程：
1. 生成唯一 `connectionId`（格式：`clientIp:port-uuid`）
2. 创建 `GrpcConnection` 对象（core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcConnection.java），封装 gRPC Stream 引用、客户端能力信息
3. 将 `Connection` 存入 `connections` Map（`connectionId → Connection`）
4. 发布 `ConnectionRegisteredEvent` 事件，通知已注册的 `ConnectionEventListener`

**二、心跳检测（refresh/beat）**

客户端定期发送心跳请求（默认每 5 秒一次）到服务端。`ConnectionManager.refresh(connectionId)` 方法更新连接的最后活跃时间戳。`ConnectionManager` 内部维护 `RuntimeConnectionEjector` 定时任务（默认每 3 秒执行一次），扫描 `connections` 中所有连接，将超过 20 秒未刷新心跳的连接标记为超时。

**三、超时剔除（eject）**

`RuntimeConnectionEjector` 检测到超时连接后，调用 `ConnectionManager.eject(connectionId)`：
1. 从 `connections` Map 中移除超时连接
2. 关闭底层 gRPC Stream（释放网络资源）
3. 发布 `ConnectionDisconnectEvent` 事件，通知 `PushService` 从推送目标列表中移除该客户端

**四、连接注销（unregister）**

客户端主动断开连接或服务端主动关闭连接时，`ConnectionManager.unregister(connectionId)` 执行与 eject 类似的清理流程。

**五、能力协商（ClientAbilities 交换）**

在连接注册阶段，客户端上报 `ClientAbilities`（core/src/main/java/com/alibaba/nacos/core/remote/ClientAbilities.java）：
- **是否支持增量推送**：`supportIncrementalPush` —— 决定 `PushService` 使用全量推送还是增量推送
- **是否支持 gRPC 健康检查**：`supportGrpcHealthCheck` —— 决定使用 TCP 还是 gRPC 健康检查
- **是否支持 TLS**：`supportTls` —— 决定是否启用连接加密

服务端根据 `ClientAbilities` 动态调整与该客户端的通信策略——旧版本客户端（不支持增量推送）仍使用全量推送模式，保证向后兼容性。

**【设计模式分析】**

1. **观察者模式（Observer Pattern）**：`ConnectionManager` 维护了多个 `ConnectionEventListener` 的注册表。当连接状态变化（注册/注销/超时）时，所有已注册的监听器都会收到通知。`PushService` 通过监听 `ConnectionDisconnectEvent` 事件来自动从推送目标列表中移除已断开的客户端。

2. **策略模式（Strategy Pattern）**：`ConnectionManager` 根据 `ClientAbilities` 选择不同的健康检查策略（TCP vs gRPC）和推送策略（全量 vs 增量）。这种设计使得同一集群可以同时服务不同版本的客户端，实现平滑升级。

3. **对象池模式（Object Pool Pattern）**：`connections` Map 作为连接对象池，复用已建立的 gRPC 连接（通过 `connectionId` 索引）。当客户端暂时断开后重连时，如果旧连接尚未被 `RuntimeConnectionEjector` 剔除，可以直接复用——减少了 gRPC 握手开销。

**【小结】**

`ConnectionManager` 通过注册、心跳检测、超时剔除和连接注销四个阶段的精细管理，实现了 Nacos 2.x 的长连接生命周期管理。`ClientAbilities` 能力协商机制使得同一集群可以同时服务不同版本的客户端，保证了向后兼容性。

## 1.10 数据模型设计：Namespace→Group→Service→Cluster→Instance

**【设计背景】**

Nacos 2.5.3 的数据模型采用五层级树状结构：Namespace（命名空间（Namespace））→ Group（分组）→ Service（服务）→ Cluster（集群）→ Instance（实例）。这五层级模型实现了从粗粒度到细粒度的逐级隔离和分类管理：Namespace 实现租户/环境隔离，Group 实现服务/配置的分类管理，Service 是注册中心的逻辑服务单元，Cluster 实现物理部署分组（如多数据中心），Instance 是最小粒度单元（IP:Port 端点）。这种层级化设计使得 Nacos 可以灵活适应从单机房单环境到多数据中心多租户的复杂部署场景。

**【核心类关系图】**

```
/* 图 1-10：Nacos 2.5.3 五层级数据模型 */
┌────────────────────────────────────────────────────────────┐
│                  Namespace (命名空间)                       │
│  实现租户/环境隔离（如：dev / test / prod）               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Group (分组)                                      │  │
│  │ 服务/配置的逻辑分类（如：ORDER / PAYMENT）         │  │
│  │ ┌────────────────────────────────────────────────┐  │  │
│  │ │ Service (服务)                               │  │  │
│  │ │ 注册中心的逻辑服务单元                        │  │  │
│  │ │ · serviceName: "order-service"               │  │  │
│  │ │ · protectThreshold: 0.8 (保护阈值)           │  │  │
│  │ │ ┌──────────────────────────────────────────┐  │  │  │
│  │ │ │ Cluster (集群)                         │  │  │  │
│  │ │ │ 物理部署分组（如：BJ / SH / GZ）      │  │  │  │
│  │ │ │ · clusterName: "DEFAULT"               │  │  │  │
│  │ │ │ · healthChecker: TCP/HTTP/MySQL        │  │  │  │
│  │ │ │ ┌────────────────────────────────────┐  │  │  │  │
│  │ │ │ │ Instance (实例)                   │  │  │  │  │
│  │ │ │ │ 网络端点 (IP:Port)               │  │  │  │  │
│  │ │ │ │ · ip: "192.168.1.100"          │  │  │  │  │
│  │ │ │ │ · port: 8080                    │  │  │  │  │
│  │ │ │ │ · ephemeral: true → AP(Distro)  │  │  │  │  │
│  │ │ │ │ · weight: 1.0                   │  │  │  │  │
│  │ │ │ │ · healthy: true                 │  │  │  │  │
│  │ │ │ └────────────────────────────────────┘  │  │  │  │
│  │ │ └──────────────────────────────────────────┘  │  │  │
│  │ └────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

**【源码走读】**

**一、Namespace（命名空间）**

`Namespace`（api/src/main/java/com/alibaba/nacos/api/common/Namespace.java）是 Nacos 的顶层隔离单元。每个 Namespace 有唯一 `namespaceId`（如 `dev`、`test`、`prod`），用于实现多租户/多环境隔离。Nacos 2.5.3 新增了 `TenantInfo`（api/src/main/java/com/alibaba/nacos/api/common/TenantInfo.java）模型，扩展了 Namespace 的元数据能力（如关联用户、描述等）。默认公共 Namespace 的 `namespaceId` 为空字符串 `""`——未指定 Namespace 的服务/配置均属于公共 Namespace。

**二、Group（分组）**

Group 是 Namespace 下的二级分类单元。在 Naming（注册中心）中，Group 用于对 Service 进行分类；在 Config（配置中心）中，Group 用于对配置进行分类。默认 Group 为 `DEFAULT_GROUP`。

**三、Service（服务）**

`Service`（naming/src/main/java/com/alibaba/nacos/naming/core/v2/pojo/Service.java）是注册中心的核心逻辑单元。核心字段：
- `namespaceId` + `groupName` + `serviceName`：唯一标识一个 Service
- `protectThreshold`：保护阈值（0-1），当健康实例比例低于此阈值时开启防雪崩保护（返回全部实例而非仅健康实例）
- `clusters`：`Map<String, Cluster>`，该 Service 下的 Cluster Map，key 为 `clusterName`

`ServiceManager`（naming/src/main/java/com/alibaba/nacos/naming/core/v2/ServiceManager.java:45-52）维护全局 `serviceMap`（`Map<String, Service>`），`getOrCreateService(namespaceId, groupName, serviceName)` 惰性创建 Service。

**四、Cluster（集群）**

`Cluster`（naming/src/main/java/com/alibaba/nacos/naming/core/v2/pojo/Cluster.java）是 Service 下的物理部署分组（如多数据中心）。核心设计：
- `ephemeralInstances`：临时实例 Map（`Map<String, Instance>`），ephemeral=true，由客户端心跳维护
- `persistentInstances`：持久化实例 Map（`Map<String, Instance>`），ephemeral=false，需手动注销
- `healthChecker`：健康检查器（TCP/HTTP/MySQL/NONE）

**五、Instance（实例）**

`Instance`（naming/src/main/java/com/alibaba/nacos/naming/core/v2/pojo/Instance.java）是 Nacos 的最小粒度单元，代表一个网络端点。核心字段：
- `instanceId`：实例唯一标识（格式：`ip#port#clusterName#serviceName`）
- `ip` + `port`：网络端点
- `ephemeral`（boolean）：临时实例标识——`true` → AP（Distro 协议去中心化同步），`false` → CP（JRaft 协议 Raft 日志复制）
- `weight`（double）：实例权重，用于客户端负载均衡（值越大分配流量越多）
- `healthy`（boolean）：健康状态，由健康检查定时任务周期性更新

**【设计模式分析】**

1. **组合模式（Composite Pattern）**：五层级数据模型（Namespace→Group→Service→Cluster→Instance）本质上是组合模式的树状结构——Namespace 包含多个 Group，Group 包含多个 Service，Service 包含多个 Cluster，Cluster 包含多个 Instance。每层可以独立管理其下层元素的生命周期。

2. **策略模式（Strategy Pattern）**：`ephemeral` 字段决定了 Instance 的一致性协议策略：`true` → `EphemeralConsistencyService`（Distro API），`false` → `RaftConsistencyServiceImpl`（CP JRaft）。`DelegateConsistencyServiceImpl`（naming/src/main/java/com/alibaba/nacos/naming/consistency/DelegateConsistencyServiceImpl.java:67-78）根据此字段动态选择一致性协议。

3. **不可变对象模式（Immutable Object Pattern）**：`Instance` 的 `instanceId` 由不可变的 `ip#port#clusterName#serviceName` 构成，确保了 Instance 全局唯一且不可变——一旦注册，`instanceId` 不再变化（除非注销后重新注册），避免了实例标识冲突。

**【小结】**

Nacos 2.5.3 的五层级数据模型通过 Namespace（租户隔离）→ Group（逻辑分类）→ Service（服务单元）→ Cluster（物理分组）→ Instance（网络端点）的逐级细化，实现了从粗粒度到细粒度的隔离和分类管理，适应从单机房到多数据中心多租户的复杂部署场景。

## 1.11 Instance 模型详解：ephemeral 字段决定 AP/CP 路由

**【设计背景】**

`Instance` 模型（naming/src/main/java/com/alibaba/nacos/naming/core/v2/pojo/Instance.java）中 `ephemeral` 布尔字段是 Nacos 一致性协议路由的核心决策点：`true` 标记临时实例，走 AP 模式（Distro 协议去中心化最终一致性）；`false` 标记持久化实例，走 CP 模式（JRaft 协议强一致性）。这种按实例粒度选择一致性协议的设计，使同一个 Service 内可以混合临时实例和持久化实例——临时实例自动剔除，持久化实例需手动注销。

**【核心类关系图】**

```
/* 图 1-11：Instance 模型 ephemeral AP/CP 路由决策 */
                    ┌──────────────────────┐
                    │   Instance (Instance)   │
                    │ · instanceId          │
                    │ · ip:port             │
                    │ · ephemeral: boolean   │────────┐
                    │ · weight: double      │        │
                    │ · healthy: boolean    │        │
                    │ · metadata: Map       │        │
                    └──────────────────────┘        │
                                                  │
                    ┌─────────────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │ ephemeral == true ?    │
        └───────────────────────┘
            │               │
         YES│               │NO
            ▼               ▼
   ┌────────────────┐ ┌────────────────┐
   │ AP (Distro)    │ │ CP (JRaft)     │
   │ · 去中心化同步  │ │ · Leader 选举    │
   │ · 一致性哈希    │ │ · 日志复制      │
   │ · 最终一致性    │ │ · 强一致性      │
   │ · 自动健康检查  │ │ · 手动注销      │
   │ · 心跳超时剔除  │ │ · 持久化存储    │
   └────────────────┘ └────────────────┘
            │               │
            ▼               ▼
   ┌────────────────────────────────────┐
   │  DelegateConsistencyServiceImpl    │
   │  .put(key, instances)            │
   │  → 根据 ephemeral 路由到:         │
   │    EphemeralConsistencyService    │
   │    或 RaftConsistencyServiceImpl  │
   └────────────────────────────────────┘
```

**【源码走读】**

**一、ephemeral 字段的定义与语义**

`Instance.ephemeral`（boolean）在 `Instance` 模型中的定义：`true` 表示临时实例——由客户端心跳维护，服务端定时检测心跳超时自动剔除；`false` 表示持久化实例——客户端断开后不会自动剔除，需手动调用 `deregisterInstance()` API 注销。

默认值：`ephemeral = true`（大多数微服务场景使用临时实例）。

**二、AP/CP 路由决策**

`DelegateConsistencyServiceImpl`（naming/src/main/java/com/alibaba/nacos/naming/consistency/DelegateConsistencyServiceImpl.java:67-78）的 `put(key, instances)` 方法根据 `instances[0].isEphemeral()` 路由：

```java
if (instances[0].isEphemeral()) {
    ephemeralConsistencyService.put(key, instances);  // AP → Distro
// DelegateConsistencyServiceImpl.put()（naming/.../DelegateConsistencyServiceImpl.java:67-78）
} else {
    raftConsistencyService.put(key, instances);  // CP → JRaft
}
```

**三、AP 模式（ephemeral=true）：Distro 去中心化同步**

临时实例使用 Distro 协议（naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/EphemeralConsistencyService.java）：
1. 一致性哈希算法将服务数据按哈希环分发到不同节点，每个节点只负责其哈希区段内的数据权威写入
2. 数据变更后，负责节点通过 gRPC 向哈希环上的目标节点发送增量数据同步
3. 客户端心跳超时（默认 15 秒未刷新），`ClientBeatCheckTask`（naming/src/main/java/com/alibaba/nacos/naming/healthcheck/ClientBeatCheckTask.java）自动剔除过期临时实例

**四、CP 模式（ephemeral=false）：JRaft 强一致性**

持久化实例使用 JRaft 协议（naming/src/main/java/com/alibaba/nacos/naming/consistency/persistent/RaftConsistencyServiceImpl.java）：
1. Proposal 提交到 JRaft Leader，Leader 将日志条目复制到所有 Follower
2. Leader 在收到多数 Follower 确认后，将日志条目应用到状态机（持久化到 MySQL/Derby）
3. 持久化实例不会自动剔除——客户端断开后，实例数据仍持久化存储，直到手动注销

**五、混合部署场景**

同一个 Service 可以同时包含临时实例（如 Kubernetes Pod 自动扩缩容）和持久化实例（如物理机部署的关键服务）。`ServiceManager` 内部维护两个 Map：
- `Cluster.ephemeralInstances`：存储 `ephemeral=true` 的临时实例
- `Cluster.persistentInstances`：存储 `ephemeral=false` 的持久化实例

两个 Map 独立管理，互不影响。

**【设计模式分析】**

1. **策略模式（Strategy Pattern）**：`DelegateConsistencyServiceImpl` 作为上下文，`EphemeralConsistencyService`（Distro 策略）和 `RaftConsistencyServiceImpl`（JRaft 策略）是两种具体的一致性协议策略。`put(key, instances)` 方法根据 `ephemeral` 字段动态选择策略——这是策略模式的典型应用。

2. **状态模式（State Pattern）**：`ephemeral` 字段本质上是 Instance 的状态标识——`true`（临时状态）触发自动心跳检测和超时剔除行为；`false`（持久化状态）触发持久化存储和手动注销行为。同一个 Instance 在生命周期中可能切换状态（如从临时实例转持久化实例）。

**【小结】**

`ephemeral` 字段是 Nacos 一致性协议设计的核心：`true`→AP（Distro 去中心化最终一致性 + 自动健康检查），`false`→CP（JRaft 强一致性 + 持久化存储）。这种按实例粒度选择一致性协议的设计，使同一个 Service 内的实例可以灵活适应不同部署场景（临时容器 vs 持久物理机）。

## 1.12 配置数据模型：ConfigInfo → HisConfigInfo

**【设计背景】**

Nacos Config 模块的配置数据模型以 `ConfigInfo`（config/src/main/java/com/alibaba/nacos/config/server/model/ConfigInfo.java）为核心，记录配置的最新版本信息；以 `HisConfigInfo`（config/src/main/java/com/alibaba/nacos/config/server/model/HisConfigInfo.java）为历史版本链，记录每次配置变更的历史版本。这种「当前版本 + 历史版本链」的双模型设计使得 Nacos 支持配置变更历史追溯和一键回滚能力。Config 模块在 2.5.3 中新增了 `persistence/` 独立持久化模块，`ConfigInfo` 的持久化从分散在 Config 模块中统一迁移到 `persistence/` 模块。

**【核心类关系图】**

```
/* 图 1-12：ConfigInfo → HisConfigInfo 配置数据模型 */
┌────────────────────────────────────────────────────────────┐
│                    ConfigInfo (当前版本)                       │
│  · id: long (自增主键)                                  │
│  · dataId: String (配置唯一标识)                         │
│  · groupId: String (分组)                                │
│  · tenantId: String (命名空间)                           │
│  · content: String (配置内容)                            │
│  · md5: String (内容 MD5 校验值)                         │
│  · type: String (配置类型: json/xml/text/...)             │
│  · encryptedDataKey: String (加密数据密钥)                │
│  └──────────────────────────────────────────────────────┘    │
│                          │                                  │
│                          │ 每次发布/更新自动创建历史版本      │
│                          ▼                                  │
│  ┌──────────────────────────────────────────────────────┐    │
│  │                HisConfigInfo (历史版本)                 │
│  │  · nid: long (关联 ConfigInfo.id)                    │
│  │  · content: String (历史版本配置内容)                 │
│  │  · md5: String (历史版本 MD5)                        │
│  │  · gmtCreate: timestamp (创建时间)                    │
│  │  · srcUser: String (变更用户)                         │
│  │  · opType: String (操作类型: INSERT/UPDATE/DELETE)   │
│  └──────────────────────────────────────────────────────┘    │
│                                                            │
│  历史版本保留策略: 默认保留最近 30 天                      │
│  回滚操作: ConfigController.rollback() → 恢复历史版本    │
└────────────────────────────────────────────────────────────┘
```

**【源码走读】**

**一、ConfigInfo：当前版本配置**

`ConfigInfo` 记录配置的最新版本信息：
- `dataId + groupId + tenantId`：三元组唯一确定一个配置项
- `content`：配置内容（最长不超过 4MB）
- `md5`：配置内容的 MD5 校验值——用于客户端判断配置是否发生变更
- `encryptedDataKey`：加密数据密钥（如果配置启用加密插件）
- `type`：配置类型（json/xml/text/properties/yaml/html）

`ConfigInfo` 通过 `ConfigCacheService`（config/src/main/java/com/alibaba/nacos/config/server/service/ConfigCacheService.java:636）维护本地缓存——服务端周期性（默认每 5 秒）从 MySQL/Derby 加载最新配置到内存缓存（`ConcurrentHashMap`），减少数据库查询压力。

**二、HisConfigInfo：历史版本链**

每次配置发布或更新时，系统自动创建 `HisConfigInfo` 记录：
- `nid`：关联的 `ConfigInfo.id`（外键关系）
- `content`：历史版本配置内容（原封不动保存）
- `srcUser`：变更来源用户（用于审计追溯）
- `opType`：操作类型——`INSERT`（首次发布）、`UPDATE`（更新配置）、`DELETE`（删除配置）

历史版本保留策略：默认保留最近 30 天的历史版本。超过 30 天的历史版本由 `HistoryConfigCleaner` 定时任务定期清理（默认每天凌晨 3 点执行）。回滚操作：`ConfigController.rollback()`（config/src/main/java/com/alibaba/nacos/config/server/controller/ConfigController.java）根据 `nid` 查询历史版本内容，调用 `publishConfig()` 将历史版本内容重新发布——实现一键回滚。

**三、持久化迁移：persistence/ 模块（2.5.3 新增）**

在 Nacos 2.5.3 中，`ConfigInfo` 和 `HisConfigInfo` 的持久化从原来分散在 Config 模块中统一迁移到 `persistence/` 独立模块：
- `ExternalDataSourceServiceImpl`（persistence/src/main/java/com/alibaba/nacos/persistence/datasource/ExternalDataSourceServiceImpl.java）：提供 MySQL 持久化实现——`ConfigInfo` 对应 `config_info` 表，`HisConfigInfo` 对应 `his_config_info` 表
- `EmbeddedStoragePersistServiceImpl`：提供 Derby 嵌入式持久化实现——使用 Apache Derby 嵌入式数据库（单机模式）

**四、配置加密**

敏感配置（如数据库密码）可通过 SPI 加密插件加密存储：
- `ConfigEncryptionPluginService`（plugin/config/src/main/java/com/alibaba/nacos/plugin/config/ConfigEncryptionPluginService.java）：加密插件 SPI 接口
- 加密后 `content` 字段存储密文，`encryptedDataKey` 字段存储加密数据密钥
- 客户端拉取配置时，SDK 自动解密——对客户端透明

**【设计模式分析】**

1. **快照模式（Snapshot Pattern）**：`ConfigInfo` 相当于当前版本的"快照"，`HisConfigInfo` 记录每次变更的增量历史。这种「快照 + 增量历史」的设计，使得配置回溯只需找到历史快照即可恢复，无需从头重建。

2. **策略模式（Strategy Pattern）**：`DataSourceService` 接口定义了数据访问策略，`ExternalDataSourceServiceImpl`（MySQL 策略）和 `EmbeddedStoragePersistServiceImpl`（Derby 策略）分别实现。`DynamicDataSource` 根据 `StorageConfiguration` 条件注解在运行时选择具体策略——这是策略模式的实践。

3. **命令模式（Command Pattern）**：每次配置发布/更新操作对应一个 `ConfigChangeEvent` 命令，`NotifyCenter.publishEvent()` 发布命令，`AsyncNotifyService` 异步执行命令（向集群其他节点发送 HTTP 通知）。这种设计使得配置发布操作和通知逻辑完全解耦。

**【小结】**

Nacos 2.5.3 的配置数据模型通过「ConfigInfo（当前版本）+ HisConfigInfo（历史版本链）」双模型设计，实现了配置变更历史追溯和一键回滚能力。2.5.3 将持久化迁移到独立的 `persistence/` 模块，进一步强化了存储层的模块化和可替换性。

## 1.13 NotifyCenter 事件驱动架构：EventPublisher + Subscriber 注册机制

**【设计背景】**

Nacos 2.5.3 内部模块间通信的核心机制是 `NotifyCenter`（common/src/main/java/com/alibaba/nacos/common/notify/NotifyCenter.java）事件驱动架构。各模块通过 `NotifyCenter` 发布事件（Event）和订阅事件（Subscriber），实现模块间的完全解耦通信。例如：Config 模块发布 `ConfigDataChangeEvent` 事件，Naming 模块中的 `AsyncNotifyService` 订阅此事件并异步通知集群其他节点；Naming 模块发布 `ServiceChangeEvent` 事件，`PushService` 订阅此事件并通过 gRPC Bi-directional Stream 推送客户端。`NotifyCenter` 是 Nacos 内部事件总线的核心。

**【核心类关系图】**

```
/* 图 1-13：NotifyCenter 事件驱动架构 */
┌────────────────────────────────────────────────────────────┐
│                      NotifyCenter                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ publishEvent(Event)                                 │  │
│  │ · 根据 Event 类型查找 Subscriber 列表               │  │
│  │ · 逐个调用 Subscriber.onEvent(event)               │  │
│  │ · 支持同步/异步发布 (Async/FastNotifyCenter)       │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Subscriber 注册表                                    │  │
│  │  Map<Class<? extends Event>,                         │  │
│  │      List<Subscriber>>                               │  │
│  └──────────────────────────────────────────────────────┘  │
│                          │                                  │
│     ┌────────────────────┼────────────────────┐            │
│     │                    │                    │            │
│  ┌──▼──────────┐ ┌─────▼────────┐ ┌──────▼─────────┐ │
│  │ ConfigData   │ │ ServiceChange │ │ Connection     │ │
│  │ ChangeEvent  │ │ Event        │ │ Event          │ │
│  │ · dataId    │ │ · serviceName│ │ · connectionId  │ │
│  │ · groupId   │ │ · instances  │ │ · eventType    │ │
│  └──┬──────────┘ └─────┬────────┘ └──────┬─────────┘ │
│     │                   │                   │              │
│  ┌──▼──────────┐ ┌─────▼────────┐ ┌──────▼─────────┐ │
│  │ AsyncNotify  │ │ PushService  │ │ Connection     │ │
│  │ Service     │ │             │ │ EventListener  │ │
│  │ (Subscriber)│ │ (Subscriber)│ │ (Subscriber)  │ │
│  └─────────────┘ └─────────────┘ └────────────────┘ │
└────────────────────────────────────────────────────────────┘
```

**【源码走读】**

**一、NotifyCenter 核心机制**

`NotifyCenter` 内部维护 `Map<Class<? extends Event>, List<Subscriber>>` 注册表。核心方法：

```java
// NotifyCenter.publishEvent()（common/src/main/java/com/alibaba/nacos/common/notify/NotifyCenter.java）
public static void publishEvent(Event event) {
    Class<? extends Event> eventType = event.getClass();
    List<Subscriber> subscribers = subscriberMap.get(eventType);
    if (subscribers != null) {
        for (Subscriber subscriber : subscribers) {
            subscriber.onEvent(event);  // 同步逐个通知所有订阅者
        }
    }
}
```

注册 Subscriber
public static void registerSubscriber(Subscriber subscriber, Class<? extends Event> eventType) {
    subscriberMap.computeIfAbsent(eventType, k -> new ArrayList<>()).add(subscriber);
}
```

**二、Event 类型体系**

Nacos 2.5.3 中的核心 Event 类型：

| Event 类型 | 发布者 | Subscriber | 触发时机 |
|-----------|--------|-----------|---------|
| `ConfigDataChangeEvent` | `ConfigCacheService` | `AsyncNotifyService` | 配置发布/更新后 |
| `ServiceChangeEvent` | `ServiceManager` | `PushService` | 服务实例注册/注销/健康状态变更 |
| `ConnectionRegisteredEvent` | `ConnectionManager` | `PushService` | 客户端建立 gRPC 连接 |
| `ConnectionDisconnectEvent` | `ConnectionManager` | `PushService` | 客户端断开 gRPC 连接 |
| `LocalDataChangeEvent` | `DistroProtocol` | `EphemeralConsistencyService` | Distro 数据同步完成 |

**三、同步 vs 异步发布**

`NotifyCenter` 提供两种发布模式：
- **同步发布**：`NotifyCenter.publishEvent(event)` —— 逐个同步调用 Subscriber 的 `onEvent()`，发布者阻塞直到所有 Subscriber 处理完成
- **异步发布**：`FastNotifyCenter.publishEvent(event)` —— 将事件放入线程池异步处理，发布者立即返回

默认使用异步发布模式（`FastNotifyCenter`），避免事件处理阻塞核心业务逻辑。

**四、Subscriber 注册机制**

各模块在启动阶段通过 `@PostConstruct` 方法注册自己的 Subscriber：
```java
@PostConstruct
public void init() {
    NotifyCenter.registerSubscriber(new AsyncNotifyService(), ConfigDataChangeEvent.class);
}
```

这种注册机制使得新增模块只需注册自己的 Subscriber 即可接入事件驱动架构，无需修改发布者代码——实现了模块间的完全解耦。

**五、事件驱动架构的优势**

1. **模块解耦**：Config 模块发布 `ConfigDataChangeEvent` 时不需要知道 `AsyncNotifyService` 的存在——`NotifyCenter` 负责事件路由
2. **扩展性**：新增功能只需注册新的 Subscriber 即可监听已有事件，无需修改发布者代码
3. **异步处理**：`FastNotifyCenter` 异步发布避免事件处理阻塞核心业务逻辑
4. **可观测性**：所有事件发布/订阅都可通过 `NotifyCenter` 的 Metrics 监控事件处理延迟和成功率

**【设计模式分析】**

1. **观察者模式（Observer Pattern）**：`NotifyCenter` 是经典的观察者模式实现——`EventPublisher`（主题/被观察者）发布 Event，`Subscriber`（观察者）订阅并响应 Event。`NotifyCenter` 作为中介者负责事件路由和解耦。

2. **中介者模式（Mediator Pattern）**：`NotifyCenter` 作为各模块间通信的中介者——模块间不直接通信，而是通过 `NotifyCenter` 发布和订阅 Event。这种设计避免了模块间的网状依赖，转为星型依赖（所有模块只依赖 `NotifyCenter`）。

3. **发布-订阅模式（Pub-Sub Pattern）**：`NotifyCenter` 的 `Map<Class<? extends Event>, List<Subscriber>>` 注册表实现了基于事件类型的发布-订阅——每个 Event 类型对应一个 Subscriber 列表，发布者不需要知道 Subscriber 的存在。

**【小结】**

`NotifyCenter` 是 Nacos 2.5.3 内部模块间通信的核心机制——通过 Event 发布/订阅实现模块间的完全解耦。各模块只需注册自己的 Subscriber 即可接入事件驱动架构，新增功能无需修改发布者代码。异步发布模式（`FastNotifyCenter`）避免了事件处理阻塞核心业务逻辑。
