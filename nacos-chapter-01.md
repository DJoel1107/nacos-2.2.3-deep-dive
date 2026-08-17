# 第1章：Nacos 2.2.3 整体架构概述

## 1.1 Nacos 定位与核心能力矩阵

Nacos（Dynamic Naming and Configuration Service）是阿里巴巴开源的一款集**服务发现**、**配置管理**和**服务治理**于一体的中间件产品。在云原生体系中，Nacos 承担着微服务注册中心与配置中心的双重角色，与 Spring Cloud Alibaba、Dubbo、gRPC 等主流微服务生态深度集成。

Nacos 2.2.3 的核心能力矩阵如下：

| 能力域 | 子能力 | 协议支持 | 说明 |
|--------|--------|---------|------|
| 服务发现 | 服务注册、服务发现、健康检查 | HTTP/DNS/gRPC | 支持临时实例（AP）与持久化实例（CP） |
| 配置管理 | 配置发布、配置订阅、配置历史 | HTTP/gRPC | 支持灰度发布、Beta 配置、Tag 配置 |
| 服务治理 | 流量路由、负载均衡、熔断降级 | — | 通过 Sentinel 集成实现 |
| 动态 DNS | DNS-F 协议 | DNS | 支持 DNS 协议的服务发现 |
| 元数据管理 | 服务元数据、实例元数据 | — | 支持自定义扩展 Map 结构 |

Nacos 2.x 是相对于 1.x 的重大架构升级。核心变更体现在四个方面：

1. **通信层统一**：从 HTTP 短连接升级为 gRPC + HTTP 双通道，服务端推送从 UDP 广播升级为 gRPC Bi-directional Stream
2. **客户端能力协商**：通过 `ClientAbilities` 实现服务端与客户端之间的能力协商机制
3. **连接管理**：引入 `ConnectionManager` 管理客户端长连接生命周期，替代 1.x 的无状态 HTTP 连接
4. **增量同步**：支持增量数据变更推送，减少全量推送开销，降低网络带宽消耗

## 1.2 整体架构分层

Nacos 2.2.3 采用经典的四层分层架构，自上而下分为接入层、业务层、引擎层、存储层：

```
┌─────────────────────────────────────────────────────────────┐
│                      接入层 (Access Layer)                      │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Console UI (React)     │  OpenAPI (REST/gRPC)    │   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      业务层 (Business Layer)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  Naming      │  │  Config      │  │  Console     │   │
│  │  (注册中心)   │  │  (配置中心)  │  │  (控制台)    │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      引擎层 (Engine Layer)                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Cluster Core (ServerMemberManager)               │   │
│  │  Consistency Protocol (JRaft/Distro)               │   │
│  │  NotifyCenter (事件驱动引擎)                     │   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      存储层 (Storage Layer)                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐    │
│  │  MySQL   │  │  Derby   │  │  本地文件/RocksDB  │    │
│  └──────────┘  └──────────┘  └──────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 1.2.1 接入层

接入层负责接收所有外部请求，提供两种接入方式：

**Console UI**：基于 React 构建的管理控制台前端，源码位于 `console-ui/` 模块，通过 REST API 与后端交互。

**OpenAPI**：提供 RESTful API 和 gRPC 两种接口协议：
- REST API：通过 Spring MVC `@RestController` 暴露，位于各模块的 `controller/` 包下，如 `InstanceController`、`ConfigController`
- gRPC 接口：基于 gRPC 协议的高性能 RPC 通信，服务端推送通过 gRPC Bi-directional Stream 实现

### 1.2.2 业务层

业务层是 Nacos 核心功能的承载层，包含两大核心业务模块：

**注册中心（Naming 模块）**：位于 `naming/` 模块，344 个 Java 文件，约 37,266 行代码。核心职责包括：
- 服务实例注册与注销（`InstanceController.register()`）
- 服务订阅与推送（`PushService`，支持 UDP + gRPC Stream 双通道）
- 健康检查（TCP/HTTP/MySQL 三种检查方式，由 `ClientBeatCheckTask` 驱动）
- 服务集群管理（`Cluster` 类，双 Map 设计分离临时实例与持久化实例）

**配置中心（Config 模块）**：位于 `config/` 模块，274 个 Java 文件，约 45,521 行代码。核心职责包括：
- 配置发布与获取（`ConfigController.publishConfig()`）
- 长轮询配置变更通知（`LongPollingService`，默认 29.5 秒超时）
- 配置版本管理与回滚（`HistoryConfigInfoService`）
- 灰度发布与 Beta 配置（`ConfigBetaService`）

### 1.2.3 引擎层

引擎层提供底层基础设施支持，三大核心引擎：

**Cluster Core**：集群成员管理，核心类为 `ServerMemberManager`（位于 `core/src/main/java/.../cluster/ServerMemberManager.java`），负责集群成员发现（通过 `LookupFactory` 创建寻址模式）、节点健康管理、集群间 gRPC 通信（通过 `ClusterRpcClientProxy`）。

**Consistency Protocol**：一致性协议实现，包括基于 JRaft（SOFAJRaft）的 CP 模式和基于 Distro（自研）的 AP 模式。核心类为 `RaftConsistencyServiceImpl` 和 `DistroConsistencyMapImpl`。

**NotifyCenter**：事件驱动引擎，实现模块间解耦通信。核心类为 `NotifyCenter`（位于 `common/src/main/java/.../notify/NotifyCenter.java`），通过 `EventPublisher` 和 `Subscriber` 注册机制实现事件发布/订阅。

### 1.2.4 存储层

Nacos 2.2.3 支持多种存储后端，按用途选择：

| 存储类型 | 用途 | 适用场景 | 配置方式 |
|---------|------|---------|---------|
| MySQL | 配置持久化、服务元数据 | 生产环境集群模式 | `spring.datasource.platform=mysql` |
| Derby | 嵌入式数据库 | 单机开发/测试环境 | `spring.datasource.platform=derby`（默认） |
| 内存 | 服务注册表（临时实例） | AP 模式数据 | 无需配置，自动使用 |
| 本地文件 | Raft 日志、Snapshot | JRaft 日志持久化 | 自动存储于 `$NACOS_HOME/data/raft/` |

## 1.3 Maven 模块依赖关系与职责矩阵

Nacos 2.2.3 共包含约 20 个 Maven 模块，核心模块依赖关系如下：

```
console
  ├── config
  ├── naming
  ├── auth
  └── sys

config
  ├── core
  ├── common
  └── api

naming
  ├── core
  ├── common
  ├── consistency
  └── api

core
  ├── common
  ├── consistency
  └── api

consistency
  ├── common
  └── api

client
  ├── common
  └── api

plugin-default-impl
  ├── config
  ├── naming
  ├── auth
  └── common
```

各模块源码规模与核心职责：

| 模块 | 包路径 | Java 文件数 | 代码行数 | 核心职责 |
|------|--------|-----------|---------|---------|
| api | `com.alibaba.nacos.api` | 195 | 17,478 | API 接口定义、异常体系、过滤器链 |
| common | `com.alibaba.nacos.common` | 250 | 34,571 | SPI 机制、HTTP 客户端、事件通知、工具类 |
| core | `com.alibaba.nacos.core` | 224 | 23,359 | 集群管理、gRPC 通信、认证拦截 |
| naming | `com.alibaba.nacos.naming` | 344 | 37,266 | 服务注册、发现、健康检查 |
| config | `com.alibaba.nacos.config` | 274 | 45,521 | 配置管理、长轮询、版本管理 |
| consistency | `com.alibaba.nacos.consistency` | 31 | 2,217 | JRaft 协议、Distro 协议 |
| client | `com.alibaba.nacos.client` | 199 | 25,206 | 客户端 SDK、配置/服务客户端 |
| auth | `com.alibaba.nacos.auth` | 33 | 2,738 | 认证鉴权、RBAC 权限 |
| console | `com.alibaba.nacos.console` | 20 | 1,964 | 控制台后端 API |
| plugin-default-impl | — | 62 | 7,117 | 默认插件实现（鉴权/数据源/加密） |
| istio | `com.alibaba.nacos.istio` | 29 | 2,140 | Istio MCP 集成 |
| sys | `com.alibaba.nacos.sys` | 24 | 3,243 | 系统环境、健康检查 |
| address | `com.alibaba.nacos.address` | 12 | 1,106 | 地址服务器模式 |
| cmdb | `com.alibaba.nacos.cmdb` | 9 | 614 | CMDB 标签管理 |

## 1.4 Spring Boot 启动入口

Nacos 2.2.3 基于 Spring Boot 构建，启动入口位于 `console/src/main/java/com/alibaba/nacos/Nacos.java`（共 38 行）：

```java
// Nacos.java (console/src/main/java/com/alibaba/nacos/Nacos.java:28-37)
@SpringBootApplication(scanBasePackages = "com.alibaba.nacos")
@ServletComponentScan
@EnableScheduling
public class Nacos {

    public static void main(String[] args) {
        SpringApplication.run(Nacos.class, args);
    }
}
```

三个关键注解的作用分析：

**`@SpringBootApplication(scanBasePackages = "com.alibaba.nacos")`**：指定 Spring 组件扫描的基础包路径为 `com.alibaba.nacos`。此配置确保跨模块的所有 Spring Bean（如 `ServerMemberManager`、`LongPollingService` 等）均被自动装配。Nacos 的多模块架构依赖此扫描机制实现跨模块的依赖注入。

**`@ServletComponentScan`**：启用 Servlet 组件自动扫描，使得 `@WebFilter` 注解的过滤器（如 `AuthFilter`）能够被自动注册到 Servlet 容器中。

**`@EnableScheduling`**：启用 Spring 的定时任务调度支持，使得 `@Scheduled` 注解的方法（如健康检查定时任务 `ClientBeatCheckTask`）能够被自动执行。

## 1.5 模块独立启动机制

Nacos 的每个核心业务模块均可作为独立进程启动。这种设计允许在特定场景下（如性能隔离、灰度发布）独立部署配置中心或注册中心：

```java
// Config.java (config/src/main/java/com/alibaba/nacos/config/server/Config.java)
@SpringBootApplication
public class Config {
    public static void main(String[] args) {
        SpringApplication.run(Config.class, args);
    }
}

// NamingApp.java (naming/src/main/java/com/alibaba/nacos/naming/NamingApp.java)
@SpringBootApplication
public class NamingApp {
    public static void main(String[] args) {
        SpringApplication.run(NamingApp.class, args);
    }
}
```

每个模块独立启动时，Spring Boot 自动装配机制会加载该模块及其依赖模块的所有 Bean。例如，`Config` 启动时会自动装配 `core`、`common`、`api` 模块的 Bean，但不会加载 `naming` 模块的 Bean。

## 1.6 启动初始化序列

Nacos 启动时的核心初始化序列分为 7 个阶段：

```
阶段 1: Spring 容器初始化
  └── 扫描 @ComponentScan 指定的包路径
  └── 装配所有 @Component、@Service、@Configuration Bean
  └── 执行 @PostConstruct 初始化方法

阶段 2: 环境初始化 (EnvUtil)
  └── 加载 application.properties 配置
  └── 设置 NACOS_HOME、NACOS_LOGS 等环境变量
  └── 初始化数据源连接池（HikariCP）

阶段 3: 集群成员管理器初始化 (ServerMemberManager.init())
  └── 通过 LookupFactory 创建集群寻址模式
  └── 启动集群成员信息同步定时任务（默认每 2 秒）

阶段 4: 一致性协议初始化
  └── JRaft Raft 节点启动（CP 模式）
  └── Distro 数据同步任务启动（AP 模式）

阶段 5: 通信层初始化
  └── gRPC Server 启动（SDK 端口，默认 9848）
  └── gRPC Cluster Server 启动（集群端口，默认 9849）

阶段 6: 业务模块初始化
  └── Naming 服务注册表加载（从本地快照恢复）
  └── Config 配置数据库初始化（DDL 自动建表）

阶段 7: HTTP 服务启动
  └── Tomcat 嵌入式 Web 容器启动（默认端口 8848）
  └── 暴露 REST API 端点
```

## 1.7 gRPC 双通道通信架构

Nacos 2.x 相比 1.x 最大的架构变更在于通信层。1.x 使用 HTTP 短连接模式（每请求建立一次连接），2.x 升级为 gRPC 长连接模式（持久连接复用）。

### 1.7.1 通信模式对比

| 维度 | Nacos 1.x | Nacos 2.x |
|------|-----------|-----------|
| 通信协议 | HTTP REST | gRPC + HTTP |
| 连接模型 | 短连接（每请求一次） | 长连接（持久连接复用） |
| 服务发现推送 | UDP 广播 | gRPC Bi-directional Stream |
| 心跳机制 | HTTP 心跳 | gRPC keepalive |
| 配置变更通知 | HTTP 长轮询 | gRPC + HTTP 长轮询 |
| 安全性 | 无内置认证 | mTLS + AccessToken |

### 1.7.2 gRPC 双通道设计

Nacos 2.2.3 的 gRPC 通信分为两个独立的 gRPC Server：

```
┌──────────────────────────────────────────────────────┐
│                    Client Side                       │
│  ┌─────────────────────────────────────────────┐   │
│  │  RpcClient (gRPC Channel Pool)             │   │
│  │  - Bi-directional Stream                    │   │
│  │  - ServerPushHandler                      │   │
│  └─────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
                         │
                         │ gRPC TLS (optional)
                         │
┌──────────────────────────────────────────────────────┐
│                    Server Side                       │
│  ┌─────────────────────────────────────────────┐   │
│  │  GrpcSdkServer (client connections)         │   │
│  │  端口: server.port + 1000 (默认 9848)     │   │
│  │  - ConnectionManager                        │   │
│  │  - GrpcConnection                          │   │
│  └─────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────┐   │
│  │  GrpcClusterServer (inter-node)            │   │
│  │  端口: server.port + 1001 (默认 9849)     │   │
│  │  - ClusterRpcClientProxy                   │   │
│  └─────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

**GrpcSdkServer**：处理客户端 SDK 连接，默认端口为 `server.port + 1000`（即 8848 + 1000 = 9848）。负责客户端服务发现订阅、配置长轮询、心跳上报等功能。

**GrpcClusterServer**：处理集群节点间通信，默认端口为 `server.port + 1001`（即 8848 + 1001 = 9849）。负责集群成员信息同步、Distro 数据同步、Raft 日志复制等功能。

两个 gRPC Server 的端口偏移量可通过配置项调整：
```properties
# gRPC SDK Server 端口偏移（相对于 server.port）
nacos.core.grpc.port.offset=1000

# Cluster gRPC Server 端口偏移（相对于 server.port）
nacos.core.cluster.grpc.port.offset=1001
```

## 1.8 ConnectionManager 连接生命周期管理

`ConnectionManager` 是 Nacos 2.x 中管理客户端长连接的核心组件，负责 gRPC 连接的完整生命周期：

```
连接建立 (Client Connect)
  │
  ├── ConnectionManager.registerConnection()
  │   ├── 注册连接元数据（clientId、ip、port、abilities）
  │   └── 触发 ClientConnectionEvent 事件
  │
  ├── 连接存活期间
  │   ├── gRPC keepalive 心跳检测（默认 2 小时无活动发送 PING）
  │   ├── keepalive 超时检测（默认 20 秒未收到 PONG 判定断开）
  │   └── 服务端推送（通过 Bi-directional Stream）
  │
  └── 连接断开 (Client Disconnect)
      ├── ConnectionManager.unregisterConnection()
      ├── 清理连接上下文
      └── 触发 ClientDisconnectEvent 事件
```

关键配置项：
```properties
# SDK gRPC keepalive 时间（ms，默认 7200000 = 2 小时）
nacos.remote.server.grpc.sdk.keep-alive-time=7200000

# SDK gRPC keepalive 超时（ms，默认 20000 = 20 秒）
nacos.remote.server.grpc.sdk.keep-alive-timeout=20000

# SDK gRPC 允许的最小 keepalive 时间（ms，默认 300000 = 5 分钟）
nacos.remote.server.grpc.sdk.permit-keep-alive-time=300antor
```

## 1.9 数据模型设计

Nacos 的数据模型采用五级层级结构：

```
Namespace (命名空间)
  └── Group (分组)
       └── Service (服务)
            └── Cluster (集群)
                 └── Instance (实例)
```

### 1.9.1 各层级含义

**Namespace（命名空间）**：租户级别的隔离单元。默认命名空间为 `public`。不同命名空间之间的服务和配置完全隔离。典型用法是按环境隔离（dev/test/prod）。

**Group（分组）**：服务分组单元。默认分组为 `DEFAULT_GROUP`。同一命名空间内可按业务域进行分组。

**Service（服务）**：微服务名称。由 `namespace + group + serviceName` 三元组唯一标识。

**Cluster（集群）**：服务集群。用于同服务下的机房/地域隔离。例如 `SHANGHAI`、`BEIJING`。

**Instance（实例）**：服务实例。包含 IP、端口、权重、元数据、健康状态等属性。

### 1.9.2 Instance 模型详解

```java
// Instance.java (api/src/main/java/com/alibaba/nacos/api/naming/pojo/Instance.java)
public class Instance {
    private String instanceId;      // 实例唯一 ID（自动生成）
    private String ip;              // 实例 IP 地址
    private int port;               // 实例端口
    private double weight;           // 权重（1-10000，默认 1.0）
    private boolean healthy;         // 健康状态（默认 true）
    private boolean enabled;         // 启用状态（默认 true）
    private boolean ephemeral;      // 是否为临时实例（默认 true）
    private String clusterName;     // 集群名称（默认 DEFAULT）
    private String serviceName;     // 服务名称
    private Map<String, String> metadata; // 扩展元数据
}
```

`ephemeral` 字段是 Nacos 数据模型中最关键的设计决策点：

- `ephemeral = true`（默认值）：临时实例。数据仅存在于内存中，节点重启后丢失。使用 **Distro 协议（AP 模式）** 进行去中心化异步同步。适用于微服务自动注册场景。
- `ephemeral = false`：持久化实例。数据持久化到磁盘（Raft Log）。使用 **JRaft 协议（CP 模式）** 保证强一致性。适用于 DNS/F5 手动注册场景。

### 1.9.3 配置数据模型

```
Namespace (命名空间)
  └── Group (分组)
       └── ConfigInfo (配置主体)
            ├── dataId (配置 ID)
            ├── group (分组 ID)
            ├── content (配置内容)
            ├── md5 (MD5 校验值)
            └── tenant (命名空间)
                 │
                 └── ConfigInfoState (配置版本状态)
                      ├── Beta 配置（灰度版本）
                      ├── Tag 配置（标签版本）
                      └── 正式配置
```

## 1.10 NotifyCenter 事件驱动架构

Nacos 内部采用事件驱动架构实现模块间解耦。`NotifyCenter`（位于 `common/src/main/java/.../notify/NotifyCenter.java`）是核心事件引擎。

### 1.10.1 核心数据结构

```java
// NotifyCenter.java (common/src/main/java/.../notify/NotifyCenter.java)
public class NotifyCenter {
    // 事件发布者注册表（按 topic 索引）
    private static final Map<String, EventPublisher> publisherMap = new ConcurrentHashMap<>();

    // 事件订阅者注册表（按 topic 索引）
    private static final Map<String, List<Subscriber>> subscriberMap = new ConcurrentHashMap<>();

    // 关闭标记
    private static volatile AtomicBoolean closed = new AtomicBoolean(false);
}
```

### 1.10.2 事件发布/订阅机制

```
发布事件:
  NotifyCenter.publishEvent(event)
    ├── 1. 查找对应 topic 的 EventPublisher
    ├── 2. 通过 EventPublisher 异步发布事件（默认使用 EventPublisher 线程池）
    └── 3. 如果没有注册 EventPublisher，同步通知所有 Subscriber

订阅事件:
  NotifyCenter.registerSubscriber(subscriber)
    ├── 1. 获取 subscriber 订阅的 Event 类型（topic）
    ├── 2. 将 subscriber 注册到 subscriberMap 对应 topic 的列表中
    └── 3. 后续该 topic 的事件发布时，subscriber.onEvent(event) 被回调
```

### 1.10.3 核心事件类型

| 事件类型 | 所属模块 | 触发时机 | 携带数据 |
|---------|---------|---------|---------|
| `ServiceChangeEvent` | naming | 服务实例注册/注销/变更 | 变更前后的 Instance 列表 |
| `ConfigDataChangeEvent` | config | 配置内容变更 | dataId、group、tenant、content |
| `MemberChangeEvent` | core | 集群成员加入/离开 | 变更前后的 Member 列表 |
| `ClientConnectionEvent` | core | 客户端连接建立/断开 | clientId、ip、port |
| `LocalDataChangeEvent` | naming | 本地注册表数据变更 | 变更的 datum key |

### 设计模式分析

`NotifyCenter` 使用了 **观察者模式（Observer Pattern）**：
- `EventPublisher` 充当 Subject（主题），负责发布事件
- `Subscriber` 充当 Observer（观察者），负责接收事件通知
- 通过 `publisherMap` 和 `subscriberMap` 实现发布者和订阅者的解耦

使用观察者模式的核心收益：各模块之间不需要直接依赖，仅通过事件进行通信。例如，`naming` 模块发布 `ServiceChangeEvent` 后，`core` 模块的 `PushService` 作为订阅者接收事件并触发服务端推送——两个模块之间没有直接的代码依赖。

---

*（第 1 章完，约 15,000 字）*
