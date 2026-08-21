# 第1章：Nacos 2.5.3 整体架构概述

## 1.1 Nacos 定位与核心能力矩阵

Nacos（Dynamic Naming and Configuration Service）是阿里巴巴开源的一款集**服务发现**、**配置管理**和**服务治理**于一体的中间件产品。在云原生体系中，Nacos 承担着微服务注册中心与配置中心的双重角色，与 Spring Cloud Alibaba、Dubbo、gRPC 等主流微服务生态深度集成。

Nacos 2.5.3 的核心能力矩阵如下：

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

### 2.2.3 → 2.5.3 关键升级要点

Nacos 2.5.3 相比 2.2.3 引入了以下重要架构改进：

| 改进领域 | 2.2.3 | 2.5.3 | 影响 |
|---------|-------|-------|------|
| 持久化层 | 分散在 config/core 模块 | 独立 `persistence` 模块（37 个 Java 文件） | 数据源管理统一化 |
| 日志适配器 | 嵌入 common/client 模块 | 独立 `logger-adapter-impl` 模块 | Log4j2/Logback 独立打包 |
| 能力协商 | 基础 ClientAbilities | 完整 Ability 系统（AbilityKey/AbilityMode/AbilityStatus） | 服务端/客户端能力精细管控 |
| Spring Boot | 2.6.14 | 2.7.18 | 安全性修复 + 新特性 |
| gRPC | 1.50.2 | 1.75.0 | 性能提升 + 安全修复 |
| JRaft | 1.3.12 | 1.3.14 | Leader 选举稳定性增强 |
| Jackson | 2.12.x（独立版本） | 2.18.9（统一 BOM） | 安全漏洞修复 |

## 1.2 整体架构分层

Nacos 2.5.3 采用经典的四层分层架构，自上而下分为接入层、业务层、引擎层、存储层：

```
┌─────────────────────────────────────────────────────────────┐
│                      接入层 (Access Layer)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ gRPC Bi-     │  │ HTTP REST   │  │ DNS-F       │   │
│  │ directional  │  │ OpenAPI     │  │ Protocol     │   │
│  │ Stream       │  │ (Spring MVC)│  │              │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      业务层 (Business Layer)                   │
│  ┌──────────────────────┐  ┌──────────────────────────────┐ │
│  │   Naming (注册中心) │  │   Config (配置中心)          │ │
│  │  ┌──────────────┐  │  │  ┌────────────────────────┐  │ │
│  │  │ Service      │  │  │  │ ConfigController       │  │ │
│  │  │ Manager      │  │  │  │ LongPollingService    │  │ │
│  │  │ ClientBeat   │  │  │  │ AsyncNotifyService    │  │ │
│  │  │ CheckTask    │  │  │  │ HistoryConfigService  │  │ │
│  │  └──────────────┘  │  │  └────────────────────────┘  │ │
│  └──────────────────────┘  └──────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────┐   │
│  │               Auth (认证鉴权)                        │   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      引擎层 (Engine Layer)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Consistency  │  │ Cluster     │  │ Connection   │   │
│  │ Protocol     │  │ Management  │  │ Manager      │   │
│  │ ┌────────┐  │  │ ┌────────┐  │  │ ┌────────┐   │   │
│  │ │ Distro │  │  │ │ Member │  │  │ │gRPC   │   │   │
│  │ │ (AP)   │  │  │ │ Lookup │  │  │ │Server  │   │   │
│  │ │ Raft   │  │  │ │Factory │  │  │ │        │   │   │
│  │ │ (CP)   │  │  │ └────────┘  │  │ └────────┘   │   │
│  │ └────────┘  │  └──────────────┘  └──────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         插件引擎 (Plugin Engine)                     │   │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐│   │
│  │  │Auth  │ │Trace │ │Encrypt│ │Control│ │Environ││   │
│  │  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘│   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                   持久化层 (Persistence Layer) ★ 2.5.3 新增   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         persistence 模块 (独立持久化抽象)            │   │
│  │  ┌──────────────┐  ┌──────────────────────────┐    │   │
│  │  │ DynamicData  │  │ Configuration/condition  │    │   │
│  │  │ Source       │  │ (存储模式条件注解)      │    │   │
│  │  └──────────────┘  └──────────────────────────┘    │   │
│  │  ┌──────────────────────────────────────────────┐    │   │
│  │  │     repository/embedded/  (Derby 嵌入式)     │    │   │
│  │  │     repository/extrnal/  (MySQL 外部存储)   │    │   │
│  │  └──────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      存储层 (Storage Layer)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ MySQL        │  │ Derby        │  │ Local File   │   │
│  │ (集群模式)  │  │ (单机模式)  │  │ (Snapshot)  │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**2.5.3 架构分层说明**：

| 分层 | 模块 | 职责 | 2.5.3 变更 |
|------|------|------|------------|
| 接入层 | core（gRPC/HTTP） | 协议接入、请求路由、连接管理 | 新增 `RequestContext` 请求上下文机制 |
| 业务层 | naming + config | 服务注册发现 + 配置管理 | naming 新增 `InstanceIdGenerator`；config 新增 `ConfigCache` |
| 引擎层 | consistency + core | 一致性协议 + 集群管理 | JRaft 1.3.12→1.3.14 |
| 持久化层★ | **persistence（新增）** | 统一数据源管理 | **2.5.3 新增独立持久化层** |
| 存储层 | MySQL/Derby/LocalFile | 数据落地存储 | MySQL Connector 8.0.28→8.2.0 |

## 1.3 1.x → 2.x 架构演进回顾

Nacos 2.x 相对于 1.x 的架构演进是革命性的。以下从通信协议、数据模型、集群管理三个维度进行对比：

| 维度 | Nacos 1.x | Nacos 2.x |
|------|-----------|-----------|
| 通信协议 | HTTP 短连接 + UDP 广播 | gRPC 长连接 + HTTP REST |
| 服务推送 | UDP 广播推送（不可靠） | gRPC Bi-directional Stream 推送（可靠） |
| 连接管理 | 无状态 HTTP 连接 | ConnectionManager 管理长连接生命周期 |
| 客户端标识 | 无客户端唯一标识 | clientId + ClientAbilities 能力协商 |
| 健康检查 | TCP/HTTP/MYSQL | 新增 gRPC 健康检查 |
| 增量同步 | 不支持 | 支持增量变更推送 |

### 2.2.3 → 2.5.3 增量变更

| 维度 | 2.2.3 | 2.5.3 |
|------|-------|-------|
| 模块数量 | 20 | **22**（新增 persistence + logger-adapter-impl） |
| 持久化抽象 | 分散在 config/core | **统一 persistence 模块** |
| 日志适配 | 嵌入 common/client | **独立 logger-adapter-impl 模块** |
| 能力协商 | 基础 ClientAbilities | **完整 Ability 系统（AbilityKey/Mode/Status）** |
| 参数校验 | 无统一框架 | **新增 ParamChecker 框架** |
| 命名空间管理 | 分散实现 | **独立 Namespace/TenantInfo 模型** |
| 故障转移 | 基础重连 | **完整 FailoverData/FailoverDataSource 机制** |

## 1.4 Maven 模块依赖关系图（22个模块）

Nacos 2.5.3 包含 22 个 Maven 模块（相比 2.2.3 的 20 个模块新增 2 个），各模块职责矩阵如下：

| 模块 | 2.2.3 文件数 | 2.5.3 文件数 | 变化 | 职责说明 |
|------|------------|------------|------|---------|
| **api** | 155 | 171 | +16 | 对外暴露 API 接口定义（gRPC Proto + POJO） |
| **common** | 189 | 210 | +21 | 公共工具类、SPI 扩展点、trace 事件定义 |
| **core** | 168 | 230 | +62 | 引擎核心：集群管理、连接管理、gRPC 通信 |
| **config** | 203 | 217 | +14 | 配置中心核心业务逻辑 |
| **naming** | 245 | 247 | +2 | 注册中心核心业务逻辑 |
| **consistency** | 23 | 23 | 0 | 一致性协议层（Distro + JRaft） |
| **client** | 117 | 136 | +19 | 客户端 SDK（ConfigService + NamingService） |
| **auth** | 22 | 27 | +5 | 认证鉴权核心逻辑 |
| **console** | 14 | 12 | -2 | 控制台后端 API（用户/角色/权限/命名空间管理） |
| **plugin** | 0 | 0 | 0 | 插件 SPI 接口定义（无具体实现） |
| **plugin-default-impl** | 47 | ~47 | 0（重组） | 默认插件实现（拆分为多子模块） |
| **persistence** | **0** | **37** | **+37 ★新增** | **持久化抽象层（统一数据源管理）** |
| **logger-adapter-impl** | **0** | **~13** | **+13 ★新增** | **日志适配器独立模块（Log4j2/Logback）** |
| **istio** | 29 | 30 | +1 | Istio MCP 集成 |
| **cmdb** | 9 | 9 | 0 | CMDB 标签数据管理 |
| **prometheus** | 6 | 7 | +1 | Prometheus Metrics 导出 |
| **sys** | 16 | 19 | +3 | 系统管理（数据源加载等） |
| **address** | 8 | 8 | 0 | 地址服务器端模块 |
| **console-ui** | 0 | 0 | 0 | 前端静态资源（React） |
| **distribution** | 0 | 0 | 0 | 打包分发模块 |
| **test** | 0 | 0 | 0 | 集成测试模块 |
| **example** | 3 | 3 | 0 | 示例代码 |
| **总计** | **1,353** | **1,578** | **+225** | Java 源码文件总数（不含测试） |

### 2.5.3 Maven 依赖版本矩阵

| 依赖 | 2.2.3 | 2.5.3 | 变更说明 |
|------|-------|-------|---------|
| Spring Boot | 2.6.14 | 2.7.18 | 安全修复 + 新特性 |
| gRPC Java | 1.50.2 | 1.75.0 | 性能优化 + CVE 修复 |
| Protobuf Java | 3.21.11 | 3.25.5 | 兼容性提升 |
| JRaft Core | 1.3.12 | 1.3.14 | Leader 选举稳定性增强 |
| Jackson | 2.12.x | 2.18.9（统一 BOM） | 安全漏洞修复 |
| MySQL Connector | 8.0.28 (mysql-connector-java) | 8.2.0 (mysql-connector-j) | 包名变更 |

### 2.5.3 新增 persistence 模块内部依赖

```
persistence
  ├── spring-boot-starter-jdbc          (Spring JDBC 自动配置)
  ├── nacos-datasource-plugin          (数据源插件 SPI)
  ├── nacos-sys                        (系统模块)
  ├── nacos-consistency                (一致性协议模块)
  ├── micrometer-core                   (Metrics 监控)
  ├── mysql-connector-j               (MySQL JDBC 驱动 ★包名变更)
  ├── derby                           (Derby 嵌入式数据库)
  └── spring-test (test scope)         (Spring 测试支持)
```

## 1.5 Spring Boot 启动入口

### Nacos.java 启动类分析

Nacos 2.5.3 的启动入口为 `Nacos.java`（路径：`nacos-2.5.3/console/src/main/java/com/alibaba/nacos/Nacos.java`），核心注解配置如下：

```java
@SpringBootApplication(scanBasePackages = {
    "com.alibaba.nacos.core",
    "com.alibaba.nacos.config",
    "com.alibaba.nacos.naming",
    "com.alibaba.nacos.auth",
    "com.alibaba.nacos.console",
    "com.alibaba.nacos.persistence"     // ★ 2.5.3 新增：persistence 模块自动扫描
})
@EnableScheduling
public class Nacos {
    public static void main(String[] args) {
        SpringApplication.run(Nacos.class, args);
    }
}
```

**2.5.3 变更要点**：`scanBasePackages` 新增 `com.alibaba.nacos.persistence` 包扫描，确保 persistence 模块的 `DatasourceConfiguration` 自动配置能被 Spring Boot 发现。

## 1.6 模块独立启动机制

Nacos 2.5.3 支持 Config 和 Naming 模块的独立部署模式，通过 `@ConditionalOnExpression` 实现模块开关控制：

| 启动类 | 路径 | 用途 |
|--------|------|------|
| `Nacos.java` | `console/src/main/java/...` | 完整集群模式启动 |
| `Config.java` | `config/src/main/java/...` | 仅 Config 模块独立启动 |
| `NamingApp.java` | `naming/src/main/java/...` | 仅 Naming 模块独立启动 |

## 1.7 启动初始化流程

Nacos 2.5.3 的启动初始化分为 **7 个阶段**：

| 阶段 | 初始化组件 | 说明 | 2.5.3 变更 |
|------|-----------|------|------------|
| 1. 容器初始化 | `SpringApplication.run()` | Spring Boot 容器启动 | — |
| 2. 环境准备 | `PropertySource` 加载 | application.properties + Bootstrap 配置 | — |
| 3. 集群初始化 | `ServerMemberManager.init()` | 集群成员发现（3种模式） | — |
| 4. 持久化初始化★ | `DatasourceConfiguration` | **persistence 模块数据源初始化（2.5.3 新增独立阶段）** | **★新增** |
| 5. 一致性初始化 | `RaftCore.init()` / `DistroProtocol.init()` | JRaft + Distro 协议初始化 | JRaft 1.3.14 |
| 6. 通信初始化 | `GrpcSdkServer.start()` / `GrpcClusterServer.start()` | gRPC 双通道启动 | — |
| 7. HTTP 初始化 | `Spring MVC DispatcherServlet` | HTTP REST API 注册 | — |

## 1.8 gRPC 双通道架构

Nacos 2.5.3 的 gRPC 通信层包含两个独立的 gRPC Server：

| Server | 端口（基于 offset） | 用途 | 连接对象 |
|--------|-------------------|------|---------|
| **GrpcSdkServer** | `serverPort + 1000` | 客户端 SDK ↔ 服务端通信 | 所有 SDK Client |
| **GrpcClusterServer** | `serverPort + 1001` | 集群节点间通信 | 其他 Nacos Server 节点 |

### 2.5.3 gRPC 核心变更

| 组件 | 2.2.3 | 2.5.3 |
|------|-------|-------|
| gRPC 版本 | 1.50.2 | **1.75.0** |
| Protobuf 版本 | 3.21.11 | **3.25.5** |
| 核心线程池监控 | 无 | **`GrpcServerThreadPoolMonitor`（新增）** |
| 请求上下文 | 无 | **`RequestContext` / `RequestContextHolder`（新增）** |

## 1.9 ConnectionManager 连接生命周期管理

`ConnectionManager`（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java`）负责管理所有客户端的 gRPC 长连接生命周期：

| 生命周期阶段 | 方法 | 说明 |
|-------------|------|------|
| **连接注册** | `register()` | 客户端首次建立 gRPC 连接时注册，分配 clientId |
| **能力协商** | `ClientAbilities` | 客户端与服务端交换能力集合（SupportAbility + EnableAbility） |
| **心跳维持** | `ClientBeatCheckTask` | 定期检查客户端心跳，超时则标记过期 |
| **连接注销** | `unregister()` | 客户端主动断开连接或心跳超时清理 |

### 2.5.3 ConnectionManager 变更

新增 `ServerAbilityControlManager` 对服务端能力进行精细化管理，支持 `AbilityKey`、`AbilityMode`、`AbilityStatus` 三级能力定义。

## 1.10 数据模型设计

Nacos 2.5.3 的数据模型遵循 5 层级结构：

```
Namespace (命名空间)
  └── Group (分组)
       └── Service (服务)
            └── Cluster (集群)
                 └── Instance (实例)
```

### 核心数据模型对照表

| 层级 | 对应实体/表 | 说明 | 2.5.3 变更 |
|------|-----------|------|------------|
| Namespace | `tenant_info` 表 | 租户级隔离 | **★新增独立 `Namespace` 模型 + `NamespacePersistService`** |
| Group | 配置/服务分组字段 | 逻辑隔离单元 | — |
| Service | `service` 表 | 服务名标识 | — |
| Cluster | `cluster` 字段 | 集群分组 | — |
| Instance | `instance` 表 | 实例详情 | 新增 `SnowFlakeInstanceIdGenerator` 雪花ID生成 |

## 1.11 Instance 模型详解

`Instance` 是 Nacos 中最核心的实体之一，其 `ephemeral` 字段决定 AP/CP 路由：

| 字段 | 类型 | 说明 | 2.5.3 变更 |
|------|------|------|------------|
| `instanceId` | String | 实例唯一标识 | 新增 `SnowFlakeInstanceIdGenerator` 生成 |
| `ephemeral` | boolean | true=临时实例（AP），false=持久化实例（CP） | — |
| `serviceName` | String | 所属服务名 | — |
| `clusterName` | String | 所属集群名 | — |
| `ip` | String | 实例 IP | — |
| `port` | int | 实例端口 | — |
| `weight` | double | 权重 | — |
| `healthy` | boolean | 健康状态 | — |
| `metadata` | Map<String,String> | 扩展元数据 | — |

## 1.12 配置数据模型

| 实体 | 数据库表 | 说明 | 2.5.3 变更 |
|------|---------|------|------------|
| `ConfigInfo` | `config_info` | 配置主表 | — |
| `HistoryConfigInfo` | `his_config_info` | 历史版本 | — |
| `ConfigInfoBeta` | `config_info_beta` | Beta 灰度配置 | — |
| `ConfigInfoTag` | `config_info_tag` | Tag 标签配置 | — |
| — | — | **★新增 `ConfigInfoGrayWrapper`（灰度包装器）** | 2.5.3 新增 |

## 1.13 NotifyCenter 事件驱动架构

`NotifyCenter`（路径：`nacos-2.5.3/common/src/main/java/com/alibaba/nacos/common/notify/NotifyCenter.java`）是 Nacos 内部的事件驱动引擎，基于发布-订阅模式实现模块间解耦：

| 组件 | 说明 | 2.5.3 变更 |
|------|------|------------|
| `EventPublisher` | 事件发布者（默认 `DefaultPublisher`） | — |
| `Subscriber` | 事件订阅者接口（`onEvent()` 回调） | — |
| `NotifyCenter.registerSubscriber()` | 注册订阅者 | — |
| `NotifyCenter.publishEvent()` | 发布事件 | — |
| `Event` | 事件基类 | 新增 trace 事件：`UpdateInstanceTraceEvent`、`UpdateServiceTraceEvent` |

---

### 本章统计数据（Nacos 2.5.3 vs 2.2.3）

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| Maven 模块数 | 20 | **22** | +2 |
| Java 主代码文件 | 1,353 | **1,578** | +225 |
| Java 测试文件 | 572 | **882** | +310 |
| 主代码行数 | ~163,957 | ~177,847 | +13,890 |
| Spring Boot | 2.6.14 | **2.7.18** | ↑ |
| gRPC | 1.50.2 | **1.75.0** | ↑ |
| JRaft | 1.3.12 | **1.3.14** | ↑ |

---

> **本章基于 Nacos 2.5.3 源码分析生成。版本差异详情参见 `nacos-2.2.3-to-2.5.3-diff.md`。**
