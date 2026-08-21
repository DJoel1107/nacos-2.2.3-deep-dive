# Nacos 2.5.3 深度研究——前置知识

> **读者对象**：具备 Java 开发基础，希望深入理解 Nacos 源码的开发者  
> **阅读建议**：本章不涉及 Nacos 源码细节，仅覆盖阅读正文所需的关联知识。已熟悉某主题可直接跳过对应小节。

---

## 一、分布式系统理论基础

### 1.1 CAP 定理

CAP 定理（Brewer's Theorem）指出，一个分布式系统最多只能同时满足以下三项中的两项：

| 属性 | 含义 | 牺牲时的表现 |
|------|------|-------------|
| **C**onsistency（一致性） | 所有节点在同一时刻看到相同数据 | 写入后立即读到最新值 |
| **A**vailability（可用性） | 每个请求都能获得非错误响应 | 部分节点故障时系统仍可服务 |
| **P**artition Tolerance（分区容忍） | 系统在任意网络分区下仍能正常工作 | 节点间网络断开时系统不崩溃 |

Nacos 同时提供了 **AP 模式**（基于 Distro 协议，牺牲强一致性换取高可用）和 **CP 模式**（基于 JRaft 协议，牺牲可用性换取强一致性），用户可按服务粒度选择模式。关键设计：Instance 模型的 `ephemeral` 字段决定路由到 AP 还是 CP。

### 1.2 BASE 理论

BASE 是对 CAP 中 AP 模式的实践延伸：

- **B**asically **A**vailable（基本可用）：系统出现故障时允许损失部分可用性
- **S**oft state（软状态）：允许系统中的数据存在中间状态，该状态不影响系统整体可用性
- **E**ventually consistent（最终一致性）：系统中的所有数据副本经过一段时间的同步后最终达到一致状态

Nacos 的 Distro 协议就是 BASE 理论的典型实现：服务实例注册后不一定立即在所有节点可见，但最终会在集群内达成一致。

### 1.3 一致性模型

| 模型 | 保证 | Nacos 中的应用 |
|------|------|--------------|
| **强一致性（Linearizability）** | 写入后任何后续读都能看到最新值 | CP 模式下的服务注册（JRaft） |
| **最终一致性（Eventual Consistency）** | 如果不发生新的更新，最终所有副本将收敛到相同值 | AP 模式下的服务注册（Distro） |
| **读写一致性（Read-after-Write）** | 写入者自己能看到最新写入 | 配置中心的发布订阅通知 |

---

## 二、Raft 一致性协议基础

Nacos CP 模式的核心依赖是 JRaft（阿里巴巴开源的 Raft Java 实现）。理解 Nacos 的一致性机制需要先了解 Raft 协议的基础概念。

### 2.1 Raft 协议核心机制

Raft 将一致性分解为三个子问题：

**Leader 选举（Leader Election）**：
- 每个节点在三种状态之间转换：Follower → Candidate → Leader
- 选举基于**任期（Term）**，每个 Term 最多一个 Leader
- 节点在选举超时（150-300ms 随机）未收到 Leader 心跳时发起选举
- 获得超过半数投票的 Candidate 成为 Leader

**日志复制（Log Replication）**：
- Leader 接收客户端请求 → 追加到本地日志 → 并行发送 AppendEntries RPC 给所有 Follower
- Leader 在收到**超过半数** Follower 确认后，将日志条目应用到状态机，响应客户端
- Follower 的日志不一致时，Leader 通过回退找到双方日志中最后一个一致的条目，强制覆盖 Follower 之后的不一致日志

**快照（Snapshot）**：
- 日志不断增长会占用大量磁盘空间，Raft 定期对状态机做快照
- 快照后的日志条目可以安全删除
- 新加入节点或严重落后节点可直接通过快照 + 后续日志快速追上 Leader

### 2.2 Multi-Raft 与 JRaft

Nacos 使用的是 **Multi-Raft** 架构（多个 Raft 组），而非全局单个 Raft 组：
- 每个服务或配置分组独立维护一个 Raft 组
- 不同 Raft 组的 Leader 可以分布在不同的集群节点上，实现负载分散
- JRaft 在标准 Raft 基础上增加了 pre-vote、learner 等优化

---

## 三、gRPC 通信基础

Nacos 2.x 核心通信协议为 gRPC。理解 Nacos 的通信机制需要了解 gRPC 的基础概念。

### 3.1 Protobuf 序列化

gRPC 默认使用 Protocol Buffers（Protobuf）作为接口定义语言（IDL）和序列化协议：

- **定义 `.proto` 文件**：定义服务（Service）和消息（Message）
- **代码生成**：使用 `protoc` 编译器生成客户端和服务端代码
- **二进制序列化**：比 JSON/XML 体积更小、解析更快

Nacos 在 `api/src/main/proto/` 目录下定义了 gRPC 服务接口（如 `nacos_grpc_service.proto`）。

### 3.2 gRPC 四种通信模式

| 模式 | 说明 | Nacos 中的应用 |
|------|------|-------------|
| **Unary RPC** | 客户端发送单个请求，服务端返回单个响应 | 配置获取 `getConfig()` |
| **Server Streaming** | 客户端发送请求，服务端返回流式响应 | 历史配置批量导出 |
| **Client Streaming** | 客户端流式发送请求，服务端返回单个响应 | 批量注册实例（较少用） |
| **Bidirectional Streaming** | 双方可独立发送流式消息 | **核心：客户端服务订阅 + 服务端配置推送** |

Nacos 2.x 最关键的通信模式是 **Bi-directional Streaming**：客户端建立一条 gRPC 双向流连接后，服务端可以随时向客户端推送配置变更和服务实例变更通知，不需要客户端轮询。

### 3.3 gRPC 长连接 vs HTTP 短连接

| 维度 | HTTP 短连接（Nacos 1.x） | gRPC 长连接（Nacos 2.x） |
|------|---------------------------|---------------------------|
| 连接模型 | 每次请求建立新 TCP 连接 | 维持一条持久 TCP 连接 |
| 服务端推送 | 不支持，需客户端轮询 | 支持，基于 Bi-directional Stream |
| 连接开销 | 每次 TCP 握手 + TLS 握手 | 一次握手，后续复用 |
| 负载均衡 | L4 负载均衡 | L7 基于 HTTP/2 帧级负载均衡 |

---

## 四、Spring Boot 基础

Nacos 服务端本身是基于 Spring Boot 构建的 Java 应用。理解 Nacos 的启动流程和内部事件机制需要了解 Spring Boot 的相关机制。

### 4.1 Spring Boot 自动配置

`@SpringBootApplication` 注解包含三个核心注解：

- `@SpringBootConfiguration`：标记为配置类
- `@EnableAutoConfiguration`：触发 Spring Boot 自动配置机制。通过 `spring.factories` 文件注册自动配置类
- `@ComponentScan`：扫描当前包及子包下的 `@Component`、`@Service`、`@Controller` 等注解

Nacos 启动类 `Nacos.java`（`console/src/main/java/com/alibaba/nacos/Nacos.java`）使用 `@SpringBootApplication` 注解，并通过 `spring.factories` 加载各个模块的自动配置类。

### 4.2 Spring ApplicationEvent 事件机制

Spring 提供了基于观察者模式的事件机制：

- **事件发布**：`ApplicationEventPublisher.publishEvent(event)`
- **事件监听**：`@EventListener` 注解或 `ApplicationListener` 接口

Nacos 内部核心使用了 Spring 事件机制。`NotifyCenter` 是 Nacos 自建的事件发布/订阅中心（非 Spring 原生），用于模块间解耦。两者并存：启动初始化阶段使用 Spring 事件，运行时业务事件使用 `NotifyCenter`。

---

## 五、Java SPI 机制

Nacos 的插件体系完全基于 Java SPI（Service Provider Interface）机制实现。理解 Nacos 的插件扩展必须先了解 SPI。

### 5.1 Java SPI 基础

Java SPI 是一种服务发现机制：

1. **定义接口**：在 `api` 模块中定义 SPI 接口（如 `AuthPluginService`）
2. **提供实现**：在 `plugin-default-impl` 模块中提供接口的具体实现类
3. **注册实现**：在 `META-INF/services/` 目录下创建文件，文件名=接口全限定名，文件内容=实现类全限定名
4. **加载实现**：通过 `java.util.ServiceLoader.load(接口.class)` 加载所有实现类

### 5.2 Nacos SPI 扩展点

Nacos 2.5.3 提供的主要 SPI 扩展点：

| SPI 接口 | 用途 | 默认实现 |
|----------|------|---------|
| `AuthPluginService` | 认证插件 | `NacosDefaultAuthPluginServiceImpl` |
| `DataSourcePluginService` | 数据源插件 | Derby + MySQL 内置 |
| `ConfigEncryptionPluginService` | 配置加密插件 | AES 对称加密 |
| `TracePluginService` | 链路追踪插件 | 无默认实现 |

---

## 六、微服务架构基础

### 6.1 服务发现模式

| 模式 | 说明 | Nacos 的对应实现 |
|------|------|-----------------|
| **客户端发现** | 客户端直接从注册中心查询并负载均衡 | Nacos SDK + Ribbon/LoadBalancer |
| **服务端发现** | 客户端通过负载均衡器转发，负载均衡器查询注册中心 | Nacos + Spring Cloud Gateway |
| **DNS-F 发现** | 通过 DNS 解析出服务端点 | Nacos DNS-F 支持标准 DNS 查询 |

### 6.2 配置管理核心概念

| 概念 | Nacos 中的对应 |
|------|---------------|
| **配置发布** | 通过 ConfigController REST API 或 Nacos SDK 发布配置内容 |
| **配置订阅** | 客户端通过 gRPC Bi-directional Stream 订阅配置变更通知 |
| **灰度发布** | 配置变更可指定目标 IP 范围逐步生效 |
| **历史回滚** | 保留最近 30 天配置变更历史，支持一键回滚 |
| **配置加密** | 敏感配置（如数据库密码）可通过 SPI 加密插件加解密 |

### 6.3 健康检查机制

| 检查类型 | 说明 | Nacos 实现 |
|----------|------|-----------|
| **TCP 检查** | 检测实例端口是否可 TCP 连接 | `TcpSuperSenseProcessor` |
| **HTTP 检查** | 检测实例 HTTP 接口是否返回 200 | `HttpHealthCheckProcessor` |
| **MySQL 检查** | 检测实例 MySQL 连接状态 | `MysqlHealthCheckProcessor` |
| **无检查** | 永久实例（ephemeral=false）不自动健康检查 | — |

---

## 七、Nacos 数据模型速览

在进入正文前，快速预览 Nacos 的数据层级模型：

```
Namespace (命名空间，租户隔离顶层)
 └── Group (分组，服务/配置分类)
      └── Service (服务，或 Config 配置)
           └── Cluster (集群，数据中心/机房物理分组)
                └── Instance (实例，IP:Port 网络端点)
```

**关键字段**：
- `ephemeral`（boolean）：`true` = 临时实例（由客户端心跳维护，过期自动剔除，走 AP Distro），`false` = 永久实例（需手动注销，走 CP JRaft）
- `healthy`（boolean）：当前实例健康状态
- `weight`（double）：实例权重，用于客户端负载均衡

---

> **阅读建议**：本章不涉及 Nacos 源码细节。进入每个章节正文时，可回到此处回顾相关前置概念。
