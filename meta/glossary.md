# Nacos 2.5.3 深度研究文档——术语表

> **使用规则**：
> 1. 全文同一概念使用统一的术语
> 2. 每章首次出现术语时给出全称，格式：`中文术语（English Full Name）`
> 3. 禁止使用"禁止替代词"列中的表达

---

## 一、核心概念

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 1 | 服务发现 | Service Discovery | Nacos Naming 模块提供的服务注册与发现能力，支持 HTTP/DNS-F/gRPC 三种协议 | 1.1 | "服务注册发现" |
| 2 | 配置管理 | Configuration Management | Nacos Config 模块提供的配置发布、订阅、历史版本管理能力 | 1.1 | "配置中心功能" |
| 3 | 服务治理 | Service Governance | 包括流量控制、熔断降级、负载均衡等服务间通信管控能力 | 1.1 | "服务管控" |
| 4 | 命名空间 | Namespace | Nacos 中的租户隔离单元，用于实现多环境/多租户的配置和服务隔离 | 1.10 | "命名空间ID"（仅指 tenant ID） |
| 5 | 分组 | Group | Nacos 中的二级分类单元，用于在同一 Namespace 内对服务或配置进行归类 | 1.10 | — |
| 6 | 服务 | Service | Nacos Naming 中的一个逻辑服务单元，包含多个集群和实例 | 1.10 | "微服务"（指业务概念时除外） |
| 7 | 集群 | Cluster | Service 下的物理分组，同一 Service 内的不同 Cluster 可部署在不同数据中心 | 1.10 | — |
| 8 | 实例 | Instance | 服务的最小粒度单元，代表一个可提供服务的网络端点（IP:Port） | 1.10 | "节点"、"服务器" |

## 二、架构分层

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 9 | 接入层 | Access Layer | 负责客户端请求接入与协议转换，包括 HTTP REST API 和 gRPC 两种接入方式 | 1.3 | "接入模块" |
| 10 | 业务层 | Business Layer | 核心业务逻辑层，包含 Naming（注册中心）和 Config（配置中心）两个核心子模块 | 1.3 | "业务模块" |
| 11 | 引擎层 | Engine Layer | 提供一致性协议、集群管理、数据持久化等底层引擎能力 | 1.3 | "核心引擎层" |
| 12 | 存储层 | Storage Layer | 负责数据持久化，支持嵌入式 Derby（内置）和外部 MySQL（生产）两种模式 | 1.3 | "持久化层" |
| 13 | 控制台 | Console | Web 管理控制台，提供服务管理、配置管理、集群管理、权限控制等可视化界面 | 10.1 | "管理后台" |

## 三、核心模块

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 14 | Naming 模块 | Naming Module | 注册中心核心模块，提供服务注册、发现、健康检查能力，基于 AP（Distro）和 CP（JRaft）双模式 | 丛 2.1 | "Naming" |
| 15 | Config 模块 | Config Module | 配置中心核心模块，提供配置发布、订阅、灰度发布、历史回滚能力 | 3.1 | "Config" |
| 16 | Core 模块 | Core Module | 集群管理核心模块，负责集群成员管理、节点寻址、元数据同步 | 5.1 | "Core" |
| 17 | Persistence 模块 | Persistence Module | **2.5.3 新增** 独立持久化模块，将原分散在 Config/Core 中的数据源管理抽离为独立模块 | 6.1 | "持久化模块" |
| 18 | Client 模块 | Client SDK | Nacos 客户端 SDK，提供 ConfigService 和 NamingService 两大核心接口 | 7.1 | "客户端" |
| 19 | Plugin 模块 | Plugin Module | 插件 SPI 扩展模块，提供鉴权、数据源、配置加密等扩展点 | 8.1 | "插件模块" |
| 20 | Auth 模块 | Auth Module | 认证与授权模块，提供 JWT、AK-SK 等多种认证方式及 RBAC 权限模型 | 9.1 | "认证模块" |
| 21 | Istio 模块 | Istio Module | Istio Service Mesh 集成模块，提供 MCP（Mesh Configuration Protocol）服务网格支持 | 1.4 | "Istio" |
| 22 | Address 模块 | Address Module | 节点寻址模块，负责集群节点地址的发现与管理 | 5. sacrific | "地址模块" |

## 四、一致性协议

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 23 | AP 模式 | AP Mode (Available-Partition) | 可用优先模式，基于 Distro 协议实现的去中心化最终一致性，优先保证可用性 | LLA 1.2 | "AP" |
| 24 | CP 模式 | CP Mode (Consistent-Partition) | 一致优先模式，基于 JRaft 协议实现的强一致性，优先保证数据一致性 | 1.2 | "CP" |
| 25 | Distro 协议 | Distro Protocol | Nacos 自研的去中心化最终一致性协议，使用一致性哈希算法分发数据，用于 AP 模式下的服务数据同步 | 2.7 | "Distro" |
| 26 | JRaft | JRaft | 阿里巴巴开源的 Raft 协议 Java 实现，包含 Leader 选举、日志复制、快照三大核心组件，用于 CP 模式 | 4.1 | "Raft"（指代 Nacos 中的 JRaft 实现时） |
| 27 | Raft Leader 选举 | Raft Leader Election | JRaft 集群中的 Leader 选举机制，基于任期（Term）和投票（Vote）机制 | 4.2 | "Leader 选取" |
| 28 | Raft 日志复制 | Raft Log Replication | JRaft 集群中的日志复制机制，Leader 将日志条目复制到所有 Follower，达成多数确认后提交 | 4.3 | "日志同步" |
| 29 | Raft 快照 | Raft Snapshot | JRaft 状态机快照机制，定期对状态机做快照以压缩日志、加速回放 | 4.4 | — |
| 30 | 一致性哈希 | Consistent Hash | Distro 协议使用的数据分布算法，通过虚拟节点映射解决物理节点增删时的数据倾斜问题 | 2.8 | "一致性hash" |

## 五、通信协议

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 31 | gRPC | gRPC Remote Procedure Call | Google 开源的高性能 RPC 框架，Nacos 2.x 的核心通信协议，支持 Bi-directional Stream | 1.2 | "grpc" |
| 32 | Bi-directional Stream | Bi-directional Stream | gRPC 双向流通信模式，客户端和服务端可同时发送多条消息，实现服务端推送能力 | 1.8 | "双向流" |
| 33 | HTTP REST API | HTTP REST API | Nacos 提供的 HTTP RESTful 接口，兼容 1.x 客户端及非 Java 语言接入 | 1.2 | "HTTP API" |
| 34 | DNS-F | DNS for Service Discovery | 基于 DNS 协议的服务发现方式，支持 DNS 标准格式的服务查询 | 2.10 | "DNS" |
| 35 | GrpcSdkServer | gRPC SDK Server | 面向 SDK 客户端的 gRPC 服务端，负责处理客户端服务发现和配置订阅请求 | 1.8 | "SDK Server" |
| 36 | GrpcClusterServer | gRPC Cluster Server | 面向集群节点间的 gRPC 服务端，负责集群间数据同步和通信 | 1.8 | "Cluster Server" |
| 37 | ConnectionManager | Connection Manager | 连接管理器，负责 gRPC 连接的注册、注销、心跳检测、能力协商 | 1.9 | "连接管理" |

## 六、数据模型

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 38 | ConfigInfo | Configuration Information | 配置信息模型，包含配置内容的完整信息，如 dataId、group、content、md5 | 1.12 | "配置信息" |
| 39 | HisConfigInfo | History Configuration Information | 历史配置信息模型，记录每次配置变更的历史版本，用于配置回滚 | 1.12 | "历史配置" |
| 40 | ServiceManager | Service Manager | 服务注册表核心管理组件，维护 serviceMap 数据结构 | 2.4 | "服务管理器" |
| 41 | ephemeral | Ephemeral Instance | 临时实例标识，true 表示 AP 模式（Distro 协议），false 表示 CP 模式（JRaft 协议） | 1.11 | "临时节点" |
| 42 | BeatInfo | Beat Information | 心跳信息模型，包含服务名、IP、端口、心跳周期等客户端心跳上报数据 | 2.14 | "心跳信息" |

## 七、健康检查

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 43 | HealthCheckType | Health Check Type | 健康检查类型枚举，包含 TCP、HTTP、MySQL、None 四种检查方式 | 2.13 | "健康检查类型" |
| 44 | ClientBeatCheckTask | Client Beat Check Task | 客户端心跳超时检测定时任务，负责检测过期实例并自动清理 | 2.14 | "心跳检测任务" |
| 45 | TcpSuperSenseProcessor | TCP Super Sense Processor | TCP Socket 连接检测处理器，通过 TCP 连接方式探测实例健康状态 | 2.15 | "TCP 检测" |
| 46 | ProtectManager | Protection Manager | 防雪崩保护管理器，在健康实例比例低于阈值时开启保护模式，返回缓存快照 | 2.16 | "保护管理器" |

## 八、配置管理

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 47 | Listener | Configuration Listener | 配置监听器，客户端注册的配置变更回调接口，用于接收服务端推送的配置变更 | 3.8 | "监听器" |
| 48 | LongPolling | Long Polling | 长轮询机制，客户端请求配置检查时阻塞等待，有变更时立即返回 | 3.7 | "长轮训" |
| 49 | ConfigCache | Configuration Cache | 配置缓存，客户端本地缓存配置内容，降低对服务端的请求频率 | — | "配置缓存" |
| 50 | GrayRelease | Gray Release | 灰度发布，配置变更可指定目标 IP 范围逐步发布，降低配置变更风险 | 3.9. | "灰度" |
| 51 | Beta 发布 | Beta Release | 实验性配置发布，配置变更仅对特定 Beta IP 列表生效，用于功能验证 | — | "Beta" |
| 52 | MD5 | MD5 Checksum | 配置内容 MD5 校验值，用于客户端判断配置内容是否发生变更 | 3.7 | "md5" |

## 九、持久化层（2.5.3 新增模块）

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 53 | Derby | Apache Derby | 嵌入式 Java 关系数据库，Nacos 内置单机模式的默认存储后端 | 6.2 | "derby" |
| 54 | External MySQL | External MySQL | 外部 MySQL 数据库，Nacos 生产集群模式的推荐存储后端，支持主从复制和高可用 | 6.iat | "MySQL" |
| 55 | DynamicDataSource | Dynamic Data Source | 动态数据源，根据配置条件动态切换 Derby/MySQL 数据源实现 | 6.4 | "动态数据源" |
| 56 | DataSourceService | Data Source Service | 数据源服务接口，抽象了 Derby 和 MySQL 的数据源初始化、健康检查、关闭等生命周期管理 | 6.3 | "数据源服务" |
| 57 | StorageConfiguration | Storage Configuration | 存储配置条件注解，用于条件化加载嵌入式或外部存储实现 | 6.1 | "存储配置" |

## 十、集群管理

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 58 | ClusterMemberManager | Cluster Member Manager | 集群成员管理器，管理集群节点的加入、退出、状态变更 | 5.2 | "集群成员管理" |
| 59 | ServerAddressFinder | Server Address Finder | 服务端地址寻址器，负责节点地址发现和拓扑感知 | 5.3 | "地址寻址" |
| 60 | MemberMetadata | Member Metadata | 集群成员元数据，包含节点权重、扩展信息、能力标识等元数据 | 5.4 | "节点元数据" |

## 十一、事件驱动

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 61 | NotifyCenter | Notify Center | 事件通知中心，Nacos 内部事件的发布/订阅机制核心组件 | 1.13 | "通知中心" |
| 62 | EventPublisher | Event Publisher | 事件发布器接口，负责将事件发布到对应的 Subscriber | 1.13 | "事件发布者" |
| 63 | Subscriber | Event Subscriber | 事件订阅器接口，接收并处理特定类型的 Event | 1.13 | "事件订阅者" |
| 64 | Event | Event | 事件基类，所有 Nacos 内部事件的父类 | 1.13 | — |

## 十二、安全机制

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 65 | JWT (JSON Web Token) | JSON Web Token | 基于 JSON 的 Token 认证机制，Nacos 默认认证方式，包含 Header.Payload.Signature 三部分 | 9.3 | "jwt" |
| 66 | AK-SK | Access Key-Secret Key | 阿里云凭证认证方式，使用 Access Key ID 和 Secret Access Key 对请求进行签名 | 9.4 | "AKSK" |
| 67 | RBAC | Role-Based Access Control | 基于角色的访问控制模型，支持自定义角色和权限绑定 | 9.5 | "角色权限" |
| 68 | Tantor | Tantor Authentication Plugin | Nacos 内置认证插件体系，支持多种认证方式的可插拔机制 | 9.2 | — |
| 69 | Permission | Permission | 权限实体，关联 Role 和 Resource，定义可执行的操作 | 9.5 | "权限" |

## 十三、SPI 扩展

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 70 | SPI (Service Provider Interface) | Service Provider Interface | Java SPI 服务提供者接口机制，Nacos 所有插件扩展基于此机制实现 | 8.打2 | "SPI" |
| 71 | AuthPluginService | Auth Plugin Service | 鉴权插件 SPI 接口，用于实现自定义认证方式 | 8.3 | "鉴权插件" |
| 72 | DataSourcePluginService | Data Source Plugin Service | 数据源插件 SPI 接口，用于接入外部数据库（如 PolarDB、OceanBase） | 8.4 | "数据源插件" |
| 73 | ConfigEncryptionPluginService | Configuration Encryption Plugin Service | 配置加密插件 SPI 接口，用于敏感配置的加密/解密处理 | 8.5 | "加密插件" |
| 74 | TrackerPluginService | Tracker Plugin Service | 链路追踪插件 SPI 接口，用于集成外部分布式追踪系统 | — | "追踪插件" |

## 十四、性能与调优

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 75 | JMH (Java Microbenchmark Harness) | Java Microbenchmark Harness | OpenJDK 微基准测试框架，用于 Nacos 内部性能分析和优化验证 | — | "JMH" |
| 76 | FlowControl | Flow Control | 流量控制机制，Nacos 中用于限制客户端请求速率，防止过载 | — | "限流" |
| 77 | TPS (Transactions Per Second) | Transactions Per Second | 每秒事务处理量，衡量 Nacos 服务端处理能力的核心性能指标 | — | "TPS" |
| 78 | Off-Heap Memory | Off-Heap Memory | JVM 堆外内存，Nacos 使用堆外内存减少 GC 停顿对服务的影响 | — | "堆外内存" |
| 79 | G1GC | G1 Garbage Collector | G1 垃圾回收器，Nacos 生产环境推荐的 JVM GC 策略 | — | "G1" |

## 十五、部署架构

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 80 | K8s (Kubernetes) | Kubernetes | 容器编排平台，Nacos 生产环境推荐的容器化部署方案 | — | "k8s" |
| 81 | Multi-DC | Multi-Data Center | 多数据中心部署架构，Nacos 支持异地多活和就近访问 | — | "多数据中心" |
| 82 | StatefulSet | StatefulSet | K8s 有状态应用控制器，用于 Nacos 集群在 K8s 中的持久化部署 | — | "有状态集合" |
| 83 | Helm Chart | Helm Chart | K8s 包管理工具，Nacos 提供了官方 Helm Chart 用于快速部署 | — | "Helm" |

## 十六、客户端

| # | 中文术语 | 英文全称 | 定义 | 首次出现 | 禁止替代词 |
|---|---------|---------|------|---------|-----------|
| 84 | ConfigService | Configuration Service | Nacos 客户端配置服务接口，提供 getConfig / publishConfig / addListener / removeListener 等配置操作 | 7.2 | "配置服务" |
| 85 | NamingService | Naming Service | Nacos 客户端注册中心服务接口，提供 registerInstance / deregisterInstance / subscribe / unsubscribe 等服务发现操作 | 7.3 | "命名服务" |
| 86 | NacosConfigService | Nacos Config Service | ConfigService 接口的核心实现类，负责与 Nacos 服务端交互执行配置操作 | 7.2 | — |
| 87 | NacosNamingService | Nacos Naming Service | NamingService 接口的核心实现类，负责与 Nacos 服务端交互执行服务发现操作 | 7.3 | — |
| 88 | ClientWorker | Client Worker | 客户端工作线程，负责长轮询配置变更检查和长连接维护 | 7.4 | "客户端工作线程" |
| 89 | ServerListManager | Server List Manager | 服务端列表管理器，维护 Nacos 服务端地址列表和动态更新 | 7.5 | "服务列表管理" |
| 90 | ServerRequestHandler | Server Request Handler | 服务端请求处理器，处理客户端 gRPC Bi-directional Stream 请求 | 7.6 | "请求处理器" |
| 91 | ConfigFilterChainManager | Config Filter Chain Manager | 配置过滤器链管理器，管理客户端请求/响应的过滤器链 | 7.7 | "过滤器链" |

---

## 附录：术语缩写索引

| 缩写 | 全称 | 中文 |
|------|------|------|
| AP | Available-Partition | 可用优先 |
| CP | Consistent-Partition | 一致优先 |
| gRPC | gRPC Remote Procedure Call | gRPC 远程过程调用 |
| SPI | Service Provider Interface | 服务提供者接口 |
| JWT | JSON Web Token | JSON Web 令牌 |
| AK-SK | Access Key-Secret Key | 访问密钥对 |
| RBAC | Role-Based Access Control | 基于角色的访问控制 |
| DNS-F | DNS for Service Discovery | DNS 服务发现 |
| MD5 | Message Digest Algorithm 5 | MD5 摘要算法 |
| JMH | Java Microbenchmark Harness | Java 微基准测试 |
| TPS | Transactions Per Second | 每秒事务量 |
| G1GC | G1 Garbage Collector | G1 垃圾回收器 |
| K8s | Kubernetes | Kubernetes 容器编排平台 |

---

> **总术语数**：91 个，覆盖 16 个类别
