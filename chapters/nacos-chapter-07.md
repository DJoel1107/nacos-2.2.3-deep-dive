# 第 7 章：客户端 SDK 源码深度分析

> **基于源码**: Nacos 2.5.3 (`client/src/main/java/com/alibaba/nacos/client/`)  
> **模块规模**: 136 个 Java 文件  
> **核心入口**: `NacosConfigService`（配置拉取）、`NacosNamingService`（服务发现）

---

### 7.1 客户端 SDK 整体架构

### 7.1.1 设计背景

Nacos 2.5.3 的 `client/` 模块（136 个 Java 文件）提供两大核心 SDK 能力：**配置管理**（`NacosConfigService`）和**服务发现**（`NacosNamingService`）。客户端 SDK 封装了与 Nacos Server 之间的网络通信、故障转移、长轮询、服务缓存等复杂逻辑——业务开发者仅需通过简单的 API 调用即可实现配置实时更新和服务注册/发现——无需了解底层 gRPC 双向流推送、配置快照 MD5 校验、服务实例心跳上报等实现细节。

`client/` 模块的核心架构分为三层：

1. **接口层（API Layer）**：`ConfigService` / `NamingService` 接口定义——对业务开发者暴露的公共 API——`getConfig()`/`publishConfig()`（配置管理）、`registerInstance()`/`getAllInstances()`（服务发现）。
2. **核心实现层（Core Impl Layer）**：`ClientWorker`（配置长轮询线程）、`NacosNamingService`（服务发现实现）、`NamingClientProxy`（gRPC/HTTP 通信代理）——封装网络通信、心跳上报、故障转移。
3. **通信层（Communication Layer）**：`RpcClient`（gRPC 双向流推送）、`HttpSimpleClient`（HTTP 短连接请求）——底层网络通信适配——支持 gRPC + HTTP 双协议自动切换。

Nacos 2.5.3 的客户端 SDK 相比 2.2.x 的重大变更：

1. **gRPC 双向流推送替代 HTTP 长轮询**：配置变更通知和服务实例变更通知全部通过 gRPC 双向流推送实时推送——不再需要 `ClientWorker` 线程定期轮询 `/v1/cs/configs/listener` HTTP 接口——减少了轮询延迟和服务器负载。
2. **服务缓存快照机制**：`ServiceInfoHolder` 本地缓存全量服务实例信息——避免每次服务发现都请求 Nacos Server——减少网络开销。
3. **配置快照本地兜底**：`LocalConfigInfoProcessor` 将配置内容以文件形式缓存到本地磁盘——当 Nacos Server 不可用时——客户端仍可读取本地缓存的配置快照——保证业务服务的可用性。

