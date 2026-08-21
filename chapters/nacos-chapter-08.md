# 第8章：Nacos 2.5.3 全量配置项详解

## 8.1 Spring Boot 基础配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `server.port` | `8848` | Nacos 主 HTTP 端口 |
| `server.servlet.contextPath` | `/nacos` | Web 上下文路径 |
| `nacos.inetutils.ip-address` | 自动检测 | 指定 Nacos Server IP |
| `nacos.inetutils.prefer-hostname-over-ip` | `false` | 是否优先使用主机名 |

## 8.2 网络相关配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.server.ip` | 自动检测 | Nacos Server 本机 IP |
| `nacos.remote.client.connect.timeout` | 3000 | 客户端连接超时（ms） |
| `nacos.remote.server.grpc.sdk.max-inbound-message-size` | 10485760 | gRPC SDK Server 最大入站消息大小（10MB） |
| `nacos.remote.server.grpc.cluster.max-inbound-message-size` | 10485760 | gRPC Cluster Server 最大入站消息大小 |

## 8.3 Config 模块——数据源配置

**★ 2.5.3 变更**：数据源配置项已从 Config 模块迁移到 `persistence` 模块统一管理：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spring.datasource.platform` | `derby` | 数据源平台（derby/mysql） |
| `db.num` | `1` | MySQL 数据源数量 |
| `db.url.0` | — | MySQL JDBC URL（★ 2.5.3: 驱动类变为 `com.mysql.cj.jdbc.Driver`） |
| `db.user.0` | — | MySQL 用户名 |
| `db.password.0` | — | MySQL 密码 |
| `db.pool.config.connectionTimeoutMs` | 30000 | HikariCP 连接超时（ms） |
| `db.pool.config.idleTimeoutMs` | 600000 | HikariCP 空闲超时（ms） |
| `db.pool.config.maxLifetimeMs` | 1800000 | HikariCP 最大生命周期（ms） |
| `db.pool.config.maxPoolSize` | 20 | HikariCP 最大连接池大小 |
| `db.pool.config.minimumIdle` | 2 | HikariCP 最小空闲连接数 |

### ★ MySQL Connector 包名变更

| 版本 | Maven 依赖 | 驱动类 |
|------|-----------|--------|
| 2.2.3 | `mysql-connector-java` (8.0.28) | `com.mysql.cj.jdbc.Driver` |
| **2.5.3** | **`mysql-connector-j` (8.2.0)** | `com.mysql.cj.jdbc.Driver` |

## 8.4 Config 模块——持久化配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.config.history.max.size` | 128 | 配置历史最大保留数量 |
| `nacos.config.max.content.size` | 10240 (10KB) | 配置内容最大尺寸（字节） |
| `nacos.config.max.config.count` | 10000 | 最大配置数量 |

## 8.5 Config 模块——长轮询配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.config.longpolling.timeout` | 29500 (29.5秒) | 长轮询超时（ms） |
| `nacos.config.longpolling.thread.core` | 4 | 长轮询核心线程数 |
| `nacos.config.longpolling.thread.max` | 32 | 长轮询最大线程数 |
| `nacos.config.longpolling.batch.size` | 20 | 批量处理长轮询请求数 |

## 8.6 Config 模块——配置加密

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.config.encrypt.enabled` | `false` | 是否启用配置加密 |
| `nacos.config.encrypt.data-key` | — | AES 加密密钥 |

## 8.7 Config 模块——Dump 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.config.dump.enabled` | `true` | 是否启用配置落盘 |
| `nacos.config.dump.interval` | 3600 | Dump 间隔（秒） |
| `nacos.config.dump.dir` | `${nacos.home}/data/config/` | Dump 目录 |

## 8.8 Config 模块——性能配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.config.query.timeout` | 5 | 查询超时（秒） |
| `nacos.config.cache.enabled` | `true` | **★ 2.5.3 新增：启用 ConfigCache 缓存** |
| `nacos.config.cache.size` | 10000 | **★ 2.5.3 新增：ConfigCache 最大条目数** |

## 8.9 Naming 模块——健康检查配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.naming.healthcheck.heartbeat.timeout` | 15000 | 心跳超时（毫秒） |
| `nacos.naming.healthcheck.heartbeat.interval` | 5000 | 心跳间隔（毫秒） |
| `nacos.naming.healthcheck.expire.time` | 30000 | 实例过期时间（毫秒） |

## 8.10 Naming 模块——防雪崩保护配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.naming.protect.enabled` | `false` | 是否启用防雪崩保护 |
| `nacos.naming.protect.threshold` | `0.5` | 健康实例比例阈值 |

## 8.11 Naming 模块——Distro 协议配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.naming.distro.sync.delay` | 1000 | Distro 同步延迟（毫秒） |
| `nacos.naming.distro.sync.retry.delay` | 3000 | Distro 同步重试延迟（毫秒） |
| `nacos.naming.distro.verify.interval` | 5000 | Distro 校验间隔（毫秒） |

## 8.12 Naming 模块——元数据配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.naming.metadata.max.size` | 1024 | 服务元数据最大 Size（字节） |
| `nacos.naming.instance.metadata.max.size` | 1024 | 实例元数据最大 Size（字节） |

## 8.13 Naming 模块——注册表配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.naming.max.service.count` | 100000 | 最大服务数量 |
| `nacos.naming.max.instance.count` | 1000000 | 最大实例数量 |

## 8.14 Core 模块——集群管理配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.core.member.lookup.type` | `file` | 集群寻址方式（file/address-server/standalone） |
| `nacos.core.member.lookup.address-server` | — | 地址服务器 URL |
| `nacos.core.member.fail.timeout` | 5000 | 成员故障超时（毫秒） |

## 8.15 Core 模块——gRPC 通信配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.core.grpc.port.offset` | `1000` | gRPC 端口偏移（SDK Server = 8848+1000=9848） |
| `nacos.remote.server.grpc.sdk.thread-pool.core-size` | 64 | gRPC SDK Server 核心线程数 |
| `nacos.remote.server.grpc.sdk.thread-pool.max-size` | 128 | gRPC SDK Server 最大线程数 |

## 8.16 Core 模块——连接管理配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.remote.server.max.connection` | 30000 | 最大连接数 |
| `nacos.remote.server.idle.timeout` | 30000 | 空闲超时（毫秒） |
| `nacos.remote.push.thread.count` | 8 | 推送线程数 |
| `nacos.remote.push.queue.capacity` | 4096 | 推送队列容量 |

## 8.17 鉴权安全配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.core.auth.enabled` | `false` | 是否启用认证 |
| `nacos.core.auth.system.type` | `nacos` | 认证系统类型 |
| `nacos.core.auth.token.secret.key` | 无默认值 | JWT Token 签名密钥（生产必须配置） |
| `nacos.core.auth.token.expire.seconds` | 18000 | Token 过期时间（秒） |

## 8.18 Istio 集成配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.istio.enabled` | `false` | 是否启用 Istio 集成 |
| `nacos.istio.mcp.server.addr` | — | MCP Server 地址 |
| `nacos.istio.domain.suffix` | `svc.cluster.local` | 服务域名后缀 |

## 8.19 监控与 Metrics 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.metrics.prometheus.enabled` | `false` | 是否启用 Prometheus |
| `nacos.access.log.enabled` | `true` | 是否启用访问日志 |
| `nacos.slow.sql.ms` | 1000 | 慢 SQL 阈值（毫秒） |

## 8.20 日志配置

### ★ 2.5.3 日志系统变更

| 变更点 | 2.2.3 | 2.5.3 |
|--------|-------|-------|
| Logback 实现 | 嵌入 `common` 模块 | **独立 `logger-adapter-impl/logback-adapter-12` 模块** |
| Log4j2 实现 | 嵌入 `client` 模块 | **独立 `logger-adapter-impl/log4j2-adapter` 模块** |
| 适配器接口 | `AbstractNacosLogging` | **`NacosLoggingAdapter`（统一接口）** |

### ★ 2.5.3 新增配置项汇总

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.config.cache.enabled` | `true` | ConfigCache 缓存启用开关 |
| `nacos.config.cache.size` | 10000 | ConfigCache 最大条目数 |
| `nacos.persistence.datasource.metrics.enabled` | `true` | persistence 数据源 Metrics 启用 |
| `nacos.module.health.check.enabled` | `true` | 模块健康检查启用 |
| `nacos.ability.server.enabled` | `true` | Server Ability 系统启用 |
| `nacos.param.check.enabled` | `true` | ParamChecker 参数校验启用 |

---

> **本章基于 Nacos 2.5.3 全量配置项梳理生成。**
