# Nacos 2.2.3 → 2.5.3 版本差异分析报告

> **生成日期**：2026-08-18  
> **基线版本**：Nacos 2.2.3  
> **目标版本**：Nacos 2.5.3  
> **分析范围**：模块结构、Java 文件数量、核心类路径变更、POM 依赖版本、新增模块

---

## 一、版本号变更

| 项目 | 2.2.3 | 2.5.3 |
|------|-------|-------|
| `<revision>` | 2.2.3 | 2.5.3 |
| Spring Boot | 2.6.14 | 2.7.18 |
| gRPC | 1.50.2 | 1.75.0 |
| Protobuf | 3.21.11 | 3.25.5 |
| JRaft | 1.3.12 | 1.3.14 |
| Jackson | 2.12.x（独立版本） | 2.18.9（统一 BOM） |
| MySQL Connector | 8.0.28 (mysql-connector-java) | 8.2.0 (mysql-connector-j) |

---

## 二、模块结构变化

### 2.1 新增模块

#### 2.1.1 `persistence/` - 持久化层独立模块（新增）

**定位**：将原来分散在 `config` 和 `core` 模块中的数据源管理、持久化逻辑抽离为独立模块。

**包含 37 个 Java 文件**，主要包结构：

| 包路径 | 说明 | 文件数 |
|--------|------|--------|
| `persistence/configuration/` | 数据源配置 + 存储条件注解 | 括6 |
| `persistence/datasource/` | 数据源服务接口 + 动态数据源实现 | 5 |
| `persistence/model/` | 分页模型 + Derby 事件 |3 |
| `persistence/monitor/` | 数据源 Metrics 监控 | 1 |
| `persistence/repository/embedded/` | 嵌入式 Derby 持久化实现 | 8 |
| `persistence/repository/extrnal/` | 外部 MySQL 持久化实现 | 1 |
| `persistence/utils/` | 连接检查 + Derby 工具类 | 4 |

**核心类**：
- `DatasourceConfiguration` - 数据源配置自动装配
- `DynamicDataSource` - 动态数据源路由
- `DataSourceService` - 数据源服务统一接口
- `ExternalDataSourceServiceImpl` - MySQL 数据源实现
- `LocalDataSourceServiceImpl` - Derby 嵌入式数据源实现
- `PaginationHelper` - 分页帮助器接口
- `EmbeddedStorageContextHolder` - 嵌入式存储上下文持有者

**依赖关系**：
```
persistence
  ├── spring-boot-starter-jdbc
  ├── nacos-datasource-plugin（插件 SPI）
  ├── nacos-sys
  └── nacos-consistency
```

#### 2.1.2 `logger-adapter-impl/` - 日志适配器实现模块（新增）

**定位**：将日志框架适配器从 `common` 模块抽离，支持 Log4j2 和 Logback 两种实现的独立打包。

**子模块结构**：

| 子模块 | 说明 | Java 文件 |
|--------|------|-----------|
| `log4j2-adapter/` | Log4j2 日志适配器实现 | 5 个 Main + 3 个 Test |
| `logback-adapter-12/` | Logback 1.2.x 适配器实现 | 4 个 Main + 4 个 Test |

**核心类**：
- `Log4J2NacosLoggingAdapter` - Log4j2 适配器主类
- `Log4J2NacosLoggingAdapterBuilder` - Log4j2 适配器构建器
- `LogbackNacosLoggingAdapter` - Logback 适配器主类
- `LogbackNacosLoggingAdapterBuilder` - Logback 适配器构建

### 2.2 模块结构重组

#### `plugin-default-impl/` 目录重组

2.2.3 中 `plugin-default-impl/` 是扁平结构（47 个 Java 文件），2.5.3 中拆分为子模块结构：

```
plugin-default-impl/
  ├── nacos-default-plugin-all/          (聚合模块)
  ├── nacos-default-auth-plugin/         (默认认证插件)
  └── nacos-default-control-plugin/      (默认控制插件)
```

---

## 三、Java 文件数量统计

### 3.1 主代码文件数（不含测试）

| 模块 | 2.2.3 | 2.13 2.5.3 | 变化 |
|------|-------|-------|------|
| address | 8 | 8 | 0 |
| api | 155 | 171 | +16 |
| auth | 22 | 27 | +5 |
| client | 117 | 136 | +19 |
| cmdb | 9 | 9 | 0 |
| common | 189 | 210 | +21 |
| config | 203 | 217 | +14 |
| consistency | 23 | 23 | 0 |
| console | 14 | 12 | -2 |
| core | 168 | 230 | +62 |
| istio | 29 | 30 | +1 |
| naming | 245 | 247 | +2 |
| **persistence** | **0** | **37** | **+37** |
| plugin-default-impl | 47 | ~47 | 0 |
| prometheus | 6 | 7 | +1 |
| sys | 16 | 19 | +3 |
| **总计** | **1,353** | **1,578** | **+225** |

### 3.2 测试文件数

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| 测试 Java 文件 | 572 | 882 | +310 |
| 主代码行数 | ~163,957 | ~177,847 | +13,890 |

---

## 四、核心类路径/名称变更

### 4.1 Config 模块

| 2.2.3 | 2.5.3 | 变更说明 |
|--------|--------|---------|
| `ConditionDistributedEmbedStorage` | → 移至 `persistence` 模块 | 存储条件注解移入持久化模块 |
| `ConditionOnEmbeddedStorage` | → 移至 `persistence` 模块 | 同上 |
| `ConditionOnExternalStorage` | → 移至 `persistence` 模块 | 同上 |
| `ConditionStandaloneEmbedStorage` | → 移至 `persistence` 模块 | 同上 |
| `NJdbcException` | → 移至 `persistence` 模块 | JDBC 异常类移入持久化模块 |
| — | `ConfigChangeAspect` | **新增**：配置变更切面 |
| — | `ConfigChangeConfigs` | **新增**：配置变更配置类 |
| — | `ConfigCache` / `ConfigCacheFactory` | **新增**：配置缓存机制 |
| — | `ConfigInfoGrayWrapper` | **新增**：灰度配置包装器 |
| — | `ApiVersionEnum` | **新增**：API 版本枚举 |
| — | `ConfigEnabledFilter` | **新增**：配置启用过滤器 |
| — | `OperationType` | **新增**：操作类型枚举 |

### 4.2 Naming 模块

| 2.2.3 | 2.5.3 | 变更说明 |
|--------|--------|---------|
| `ConsistencyService` (naming.consistency) | → 已移除 | 一致性服务接口移出 naming |
| `RecordListener` | → 已移除 | 记录监听器移除 |
| `PersistentConsistencyService` | → 已移除 | 持久化一致性服务移除 |
| `PersistentServiceProcessor` | → 已移除 | 持久化服务处理器移除 |
| `StandalonePersistentServiceProcessor` | → 已移除 | 单机持久化处理器移除 |
| `NamingKvStorage` | → 已移除 | KV 存储移除 |
| — | `NamingReadinessCheckService` | **新增**：命名模块就绪检查 |
| — | `NamingEnabledFilter` | **新增**：命名启用过滤器 |
| — | `OldDataOperation` | **新增**：旧数据操作 |
| — | `ServiceTopNCounter` | **新增**：服务 TopN 计数器 |
| — | `InstanceIdGeneratorManager` | **新增**：实例 ID 生成器管理器 |
| — | `SnowFlakeInstanceIdGenerator` | **新增**：雪花 ID 生成器 |
| — | `NamingDefaultHttpParamExtractor` | **新增**：HTTP 参数提取器 |
| — | `PersistentInstanceRequestHandler` | **新增**：持久化实例请求处理器 |
| — | `NamingRequestUtil` | **新增**：命名请求工具 |

### 4.3 Core 模块（变更最大）

| 2.2.3 | 2.5.3 | 变更说明 |
|--------|--------|---------|
| — | `RequestContext` / `RequestContextHolder` | **新增**：请求上下文机制 |
| — | `AddressContext` / `AuthContext` / `BasicContext` / `EngineContext` | **新增**：上下文附加信息 |
| — | `HttpRequestContextConfig` / `HttpRequestContextFilter` | **新增**：HTTP 请求上下文配置/过滤器 |
| — | `AbstractModuleHealthChecker` | **新增**：模块健康检查抽象 |
| — | `ModuleHealthCheckerHolder` | **新增**：模块健康检查持有者 |
| — | `ReadinessResult` | **新增**：就绪检查结果 |
| — | `DistroModuleStateBuilder` / `RaftModuleStateBuilder` | **新增**：模块状态构建器 |
| — | `ServerAbilityControlManager` | **新增**：服务能力控制管理器 |
| — | `Namespace` / `TenantInfo` / `NamespaceForm` | **新增**：命名空间模型 |
| — | `NamespacePersistService` | **新增**：命名空间持久化服务 |
| — | `ParamCheckerFilter` / `ExtractorManager` | **新增**：参数校验过滤器 |
| `SnakflowerException` | → 已移除 | 重命名/移除 |
| — | `TopNConfig` / `BaseTopNCounter` | **新增**：TopN 计数器基础设施 |
| — | `GrpcServerThreadPoolMonitor` | **新增**：gRPC 线程池监控 |
| `NacosHttpTpsControlInterceptor` | `NacosHttpTpsFilter` | 类名变更 + 重构 |

### 4.4 Common 模块

| 2.2.3 | 2.5.3 | 变更说明 |
|--------|--------|---------|
| `RequestHandler` / `ResponseHandler` | → 已移除 | HTTP 处理接口移除 |
| `NacosLogbackConfigurator` | → 移至 `logger-adapter-impl` | 日志配置器移入独立模块 |
| `NacosLogbackProperties` | → `NacosLoggingProperties` | 类名变更 |
| `AbstractAssert` | → 移至 `packagescan/util` | 包路径调整 |
| `TopnCounterMetricsContainer` | → 已移除 | TopN 计数器移除 |
| — | `AbstractAbilityControlManager` | **新增**：抽象能力控制管理器 |
| — | `NacosAbilityManagerHolder` | **新增**：能力管理器持有者 |
| — | `LabelsCollector` / `LabelsCollectorManager` | **新增**：标签收集器 |
| — | `NacosLoggingAdapter` | **新增**：日志适配器统一接口 |
| — | `AbstractParamChecker` / `DefaultParamChecker` | **新增**：参数校验器 |
| — | `PathEncoder` / `PathEncoderManager` | **新增**：路径编码器 |
| — | `ConnLabelsUtils` | **新增**：连接标签工具类 |
| — | `RpcClientConfigFactory` / `RpcTlsConfigFactory` | **新增**：RPC 客户端配置工厂 |

### 4.5 API 模块

| 2.2.3 | 2.5.3 | 变更说明 |
|--------|--------|---------|
| `IdGenerator` | → `InstanceIdGenerator` | 接口重命名，语义更精确 |
| — | `AbilityKey` / `AbilityMode` / `AbilityStatus` | **新增**：能力常量定义 |
| — | `AbstractAbilityRegistry` | **新增**：抽象能力注册器 |
| — | `ClusterClientAbilities` | **新增**：集群客户端能力 |
| — | `SdkClientAbilities` | **新增**：SDK 客户端能力 |
| — | `ServerAbilities` | **新增**：服务端能力 |
| — | `PersistentInstanceRequest` | **新增**：持久化实例请求 |
| — | `NamingSelector` / `NamingContext` / `NamingResult` | **新增**：命名选择器接口 |
| — | `SetupAckRequest` / `SetupAckResponse` | **新增**：连接建立确认请求/响应 |
| — | `Selector` / `SelectResult` | **新增**：客户端选择器接口 |

### 4.6 Client 模块

| 2.2.3 | 2.5.3 | 变更说明 |
|--------|--------|---------|
| `ServerListManager` | → 移除 | 服务端列表管理器重构 |
| `ServerlistChangeEvent` | → `ServerListChangeEvent` | 驼峰命名修正 |
| `ConcurrentDiskUtil` | → 移除 | 磁盘工具类移除 |
| `AbstractNacosLogging` | → 移除（移至 logger-adapter-impl） | 日志抽象移入独立模块 |
| — | `ClientAbilityControlManager` | **新增**：客户端能力控制管理器 |
| — | `AbstractServerListManager` | **新增**：抽象服务端列表管理器 |
| — | `AbstractServerListProvider` | **新增**：抽象服务端列表提供者 |
| — | `ConfigServerListManager` | **新增**：配置服务端列表管理器 |
| — | `NamingServerListManager` | **新增**：命名服务端列表管理器 |
| — | `FailoverData` / `FailoverDataSource` / `FailoverSwitch` | **新增**：故障转移机制 |
| — | `NamingFailoverData` | **新增**：命名故障转移数据 |
| — | `InstancesDiffer` | **新增**：实例差异计算器 |
| — | `RamUtil` / `CalculateV4SigningKeyUtil` | **新增**：RAM 认证工具类 |

---

## 五、关键架构变更总结

### 5.1 持久化层独立（persistence 模块）

2.2.3 中，数据源配置和持久化逻辑分散在 `config` 和 `core` 模块中。2.5.3 将这些逻辑抽离为独立的 `persistence` 模块，实现了：
- 统一的数据源管理（`DynamicDataSource`）
- 嵌入式 Derby 和外部 MySQL 的统一抽象
- 独立的分页查询帮助器
- 数据源 Metrics 监控

### 5.2 日志适配器独立（logger-adapter-impl 模块）

2.2.3 中，日志适配器代码（`Log4J2NacosLogging`、`LogbackNacosLogging`）位于 `common` 和 `client` 模块中。2.5.3 抽离为独立模块，支持 Log4j2 和 Logback 两种实现的独立打包。

### 5.3 能力协商增强（Ability System）

2.5.3 引入了完整的能力协商系统：
- `AbstractAbilityControlManager` - 能力控制管理器
- `ServerAbilityControlManager` - 服务端能力管理器
- `ClientAbilityControlManager` - 客户端能力管理器
- API 层新增 `AbilityKey` / `AbilityMode` / `AbilityStatus` 常量

### 5.4 参数校验系统（ParamChecker）

2.5.3 新增了统一的参数校验框架：
- `AbstractParamChecker` - 抽象参数校验器
- `DefaultParamChecker` - 默认参数校验器
- `ParamCheckerFilter` - 参数校验过滤器
- `ParamCheckRule` / `ParamCheckResponse` - 校验规则/响应

### 5.5 命名空间模型独立

2.5.3 在 `core` 模块中新增独立的命名空间管理：
- `Namespace` / `TenantInfo` / `NamespaceForm` - 命名空间模型
- `NamespacePersistService` - 命名空间持久化服务
- `EmbeddedNamespacePersistServiceImpl` - 嵌入式实现
- `ExternalNamespacePersistServiceImpl` - 外部 MySQL 实现

### 5.6 健康检查增强

- `AbstractModuleHealthChecker` - 模块级健康检查抽象
- `ModuleHealthCheckerHolder` - 健康检查持有者
- `ReadinessResult` - 就绪检查结果
- `NamingReadinessCheckService` - Naming 模块就绪检查

### 5.7 客户端故障转移机制

2.5.3 客户端新增完整的故障转移机制：
- `FailoverData` / `FailoverDataSource` / `FailoverSwitch` - 故障转移核心
- `DiskFailoverDataSource` - 磁盘故障转移数据源
- `NamingFailoverData` - 命名故障转移数据

### 5.8 配置缓存机制

2.5.3 新增配置缓存优化：
- `ConfigCache` / `ConfigCacheFactory` - 配置缓存
- `ConfigCacheGray` - 灰度缓存
- `ConfigCachePostProcessor` - 缓存后处理器

---

## 六、对文档更新的影响分析

基于以上差异分析，12 章文档需要更新的关键点：

| 章节 | 需要更新的内容 |
|------|---------------|
| 第 1 章（架构概述） | 模块数量 20→22（新增 persistence、logger-adapter-impl）；四层架构中「存储层」描述需新增 persistence 独立层 |
| 第 2 章（注册中心） | Naming 模块移除了 PersistentConsistencyService 等类；新增 InstanceIdGenerator、NamingRequestUtil |
| 第 3 章（配置中心） | Config 模块存储条件注解移至 persistence 模块；新增 ConfigCache、ConfigChangeAspect |
| 第 4 章（一致性协议） | JRaft 版本 1.3.12→1.3.14；一致性相关类路径变更 |
| 第 5 章（集群管理 + 客户端） | Core 模块新增命名空间管理；Client 模块 ServerListManager 重构 |
| 第 6 章（插件体系） | plugin-default-impl 目录结构重组为多子模块 |
| 第 7 章（认证安全 + 控制台） | 新增 ability 系统对认证的控制影响 |
| 第 8 章（全量配置） | 新增 persistence 模块相关配置项 |
| 第 9 章（部署架构） | MySQL 驱动包名从 mysql-connector-java 变为 mysql-connector-j |
| 第 10 章（高可用） | JRaft 版本升级影响 Leader 选举参数 |
| 第 11 章（性能调优） | gRPC 版本升级（1.50.2→1.75.0）影响通信性能参数 |
| 第 12 章（监控运维） | 新增模块的健康检查 Metrics；Prometheus 指标增加模块级健康状态 |

---

> **本文档基于 Nacos 2.2.3 与 2.5.3 源码实际对比生成，所有统计数据和类路径信息均通过自动化脚本提取。**
