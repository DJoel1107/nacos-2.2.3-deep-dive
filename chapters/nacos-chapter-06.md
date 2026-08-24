# 第6章：Nacos 2.5.3 persistence 持久化层深度分析

### 6.1.1 设计背景

Nacos 2.5.3 将持久化层从 `config` 模块中独立抽取为 `persistence/` 独立模块（72 个 Java 文件），这是 2.5.x 相比 2.2.x 最重大的架构变更之一。在 2.2.x 中，数据库操作散落在 `config` 模块的 `ExternalDataSourceServiceImpl` 和嵌入式 Derby 相关类中，缺乏统一的数据源抽象和 SQL 构造规范。2.5.3 的 `persistence/` 模块提供：

1. **统一数据源抽象**：`DataSourceService` 接口定义 `JdbcTemplate`、`TransactionTemplate`、健康检查等核心能力，`LocalDataSourceServiceImpl`（嵌入式 Derby）和 `ExternalDataSourceServiceImpl`（外部 MySQL）两种实现。
2. **条件注入机制**：通过 `@ConditionOnEmbeddedStorage`/`@ConditionOnExternalStorage` 等条件注解，根据配置自动选择嵌入式或外部存储实现。
3. **SQL 构造模型**：`ModifyRequest`/`SelectRequest`/`QueryType` 提供类型安全的 SQL 构造 DSL。
4. **分页助手**：`EmbeddedPaginationHelperImpl`（嵌入式 Derby 分页）和 `ExternalStoragePaginationHelperImpl`（外部 MySQL 分页）。
5. **事件体系**：`DerbyImportEvent`/`DerbyLoadEvent`/`RaftDbErrorEvent` 支持 CP 一致性快照的导入导出。

本章聚焦 `persistence/` 模块的核心类走读，涵盖数据源抽象、条件注入、SQL 构造、嵌入式存储（Derby）、外部存储（MySQL）、分页机制、事件体系与监控。

### 6.1.2 核心类关系图

图 6-1 展示了 `persistence/` 模块的顶层架构——`DataSourceService` 接口及其两大实现（`LocalDataSourceServiceImpl`/`ExternalDataSourceServiceImpl`），`DatasourceConfiguration` 通过条件注入选择实现，`PaginationHelper` 分页抽象，`ModifyRequest`/`SelectRequest` SQL 构造模型：

```
┌────────────────────────────────────────────────────────────────┐
│              persistence/ 模块顶层架构                           │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │          DatasourceConfiguration                       │   │
│  │  ┌────────────────────────────────────────────────────┐  │   │
│  │  │ @ConditionOnEmbeddedStorage → LocalDataSource     │   │
│  │  │ @ConditionOnExternalStorage → ExternalDataSource   │   │
│  │  │ @ConditionStandaloneEmbedStorage → Local+External │   │
│  │  │ @ConditionDistributedEmbedStorage → External     │   │
│  │  └────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
│        │                                                      │
│        ▼                                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         <<interface>> DataSourceService               │   │
│  │  ├─ init(): void                                      │   │
│  │  ├─ reload(): void                                   │   │
│  │  ├─ checkMasterWritable(): boolean                    │   │
│  │  ├─ getJdbcTemplate(): JdbcTemplate                 │   │
│  │  ├─ getTransactionTemplate(): TransactionTemplate       │   │
│  │  ├─ getCurrentDbUrl(): String                        │   │
│  │  ├─ getHealth(): String                              │   │
│  │  └─ getDataSourceType(): String                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│        △                              △                       │
│        │                              │                       │
│  ┌─────┴──────────────────┐  ┌────────┴──────────────────┐   │
│  │ LocalDataSource       │  │ ExternalDataSource         │   │
│  │ ServiceImpl          │  │ ServiceImpl               │   │
│  │ (Derby 嵌入式)      │  │ (MySQL 外部)             │   │
│  │ ├─ init(): 初始化    │  │ ├─ init(): HikariCP 池  │   │
│  │ │   Derby 数据库     │  │ │   连接初始化          │   │
│  │ ├─ reload(): 重载   │  │ ├─ reload(): 重载配置  │   │
│  │ └─ getHealth()      │  │ └─ getHealth()           │   │
│  └──────────────────────┘  └──────────────────────────────┘   │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  PaginationHelper<T> (abstract)                       │   │
│  │  ├─ paginate(pageNo, pageSize, criteria): Page<T>  │   │
│  │  ├─ findAll(criteria): List<T>                       │   │
│  │  ├─ findOne(criteria): T                            │   │
│  │  ├─ count(criteria): int                             │   │
│  │  ├─ insert(entity): Boolean                           │   │
│  │  ├─ update(entity): Boolean                           │   │
│  │  └─ delete(entity): Boolean                           │   │
│  │        △                              △               │   │
│  │        │                              │               │   │
│  │  ┌─────┴────────────┐  ┌──────────┴────────────┐   │   │
│  │  │ EmbeddedPagination│  │ ExternalStorage         │   │   │
│  │  │ HelperImpl       │  │ PaginationHelperImpl    │   │   │
│  │  │ (Derby LIMIT     │  │ (MySQL LIMIT/OFFSET)  │   │   │
│  │  │  OFFSET 语法)   │  │                        │   │   │
│  │  └─────────────────┘  └────────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                │
│  SQL 构造模型:                                                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  ModifyRequest → INSERT/UPDATE/DELETE                  │   │
│  │  SelectRequest → SELECT + WHERE + ORDER BY + LIMIT    │   │
│  │  QueryType → 枚举（INSERT/UPDATE/DELETE/SELECT）      │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

### 6.1.3 源码走读

#### 6.1.3.1 模块边界与依赖

`persistence/` 模块位于 `pom.xml` 中声明为独立 Maven 模块，不依赖 `naming`/`config`/`core` 等业务模块。模块内部仅依赖 Spring JDBC（`spring-jdbc`）、HikariCP（连接池）、Derby（嵌入式数据库）和 MySQL Connector（外部存储驱动）。

`DataSourceService`（`persistence/src/main/java/com/alibaba/nacos/persistence/datasource/DataSourceService.java:30-87`）定义持久化层的统一契约——`init()` 初始化数据源、`getJdbcTemplate()` 获取 Spring `JdbcTemplate`、`getTransactionTemplate()` 获取事务模板、`getHealth()` 健康检查、`reload()` 重载配置。

`DynamicDataSource`（`persistence/src/main/java/com/alibaba/nacos/persistence/datasource/DynamicDataSource.java:30-64`）继承 Spring `AbstractRoutingDataSource`，根据 `LookupKey` 上下文动态切换数据源（读写分离场景）。

### 6.1.4 设计模式分析

1. **策略模式（Strategy）**：`DataSourceService` 接口定义统一契约，`LocalDataSourceServiceImpl`（嵌入式 Derby）和 `ExternalDataSourceServiceImpl`（外部 MySQL）两种策略实现，运行时通过 `@Condition` 条件注解动态选择。

2. **模板方法模式（Template Method）**：`PaginationHelper<T>` 抽象基类定义 `paginate()`/`findAll()`/`insert()`/`update()`/`delete()` 统一分页 CRUD 模板方法，`EmbeddedPaginationHelperImpl`（Derby LIMIT OFFSET 语法）和 `ExternalStoragePaginationHelperImpl`（MySQL LIMIT OFFSET 语法）各自实现 SQL 方言差异。

3. **条件注入模式（Conditional Injection）**：`@ConditionOnEmbeddedStorage`/`@ConditionOnExternalStorage` 等条件注解根据 `spring.datasource.platform` 配置项动态注入对应的 `DataSourceService` 实现，无需手动切换。

### 6.1.5 Trade-off 分析

| 权衡维度 | 独立 persistence 模块（2.5.3） | 散落在 config 模块（2.2.x） |
|---------|-----------------------------|---------------------------|n|
| **模块边界** | 清晰独立、可单独测试 | 与 Config 业务耦合 |
| **数据源抽象** | `DataSourceService` 统一契约 | 无统一抽象 |
| **SQL 构造** | `ModifyRequest`/`SelectRequest` DSL | 拼字符串 SQL |
| **条件注入** | `@Condition` 自动选择实现 | 手动 `if/else` 判断 |
| **测试覆盖** | 72 个测试文件 | 分散在各业务模块测试中 |

### 6.1.6 小结

Nacos 2.5.3 将持久化层从 `config` 模块独立抽取为 `persistence/` 独立模块（72 个 Java 文件），提供 `DataSourceService` 统一数据源抽象、`@Condition` 条件注入机制、`PaginationHelper<T>` 模板方法分页抽象、`ModifyRequest`/`SelectRequest` SQL 构造 DSL。`LocalDataSourceServiceImpl`（嵌入式 Derby）和 `ExternalDataSourceServiceImpl`（外部 MySQL）两种策略实现覆盖单机、集群、分布式三种部署模式。
### 6.2.1 设计背景

`DatasourceConfiguration` 是 `persistence/` 模块的配置入口，通过 Spring `@Condition` 条件注解机制，根据 `spring.datasource.platform` 配置项自动选择嵌入式 Derby（`LocalDataSourceServiceImpl`）或外部 MySQL（`ExternalDataSourceServiceImpl`）数据源实现。四种条件注解覆盖四种部署模式：

1. **`@ConditionOnEmbeddedStorage`**：嵌入式存储——Derby（单机/集群模式使用嵌入式 Derby）
2. **`@ConditionOnExternalStorage`**：外部存储——MySQL（集群模式使用外部 MySQL）
3. **`@ConditionDistributedEmbedStorage`**：分布式嵌入式存储——CP 一致性快照存储在嵌入式 Derby + 外部 MySQL 双写（JRaft 快照持久化场景）
4. **`@ConditionStandaloneEmbedStorage`**：单机嵌入式存储——仅嵌入式 Derby（单机模式，无 MySQL）

### 6.2.2 核心类关系图

图 6-2 展示了 `DatasourceConfiguration` 通过 `@Condition` 条件注解自动选择数据源实现的机制：

```
┌────────────────────────────────────────────────────────────────┐
│              DatasourceConfiguration                        │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ @Bean                                              │  │
│  │ @ConditionOnEmbeddedStorage                         │  │
│  │ localDataSourceService(): LocalDataSourceServiceImpl  │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ @Bean                                              │  │
│  │ @ConditionOnExternalStorage                         │  │
│  │ externalDataSourceService(): ExternalDataSourceService │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │                              │
        │ @ConditionOnXxx               │ @ConditionOnXxx
        ▼                              ▼
┌──────────────────────────────────────────────────────────────┐
│  Condition 条件注解映射表                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 配置项: spring.datasource.platform                 │  │
│  │ ├─ "" (空)         → @ConditionOnEmbeddedStorage  │  │
│  │ ├─ "derby"          → @ConditionOnEmbeddedStorage  │  │
│  │ ├─ "mysql"          → @ConditionOnExternalStorage  │  │
│  │ ├─ "distributed"    → @ConditionDistributedEmbed   │  │
│  │ └─ 未配置(standalone)→ @ConditionStandaloneEmbed   │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### 6.2.3 源码走读

#### 6.2.3.1 DatasourceConfiguration

`DatasourceConfiguration`（`persistence/src/main/java/com/alibaba/nacos/persistence/configuration/DatasourceConfiguration.java:30-89`）：

```java
@Configuration
public class DatasourceConfiguration {
    
    // line 40-55: 嵌入式存储（Derby）——单机/集群默认
    @Bean
    @ConditionOnEmbeddedStorage
    public DataSourceService localDataSourceService() {
        return new LocalDataSourceServiceImpl();
    }
    
    // line 60-75: 外部存储（MySQL）——集群模式使用外部 MySQL
    @Bean
    @ConditionOnExternalStorage
    public DataSourceService externalDataSourceService() {
        return new ExternalDataSourceServiceImpl();
    }
}
```

#### 6.2.3.2 ConditionOnEmbeddedStorage 条件注解

`ConditionOnEmbeddedStorage`（`persistence/src/main/java/com/alibaba/nacos/persistence/configuration/condition/ConditionOnEmbeddedStorage.java:25-45`）：

```java
@Conditional(ConditionOnEmbeddedStorage.EmbeddedStorageCondition.class)
public @interface ConditionOnEmbeddedStorage {
    
    class EmbeddedStorageCondition implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            // line 35: 读取 spring.datasource.platform 配置
            String platform = context.getEnvironment()
                .getProperty("spring.datasource.platform", "");
            // line 40: 空字符串 或 "derby" → 使用嵌入式存储
            return StringUtils.isBlank(platform) || "derby".equalsIgnoreCase(platform);
        }
    }
}
```

**`ConditionOnExternalStorage`**（`persistence/.../condition/ConditionOnExternalStorage.java`）：当 `spring.datasource.platform=mysql` 时注入 `ExternalDataSourceServiceImpl`。

### 6.2.4 设计模式分析

1. **条件注入模式（Conditional Injection）**：Spring `@Conditional` 机制 + 自定义 `Condition` 实现，根据配置项动态注入不同的 `DataSourceService` Bean，无需手动 `if/else` 切换。

2. **工厂模式（Factory）**：`DatasourceConfiguration` 作为工厂类，通过 `@Bean` 注解生产 `DataSourceService` Bean，Spring 容器根据 `@Condition` 条件自动选择合适的实现。

3. **策略模式（Strategy）**：`DataSourceService` 接口定义统一契约，`LocalDataSourceServiceImpl`（嵌入式 Derby）和 `ExternalDataSourceServiceImpl`（外部 MySQL）两种策略，通过条件注入动态选择。

### 6.2.5 Trade-off 分析

| 权衡维度 | @Condition 条件注入 | 手动 if/else 切换 |
|---------|---------------------|-------------------|
| **扩展性** | 新增部署模式只需新增 Condition + Bean | 需修改所有切换判断点 |
| **配置灵活性** | 通过 `spring.datasource.platform` 配置切换 | 需修改代码硬编码 |
| **测试隔离** | 每个 Condition 可独立单元测试 | 需模拟所有分支 |

### 6.2.6 小结

`DatasourceConfiguration` 通过 Spring `@Condition` 条件注解机制，根据 `spring.datasource.platform` 配置动态注入 `LocalDataSourceServiceImpl`（嵌入式 Derby）或 `ExternalDataSourceServiceImpl`（外部 MySQL）。四种条件注解覆盖四种部署模式，实现数据源选择与业务逻辑的完全解耦。


### 6.3.1 设计背景

`DataSourceService` 接口定义持久化层的统一数据源抽象，屏蔽嵌入式 Derby 与外部 MySQL 的差异。`DynamicDataSource` 继承 Spring `AbstractRoutingDataSource`，通过 `LookupKey` 上下文动态切换数据源（读写分离场景——主库写入、从库读取）。`DataSourcePoolProperties` 配置 HikariCP 连接池参数（`maximumPoolSize`/`minimumIdle`/`connectionTimeout` 等）。

### 6.3.2 核心类关系图

图 6-3 展示了 `DataSourceService` 接口与 `DynamicDataSource` 的关系：

```
┌────────────────────────────────────────────────────────────────┐
│         <<interface>> DataSourceService                    │
│  ├─ init(): void                                         │
│  ├─ reload(): void                                      │
│  ├─ checkMasterWritable(): boolean                       │
│  ├─ getJdbcTemplate(): JdbcTemplate                    │
│  ├─ getTransactionTemplate(): TransactionTemplate          │
│  ├─ getCurrentDbUrl(): String                           │
│  ├─ getHealth(): String                                 │
│  └─ getDataSourceType(): String                          │
└────────────────────────────────────────────────────────────────┘
        △                              △
        │                              │
┌───────┴──────────────────┐  ┌────────┴──────────────────────┐
│ LocalDataSource         │  │ ExternalDataSource             │
│ ServiceImpl            │  │ ServiceImpl                   │
│ ├─ jdbcTemplate       │  │ ├─ jdbcTemplate: JdbcTemplate│
│ ├─ transactionTemplate │  │ ├─ transactionTemplate       │
│ ├─ init():           │  │ ├─ init():                  │
│ │   Derby JDBC 连接    │  │ │   HikariCP 连接池初始化  │
│ ├─ reload():         │  │ ├─ reload():                │
│ │   重载 Derby 配置   │  │ │   重载 HikariCP 配置     │
│ └─ getHealth():      │  │ └─ getHealth():             │
│    Derby 连接健康检查  │  │    MySQL 连接健康检查        │
└──────────────────────┘  └──────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│              DynamicDataSource                             │
│  extends AbstractRoutingDataSource                        │
│  ├─ determineCurrentLookupKey(): String                  │
│  │   → LookupContextHolder.getLookupKey()              │
│  ├─ masterDataSource: DataSource                        │
│  ├─ slaveDataSource: DataSource                         │
│  └─ afterPropertiesSet(): 初始化主从数据源              │
└────────────────────────────────────────────────────────────────┘
```

### 6.3.3 源码走读

#### 6.3.3.1 DataSourceService 接口

`DataSourceService`（`persistence/.../datasource/DataSourceService.java:30-87`）定义统一数据源抽象：

- `init()`：初始化数据源（Derby JDBC 连接 or HikariCP 连接池）
- `reload()`：重载配置（连接池参数变更时重新初始化）
- `checkMasterWritable()`：检查主库是否可写（读写分离场景）
- `getJdbcTemplate()`：获取 Spring `JdbcTemplate`（SQL 执行入口）
- `getTransactionTemplate()`：获取事务模板（声明式事务管理）
- `getCurrentDbUrl()`：获取当前数据库 JDBC URL
- `getHealth()`：健康检查（`SELECT 1` 探测数据库连接可用性）
- `getDataSourceType()`：获取数据源类型（"derby"/"mysql"）

#### 6.3.3.2 DynamicDataSource 动态数据源

`DynamicDataSource`（`persistence/.../datasource/DynamicDataSource.java:30-64`）继承 Spring `AbstractRoutingDataSource`：

```java
public class DynamicDataSource extends AbstractRoutingDataSource {
    
    @Override
    protected Object determineCurrentLookupKey() {
        // line 45: 从 LookupContextHolder 获取当前数据源 LookupKey
        return LookupContextHolder.getLookupKey();
    }
    
    // line 50-60: 初始化主从数据源映射
    @Override
    public void afterPropertiesSet() {
        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put("master", masterDataSource);   // 主库（写入）
        targetDataSources.put("slave", slaveDataSource);     // 从库（读取）
        super.setTargetDataSources(targetDataSources);
        super.setDefaultTargetDataSource(masterDataSource); // 默认主库
        super.afterPropertiesSet();
    }
}
```

`LookupContextHolder` 通过 `ThreadLocal` 持有当前线程的数据源 LookupKey（`"master"`/`"slave"`），读写操作前设置对应的 LookupKey → `DynamicDataSource.determineCurrentLookupKey()` 返回对应数据源 → Spring 自动路由到主库或从库。

### 6.3.4 设计模式分析

1. **策略模式（Strategy）**：`DataSourceService` 接口定义统一契约，`LocalDataSourceServiceImpl`（Derby）和 `ExternalDataSourceServiceImpl`（MySQL）两种策略实现。

2. **路由模式（Routing）**：`DynamicDataSource` 继承 `AbstractRoutingDataSource`，通过 `LookupContextHolder`（`ThreadLocal`）动态切换主/从数据源，实现读写分离。

3. **模板方法模式（Template Method）**：`AbstractRoutingDataSource.determineCurrentLookupKey()` 定义抽象方法，`DynamicDataSource` 实现具体 LookupKey 解析逻辑。

### 6.3.5 Trade-off 分析

| 权衡维度 | DynamicDataSource 动态路由 | 单一数据源 |
|---------|--------------------------|-----------|
| **读写分离** | ✅ 主库写入 + 从库读取 | ❌ 单库承载全部负载 |
| **扩展性** | 可扩展为多从库负载均衡 | 单库瓶颈 |
| **复杂度** | 需维护主从复制 + ThreadLocal LookupKey | 简单直接 |

### 6.3.6 小结

`DataSourceService` 接口定义持久化层统一数据源抽象，`LocalDataSourceServiceImpl`（嵌入式 Derby）和 `ExternalDataSourceServiceImpl`（外部 MySQL）两种实现覆盖单机/集群模式。`DynamicDataSource` 通过 `AbstractRoutingDataSource` + `LookupContextHolder`（`ThreadLocal`）实现读写分离动态路由。6.4-6.5 节分别展开两种实现的详细走读。
### 6.4.1 设计背景

`LocalDataSourceServiceImpl` 是 `DataSourceService` 的嵌入式 Derby 实现，适用于单机模式和集群模式（当未配置外部 MySQL 时默认使用嵌入式 Derby）。Derby 是 Apache 开源嵌入式 Java 数据库，无需独立安装——随 Nacos 进程启动内嵌运行，数据文件存储在 `~/nacos/data/derby/` 目录下。

嵌入式 Derby 的优势：零运维（无需独立数据库服务器）、零配置（开箱即用）、适合中小规模集群（<10 节点）。劣势：不支持高并发读写（单连接写入）、数据迁移困难（需通过 `DerbyImportEvent`/`DerbyLoadEvent` 导入导出）。

### 6.4.2 核心类关系图

图 6-4 展示了 `LocalDataSourceServiceImpl` 的嵌入式 Derby 初始化流程：

```
┌────────────────────────────────────────────────────────────────┐
│              LocalDataSourceServiceImpl                      │
│  implements DataSourceService                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - jdbcTemplate: JdbcTemplate                       │  │
│  │ - transactionTemplate: TransactionTemplate           │  │
│  │ - dataSource: DataSource                          │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ init():                                           │  │
│  │   STEP 1: DriverManager.getConnection(DERBY_URL)  │  │
│  │   STEP 2: new JdbcTemplate(dataSource)            │  │
│  │   STEP 3: new TransactionTemplate(txManager)       │  │
│  │   STEP 4: 执行 Derby 初始化 SQL (CREATE TABLE)    │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ reload():                                         │  │
│  │   → 关闭旧连接 → 重新 getConnection()             │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ getHealth():                                      │  │
│  │   → jdbcTemplate.queryForObject("SELECT 1", ...) │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        │ Derby JDBC URL: jdbc:derby:{derby.home}/data;create=true
        ▼
┌────────────────────────────────────────────────────────────────┐
│  Derby 嵌入式数据库                                      │
│  ├─ 数据文件: ~/nacos/data/derby/                       │
│  ├─ 表结构: config_info, his_config_info, users, roles, │
│  │          permissions, tenant_info, tenant_capacity       │
│  └─ 单连接写入（嵌入式 Derby 限制）                     │
└────────────────────────────────────────────────────────────────┘
```

### 6.4.3 源码走读

#### 6.4.3.1 init()——嵌入式 Derby 初始化

`LocalDataSourceServiceImpl.init()`（`persistence/src/main/java/com/alibaba/nacos/persistence/datasource/LocalDataSourceServiceImpl.java:60-120`）：

```java
public class LocalDataSourceServiceImpl implements DataSourceService {
    
    private JdbcTemplate jdbcTemplate;
    private TransactionTemplate transactionTemplate;
    private DataSource dataSource;
    
    @Override
    public void init() throws Exception {
        // line 70: 设置 Derby 系统属性（derby.home）
        System.setProperty("derby.stream.error.file", 
            EnvUtil.getNacosHome() + "/data/derby/derby.log");
        // line 75-85: 加载 Derby JDBC 驱动 + 建立连接
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        String driverUrl = "jdbc:derby:" + EnvUtil.getNacosHome() 
            + "/data/derby;create=true";
        this.dataSource = DriverManager.getConnection(driverUrl);
        // line 90-100: 创建 JdbcTemplate + TransactionTemplate
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        DataSourceTransactionManager txManager = 
            new DataSourceTransactionManager(dataSource);
        this.transactionTemplate = new TransactionTemplate(txManager);
        // line 105-115: 执行 Derby 初始化 DDL（CREATE TABLE）
        jdbcTemplate.execute(INIT_DERBY_SQL);
    }
}
```

#### 6.4.3.2 getHealth()——健康检查

`getHealth()`（`LocalDataSourceServiceImpl.java:200-215`）：

```java
public String getHealth() {
    try {
        // line 205: SELECT 1 探测 Derby 连接可用性
        jdbcTemplate.queryForObject("SELECT 1 FROM SYSIBM.SYSDUMMY1", Integer.class);
        return "UP";
    } catch (Exception e) {
        return "DOWN";
    }
}
```

### 6.4.4 设计模式分析

1. **模板方法模式（Template Method）**：`DataSourceService` 接口定义 `init()`/`reload()`/`getHealth()` 等模板方法，`LocalDataSourceServiceImpl` 实现嵌入式 Derby 的具体逻辑。

2. **工厂模式（Factory）**：`DriverManager.getConnection()` 作为工厂方法创建 Derby JDBC 连接。

### 6.4.5 Trade-off 分析

| 权衡维度 | 嵌入式 Derby | 外部 MySQL |
|---------|------------|-----------|
| **运维复杂度** | 零运维（内嵌运行） | 需独立 MySQL 服务器 |
| **并发能力** | 单连接写入（Derby 限制） | 高并发读写（HikariCP 连接池） |
| **数据迁移** | 需 `DerbyImportEvent`/`DerbyLoadEvent` | MySQL dump/import |
| **适用规模** | <10 节点小集群 | >10 节点大集群 |

### 6.4.6 小结

`LocalDataSourceServiceImpl` 通过 Derby JDBC 嵌入式驱动实现零配置内嵌数据库，适用于中小规模集群（<10 节点）。`init()` 自动创建 Derby 数据库文件 + DDL 建表，`getHealth()` 通过 `SELECT 1` 探测连接可用性。外部 MySQL 实现参见 6.5 节。


### 6.5.1 设计背景

`ExternalDataSourceServiceImpl` 是 `DataSourceService` 的外部 MySQL 实现，适用于生产环境大集群（>10 节点）。通过 HikariCP 连接池管理 MySQL 连接，支持高并发读写。配置项包括 `spring.datasource.url`/`username`/`password`/`driver-class-name` 等标准 Spring DataSource 配置。

### 6.5.2 核心类关系图

图 6-5 展示了 `ExternalDataSourceServiceImpl` 通过 HikariCP 连接池管理 MySQL 连接的架构：

```
┌────────────────────────────────────────────────────────────────┐
│              ExternalDataSourceServiceImpl                    │
│  implements DataSourceService                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - jdbcTemplate: JdbcTemplate                       │  │
│  │ - transactionTemplate: TransactionTemplate           │  │
│  │ - dataSource: HikariDataSource                     │  │
│  │ - poolProperties: DataSourcePoolProperties         │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ init():                                           │  │
│  │   STEP 1: HikariDataSource ds = new HikariDS()    │  │
│  │   STEP 2: ds.setJdbcUrl(url)                     │  │
│  │   STEP 3: ds.setUsername(username)                 │  │
│  │   STEP 4: ds.setPassword(password)                 │  │
│  │   STEP 5: ds.setMaximumPoolSize(maxPoolSize)      │  │
│  │   STEP 6: ds.setMinimumIdle(minIdle)             │  │
│  │   STEP 7: ds.setConnectionTimeout(connTimeout)     │  │
│  │   STEP 8: jdbcTemplate = new JdbcTemplate(ds)     │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ getHealth():                                      │  │
│  │   → jdbcTemplate.queryForObject("SELECT 1", ...) │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        │ HikariCP 连接池
        ▼
┌────────────────────────────────────────────────────────────────┐
│  HikariDataSource                                         │
│  ├─ jdbcUrl: jdbc:mysql://localhost:3306/nacos           │
│  ├─ username: root                                       │
│  ├─ password: ****                                       │
│  ├─ maximumPoolSize: 20                                  │
│  ├─ minimumIdle: 10                                     │
│  ├─ connectionTimeout: 30000                             │
│  ├─ idleTimeout: 600000                                │
│  └─ maxLifetime: 1800000                               │
└────────────────────────────────────────────────────────────────┘
```

### 6.5.3 源码走读

#### 6.5.3.1 init()——HikariCP 连接池初始化

`ExternalDataSourceServiceImpl.init()`（`persistence/src/main/java/com/alibaba/nacos/persistence/datasource/ExternalDataSourceServiceImpl.java:60-180`）：

```java
public class ExternalDataSourceServiceImpl implements DataSourceService {
    
    private JdbcTemplate jdbcTemplate;
    private TransactionTemplate transactionTemplate;
    private HikariDataSource dataSource;
    private DataSourcePoolProperties poolProperties;
    
    @Override
    public void init() throws Exception {
        // line 75-85: 加载 ExternalDataSourceProperties 配置
        ExternalDataSourceProperties properties = 
            new ExternalDataSourceProperties();
        // line 90-110: 创建 HikariCP 连接池
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(properties.getUrl());                    // jdbc:mysql://localhost:3306/nacos
        ds.setUsername(properties.getUsername());               // root
        ds.setPassword(properties.getPassword());               // ****
        ds.setMaximumPoolSize(poolProperties.getMaximumPoolSize()); // 20
        ds.setMinimumIdle(poolProperties.getMinimumIdle());     // 10
        ds.setConnectionTimeout(poolProperties.getConnectionTimeout()); // 30000
        ds.setIdleTimeout(poolProperties.getIdleTimeout());     // 600000
        ds.setMaxLifetime(poolProperties.getMaxLifetime());     // 1800000
        ds.setDriverClassName(properties.getDriverClassName());   // com.mysql.cj.jdbc.Driver
        this.dataSource = ds;
        // line 115-120: 创建 JdbcTemplate + TransactionTemplate
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        DataSourceTransactionManager txManager = 
            new DataSourceTransactionManager(dataSource);
        this.transactionTemplate = new TransactionTemplate(txManager);
    }
}
```

### 6.5.4 设计模式分析

1. **对象池模式（Object Pool）**：HikariCP 连接池管理 MySQL 连接的创建、复用和销毁，`maximumPoolSize=20` 最大连接数，`minimumIdle=10` 最小空闲连接数。

2. **工厂模式（Factory）**：`HikariDataSource` 作为连接工厂，通过 `getConnection()` 从连接池获取可用连接。

### 6.5.5 Trade-off 分析

| 权衡维度 | HikariCP 连接池 | 单连接（DriverManager） |
|---------|-----------------|----------------------|
| **并发能力** | 多连接并发读写 | 单连接串行写入 |
| **连接复用** | ✅ 连接池复用 | ❌ 每次新建连接 |
| **连接超时** | `connectionTimeout=30000ms` | 无超时控制 |
| **空闲回收** | `idleTimeout=600000ms` | 无自动回收 |

### 6.5.6 小结

`ExternalDataSourceServiceImpl` 通过 HikariCP 连接池管理 MySQL 连接，支持高并发读写。`init()` 自动配置 `maximumPoolSize=20`/`minimumIdle=10` 等连接池参数。适用于生产环境大集群（>10 节点）。嵌入式 Derby 实现参见 6.4 节。
### 6.6.1 设计背景

`PaginationHelper<T>` 是 `persistence/` 模块的分页抽象基类，提供统一的分页 CRUD 模板方法——`paginate(pageNo, pageSize, criteria)` 分页查询、`findAll(criteria)` 全量查询、`findOne(criteria)` 单条查询、`count(criteria)` 计数、`insert(entity)` 插入、`update(entity)` 更新、`delete(entity)` 删除。`EmbeddedPaginationHelperImpl` 实现嵌入式 Derby 的 `LIMIT OFFSET` 分页语法（Derby 不支持 `LIMIT` 关键字，需使用 `OFFSET ... FETCH NEXT ... ROWS ONLY` 语法）。`EmbeddedStorageContextHolder` 持有嵌入式存储的 `DataSource` 和 `JdbcTemplate` 上下文。

### 6.6.2 核心类关系图

图 6-6 展示了 `PaginationHelper<T>` 抽象基类与 `EmbeddedPaginationHelperImpl` 的分页机制：

```
┌────────────────────────────────────────────────────────────────┐
│           PaginationHelper<T> (abstract)                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ + paginate(int pageNo, int pageSize, T criteria)  │  │
│  │     → Page<T> (分页查询)                         │  │
│  │ + findAll(T criteria) → List<T>                   │  │
│  │ + findOne(T criteria) → T                         │  │
│  │ + count(T criteria) → int                          │  │
│  │ + insert(T entity) → Boolean                       │  │
│  │ + update(T entity) → Boolean                       │  │
│  │ + delete(T entity) → Boolean                       │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        △                              △
        │                              │
┌───────┴──────────────────┐  ┌────────┴──────────────────────┐
│ EmbeddedPagination      │  │ ExternalStorage               │
│ HelperImpl             │  │ PaginationHelperImpl          │
│ ├─ paginate():       │  │ ├─ paginate():               │
│ │   SELECT * FROM t   │  │ │   SELECT * FROM t         │
│ │   OFFSET x ROWS   │  │ │   LIMIT x OFFSET y       │
│ │   FETCH NEXT y     │  │ └─ count(): SELECT COUNT(*)  │
│ │   ROWS ONLY        │  └──────────────────────────────┘
│ ├─ count():          │
│ │   SELECT COUNT(*)   │
│ └────────────────────┘
└────────────────────────────────────────────────────────────────┘
```

### 6.6.3 源码走读

#### 6.6.3.1 PaginationHelper<T> 抽象基类

`PaginationHelper<T>`（`persistence/.../repository/embedded/EmbeddedPaginationHelperImpl.java:40-150`——嵌入式 Derby 实现）：

```java
public class EmbeddedPaginationHelperImpl<T> extends PaginationHelper<T> {
    
    // line 55: 持有 EmbeddedStorageContextHolder
    private final EmbeddedStorageContextHolder contextHolder;
    
    @Override
    public Page<T> paginate(int pageNo, int pageSize, T criteria) {
        // line 65-75: 构造 Derby 分页 SQL（OFFSET...FETCH NEXT ROWS ONLY）
        String sql = "SELECT * FROM " + getTableName(criteria) 
            + " WHERE 1=1 " + buildWhereClause(criteria)
            + " ORDER BY id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
        // line 80: 执行分页查询
        List<T> list = contextHolder.getJdbcTemplate().query(
            sql, new Object[]{ (pageNo - 1) * pageSize, pageSize },
            new BeanPropertyRowMapper<>(getEntityClass())
        );
        // line 85: 查询总数
        int totalCount = count(criteria);
        return new Page<>(list, pageNo, pageSize, totalCount);
    }
    
    @Override
    public int count(T criteria) {
        // line 95-100: SELECT COUNT(*) 查询总数
        String sql = "SELECT COUNT(*) FROM " + getTableName(criteria) 
            + " WHERE 1=1 " + buildWhereClause(criteria);
        return contextHolder.getJdbcTemplate().queryForObject(sql, Integer.class);
    }
}
```

#### 6.6.3.2 EmbeddedStorageContextHolder

`EmbeddedStorageContextHolder` 通过 `ThreadLocal` 持有当前线程的嵌入式存储上下文（`DataSource` + `JdbcTemplate`），保证多线程环境下的数据源隔离。

### 6.6.4 设计模式分析

1. **模板方法模式（Template Method）**：`PaginationHelper<T>` 抽象基类定义 `paginate()`/`findAll()`/`count()` 等模板方法，`EmbeddedPaginationHelperImpl` 实现嵌入式 Derby 的 `OFFSET...FETCH NEXT ROWS ONLY` 分页语法。

2. **策略模式（Strategy）**：`EmbeddedPaginationHelperImpl`（Derby `LIMIT OFFSET` 语法）和 `ExternalStoragePaginationHelperImpl`（MySQL `LIMIT OFFSET` 语法）两种分页策略。

3. **上下文持有者模式（Context Holder）**：`EmbeddedStorageContextHolder` 通过 `ThreadLocal` 持有嵌入式存储上下文，保证线程安全。

### 6.6.5 Trade-off 分析

| 权衡维度 | Derby OFFSET...FETCH NEXT | MySQL LIMIT OFFSET |
|---------|--------------------------|---------------------|
| **SQL 标准** | SQL:2008 标准 | MySQL 方言 |
| **性能** | 大偏移量性能下降（需扫描跳过行） | 同左 |
| **适用场景** | 嵌入式 Derby 环境 | 外部 MySQL 环境 |

### 6.6.6 小结

`PaginationHelper<T>` 抽象基类提供统一的分页 CRUD 模板方法，`EmbeddedPaginationHelperImpl` 实现嵌入式 Derby 的 `OFFSET...FETCH NEXT ROWS ONLY` 分页语法。`EmbeddedStorageContextHolder` 通过 `ThreadLocal` 持有嵌入式存储上下文。外部 MySQL 分页实现参见 6.8 节。


### 6.7.1 设计背景

`DatabaseOperate` 接口定义 SQL 操作的统一抽象——`executeModify(ModifyRequest)` 执行 INSERT/UPDATE/DELETE、`executeQuery(SelectRequest)` 执行 SELECT 查询。`StandaloneDatabaseOperateImpl` 是单机模式的默认实现，通过 `JdbcTemplate` 执行 SQL 操作。`ModifyRequest`/`SelectRequest` 提供类型安全的 SQL 构造 DSL——避免字符串拼接 SQL 的安全风险（SQL 注入）和维护复杂性。

### 6.7.2 核心类关系图

图 6-7 展示了 `DatabaseOperate` 接口与 `ModifyRequest`/`SelectRequest` SQL DSL 的关系：

```
┌────────────────────────────────────────────────────────────────┐
│          <<interface>> DatabaseOperate                      │
│  ├─ executeModify(ModifyRequest): Boolean                 │
│  ├─ executeQuery(SelectRequest): List<T>                 │
│  ├─ executeCount(SelectRequest): int                      │
│  └─ executeOne(SelectRequest): T                          │
└────────────────────────────────────────────────────────────────┘
        △
        │ implements
┌───────┴────────────────────────────────────────────────────────┐
│            StandaloneDatabaseOperateImpl                    │
│  ├─ jdbcTemplate: JdbcTemplate                           │
│  ├─ executeModify(ModifyRequest):                        │
│  │   → jdbcTemplate.update(sql, args)                   │
│  └─ executeQuery(SelectRequest):                         │
│      → jdbcTemplate.query(sql, args, rowMapper)          │
└────────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
┌──────────────────────┐    ┌──────────────────────────────────┐
│  ModifyRequest      │    │  SelectRequest                  │
│  ├─ sql: String    │    │  ├─ selectColumns: String[]  │
│  ├─ args: Object[] │    │  ├─ tableName: String        │
│  ├─ tableName      │    │  ├─ whereClause: String      │
│  └─ queryType      │    │  ├─ orderByClause: String    │
│    (INSERT/UPDATE/  │    │  ├─ limitClause: String      │
│     DELETE)        │    │  └─ queryType: QueryType      │
└──────────────────────┘    └──────────────────────────────────┘
```

### 6.7.3 源码走读

#### 6.7.3.1 DatabaseOperate 接口

`DatabaseOperate`（`persistence/.../repository/embedded/operate/DatabaseOperate.java:28-55`）：

```java
public interface DatabaseOperate {
    
    // 执行 UPDATE/INSERT/DELETE
    <T> Boolean executeModify(ModifyRequest modifyRequest);
    
    // 执行 SELECT 查询（返回列表）
    <T> List<T> executeQuery(SelectRequest selectRequest);
    
    // 执行 SELECT COUNT 查询
    <T> int executeCount(SelectRequest selectRequest);
    
    // 执行 SELECT 查询（返回单个对象）
    <T> T executeOne(SelectRequest selectRequest);
}
```

#### 6.7.3.2 StandaloneDatabaseOperateImpl

`StandaloneDatabaseOperateImpl`（`persistence/.../repository/embedded/operate/StandaloneDatabaseOperateImpl.java:35-100`）：

```java
public class StandaloneDatabaseOperateImpl implements DatabaseOperate {
    
    private final JdbcTemplate jdbcTemplate;
    
    @Override
    public <T> Boolean executeModify(ModifyRequest modifyRequest) {
        // line 50-55: 通过 JdbcTemplate.update() 执行 INSERT/UPDATE/DELETE
        int affectedRows = jdbcTemplate.update(
            modifyRequest.getSql(), 
            modifyRequest.getArgs()
        );
        return affectedRows > 0;
    }
    
    @Override
    public <T> List<T> executeQuery(SelectRequest selectRequest) {
        // line 65-75: 通过 JdbcTemplate.query() 执行 SELECT 查询
        return jdbcTemplate.query(
            selectRequest.getSql(),
            selectRequest.getArgs(),
            selectRequest.getRowMapper()
        );
    }
}
```

### 6.7.4 设计模式分析

1. **命令模式（Command）**：`ModifyRequest`（INSERT/UPDATE/DELETE）和 `SelectRequest`（SELECT）封装 SQL 命令及其参数，`DatabaseOperate.executeModify()`/`executeQuery()` 作为命令执行器。

2. **DSL 模式（Domain-Specific Language）**：`ModifyRequest`/`SelectRequest` 提供类型安全的 SQL 构造 DSL，避免字符串拼接 SQL 的 SQL 注入风险和维护复杂性。

3. **模板方法模式（Template Method）**：`DatabaseOperate` 接口定义 `executeModify()`/`executeQuery()`/`executeCount()`/`executeOne()` 统一 SQL 操作模板方法。

### 6.7.5 Trade-off 分析

| 权衡维度 | SQL DSL (ModifyRequest/SelectRequest) | 字符串拼接 SQL |
|---------|--------------------------------------|----------------|
| **类型安全** | ✅ 编译期类型检查 | ❌ 运行时字符串拼接 |
| **SQL 注入防护** | ✅ 参数化查询自动防护 | ❌ 需手动转义 |
| **可维护性** | ✅ SQL 结构清晰 | ❌ 字符串拼接难以维护 |

### 6.7.6 小结

`DatabaseOperate` 接口定义统一的 SQL 操作抽象，`StandaloneDatabaseOperateImpl` 通过 `JdbcTemplate` 执行 SQL 操作。`ModifyRequest`/`SelectRequest` 提供类型安全的 SQL 构造 DSL——避免字符串拼接 SQL 注入风险和维护复杂性。


### 6.8.1 设计背景

`ExternalStoragePaginationHelperImpl` 是 `PaginationHelper<T>` 的外部 MySQL 分页实现，使用 MySQL 标准的 `LIMIT x OFFSET y` 分页语法。与嵌入式 Derby 的 `OFFSET...FETCH NEXT ROWS ONLY` 语法不同，MySQL 直接支持 `LIMIT` 关键字，分页 SQL 更简洁。

### 6.8.2 核心类关系图

图 6-8 展示了 `ExternalStoragePaginationHelperImpl` 基于 MySQL `LIMIT OFFSET` 分页语法：

```
┌────────────────────────────────────────────────────────────────┐
│        ExternalStoragePaginationHelperImpl<T>               │
│  extends PaginationHelper<T>                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ paginate(pageNo, pageSize, criteria): Page<T>      │  │
│  │   SELECT * FROM t WHERE ... ORDER BY id            │  │
│  │   LIMIT ? OFFSET ?                                │  │
│  │   → jdbcTemplate.query(sql, args, rowMapper)      │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ count(criteria): int                               │  │
│  │   SELECT COUNT(*) FROM t WHERE ...                 │  │
│  │   → jdbcTemplate.queryForObject(sql, Integer.class)│  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 6.8.3 源码走读

`ExternalStoragePaginationHelperImpl.paginate()`——MySQL `LIMIT OFFSET` 分页：

```java
@Override
public Page<T> paginate(int pageNo, int pageSize, T criteria) {
    // line 55-65: 构造 MySQL LIMIT OFFSET 分页 SQL
    String sql = "SELECT * FROM " + getTableName(criteria) 
        + " WHERE 1=1 " + buildWhereClause(criteria)
        + " ORDER BY id LIMIT ? OFFSET ?";
    List<T> list = jdbcTemplate.query(
        sql, 
        new Object[]{ pageSize, (pageNo - 1) * pageSize },
        new BeanPropertyRowMapper<>(getEntityClass())
    );
    int totalCount = count(criteria);
    return new Page<>(list, pageNo, pageSize, totalCount);
}
```

### 6.8.4 设计模式分析

1. **策略模式（Strategy）**：`EmbeddedPaginationHelperImpl`（Derby `OFFSET...FETCH NEXT`）和 `ExternalStoragePaginationHelperImpl`（MySQL `LIMIT OFFSET`）两种分页策略，适配不同数据库的 SQL 方言。

### 6.8.5 Trade-off 分析

| 权衡维度 | MySQL LIMIT OFFSET | Derby OFFSET...FETCH NEXT |
|---------|-------------------|--------------------------|
| **SQL 简洁性** | ✅ `LIMIT ? OFFSET ?` | `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY` |
| **数据库支持** | MySQL/MariaDB/PostgreSQL | Derby/Apache Derby |
| **性能** | 大偏移量性能下降（InnoDB 扫描跳过行） | 同左 |

### 6.8.6 小结

`ExternalStoragePaginationHelperImpl` 使用 MySQL 标准的 `LIMIT x OFFSET y` 分页语法，SQL 更简洁。嵌入式 Derby 分页实现参见 6.6 节。
### 6.9.1 设计背景

`ModifyRequest`/`SelectRequest` 是 `persistence/` 模块的类型安全 SQL 构造 DSL（Domain-Specific Language），封装 SQL 语句的构造细节——表名、WHERE 条件、ORDER BY 排序、LIMIT 分页、参数绑定等。`QueryType` 枚举定义 SQL 操作类型（`INSERT`/`UPDATE`/`DELETE`/`SELECT`/`COUNT`），通过 `SqlLimiter` 限制 SQL 操作范围（如单次 DELETE 最大行数限制防误删）。

### 6.9.2 核心类关系图

图 6-9 展示了 `ModifyRequest`/`SelectRequest` SQL 构造 DSL 的层次结构：

```
┌────────────────────────────────────────────────────────────────┐
│              ModifyRequest                                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - sql: String          ← 最终构造的 SQL 语句       │  │
│  │ - args: Object[]       ← 参数绑定数组              │  │
│  │ - tableName: String    ← 操作表名                  │  │
│  │ - queryType: QueryType ← INSERT/UPDATE/DELETE      │  │
│  │ - SqlLimiter          ← SQL 操作限制器            │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ + getSql(): String                                 │  │
│  │ + getArgs(): Object[]                              │  │
│  │ + getQueryType(): QueryType                        │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│              SelectRequest                                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ - selectColumns: String[] ← SELECT 列名数组         │  │
│  │ - tableName: String       ← FROM 表名               │  │
│  │ - whereClause: String    ← WHERE 条件子句          │  │
│  │ - orderByClause: String ← ORDER BY 排序子句       │  │
│  │ - limitClause: String    ← LIMIT OFFSET 分句子句  │  │
│  │ - queryType: QueryType   ← SELECT / COUNT           │  │
│  ├──────────────────────────────────────────────────────┤  │
│  │ + getSql(): String                                 │  │
│  │ + getArgs(): Object[]                              │  │
│  │ + getRowMapper(): RowMapper<T>                    │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│              QueryType (enum)                               │
│  ├─ INSERT    → executeModify(modifyRequest)             │
│  ├─ UPDATE    → executeModify(modifyRequest)             │
│  ├─ DELETE    → executeModify(modifyRequest)             │
│  ├─ SELECT    → executeQuery(selectRequest)               │
│  └─ COUNT     → executeCount(selectRequest)               │
└────────────────────────────────────────────────────────────────┘
```

### 6.9.3 源码走读

#### 6.9.3.1 ModifyRequest SQL 构造

`ModifyRequest`——INSERT/UPDATE/DELETE SQL 构造：

```java
public class ModifyRequest {
    
    private String sql;
    private Object[] args;
    private String tableName;
    private QueryType queryType;
    private SqlLimiter sqlLimiter;
    
    public static ModifyBuilder newBuilder() {
        return new ModifyBuilder();
    }
    
    public static class ModifyBuilder {
        
        public ModifyBuilder table(String tableName) { ... }
        public ModifyBuilder setColumns(String[] columns, Object[] values) { ... }
        public ModifyBuilder where(String whereClause看在) { ... }
        public ModifyBuilder queryType(QueryType type) { ... }
        public ModifyBuilder limiter(SqlLimiter limiter) { ... }
        public ModifyRequest build() { ... }
    }
}
```

#### 6.9.3.2 SelectRequest SQL 构造

`SelectRequest`——SELECT/COUNT SQL 构造：

```java
public class SelectRequest {
    
    private String sql;
    private Object[] args;
    private String[] selectColumns;
    private String tableName;
    private String whereClause;
    private String orderByClause;
    private String limitClause;
    private QueryType queryType;
    
    public static SelectBuilder newBuilder() { ... }
    
    public static class SelectBuilder {
        public SelectBuilder select(String... columns) { ... }
        public SelectBuilder from(String tableName) { ... }
        public SelectBuilder where(String whereClause) { ... }
        public SelectBuilder orderBy(String orderByClause) { ... }
        public SelectBuilder limit(int offset, int limit) { ... }
        public SelectRequest build() { ... }
    }
}
```

### 6.9.4 设计模式分析

1. **建造者模式（Builder）**：`ModifyRequest.ModifyBuilder`/`SelectRequest.SelectBuilder` 通过 Builder 模式分步构造 SQL 语句——`.table()`→`.where()`→`.orderBy()`→`.limit()`→`.build()`，链式调用清晰可读。

2. **DSL 模式（Domain-Specific Language）**：`ModifyRequest`/`SelectRequest` 提供类型安全的 SQL 构造 DSL，编译期检查表名/列名/条件子句的正确性。

3. **策略模式（Strategy）**：`QueryType` 枚举定义 SQL 操作类型（`INSERT`/`UPDATE`/`DELETE`/`SELECT`/`COUNT`），`DatabaseOperate` 根据 `QueryType` 分发到不同的执行方法。

### 6.9.5 Trade-off 分析

| 权衡维度 | Builder DSL | 字符串拼接 SQL |
|---------|-----------|----------------|
| **类型安全** | ✅ Builder 方法编译期检查 | ❌ 运行时字符串拼接 |
| **SQL 注入防护** | ✅ 参数化查询自动防护 | ❌ 需手动转义 |
| **可读性** | ✅ 链式调用清晰可读 | ❌ 长字符串难以维护 |

### 6.9.6 小结

`ModifyRequest`/`SelectRequest` 通过 Builder 模式提供类型安全的 SQL 构造 DSL——避免字符串拼接 SQL 注入风险和维护复杂性。`QueryType` 枚举定义 SQL 操作类型，`DatabaseOperate` 根据 `QueryType` 分发到不同的执行方法。


### 6.10.1 设计背景

`persistence/` 模块通过事件体系支持 CP 一致性快照的导入导出。`DerbyImportEvent`（Derby 数据导入事件——从 CP Leader 拉取快照并导入本地 Derby）、`DerbyLoadEvent`（Derby 数据加载事件——从本地 Derby 加载快照数据）、`RaftDbErrorEvent`（Raft 数据库错误事件——JRaft 快照写入/读取异常时触发）。事件通过 `NotifyCenter` 发布，订阅者（如 `JRaftProtocol`）接收事件并执行对应的快照操作。

### 6.10.2 核心类关系图

图 6-10 展示了 Derby 事件体系的导入/导出流程：

```
┌────────────────────────────────────────────────────────────────┐
│                  NotifyCenter (事件总线)                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ publishEvent(event): void                           │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
│ DerbyImport  │  │ DerbyLoad    │  │ RaftDbError         │
│ Event       │  │ Event        │  │ Event                │
│ ├─ snapshot  │  │ ├─ tableName │  │ ├─ errorMsg: String │
│ └─ data: byte│  │ └─ data: List│  │ └─ exception: Throw │
└──────────────┘  └──────────────┘  └──────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│  JRaftProtocol.onEvent(DerbyImportEvent)               │
│  ├─ onSnapshotLoad(snapshot):                           │
│  │   → 从 Leader 拉取快照 → 写入本地 Derby            │
│  ├─ onSnapshotSave(snapshot):                           │
│  │   → 从本地 Derby 加载快照 → 发送给 Follower       │
│  └─ onRaftDbError(event):                               │
│      → 记录错误日志 + 触发快照重试                     │
└──────────────────────────────────────────────────────────────┘
```

### 6.10.3 源码走读

#### 6.10.3.1 DerbyImportEvent

`DerbyImportEvent`——Derby 数据导入事件（从 CP Leader 拉取快照并导入本地 Derby）：

```java
public class DerbyImportEvent {
    
    private String snapshotData;  // 快照数据（字节数组）
    private long snapshotVersion; // 快照版本号
    private String tableName;     // 目标表名
    
    public DerbyImportEvent(String tableName, String snapshotData, long version) {
        this.tableName = tableName;
        this.snapshotData = snapshotData;
        this.snapshotVersion = version;
    }
}
```

#### 6.10.3.2 RaftDbErrorEvent

`RaftDbErrorEvent`——Raft 数据库错误事件（JRaft 快照写入/读取异常时触发）：

```java
public class RaftDbErrorEvent {
    
    private String errorMsg;       // 错误消息
    private Throwable exception;   // 异常对象
    
    public RaftDbErrorEvent(String errorMsg, Throwable exception) {
        this.errorMsg = errorMsg;
        this.exception = exception;
    }
}
```

### 6.10.4 设计模式分析

1. **观察者模式（Observer）**：`DerbyImportEvent`/`DerbyLoadEvent`/`RaftDbErrorEvent` 通过 `NotifyCenter.publishEvent()` 发布事件，`JRaftProtocol` 订阅并执行对应的快照导入/导出操作。

2. **事件驱动模式（Event-Driven）**：快照导入/导出完全通过事件驱动——Leader 发布 `DerbyImportEvent`→Follower 接收事件→执行 `onSnapshotLoad()`→写入本地 Derby。

### 6.10.5 Trade-off 分析

| 权衡维度 | 事件驱动快照导入/导出 | 直接调用 |
|---------|----------------------|---------|
| **解耦性** | ✅ 事件发布者与订阅者完全解耦 | ❌ 直接耦合 |
| **异步性** | ✅ 异步事件处理 | ❌ 同步调用阻塞 |
| **可靠性** | ⚠️ 事件丢失风险（需持久化事件日志） | ✅ 同步调用确定性强 |

### 6.10.6 小结

`DerbyImportEvent`/`DerbyLoadEvent`/`RaftDbErrorEvent` 通过 `NotifyCenter` 事件总线实现 CP 一致性快照的导入导出——Leader 发布 `DerbyImportEvent`→Follower 接收→`JRaftProtocol.onSnapshotLoad()`→写入本地 Derby。事件驱动模式实现快照导入/导出与 CP 一致性协议的完全解耦。


### 6.11.1 设计背景

Nacos 2.5.3 在 `persistence/` 模块中引入 `DatasourceMetrics` 数据源监控，基于 Micrometer 指标库暴露数据源健康状态和性能指标。监控指标包括：`datasource.health`（数据源健康状态——UP/DOWN）、`datasource.active.connections`（活跃连接数）、`datasource.idle.connections`（空闲连接数）、`datasource.pending.connections`（等待连接数）、`datasource.query.time`（查询耗时分布）。

### 6.11.2 核心类关系图

图 6-11 展示了 `DatasourceMetrics` 数据源监控指标：

```
┌────────────────────────────────────────────────────────────────┐
│              DatasourceMetrics                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ @Bean                                              │  │
│  │ datasourceMetrics(MeterRegistry registry):          │  │
│  │   ├─ Gauge.builder("datasource.health", ds,       │  │
│  │   │    ds -> ds.getHealth())                      │  │
│  │   ├─ Gauge.builder("datasource.active", ds,       │  │
│  │   │    ds -> ds.getActiveConnections())           │  │
│  │   ├─ Gauge.builder("datasource.idle", ds,         │  │
│  │   │    ds -> ds.getIdleConnections())             │  │
│  │   ├─ Gauge.builder("datasource.pending", ds,      │  │
│  │   │    ds -> ds.getPendingConnections())           │  │
│  │   └─ Timer.builder("datasource.query.time")         │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 6.11.3 源码走读

`DatasourceMetrics`——基于 Micrometer `MeterRegistry` 暴露数据源指标：

```java
@Configuration
public class DatasourceMetrics {
    
    @Bean
    public MeterBinder datasourceMetrics(DataSourceService dataSourceService) {
        return registry -> {
            // Gauge: 数据源健康状态（UP=1, DOWN=0）
            Gauge.builder("datasource.health", dataSourceService, ds -> {
                return "UP".equals(ds.getHealth()) ? 1 : 0;
            }).register(registry);
            
            // Gauge: 活跃连接数
            Gauge.builder("datasource.active.connections", dataSourceService, ds -> {
                return ds.getActiveConnections();
            }).register(registry);
            
            // Gauge: 空闲连接数
            Gauge.builder("datasource.idle.connections", dataSourceService, ds -> {
                return ds.getIdleConnections();
            }).register(registry);
        };
    }
}
```

### 6.11.4 设计模式分析

1. **观察者模式（Observer）**：Micrometer `MeterRegistry` 定期拉取（`Gauge`）数据源指标，Prometheus/Grafana 通过 `/actuator/prometheus` 端点暴露指标。

2. **门面模式（Facade）**：`DatasourceMetrics` 封装 Micrometer 指标注册细节，向运维监控系统（Prometheus + Grafana）提供统一的数据源健康监控视图。

### 6.11.5 Trade-off 分析

| 权衡维度 | Micrometer 指标监控 | 日志监控 |
|---------|-------------------|---------|
| **实时性** | ✅ Prometheus 每 15s 拉取 | ❌ 需日志分析工具 |
| **可视化** | ✅ Grafana Dashboard | ❌ 需额外工具 |
| **告警能力** | ✅ Prometheus AlertManager | ❌ 需自定义告警脚本 |

### 6.11.6 小结

`DatasourceMetrics` 基于 Micrometer `MeterRegistry` 暴露数据源健康状态和性能指标——`datasource.health`/`datasource.active.connections`/`datasource.idle.connections`/`datasource.query.time`。Prometheus + Grafana 通过 `/actuator/prometheus` 端点可视化监控数据源健康状态。


### 6.12.1 设计背景

`ConnectionCheckUtil` 和 `DatasourcePlatformUtil` 是 `persistence/` 模块的工具类，提供数据库连接可用性检查和数据源平台识别功能。`ConnectionCheckUtil.checkConnection()` 通过 `SELECT 1` 探测数据库连接可用性，`DatasourcePlatformUtil.getPlatform()` 识别当前数据源平台类型（`derby`/`mysql`）。

### 6.12.2 核心类关系图

图 6-12 展示了 `ConnectionCheckUtil` 和 `DatasourcePlatformUtil` 工具类：

```
┌────────────────────────────────────────────────────────────────┐
│              ConnectionCheckUtil                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ static checkConnection(DataSource ds): boolean       │  │
│  │   → try { ds.getConnection().createStatement()     │  │
│  │          .execute("SELECT 1") } catch { return false}│  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────────┐
│            DatasourcePlatformUtil                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ static getPlatform(DataSource ds): String            │  │
│  │   → DatabaseMetaData meta = ds.getConnection()       │  │
│  │     .getMetaData()                                 │  │
│  │   → meta.getDatabaseProductName()                  │  │
│  │   → "Apache Derby" → return "derby"               │  │
│  │   → "MySQL" → return "mysql"                       │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 6.12.3 源码走读

#### 6.12.3.1 ConnectionCheckUtil.checkConnection()

`ConnectionCheckUtil.checkConnection()`（`persistence/.../utils/ConnectionCheckUtil.java:28-48`）：

```java
public static boolean checkConnection(DataSource ds) {
    try (Connection conn = ds.getConnection()) {
        // line 35: 执行 SELECT 1 探测连接可用性
        Statement stmt = conn.createStatement();
        stmt.execute("SELECT 1");
        return true;
    } catch (SQLException e) {
        return false;
    }
}
```

#### 6.12.3.2 DatasourcePlatformUtil.getPlatform()

`DatasourcePlatformUtil.getPlatform()`——通过 JDBC `DatabaseMetaData.getDatabaseProductName()` 识别数据源平台类型：

```java
public static String getPlatform(DataSource ds) {
    try (Connection conn = ds.getConnection()) {
        DatabaseMetaData meta = conn.getMetaData();
        String productName = meta.getDatabaseProductName();
        if ("Apache Derby".equals(productName)) {
            return "derby";
        } else if ("MySQL".equals(productName)) {
            return "mysql";
        }
        return "unknown";
    } catch (SQLException e) {
        return "error";
    }
}
```

### 6.12.4 设计模式分析

1. **工具类模式（Utility Pattern）**：`ConnectionCheckUtil` 和 `DatasourcePlatformUtil` 提供静态工具方法——无需实例化，直接通过类名调用。

2. **适配器模式（Adapter）**：`DatasourcePlatformUtil.getPlatform()` 将 JDBC `DatabaseMetaData.getDatabaseProductName()` 的原始返回值适配为 Nacos 内部平台标识（`"derby"`/`"mysql"`）。

### 6.12.5 Trade-off 分析

| 权衡维度 | 工具类静态方法 | 实例方法 |
|---------|--------------|---------|
| **调用简便性** | ✅ `Util.method()` 直接调用 | 需 `new Util().method()` |
| **可测试性** | ⚠️ 静态方法难以 Mock | ✅ 实例方法可 Mock |
| **状态管理** | ❌ 无状态（纯函数） | ✅ 可持有状态 |

### 6.12.6 小结

`ConnectionCheckUtil.checkConnection()` 通过 `SELECT 1` 探测数据库连接可用性，`DatasourcePlatformUtil.getPlatform()` 通过 JDBC `DatabaseMetaData` 识别数据源平台类型（`derby`/`mysql`）。两个工具类提供无状态的纯函数式工具方法。
