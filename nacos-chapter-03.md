# 第3章：Nacos 2.5.3 配置中心（Config）源码深度分析

## 3.1 Config 模块全景

Config 模块（路径：`nacos-2.5.3/config/`）是 Nacos 配置中心的核心模块，负责配置发布、订阅、长轮询、历史版本管理等功能。2.5.3 版本包含 **217 个 Java 主代码文件**（不含测试），分布在以下子包中：

| 子包 | 核心类 | 职责 | 2.5.3 变更 |
|------|--------|------|------------|
| `config/server/controller/` | ConfigController | REST API 入口 | — |
| `config/server/service/` | LongPollingService、AsyncNotifyService | 长轮询引擎 | — |
| `config/server/service/dump/` | DumpService | 配置落盘 | — |
| `config/server/model/` | ConfigInfo、HistoryConfigInfo | 配置实体 | **★新增 ConfigCache 缓存机制** |
| `config/server/service/repository/` | PersistService | 持久化服务 | **★存储条件注解移至 persistence 模块** |
| `config/server/aspect/` | ConfigChangeAspect | 配置变更切面 | **★新增 ConfigChangeAspect** |
| `config/server/filter/` | ConfigEnabledFilter | 模块启用过滤器 | **★新增 ConfigEnabledFilter** |
| `config/server/enums/` | ApiVersionEnum | API 版本枚举 | **★新增 ApiVersionEnum** |
| `config/server/constant/` | ConfigModuleStateBuilder | 模块状态构建器 | **★新增 ConfigModuleStateBuilder** |

### 2.2.3 → 2.5.3 Config 模块核心变更

| 变更类型 | 2.2.3 | 2.5.3 | 说明 |
|---------|-------|-------|------|
| 存储条件注解 | `ConditionOnEmbeddedStorage` 等 | **移至 persistence 模块** | 持久化逻辑独立 |
| JDBC 异常 | `NJdbcException` | **移至 persistence 模块** | 统一异常处理 |
| 配置缓存 | 无 | **ConfigCache / ConfigCacheFactory** | **★新增多级缓存** |
| 灰度配置 | ConfigInfoBeta | **ConfigInfoGrayWrapper** | **★新增灰度包装器** |
| 配置变更切面 | 无 | **ConfigChangeAspect** | **★新增 AOP 切面** |
| API 版本 | 无 | **ApiVersionEnum** | **★新增 API 版本枚举** |
| 操作类型 | 无 | **OperationType** | **★新增操作类型枚举** |

## 3.2 核心类关系图

```
┌──────────────────────────────────────────────────────────────┐
│              Config 模块核心类关系图 (2.5.3)                  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ConfigController ───────────────────────────────────────┐    │
│  │ POST /v1/cs/configs (publishConfig)                │    │
│  │ GET /v1/cs/configs (getConfig)                    │    │
│  │ DELETE /v1/cs/configs (removeConfig)               │    │
│  │ POST /v1/cs/configs/listener (getConfigListen)    │    │
│  ▼                                                   │    │
│  ConfigChangePublisher ───────────────────────────────┐│    │
│  │ publishConfigChange()                            ││    │
│  │ asyncNotifyService.onConfigChange()              ││    │
│  ▼                                                 ││    │
│  PersistService (持久化层) ─────────────────────┐  ││    │
│  │ insertOrUpdateConfig()                       │  ││    │
│  │ insertHistoryConfig()                        │  ││    │
│  │ ★ 2.5.3: PersistServiceIF → persistence模块  │  ││    │
│  ▼                                             │  ││    │
│  LongPollingService ──────────────────────┐     │  ││    │
│  │ allSubs: Queue<ClientLongPolling>   │     │  ││    │
│  │ LongPollingRunnable.generateResponse()│     │  ││    │
│  │ ★ MD5 对比 + 304 Not Modified       │     │  ││    │
│  ▼                                     │     │  ││    │
│  AsyncNotifyService ─────────────┐      │     │  ││    │
│  │ /v1/cs/communication/notify   │      │     │  ││    │
│  │ ★ 集群间 HTTP 异步通知      │      │     │  ││    │
│  ▼                               │      │     │  ││    │
│  ConfigCache★ ──────────────┐   │      │     │  ││    │
│  │ ConfigCacheFactory       │   │      │     │  ││    │
│  │ ConfigCacheGray          │   │      │     │  ││    │
│  │ ConfigCachePostProcessor │   │      │     │  ││    │
│  └─────────────────────────┘   │      │     │  ││    │
│                                │      │     │  ││    │
│  HistoryConfigInfoService ─────┘      │     │  ││    │
│  │ 配置历史版本管理                   │     │  ││    │
│  │ ★ 回滚到指定历史版本              │     │  ││    │
│                                      │     │  ││    │
│  ConfigChangeAspect★ ────────────────┘     │  ││    │
│  │ @Around("@annotation(ConfigChange)")    │  ││    │
│  │ ★ 配置变更前后的切面处理               │  ││    │
└──────────────────────────────────────────────────────────────┘
```

## 3.3 ConfigController.publishConfig() 完整源码走读

`ConfigController.publishConfig()` 是配置发布的入口方法：

**核心流程**：

```java
// ConfigController.publishConfig() (2.5.3)
@PostMapping
public Result<Boolean> publishConfig(
    @ModelAttribute ConfigForm configForm) throws NacosException {
    
    // Step 1: 参数校验
    ParamUtil.checkParam(configForm.getDataId(), configForm.getGroup(),
                       configForm.getContent(), configForm.getType());
    
    // Step 2: MD5 校验——跳过重复发布
    String md5 = MD5Utils.md5Hex(configForm.getContent());
    ConfigInfo configInfo = 
        configPersistService.findConfigInfo(configForm.getDataId(),
                                           configForm.getGroup(),
                                           configForm.getNamespaceId());
    if (configInfo != null && md5.equals(configInfo.getMd5())) {
        return Result.success(true); // 内容未变，跳过重复发布
    }
    
    // Step 3: 持久化到数据库（★ 2.5.3: PersistService 调用 persistence 模块）
    configPersistService.insertOrUpdate(configForm);
    
    // Step 4: 发布 ConfigChangeEvent 事件
    ConfigChangePublisher.notifyConfigChange(
        new ConfigDataChangeEvent(configForm.getDataId(), 
                                  configForm.getGroup(),
                                  configForm.getNamespaceId(),
                                  System.currentTimeMillis()));
    
    // Step 5: ★ 2.5.3: ConfigChangeAspect 后置处理
    // (通过 AOP 自动触发 ConfigCache 更新、ConfigEnabledFilter 检查等)
    
    // Step 6: 插入历史版本
    historyConfigInfoService.insertHistory(configForm);
    
    return Result.success(true);
}
```

## 3.4 ConfigChangePublisher：配置变更发布引擎

`ConfigChangePublisher` 负责配置变更事件分发：

| 事件 | 订阅者 | 行为 |
|------|--------|------|
| `ConfigDataChangeEvent` | `LongPollingService` | 唤醒长轮询客户端 |
| `ConfigDataChangeEvent` | `AsyncNotifyService` | 通知集群其他节点 |
| `ConfigDataChangeEvent` | `DumpService` | 配置落盘（文件备份） |
| ★ `ConfigChangeEvent` | **`ConfigChangeAspect`（2.5.3 新增）** | **AOP 切面后置处理** |

## 3.5 MySQL 持久化：persistence 模块集成

Nacos 2.5.3 的持久化层已抽离为独立的 **`persistence` 模块**（路径：`nacos-2.5.3/persistence/`），Config 模块通过 `PersistServiceIF` 接口调用：

| 持久化方式 | 实现类 | 配置 | 适用模式 |
|-----------|--------|------|---------|
| **Embedded Derby** | `EmbeddedStoragePersistServiceImpl` → `persistence` 模块 | `spring.datasource.platform=derby` | 单机模式 |
| **External MySQL** | `ExternalDataSourceServiceImpl` → `persistence` 模块 | `spring.datasource.platform=mysql` | 集群模式 |

### 2.5.3 persistence 模块配置条件注解

| 注解 | 条件 | 说明 |
|------|------|------|
| `ConditionOnEmbeddedStorage` | `platform=derby` | Derby 嵌入式存储模式 |
| `ConditionOnExternalStorage` | `platform=mysql` | MySQL 外部存储模式 |
| `ConditionStandaloneEmbedStorage` | `nacos.standalone=true` | 单机模式 |
| `ConditionDistributedEmbedStorage` | `nacos.standalone=false` | 集群分布式模式 |

**★ 2.5.3 变更**：以上4个条件注解从 `config/server/configuration/` 移到了 `persistence/configuration/condition/` 路径下。

### config_info 表结构

```sql
CREATE TABLE config_info (
    id BIGINT(64) NOT NULL AUTO_INCREMENT,
    data_id VARCHAR(255) NOT NULL,
    group_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(128) DEFAULT '' NOT NULL,
    app_name VARCHAR(128) DEFAULT NULL,
    content LONGTEXT NOT NULL,
    md5 VARCHAR(32) DEFAULT NULL,
    gmt_create DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    gmt_modified DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    src_user TEXT DEFAULT NULL,
    src_ip VARCHAR(50) DEFAULT NULL,
    c_desc VARCHAR(256) DEFAULT NULL,
    c_use VARCHAR(64) DEFAULT NULL,
    effect VARCHAR(64) DEFAULT NULL,
    type VARCHAR(64) DEFAULT NULL,
    c_schema LONGTEXT DEFAULT NULL,
    encrypted_data_key TEXT DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_configinfo_datagrouptenant(data_id, group_id, tenant_id)
);
```

## 3.6 Derby 嵌入式存储

Derby 嵌入式数据库用于单机模式，2.5.3 通过 `persistence` 模块的 `LocalDataSourceServiceImpl` 管理：

| 配置项 | 说明 |
|--------|------|
| `DerbyUtils` | Derby 工具类（路径：`persistence/utils/DerbyUtils.java`） |
| `EmbeddedStorageContextHolder` | 嵌入式存储上下文持有者 |
| `EmbeddedPaginationHelperImpl` | Derby 分页查询帮助器 |
| `StandaloneDatabaseOperateImpl` | 单机 Derby 数据库操作 |

## 3.7 LongPollingService：长轮询核心引擎

`LongPollingService`（路径：`nacos-2.5.3/config/src/main/java/com/alibaba/nacos/config/server/service/LongPollingService.java`）是配置中心的长轮询核心引擎：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `longPollingTimeout` | 29.5 秒 | 长轮询超时时间 |
| `maxLongPollingThreads` | 32 | 最大长轮询线程数 |
| `configLongPollingBatchSize` | 20 | 批量处理长轮询请求数量 |

### 长轮询核心流程

1. `ClientLongPolling` 任务加入 `allSubs` 队列
2. `LongPollingRunnable.run()` 定期扫描 `allSubs` 队列
3. 通过 MD5 对比判断配置是否变更：
   - 未变更 → 等待 29.5 秒超时后返回 304 Not Modified
   - 已变更 → 立即返回新配置内容
4. 超时后客户端立即发起下一次长轮询

## 3.8 ClientLongPolling：客户端长轮询任务

`ClientLongPolling` 代表一个客户端的长轮询请求：

```java
// ClientLongPolling (2.5.3)
class ClientLongPolling implements Runnable {
    
    final AsyncContext asyncContext;
    final String md5;          // 客户端当前 MD5
    final String probeModify;    // 探测修改标识
    
    @Override
    public void run() {
        // 检查配置是否变更
        ConfigInfo configInfo = 
            configPersistService.findConfigInfo(dataId, group, tenant);
        
        if (configInfo != null && !md5.equals(configInfo.getMd5())) {
            // 配置已变更 → 立即返回
            generateResponse(configInfo);
        }
        // 否则等待超时，返回 304 Not Modified
    }
    
    void generateResponse(ConfigInfo configInfo) {
        // 生成 JSON 响应：{"content":"...", "md5":"..."}
        HttpServletResponse response = 
            (HttpServletResponse) asyncContext.getResponse();
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(
            JSON.toJSONString(configInfo)
        );
        asyncContext.complete();
    }
}
```

## 3.9 长轮询流程图

```
客户端                            Nacos Server                       数据库
  │                                   │                               │
  │── GET /v1/cs/configs/listener ──▶│                               │
  │   (probeModify + md5)           │                               │
  │                                   │── ClientLongPolling ─────────▶│
  │                                   │   (加入 allSubs 队列)        │
  │                                   │                               │
  │         ... 等待 29.5 秒 ...      │                               │
  │                                   │                               │
  │                                   │◀── ConfigChangeEvent ─────────│
  │                                   │   (配置变更事件触发)          │
  │                                   │                               │
  │                                   │── MD5 对比 ────────────────▶│
  │                                   │   (查找 config_info 表)       │
  │                                   │                               │
  │◀── 200 OK (content + md5) ─────│                               │
  │                                   │                               │
  │── GET /v1/cs/configs/listener ──▶│                               │
  │   (带新 md5)                      │                               │
  │                        ... 下一轮循环 ...                        │
```

## 3.10 AsyncNotifyService：集群间 HTTP 异步通知

`AsyncNotifyService`（路径：`nacos-2.5.3/config/src/main/java/com/alibaba/nacos/config/server/service/notify/AsyncNotifyService.java`）负责集群间配置变更通知：

| 方法 | 说明 |
|------|------|
| `onConfigChange()` | 接收 local 配置变更事件 |
| `asyncNotify()` | 异步 HTTP POST 通知所有集群节点 |
| `handleNotify()` | 处理其他节点发来的通知 |

## 3.11 CommunicationController：集群间通知接收端点

`CommunicationController` 提供以下端点：

| HTTP 方法 | 路径 | 说明 |
|-----------|------|------|
| POST | `/v1/cs/communication/dataChange` | 集群间配置变更通知 |
| POST | `/v1/cs/communication/verify` | 集群间配置校验 |

## 3.12 配置历史版本管理

`HistoryConfigInfoService` 提供配置历史版本管理：

| 操作 | 说明 |
|------|------|
| `insertHistory()` | 插入历史版本 |
| `listHistoryConfigs()` | 查询历史版本列表 |
| `getHistoryConfigDetail()` | 查询历史版本详情 |
| `rollback()` | 回滚到指定历史版本 |
| `removeHistory()` | 删除历史版本 |

## 3.13 配置导入导出

Nacos 2.5.3 支持配置的导入导出，导出格式为 ZIP 压缩包：

```
ZIP 压缩包结构:
├── {namespaceId}/
│   ├── {group}/
│   │   ├── {dataId1}
│   │   ├── {dataId2}
│   │   └── ...
│   └── ...
└── ...
```

| API | HTTP 方法 | 路径 |
|-----|-----------|------|
| 导出 | POST | `/nacos/v1/cs/configs/export` |
| 导入 | POST | `/nacos/v1/cs/configs/import` |

## 3.14 Beta 配置发布

Beta 配置发布支持按 IP 白名单灰度发布：

| API | HTTP 方法 | 路径 |
|-----|-----------|------|
| 发布 Beta 配置 | POST | `/nacos/v1/cs/configs?beta=true` |
| 停止 Beta 配置 | DELETE | `/nacos/v1/cs/configs?beta=true` |

**2.5.3 新增**：`ConfigInfoGrayWrapper` - 灰度配置包装器，支持更灵活的灰度策略。

## 3.15 Tag 配置发布

Tag 配置发布支持按标签灰度下发配置：

| API | HTTP 方法 | 路径 |
|-----|-----------|------|
| 发布 Tag 配置 | POST | `/nacos/v1/cs/configs?tag=xxx` |
| 停止 Tag 配置 | DELETE | `/nacos/v1/cs/configs?tag=xxx` |

## 3.16 配置加密插件

Nacos 2.5.3 支持配置加密插件，默认使用 AES/GCM/NoPadding 算法：

| 配置项 | 说明 |
|--------|------|
| `nacos.config.encrypt.data-key` | 加密密钥 |
| `nacos.config.encrypt.enabled` | 是否启用加密 |

## 3.17 2.5.3 新增功能详解

### 3.17.1 ConfigCache：多级配置缓存

2.5.3 新增配置多级缓存机制（路径：`nacos-2.5.3/config/src/main/java/com/alibaba/nacos/config/server/model/ConfigCache.java`）：

| 缓存层级 | 实现类 | 说明 |
|---------|--------|------|
| L1 缓存 | `ConfigCache` | 本地内存缓存（Caffeine/Guava） |
| L2 缓存 | `ConfigCacheGray` | 灰度配置缓存 |
| 缓存工厂 | `ConfigCacheFactory` | 缓存工厂（支持多种缓存策略） |
| 后处理器 | `ConfigCachePostProcessor` | 缓存后处理器（支持自定义缓存逻辑） |

### 3.17.2 ConfigChangeAspect：配置变更切面

2.5.3 新增 `ConfigChangeAspect`（路径：`nacos-2.5.3/config/src/main/java/com/alibaba/nacos/config/server/aspect/ConfigChangeAspect.java`）：

```java
@Aspect
@Component
public class ConfigChangeAspect {
    
    /**
     * ★ 2.5.3 新增：配置变更前后的切面处理
     * 拦截所有标注 @ConfigChange 的方法
     */
    @Around("@annotation(com.alibaba.nacos.config.server.annotation.ConfigChange)")
    public Object aroundConfigChange(ProceedingJoinPoint joinPoint) 
        throws Throwable {
        // 前置处理：记录变更前状态
        Object[] args = joinPoint.getArgs();
        
        // 执行目标方法
        Object result = joinPoint.proceed();
        
        // 后置处理：
        // 1. 更新 ConfigCache 缓存
        // 2. 触发 ConfigEnabledFilter 检查
        // 3. 记录操作审计日志
        
        return result;
    }
}
```

### 3.17.3 ApiVersionEnum：API 版本枚举

2.5.3 新增 `ApiVersionEnum`（路径：`nacos-2.5.3/config/src/main/java/com/alibaba/nacos/config/server/enums/ApiVersionEnum.java`）：

```java
public enum ApiVersionEnum {
    V1("v1"),   // v1 API 版本
    V2("v2");   // v2 API 版本（2.5.3 新增）
}
```

### 3.17.4 ConfigEnabledFilter：模块启用过滤器

2.5.3 新增 `ConfigEnabledFilter`（路径：`nacos-2.5.3/config/src/main/java/com/alibaba/nacos/config/server/filter/ConfigEnabledFilter.java`），用于在 Config 模块未启用时拦截请求。

### 3.17.5 ConfigModuleStateBuilder：模块状态构建器

2.5.3 新增 `ConfigModuleStateBuilder`（路径：`nacos-2.5.3/config/src/main/java/com/alibaba/nacos/config/server/constant/ConfigModuleStateBuilder.java`），用于构建 Config 模块的健康状态信息。

---

### 本章统计数据（Config 模块 2.5.3 vs 2.2.3）

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| Java 主代码文件 | 203 | **217** | +14 |
| 存储条件注解 | 在 config 模块 | **移至 persistence 模块** | 架构调整 |
| JDBC 异常 | NJdbcException (config) | **移至 persistence 模块** | 统一异常 |
| 新增 ConfigCache | 无 | **ConfigCache/ConfigCacheFactory** | ★新增 |
| 新增 ConfigChangeAspect | 无 | **ConfigChangeAspect** | ★新增 |
| 新增 ApiVersionEnum | 无 | **ApiVersionEnum** | ★新增 |
| 新增 ConfigEnabledFilter | 无 | **ConfigEnabledFilter** | ★新增 |

---

> **本章基于 Nacos 2.5.3 源码分析生成。**
