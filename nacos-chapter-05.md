# 第5章：Nacos 2.5.3 集群管理（Core）+ 客户端 SDK 深度分析

## 5.1 Core 模块整体架构

Core 模块（路径：`nacos-2.5.3/core/`）是 Nacos 的引擎核心，负责集群管理、gRPC 通信、连接管理、命名空间管理等基础设施功能。2.5.3 版本包含 **230 个 Java 主代码文件**（不含测试），相比 2.2.3 的 168 个增加了 **62 个文件**，是变化最大的模块。

### Core 模块子包全景

| 子包 | 核心类 | 职责 | 2.5.3 变更 |
|------|--------|------|------------|
| `cluster/` | ServerMemberManager | 集群成员管理 | — |
| `cluster/health/` | AbstractModuleHealthChecker | 模块健康检查 | **★新增** |
| `cluster/lookup/` | LookupFactory、FileConfigMemberLookup | 集群寻址 | — |
| `remote/` | GrpcSdkServer、GrpcClusterServer | gRPC 通信 | — |
| `remote/grpc/` | ConnectionManager | 连接管理 | — |
| `namespace/` | Namespace、TenantInfo | 命名空间管理 | **★新增独立命名空间模型** |
| `namespace/injector/` | AbstractNamespaceDetailInjector | 命名空间详情注入 | **★新增** |
| `namespace/repository/` | NamespacePersistService | 命名空间持久化 | **★新增** |
| `control/` | NacosHttpTpsFilter | HTTP TPS 控制 | 类名变更 |
| `paramcheck/` | ParamCheckerFilter、ExtractorManager | 参数校验框架 | **★新增 ParamChecker 框架** |
| `context/` | RequestContext、RequestContextHolder | 请求上下文机制 | **★新增** |
| `ability/` | ServerAbilityControlManager | 服务能力控制 | **★新增** |
| `monitor/` | GrpcServerThreadPoolMonitor、TopNCounter | 监控指标 | **★新增** |
| `config/` | DistroModuleStateBuilder、RaftModuleStateBuilder | 模块状态构建 | **★新增** |

## 5.2 ServerMemberManager：集群成员管理器

`ServerMemberManager`（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java`）：

```java
// ServerMemberManager 核心数据结构 (2.5.3)
@Component
public class ServerMemberManager {
    
    /** 集群成员 Map<IP:Port, Member> */
    private final ConcurrentHashMap<String, Member> serverList = 
        new ConcurrentHashMap<>();
    
    /** 集群成员变化监听器 */
    private volatile List<MemberChangeListener> listeners = 
        new CopyOnWriteArrayList<>();
    
    /** ★ 2.5.3: 模块健康检查持有者 */
    @Autowired(required = false)
    private ModuleHealthCheckerHolder moduleHealthCheckerHolder;
    
    /**
     * 初始化：根据 Lookup 模式发现集群成员
     * ★ 2.5.3: 初始化完成后触发模块健康检查
     */
    @PostConstruct
    public void init() {
        // 1. 根据 LookupFactory 确定寻址模式
        MemberLookup lookup = LookupFactory.createLookup();
        // 2. 启动 Lookup 发现集群成员
        lookup.start();
        // ★ 3. 初始化模块健康检查（2.5.3 新增）
        if (moduleHealthCheckerHolder != null) {
            moduleHealthCheckerHolder.init();
        }
    }
}
```

## 5.3 LookupFactory：三种集群寻址模式

```java
// LookupFactory 集群寻址模式工厂 (2.5.3)
public class LookupFactory {
    
    public static MemberLookup createLookup() {
        String lookupType = EnvUtil.getProperty("nacos.member.lookup.type");
        
        switch (lookupType) {
            case "file":
                return new FileConfigMemberLookup();    // cluster.conf 文件
            case "address-server":
                return new AddressServerMemberLookup();  // 地址服务器 HTTP API
            default:
                return new StandaloneMemberLookup();      // 单机模式
        }
    }
}
```

## 5.4 FileConfigMemberLookup

`FileConfigMemberLookup` （路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/FileConfigMemberLookup.java`）定期读取 `cluster.conf` 文件：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `cluster.conf` | `${NACOS_HOME}/conf/cluster.conf` | 集群成员配置文件 |
| 格式 | `ip1:port1\nip2:port2\n...` | 每行一个节点 |

## 5.5 AddressServerMemberLookup

`AddressServerMemberLookup`（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/AddressServerMemberLookup.java`）定期 HTTP 查询地址服务器：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.core.member.lookup.address-server` | — | 地址服务器 URL |
| `nacos.core.member.lookup.interval` | 5 秒 | 查询间隔 |

## 5.6 StandaloneMemberLookup

单机模式使用本地 IP + 端口：

```java
@Component
public class StandaloneMemberLookup implements MemberLookup {
    
    @Override
    public void start() {
        String localIp = NetUtils.localIP();
        int port = EnvUtil.getPort();
        Member localMember = Member.builder()
            .ip(localIp)
            .port(port)
            .state(MemberState.UP)
            .build();
        afterLookup(Collections.singletonList(localMember));
    }
}
```

## 5.7 Member 模型

| 字段 | 类型 | 说明 |
|------|------|------|
| `ip` | String | 节点 IP |
| `port` | int | 节点端口 |
| `state` | MemberState | 节点状态（UP/SUSPECT/DOWN） |
| `extendInfo` | Map<String, Object> | 扩展元数据 |
| `memberAddress` | String | 成员地址（ip:port） |

## 5.8 ClusterRpcClientProxy：集群间 gRPC 通信代理

`ClusterRpcClientProxy`（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/remote/grpc/ClusterRpcClientProxy.java`）：

```java
// ClusterRpcClientProxy 集群间通信代理 (2.5.3)
@Component
public class ClusterRpcClientProxy {
    
    /** gRPC Channel 缓存 */
    private final Map<String, ManagedChannel> channelMap = 
        new ConcurrentHashMap<>();
    
    /**
     * 同步请求（单个目标节点）
     */
    public <T> T syncRequest(Member target, Request request, 
                            Class<T> responseType) {
        ManagedChannel channel = getOrCreateChannel(target);
        // 发起 gRPC 同步请求
        return GrpcUtils.syncRequest(channel, request, responseType);
    }
    
    /**
     * 广播请求（所有集群节点）
     */
    public <T> List<T> broadcast(Request request, Class<T> responseType) {
        List<T> responses = new ArrayList<>();
        for (Member member : serverMemberManager.getServerList()) {
            if (member.isAlive()) {
                T response = syncRequest(member, request, responseType);
                responses.add(response);
            }
        }
        return responses;
    }
}
```

## 5.9 NacosConfigService：配置客户端核心实现

`NacosConfigService`（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/config/NacosConfigService.java`）：

```java
// NacosConfigService 配置客户端实现 (2.5.3)
public class NacosConfigService implements ConfigService {
    
    private final ClientWorker clientWorker;
    private final ServerListManager serverListManager; // ★ 2.5.3: ConfigServerListManager
    
    @Override
    public String getConfig(String dataId, String group, long timeoutMs) 
        throws NacosException {
        
        // Step 1: 检查本地缓存快照
        String content = LocalConfigInfoProcessor.getSnapshot(
            LocalConfigInfoProcessor.getFailoverFileName(dataId, group, tenant));
        
        if (content != null) {
            return content;
        }
        
        // Step 2: 远程请求获取配置
        // ★ 2.5.3: ConfigServerListManager 获取服务端地址
        List<String> serverList = configServerListManager.getServerList();
        
        for (String serverAddr : serverList) {
            try {
                HttpResult result = httpAgent.httpGet(
                    serverAddr + "/nacos/v1/cs/configs",
                    params
                );
                if (result.code == 200) {
                    // 保存到本地缓存快照
                    LocalConfigInfoProcessor.saveSnapshot(dataId, group, tenant, 
                                                        result.content);
                    return result.content;
                }
            } catch (Exception e) {
                // 重试下一个服务器
            }
        }
        throw new NacosException(NacosException.SERVER_ERROR, 
                                 "get config failed");
    }
    
    @Override
    public void addListener(String dataId, String group, Listener listener) {
        // 注册配置变更监听器
        clientWorker.addTenantListeners(dataId, group, 
            Collections.singletonList(listener));
    }
}
```

## 5.10 ClientWorker：长轮询工作线程

`ClientWorker`（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/config/impl/ClientWorker.java`）：

```java
// ClientWorker 长轮询工作线程 (2.5.3)
public class ClientWorker implements Closeable {
    
    /** 长轮询线程池 */
    private final ScheduledExecutorService executor;
    
    /**
     * LongPollingRunnable：长轮询任务
     */
    class LongPollingRunnable implements Runnable {
        @Override
        public void run() {
            // 1. 从本地缓存中检查配置 MD5
            for (CacheData cacheData : cacheMap.values()) {
                // 2. 发起 HTTP 长轮询请求
                List<String> changedGroups = 
                    checkConfigUpdate(cacheData.dataId, 
                                     cacheData.group, 
                                     cacheData.md5);
                // 3. 处理变更响应
                if (!changedGroups.isEmpty()) {
                    // 获取新的配置内容
                    String content = getServerConfig(
                        cacheData.dataId, 
                        cacheData.group,
                        cacheData.tenant);
                    // 更新本地缓存
                    cacheData.setContent(content);
                    // 通知 Listener
                    cacheData.checkListenerMd5();
                }
            }
        }
    }
}
```

## 5.11 NacosNamingService：注册客户端核心实现

`NacosNamingService`（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/naming/NacosNamingService.java`）：

```java
// NacosNamingService 注册客户端实现 (2.5.3)
public class NacosNamingService implements NamingService {
    
    private final NamingClientProxy clientProxy;
    private final BeatReactor beatReactor;
    /** ★ 2.5.3: 故障转移数据管理 */
    private final FailoverSwitch failoverSwitch;
    private final DiskFailoverDataSource failoverDataSource;
    
    @Override
    public void registerInstance(String serviceName, 
                                 String groupName, 
                                 Instance instance) 
        throws NacosException {
        
        // Step 1: 参数校验
        checkInstanceIsLegal(instance);
        
        // Step 2: ★ 2.5.3: 生成雪花 instanceId
        if (StringUtils.isEmpty(instance.getInstanceId())) {
            instance.setInstanceId(
                InstanceIdGeneratorManager.getInstance()
                    .generateInstanceId(instance));
        }
        
        // Step 3: 注册实例
        clientProxy.registerService(serviceName, groupName, instance);
        
        // Step 4: ★ 2.5.3: 更新故障转移数据
        if (failoverSwitch.isEnabled()) {
            failoverDataSource.save(new NamingFailoverData(
                serviceName, groupName, instance));
        }
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, 
                                          String groupName,
                                          boolean healthy) 
        throws NacosException {
        // ★ 2.5.3: 优先从故障转移数据获取
        if (failoverSwitch.isEnabled()) {
            NamingFailoverData failoverData = 
                failoverDataSource.load(serviceName, groupName);
            if (failoverData != null) {
                return failoverData.getInstances();
            }
        }
        
        // 正常情况下从服务端获取
        return clientProxy.queryInstances(serviceName, groupName, 
                                         healthy);
    }
}
```

## 5.12 BeatReactor：客户端心跳引擎

`BeatReactor`（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/naming/core/BeatReactor.java`）：

```java
// BeatReactor 客户端心跳引擎 (2.5.3)
public class BeatReactor {
    
    /** BeatTask Map<serviceName#ip#port, BeatInfo> */
    private final Map<String, BeatInfo> dom2Beat = 
        new ConcurrentHashMap<>();
    
    /** 心跳线程池 */
    private final ScheduledExecutorService executorService;
    
    /**
     * 添加心跳任务
     */
    public void addBeatInfo(String serviceName, BeatInfo beatInfo) {
        String key = buildKey(serviceName, beatInfo.getIp(), 
                             beatInfo.getPort());
        BeatInfo existBeat = dom2Beat.putIfAbsent(key, beatInfo);
        if (existBeat == null) {
            // ★ 2.5.3: 动态心跳间隔支持
            long clientBeatInterval = getClientBeatInterval(beatInfo);
            executorService.scheduleAtFixedRate(
                new BeatTask(beatInfo), 
                0, 
                clientBeatInterval, 
                TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * ★ 2.5.3: 获取动态心跳间隔
     */
    private long getClientBeatInterval(BeatInfo beatInfo) {
        // 优先使用服务端返回的心跳间隔
        Long serverInterval = beatInfo.getScheduledHeartBeatInterval();
        return serverInterval != null ? serverInterval : 5000L;
    }
}
```

## 5.13 本地缓存快照机制

`LocalConfigInfoProcessor`（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/config/impl/LocalConfigInfoProcessor.java`）：

| 方法 | 说明 |
|------|------|
| `saveSnapshot()` | 保存配置快照到本地文件 |
| `getSnapshot()` | 从本地文件读取配置快照 |
| `cleanSnapshot()` | 清理本地快照文件 |

快照文件路径：`${user.home}/nacos/config/{tenant_id}/{group}/{dataId}`

## 5.14 2.5.3 新增 Core 模块功能详解

### 5.14.1 Namespace：命名空间模型

2.5.3 新增独立的命名空间模型（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/namespace/`）：

| 类 | 说明 |
|----|------|
| `Namespace` | 命名空间实体 |
| `TenantInfo` | 租户信息 |
| `NamespaceTypeEnum` | 命名空间类型枚举（GLOBAL/CUSTOM） |
| `NamespaceForm` | 命名空间表单 |
| `NamespacePersistService` | 命名空间持久化服务接口 |
| `EmbeddedNamespacePersistServiceImpl` | Derby 嵌入式实现 |
| `ExternalNamespacePersistServiceImpl` | MySQL 外部存储实现 |

### 5.14.2 AbstractModuleHealthChecker：模块健康检查

2.5.3 新增模块级健康检查机制（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/cluster/health/`）：

| 类 | 说明 |
|----|------|
| `AbstractModuleHealthChecker` | 模块健康检查抽象基类 |
| `ModuleHealthCheckerHolder` | 模块健康检查持有者 |
| `ReadinessResult` | 就绪检查结果 |

### 5.14.3 RequestContext：请求上下文机制

2.5.3 新增请求上下文机制（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/context/`）：

| 类 | 说明 |
|----|------|
| `RequestContext` | 请求上下文（ThreadLocal 实现） |
| `RequestContextHolder` | 请求上下文持有者 |
| `AddressContext` | 地址上下文 |
| `AuthContext` | 认证上下文 |
| `BasicContext` | 基础上下文 |
| `EngineContext` | 引擎上下文 |
| `HttpRequestContextConfig` | HTTP 请求上下文配置 |
| `HttpRequestContextFilter` | HTTP 请求上下文过滤器 |

### 5.14.4 ServerListProvider：地址服务提供者抽象

2.5.3 新增地址服务提供者抽象（路径：`nacos-2.5.3/client/src/main/java/com/alibaba/nacos/client/address/`）：

| 类 | 说明 |
|----|------|
| `AbstractServerListManager` | 抽象服务端列表管理器 |
| `AbstractServerListProvider` | 抽象服务端列表提供者 |
| `ServerListProvider` | 服务端列表提供者接口 |
| `EndpointServerListProvider` | Endpoint 模式列表提供者 |
| `PropertiesListProvider` | Properties 文件模式列表提供者 |
| `ConfigServerListManager` | Config 模块服务端列表管理器 |
| `NamingServerListManager` | Naming 模块服务端列表管理器 |

### 5.14.5 TopNCounter：TopN 监控计数器

2.5.3 新增 TopN 监控计数器基础设施（路径：`nacos-2.5.3/core/src/main/java/com/alibaba/nacos/core/monitor/topn/`）：

| 类 | 说明 |
|----|------|
| `BaseTopNCounter` | TopN 计数器抽象基类 |
| `StringTopNCounter` | 字符串 TopN 计数器 |
| `FixedSizePriorityQueue` | 固定大小优先队列 |
| `TopNConfig` | TopN 配置 |

---

### 本章统计数据（Core 模块 2.5.3 vs 2.2.3）

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| Java 主代码文件 | 168 | **230** | **+62（最大变化模块）** |
| 命名空间模型 | 分散实现 | **独立 Namespace/TenantInfo 模型** | ★新增 |
| 健康检查 | 无模块级健康检查 | **AbstractModuleHealthChecker** | ★新增 |
| 请求上下文 | 无 | **RequestContext 机制** | ★新增 |
| 参数校验框架 | 无 | **ParamCheckerFilter** | ★新增 |
| ServerListManager | ServerListManager（全局） | **ConfigServerListManager + NamingServerListManager** | ★按模块拆分 |

---

> **本章基于 Nacos 2.5.3 源码分析生成。**
