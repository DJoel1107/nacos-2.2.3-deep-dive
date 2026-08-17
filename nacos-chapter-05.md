# 第5章：集群管理 (Core) 与客户端 SDK 深度分析

## 5.1 Core 模块架构概述

Core 模块（224文件/23,359行）是 Nacos 集群管理的核心，负责集群成员发现、节点健康管理、集群间通信和认证拦截。

### 5.1.1 核心类关系图

```
ServerMemberManager (集群成员管理器——核心)
    │
    ├── LookupFactory (集群寻址工厂)
    │   ├── FileConfigMemberLookup (配置文件寻址)
    │   ├── AddressServerMemberLookup (地址服务器寻址)
    │   └── StandaloneMemberLookup (单机模式)
    │
    ├── Member (集群节点模型)
    │   ├── ip / port
    │   ├── state (UP/DOWN/SUSPECTED)
    │   └── extendInfo (扩展元数据)
    │
    ├── ClusterRpcClientProxy (集群间 gRPC 通信代理)
    │   └── GrpcClusterServer (集群 gRPC 服务端)
    │
    └── ServerHealthMonitor (节点健康监控)
```

## 5.2 ServerMemberManager——集群成员管理器

### 5.2.1 核心数据结构

```java
@Component(value = "serverMemberManager")
public class ServerMemberManager 
        implements ApplicationListener<WebServerInitializedEvent> {
    
    // 集群成员列表
    private volatile List<Member> serverList = new CopyOnWriteArrayList<>();
    
    // 当前节点信息
    private Member self;
    
    // 集群寻址服务
    private MemberLookup lookup;
    
    // 节点信息同步定时任务
    private ScheduledExecutorService memberInfoSyncExecutor;
    
    /**
     * 初始化
     */
    @PostConstruct
    public void init() {
        // 1. 初始化集群寻址模式
        initAndStartLookup();
        
        // 2. 启动节点信息同步任务（每 2 秒一次）
        memberInfoSyncExecutor = Executors.newSingleThreadScheduledExecutor(
            new NameThreadFactory("nacos.member.sync"));
        memberInfoSyncExecutor.scheduleAtFixedRate(
            this::syncMemberInfo, 2000L, 2000L, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 初始化并启动集群寻址模式
     */
    private void initAndStartLookup() {
        // 通过 LookupFactory 创建寻址实例
        lookup = LookupFactory.createLookup(this);
        lookup.start();
    }
    
    /**
     * 更新集群成员列表
     */
    public boolean memberChange(Collection<Member> members) {
        synchronized (serverList) {
            // 计算成员变更差异
            List<Member> newMembers = new ArrayList<>(members);
            List<Member> oldMembers = new ArrayList<>(serverList);
            
            // 新增成员
            for (Member member : newMembers) {
                if (!oldMembers.contains(member)) {
                    memberJoin(member);
                }
            }
            
            // 离开成员
            for (Member member : oldMembers) {
                if (!newMembers.contains(member)) {
                    memberLeave(member);
                }
            }
            
            // 更新本地成员列表
            serverList = new CopyOnWriteArrayList<>(newMembers);
            
            // 通知事件
            NotifyCenter.publishEvent(new MemberChangeEvent(oldMembers, newMembers));
            return true;
        }
    }
    
    /**
     * 检查节点健康状态
     */
    public boolean isUnHealth(String address) {
        for (Member member : serverList) {
            if (member.getAddress().equals(address)) {
                return member.getState() == Member.State.SUSPECTED 
                    || member.getState() == Member.State.DOWN;
            }
        }
        return false;
    }
}
```

## 5.3 LookupFactory——集群寻址模式

Nacos 支持三种集群寻址模式，通过 `LookupFactory` 生产对应的 `MemberLookup` 实现：

### 5.3.1 配置文件寻址（FileConfigMemberLookup）

```java
// FileConfigMemberLookup.java - 配置文件寻址
@Service
@ConditionalOnMissingBean(MemberLookup.class)
public class FileConfigMemberLookup extends AbstractMemberLookup {
    
    // 集群成员配置文件路径
    private static final String CLUSTER_CONF_PATH = 
        EnvUtil.getNacosHome() + "/conf/cluster.conf";
    
    @Override
    public void doStart() {
        // 1. 定期读取 cluster.conf 文件
        ScheduledExecutorService fileReader = Executors.newSingleThreadScheduledExecutor(
            new NameThreadFactory("nacos.cluster.file.reader"));
        
        fileReader.scheduleAtFixedRate(() -> {
            try {
                List<Member> members = readClusterConf();
                if (!members.isEmpty()) {
                    afterLookup(members);
                }
            } catch (Exception e) {
                Loggers.CORE.error("Failed to read cluster.conf", e);
            }
        }, 0L, 5000L, TimeUnit.MILLISECONDS);
    }
    
    private List<Member> readClusterConf() throws IOException {
        List<Member> members = new ArrayList<>();
        File file = new File(CLUSTER_CONF_PATH);
        if (!file.exists()) {
            Loggers.CORE.warn("cluster.conf not found at: {}", CLUSTER_CONF_PATH);
            return members;
        }
        
        List<String> lines = Files.readAllLines(file.toPath());
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            
            // cluster.conf 每行格式：ip:port
            String[] address = line.split(":");
            if (address.length == 1) {
                // 没有指定端口，使用默认端口 8848
                Member member = new Member();
                member.setIp(address[0].trim());
                member.setPort(DEFAULT_PORT);
                member.setState(Member.State.UP);
                members.add(member);
            } else if (address.length == 2) {
                Member member = new Member();
                member.setIp(address[0].trim());
                member.setPort(Integer.parseInt(address[1].trim()));
                member.setState(Member.State.UP);
                members.add(member);
            }
        }
        return members;
    }
}
```

### 5.3.2 地址服务器寻址（AddressServerMemberLookup）

```java
// AddressServerMemberLookup.java - 地址服务器寻址
@Service
@ConditionalOnProperty(name = "nacos.core.member.lookup.type", 
    havingValue = "addressServer")
public class AddressServerMemberLookup extends AbstractMemberLookup {
    
    @Value("${nacos.core.member.lookup.address:}")
    private String addressServerUrl;
    
    @Override
    public void doStart() {
        // 定期向地址服务器查询集群节点列表
        GlobalExecutor.scheduleByFixedRate(() -> {
            try {
                String addressServerUrl = EnvUtil.getProperty(
                    "nacos.core.member.lookup.address");
                
                // GET http://address-server/nacos/serverlist
                Result<String> result = HttpClientBeanHolder.getNacosRestTemplate()
                    .get(addressServerUrl + "/nacos/serverlist", 
                        Header.EMPTY, Query.EMPTY, String.class);
                
                if (result.isOk()) {
                    List<Member> members = parseServerList(result.getData());
                    afterLookup(members);
                }
            } catch (Exception e) {
                Loggers.CORE.error("Failed to lookup from address server", e);
            }
        }, 0L, 5000L, TimeUnit.MILLISECONDS);
    }
    
    private List<Member> parseServerList(String json) {
        List<Member> members = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(json);
        JSONArray servers = jsonObject.getJSONArray("servers");
        for (int i = 0; i < servers.size(); i++) {
            JSONObject server = servers.getJSONObject(i);
            Member member = new Member();
            member.setIp(server.getString("ip"));
            member.setPort(server.getIntValue("port"));
            member.setState(Member.State.UP);
            members.add(member);
        }
        return members;
    }
}
```

**这就是为什么之前截图报错 `UnknownHostException: jmevv.tbsite.net` 的原因：Nacos 配置了 `nacos.core.member.lookup.address=http://jmevv.tbsite.net`，而这个地址服务器域名无法解析。**

### 5.3.3 单机模式寻址（StandaloneMemberLookup）

```java
// StandaloneMemberLookup.java - 单机模式寻址
@Service
@ConditionalOnProperty(name = "nacos.standalone", havingValue = "true")
public class StandaloneMemberLookup extends AbstractMemberLookup {
    
    @Override
    public void doStart() {
        // 1. 仅自己作为唯一成员
        Member self = new Member();
        self.setIp(NetUtils.getLocalIP());
        self.setPort(EnvUtil.getPort());
        self.setState(Member.State.UP);
        
        List<Member> members = new ArrayList<>();
        members.add(self);
        
        afterLookup(members);
    }
}
```

## 5.4 集群间 gRPC 通信

### 5.4.1 ClusterRpcClientProxy——集群间RPC代理

```java
// ClusterRpcClientProxy.java - 集群间 gRPC 通信代理
@Component
public class ClusterRpcClientProxy {
    
    // 集群节点到 gRPC Channel 的映射
    private final Map<String, GrpcConnection> connections = 
        new ConcurrentHashMap<>();
    
    /**
     * 同步请求集群其他节点
     */
    public <T> T request(Member member, Request request, long timeoutMs) 
            throws NacosException {
        try {
            // 1. 获取或创建 gRPC Channel
            GrpcConnection connection = getOrCreateConnection(member);
            
            // 2. 发起同步请求
            Response response = connection.request(request, timeoutMs);
            
            if (response.getResultCode() != ResponseCode.SUCCESS) {
                throw new NacosException(response.getErrorCode(), 
                    response.getMessage());
            }
            
            return (T) response.getBody();
        } catch (Exception e) {
            // 标记节点为 SUSPECTED 状态
            serverMemberManager.updateMemberState(member, 
                Member.State.SUSPECTED);
            throw new NacosException(NacosException.SERVER_ERROR, 
                "Cluster RPC failed: " + e.getMessage());
        }
    }
    
    /**
     * 异步请求集群其他节点
     */
    public void asyncRequest(Member member, Request request, 
            Callback<Response> callback) {
        GrpcConnection connection = getOrCreateConnection(member);
        connection.request(request, callback);
    }
    
    /**
     * 获取或创建到指定节点的 gRPC 连接
     */
    private GrpcConnection getOrCreateConnection(Member member) {
        String serverAddress = member.getAddress() + ":" + 
            (EnvUtil.getPort() + Constants.CLUSTER_GRPC_PORT_OFFSET);
        
        return connections.computeIfAbsent(serverAddress, addr -> {
            // 创建 gRPC Channel
            ManagedChannel channel = ManagedChannelBuilder
                .forTarget(addr)
                .usePlaintext()
                .maxInboundMessageSize(10 * 1024 * 1024) // 10MB
                .keepAliveTime(7200, TimeUnit.SECONDS)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .permitKeepAliveTime(300, TimeUnit.SECONDS)
                .build();
            return new GrpcConnection(channel);
        });
    }
    
    /**
     * 获取所有健康节点连接用于广播
     */
    public void broadcast(Request request) {
        List<Member> healthyMembers = serverMemberManager.getServerList()
            .stream()
            .filter(m -> m.getState() == Member.State.UP)
            .filter(m -> !m.getAddress().equals(selfAddress))
            .collect(Collectors.toList());
        
        for (Member member : healthyMembers) {
            asyncRequest(member, request, new Callback<Response>() {
                @Override
                public void onReceive(Result<Response> result) {
                    if (!result.isOk()) {
                        Loggers.CORE.warn("Broadcast failed to {}: {}", 
                            member.getAddress(), result.getMessage());
                    }
                }
            });
        }
    }
}
```

## 5.5 客户端 SDK 深度分析

客户端模块（199 文件/25,266 行）提供 Java SDK，实现配置获取和服务发现功能。

### 5.5.1 NacosConfigService——配置客户端

```java
// NacosConfigService.java - 配置客户端核心实现
public class NacosConfigService implements ConfigService {
    
    private final ClientWorker clientWorker;
    private final String namespace;
    private final ConfigFilterChainManager configFilterChainManager;
    
    public NacosConfigService(Properties properties) {
        // 1. 初始化配置
        String serverAddr = properties.getProperty(PropertyKeyConst.SERVER_ADDR);
        namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        
        // 2. 创建 ClientWorker（负责长轮询）
        clientWorker = new ClientWorker(this, serverAddr, properties);
        
        // 3. 创建配置过滤器链（JM/Local Config Filter）
        configFilterChainManager = new ConfigFilterChainManager(properties);
    }
    
    @Override
    public String getConfig(String dataId, String group, long timeoutMs) 
            throws NacosException {
        // 1. 参数校验
        ParamUtils.checkKeyParam(dataId, group);
        
        // 2. 先查本地缓存快照
        String content = LocalConfigInfoProcessor.getSnapshot(namespace, 
            dataId, group);
        if (content != null) {
            return content;
        }
        
        // 3. 远程拉取配置
        String[] serverList = serverListManager.getServerList();
        for (String server : serverList) {
            try {
                content = clientWorker.getServerConfig(dataId, group, 
                    server, timeoutMs);
                if (content != null) {
                    // 更新本地缓存快照
                    LocalConfigInfoProcessor.saveSnapshot(namespace, 
                        dataId, group, content);
                    return content;
                }
            } catch (NacosException e) {
                LogUtils.NAMING_LOGGER.warn(
                    "Failed to get config from server: {}", server, e);
            }
        }
        
        throw new NacosException(NacosException.SERVER_ERROR, 
            "Failed to get config after all servers tried");
    }
    
    @Override
    public void addListener(String dataId, String group, Listener listener) {
        // 1. 注册监听器
        clientWorker.addListener(dataId, group, 
            namespace, listener);
        
        // 2. 发送配置订阅请求（建立长轮询）
        clientWorker.addTenantListeners(dataId, group, namespace);
    }
}
```

### 5.5.2 ClientWorker——长轮询工作线程

```java
// ClientWorker.java - 客户端长轮询工作线程
public class ClientWorker implements Closeable {
    
    // 长轮询线程池（单线程）
    private final ScheduledExecutorService executor;
    
    // 长轮询检查周期（默认 10ms）
    private final int longPollingInterval = 10;
    
    // CacheData Map（按 dataId + group + namespace 聚合）
    private final AtomicReference<Map<String, CacheData>> cacheMap = 
        new AtomicReference<>(new HashMap<>());
    
    public ClientWorker(NacosConfigService configService, 
            String serverAddr, Properties properties) {
        this.executor = Executors.newScheduledThreadPool(1,
            new NameThreadFactory("com.alibaba.nacos.client.config.worker"));
        
        // 启动长轮询线程
        this.executor.scheduleAtFixedRate(new LongPollingRunnable(),
            longPollingInterval, longPollingInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 长轮询检查线程
     */
    class LongPollingRunnable implements Runnable {
        @Override
        public void run() {
            try {
                // 1. 检查本地配置缓存
                checkLocalConfig();
                
                // 2. 向 Nacos 服务端发起长轮询
                checkServerConfig();
            } catch (Throwable e) {
                LogUtils.NAMING_LOGGER.error("[runnable-error]", e);
            }
        }
    }
    
    /**
     * 长轮询的核心逻辑
     */
    void checkServerConfig() {
        // 1. 收集所有需要监听的数据（dataId + group + namespace）
        List<CacheData> cacheDatas = new ArrayList<>(cacheMap.get().values());
        
        // 2. 按 namespace 聚合，逐个发起长轮询
        Map<String, List<CacheData>> grouped = groupByTenant(cacheDatas);
        for (Map.Entry<String, List<CacheData>> entry : grouped.entrySet()) {
            String tenant = entry.getKey();
            List<String> listenStr = entry.getValue().stream()
                .map(CacheData::buildListenerKey)
                .collect(Collectors.toList());
            
            // 3. 发起长轮询请求（POST /v1/cs/configs/listeners）
            HttpResult result = httpAgent.httpPost(
                getConfigListenerPath(), listenStr, tenant, 
                Constants.LONG_POLLING_TIME_OUT);
            
            // 4. 处理长轮询返回结果
            if (result.isSuccess()) {
                String changedData = result.getContent();
                if (!StringUtils.isEmpty(changedData)) {
                    // 有配置变更，拉取最新配置
                    List<String> changedList = Arrays.asList(
                        changedData.split("%01"));
                    for (String str : changedList) {
                        String[] data = str.split("%02");
                        if (isChanged(data)) {
                            // 获取最新配置内容
                            String content = getServerConfig(
                                data[0],  // dataId
                                data[1],  // group
                                tenant
                            );
                            // 更新本地缓存并通知监听器
                            CacheData cache = cacheMap.get().get(
                                GroupKey.getKeyTenant(data[0], data[1], tenant));
                            cache.setContent(content);
                            cache.checkListenerMd5();
                        }
                    }
                }
            }
        }
        
        // 5. 定时执行下一次长轮询
        executor.schedule(this, longPollingInterval, TimeUnit.MILLISECONDS);
    }
}
```

### 5.5.3 NacosNamingService——注册客户端

```java
// NacosNamingService.java - 注册客户端核心实现
public class NacosNamingService implements NamingService {
    
    private final NamingClientProxy clientProxy;
    private final BeatReactor beatReactor;
    private final HostReactor hostReactor;
    
    public NacosNamingService(Properties properties) {
        this.clientProxy = new NamingClientProxy(properties);
        this.beatReactor = new BeatReactor(clientProxy, properties);
        this.hostReactor = new HostReactor(clientProxy, properties);
    }
    
    @Override
    public void registerInstance(String serviceName, String groupName, 
            Instance instance) throws NacosException {
        // 1. 通过 gRPC 注册实例
        clientProxy.registerService(serviceName, groupName, instance);
        
        // 2. 启动心跳定时任务
        beatReactor.addBeatInfo(serviceName, groupName, instance);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName) 
            throws NacosException {
        return hostReactor.getServiceInfo(serviceName, groupName).getHosts();
    }
    
    @Override
    public void subscribe(String serviceName, String groupName, 
            EventListener listener) throws NacosException {
        hostReactor.subscribe(serviceName, groupName, 
            StringUtils.defaultIfEmpty(clusters, " "), listener);
    }
}
```

### 5.5.4 BeatReactor——心跳反应器

```java
// BeatReactor.java - 客户端心跳引擎
public class BeatReactor implements Closeable {
    
    // 心跳线程池
    private final ScheduledExecutorService executor;
    
    // 心跳间隔（默认 5 秒）
    private final long beatInterval = 5000L;
    
    // BeatInfo Map（按 serviceName + ip + port 聚合）
    private final Map<String, BeatInfo> domMap = new ConcurrentHashMap<>();
    
    /**
     * 添加心跳实例
     */
    public void addBeatInfo(String serviceName, String groupName, 
            Instance instance) {
        String key = buildKey(serviceName, instance.getIp(), 
            instance.getPort());
        
        BeatInfo beatInfo = new BeatInfo();
        beatInfo.setServiceName(serviceName);
        beatInfo.setGroupName(groupName);
        beatInfo.setIp(instance.getIp());
        beatInfo.setPort(instance.getPort());
        beatInfo.setWeight(instance.getWeight());
        beatInfo.setScheduled(false);
        
        domMap.put(key, beatInfo);
        
        // 启动定时心跳任务
        executor.schedule(new BeatTask(beatInfo), beatInterval, 
            TimeUnit.MILLISECONDS);
    }
    
    /**
     * 心跳任务
     */
    class BeatTask implements Runnable {
        private final BeatInfo beatInfo;
        
        @Override
        public void run() {
            if (beatInfo.isStopped()) {
                return;
            }
            
            long nextBeatInterval = beatInterval;
            try {
                // 发送心跳请求（通过 gRPC）
                BeatRequest request = new BeatRequest();
                request.setBeatInfo(beatInfo);
                request.setServiceName(beatInfo.getServiceName());
                
                BeatResponse response = clientProxy.sendBeat(request);
                
                if (response.getClientBeatInterval() > 0) {
                    nextBeatInterval = response.getClientBeatInterval();
                }
            } catch (Exception e) {
                LogUtils.NAMING_LOGGER.error("[CLIENT-BEAT] failed", e);
            }
            
            // 调度下一次心跳
            executor.schedule(this, nextBeatInterval, TimeUnit.MILLISECONDS);
        }
    }
}
```

### 5.5.5 本地缓存快照机制

```java
// LocalConfigInfoProcessor.java - 本地配置缓存快照
public class LocalConfigInfoProcessor {
    
    // 本地缓存快照目录
    static private final String LOCAL_SNAPSHOT_PATH = 
        System.getProperty("user.home") + File.separator + 
        "nacos" + File.separator + "config";
    
    /**
     * 保存配置快照到本地文件
     */
    public static void saveSnapshot(String namespace, String dataId, 
            String group, String content) {
        try {
            File file = getSnapshotFile(namespace, dataId, group);
            Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LogUtils.NAMING_LOGGER.error("Failed to save snapshot", e);
        }
    }
    
    /**
     * 从本地快照加载配置
     */
    public static String getSnapshot(String namespace, String dataId, 
            String group) {
        try {
            File file = getSnapshotFile(namespace, dataId, group);
            if (!file.exists()) {
                return null;
            }
            return new String(Files.readAllBytes(file.toPath()), 
                StandardCharsets.UTF_8);
        } catch (IOException e) {
            LogUtils.NAMING_LOGGER.error("Failed to get snapshot", e);
            return null;
        }
    }
    
    private static File getSnapshotFile(String namespace, String dataId, 
            String group) {
        String fileName = namespace + Constants.FILE_SEPARATOR + 
            group + Constants.FILE_SEPARATOR + dataId;
        return new File(LOCAL_SNAPSHOT_PATH, fileName);
    }
}
```

## 5.6 公共组件与 SPI 扩展机制（Common 模块）

### 5.6.1 SPI 机制设计

Nacos 的插件体系基于 Java SPI（Service Provider Interface）机制，核心类为 `NacosServiceLoader`：

```java
// NacosServiceLoader.java - SPI 服务加载器
public class NacosServiceLoader {
    
    /**
     * 加载指定接口的所有 SPI 实现
     */
    public static <T> Collection<T> load(Class<T> serviceClass) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(
            serviceClass, NacosServiceLoader.class.getClassLoader());
        
        List<T> results = new ArrayList<>();
        for (T service : serviceLoader) {
            results.add(service);
        }
        
        // 按 @Order 注解排序
        results.sort((o1, o2) -> {
            Order order1 = o1.getClass().getAnnotation(Order.class);
            Order order2 = o2.getClass().getAnnotation(Order.class);
            int i1 = order1 == null ? 0 : order1.value();
            int i2 = order2 == null ? 0 : order2.value();
            return Integer.compare(i1, i2);
        });
        
        return results;
    }
}
```

### 5.6.2 NotifyCenter——事件通知中心

```java
// NotifyCenter.java - 事件通知中心
public class NotifyCenter {
    
    // 事件发布者注册表
    private static final Map<String, EventPublisher> publisherMap = 
        new ConcurrentHashMap<>();
    
    // 事件订阅者注册表
    private static final Map<String, List<Subscriber>> subscriberMap = 
        new ConcurrentHashMap<>();
    
    // 是否关闭
    private static volatile AtomicBoolean closed = new AtomicBoolean(false);
    
    /**
     * 注册事件订阅者
     */
    public static void registerSubscriber(Subscriber consumer) {
        String topic = consumer.subscribeType().getClass().getCanonicalName();
        List<Subscriber> subscribers = subscriberMap.computeIfAbsent(
            topic, k -> new CopyOnWriteArrayList<>());
        subscribers.add(consumer);
    }
    
    /**
     * 发布事件
     */
    public static boolean publishEvent(Event event) {
        if (closed.get()) {
            return false;
        }
        
        // 1. 通过 EventPublisher 异步发布事件
        String topic = event.getClass().getCanonicalName();
        EventPublisher publisher = publisherMap.get(topic);
        if (publisher != null) {
            return publisher.publish(event);
        }
        
        // 2. 同步通知订阅者
        List<Subscriber> subscribers = subscriberMap.get(topic);
        if (subscribers != null) {
            for (Subscriber subscriber : subscribers) {
                subscriber.onEvent(event);
            }
        }
        return true;
    }
    
    /**
     * 注册事件发布者
     */
    public static void registerPublisher(EventPublisher publisher) {
        String topic = publisher.getTopic();
        publisherMap.put(topic, publisher);
    }
    
    /**
     * 关闭通知中心
     */
    public static void shutdown() {
        closed.set(true);
        publisherMap.clear();
        subscriberMap.clear();
    }
}
```

---

*（第五章完，约 2.4 万字）*
