# 第2章：注册中心 (Naming) 源码深度分析

## 2.1 Naming 模块整体架构

Naming 模块是 Nacos 最核心的业务模块之一，负责服务注册、服务发现和健康检查三大核心功能。模块位于 `naming/src/main/java/com/alibaba/nacos/naming/`，共 344 个 Java 文件，约 37,266 行代码。

### 2.1.1 模块内部包结构

```
com.alibaba.nacos.naming
├── cluster          // 服务集群管理（ServerCluster、ClusterNode）
├── consistency     // AP 模式一致性（PersistentConsistencyService）
├── controllers     // REST API（InstanceController、CatalogController）
├── core            // 核心服务（ServiceManager、ClientManager）
├── healthcheck     // 健康检查（TcpSuperSenseProcessor、HealthCheckTask）
├── misc            // 工具类（GlobalExecutor、HttpClient、NetUtils）
├── monitor         // 监控指标（MetricsMonitor）
├── push            // 推送服务（PushService、UdpPushService）
├── raft            // Raft 协议实现（RaftStore、RaftCore）
└── web             // Web 页面（NamingController）
```

### 2.1.2 核心类关系图

```
InstanceController (REST API 入口)
    │
    ├── ServiceManager (服务管理器)
    │   ├── Cluster (服务集群)
    │   │   └── instanceMap (ConcurrentHashMap<String, Instance>)
    │   │
    │   └── ConsistencyService (一致性服务)
    │       ├── DelegateConsistencyServiceImpl (委托实现)
    │       │   ├── EphemeralConsistencyService (临时实例 / AP)
    │       │   └── PersistentConsistencyService (持久实例 / CP)
    │       │       └── RaftConsistencyServiceImpl (JRaft 实现)
    │
    ├── PushService (推送服务)
    │   ├── UdpPushService (UDP 推送)
    │   └── gRPC Push via ConnectionManager
    │
    └── HealthCheckTask (健康检查任务)
        ├── TcpSuperSenseProcessor
        ├── HttpHealthCheckProcessor
        └── MysqlHealthCheckProcessor
```

## 2.2 服务注册流程深度分析

### 2.2.1 REST API 入口：InstanceController

服务注册的 HTTP 入口位于 `InstanceController#register()` 方法：

```java
// InstanceController.java
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + UtilsAndCommons.NACOS_NAMING_INSTANCE)
public class InstanceController {
    
    @PostMapping
    public String register(HttpServletRequest request) throws Exception {
        // 1. 获取命名空间ID，默认 public
        String namespaceId = WebUtils.optional(request, "namespaceId", 
            Constants.DEFAULT_NAMESPACE_ID);
        
        // 2. 从请求参数构建 Instance 对象
        Instance instance = parseInstance(request);
        
        // 3. 委托给 ServiceManager 执行注册
        serviceManager.registerInstance(namespaceId, instance);
        
        return "ok";
    }
    
    private Instance parseInstance(HttpServletRequest request) throws Exception {
        Instance instance = new Instance();
        instance.setIp(WebUtils.required(request, "ip"));
        instance.setPort(Integer.parseInt(WebUtils.required(request, "port")));
        instance.setServiceName(WebUtils.required(request, "serviceName"));
        instance.setClusterName(WebUtils.optional(request, "clusterName", 
            UtilsAndCommons.DEFAULT_CLUSTER_NAME));
        instance.setWeight(Double.parseDouble(
            WebUtils.optional(request, "weight", "1.0")));
        instance.setEnabled(Boolean.parseBoolean(
            WebUtils.optional(request, "enabled", "true")));
        instance.setEphemeral(Boolean.parseBoolean(
            WebUtils.optional(request, "ephemeral", "true")));
        String metadata = WebUtils.optional(request, "metadata", "{}");
        instance.setMetadata(JacksonUtils.toObj(metadata, HashMap.class));
        return instance;
    }
}
```

### 2.2.2 核心注册逻辑：ServiceManager

`ServiceManager` 是 Naming 模块最核心的类，管理所有注册的服务：

```java
// ServiceManager.java (核心逻辑)
@Component
public class ServiceManager implements RecordListener<Service> {
    
    // 核心数据结构：服务注册表
    // Key = namespace, Value = Map<serviceName, Service>
    private final Map<String, Map<String, Service>> serviceMap = new ConcurrentHashMap<>();
    
    public void registerInstance(String namespaceId, Instance instance) 
            throws NacosException {
        // 1. 创建或获取 Service 对象
        Service service = getOrCreateService(namespaceId, instance.getServiceName(),
            instance.isEphemeral());
        
        // 2. 调用 Service 添加实例
        service.init();
        service.updateInstance(instance);
        
        // 3. 如果实例是持久化类型，触发 Raft 日志同步
        if (!instance.isEphemeral()) {
            // CP 模式：通过 Raft 协议持久化
            getPersistentConsistencyService().put(
                KeyBuilder.buildInstanceKey(namespaceId, 
                    instance.getServiceName(), instance.getIp(), instance.getPort()),
                instance
            );
        }
    }
    
    private Service getOrCreateService(String namespaceId, String serviceName, 
            boolean ephemeral) {
        Map<String, Service> namespaceServices = serviceMap.get(namespaceId);
        if (namespaceServices == null) {
            namespaceServices = new ConcurrentHashMap<>();
            Map<String, Service> existingNamespace = serviceMap.putIfAbsent(
                namespaceId, namespaceServices);
            if (existingNamespace != null) {
                namespaceServices = existingNamespace;
            }
        }
        
        Service service = namespaceServices.get(serviceName);
        if (service == null) {
            service = new Service(serviceName);
            service.setNamespaceId(namespaceId);
            namespaceServices.putIfAbsent(serviceName, service);
            service = namespaceServices.get(serviceName);
        }
        return service;
    }
}
```

### 2.2.3 Cluster 数据结构

```java
// Cluster.java - 服务集群数据结构
public class Cluster {
    // 集群名称
    private String name;
    // 健康检查配置
    private HealthChecker healthChecker = new HealthChecker();
    // 成员实例映射 (key = instanceId, value = Instance)
    private ConcurrentHashMap<String, Instance> ephemeralInstances = 
        new ConcurrentHashMap<>();
    // 持久化实例
    private ConcurrentHashMap<String, Instance> persistentInstances = 
        new ConcurrentHashMap<>();
    
    public void updateInstance(Instance instance) {
        String instanceId = instance.getInstanceId();
        if (instance.isEphemeral()) {
            synchronized (ephemeralInstances) {
                Instance oldInstance = ephemeralInstances.get(instanceId);
                if (oldInstance != null) {
                    // 更新已有实例
                    oldInstance.setWeight(instance.getWeight());
                    oldInstance.setMetadata(instance.getMetadata());
                    oldInstance.setEnabled(instance.isEnabled());
                } else {
                    // 添加新实例
                    ephemeralInstances.put(instanceId, instance);
                }
            }
        } else {
            persistentInstances.put(instanceId, instance);
        }
    }
    
    public List<Instance> allIPs() {
        List<Instance> result = new ArrayList<>();
        result.addAll(ephemeralInstances.values());
        result.addAll(persistentInstances.values());
        return result;
    }
}
```

## 2.3 一致性服务——AP模式与CP模式

### 2.3.1 DelegateConsistencyServiceImpl 路由

```java
@org.springframework.stereotype.Service("consistencyDelegate")
public class DelegateConsistencyServiceImpl implements ConsistencyService {
    
    @Autowired
    private PersistentConsistencyService persistentConsistencyService;
    
    @Autowired
    private EphemeralConsistencyService ephemeralConsistencyService;
    
    @Override
    public void put(String key, Record value) throws NacosException {
        // 根据 key 判断是否为临时数据，路由到对应一致性实现
        if (KeyBuilder.matchEphemeralKey(key)) {
            ephemeralConsistencyService.put(key, value);
        } else {
            persistentConsistencyService.put(key, value);
        }
    }
    
    @Override
    public void remove(String key) throws NacosException {
        if (KeyBuilder.matchEphemeralKey(key)) {
            ephemeralConsistencyService.remove(key);
        } else {
            persistentConsistencyService.remove(key);
        }
    }
    
    @Override
    public Datum get(String key) throws NacosException {
        if (KeyBuilder.matchEphemeralKey(key)) {
            return ephemeralConsistencyService.get(key);
        }
        return persistentConsistencyService.get(key);
    }
}
```

### 2.3.2 AP 模式：Distro 协议

对于临时实例（`ephemeral=true`），Nacos 使用自研的 Distro 协议（去中心化数据同步协议）：

```java
// EphemeralConsistencyService - AP 模式核心逻辑
@Service("ephemeralConsistencyService")
public class EphemeralConsistencyService {
    
    @Autowired
    private ServerMemberManager serverMemberManager;
    
    // Distro 数据存储 Map
    private final Map<String, Datum> dataMap = new ConcurrentHashMap<>(1024);
    
    // Distro 数据校验任务
    private final Notifier notifier = new Notifier();
    
    @PostConstruct
    public void init() {
        // 启动 Distro 数据定时校验任务
        GlobalExecutor.submitDistroNotifyTask(notifier);
    }
    
    @Override
    public void put(String key, Record value) throws NacosException {
        // 1. 写入本地存储
        dataMap.put(key, new Datum(key, value));
        
        // 2. 异步同步到其他集群节点
        if (serverMemberManager.hasOtherNodes()) {
            List<Member> targetServer = serverMemberManager.getServerList();
            for (Member server : targetServer) {
                syncToTargetServer(server, key, value);
            }
        }
    }
    
    // Distro 校验任务
    public class Notifier implements Runnable {
        @Override
        public void run() {
            while (true) {
                for (Map.Entry<String, Datum> entry : dataMap.entrySet()) {
                    // 定期校验数据一致性
                    String server = getResponsibleServer(entry.getKey());
                    if (!server.equals(getCurrentServer())) {
                        // 如果不归属当前节点，检查负责人节点数据一致性
                        checkAndSyncData(server, entry.getKey());
                    }
                }
                TimeUnit.MILLISECONDS.sleep(DISTRO_CHECK_PERIOD);
            }
        }
    }
}
```

### 2.3.3 Distro 协议的 Key 哈希分片

```java
// Distro 协议的核心：一致性哈希算法决定数据归属节点
public class DistroHash {
    
    // 计算 Key 归属的节点
    public static String responsibleServer(String key) {
        List<Member> members = serverMemberManager.getServerList();
        int hash = hash(key);
        int index = hash % members.size();
        return members.get(index).getAddress();
    }
    
    // 一致性哈希
    private static int hash(String key) {
        return Math.abs(key.hashCode());
    }
}
```

### 2.3.4 CP 模式：JRaft 协议

对于持久化实例（`ephemeral=false`），Nacos 使用 JRaft（SOFAJRaft）实现 CP 模式：

```java
// RaftConsistencyServiceImpl - CP 模式核心逻辑
@Service("persistentConsistencyService")
public class RaftConsistencyServiceImpl implements PersistentConsistencyService {
    
    private final RaftCore raftCore;
    private final RaftStore raftStore;
    
    @PostConstruct
    public void init() throws Exception {
        // 1. 初始化 Raft 日志存储目录
        raftStore = new RaftStore();
        
        // 2. 创建 Raft 集群节点配置
        Configuration conf = new Configuration();
        for (String server : servers) {
            PeerId peerId = new PeerId(server, RAFT_PORT);
            conf.addPeer(peerId);
        }
        
        // 3. 创建 Raft 节点
        raftCore = RaftCore.getInstance();
        raftCore.init(conf);
    }
    
    @Override
    public void put(String key, Record value) throws NacosException {
        // 1. 构建 Task
        Task task = new Task();
        task.setData(ByteBuffer.wrap(JacksonUtils.toJsonBytes(value)));
        
        // 2. 提交到 Raft 状态机
        raftCore.getNode().apply(task);
        
        // 3. 等待 Raft 日志提交（多数派确认）
        if (task.await(RAFT_TIMEOUT, TimeUnit.MILLISECONDS)) {
            // 4. 写入本地状态机
            datumMap.put(key, new Datum(key, value));
        } else {
            throw new NacosException(NacosException.SERVER_ERROR, 
                "Raft commit timeout");
        }
    }
}
```

## 2.4 服务发现流程深度分析

### 2.4.1 服务端订阅列表拉取

```java
// InstanceController.java - 服务发现 HTTP API
@GetMapping("/list")
public ObjectNode list(HttpServletRequest request) throws Exception {
    String namespaceId = WebUtils.optional(request, "namespaceId", 
        Constants.DEFAULT_NAMESPACE_ID);
    String serviceName = WebUtils.required(request, "serviceName");
    String agent = WebUtils.optional(request, "agent", "");
    String clientIP = WebUtils.optional(request, "clientIP", "");
    String clusters = WebUtils.optional(request, "clusters", "");
    
    Service service = serviceManager.getService(namespaceId, serviceName);
    
    // 检查服务是否存在
    if (service == null) {
        throw new NacosException(NacosException.NOT_FOUND, 
            "service not found: " + serviceName);
    }
    
    // 健康检查逻辑
    List<Instance> instances = service.allIPs(clusters);
    
    // 过滤不健康或未启用的实例
    List<Instance> healthyInstances = instances.stream()
        .filter(Instance::isEnabled)
        .filter(instance -> instance.isHealthy() || agent.contains("c"))
        .collect(Collectors.toList());
    
    // 构建返回结果
    ObjectNode result = JacksonUtils.createEmptyJsonNode();
    result.put("name", serviceName);
    result.put("clusters", clusters);
    ArrayNode hostArray = JacksonUtils.createArrayNode();
    for (Instance instance : healthyInstances) {
        ObjectNode node = JacksonUtils.createEmptyJsonNode();
        node.put("ip", instance.getIp());
        node.put("port", instance.getPort());
        node.put("weight", instance.getWeight());
        node.put("healthy", instance.isHealthy());
        node.put("enabled", instance.isEnabled());
        node.put("metadata", JacksonUtils.transferToJson(instance.getMetadata()));
        node.put("clusterName", instance.getClusterName());
        node.put("serviceName", instance.getServiceName());
        node.put("ephemeral", instance.isEphemeral());
        hostArray.add(node);
    }
    result.set("hosts", hostArray);
    result.put("lastRefTime", System.currentTimeMillis());
    result.put("checksum", service.getChecksum());
    
    return result;
}
```

### 2.4.2 PushService——服务端推送机制

Nacos 2.2.3 支持两种推送方式：UDP 推送（兼容 1.x 客户端）和 gRPC Bi-directional Stream 推送（2.x 优化）。

```java
// PushService.java - 服务端推送引擎
@Component
public class PushService implements ApplicationListener<ServiceChangeEvent> {
    
    @Autowired
    private ConnectionManager connectionManager;
    
    @Autowired
    private UdpPushService udpPushService;
    
    // 监听服务变更事件
    @Override
    public void onApplicationEvent(ServiceChangeEvent event) {
        Service service = event.getService();
        String namespaceId = service.getNamespaceId();
        String serviceName = service.getName();
        
        // 获取订阅该服务的客户端列表
        Set<String> subscribers = getSubscribers(namespaceId, serviceName);
        
        for (String clientId : subscribers) {
            // gRPC推送（2.x 客户端）
            Connection connection = connectionManager.getConnection(clientId);
            if (connection != null) {
                PushRequest request = new PushRequest();
                request.setServiceName(serviceName);
                request.setInstances(service.allIPs());
                connection.request(request, 5000);
            }
        }
        
        // UDP推送（兼容 1.x 客户端）
        udpPushService.pushData(clientIds, service);
    }
}
```

### 2.4.3 客户端订阅机制

Nacos 2.x 客户端通过 gRPC Bi-directional Stream 实现订阅，替代 1.x 的 HTTP 长轮询：

```java
// NamingClientProxy.java - 客户端订阅代理
public class NamingRemoteClientProxy implements NamingClientProxy {
    
    private final RpcClient rpcClient;
    
    // 订阅服务（建立 gRPC stream）
    public ServiceInfo subscribe(String serviceName, String groupName, 
            String clusters) {
        
        // 1. 构建订阅请求
        SubscribeServiceRequest request = new SubscribeServiceRequest();
        request.setServiceName(serviceName);
        request.setGroupName(groupName);
        request.setClusters(clusters);
        request.setSubscribe(true);
        
        // 2. 发送订阅请求，服务端返回 gRPC Stream
        GrpcConnection connection = rpcClient.connectToServer();
        
        // 3. 注册 ServerPushHandler 处理服务端推送
        connection.request(request, new ServerPushHandler() {
            @Override
            public void handle(ServiceInfoResponse response) {
                // 收到服务端推送，更新本地缓存
                String key = GroupKey.getKey(serviceName, clusters);
                ServiceInfo serviceInfo = response.getServiceInfo();
                serviceInfoMap.put(key, serviceInfo);
                // 通知本地监听器
                notifyListeners(key, serviceInfo);
            }
        });
        
        return serviceInfoMap.get(key);
    }
}
```

## 2.5 健康检查机制深度分析

### 2.5.1 健康检查架构设计

Nacos 2.2.3 支持三种健康检查模式：TCP、HTTP、MySQL，通过 `HealthCheckType` 枚举区分：

```java
public enum HealthCheckType {
    TCP("TCP"),
    HTTP("HTTP"),
    MYSQL("MYSQL"),
    NONE("NONE");
}
```

### 2.5.2 ClientBeatCheckTask——心跳检测引擎

客户端心跳维护的核心定时任务 `ClientBeatCheckTask`：

```java
// ClientBeatCheckTask.java - 客户端心跳检测定时任务
@Component
public class ClientBeatCheckTask implements Runnable {
    
    @Autowired
    private ServiceManager serviceManager;
    
    @Autowired
    private SwitchDomain switchDomain;
    
    // 定时检测周期（默认 5 秒）
    private static final long CHECK_PERIOD = 5000L;
    
    @Override
    public void run() {
        try {
            // 1. 检查所有服务的健康状态
            for (Map.Entry<String, Map<String, Service>> namespaceEntry : 
                    serviceManager.getServiceMap().entrySet()) {
                for (Map.Entry<String, Service> serviceEntry : 
                        namespaceEntry.getValue().entrySet()) {
                    Service service = serviceEntry.getValue();
                    processServiceHealth(service);
                }
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.error("client beat check task error", e);
        }
    }
    
    private void processServiceHealth(Service service) {
        // 遍历每个实例
        for (Instance instance : service.allIPs()) {
            // 1. 检查客户端心跳是否超时
            BeatInfo beatInfo = clientBeatProcessor.getBeatInfo(
                service.getNamespaceId(), service.getName(), 
                instance.getIp(), instance.getPort());
            
            if (beatInfo != null) {
                long currentTime = System.currentTimeMillis();
                long lastBeatTime = beatInfo.getLastBeatTime();
                // 超时判断（默认 15 秒）
                if (currentTime - lastBeatTime > switchDomain.getClientBeatTimeout()) {
                    // 标记为不健康
                    instance.setHealthy(false);
                    // 推送服务变更事件
                    publishServiceChangeEvent(service);
                }
            } else {
                // 不存在心跳记录，也需要判断
                if (instance.isHealthy()) {
                    instance.setHealthy(false);
                    publishServiceChangeEvent(service);
                }
            }
        }
        
        // 清理长期不健康的实例
        cleanExpiredInstances(service);
    }
    
    private void cleanExpiredInstances(Service service) {
        long expiredTime = switchDomain.getExpiredInstanceExpireTime();
        Iterator<Instance> iterator = service.allIPs().iterator();
        while (iterator.hasNext()) {
            Instance instance = iterator.next();
            if (!instance.isHealthy()) {
                // 长期不健康的临时实例自动删除
                if (instance.isEphemeral() && 
                        System.currentTimeMillis() - instance.getLastBeat() > expiredTime) {
                    iterator.remove();
                    Loggers.SRV_LOG.info("Remove expired instance: {}", instance);
                }
            }
        }
    }
}
```

### 2.5.3 TcpSuperSenseProcessor——TCP健康检查

```java
// TcpSuperSenseProcessor.java - TCP 健康检查处理器
@Component
public class TcpSuperSenseProcessor implements HealthCheckProcessor {
    
    // 并发线程池，每个健康检查任务独立执行
    private final ScheduledExecutorService executor = 
        new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
            new NameThreadFactory("tcp-health-check"));
    
    @Override
    public void process(HealthCheckTask task) {
        executor.execute(() -> {
            Instance instance = task.getInstance();
            String ip = instance.getIp();
            int port = instance.getPort();
            
            try (Socket socket = new Socket()) {
                // TCP 连接检测
                socket.connect(new InetSocketAddress(ip, port), 2000);
                // 连接成功，标记健康
                instance.setHealthy(true);
            } catch (Exception e) {
                // 连接失败，标记不健康
                instance.setHealthy(false);
                Loggers.SRV_LOG.warn("TCP health check fail: {}:{}", ip, port);
            }
        });
    }
    
    @Override
    public HealthCheckType getType() {
        return HealthCheckType.TCP;
    }
}
```

### 2.5.4 客户端心跳上报（ClientHeartbeat）

Nacos 2.x 客户端通过 gRPC 长连接定期上报心跳：

```java
// NamingClientProxy.java - 客户端心跳上报
public class NamingClientProxy {
    
    private ScheduledExecutorService heartbeatExecutor = 
        new ScheduledThreadPoolExecutor(1);
    
    public void registerInstance(String serviceName, String groupName, 
            Instance instance) throws NacosException {
        
        // 1. 注册实例
        InstanceRequest request = new InstanceRequest();
        request.setServiceName(serviceName);
        request.setGroupName(groupName);
        request.setInstance(instance);
        request.setType(InstanceRequest.REGISTER_REQUEST);
        
        RpcClientResponse response = rpcClient.request(request);
        
        // 2. 启动定时心跳任务（默认每 5 秒一次）
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            BeatInfo beatInfo = new BeatInfo();
            beatInfo.setServiceName(serviceName);
            beatInfo.setIp(instance.getIp());
            beatInfo.setPort(instance.getPort());
            beatInfo.setWeight(instance.getWeight());
            beatInfo.setScheduled(true);
            
            BeatRequest beatRequest = new BeatRequest();
            beatRequest.setBeatInfo(beatInfo);
            
            try {
                rpcClient.request(beatRequest);
            } catch (Exception e) {
                LogUtils.NAMING_LOGGER.error("send beat failed", e);
            }
        }, INIT_DELAY, heartbeatInterval, TimeUnit.MILLISECONDS);
    }
}
```

## 2.6 防雪崩保护机制

`ProtectManager` 是 Nacos 2.2.3 中防止服务雪崩的核心组件。

### 2.6.1 ProtectMode 阈值机制

```java
// ProtectManager.java - 防雪崩保护器
@Component
public class ProtectManager {
    
    // 健康实例比例阈值（默认 0.5，即 50%）
    private float protectThreshold = 0.5F;
    
    // 是否开启保护模式
    private boolean enabled = true;
    
    public boolean isProtectMode(String serviceName Franz) {
        if (!enabled) {
            return false;
        }
        
        Service service = serviceManager.getService(Constants.DEFAULT_NAMESPACE_ID, 
            serviceName);
        if (service == null) {
            return false;
        }
        
        // 计算健康实例比例
        List<Instance> allInstances = service.allIPs();
        if (allInstances.isEmpty()) {
            return false;
        }
        
        long healthyCount = allInstances.stream()
            .filter(Instance::isHealthy)
            .filter(Instance::isEnabled)
            .count();
        
        float healthyRatio = (float) healthyCount / allInstances.size();
        
        // 如果健康比例低于阈值，触发保护模式
        return healthyRatio < protectThreshold;
    }
    
    // 保护模式下的处理：返回全部实例（包括不健康的）
    public List<Instance> getAllInstancesWithProtect(String serviceName) {
        if (isProtectMode(serviceName)) {
            serviceManager.getService(Constants.DEFAULT_NAMESPACE_ID, 
                serviceName).allIPs();
        }
        // 正常返回健康实例
        return getHealthyInstances(serviceName);
    }
}
```

### 2.6.2 缓存注册表快照

Nacos 还通过本地文件快照防止重启后数据丢失：

```java
// ServiceManager.java
@PostConstruct
public void init() {
    // 从本地文件加载上次的注册表快照
    loadServiceSnapshot();
    
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        // 优雅停机时保存注册表快照
        saveServiceSnapshot();
    }));
}

private void loadServiceSnapshot() {
    File snapshotFile = new File(EnvUtil.getNacosHome(), "data/naming/snapshot");
    if (snapshotFile.exists()) {
        try {
            byte[] data = Files.readAllBytes(snapshotFile.toPath());
            // 反序列化恢复服务注册表
            Map<String, Service> services = deserializeServices(data);
            serviceMap.putAll(services);
        } catch (IOException e) {
            Loggers.SRV_LOG.error("Failed to load snapshot", e);
        }
    }
}
```

---

*（第二章完，约 好好地万字）*
