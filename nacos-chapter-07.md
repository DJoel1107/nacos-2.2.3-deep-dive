# 第7章：控制台与系统管理 + Istio/CMDB/Address模块

## 7.1 Console 控制台后端 API

### 7.1.1 ConsoleController——控制台核心Controller

```java
// ConsoleController.java - 控制台后端核心 API
@RestController
@RequestMapping("/v1/console")
public class ConsoleController {
    
    @Autowired
    private UserService userService;
    @Autowired
    private RoleService roleService;
    @Autowired
    private PermissionService permissionService;
    @Autowired
    private NamespaceService namespaceService;
    @Autowired
    private HealthCheckService healthCheckService;
    
    // ==================== 用户管理 ====================
    
    /**
     * 创建用户
     */
    @PostMapping("/users")
    public Boolean createUser(@RequestBody UserForm userForm) {
        return userService.createUser(userForm.getUsername(), 
            userForm.getPassword());
    }
    
    /**
     * 获取用户列表
     */
    @GetMapping("/users")
    public Page<User> getUsers(@RequestParam int pageNo, 
            @RequestParam int pageSize) {
        return userService.getUsers(pageNo, pageSize);
    }
    
    /**
     * 删除用户
     */
    @DeleteMapping("/users")
    public Boolean deleteUser(@RequestParam String username) {
        return userService.deleteUser(username);
    }
    
    /**
     * 更新用户密码
     */
    @PutMapping("/users/password")
    public Boolean updateUserPassword(@RequestBody UserForm userForm) {
        return userService.updatePassword(userForm.getUsername(), 
            userForm.getPassword());
    }
    
    // ==================== 用户登录 ====================
    
    /**
     * 用户登录
     */
    @PostMapping("/auth/login")
    public Object login(@RequestParam String username, 
            @RequestParam String password) {
        if (!authPluginService.validateIdentity(username, password)) {
            throw new NacosException(NacosException.INVALID_USERNAME_PASSWORD,
                "username or password is wrong");
        }
        
        String accessToken = authPluginService.generateAccessToken(username);
        
        JSONObject result = new JSONObject();
        result.put("accessToken", accessToken);
        result.put("tokenTTL", tokenExpireSeconds);
        result.put("globalAdmin", isGlobalAdmin(username));
        return result;
    }
    
    private boolean isGlobalAdmin(String username) {
        List<String> roles = authPluginService.getRoles(username);
        return roles.contains("ROLE_ADMIN");
    }
    
    // ==================== 命名空间管理 ====================
    
    /**
     * 获取命名空间列表
     */
    @GetMapping("/namespaces")
    public List<Namespace> getNamespaces() {
        return namespaceService.getNamespaceList();
    }
    
    /**
     * 创建命名空间
     */
    @PostMapping("/namespaces")
    public Boolean createNamespace(@RequestBody NamespaceForm namespaceForm) {
        return namespaceService.createNamespace(
            namespaceForm.getNamespaceName(),
            namespaceForm.getNamespaceDesc());
    }
    
    /**
     * 删除命名空间
     */
    @DeleteMapping("/namespaces")
    public Boolean deleteNamespace(@RequestParam String namespaceId) {
        return namespaceService.deleteNamespace(namespaceId);
    }
    
    /**
     * 修改命名空间
     */
    @PutMapping("/namespaces")
    public Boolean updateNamespace(@RequestBody NamespaceForm namespaceForm) {
        return namespaceService.updateNamespace(
            namespaceForm.getNamespaceId(),
            namespaceForm.getNamespaceName(),
            namespaceForm.getNamespaceDesc());
    }
    
    // ==================== 角色管理 ====================
    
    @GetMapping("/roles")
    public List<RoleInfo> getRoles(@RequestParam String username) {
        return roleService.getRoles(username);
    }
    
    @PostMapping("/roles")
    public Boolean addRole(@RequestParam String username, 
            @RequestParam String role) {
        return roleService.addRole(username, role);
    }
    
    @DeleteMapping("/roles")
    public Boolean deleteRole(@RequestParam String username, 
            @RequestParam String role) {
        return roleService.deleteRole(username, role);
    }
    
    // ==================== 权限管理 ====================
    
    @GetMapping("/permissions")
    public List<PermissionInfo> getPermissions(@RequestParam String role) {
        return permissionService.getPermissions(role);
    }
    
    @PostMapping("/permissions")
    public Boolean addPermission(@RequestParam String role, 
            @RequestParam String resource, @RequestParam String action) {
        return permissionService.addPermission(role, resource, action);
    }
    
    @DeleteMapping("/permissions")
    public Boolean deletePermission(@RequestParam String role, 
            @RequestParam String resource, @RequestParam String action) {
        return permissionService.deletePermission(role, resource, action);
    }
}
```

## 7.2 系统管理（sys 模块）

### 7.2.1 系统健康检查

```java
// HealthController.java - 系统健康检查 API
@RestController
@RequestMapping("/v1/console")
public class HealthController {
    
    @Autowired
    private ServerMemberManager serverMemberManager;
    
    /**
     * 检查 Nacos 服务器健康状态
     */
    @GetMapping("/health")
    public Object health() {
        JSONObject result = new JSONObject();
        
        // 集群状态
        JSONObject clusterStatus = new JSONObject();
        clusterStatus.put("leaderStatus", getLeaderStatus());
        clusterStatus.put("memberCount", 
            serverMemberManager.getServerList().size());
        clusterStatus.put("healthyMemberCount", 
            getHealthyMemberCount());
        result.put("clusterStatus", clusterStatus);
        
        // 模块状态
        JSONObject moduleStatus = new JSONObject();
        moduleStatus.put("naming", "UP");
        moduleStatus.put("config", "UP");
        moduleStatus.put("console", "UP");
        result.put("moduleStatus", moduleStatus);
        
        // 数据源状态
        JSONObject dataSourceStatus = new JSONObject();
        dataSourceStatus.put("dataSourceType", 
            EnvUtil.getProperty("spring.datasource.platform"));
        dataSourceStatus.put("dataSourceHealth", checkDataSourceHealth());
        result.put("dataSourceStatus", dataSourceStatus);
        
        // 基本信息
        result.put("version", VersionUtils.version);
        result.put("startTime", EnvUtil.getStartTime());
        result.put("serverAddr", EnvUtil.getLocalAddress());
        
        return result;
    }
    
    /**
     * 获取 Leader 状态
     */
    @GetMapping("/health/metrics")
    public Object metrics() {
        JSONObject result = new JSONObject();
        result.put("cpuUsage", getCpuUsage());
        result.put("memoryUsage", getMemoryUsage());
        result.put("diskUsage", getDiskUsage());
        result.put("networkIn", getNetworkIn());
        result.put("networkOut", getNetworkOut());
        return result;
    }
}
```

## 7.3 Istio 集成模块

### 7.3.1 IstioServiceEntryRegistry

Nacos 2.2.3 支持与 Istio Service Mesh 集成，通过在 Kubernetes 环境中将 Nacos 注册的服务自动同步为 Istio ServiceEntry：

```java
// IstioServiceEntryRegistry.java - Istio ServiceEntry 注册器
@Component
@ConditionalOnProperty(name = "nacos.istio.enabled", havingValue = "true")
public class IstioServiceEntryRegistry {
    
    @Autowired
    private ServiceManager serviceManager;
    
    // Istio MCP (Mesh Configuration Protocol) 客户端
    private MCPClient mcpClient;
    
    @PostConstruct
    public void init() {
        // 1. 连接 Istio Pilot MCP Server
        String istioPilotAddress = EnvUtil.getProperty(
            "nacos.istio.mcp.server.addr", "istio-pilot.istio-system:15010");
        mcpClient = new MCPClient(istioPilotAddress);
        mcpClient.connect();
        
        // 2. 启动服务同步任务
        GlobalExecutor.scheduleByFixedRate(() -> {
            syncServicesToIstio();
        }, INITIAL_DELAY, SYNC_PERIOD, TimeUnit.SECONDS);
    }
    
    /**
     * 同步 Nacos 服务到 Istio ServiceEntry
     */
    private void syncServicesToIstio() {
        Set<String> istioDomains = getIstioDomains();
        
        for (Map.Entry<String, Map<String, Service>> namespaceEntry : 
                serviceManager.getServiceMap().entrySet()) {
            String namespace = namespaceEntry.getKey();
            
            for (Map.Entry<String, Service> serviceEntry : 
                    namespaceEntry.getValue().entrySet()) {
                String serviceName = serviceEntry.getKey();
                Service service = serviceEntry.getValue();
                
                String domain = serviceName + "." + namespace + ".nacos";
                
                // 检查是否需要更新
                if (needUpdateIstioService(domain, service)) {
                    // 注册 ServiceEntry
                    ServiceEntry serviceEntryConfig = buildServiceEntry(
                        domain, service);
                    mcpClient.push(serviceEntryConfig);
                }
            }
        }
    }
    
    /**
     * 构建 Istio ServiceEntry 配置
     */
    private ServiceEntry buildServiceEntry(String domain, Service service) {
        ServiceEntry.Builder builder = ServiceEntry.newBuilder()
            .setHost(domain)
            .setResolution(ServiceEntry.Resolution.STATIC);
        
        // 添加实例 Endpoints
        for (Instance instance : service.allIPs()) {
            Port port = Port.newBuilder()
                .setNumber(instance.getPort())
                .setName("http")
                .setProtocol("HTTP")
                .build();
            
            Endpoint endpoint = Endpoint.newBuilder()
                .setAddress(instance.getIp())
                .putLabels("weight", 
                    String.valueOf(instance.getWeight()))
                .build();
            
            WorkloadEntry workloadEntry = WorkloadEntry.newBuilder()
                .setAddress(instance.getIp())
                .putLabels("weight", 
                    String.valueOf(instance.getWeight()))
                .build();
        }
        
        return builder.build();
    }
}
```

## 7.4 CMDB 标签数据管理

### 7.4.1 CmdbService

CMDB 模块用于管理实例的标签数据，支持按机房/地域等维度进行标签过滤：

```java
// CmdbService.java - CMDB 标签管理服务
@Component
public class CmdbService {
    
    @Autowired
    private CmdbMapper cmdbMapper;
    
    /**
     * 获取实例的标签列表
     */
    public Map<String, String> getInstanceLabels(String ip) {
        CmdbInstance cmdbInstance = cmdbMapper.selectByIp(ip);
        if (cmdbInstance == null) {
            return Collections.emptyMap();
        }
        return cmdbInstance.getLabels();
    }
    
    /**
     * 更新实例标签
     */
    public void updateInstanceLabels(String ip, Map<String, String> labels) {
        CmdbInstance cmdbInstance = cmdbMapper.selectByIp(ip);
        if (cmdbInstance == null) {
            cmdbInstance = new CmdbInstance();
            cmdbInstance.setIp(ip);
            cmdbInstance.setLabels(labels);
            cmdbMapper.insert(cmdbInstance);
        } else {
            cmdbInstance.getLabels().putAll(labels);
            cmdbMapper.update(cmdbInstance);
        }
    }
    
    /**
     * 根据标签匹配实例
     */
    public List<String> queryByLabels(Map<String, String> queryLabels) {
        return cmdbMapper.selectByLabels(queryLabels);
    }
}
```

## 7.5 Address 地址服务器模块

### 7.5.1 AddressServer——地址服务器模式

Address Server 是 Nacos 集群的一种地址寻址模式，通过中心化的地址服务器来管理集群节点列表：

```java
// AddressServer.java - 地址服务器主类
@SpringBootApplication
public class AddressServer {
    public static void main(String[] args) {
        SpringApplication.run(AddressServer.class, args);
    }
}

// AddressServerController.java - 地址服务器 API
@RestController
@RequestMapping("/nacos")
public class AddressServerController {
    
    @Autowired
    private ServerMemberManager serverMemberManager;
    
    /**
     * 获取 Nacos 集群节点列表
     * GET /nacos/serverlist
     */
    @GetMapping("/serverlist")
    public JSONObject getClusterNodes() {
        List<Member> serverList = serverMemberManager.getServerList();
        
        JSONObject result = new JSONObject();
        result.put("count", serverList.size());
        
        JSONArray servers = new JSONArray();
        for (Member member : serverList) {
            if (member.getState() == Member.State.UP) {
                JSONObject server = new JSONObject();
                server.put("ip", member.getIp());
                server.put("port", member.getPort());
                servers.add(server);
            }
        }
        result.put("servers", servers);
        return result;
    }
}
```

### 7.5.2 Address Server 部署架构

```
                      ┌─────────────────────┐
                      │   Address Server    │
                      │   (独立部署)        │
                      └──────────┬──────────┘
                                 │
                    GET /nacos/serverlist
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
          ▼                      ▼                      ▼
   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
   │  Nacos     │    │  Nacos     │    │  Nacos     │
   │  Node 1    │    │  Node 2    │    │  Node 3    │
   └─────────────┘    └─────────────┘    └─────────────┘
```

**配置方式**：
```properties
# Nacos 集群节点的配置
# 指定使用地址服务器模式
nacos.core.member.lookup.type=addressServer

# 地址服务器 URL
nacos.core.member.lookup.address=http://address-server:8080/nacos
```

**优势**：
- 集中管理集群节点列表，避免维护 cluster.conf 文件
- 节点上下线自动感知，无需手动修改配置
- 支持动态扩容缩容

**缺点**：
- 地址服务器成为单点依赖（需要部署多个 Address Server 保证高可用）

---

*（第七章完，约 0.9 万字）*
