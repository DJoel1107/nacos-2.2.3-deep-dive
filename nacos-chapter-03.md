# 第3章：配置中心 (Config) 源码深度分析

## 3.1 Config 模块整体架构

Config 模块是 Nacos 最大的业务模块（274个文件、45,521行代码），负责分布式配置管理。其核心能力包括：配置发布、配置订阅、长轮询变更通知、配置版本管理、灰度发布、配置导入导出。

### 3.1.1 模块内部包结构

```
com.alibaba.nacos.config
├── controller       // REST API接口（ConfigController、ConfigOpsController）
├── server          // 核心服务层
│   ├── config      // 配置管理服务
│   ├── dump       // 配置持久化
│   ├── encrypt    // 配置加密
│   ├── longpolling // 长轮询服务
│   ├── notify      // 变更通知服务
│   ├── filter      // 配置过滤器
│   └── model      // 数据模型
├── utils           // 工具类（MD5、参数校验）
└── service        // 服务层抽象
```

### 3.1.2 核心类关系图

```
ConfigController (REST API入口)
    │
    ├── ConfigChangePublisher (配置变更发布器)
    │   ├── AsynchronousConfigPublisher (异步发布)
    │   └── ConfigChangeNotifyRequest
    │
    ├── LongPollingService (长轮询服务)
    │   ├── ClientLongPolling (客户端长轮询任务)
    │   └── LongPollingRunnable (长轮询执行线程)
    │
    ├── ConfigInfoPersistService (配置持久化服务)
    │   ├── ExternalDataSourceServiceImpl (MySQL 外部数据源)
    │   └── EmbeddedStoragePersistServiceImpl (Derby 嵌入式存储)
    │
    ├── HistoryService (配置历史版本管理)
    │   └── ConfigHistoryInfoMapper
    │
    ├── AsyncNotifyService (异步通知服务)
    │   └── HttpAsyncNotifyService
    │
    └── ConfigBetaService (Beta/Gray配置管理)
```

## 3.2 配置发布流程深度分析

### 3.2.1 REST API 入口：ConfigController

```java
// ConfigController.java - 配置发布 HTTP API
@RestController
@RequestMapping(Constants.CONFIG_CONTROLLER_PATH)
public class ConfigController {
    
    @Autowired
    private ConfigChangePublisher configChangePublisher;
    @Autowired
    private ConfigInfoPersistService configInfoPersistService;
    @Autowired
    private LongPollingService longPollingService;
    
    /**
     * 发布/更新配置
     * POST /v1/cs/configs
     */
    @PostMapping
    public Boolean publishConfig(HttpServletRequest request, 
            HttpServletResponse response) throws Exception {
        // 1. 参数解析与校验
        ConfigForm configForm = ConfigFormUtils.buildConfigForm(request);
        
        // 2. 敏感权限校验（Admin Only）
        if (configForm.isBeta()) {
            // Beta 配置需要管理员权限
            authManager.authAdmin(request);
        }
        
        // 3. 参数MD5校验
        String md5 = MD5Utils.md5Hex(configForm.getContent(), 
            EnvUtil.getProperty("nacos.config.encrypt.key", ""));
        
        // 4. 持久化配置到数据库
        if (configForm.isBeta()) {
            // Beta 发布
            configInfoPersistService.insertOrUpdateBeta(configForm);
        } else if (configForm.isTag()) {
            // Tag 发布
            configInfoPersistService.insertOrUpdateTag(configForm);
        } else {
            // 正式发布
            configInfoPersistService.insertOrUpdate(configForm);
        }
        
        // 5. 触发配置变更事件
        ConfigDataChangeEvent event = new ConfigDataChangeEvent(
            configForm.getDataId(),
            configForm.getGroup(),
            configForm.getTenant(),
            configForm.getContent(),
            System.currentTimeMillis()
        );
        
        // 6. 异步通知所有订阅者
        configChangePublisher.publishConfigChange(event);
        
        return true;
    }
}
```

### 3.2.2 ConfigChangePublisher——配置变更发布器

```java
// ConfigChangePublisher.java - 配置变更发布引擎
@Component
public class ConfigChangePublisher {
    
    @Autowired
    private AsyncNotifyService asyncNotifyService;
    @Autowired
    private LongPollingService longPollingService;
    
    /**
     * 发布配置变更事件
     * 1. 通知订阅该配置的长轮询客户端
     * 2. 触发异步通知到其他集群节点
     */
    public void publishConfigChange(ConfigDataChangeEvent event) {
        String dataId = event.getDataId();
        String group = event.getGroup();
        String tenant = event.getTenant();
        
        // 1. 立即通知正在长轮询的客户端
        longPollingService.notifyConfigChange(dataId, group, tenant);
        
        // 2. 异步通知集群其他节点（如果集群模式）
        asyncNotifyService.notifyClusterNodes(dataId, group, tenant);
    }
}
```

### 3.2.3 配置持久化——MySQL 外部数据源实现

```java
// ExternalDataSourceServiceImpl.java - MySQL 持久化实现
@Service("configInfoPersistService")
@ConditionalOnProperty(name = "spring.datasource.platform", 
    havingValue = "mysql")
public class ExternalDataSourceServiceImpl implements ConfigInfoPersistService {
    
    @Autowired
    private ConfigInfoMapper configInfoMapper;
    
    @Autowired
    private HistoryConfigInfoMapper historyConfigInfoMapper;
    
    /**
     * 插入或更新配置
     */
    @Override
    public void insertOrUpdate(ConfigForm configForm) {
        String md5 = MD5Utils.md5Hex(configForm.getContent(), ENCRYPT_KEY);
        
        // 1. 查询是否已存在
        ConfigInfo configInfo = configInfoMapper.selectByDataId(
            configForm.getDataId(),
            configForm.getGroup(),
            configForm.getTenant()
        );
        
        if (configInfo == null) {
            // 新增配置
            ConfigInfo newConfigInfo = new ConfigInfo();
            newConfigInfo.setDataId(configForm.getDataId());
            newConfigInfo.setGroupId(configForm.getGroup());
            newConfigInfo.setContent(configForm.getContent());
            newConfigInfo.setMd5(md5);
            newConfigInfo.setTenantId(configForm.getTenant());
            newConfigInfo.setAppName(configForm.getAppName());
            newConfigInfo.setGmtCreate(new Timestamp(System.currentTimeMillis()));
            newConfigInfo.setGmtModified(new Timestamp(System.currentTimeMillis()));
            
            configInfoMapper.insert(newConfigInfo);
        } else {
            // 更新已有配置
            configInfo.setContent(configForm.getContent());
            configInfo.setMd5(md5);
            configInfo.setGmtModified(new Timestamp(System.currentTimeMillis()));
            
            configInfoMapper.update(configInfo);
        }
        
        // 2. 记录历史版本
        HistoryConfigInfo historyConfigInfo = new HistoryConfigInfo();
        historyConfigInfo.setDataId(configForm.getDataId());
        historyConfigInfo.setGroupId(configForm.getGroup());
        historyConfigInfo.setContent(configForm.getContent());
        historyConfigInfo.setMd5(md5);
        historyConfigInfo.setTenantId(configForm.getTenant());
        historyConfigInfo.setGmtCreate(new Timestamp(System.currentTimeMillis()));
        
        historyConfigInfoMapper.insert(historyConfigInfo);
    }
}
```

### 3.2.4 配置表结构分析

MySQL 中 config_info 表结构：

```sql
CREATE TABLE config_info (
    id bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    data_id varchar(255) NOT NULL COMMENT '配置ID',
    group_id varchar(128) NOT NULL COMMENT '分组ID',
    content longtext NOT NULL COMMENT '配置内容',
    md5 varchar(32) DEFAULT NULL COMMENT 'MD5校验值',
    tenant_id varchar(128) DEFAULT '' COMMENT '租户/命名空间',
    app_name varchar(128) DEFAULT NULL COMMENT '应用名',
    type varchar(64) DEFAULT NULL COMMENT '配置类型',
    gmt_create datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    gmt_modfied datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
    encrypted_data_key varchar(1024) DEFAULT NULL COMMENT '加密密钥',
    c_desc varchar(256) DEFAULT NULL COMMENT '配置描述',
    effect_state varchar(32) DEFAULT NULL COMMENT '生效状态',
    PRIMARY KEY (id),
    UNIQUE KEY uk_configinfo_datagrouptenant (data_id, group_id, tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

### 3.2.5 Derby 嵌入式存储

单机模式下，Nacos 使用 Apache Derby 作为嵌入式数据库：

```java
// EmbeddedStoragePersistServiceImpl.java - Derby 嵌入式存储实现
@Service("configInfoPersistService")
@ConditionalOnProperty(name = "spring.datasource.platform", 
    havingValue = "derby")
public class EmbeddedStoragePersistServiceImpl implements ConfigInfoPersistService {
    
    @PostConstruct
    public void init() throws Exception {
        // 1. 创建 Derby 嵌入式数据库连接
        String derbyHome = EnvUtil.getNacosHome() + "/data/derby";
        System.setProperty("derby.system.home", derbyHome);
        
        // 2. 加载 Derby JDBC 驱动
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        connection = DriverManager.getConnection(
            "jdbc:derby:" + derbyHome + "/nacos;create=true");
        
        // 3. 初始化建表SQL
        executeInitSQL();
    }
    
    @Override
    public void insertOrUpdate(ConfigForm configForm) {
        // Derby SQL INSERT/UPDATE 实现
        String sql = "MERGE INTO config_info AS target "
            + "USING (VALUES (?, ?, ?, ?, ?, ?)) AS source "
            + "(data_id, group_id, tenant_id, content, md5, gmt_modified) "
            + "ON target.data_id = source.data_id "
            + "AND target.group_id = source.group_id "
            + "AND target.tenant_id = source.tenant_id "
            + "WHEN MATCHED THEN UPDATE SET "
            + "target.content = source.content, "
            + "target.md5 = source.md5, "
            + "target.gmt_modified = source.gmt_modified "
            + "WHEN NOT MATCHED THEN INSERT ...";
        
        PreparedStatement pstmt = connection.prepareStatement(sql);
        pstmt.setString(1, configForm.getDataId());
        // ... 设置其他参数
        pstmt.executeUpdate();
    }
}
```

## 3.3 配置获取——客户端长轮询机制

### 3.3.1 LongPollingService——长轮询服务

Nacos 配置中心的核心机制是"长轮询"（Long Polling），即客户端发起 HTTP 请求后，服务端暂不立即返回结果，直到配置变更或超时才返回：

```java
// LongPollingService.java - 长轮询核心引擎
@Service
public class LongPollingService {
    
    // 长轮询超时时间（默认29.5秒，比HTTP超时30秒稍短）
    private static final int LONG_POLLING_TIME_OUT = 29500;
    
    // 客户端长轮询任务队列
    final Queue<ClientLongPolling> allSubs = new ConcurrentLinkedQueue<>();
    
    /**
     * 添加长轮询客户端
     */
    public void addLongPollingClient(HttpServletRequest request, 
            HttpServletResponse response, String clientMd5, 
            int longPollingTimeout) {
        
        // 1. 获取异步异步超时时间
        int delayTime = LONG_POLLING_TIME_OUT;
        if (longPollingTimeout > 0) {
            delayTime = longPollingTimeout;
        }
        
        // 2. 创建客户端长轮询任务
        ClientLongPolling clientLongPolling = new ClientLongPolling(
            request, response, clientMd5, delayTime);
        
        // 3. 加入等待队列
        allSubs.add(clientLongPolling);
        
        // 4. 启动异步定时器，在超时后返回
        ScheduledFuture<?> future = ConfigExecutor.scheduleLongPolling(
            new LongPollingRunnable(clientLongPolling), 
            delayTime, TimeUnit.MILLISECONDS);
        
        clientLongPolling.setAsyncTimeoutFuture(future);
    }
    
    /**
     * 当配置发生变更时，通知所有正在等待该配置的客户端
     */
    public void notifyConfigChange(String dataId, String group, String tenant) {
        Iterator<ClientLongPolling> iterator = allSubs.iterator();
        while (iterator.hasNext()) {
            ClientLongPolling clientLongPolling = iterator.next();
            // 匹配 dataId, group, tenant
            if (matchClientConfig(clientLongPolling, dataId, group, tenant)) {
                // 生成新MD5并立刻返回
                String newMd5 = getMd5String(dataId, group, tenant);
                clientLongPolling.sendResponse(newMd5);
                // 从等待队列移除
                iterator.remove();
            }
        }
    }
    
    /**
     * 匹配客户端是否订阅该配置
     */
    private boolean matchClientConfig(ClientLongPolling clientLongPolling, 
            String dataId, String group, String tenant) {
        if (!StringUtils.equals(clientLongPolling.getDataId(), dataId)) {
            return false;
        }
        if (!StringUtils.equals(clientLongPolling.getGroup(), group)) {
            return false;
        }
        if (!StringUtils.equals(clientLongPolling.getTenant(), tenant)) {
            return false;
        }
        return true;
    }
}
```

### 3.3.2 ClientLongPolling——客户端长轮询任务

```java
// ClientLongPollsring.java - 客户端长轮询任务
public class ClientLongPolling {
    
    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final String clientMd5;
    private final int delayTime;
    private ScheduledFuture<?> asyncTimeoutFuture;
    
    /**
     * 向客户端返回配置变更结果
     */
    public void sendResponse(String newMd5) {
        try {
            // 取消超时定时器
            if (asyncTimeoutFuture != null) {
                asyncTimeoutFuture.cancel(false);
            }
            
            // 生成响应JSON
            String responseJson = generateResponseJson(newMd5);
            
            // 写入 HttpServletResponse
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().write(responseJson);
            response.getWriter().flush();
        } catch (IOException e) {
            Loggers.CONFIG_LOG.error("Failed to send long poll response", e);
        }
    }
    
    /**
     * 生成响应JSON格式
     */
    private String generateResponseJson(String md5) {
        // 如果有变更，返回新的配置内容
        ConfigInfo configInfo = configInfoPersistService.findConfigInfo(
            dataId, group, tenant);
        JSONObject json = new JSONObject();
        if (configInfo != null && !md5.equals(configInfo.getMd5())) {
            json.put("dataId", configInfo.getDataId());
            json.put("group", configInfo.getGroup());
            json.put("tenant", configInfo.getTenant());
            json.put("content", configInfo.getContent());
            json.put("md5", configInfo.getMd5());
        } else {
            json.put("md5", md5);
        }
        return json.toJSONString();
    }
}
```

### 3.3.3 客户端长轮询流程图

```
Client Side                          Nacos Server Side
    │                                       │
    │ ── GET /v1/cs/configs/listener ────► │
    │    (dataId, group, md5)               │
    │                                       ├── 注册 ClientLongPolling
    │                                       ├── 启动超时定时器（29.5s）
    │                                       │
    │                              ┌────────┤
    │                              │ 配置变更? │
    │                              └────┬───┘
    │                      Yes         │         No
    │              ┌────────────      │      ────────────┐
    │              ▼                                   ▼
    │       ┌──────────────┐              ┌──────────────────┐
    │       │ 计算新MD5    │              │ 等到 29.5秒超时│
    │       │ 返回变更结果  │              │ 返回304 Not Mod │
    │       └──────┬───────┘              └──────┬───────────┘
    │              │                               │
    │ ◄──────────┘                               │
    │  (收到变更)              ◄────────────────┘
    │                                       (无变更)
    │ ── GET /v1/cs/configs ─────────────►
    │    (拉取最新配置内容)
    │ ◄── 返回完整配置 ───────────────────
```

## 3.4 配置变更通知机制——AsyncNotifyService

### 3.4.1 异步通知服务架构

```java
// AsyncNotifyService.java - 异步通知服务
@Service
public class AsyncNotifyService {
    
    @Autowired
    private ServerMemberManager serverMemberManager;
    
    @Autowired
    private NacosAsyncRestTemplate nacosAsyncRestTemplate;
    
    /**
     * 通知集群其他节点配置变更
     */
    public void notifyClusterNodes(String dataId, String group, String tenant) {
        // 1. 获取集群其他节点列表
        List<Member> members = serverMemberManager.getServerList();
        String currentServerAddress = serverMemberManager.getCurrentServerAddress();
        
        // 2. 构建变更通知请求
        ConfigChangeNotifyRequest notifyRequest = new ConfigChangeNotifyRequest();
        notifyRequest.setDataId(dataId);
        notifyRequest.setGroup(group);
        notifyRequest.setTenant(tenant);
        
        // 3. 异步通知每个其他集群节点
        for (Member member : members) {
            if (member.getAddress().equals(currentServerAddress)) {
                continue; // 跳过当前节点
            }
            
            String targetUrl = String.format("http://%s:%d/v1/cs/communications/configChange",
                member.getIp(), member.getPort());
            
            nacosAsyncRestTemplate.post(targetUrl, 
                JacksonUtils.toJson(notifyRequest),
                new Callback<String>() {
                    @Override
                    public void onReceive(Result<String> result) {
                        if (!result.isOk()) {
                            Loggers.CONFIG_LOG.warn(
                                "Failed to notify node: {}, error: {}", 
                                member.getAddress(), result.getMessage());
                        }
                    }
                });
        }
    }
}
```

### 3.4.2 集群间配置同步机制

当集群中某节点接收到配置变更通知时，会触发本地长轮询通知：

```java
// CommunicationController.java - 集群间通信控制器
@RestController
@RequestMapping(Constants.COMMUNICATION_CONTROLLER_PATH)
public class CommunicationController {
    
    @Autowired
    private LongPollingService longPollingService;
    
    /**
     * 接收集群其他节点的配置变更通知
     */
    @PostMapping("/configChange")
    public Boolean notifyConfigChange(HttpServletRequest request) {
        String dataId = WebUtils.required(request, "dataId");
        String group = WebUtils.required(request, "group");
        String tenant = WebUtils.required(request, "tenant");
        
        // 通知本地正在长轮询的客户端
        longPollingService.notifyConfigChange(dataId, group, tenant);
        
        return true;
    }
}
```

## 3.5 配置历史版本管理

### 3.5.1 HistoryConfigInfoService

```java
// HistoryConfigInfoService.java - 配置历史版本服务
@Service
public class HistoryConfigInfoService {
    
    @Autowired
    private HistoryConfigInfoMapper historyConfigInfoMapper;
    
    /**
     * 查询配置历史版本列表
     */
    public Page<HistoryConfigInfo> listHistoryConfigInfo(String dataId, 
            String group, String tenant, int pageNo, int pageSize) {
        return historyConfigInfoMapper.selectByDataId(dataId, group, tenant, 
            pageNo, pageSize);
    }
    
    /**
     * 回滚到指定历史版本
     */
    public void rollback(String dataId, String group, String tenant, 
            Long historyId) {
        // 1. 获取历史版本配置内容
        HistoryConfigInfo historyConfigInfo = historyConfigInfoMapper.selectById(historyId);
        
        // 2. 用历史版本内容更新当前配置
        ConfigInfo configInfo = configInfoMapper.selectByDataId(dataId, group, tenant);
        configInfo.setContent(historyConfigInfo.getContent());
        configInfo.setMd5(MD5Utils.md5Hex(historyConfigInfo.getContent(), ENCRYPT_KEY));
        configInfo.setGmtModified(new Timestamp(System.currentTimeMillis()));
        configInfoMapper.update(configInfo);
        
        // 3. 记录新的历史版本（以便继续回滚）
        HistoryConfigInfo newHistory = new HistoryConfigInfo();
        newHistory.setDataId(dataId);
        newHistory.setGroupId(group);
        newHistory.setContent(historyConfigInfo.getContent());
        newHistory.setMd5(configInfo.getMd5());
        newHistory.setTenantId(tenant);
        newHistory.setGmtCreate(new Timestamp(System.currentTimeMillis()));
        historyConfigInfoMapper.insert(newHistory);
    }
}
```

### 3.5.2 历史版本保留策略

```properties
# 历史版本保留天数（默认30天）
nacos.config.history.retention.days=30

# 每个配置最多保留的历史版本数（默认100）
nacos.config.history.max.size=100
```

## 3.6 灰度发布机制

### 3.6.1 Beta 配置发布

Nacos 支持 Beta 配置发布，即对指定 IP 列表的客户端下发 Beta 版本配置：

```java
// ConfigBetaService.java - Beta 配置发布服务
@Service
public class ConfigBetaService {
    
    @Autowired
    private ConfigInfoPersistService configInfoPersistService;
    
    /**
     * 发布 Beta 配置
     */
    public void publishBeta(ConfigForm configForm) {
        // 1. 验证 beta ips
        String betaIps = configForm.getBetaIps();
        if (StringUtils.isBlank(betaIps)) {
            throw new IllegalArgumentException("betaIps is required for beta publish");
        }
        
        // 2. 插入或更新 Beta 配置
        configInfoPersistService.insertOrUpdateBeta(configForm);
        
        // 3. 只通知 betaIps 列表中的客户端
        notifyBetaClients(configForm.getDataId(), configForm.getGroup(),
            configForm.getTenant(), betaIps);
    }
    
    /**
     * 停止 Beta 配置（切换回正式配置）
     */
    public void stopBeta(String dataId, String group, String tenant) {
        // 1. 删除 Beta配置
        configInfoPersistService.removeBeta(dataId, group, tenant);
        
        // 2. 通知所有客户端重新拉取正式配置
        asyncNotifyService.notifyAll(dataId, group, tenant);
    }
}
```

### 3.6.2 Tag 配置发布

Tag 配置允许按标签对不同实例下发不同配置：

```java
// ConfigTagService.java - Tag 配置发布服务
@Service
public class ConfigTagService {
    
    /**
     * 发布 Tag 配置
     */
    public void publishTag(ConfigForm configForm) {
        String tagName = configForm.getTagName();
        if (StringUtils.isBlank(tagName)) {
            throw new IllegalArgumentException("tagName is required for tag publish");
        }
        
        // 插入或更新 Tag 配置
        configInfoPersistService.insertOrUpdateTag(configForm);
        
        // 只通知带有该标签的客户端
        notifyTagClients(configForm.getDataId(), configForm.getGroup(),
            configForm.getTenant(), tagName);
    }
}
```

## 3.7 配置导入导出机制

### 3.7.1 配置导出

```java
// ConfigController.java - 配置导出API
@GetMapping("/export")
public void exportConfig(HttpServletRequest request, HttpServletResponse response) {
    String tenant = WebUtils.optional(request, "tenant", "");
    String dataId = WebUtils.optional(request, "dataId", "");
    String group = WebUtils.optional(request, "group", "");
    
    // 查询符合条件的配置列表
    List<ConfigInfo> configList = configInfoPersistService.findAllConfigInfo(
        dataId, group, tenant);
    
    // 导出为 ZIP 压缩包
    response.setContentType("application/zip");
    response.setHeader("Content-Disposition", 
        "attachment; filename=nacos-config-export.zip");
    
    try (ZipOutputStream zipOut = new ZipOutputStream(response.getOutputStream())) {
        for (ConfigInfo configInfo : configList) {
            String fileName = configInfo.getGroup() + "/" + configInfo.getDataId();
            ZipEntry entry = new ZipEntry(fileName);
            zipOut.putNextEntry(entry);
            zipOut.write(configInfo.getContent().getBytes(StandardCharsets.UTF_8));
            zipOut.closeEntry();
        }
    }
}
```

### 3.7.2 配置导入

```java
// ConfigController.java - 配置导入API
@PostMapping("/import")
public Map<String, Object> importConfig(@RequestParam("file") MultipartFile file, 
        HttpServletRequest request) throws IOException {
    
    String namespace = WebUtils.optional(request, "namespace", "");
    
    Map<String, Object> result = new HashMap<>();
    int successCount = 0;
    int failCount = 0;
    
    // 解析 ZIP 文件
    try (ZipInputStream zipIn = new ZipInputStream(file.getInputStream())) {
        ZipEntry entry;
        while ((entry = zipIn.nextEntry()) != null) {
            try {
                String fileName = entry.getName();
                // 从文件名解析 group 和 dataId
                int slashIndex = fileName.indexOf('/');
                String group = slashIndex > 0 ? 
                    fileName.substring(0, slashIndex) : "DEFAULT_GROUP";
                String dataId = slashIndex > 0 ? 
                    fileName.substring(slashIndex + 1) : fileName;
                
                // 读取配置内容
                String content = new String(readAllBytes(zipIn), 
                    StandardCharsets.UTF_8);
                
                // 持久化配置
                ConfigForm configForm = new ConfigForm();
                configForm.setDataId(dataId);
                configForm.setGroup(group);
                configForm.setTenant(namespace);
                configForm.setContent(content);
                
                configInfoPersistService.insertOrUpdate(configForm);
                successCount++;
            } catch (Exception e) {
                failCount++;
                Loggers.CONFIG_LOG.error("Import config failed: {}", 
                    entry.getName(), e);
            }
        }
    }
    
    result.put("successCount", successCount);
    result.put("failCount", failCount);
    return result;
}
```

---

*（第三章完，约 Arial汉字）*
