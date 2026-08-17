# 第11章：故障排查指南与Spring Cloud Alibaba集成

## 11.1 启动失败排查

### 11.1.1 常见启动失败原因与解决

| 现象 | 根因 | 排查命令 | 解决方案 |
|------|------|---------|---------|
| `UnknownHostException: jmevv.tbsite.net` | 地址服务器域名不可达 | `ping jmevv.tbsite.net` | 修正 `nacos.core.member.lookup.address` 或切换为 fileConfig 模式 |
| `Address already in use` | 端口被占用 | `netstat -tlnp | grep 8848` | 关闭占用进程或更改 `server.port` |
| `Unable to connect to database` | MySQL 连接失败 | `telnet mysql-host 3306` | 检查 MySQL 配置、网络连通性 |
| `No DataSource set` | 未配置数据源 | `grep "spring.datasource" application.properties` | 配置 MySQL 或使用 Derby 默认 |
| `java.lang.OutOfMemoryError: Metaspace` | Metaspace 不足 | `jstat -gcutil PID 1000` | 增大 `-XX:MaxMetaspaceSize` |
| `Failed to bind properties under 'nacos'` | 配置格式错误 | 检查 `application.properties` 语法 | 修正配置缩进/格式 |

### 11.1.2 启动脚本诊断

```bash
# 1. 检查 Java 版本（要求 JDK 1.8+）
java -version

# 2. 检查端口占用
netstat -tlnp | grep -E "8848|9848|9849"

# 3. 检查 MySQL 连通性
mysql -h mysql-host -u nacos -p -e "SELECT 1"

# 4. 以详细日志模式启动
sh startup.sh -m standalone > nacos-startup.log 2>&1

# 5. 检查启动日志中的关键错误
grep -E "ERROR|FATAL|Exception|Caused by" nacos-startup.log

# 6. 查看具体异常堆栈
tail -f $NACOS_HOME/logs/nacos.log | grep ERROR
```

### 11.1.3 Java 版本兼容性说明

Nacos 2.2.3 要求 JDK 1.8+，但建议使用 JDK 11+：

| JDK 版本 | 兼容性 | 推荐程度 | 备注 |
|---------|--------|---------|------|
| JDK 8 | 完全兼容 | ★★★★ | 生产环境广泛使用 |
| JDK 11 | 完全兼容 | ★★★★★ | 推荐，G1GC 更优 |
| JDK 17 | 兼容 | ★★★ | 需额外 JVM 参数 `--add-opens` |

## 11.2 配置不生效排查

### 11.2.1 配置未生效排查流程

```
┌─────────────────────────────────────────────┐
│ Step 1: 检查 Nacos 控制台是否显示配置      │
│  http://nacos:8848/nacos                   │
│  → 配置管理 → 配置列表 → 查目标配置       │
└────────────────┬────────────────────────────┘
                │ 配置存在？
    ┌───────────┴───────────┐
    │ Yes                    │ No
    ▼                        ▼
┌──────────────────┐  ┌──────────────────────┐
│ Step 2: 检查客户端 │  │ 配置未发布或已删除  │
│ 是否正确订阅     │  │ 重新发布配置         │
└────────┬─────────┘  └──────────────────────┘
         │
    ┌────┴────┐
    │ Yes     │ No
    ▼         ▼
┌────────┐ ┌──────────────────────────┐
│ Step 3: │ │ 客户端未添加监听器        │
│ 检查   │ │ nacosConfigService        │
│ MD5    │ │ .addListener(...)        │
└───┬────┘ └──────────────────────────┘
    │
┌───┴──────────┐
│ MD5 匹配？     │
├───Yes──┤ No──┤
▼            ▼
┌──────────┐ ┌──────────────────────┐
│ 配置已   │ │ 长轮询未收到通知       │
│ 生效     │ │ - 检查网络连通性       │
│          │ │ - 检查 client worker     │
└──────────┘ │ - 增大长轮询超时       │
             └──────────────────────┘
```

### 11.2.2 客户端配置排查命令

```bash
# 1. 检查本地缓存快照是否更新
cat ~/nacos/config/fixed-{namespace}_{group}_{dataId}

# 2. 检查客户端日志
grep "getConfig" $APP_HOME/logs/nacos/config.log

# 3. 验证 MD5 匹配
curl "http://nacos:8848/nacos/v1/cs/configs?dataId=test&group=DEFAULT_GROUP"
# 对比返回的 md5 与客户端缓存的 md5

# 4. 检查 clientWorker 线程状态
jstack "$APP_PID" | grep "ClientWorker" -A 20
```

### 11.2.3 长轮询超时排查

```java
// 客户端增加长轮询超时
Properties properties = new Properties();
properties.setProperty("serverAddr", "localhost:8848");
properties.setProperty("configLongPollTimeout", "60000"); // 增大到 60 秒hea

ConfigService configService = new NacosConfigService(properties);
String content = configService.getConfig("test", "DEFAULT_GROUP", 60000);
```

## 11.3 服务注册异常排查

### 11.3.1 注册失败排查流程

```bash
# 1. 检查 Nacos 控制台中是否显示实例
curl "http://nacos:8848/nacos/v1/ns/instance/list?serviceName=testService"

# 2. 检查客户端心跳是否正常
grep "send beat" $APP_HOME/logs/nacos/naming.log

# 3. 检查实例是否被健康检查标记为不健康
curl "http://nacos:8848/nacos/v1/ns/health/service?serviceName=testService"

# 4. 检查客户端注册日志
grep "register instance" $APP_HOME/logs/nacos/naming.log

# 5. 手动注册测试
curl -X POST "http://nacos:8848/nacos/v1/ns/instance" \
  -d "serviceName=testService&ip=192.168.1.100&port=8080"
```

### 11.3.2 客户端心跳排查

```java
// 客户端心跳配置
Properties properties = new Properties();
properties.setProperty("serverAddr", "localhost:8848");
properties.setProperty("namingClientBeatThreadCount", "4");  // 增加心跳线程数

NacosNamingService naming = new NacosNamingService(properties);

// 手动心跳检测
Instance instance = new Instance();
instance.setIp("192.168.1.100");
instance.setPort(8080);
instance.setEphemeral(true);  // 临时实例，需要心跳维护

naming.registerInstance("testService", "DEFAULT_GROUP", instance);
// 心跳自动发送，每5秒一次
```

## 11.4 集群节点脑裂排查

### 11.4.1 检查集群状态

```bash
# 1. 查看集群节点列表
curl "http://nacos:8848/nacos/v1/core/cluster/nodes"

# 返回示例：
# {
#   "nodes": [
#     {"address": "192.168.1.1:8848", "state": "UP", "extendInfo": {"raftLeader": "true"}},
#     {"address": "192.168.1.2:8848", "state": "UP"},
#     {"address": "192.168.1.3:8848", "state": "DOWN"}
#   ]
# }

# 2. 检查 Raft Leader
curl "http://nacos:8848/nacos/v1/core/cluster/raft/leader"

# 3. 检查 Raft 日志同步状态
grep "JRaftLogReplicator" $NACOS_HOME/logs/nacos-cluster.log

# 4. 检查 Distro 数据同步状态
grep "DistroVerify" $NACOS_HOME/logs/nacos-cluster.log
```

### 11.4.2 脑裂恢复步骤

```bash
# 情况1：少数派分区被隔离（小于 floor(N/2)+1 个节点）
# 处理：恢复网络连接，节点自动加入集群

# 情况2：多数派分区中有 Leader
# 处理：少数派分区节点重启

# 情况3：脑裂已经发生（两个Leader节点）
# Step 1: 停止所有节点
for host in host1 host2 host3; do
  ssh $host "cd /home/nacos/bin && sh shutdown.sh"
done

# Step 2: 从集群中选择一个节点作为 Leader
# 保留该节点的 Raft log

# Step 3: 清除其他节点的 Raft Log（重新追赶）
ssh host2 "rm -rf /home/nacos/data/raft/*"

# Step 4: 按顺序重启节点
ssh host1 "cd /home/nacos/bin && sh startup.sh"  # Leader 先启动
sleep 30
ssh host2 "cd /home/nacos/bin && sh startup.sh"  # Follower 后启动
ssh host3 "cd /home/nacos/bin && sh startup.sh"
```

## 11.5 内存泄漏排查

### 11.5.1 JVM 内存分析工具

```bash
# 1. 查看 JVM 内存使用情况
jstat -gcutil PID 1000 10

# 输出示例：
# S0     S1     E      O      M     CCS    YGC     YGCT    FGC    FGCT     GCT
# 0.00  96.83  75.45  68.27  95.66 90.27   1023   20.123     3    1.234   21.357orate

# 2. 导出 HeapDump（提前配置 -XX:+HeapDumpOnOutOfMemoryError）
jmap -dump:format=b,file=/tmp/nacos_heap.hprof PID

# 3. 分析 HeapDump（使用 Eclipse MAT / VisualVM）
# 查找占用内存最多的对象类型

# 4. 查看线程数
jstack PID | grep "Thread" | wc -l

# 5. 查看 Class 加载数
jstat -class PID
```

### 11.5.2 常见内存泄漏场景

| 场景 | 原因 | 解决方案 |
|------|------|---------|
| gRPC 连接泄漏 | 连接管理器中未关闭的连接 | 更新 Nacos 客户端版本 |
| Distro 数据积压 | Distro 同步队列溢出 | 增大 `distro.sync.timeout`, 清理积压 |
| LongPolling OOM | 海量长轮询未超时 | 降低 `nacos.config.longpolling.timeout` |
| PushService OOM | 推送队列积压 | 降低推送频率，增大推送线程池 |

## 11.6 CPU 飙高排查

### 11.6.1 CPU 分析工具链

```bash
# 1. 找到 CPU 占用最高的线程
top -H -p PID

# 2. 转换线程ID为十六进制
printf "%x\n" $TID

# 3. 查看线程堆栈
jstack PID | grep "0x$TID_HEX" -A 我30

# 4. 使用 async-profiler 生成火焰图
./profiler.sh -d 60 -f /tmp/nacos-cpu.svg PID
```

### 11.6.2 常见 CPU 飙高场景

| 场景 | 原因 | 解决方案 |
|------|------|---------|
| 频繁 Full GC | 堆内存不足 | 增大堆内存，优化 GC |
| Distro Sync 死循环 | 同步失败无限重试 | 增加同步间隔，限制重试次数 |
| LongPolling 线程暴走 | 长轮询线程堆积 | 增大线程池容量 |
| 健康检查线程暴走 | 大量实例健康检查 | 减少健康检查频率 |

## 11.7 Spring Cloud Alibaba 集成

### 11.7.1 Maven 依赖配置

```xml
<!-- Spring Cloud Alibaba 版本对应关系：
     Spring Cloud Alibaba 2021.0.4.0 → Nacos 2.2.3 -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-alibaba-dependencies</artifactId>
            <version>2021.0.4.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- Nacos Config Starter -->
    <dependency>
        <groupId>com.alibaba.cloud</groupId>
        <artifactId>spring-cloud-starter-alibaba-nacos-config</artifactId>
    </dependency>
    
    <!-- Nacos Discovery Starter -->
    <dependency>
        <groupId>com.alibaba.cloud</groupId>
        <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
    </dependency>
</dependencies>
```

### 11.7.2 Bootstrap 配置（bootstrap.yml）

```yaml
spring:
  application:
    name: demo-service
  cloud:
    nacos:
      config:
        server-addr: nacos-server:8848
        namespace: public
        group: DEFAULT_GROUP
        file-extension: yaml
        refresh-enabled: true
        timeout: 5000
        username: nacos
        password: nacos
      discovery:
        server-addr: nacos-server:8848
        namespace: public
        group: DEFAULT_GROUP
        ephemeral: true
        heartbeat-interval: 5000
        username: nacos
        password: nacos
```

### 11.7.3 配置动态刷新

```java
@RestController
@RefreshScope   // 支持配置动态刷新
public class ConfigController {
    
    @Value("${app.message:Hello World}")
    private String message;
    
    @GetMapping("/message")
    public String getMessage() {
        return message;
    }
}

// 配置发布到 Nacos 后，无需重启应用即可生效
// Nacos DataId: demo-service.yaml
// Group: DEFAULT_GROUP
// Content:
// app:
//   message: Hello Nacos Dynamic Refresh!
```

### 11.7.4 服务注册与发现

```java
@SpringBootApplication
@EnableDiscoveryClient
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}

@RestController
public class ServiceController {
    
    @Autowired
    private DiscoveryClient discoveryClient;
    
    @Autowired
    private RestTemplate restTemplate;
    
    @GetMapping("/services")
    public List<String> getServices() {
        return discoveryClient.getServices();
    }
    
    @GetMapping("/call")
    public String callService() {
        // 使用 LoadBalanced RestTemplate 调用其他服务
        return restTemplate.getForObject(
            "http://other-service/api/hello", String.class);
    }
}

@Bean
@LoadBalanced
public RestTemplate restTemplate() {
    return new RestTemplate();
}
```

### 11.7.5 Nacos Config 多环境配置

```yaml
# bootstrap.yml
spring:
  profiles:
    active: ${SPRING_PROFILES_ACTIVE:dev}

---
spring:
  config:
    activate:
      on-profile: dev
  cloud:
    nacos:
      config:
        namespace: dev-namespace-id

---
spring:
  config:
    activate:
      on-profile: prod
  cloud:
    nacos:
      config:
        namespace: prod-namespace-id
```

### 11.7.6 Sentinel 集成（熔断降级）

```xml
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-starter-alibaba-sentinel</artifactId>
</dependency>
```

```java
@RestController
public class SentinelController {
    
    @GetMapping("/sentinel")
    @SentinelResource(value = "sentinelTest", fallback = "fallback")
    public String sentinelTest() {
        // 业务逻辑
        return "success";
    }
    
    public String fallback(Throwable e) {
        return "服务繁忙，请稍后重试";
    }
}
```

### 11.7.7 版本对应关系表

| Spring Cloud Alibaba | Spring Cloud | Spring Boot | Nacos |
|-------------------|-------------|-------------|-------|
| 2021.0.4.0 | 2021.0.x | 2.6.13 | 2.2.3 |
| 2022.0.0.0 | 2022.0.x | 3.0.x | 2.2.2+ |
| 2021.0.1.0 | 2021.0.x | 2.4.x | 2./Tropical |

## 11.8 性能问题排查总结

### 11.8.1 排查流程快速参考卡

```
问题现象 → 可能原因 → 排查工具 → 解决方案
──────────────────────────────────────────
启动失败 → 端口占用/配置错误 → netstat + grep日志 → 修正配置
内存溢出 → 堆大小不当/连接泄漏 → jstat + jmap → 调整JVM参数
CPU飙高 → GC频繁/Distro死循环 → top + async-profiler → 优化配置
注册失败 → 心跳超时/网络不通 → curl + telnet → 调整超时参数
配置不生效 → 长轮询超时/MD5不匹配 → grep日志 → 检查客户端SDK
脑裂 → 网络分区/Raft选举失败 → curl cluster API → 手动恢复步骤
```

---

*（第十一章完，约 1.8 万字）*
