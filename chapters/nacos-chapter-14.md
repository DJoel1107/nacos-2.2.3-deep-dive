# 第 14 章：故障排查指南

## 14.1 启动失败排查：6 种常见原因表 + 启动脚本诊断 6 步骤

### 设计背景

Nacos Server 启动失败是运维中最先遇到、也最频繁的一类故障。启动问题往往由环境差异（JDK 版本、内存分配、端口占用）、配置错误（`application.properties` 缺失 key、证书路径错误）、依赖不可用（数据库、存储目录无权限）等多种原因叠加引发。由于 Spring Boot 启动过程长、日志分散，缺乏系统化排查路径时容易在多个错误间反复试探。

本节以 Nacos 2.5.3 的启动入口 `Nacos` 类（`console/src/main/java/com/alibaba/nacos/Nacos.java`，`main()` 方法调用 `SpringApplication.run(Nacos.class, args)`）为起点，给出 6 种最常见启动失败原因，并提炼一套可复制的 6 步诊断流程。

### 核心类关系图（启动链路）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  Nacos 2.5.3 启动链路与失败点映射                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Nacos.main()                                                               │
│  (console/.../Nacos.java)                                                   │
│      │                                                                      │
│      ▼                                                                      │
│  SpringApplication.run()            ┌─ 失败点1: JVM 参数(-Xms/-Xmx)         │
│      │                              │   启动脚本 startup.sh                 │
│      ▼                              └─────────────────────────────────────┘ │
│  环境准备 EnvironmentPrepared    ┌─ 失败点2: JDK 版本 / 环境变量              │
│      │                           └──────────────────────────────────────┘   │
│      ▼                                                                      │
│  Bean 定义加载                    ┌─ 失败点3: 端口占用(8848/9848)           │
│      │                            └─────────────────────────────────────┘   │
│      ▼                                                                      │
│  ApplicationContext refresh   ┌─ 失败点4: 配置 key 缺失/格式错误             │
│      │                         └────────────────────────────────────────┘   │
│      ▼                                                                      │
│  依赖初始化(DB/存储目录) ┌─ 失败点5: MySQL 不可达 / 目录无权限               │
│      │                    └─────────────────────────────────────────────┘  │
│      ▼                                                                      │
│  gRPC 端口监听(9848/9849) ┌─ 失败点6: 端口冲突 / 防火墙                     │
│                           └─────────────────────────────────────────────┘   │
│            图 14-1：Nacos 启动链路与 6 类失败点映射                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6 种常见启动失败原因

| # | 失败原因 | 典型错误日志 | 判定依据 | 解决方向 |
|---|---------|-------------|---------|---------|
| 1 | **JVM 参数不合法** | `Invalid maximum heap size: -Xmx`、`Error occurred during initialization of VM` | 启动脚本与 Docker 内存不匹配 | 校验 `startup.sh` 的 `-Xms/-Xmx` 与容器/主机可用内存 |
| 2 | **JDK 版本不匹配** | `UnsupportedClassVersionError`、`org.springframework.boot` 相关 ClassNotFound | Nacos 2.5.3 要求 JDK 8 及以上，若编译目标更高需 JDK 11/17 | 切换到兼容 JDK 并确认 `JAVA_HOME` |
| 3 | **端口被占用** | `BindException: Address already in use`、`Web server failed to start` | `netstat -tlnp` 查 8848/9848/9849 | 释放端口或修改 `server.port` |
| 4 | **配置缺失/错误** | `Property 'xxx' not found`、`IllegalArgumentException` | 定位 `application.properties` 中缺失 key | 补齐配置 key 与默认值 |
| 5 | **数据库/存储不可用** | `Communications link failure`、`Access denied for user` | 检查 MySQL 连通性与鉴权 | 修复连接串/账号或初始化 SQL |
| 6 | **gRPC 端口冲突** | `Failed to bind grpc port 9848`、`gRPC server start failed` | 确认 9848/9849 与主端口偏移规则 | 调整 `server.port` 后同步调整 gRPC 端口 |

### 源码走读：启动入口与失败抛出点

Nacos 2.5.3 的启动入口定义在 `console` 模块：

```java
// console/src/main/java/com/alibaba/nacos/Nacos.java:23-51 (Nacos 2.5.3)
@SpringBootApplication
@ComponentScan(basePackages = "com.alibaba.nacos", excludeFilters = {
        @Filter(type = FilterType.CUSTOM, classes = {NacosTypeExcludeFilter.class}),
        @Filter(type = FilterType.CUSTOM, classes = {TypeExcludeFilter.class}),
        @Filter(type = FilterType.CUSTOM, classes = {AutoConfigurationExcludeFilter.class})})
@ServletComponentScan
public class Nacos {
    
    public static void main(String[] args) {
        SpringApplication.run(Nacos.class, args);
    }
}
```

`@ComponentScan(basePackages = "com.alibaba.nacos")` 指定扫描整个 `com.alibaba.nacos` 包，并通过 `NacosTypeExcludeFilter` 按模块的启用条件（`@ConditionalOnProperty` 等）选择性装配 Bean。这种设计决定了**配置 key 是否满足条件决定了对应模块能否正常启动**——例如 `nacos.core.auth.enabled`、`nacos.standalone` 等开关直接影响启动路径。若某类条件不满足，Spring 启动会在 `refresh()` 阶段抛出 `BeanCreationException`，这就是启动失败点 4 的根因。

服务启动后，Nacos 会监听主 HTTP 端口（默认 8848）以及 gRPC 端口（默认 9848/9849，由主端口偏移计算）。gRPC 端口绑定失败通常抛 `BindException`，即失败点 3/6。

### 启动脚本诊断 6 步骤

以下 6 步按"从日志到根因"的顺序执行，每步都有明确的检查命令与判定标准：

**步骤 1：确认 JVM 内存参数。**
```bash
# 查看 Nacos 启动脚本中 JVM 参数
grep -E "JAVA_OPT|Xms|Xmx|-Xmn" /path/to/nacos/bin/startup.sh
# 确认主机可用内存
free -h
```
判定：`-Xmx` 及 `-Xmn` 之和不得超过主机可用内存，否则触发失败点 1。

**步骤 2：确认 JDK 版本。**
```bash
java -version
# 定位 JAVA_HOME，确认与 startup.sh 使用一致
echo $JAVA_HOME
```
判定：Nacos 2.5.3 官方支持 JDK 8/jdk8+、11、17；出现 `UnsupportedClassVersionError` 说明 JDK 版本过低。

**步骤 3：检查启动日志定位首个异常。**
```bash
tail -100 /path/to/nacos/logs/start.out
# 搜索首个 ERROR 或异常堆栈首行
grep -m1 -E "ERROR|Exception|java.lang" /path/to/nacos/logs/start.out
```
判定：以堆栈**最上层的用户代码异常**为根因入口，Spring Boot 外围的 `BeanCreationException` 只是包装。

**步骤 4：检查端口占用。**
```bash
netstat -tlnp | grep -E "8848|9848|9849"
ss -tlnp | grep -E "8848|9848"
```
判定：8848/9848/9849 任一被占用即失败点 3/6；注意确认占用进程是否残留旧 Nacos 实例。

**步骤 5：检查配置与数据库依赖。**
```bash
# 定位并检查关键配置 key
grep -nE "server\.port|nacos\.core\.auth|mysql|db\.url" /path/to/nacos/conf/application.properties
# 测试数据库连通性
mysql -h<db> -P3306 -u<nacos> -p -e "SELECT 1;"
```
判定：缺失 key 或 DB 不可达对应失败点 4/5。

**步骤 6：检查文件系统权限与磁盘。**
```bash
ls -l /path/to/nacos/data/ && df -h /path/to/nacos/data/
touch /path/to/nacos/data/.write_test
```
判定：目录无写权限或磁盘写满会导致 `nacos.home` 相关初始化失败，属于启动失败点 5。

### Trade-off 分析

**快速失败（fail-fast） vs 容错启动**：

| 维度 | 严格校验（快速失败） | 宽松容忍（延迟暴露） |
|------|--------------------|--------------------|
| 问题发现时机 | 启动即抛错，立即暴露 | 启动看似成功，运行期才出问题 |
| 排查成本 | 低（启动期集中排查） | 高（问题隐藏到运行期） |
| 误启动风险 | 低（配置错误直接阻断） | 高（坏配置带入生产） |
| 适用场景 | 生产环境（推荐） | 开发/试用环境 |

Nacos 默认在关键依赖（数据库、核心配置）缺失时采取"快速失败"策略，避免带着错误配置带病启动。生产环境应保持这一默认行为，不因"想先看看界面"而绕过校验。

### 源码走读：NacosApplicationListener 与启动事件驱动

`NacosApplicationListener`（`core/src/main/java/com/alibaba/nacos/core/listener/NacosApplicationListener.java`）是理解 Nacos 启动失败根因的另一个关键入口。它实现了 Spring 的 `SpringApplicationRunListener` 接口，通过监听启动事件在不同阶段执行 Nacos 特有初始化：

```java
// core/src/main/java/com/alibaba/nacos/core/listener/NacosApplicationListener.java (Nacos 2.5.3, 节选)
public class NacosApplicationListener implements SpringApplicationRunListener {
    
    @Override
    public void environmentPrepared(ConfigurableEnvironment environment) {
        // 环境准备阶段：注入 nacos.home、日志路径等配置
        String home = System.getProperty(Constants.NACOS_HOME);
        if (StringUtils.isBlank(home)) {
            home = EnvironmentUtil.getNacosHome();
        }
        System.setProperty(Constants.NACOS_HOME, home);
        LogUtil.setUuid();
    }
    
    @Override
    public void contextPrepared(ConfigurableApplicationContext context) {
        // 容器准备阶段：注册 Nacos 专用的工具 Bean
    }
}
```

该监听器的 `environmentPrepared()` 在环境准备阶段完成 `nacos.home` 的解析与注入。**这解释了启动失败点 5 的一个常见诱因**：若 `nacos.home` 指向的目录不可写或不存在，此处初始化即失败，且错误可能在日志中并不显眼。因此排查启动失败时，应确认该系统属性与目录确实就绪。

`environmentPrepared()` 还会调用 `LogUtil.setUuid()` 为本次启动生成 UUID，便于在多节点集群中按启动批次关联日志。若此处抛异常，日志系统可能未完全初始化，导致后续异常堆栈打印不完整——这也是定位失败点 3 时"日志不完整"现象的潜在根因。

### 生产案例：一次 "-Xms 超限" 与 "端口残留" 的组合故障

某集群升级到 2.5.3 后节点无法启动，`start.out` 首行即 `Invalid maximum heap size`。运维按 6 步诊断：步骤 1 发现 `startup.sh` 中 `-Xmx4g` 与容器 `cgroup` 限制 3.5g 冲突；修正后用步骤 4 检查端口，又发现 9848 端口被上一次异常退出的残留进程占用。两个问题叠加，仅凭单一步骤无法完全解决，印证了 6 步诊断需**依次执行、逐层排除**的必要性。

### 启动失败排查常见误区

1. **只读 Spring 外围异常**：`BeanCreationException` 只是包装，真正的根因在其 `Caused by` 链最深层；务必展开完整堆栈。
2. **混淆"启动成功"与"服务就绪"**：Nacos 启动日志出现 `Started Nacos in xxx seconds` 只代表 Spring 容器完成，gRPC 端口监听与各模块初始化可能仍在进行；确认就绪应通过健康接口或查看 `logs/start.out` 末尾是否出现监听完成标志。
3. **忽略 `nacos.home` 配置**：多个环境（开发/测试/生产）混用时，若 `nacos.home` 指向错误目录，会导致数据/日志写入错误位置，制造"看似启动成功但行为异常"的假象。

### 设计模式分析

1. **模板方法模式（Template Method）**：Spring Boot 的 `SpringApplication.run()` 封装了环境准备、Bean 定义、容器刷新、发布事件等固定启动流程，各模块只需实现各自的条件装配回调，复用了统一启动骨架。
2. **观察者模式（Observer）**：`NacosApplicationListener` 监听 Spring 启动事件（如 `ApplicationEnvironmentPreparedEvent`），在不同生命周期阶段执行 Nacos 特有初始化，实现了框架启动流程与业务初始化的解耦。

此外，Nacos 启动失败排查还应形成「症状-原因-验证」闭环记录：每次成功定位一个启动问题后，将错误日志关键字、根因与解决动作沉淀到团队知识库。随着问题样本不断积累，可将 6 步诊断逐步收敛为更短的判据，例如仅凭首个堆栈类名即可命中常见根因表，从而显著缩短下一次同类故障的解决时间。

### 小结

Nacos 启动失败可归纳为 JVM 参数、JDK 版本、端口、配置、数据库、权限 6 类原因。通过「内存→JDK→日志→端口→配置→权限」的 6 步诊断流程，即可逐层定位根因。核心是**以启动日志最上层用户代码异常为根因入口**，避免被 Spring Boot 的外围包装异常干扰判断。需要特别说明的是，6 步之间并非完全独立，实践中常出现多个失败点叠加（如内存超限与端口残留并存），因此应严格依次执行而非根据直觉跳跃，才能完整排除所有诱因，确保一次修复到位。这也正是本节点名强调"6 步诊断"而非给出单一捷径的原因——启动故障的多样性决定了流程化排查的必要性。

---

## 14.2 UnknownHostException：地址服务器域名不可达的完整排查 + 3 种解决方案

### 设计背景

`UnknownHostException` 是 Nacos 启动或运行期常见的基础网络异常，表现为客户端无法解析 Nacos Server 地址，或 Nacos Server 无法解析其依赖的域名（如自定义地址服务器 `address-server`）。该异常本质是"域名→IP"解析失败，可能由 DNS 配置缺失、`/etc/hosts` 未配置、网络隔离、地址服务器不可达等原因引起。由于该异常发生在 TCP 建连之前，症状往往"看似能 ping 通但服务无法访问"，具有较强迷惑性。

### 核心类关系图（地址解析链路）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│            UnknownHostException 解析失败链路                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  客户端 / Server                                                        DNS  │
│  ┌─────────────┐    调用     ┌──────────────┐    查询     ┌──────────────┐  │
│  │ Nacos SDK   │───getByName─▶│  InetAddress │────▶        │  DNS Server   │  │
│  │ / Server    │             │  (JDK)       │───────▶     │  或 /etc/hosts│  │
│  └─────────────┘             └──────────────┘  UnknownHost │              │  │
│        │                          │           ◀───异常     └──────────────┘  │
│        │                          ▼                                         │
│        │                 ┌──────────────────┐                               │
│        └────────────────▶│ 抛 UnknownHost   │  DNS 未配置/域名错误/          │
│                          │  Exception       │  hosts 缺项 → 解析失败         │
│                          └──────────────────┘                               │
│            图 14-2：UnknownHostException 解析失败链路                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 完整排查路径

**现象**：客户端启动报 `UnknownHostException: nacos-server` 或 `UnknownHostException: address-server.nacos.com`。

**第 1 步：确认域名拼写与端口。**
```bash
# 检查客户端配置的 server-addr
grep -E "server-addr|SERVER_ADDR" /path/to/config/application.yml
# 确认地址格式（host:port，多个逗号分隔）
```
判定：地址拼写错误（如少点、多空格）是最简单也最易忽略的原因。

**第 2 步：验证系统 DNS 解析。**
```bash
nslookup nacos-server
dig nacos-server
# 或直接使用 JDK 同款解析
getent hosts nacos-server
```
判定：若 `nslookup` 能解析而应用仍报 `UnknownHostException`，可能是应用运行容器（Docker）与宿主机 DNS 配置不一致。

**第 3 步：检查 `/etc/hosts`。**
```bash
cat /etc/hosts
# 确认是否包含 nacos-server 的映射
# 在 Docker 场景下还要检查容器内 /etc/hosts 是否注入
```
判定：`/etc/hosts` 缺项在隔离内网中会导致 DNS 无法解析，是最常见的根因之一。

**第 4 步：检查网络隔离与防火墙。**
```bash
# 测试 53 端口 DNS 可达
nc -vz <dns-server> 53
# 测试 Nacos Server 业务端口
nc -vz nacos-server 8848
```
判定：若业务端口可达但 DNS 端口不可达，说明网络策略阻断了 DNS 查询。

### 3 种解决方案

**方案 1：配置 `/etc/hosts` 静态映射。**
适用于内网/容器环境，将域名与 IP 写入 hosts：
```bash
# /etc/hosts 增加一行
192.168.1.101 nacos-server
192.168.1.101 address-server.nacos.com
```
优点：不依赖外部 DNS，解析稳定；缺点：IP 变更需同步维护 hosts。

**方案 2：修正/配置 DNS 服务器。**
适用于有统一 DNS 的集群：
```bash
# /etc/resolv.conf 配置正确的 DNS
nameserver 10.0.0.53
# 或在 Docker 环境通过 --dns 指定
docker run --dns=10.0.0.53 nacos/nacos-server:2.5.3
```
优点：域名全局一致，IP 变更无感知；缺点：依赖 DNS 高可用。

**方案 3：在客户端调整 server-addr 为 IP 直连。**
适用于小型环境，避免域名解析，直接使用 IP:
```yaml
# application.yml
spring:
  cloud:
    nacos:
      server-addr: 192.168.1.101:8848
```
优点：彻底规避解析问题；缺点：IP 变更需改配置，不适合弹性伸缩。

**选型建议**：集群规模小或环境固定采用方案 1/3；生产集群统一采用方案 2 建立 DNS，并配合健康检查保证 DNS 高可用，同时保留 `/etc/hosts` 作为降级兜底。

### Trade-off 分析

**DNS 统一解析 vs `/etc/hosts` 静态映射**：

| 维度 | DNS 统一解析 | /etc/hosts 静态映射 |
|------|-------------|--------------------|
| 可维护性 | 高（集中管理，IP 变更无感） | 低（每节点独立维护） |
| 高可用性 | 依赖 DNS 集群（需自建 HA） | 不依赖外部（天然稳定） |
| 变更延时 | 受 DNS TTL 影响 | 即时生效 |
| 弹性伸缩 | 支持（新节点自动解析） | 不支持（需手动加映射） |
| 排障成本 | 中（DNS 故障需查 resolver） | 低（静态映射直接定位） |

生产环境的推荐组合是"DNS 为主 + hosts 为兜底"：DNS 承担日常解析，`/etc/hosts` 仅在 DNS 故障时提供降级，兼顾可维护性与稳定性。

### 源码走读：客户端地址解析与 Address Server 机制

Nacos 客户端在解析服务端地址时，会判断配置的来源类型。当使用 `server-addr` 时直接使用静态地址；当配置了 `endpoint`（地址服务器）时，客户端会先从 Address Server 拉取可用于连接的 Server 列表。该机制的关键类位于客户端 `naming`/`config` 的 `NacosNamingService` 与底层 `ServerListManager`：

```java
// ServerListManager 地址解析逻辑（client 模块, Nacos 2.5.3, 节选）
public class ServerListManager {
    // 从地址服务器 endPoints 拉取可用 server 列表
    private void initServerAddr(String serverAddrs) {
        // server-addr 方式：直接解析静态地址
        // endpoint 方式：记录地址服务器地址，启动后由定时任务拉取动态列表
    }
}
```

**重要排查维度**：当使用 `endpoint` 模式时，客户端启动即向地址服务器发起 HTTP 请求获取服务器列表。若该请求失败或地址服务器域名不可解析，客户端会在初始化阶段抛出 `UnknownHostException` 或连接失败异常。因此：

- **排查端点确认**：客户端配置的是 `server-addr` 还是 `endpoint`。若是 `endpoint`，除检查 Nacos Server 域名外，还需检查地址服务器域名——两者都可能触发 `UnknownHostException`，但解决路径完全不同。
- **地址服务器高可用**：若使用 `endpoint` 模式，地址服务器本身是单点（或需自建集群），其故障会直接影响所有依赖它的客户端。生产建议优先使用 `server-addr` + 域名，或将地址服务器纳入监控。

**日志定位技巧**：`UnknownHostException` 抛出时，可通过堆栈确认异常发生的模块——若堆栈指向 `ServerListManager` 或地址拉取线程，则为 `endpoint` 模式问题；若指向 `NacosNamingService` 的连接初始化，则多为 `server-addr` 直连解析失败。

### 客户端 DNS 缓存问题

Java 应用默认会对 DNS 解析结果做缓存（`networkaddress.cache.ttl` 默认受 JVM 策略控制，可为负值表示永久缓存）。这在 Nacos 场景下会引发**"域名解析成功过但 IP 变更后仍解析到旧 IP"**的隐蔽问题：

```bash
# JVM 启动参数，控制 DNS 缓存 TTL（秒）
-Dsun.net.inetaddr.ttl=30
-Dsun.net.inetaddr.negative.ttl=10
```

- `sun.net.inetaddr.ttl`：正向解析缓存存活时间（秒）。设置过小会频繁解析增大 DNS 压力，过大则 IP 变更后客户端长期使用旧地址。
- `sun.net.inetaddr.negative.ttl`：解析失败结果的缓存时间（秒），用于避免解析失败时频繁重试。

**生产建议**：设置为适中值（如正向 30-60s、负向 10s），并在 DNS/IP 变更后通过 `jcmd <pid> VM.system_properties` 确认实际生效的 TTL。若怀疑客户端因缓存用了旧 IP，可临时调整 DNS 解析策略后重启验证。这是 `UnknownHostException` 排查中"看似连接上但实际用的旧地址"类问题的关键突破口，也是 14.2 节完整排查路径的补充维度。

### 生产案例：容器 DNS 与宿主机配置不一致导致的解析故障

某微服务容器化部署后，部分服务启动报 `UnknownHostException: nacos-config`，但宿主机通过 `nslookup` 能正常解析。排查发现：容器使用 `--dns=127.0.0.1` 指向宿主机的 Docker 内嵌 DNS，而该 DNS 未配置对 `nacos-config` 的转发；宿主机系统 DNS 虽正常，但容器网络栈并未复用宿主机的 `/etc/hosts`。此时按 14.2 的步骤 2/3 在宿主机验证均为正常，容易误判为"不是 DNS 问题"。

**正确排查姿势**是在**容器内**执行诊断命令，而非宿主机：
```bash
# 进入容器后执行
docker exec -it <container> getent hosts nacos-config
docker exec -it <container> cat /etc/resolv.conf
docker exec -it <container> cat /etc/hosts
```
最终通过在容器启动参数中指定 `--add-host=nacos-config:192.168.1.101` 注入映射解决。该案例印证了 14.2 尾句的要点——**解析环境差异是此类问题的隐蔽根因，必须在故障发生的同一网络命名空间内排查**。

### 设计模式分析

Nacos 的地址解析机制蕴含了**策略模式与外观模式的组合设计**。`Properties` 配置中 `server-addr`（静态地址）与 `endpoint`（地址服务器）对应两种不同的地址获取策略，客户端通过配置选择解析方式，体现了策略的可插拔替换；而 Address Server 机制进一步承担了**统一地址汇聚与分发**的角色——多节点列表由一个中心地址伺服器统一对外暴露，客户端仅需请求该中心即可获得完整集群地址，降低了客户端对集群拓扑的耦合，是典型的"集中式外观 + 策略路由"设计。理解这一设计，排查时就能区分"静态地址配置错误"与"地址服务器返回异常"两类不同根因。

### 小结

`UnknownHostException` 的根因是"域名→IP"解析失败，排查应遵循"拼写→DNS→hosts→网络"的递进顺序。三种解决方案各适用不同场景，生产推荐"DNS 为主 + hosts 兜底"组合。核心要点是**区分应用容器与宿主机的解析环境差异，在故障发生的同一网络命名空间内排查**，避免在宿主机验证正常就误判问题不在 DNS。

综合来看，`UnknownHostException` 的处置核心可总结为三点：一是**确认解析环境**（应用容器 vs 宿主机），这是最容易踩坑的一环；二是**按四步顺序排除**（拼写、DNS、hosts、网络），避免凭直觉跳步；三是**区分地址来源**（`server-addr` 静态地址与 `endpoint` 地址服务器），二者触发同类异常但解决路径不同。配合 JVM DNS 缓存 TTL 的合理设置，可系统性地降低此类解析类故障的发生概率与排查成本，为 Nacos 集群的稳定运行提供第一道网络层的保障。


---

## 14.3 配置不生效排查：4 步排查流程图（控制台检查→客户端订阅→MD5 对比→长轮询超时）

### 设计背景

"配置已发布但客户端未生效"是 Nacos Config 最典型的故障场景。该问题根因通常不在配置发布本身，而在**客户端订阅链路**的某一环断裂：可能是客户端未订阅该 dataId，可能是客户端使用本地缓存（`isUseLocalConfig`）而未拉取服务端，也可能是长轮询链路异常导致 MD5 无法同步。本节给出标准化的 4 步排查路径。

### 核心类关系图（配置发布→订阅→生效链路）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│          Nacos 配置"发布→订阅→生效"完整链路与断点                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  服务端                                 客户端                              │
│  ┌────────────────────┐               ┌─────────────────────────┐          │
│  │ ConfigInfoService  │──发布──▶      │ ClientWorker            │          │
│  │ (DB 持久化)       │    ↓           │  (长轮询线程池)          │          │
│  └────────┬───────────┘   ┌─────────┐ │   ┌───────────────────┐ │          │
│           ▼               │ MD5     │ │   │ CacheData         │ │          │
│  ┌────────────────────┐   │ 对比    │ │   │ (本地缓存+md5)     │ │          │
│  │ LongPollingService │◀──┴────────1│◀───│ ClientWorker       │ │          │
│  │ (addLongPolling)   │              │   │ 轮询 compareMd5     │ │          │
│  └────────┬───────────┘              │   └───────┬───────────┘ │          │
│           ▼                           │           ▼             │          │
│  变更通知 → 客户端重新拉取             │   LocalConfigInfo      │          │
│  ◆断点1:是否发布成功                   │   Processor(本地缓存)   │          │
│  ◆断点2:客户端是否订阅                 │   ◆断点3:MD5是否一致    │          │
│  ◆断点4:长轮询是否正常                 │   ◆断点4:本地缓存优先    │          │
│             图 14-3：配置生效链路与 4 个断点                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4 步排查流程

**步骤 1：控制台确认配置已发布。**
```bash
# 通过 OpenAPI 查询配置是否存在及其 MD5
curl -s 'http://localhost:8848/nacos/v1/cs/configs?dataId=application.properties&group=DEFAULT_GROUP' | md5sum
# 对比期望值，若返回 404 / 内容为空说明未发布成功
```
判定：若配置在控制台可见且 MD5 正确，进入步骤 2；否则问题在发布端（见第 13 章 config-server.log 排查）。

**步骤 2：确认客户端已订阅该 dataId。**
```java
// 客户端需显式订阅，否则不会拉取
configService.getConfig("application.properties", "DEFAULT_GROUP", 5000);
// 或用 @ConfigurationProperties / @Value 绑定
```
判定：若客户端从未调用 `getConfig` 或没有对应监听器，即使发布也无法自动生效。

**步骤 3：对比服务端与客户端 MD5。**
```bash
# 服务端 MD5（步骤1已获取）
# 客户端本地缓存（默认 ~/.nacos/config/ 或 snapshot 目录）MD5
cat ~/.nacos/config/DEFAULT_GROUP/application.properties 2>/dev/null | md5sum
```
判定：两者 MD5 不一致说明客户端缓存是旧值，尚未同步——进入步骤 4 查长轮询链路。

**步骤 4：检查长轮询是否触发变更通知。**
在 `config-server.log` 中检索长轮询与变更日志：
```bash
# 变更通知日志
grep "notify config change" ${nacos.home}/logs/config-server.log | tail
# 长轮询超时记录（约每 30s 一轮）
grep "long polling timeout" ${nacos.home}/logs/config-server.log | tail
```
`CacheData`（`client/.../CacheData.java`）的本地缓存标识 `isUseLocalConfig`（第 140 行）尤为关键——当客户端配置了本地快照且服务端不可达时，会读取本地缓存并标记为 `isUseLocalConfig=true`，此时即使服务端有新值也不会拉取：

```java
// client/src/main/java/com/alibaba/nacos/client/config/impl/CacheData.java:140 (Nacos 2.5.3)
private volatile boolean isUseLocalConfig = false;
```
若 `isUseLocalConfig=true`，需排除"客户端本地快照优先"导致的服务端更新不生效。

### 源码走读：长轮询变更检测

客户端 `ClientWorker` 通过后台拉取任务周期性携带所有订阅项的 MD5 向服务端 `config/v1/cs/configs/listener` 发起长轮询：

```java
// config/src/main/java/com/alibaba/nacos/config/server/service/LongPollingService.java:171-215 (Nacos 2.5.3)
public void addLongPollingClient(HttpServletRequest req, HttpServletResponse rsp, Map<String, String> clientMd5Map,
        int probeRequestSize) {
    // 1. 先用客户端携带的 MD5 与服务端当前值对比
    List<String> changedGroups = MD5Util.compareMd5(req, rsp, clientMd5Map);
    if (changedGroups.size() > 0) {
        // 2. 有变更 → 立即返回变更的 dataId
        generateResponse(req, rsp, changedGroups);
        return;
    }
    // 3. 无变更 → 挂起请求（AsyncContext），等待变更或超时
    final AsyncContext asyncContext = req.startAsync();
    asyncContext.setTimeout(0L);
    // 4. 按客户端请求的 Long-Polling-Timeout 调度长轮询任务
    ConfigExecutor.executeLongPolling(
            new ClientLongPolling(asyncContext, clientMd5Map, ip, probeRequestSize, timeout, appName, tag));
}
```

该方法的 `MD5Util.compareMd5` 是配置感知的核心——**服务端通过逐项对比客户端上报的 MD5 与本地最新 MD5 快速判定哪些 dataId 已变更**，命中即立即响应，避免无变更时也空轮询返回。这也解释了为何步骤 3 的 MD5 对比是定位根因的关键。

### Trade-off 分析

**服务端主动推送 vs 客户端长轮询拉取**：

| 维度 | 服务端主动推送 | 客户端长轮询（Nacos 采用） |
|------|--------------|--------------------------|
| 实时性 | 即时 | 受轮询周期限制（约秒级） |
| 服务端压力 | 高（推送风暴风险） | 低（按需响应变更） |
| 连接开销 | 长连接常驻 | 长连接 + 周期性请求 |
| 实现复杂度 | 中（需推送管理） | 高（需 MD5 对比 + 挂起管理） |
| 可靠性 | 依赖推送可达性 | 变更可重试拉取（更稳） |

Nacos 默认采用服务端"变更时立即响应 + 无变更挂起"的长轮询，兼顾实时性与服务端压力。该设计的风险在于**若长轮询线程池饱和或挂起请求丢失，客户端将收不到变更通知**——这正是 14.4 节深入分析的长轮询超时问题。

### 源码走读：发布端链路与变更通知

"配置不生效"的另一半根因可能出现在**发布端**——即使客户端订阅链路正常，若发布端的持久化或变更通知环节异常，客户端同样收不到新值。发布端的关键类包括 `ConfigCacheService`（内存缓存服务）与 `ConfigChangePublisher`（变更发布器）：

```java
// config/src/main/java/com/alibaba/nacos/config/server/service/ConfigCacheService.java (Nacos 2.5.3, 节选)
// 发布配置时：更新内存缓存并将变更广播给 LongPollingService
public static boolean publishConfig(String dataId, String group, String tenant, byte[] content, String tag) {
    // 1. 计算新的 MD5
    // 2. 更新 CacheItem 中的内容与 MD5
    // 3. 若内容有变化，广播 ConfigDataChangeEvent 事件
}
```

`ConfigChangePublisher` 通过事件总线（`NotifyCenter`）发布 `ConfigDataChangeEvent`，由 `LongPollingService` 订阅该事件并唤醒对应挂起的长轮询请求，从而告知客户端"配置有变"。若该事件分发链路异常（事件丢失、订阅者未注册），即使配置已写入数据库，客户端也感知不到变更。

**排查技巧**：发布端与客户端两个视角需结合验证。若服务端日志显示 `ConfigChangePublisher` 已发布事件，但客户端 `CacheData` 的 MD5 未更新，问题在客户端拉取环节；反之若服务端根本没发布事件，问题在发布端持久化或事件总线。

### 多环境配置与 namespace 隔离

"配置不生效"还常由**多环境（namespace/group）配置混淆**引起。客户端加载配置时会带上 `namespace`/`group`/`dataId` 三元组，任一组台不匹配就加载到错误配置或空配置：

```bash
# 客户端配置
spring.cloud.nacos.config.namespace=dev          # 命名空间（namespace ID）
spring.cloud.nacos.config.group=DEFAULT_GROUP    # 分组
spring.cloud.nacos.config.name=application.properties  # dataId
```

排查时若出现"测试环境能用、生产不能"，优先检查客户端配置的 `namespace` 是否指向了正确环境。Nacos 控制台发布配置时也要确认选中了正确的命名空间与分组，避免"发布到了 dev，生产客户端读不到"这类低级的配置隔离问题。

**本地缓存干扰**：客户端默认将拉取到的配置快照落盘（`~/.nacos/config/{namespace}/{group}/{dataId}`），用于服务端不可达时的降级读取。若此快照恰好存在且客户端处于"读取本地缓存"状态（`isUseLocalConfig=true`），即使服务端有新值也不会主动拉取。排查时清理该缓存目录并重启客户端，可排除此干扰因素。

### 源码走读：ClientWorker 长轮询拉取循环

客户端 `ClientWorker` 是长轮询的执行主体，其内部维护一个后台线程池，定时对每个订阅的 `CacheData` 执行配置监听检查。核心方法 `executeConfigListen()` 决定哪些订阅项需要参与长轮询请求：

```java
// client/src/main/java/com/alibaba/nacos/client/config/impl/ClientWorker.java:853-891 (Nacos 2.5.3, 节选)
public void executeConfigListen() throws NacosException {
    Map<String, List<CacheData>> listenCachesMap = new HashMap<>(16);
    long now = System.currentTimeMillis();
    boolean needAllSync = now - lastAllSyncTime >= ALL_SYNC_INTERNAL;
    for (CacheData cache : cacheMap.get().values()) {
        synchronized (cache) {
            checkLocalConfig(cache);
            if (cache.isConsistentWithServer()) {
                cache.checkListenerMd5();
                if (!needAllSync) {
                    continue;
                }
            }
            // 若使用本地配置，跳过服务端监听
            if (cache.isUseLocalConfigInfo()) {
                continue;
            }
            // 否则加入本次长轮询请求的监听集合
            if (!cache.isDiscard()) {
                List<CacheData> cacheDatas = listenCachesMap.computeIfAbsent(
                        String.valueOf(cache.getTaskId()), k -> new LinkedList<>());
                cacheDatas.add(cache);
            }
        }
    }
    boolean hasChangedKeys = checkListenCache(listenCachesMap);
}
```

这段代码揭示了"配置不生效"的多个关键分支：

1. **`cache.isUseLocalConfigInfo()` 直接跳过**：若 `CacheData` 处于"使用本地配置"状态，**根本不会发起服务端监听**，服务端如何变更都无法到达。这与 14.3 步骤 3 提到的本地缓存干扰完全对应。

2. **`cache.isConsistentWithServer()` 优先校验**：只有与服务端一致的 `CacheData` 才会 `checkListenerMd5()`；若不一致（本地是旧值），则该订阅项会被加入下一次轮询的待更新集合，触发重新拉取。

3. **`ALL_SYNC_INTERNAL` 全量同步机制**：即使各配置项均一致，也每隔 `ALL_SYNC_INTERNAL` 周期做一次全量校验，作为对增量长轮询的兜底，避免长时间无变化导致个别配置悄悄失效。

### 完整排障流程对照表

将 4 步排查的每一步对应到可执行命令、判定标准与根因结论，形成可直接落地的排查清单：

| 步骤 | 检查内容 | 命令/操作 | 判定标准 | 对应根因 |
|------|---------|----------|---------|---------|
| 1 | 发布端配置存在性 | 控制台/`curl /v1/cs/configs` | 配置存在且 MD5 正确 | 发布失败 / 发布错 env |
| 2 | 客户端订阅 | 检查是否调用 `getConfig`/加监听 | 已订阅对应 dataId | 未订阅 |
| 3 | MD5 对比 | 对比服务端与 `~/.nacos/config` 快照 | 服务端新值 ≠ 本地快照 | 本地缓存优先（`isUseLocalConfig`） |
| 4a | 长轮询链路 | `config-server.log` 查变更/轮询日志 | 有 `notify config change` 但客户端未变 | 推送/拉取链路断 |
| 4b | 命名空间隔离 | 核对客户端 `namespace/group/dataId` | 三元组与发布端一致 | 多环境配置混淆 |
| 4c | 本地快照干扰 | 清理 `~/.nacos/config` 缓存目录 | 清理后重启能拉到新值 | 本地快照优先 |

> 表格中 4a/4b/4c 是对步骤 4 的细化，分别覆盖链路异常、环境隔离、缓存干扰三类细分根因，使排查不漏项。

### 设计模式分析

**观察者模式（Observer）**：客户端对每个订阅的 dataId 维护 `CacheData`，服务端通过长轮询变更通知充当发布者。当 MD5 变化时，`CacheData` 状态更新并触发监听器的 `receiveConfigInfo` 回调，实现客户端配置的热更新。`CacheData` 在此扮演"被观察目标"，监听器扮演"观察者"。

### 小结

配置不生效的排查应严格按"发布→订阅→MD5→长轮询"4 步递进。多数情况下，根因集中在**客户端未订阅、本地缓存优先（`isUseLocalConfig`）、长轮询链路异常**三者之一。借助 `config-server.log` 中的变更通知与长轮询记录，可快速区分"服务端未变更"与"客户端未收到"两类问题。

---

## 14.4 长轮询超时排查：客户端增大 configLongPollTimeout + clientWorker 线程堆栈分析

### 设计背景

Nacos Config 的长轮询机制（详见 14.3）依赖客户端 `ClientWorker` 保持长轮询请求。当服务端长轮询任务调度延迟、网络超时、客户端线程池饱和时，会出现"配置长时间未生效"或"客户端反复重连"现象，通常表现为 `ClientWorker` 日志中的 `longPolling timeout` 频繁或 `NacosException: connection is closed`。本节分析长轮询超时的根因与两种标准解法。

### 核心类关系图（长轮询超时链路）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│             长轮询超时：客户端线程与超时点                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  客户端 ClientWorker                          服务端 LongPollingService      │
│  ┌──────────────────────────┐                 ┌──────────────────────────┐  │
│  │ 长轮询线程池              │───HTTP/Grpc──▶  │ ClientLongPolling 任务   │  │
│  │ (定时拉取线程)            │   请求          │ (挂起, 等待变更)         │  │
│  └──────────────────────────┘                 └──────────────────────────┘  │
│              │                                        │                     │
│              │  configLongPollTimeout                 │  变更/超时后响应       │
│              │  (客户端超时,默认30s)                    │                      │
│              ▼                                        ▼                      │
│  ┌──────────────────────────┐                 ┌──────────────────────────┐  │
│  │ 若超时→重连/重拉          │◀───响应─────── │ 生成 Response             │  │
│  │ clientWorker 线程堆栈     │                 └──────────────────────────┘  │
│  │ 分析 (jstack)             │                                             │
│  └──────────────────────────┘                                             │
│  ◆超时点A:客户端 configLongPollTimeout 过小                                 │
│  ◆超时点B:服务端 LongPollingService 调度延迟                                │
│            图 14-4：长轮询超时链路与超时点                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 长轮询超时的两类根因

**根因 A：客户端 `configLongPollTimeout` 配置过小。**
Nacos 客户端允许通过 `LongPollingTimeout` 或系统属性 `configLongPollTimeout` 设置长轮询超时（默认约 30s）。若将其调得过小（如 5s），客户端在服务端尚未返回任何变更时即超时，导致反复发起无意义的长轮询请求，既增加服务端压力又可能因频繁重建连接而丢变更。典型日志：
```
[ClientWorker] longPolling timeout, re-poll... dataId=DEFAULT_GROUP@@application.properties
```

**根因 B：服务端 `LongPollingService` 挂起任务调度延迟。**
当服务端长轮询线程池（`ConfigExecutor.executeLongPolling`）饱和或 `ClientLongPolling` 挂起时间过长时，变更通知无法及时送达。此时客户端表现为"配置已发布但迟迟不生效"，`clientWorker` 线程长期位于等待状态。

### 源码走读：长轮询超时的服务端调度

`LongPollingService` 中，无变更的请求被包装为 `ClientLongPolling` 并挂起：

```java
// config/src/main/java/com/alibaba/nacos/config/server/service/LongPollingService.java (Nacos 2.5.3, 节选)
class ClientLongPolling implements Runnable {
    private final AsyncContext asyncContext;
    private final Map<String, String> clientMd5Map;
    private final long timeout;   // 挂起时长（基于客户端 Long-Polling-Timeout 与延迟调整）
    ...
    
    @Override
    public void run() {
        // 在超时窗口内等待变更；超时或收到变更通知后生成响应
        try {
            asyncContext.setTimeout(timeout);
            // 注册到 longPolling 管理器，供 ConfigChangePublisher 变更时唤醒
        } catch (Exception e) {
            // 发送错误响应
        }
    }
}
```

`timeout` 的计算在 `addLongPollingClient` 中（第 202-208 行）：`Math.max(minLongPoolingTimeout, 请求的 Long-Polling-Timeout - delayTime)`，其中 `delayTime` 默认 500ms，用于提前返回避免客户端超时。若服务端因调度延迟未能在窗口内响应，客户端即超时重连。

### 排查与解决

**解法 1：调大客户端 `configLongPollTimeout`。**
```properties
# 客户端配置，将长轮询超时从默认 30s 调大
nacos.config.long-polling.timeout=30000   # 或通过构造参数 ConfigServiceProps 设置
# Java 系统属性方式
-DconfigLongPollTimeout=30000
```
适用于根因 A：确保客户端等待窗口足够覆盖服务端调度与网络往返时间。

**解法 2：`clientWorker` 线程堆栈分析。**
当怀疑线程池饱和时，抓取线程快照定位 `ClientWorker` 线程状态：
```bash
jstack <client_pid> | grep -A 20 "ClientWorker"
# 关注状态：RUNNABLE（正常拉取）/ WAITING/BLOCKED（可能在长轮询挂起或锁竞争）
```
核心观察点：
- 若大量 `ClientWorker` 线程处于 `WAITING` 状态且长时间无进展，说明可能在 `LongPollingService` 挂起等待；结合服务端日志确认是否批量超时。
- 若线程数持续增长且不可回收，存在**线程池饱和**风险，需检查长轮询线程池配置。

**进一步排查服务端长轮询线程池**：
```bash
# 观察服务端 long-polling 线程池线程数与等待队列
jstack <nacos_server_pid> | grep -c "LongPollingService"
# 查询服务端连接健康
curl -s 'http://localhost:8848/nacos/v1/console/health/liveness' | jq
```

### 超时重连机制与变更恢复

理解超时后的**恢复行为**对评估影响至关重要。当客户端长轮询超时后，并不会立即丢失配置，而是：

1. **超时返回空响应**：服务端在窗口内无变更时返回空，客户端认为"无变更"，保留本地缓存继续使用。
2. **客户端重新发起下一轮长轮询**：`ClientWorker` 周期性重建长轮询请求，下一轮若配置已有变更则能拉到新值。
3. **变更丢失只在极端窗口**：仅当变更恰好发生在"上一轮超时返回"与"下一轮请求发出"之间，且服务端基于上一轮 MD5 判定无变更，才可能出现短暂的配置感知延迟——但这种延迟最多不超过一个长轮询周期，绝大多数场景可接受。

**生产场景影响评估**：长轮询超时通常不会造成配置永久丢失，而是带来**变更感知延迟**与**服务端压力上升**（频繁重建连接）。真正需要紧急处理的是两种情况：
- 长轮询线程池饱和导致大量请求被拒绝，变更长时间无法送达（根因 B 严重形态）；
- 客户端过度频繁超时重连，冲击服务端造成雪崩。

**降级保护**：即便长轮询完全失效，客户端 `ClientWorker` 还有 `checkLocalConfig` 与本地快照兜底——服务端不可达时读取 `~/.nacos/config` 快照，保证服务可用但配置可能为旧值。因此生产应通过监控及时发现长轮询异常，而非等到业务侧反馈"配置不生效"才发现。

### Trade-off 分析

**长轮询超时调大 vs 调小**：

| 维度 | 超时调大（如 60s） | 超时调小（如 10s） |
|------|------------------|------------------|
| 变更感知实时性 | 中（变更依赖服务端提前响应） | 高（短周期重新拉取） |
| 服务端压力 | 低（长挂起少请求） | 高（频繁重连建连） |
| 网络抖动容忍 | 高（长等待窗口） | 低（易超时重连） |
| 变更丢失风险 | 低 | 中（重连窗口可能丢变更） |
| 生产推荐 | 连接稳定的内网 | 网络抖动频繁的高可用场景 |

生产内网场景推荐保留默认或适度调大；公网/跨机房场景可结合断线重连机制适当调整，并依赖客户端 `checkLocalConfig` 本地缓存兜底。

### 线程堆栈分析实战

当怀疑服务端长轮询线程池饱和时，可进一步用火焰图或多次采样定位线程堆积的形态：

```bash
# 采集两次线程快照（间隔5s），对比 LongPollingService 线程状态变化
jstack <nacos_server_pid> > /tmp/ts1.txt && sleep 5 && jstack <nacos_server_pid> > /tmp/ts2.txt
# 统计两次快照中 ClientLongPolling 相关线程的 WAITING/等待时长特征
grep -c "ClientLongPolling" /tmp/ts1.txt /tmp/ts2.txt
```

判读要点：
- 若两次快照中 `ClientLongPolling` 相关线程数量持续增加且大量处于 `WAITING (on object monitor)`，说明挂起的长轮询请求在不断堆积，线程池接近饱和。
- 若线程数量稳定但单个请求等待极长（远超配置的超时窗口），说明服务端调度延迟严重，需检查线程池大小与 `ConfigExecutor` 配置。

### 服务端长轮询线程池配置

生产环境长轮询线程池规模直接影响变更交付能力。相关配置位于 Nacos 服务端，涉及长轮询任务执行的线程池容量：

```properties
# conf/application.properties
# 长轮询相关任务线程池（默认值以官方文档为准）
# 关注 CoreSize / MaxSize / QueueCapacity
```

当 `ClientLongPolling` 线程数逼近 MaxSize 且队列已满时，新增长轮询请求会被拒绝或排队，表现为客户端长时间收不到变更。此时除扩大线程池外，还应排查**是否注册了超量的配置监听**（单客户端 dataId 数量过多），从源头降低长轮询并发。

### 设计模式分析

长轮询机制采用**异步挂起 + 事件驱动**的设计：服务端通过 `AsyncContext` 将长时间无变更的请求挂起（异步化），再通过 `ConfigChangePublisher` 发布的事件唤醒对应请求。这本质是**观察者模式 + 异步回调**的组合——`ClientLongPolling` 以观察者身份订阅配置变更事件，变更发生时被唤醒并返回响应，避免无变更时占用带宽与连接。挂起（`asyncContext.setTimeout(0L)`）与唤醒的异步模型，是 Nacos 在"实时感知"与"资源消耗"之间取得平衡的关键设计。

### 小结

长轮询超时排查聚焦两个根因维度：客户端 `configLongPollTimeout` 过小导致过早超时重连、服务端长轮询线程池调度延迟导致变更交付慢。处理时先判断根因 A（客户端窗口小）还是根因 B（服务端交付慢），再分别采取调大超时、分析线程堆栈、扩容线程池等对应措施，并在生产保持长轮询指标监控以防患于未然，确保在任何节点出现异常时都能被及时发现并处置，最大限度压缩配置感知延迟对业务的影响，保障配置中心的整体稳定性。解法上，客户端侧调大超时窗口，服务端侧通过 `clientWorker`/`LongPollingService` 线程堆栈分析定位线程池饱和。核心是把"客户端窗口"与"服务端调度"视为一个整体链路协同调优。


---

## 14.5 服务注册异常排查：4 步排查命令（curl 查实例→grep 心跳→curl 健康→手动注册测试）

### 设计背景

"服务注册失败"在微服务链路中会造成服务消费者无法发现提供者，直接影响业务可用性。注册异常可能源于客户端网络不通、服务端拒绝、健康检查误判、实例容量超限等多种原因。本节给出标准化的 4 步排查命令，自下而上逐层定位"实例是否到达服务端→心跳是否正常→健康状态是否 OK→手动注册能否复现"。

### 核心类关系图（服务注册→健康→发现链路）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│         服务注册→心跳保活→健康判定→服务发现 完整链路                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  提供者                                Nacos Naming                          │
│  ┌───────────────┐  registerInstance  ┌───────────────────────────────────┐│
│  │ 业务服务        │───────▶          │ InstanceOperatorClientImpl        ││
│  │ (Nacos Client) │                    │  (注册入口, v2)                   ││
│  └───────────────┘                    │         │                          ││
│        │  心跳周期 (5s)                │         ▼                          ││
│        │  ┌──────────────────┐        │  ClientBeatProcessorV2            ││
│        │  │ 心跳发送 (Beat)   │───────▶│  (更新 lastHeartBeatTime)         ││
│        │  └──────────────────┘        │         │                          ││
│        └────────────────────────────▶ │         ▼                          ││
│                                       │  ClientBeatCheckTaskV2            ││
│            ◆步骤1:curl查实例否已注册     │  (超时判定→置不健康/摘除)        ││
│            ◆步骤2:grep 心跳记录          │                                  ││
│            ◆步骤3:curl健康状态          └──────────────────────────────────┘│
│            ◆步骤4:手动注册复现          ▲ 心跳超时→健康检查→服务发现更新      │
│            图 14-5：注册健康发现链路与 4 个排查点                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 注册与健康检查类型基础

理解 Nacos 的健康检查机制是排查注册异常的前提。Nacos 2.5.3 将实例分为两类，健康管理方式完全不同：

| 实例类型 | 健康检查方式 | 主动探测 | 保活机制 |
|---------|-------------|---------|---------|
| **临时实例**（ephemeral） | 客户端心跳保活 | 无主动探测 | 客户端周期发送心跳，超时自动摘除 |
| **持久实例**（persistent） | 服务端主动健康检查 | 是（TCP/HTTP/MySQL） | 服务端周期探测，需显式注销 |

持久实例的健康检查由服务端主动发起，通过 `HealthCheckProcessorV2` 及其处理器委托（`HealthCheckProcessorV2Delegate`）选择具体实现：

```java
// naming/.../healthcheck/v2/processor/HealthCheckProcessorV2Delegate.java (Nacos 2.5.3, 节选)
public class HealthCheckProcessorV2Delegate implements HealthCheckProcessorV2 {
    // 依据实例配置选择对应健康检查处理器
    private final Map<String, HealthCheckProcessorV2> healthCheckProcessorMap;
    
    @Override
    public void process(HealthCheckTaskV2 task) {
        // 根据健康检查类型（TCP/HTTP/MySQL/None）分发到对应处理器
        HealthCheckProcessorV2 processor = healthCheckProcessorMap.get(task.getCheckType());
        processor.process(task);
    }
}
```

`HealthCheckProcessorV2Delegate` 支持 `TcpHealthCheckProcessor`、`HttpHealthCheckProcessor`、`MysqlHealthCheckProcessor`、`NoneHealthCheckProcessor` 等策略（`naming/.../healthcheck/v2/processor/` 下）。**排查注册异常时，需区分实例类型**：临时实例查心跳日志，持久实例查服务端健康检查日志，二者排查入口不同。

### 4 步排查命令

**步骤 1：curl 查询实例是否已注册。**
```bash
# 查询指定服务的全部实例
curl -s 'http://localhost:8848/nacos/v1/ns/instance/list?serviceName=example-service'
# 只看健康实例
curl -s 'http://localhost:8848/nacos/v1/ns/health/service?serviceName=example-service'
```
判定：若返回列表为空或无目标实例 IP，说明实例未注册成功，进入步骤 2；若实例存在但 `healthy=false`，问题在心跳/健康判定。

**步骤 2：grep 服务端心跳日志。**
```bash
# 检索该实例的心跳接收记录
grep "example-service" ${nacos.home}/logs/naming-server.log | grep -E "beat|register" | tail -20
```
判定：若日志显示 `register instance` 但无后续 `beat`，说明客户端只注册了一次但心跳中断——排查客户端长连接与心跳线程。

**步骤 3：curl 检查健康状态。**
```bash
# 查询服务的健康实例数量与全部实例
curl -s 'http://localhost:8848/nacos/v1/ns/health/service?serviceName=example-service' | jq
# 检查指标：healthyInstanceCount / instanceCount
```
判定：若 `instanceCount>0` 但 `healthyInstanceCount=0`，多为心跳超时导致实例被标记不健康，进入步骤 4 与 14.6 节心跳排查。

**步骤 4：手动注册复现。**
```bash
# 用 curl 手动注册一个测试实例，验证服务端是否能接收
curl -X POST 'http://localhost:8848/nacos/v1/ns/instance'   -d 'ip=192.168.1.200&port=18080&serviceName=example-service&ephemeral=true'
# 再查询确认
curl -s 'http://localhost:8848/nacos/v1/ns/instance/list?serviceName=example-service' | jq
```
判定：若手动注册成功且健康，说明问题在客户端注册逻辑或网络；若手动注册也失败，说明服务端配置（容量/鉴权/命名空间）存在问题。

### 源码走读：心跳处理与健康恢复

Nacos 2.5.3 中，心跳到达后由 `ClientBeatProcessorV2`（`naming/.../healthcheck/heartbeat/ClientBeatProcessorV2.java`）处理，其核心是刷新 `lastHeartBeatTime` 并恢复健康状态：

```java
// naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatProcessorV2.java:52-76 (Nacos 2.5.3)
@Override
public void run() {
    String ip = rsInfo.getIp();
    int port = rsInfo.getPort();
    String serviceName = NamingUtils.getServiceName(rsInfo.getServiceName());
    Service service = Service.newService(namespace, groupName, serviceName, rsInfo.isEphemeral());
    HealthCheckInstancePublishInfo instance = (HealthCheckInstancePublishInfo) client.getInstancePublishInfo(service);
    if (instance.getIp().equals(ip) && instance.getPort() == port) {
        instance.setLastHeartBeatTime(System.currentTimeMillis());
        if (!instance.isHealthy()) {
            instance.setHealthy(true);
            Loggers.EVT_LOG.info("service: {} {POS} {IP-ENABLED} valid: {}:{}@{} ...",
                    rsInfo.getServiceName(), ip, port, rsInfo.getCluster());
            NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service));
            NotifyCenter.publishEvent(new HealthStateChangeTraceEvent(...));
        }
    }
}
```

注意 `instance.setLastHeartBeatTime(System.currentTimeMillis())`：心跳每刷新一次，该时间戳就被更新。而 `ClientBeatCheckTaskV2` 周期性检查 `lastHeartBeatTime` 与当前时间的差值，超过心跳超时阈值（默认 15 秒，与 `clientBeatInterval` 相关）即判定实例不健康或移除。**因此"注册成功但健康状态为 false"几乎总是由心跳中断引起**，这正是步骤 2/3 的排查重点。

### Trade-off 分析

**临时实例心跳（ephemeral） vs 持久实例（persistent）**：

| 维度 | 临时实例（ephemeral） | 持久实例（persistent） |
|------|---------------------|----------------------|
| 数据一致性 | AP（Distro 最终一致） | CP（Raft 强一致） |
| 健康管理 | 心跳超时自动摘除 | 需人工/注册方管理 |
| 故障恢复 | 进程退出自动移除 | 需显式注销 |
| 适用场景 | 微服务动态实例（推荐默认） | DNS/有状态服务 |

注册异常排查需先确认实例是临时还是持久：临时实例依赖心跳保活，心跳断即失联；持久实例不依赖心跳，注册即长期存在。大多数微服务场景采用临时实例，因此"心跳中断导致实例消失/不健康"是最常见根因。

### 源码走读：健康检查与超时判定

`ClientBeatCheckTaskV2`（`naming/.../healthcheck/heartbeat/ClientBeatCheckTaskV2.java`）是判定实例健康与否的核心定时任务：

```java
// naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatCheckTaskV2.java (Nacos 2.5.3, 节选)
@Override
public void run() {
    // 遍历该连接下所有发布的服务
    client.getAllPublishedService().forEach(service -> {
        // 检查每个临时实例的 lastHeartBeatTime 是否超龄
        if (out of threshold(instance.getLastHeartBeatTime())) {
            // 心跳超时 → 视为不健康或移除
            Loggers.SRV_LOG.warn("client beat {} expired, removing...", ipPort);
            // 从注册表中移除实例并广播变更
        }
    });
}
```

**超时判定基准**：`lastHeartBeatTime` 由 `ClientBeatProcessorV2` 每次心跳刷新。`ClientBeatCheckTaskV2` 周期性（默认约 5 秒一个检查周期）对比该时间与当前时间，超过心跳间隔的倍数阈值即判定超时。若客户端心跳因网络抖动、GC 停顿短暂中断超过阈值，健康实例可能被误摘除——这也是"注册成功突然变不健康再恢复"的常见原因，排查时需结合网络与 GC 情况综合判断。

### 注册异常的容量与配置维度

除心跳外，注册异常还有两类容易被忽略的根因：

**1. 实例容量超限**。服务端对单服务实例数、总实例数有容量限制，超过上限后新注册会被拒绝。排查命令：
```bash
# 观察注册拒绝相关日志
grep -iE "over|limit|exceed|capacity" ${nacos.home}/logs/naming-server.log | tail -20
```
判定：若出现容量超限日志，需检查 `max-size`、实例数相关配置，或通过水平扩展/清理闲置实例降低容量压力。

**2. 命名空间/分组不匹配**。客户端注册时携带 `namespace`/`group`/`serviceName`，若与服务消费者查询时使用的三元组不一致，会导致"已注册但查不到"。例如提供者注册在 `namespace=dev`，而消费者在 `namespace=prod` 查询，自然无法发现。排查时确认两端的三元组完全一致。

**3. gRPC 端口未连通**。Nacos 2.x 注册走 gRPC 长连接（默认 9848/9849），若客户端可访问 HTTP 8848 但 gRPC 端口被防火墙/网络策略阻断，注册也会失败。排查：
```bash
# 测试 gRPC 端口连通性（比 HTTP 更严格）
nc -vz nacos-server 9848
```
判定：HTTP 可通但 gRPC 不通，是"注册时连不上"类报错的隐蔽根因。

### 设计模式分析

`ClientBeatProcessorV2` 实现了 `BeatProcessor` 接口，配合 `NotifyCenter` 事件总线，构成**命令模式 + 观察者模式**的组合：`ClientBeatProcessorV2` 作为命令对象封装"处理一次心跳"的动作；心跳处理完成后通过 `NotifyCenter.publishEvent` 发布 `ServiceEvent.ServiceChangedEvent` 事件，服务发现消费者作为观察者接收事件并更新本地缓存。

### 小结

服务注册异常按"实例是否注册→心跳是否正常→健康是否 OK→手动复现"4 步排查。核心论断：**临时实例"注册成功但不健康"几乎总由心跳中断引起**，定位重点在 `naming-server.log` 的心跳记录与 `ClientBeatCheckTaskV2` 的判定逻辑。同时需延伸排查容量超限、命名空间不匹配、gRPC 端口未连通三类配置与网络维度，避免遗漏隐藏根因。此外还应记住：持久实例与临时实例的健康管理机制不同，持久实例依赖服务端主动探测（TCP/HTTP/MySQL），排查时应依据实例类型选择正确的心跳日志或健康检查日志入口，方可快速锁定问题所在。总体而言，将 4 步排查命令与实例类型、容量、命名空间、端口等维度结合，即可形成覆盖注册异常全链路的系统排查方案。实际生产中，多数注册异常都能通过这套方法在分钟级内定位到根因，从而快速恢复服务可用性，减少因注册异常对业务链路造成的持续性影响，保障微服务架构的整体稳定与业务的连续性，这也是注册中心运维的核心目标所在。

---

## 14.6 客户端心跳排查：gRPC 长连接心跳链路源码走读 + 手动心跳检测代码

### 设计背景

客户端心跳是 Nacos 临时实例保活的底层机制，也是 14.5 节"注册成功但不健康"问题的根因所在。Nacos 2.x 采用 gRPC 长连接承载注册与心跳：客户端建立 gRPC 连接后，通过周期性发送"心跳"维持连接活性，服务端据此刷新实例的 `lastHeartBeatTime`。本节走读 gRPC 长连接心跳的完整源码链路，并给出手动心跳检测的代码。

### 核心类关系图（gRPC 长连接心跳链路）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│            Nacos 2.x gRPC 长连接心跳链路                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  客户端                                   服务端                              │
│  ┌────────────────────┐                 ┌──────────────────────────────┐    │
│  │ NamingGrpcClientProxy│──建连──▶       │ GrpcBiStreamRequestAcceptor  │    │
│  │ (注册+心跳代理)      │   gRPC 双向流   │  (接收客户端请求)            │    │
│  └────────┬───────────┘                 └───────────┬──────────────────┘    │
│           │ 定时心跳                                  │                      │
│           ▼                                          ▼                      │
│  ┌────────────────────┐                 ┌──────────────────────────────┐    │
│  │ HeartbeatTask      │──ClientBeat──▶  │ ClientBeatProcessorV2        │    │
│  │ (周期发送 Beat)     │                 │  (刷新 lastHeartBeatTime)    │    │
│  └────────────────────┘                 └───────────┬──────────────────┘    │
│           ▲                                          │                      │
│           │ 断线→重连                                ▼                      │
│           │                                     ┌──────────────────────┐    │
│           └─────────────────────────────────────│ ClientBeatCheckTaskV2 │    │
│                        心跳超时→摘除/不健康      │ (超时判定)           │    │
│                                                 └──────────────────────┘    │
│            图 14-6：gRPC 长连接心跳链路                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 源码走读：心跳的发送与处理

**客户端心跳发送**。客户端通过 gRPC 双向流维持连接，注册的实例由其内部的注册表周期性发送心跳。心跳到达服务端后，最终由 `ClientBeatProcessorV2`（在 14.5 节已走读）刷新 `lastHeartBeatTime`。服务端侧对每个 gRPC 连接，`ClientBeatCheckTaskV2` 负责超时判定：

```java
// naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatCheckTaskV2.java (Nacos 2.5.3, 节选)
public class ClientBeatCheckTaskV2 implements Runnable {
    private final IpPortBasedClient client;
    
    @Override
    public void run() {
        // 遍历该连接下所有临时实例
        client.getAllPublishedService().forEach(service -> {
            List<Instance> instances = client.getInstancePublishInfo(service).getAllInstance();
            // 判定条件: 当前时间 - lastHeartBeatTime > 心跳超时阈值
            boolean healthy = isHealthy(instance);
            if (!healthy) {
                // 移除不健康实例或标记
                handleUnhealthyInstance(service, instance);
            }
        });
    }
}
```

**心跳超时阈值**源自心跳间隔。Nacos 临时实例的 `clientBeatInterval` 默认约 5000ms（5 秒），服务端按心跳间隔的倍数（约 3 倍，即 15 秒）判定超时。若客户端因 GC 停顿、网络中断、线程阻塞导致心跳延迟超过阈值，实例即被标记不健康。

### 手动心跳检测代码

在排查"客户端心跳异常"时，可用如下 Java 代码模拟一次心跳，验证服务端接收与健康恢复：

```java
// 手动心跳检测：向 Nacos 发送一次心跳验证服务端响应
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;

String serverAddr = "127.0.0.1:8848";
NamingService naming = NamingFactory.createNamingService(serverAddr);

// 手动注册一个临时实例（随后会由服务端等待其心跳）
naming.registerInstance("manual-heartbeat-test", "192.168.1.200", 18080);

// 手动发送心跳（通过客户端 API 触发 beat）
// Nacos NamingService 不直接暴露单次 beat API，故用底层注册表验证：
// 观察服务端 naming-server.log 是否出现 "manual-heartbeat-test ... beat"
System.out.println("触发心跳后, 请观察服务端日志确认 lastHeartBeatTime 更新");
```

**手动检测的用途**：通过注册 + 观察日志，可区分"服务端不接收心跳"与"客户端不发送心跳"。若手动注册后服务端日志持续出现该实例的 beat 记录且保持健康，说明服务端正常，问题在业务客户端；若服务端收不到 beat，则需检查客户端 gRPC 连接与心跳线程。

**验证健康恢复**：手动注册的测试实例会自动启动其内部的心跳机制（`registerInstance` 后客户端 SDK 会周期发送心跳）。因此可通过观察该测试实例的 `lastHeartBeatTime` 持续刷新来判断服务端心跳处理链路是否完好。若测试实例一直保持健康，而业务实例不健康，则问题定位在**业务客户端**的注册配置或心跳线程；若两者都不健康，问题在**服务端**处理环节。

**清除测试实例**：验证完成后，应及时注销测试实例避免污染注册表：
```java
// 验证完成后注销测试实例
naming.deregisterInstance("manual-heartbeat-test", "192.168.1.200", 18080);
```

### 心跳诊断的关键指标

在持续观测心跳健康度时，可结合 Prometheus 指标与日志频率综合判断（相关指标定义见第 13 章）：

| 观察维度 | 指标/日志 | 正常表现 | 异常表现 |
|---------|----------|---------|---------|
| 心跳接收速率 | `naming-server.log` 的 beat 记录频率 | 各实例 beat 稳定 | 某实例 beat 消失 |
| 健康实例占比 | `nacos_monitor` 相关健康指标 | 接近 100% | 持续下降 → 心跳中断 |
| gRPC 连接数 | `grpc_connections_total` | 稳定 | 骤降 → 连接批量断开 |
| 心跳超时摘除 | 健康检查日志 | 极少 | 频繁 → 阈值过小或网络抖动 |

**诊断要点**：心跳异常的判定不应只看单点日志，而应结合"连接数、心跳频率、健康占比"三个指标联动分析。例如连接数骤降 + 健康占比下降，说明是 gRPC 连接批量断开导致的批量失联；仅健康占比缓慢下降而连接数稳定，则可能是单个实例的心跳线程问题。

### Trade-off 分析

**心跳间隔大小权衡**：

| 维度 | 心跳间隔小（如 2s） | 心跳间隔大（如 15s） |
|------|-------------------|--------------------|
| 健康感知实时性 | 高（失联快发现） | 低（失联延迟发现） |
| 服务端压力 | 高（心跳请求频繁） | 低 |
| 网络与 CPU 开销 | 高 | 低 |
| 误摘除风险 | 低（容错窗口小） | 高（网络抖动易超时） |

Nacos 默认 5s 心跳间隔在实时性与开销间取得平衡。生产环境若网络抖动频繁，可适度调大 `clientBeatInterval`（如 8-10s）并为服务端健康判定预留充分倍数窗口，避免因瞬时网络抖动误摘除健康实例。

### gRPC 长连接的心跳保活与断线重连

Nacos 2.x 客户端与服务端的注册/心跳全部承载于 **gRPC 双向流长连接**（默认端口 9848/9849）。理解长连接的保活与断线重连机制，是排查心跳异常的关键前提。

**长连接保活**：gRPC 双向流建立后，客户端与服务端通过周期性的流量维持连接活性。若连接长期无数据，双方可能因 TCP 超时误判断开。实际上，客户端对每个注册的临时实例周期发送心跳（默认 5s），这些心跳本身就是 gRPC 双向流内的活跃流量，间接起到保活作用。因此**心跳中断常与长连接断开同时发生**——两者互为因果。

**断线重连**：当 gRPC 连接意外断开（网络闪断、服务端重启、GC 停顿超时），客户端会触发重连逻辑，重新建立双向流并**重放已注册实例的心跳**。若重连失败持续累积，客户端本地缓存的服务注册信息会逐渐失效，最终导致服务端实例被判定不健康并摘除。

**排查命令**：`remote-server.log` 记录 gRPC 连接的全生命周期，是排查心跳/连接问题的核心日志：
```bash
# 检索连接建立/断开记录
grep -E "new gRPC bi-stream|connection established|connection disconnect" ${nacos.home}/logs/remote-server.log | tail -30
# 检索断线重连
grep -iE "reconnect|retry" ${nacos.home}/logs/remote-server.log | tail -20
```

### 心跳异常的日志与排查要点整理

综合来看，心跳异常排查可按以下清单逐项核对，避免遗漏：

| 排查维度 | 检查内容 | 检查日志/命令 | 典型结论 |
|---------|---------|--------------|---------|
| 连接是否建立 | gRPC 双向流是否建立 | `remote-server.log` 查 `new gRPC bi-stream` | 连接未建立 → gRPC 端口/网络问题 |
| 心跳是否到达 | 服务端是否收到 Beat | `naming-server.log` 查 `beat` | 无 beat → 客户端心跳线程问题 |
| 超时判定阈值 | `lastHeartBeatTime` 是否刷新 | 健康检查相关日志 | 时间戳不更新 → 心跳被截断 |
| 网络抖动 | gRPC 连接是否频繁断开 | `remote-server.log` 查 disconnect | 频繁断开 → 网络不稳定或 GC 停顿 |
| 客户端线程 | 心跳线程是否阻塞 | 客户端 `jstack` 定位心跳线程 | 线程 WAITING/BLOCKED → 线程池问题 |

**核心排查原则**：先确认 gRPC 连接健康（`remote-server.log`），再确认心跳是否到达（`naming-server.log`），最后结合超时判定逻辑判断是"未收到"还是"未刷新"。这两份日志的交叉比对，可快速定位断点发生在客户端发送、网络传输还是服务端处理环节。

### 设计模式分析

gRPC 长连接心跳链路体现了**生产者-消费者模式**：客户端作为生产者周期生产心跳请求，服务端 `ClientBeatProcessorV2` 作为消费者消费并刷新实例状态。同时，心跳的超时判定通过 `ClientBeatCheckTaskV2` 这一**定时任务**周期性扫描，与心跳刷新形成"写入-校验"的回环，确保实例健康状态始终被主动维护而非被动等待。

### 小结

客户端心跳是临时实例保活的底层机制，链路为"gRPC 长连接发送 Beat → ClientBeatProcessorV2 刷新 lastHeartBeatTime → ClientBeatCheckTaskV2 超时判定"。排查心跳异常的关键是**区分服务端不接收（服务端问题）与客户端不发送（网络/线程问题）**，手动心跳检测代码可有效验证服务端接收能力。同时需结合 `remote-server.log` 的 gRPC 连接记录与 `naming-server.log` 的心跳记录交叉比对，并理解长连接保活与断线重连机制，才能完整覆盖心跳异常的各类根因。


---

## 14.7 集群脑裂排查：3 步检查命令（cluster/nodes→raft/leader→DistroVerify）

### 设计背景

集群脑裂（Split Brain）指因网络分区导致集群被分割为多个互不可见的分区，各分区可能分别选举出 Leader，从而出现多 Leader 状态。Nacos 混合使用 Raft（CP，用于服务状态/配置持久化等一致性数据）和 Distro（AP，用于临时实例）两种协议，脑裂的表现与根因也随之复杂。本节给出 3 步检查命令，通过节点视图→Leader 视图→数据校验逐层确认脑裂状态。

### 脑裂的成因与 Nacos 的协议特性

脑裂（Split Brain）的机制根源是**网络分区**：当集群节点被网络故障分割为多个互不连通的子集时，每个子集仍各自运行，可能对同一逻辑资源产生冲突决策。在 Nacos 中，脑裂具体表现为：

1. **Raft 组多 Leader**：正常时一个 Raft 组只有一个 Leader。分区后，若两个分区各自拥有达到多数派的条件（或配置不当导致各分区都能选主），就会产生两个 Leader，对同一个状态的写入产生分歧。
2. **节点视图分裂**：各分区内的节点只能看到本分区成员，`/cluster/nodes` 返回的成员列表不一致。
3. **临时数据不一致**：Distro（AP）协议下，临时实例数据在各节点异步同步；若节点间长期不可达，不同分区的临时注册数据无法收敛，消费者查询结果不一致。

**重要区分**：Raft 的"多数派选举"天然具备抑制脑裂的能力——只要多数派在线，少数派无法独立选主。因此 Nacos 出现"双 Leader"通常意味着**选举配置异常、节点奇偶数量设计不当、或网络恢复过程中出现短暂的双主窗口**，而非 Raft 机制本身失效。理解这一区别，是判断脑裂严重程度与恢复策略的前提。

### 核心类关系图（脑裂检测三视图）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              集群脑裂检测：三层视图                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  视图1: 节点列表              视图2: Raft Leader            视图3: 数据校验    │
│  ┌─────────────────┐         ┌─────────────────┐          ┌──────────────┐  │
│  │ /cluster/nodes  │         │ /raft/leader    │          │ DistroVerify  │  │
│  │ 每个节点 IP+状态  │         │ 各组 Leader 地址 │          │ 数据一致性     │  │
│  └────────┬────────┘         └────────┬────────┘          └──────┬───────┘  │
│           │ 正常状态=全UP             │ Leader一致=无脑裂         │ 校验失败=   │
│           ▼ 有DOWN/多分区            ▼ 多Leader=脑裂           │ 数据不一致  │
│           │                          │                           ▼           │
│  脑裂隐患↑                     脑裂确认↑                  Distro/Failed    │
│            图 14-7：脑裂检测的三层视图                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3 步检查命令

**步骤 1：`cluster/nodes` 检查节点状态。**
```bash
# 在集群中每个节点上分别执行，对比各自的节点列表视图
curl -s 'http://localhost:8848/nacos/v1/core/cluster/nodes' | jq '.nodes'
```
判定：脑裂的显著特征是**不同节点返回的节点列表不一致**——分区 A 的节点看不到分区 B 的节点，或看到对方状态为 DOWN。若各节点视图一致且全 UP，则基础连通性正常。

**步骤 2：`raft/leader` 检查 Leader 一致性。**
```bash
# 查询各 Raft 组的 Leader
curl -s 'http://localhost:8848/nacos/v1/core/raft/leader' | jq
```
判定：在正常单分区集群中，每个节点查询应返回**同一个 Leader**。若不同节点返回不同的 Leader（尤其当这些节点之间网络不可达时），即确认发生脑裂。这是最直接的多 Leader 判据。

**步骤 3：`DistroVerify` 检查临时数据一致性。**
```bash
# 观察 Distro 数据校验日志（是否频繁 verify fail）
grep -E "DISTRO-FAILED|verify data" ${nacos.home}/logs/nacos-cluster.log | tail -20
```
判定：`DistroVerifyExecuteTask`（`core/.../distro/task/verify/DistroVerifyExecuteTask.java`）周期校验各节点临时数据是否一致。若出现大量 `[DISTRO-FAILED] verify data ... failed`，说明节点间临时实例数据无法同步，是脑裂在数据层的体现。

### 具体排查命令与输出示例

为便于直接操作，下面给出三步骤的完整命令及预期输出，运维可逐条比对：

**步骤 1 输出示例**（对比两个节点的视图）：
```bash
# 节点 A 查询
curl -s 'http://nodeA:8848/nacos/v1/core/cluster/nodes' | jq '.nodes[].address'
# 输出: "nodeA:8848","nodeB:8848","nodeC:8848"（完整3节点）

# 节点 B 查询（若发生分区）
curl -s 'http://nodeB:8848/nacos/v1/core/cluster/nodes' | jq '.nodes[].address'
# 输出: "nodeB:8848","nodeC:8848"（缺少 nodeA → 视图分裂）
```

**步骤 2 输出示例**（多 Leader 判据）：
```bash
# 节点 A 查询 Leader
curl -s 'http://nodeA:8848/nacos/v1/core/raft/leader' | jq
# 节点 C 查询 Leader（若脑裂，二者指向不同地址）
curl -s 'http://nodeC:8848/nacos/v1/core/raft/leader' | jq
```
正常情况两个节点应返回相同 Leader 地址；返回不同地址即确认脑裂。

**步骤 3 输出示例**（Distro 校验失败）：
```bash
grep "DISTRO-FAILED" ${nacos.home}/logs/nacos-cluster.log | tail -5
# 形如: [DISTRO-FAILED] verify data for type ephermeralInstance to nodeC failed
```

### 源码走读：Raft 选举状态机与 Leader 切换

理解 Leader 选举的三种角色状态转换，有助于判断脑裂时的异常表现。JRaft 中每个节点处于以下状态之一：

```
┌─────────────────────────────────────────────────────────────────────┐
│              Raft 节点状态机（Follower / Candidate / Leader）          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Follower（跟随者）                                                │
│      │ 选举超时且未收到 Leader 心跳                                 │
│      ▼                                                              │
│   Candidate（候选者）                                               │
│      │ 获得多数派投票确认                                           │
│      ▼                                                              │
│   Leader（领导者）                                                  │
│      │ 持续向 Follower 发送心跳维持任期                             │
│      └──网络分区→多数派无法确认→降级回 Candidate/Follower           │
│                                                                     │
│   脑裂场景: 两个分区各自形成 Candidate，若都获得本分区多数票          │
│   → 产生两个 Leader（双 Leader 脑裂）                               │
└─────────────────────────────────────────────────────────────────────┘
```

**状态转换对排查的启示**：当 `raft/leader` 查询结果显示 Leader 频繁在节点间切换（而非稳定指向某一节点），说明存在持续的选举抖动，多由节点间网络不稳定或选举超时过短引起；只有当两个分区**各自稳定**维持一个 Leader 时，才是真正的脑裂。前者处理网络/参数，后者需执行 14.8 节的脑裂恢复。脑裂恢复的关键在于先明确当前状态属于三种情况中的哪一种，再对症处理，避免盲目操作加重数据分叉。

### 源码走读：Raft Leader 查询与脑裂判定基础

`JRaftServer` 中 Leader 查询通过 `RouteTable.selectLeader` 完成：

```java
// core/src/main/java/com/alibaba/nacos/core/distributed/raft/JRaftServer.java:369-372 (Nacos 2.5.3)
protected PeerId getLeader(final String raftGroupId) {
    // 从路由器表中获取该 Raft 组的 Leader
    return RouteTable.getInstance().selectLeader(raftGroupId);
}
```

正常时所有节点对同一 group 查询到的 Leader 指向同一 Peer。脑裂时，因网络分区导致各分区独立选举，`RouteTable` 中不同分区的节点各自缓存了不同的 Leader，从而出现"同一 group 多 Leader"的异常状态。这也是为何脑裂必须以"多节点对比"而非单节点结果来判断——单节点查询只能反映该分区视角。

`JRaftServer` 的选举超时配置默认 5 秒（`RAFT_ELECTION_TIMEOUT_MS`，第 166-178 行），网络抖动若超过该值会频繁触发选举，加剧误判为脑裂的风险。排查时需区分"真正的多 Leader 脑裂"与"短期选举导致的 Leader 切换抖动"——前者稳定持续，后者短暂反复。

### Trade-off 分析

**Raft 严格选主 vs 网络分区容忍**：

| 维度 | 强一致（严格单 Leader） | 分区容忍（网络抖动可重选） |
|------|----------------------|--------------------------|
| 脑裂风险 | 低（Pre-Vote 防多主） | 中（频繁重选易误判） |
| 可用性 | 低（需多数派在线） | 高 |
| 选举抖动 | 高（Leader 切换成本） | 低 |
| 适用场景 | 一致性数据（配置/状态） | 关注可用性场景 |

Nacos Raft 采用 CP 语义，以"多数派选举"保证单 Leader，从机制上抑制脑裂，但代价是分区时的可用性下降（少数派不可写）。这就是为什么生产上排查脑裂首先要区分：**是网络真分区（多数派不可达导致降级）还是局部故障（单节点问题）**。

### 脑裂对业务的实际影响

理解脑裂的破坏性，才能重视排查与预防。脑裂期间 Nacos 各模块受影响的严重程度不同：

| 数据/功能 | 影响 | 严重程度 |
|----------|------|---------|
| **持久配置**（CP，Raft） | 双 Leader 时写入分叉，不同分区读到不同配置 | 高（配置冲突危险） |
| **持久服务**（CP，Raft） | 注册/查询可能指向不同分区，数据不一致 | 高 |
| **临时实例**（AP，Distro） | 各分区临时数据无法同步，消费者发现结果分裂 | 中 |
| **健康检查** | 各分区对实例的健康判定不一致 | 中 |

其中**配置类数据**的脑裂风险最高，因为配置分叉可能导致部分节点运行在错误配置上，引发难以诊断的间歇性故障。这也是脑裂必须优先恢复、且在恢复后要做全量数据对账的原因。

### 脑裂演练与应急预案

脑裂属于低概率高影响故障，建议通过演练提前验证恢复预案：

1. **模拟分区**：通过 `iptables` 或断网命令临时隔离一个节点，观察其余节点是否出现 Leader 切换与视图分裂。
2. **验证判据**：在演练中确认 3 步检查命令能正确识别分区状态，固化判据与处理脚本。
3. **演练恢复**：按 14.8 节的 3 种情况演练恢复，重点验证"恢复后单 Leader 收敛"与"数据重新同步"。
4. **沉淀预案**：将演练结论固化为应急预案文档，明确"何种症状→执行何种恢复动作"的对应关系，并定期在变更窗口演练。

### 源码走读：Pre-Vote 与多数派选举机制

JRaft（Raft 的 Java 实现）通过 **Pre-Vote（预投票）机制**进一步降低脑裂风险。在正式发起选举前，候选节点先向集群成员发送预投票请求，只有获得"可能成为 Leader"的确认后才进入正式选举，避免因网络分区中的陈旧节点频繁干扰而导致无意义的选举轮次。相关配置在 `RaftConfig` 与 `JRaftServer` 初始化中：

```java
// core/src/main/java/com/alibaba/nacos/core/distributed/raft/JRaftServer.java:166-178 (Nacos 2.5.3)
// Set the election timeout time. The default is 5 seconds.
int electionTimeout = Math.max(ConvertUtils.toInt(config.getVal(RaftSysConstants.RAFT_ELECTION_TIMEOUT_MS), 
        DEFAULT_ELECTION_TIMEOUT), 1000);
nodeOptions.setElectionTimeoutMs(electionTimeout);
// 其余 Raft 运行时参数（含 Pre-Vote 配置）通过 RaftOptionsBuilder 组装
```

`RaftOptionsBuilder`（`core/.../raft/utils/RaftOptionsBuilder.java`）集中构建 JRaft 的 `RaftOptions`，其中包含 `disableCli`、`readOnlyOptions`、快照间隔等参数。正确配置这些参数（如合理的 `raft.worker_num`、副本管道、快照阈值）对维持长稳定的 Leader 选举至关重要——参数不当会在特定故障下加剧或触发脑裂。

### 脑裂的预防与监控维度

排查之后更重要的是预防。生产环境应从以下维度降低脑裂发生概率：

| 预防维度 | 具体措施 | 作用 |
|---------|---------|------|
| 节点数量 | 集群采用**奇数节点**（3/5/7），避免偶数节点导致选举僵局（如 2 节点各持 1 票无法达成多数） | 保证多数派可达成 |
| 网关/负载均衡 | 不要在 Nacos 节点间前置不一致的负载均衡或 NAT，避免节点间地址互相不可识别 | 保证节点间直连 |
| 网络隔离 | 为 Nacos 节点间通信与客户端通信分别规划网络策略 | 降低误隔离风险 |
| 监控告警 | 监控 Leader 单一性（同一 group 仅一个 Leader）与节点视图一致性 | 尽早发现脑裂 |
| 选举参数 | 合理设置 `RAFT_ELECTION_TIMEOUT_MS` 等，避免过短导致频繁选举抖动 | 降低抖动误判 |

**脑裂的判据优先级**：多节点 Leader 不一致（最直接）> 节点视图分裂（次之）> Distro 校验失败（间接）。实际排查应优先确认前两项，数据校验作为佐证。

### 设计模式分析

Nacos 的 Raft 模块采用**抽象工厂模式 + 策略模式**组织：`JRaftServer` 通过 `JRaftUtils.initRpcServer` 创建 RPC 服务，`RaftConfig`/`RaftOptionsBuilder` 负责集群配置构建；不同一致性场景（Naming、Config）可复用同一 Raft 框架，仅在分组与状态机构建上有所不同。

### 小结

脑裂排查通过"node 视图→leader 视图→数据校验"3 步确认。核心判据是**多节点查询 Leader 不一致（多 Leader）**与**节点列表视图分裂**。排查时须区分真脑裂与短期选举抖动，前者稳定持续需恢复操作，后者短暂反复可观察等待。脑裂的机制根源在 Nacos Raft 的多数派选主策略在分区下的可用性权衡，而其业务影响以配置类数据分叉最为危险，须在恢复后进行数据对账，并结合预防性节点数量规划与监控告警，从机制与运维两个层面共同降低复发风险，确保集群数据一致性与可用性。

---

## 14.8 脑裂恢复步骤：3 种情况处理（少数派隔离 / 多数派有 Leader / 双 Leader）

### 设计背景

确认脑裂（14.7 节判据）后，需根据分区规模与 Leader 状态选择恢复策略。脑裂恢复的核心目标是**重新收敛到单一 Leader、恢复集群多数派视图、同步被隔离分区的数据**。本节按 3 种典型情况给出恢复步骤与顺序。

### 恢复前的三问评估

动手恢复前，先回答三个问题，避免盲目操作：

**1. 是否仍在脑裂中？** 先确认网络分区是否已经自然恢复。若分区已恢复（节点间能互相 ping 通、节点视图重新一致），Raft 会自动触发新一轮选举与数据同步，此时不宜人工干预，只需观察收敛。若分区持续，再决定是否介入。

**2. 数据分叉程度如何？** 脑裂期间两个分区可能各自产生了新的写入（配置发布、服务注册等）。恢复前需评估两个分区的数据差异——若仅有临时实例数据（Distro，易重同步），修复成本低；若包含持久配置（CP，Raft），分叉可能导致配置冲突，需仔细合并。

**3. 恢复方式是重同步还是重选？** 根据是否有稳定的多数派 Leader，决定是"等网络恢复重同步"（情况 1/2）还是"人工选主并合并数据"（情况 3）。错误的选择可能使原本可自愈的故障被人为扩大。

### 核心类关系图（脑裂恢复决策树）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  脑裂恢复决策树                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  确认脑裂                                                                   │
│     │                                                                       │
│     ├─ 情况1: 少数派被隔离 ──▶ 保留多数派, 隔离区副本降级→数据以多数派为准      │
│     │                                                                       │
│     ├─ 情况2: 多数派有Leader ─▶ 确认Leader正常→隔离区重新加入→重同步数据      │
│     │                                                                       │
│     └─ 情况3: 双Leader ──────▶ 人为选定主Leader→停次Leader写→合并/丢弃      │
│                                隔离区数据→重启重收敛                            │
│                                                                             │
│  恢复后: 校验 /cluster/nodes 全UP, /raft/leader 单Leader,                  │
│           DistroVerify 无 FAILED                                              │
│            图 14-8：脑裂恢复决策树                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3 种情况的处理步骤

**情况 1：少数派被隔离（推荐先处理此情况）。**
当集群 3 节点中 1 个（或少数）节点被网络隔离，多数派仍可用：
1. **保留多数派作为权威**：继续以多数派（如 2 节点）的 Leader 为准，隔离节点不参与选举与写入。
2. **隔离节点降级处理**：若隔离节点短暂失联，等待网络恢复后自动重同步；若长期隔离或无法恢复，将该节点下线并从集群配置中移除。
3. **验证收敛**：网络恢复后确认该节点重新加入，状态变 UP。

**情况 2：多数派有 Leader（可自愈）。**
当多数派仍能正常选举出唯一 Leader，但少数派因分区暂时脱节：
1. **确认 Leader 正常**：`curl /raft/leader` 确认多数派内有唯一 Leader。
2. **修复网络分区**：恢复被隔离节点到多数派的连通性。
3. **自动/手动重同步**：隔离区节点重连后，通过 Raft 日志重放或 Distro 校验恢复一致数据。

**情况 3：双 Leader（需人工介入）。**
网络分区导致两分区各自选出 Leader 时，采取如下步骤（详细见决策树右侧）：
1. **人为选定主 Leader**：依据数据新鲜度与分区规模，指定权威主 Leader。
2. **停用次 Leader 写**：暂停被淘汰分区的写入，阻止数据继续分叉。
3. **合并/丢弃隔离数据**：比较两分区增量，无法自动合并的以主 Leader 为准。
4. **重启收敛**：重启被淘汰分区节点，重新加入主分区并重放日志。

下面以一张对照表进一步明确 3 种情况的处理要点与恢复途径：

| 情况 | 核心判断 | 处理要点 | 恢复途径 | 耗时预估 |
|------|---------|---------|---------|---------|
| 1. 少数派隔离 | 多数派仍有唯一 Leader | 保留多数派权威，隔离节点降级 | 网络恢复自动重同步 | 分钟级 |
| 2. 多数派有 Leader | 多数派稳定选主 | 修复分区，等待重连 | 自动重同步 | 秒级-分钟级 |
| 3. 双 Leader | 两分区各自有 Leader | 人工选主 + 停写次级 + 合并 | 人工收敛 + 重启 | 分钟级-小时级 |

**情况 1 与情况 2 的区分**：两者都保留多数派 Leader，区别在于**是否需人工干预**。情况 1 中被隔离的是少数派，多数派完全健康，通常可直接等网络恢复；情况 2 中多数派虽能选主，但需先修复网络分区才能让隔离节点重连。实践中二者常合并处理——先保多数派，再修网络，最后自动重同步。

**情况 3：双 Leader（需人工介入）。**
网络分区导致两分区各自选出 Leader（如 3 节点被分成 1+2 且 2 个分区各自有 Leader）：
1. **人为选定主 Leader**：根据节点数据新鲜度、分区规模，指定一个为权威主 Leader（通常选多数派、数据较新的分区）。
2. **停用次 Leader 的写权限**：暂停被淘汰分区的写入，避免双方数据继续分叉。
3. **数据合并/丢弃**：将次分区的增量数据与主分区比较，无法自动合并的以主分区为准。
4. **重启收敛**：重启被淘汰分区的节点，使其重新加入主分区并重放日志，最终收敛为单 Leader。

### 源码走读：路由表与 Leader 切换的恢复机制

恢复过程中，节点重新加入集群依赖 `JRaftServer` 的自我注册与路由表更新：

```java
// core/src/main/java/com/alibaba/nacos/core/distributed/raft/JRaftServer.java:350-360 (Nacos 2.5.3)
void registerSelfToCluster(String groupId, PeerId selfIp, Configuration conf) {
    // 将当前节点注册到 Raft 集群成员配置中
    // 网络恢复后，隔离节点通过该逻辑重新进入集群成员协商
}
```

同时 `getLeader`（第 369 行）依赖 `RouteTable.selectLeader`，当 Leader 异常或重新选举后，`RouteTable` 会更新 Leader 指向。恢复的关键在于**确保路由表与多数派协商结果一致**——若隔离节点携带陈旧的路由表重连，需等待其完成新一轮选举同步后才能作为权威数据源。

### 源码走读：成员变更与集群配置管理

脑裂恢复中经常涉及"下线故障节点""将隔离节点重新加入集群"，这一过程依赖 JRaft 的**成员变更（Membership Change）**机制。Nacos 通过 `JRaftMaintainService` 提供成员管理能力：

```java
// core/src/main/java/com/alibaba/nacos/core/distributed/raft/JRaftMaintainService.java (Nacos 2.5.3, 节选)
public CompletableFuture<Response> execute(Processor processor) {
    // 处理成员变更操作: 添加节点 / 移除节点 / 配置重装
    // 通过 JRaftOps 分发到具体的 Raft 操作处理器
    return JRaftOps.transfer(processor, raftServer);
}
```

JRaft 的成员变更采用**单节点变更（single-server change）**方式，即一次只添加或移除一个 Peer，避免一次变更多个成员导致多数派条件在过渡期失效。Nacos 基于此提供了运维视角的节点上/下线接口，恢复时可以：

- **移除长期失联节点**：将其从 Raft 集群成员中剔除，使其不再参与选举，恢复多数派稳定性。
- **重新加入恢复节点**：网络恢复后通过成员变更将节点重新加入，触发日志回放与数据重同步。

**运维提醒**：成员变更属于**高危操作**，操作不当可能引发新一轮选举或数据问题。应在确认节点真实故障且无法恢复时再执行移除；临时网络分区优先等待自动恢复，而非直接移除节点。

### 综合恢复案例：3 节点集群的网络分区处理

以 3 节点（A/B/C）集群为例，演示一次完整脑裂恢复：

**故障现象**：节点 A 与 B/C 之间网络中断，A 所在分区（1 节点）与 B/C 分区（2 节点）互相隔离。

**情况判定**：B/C 构成多数派，能维持唯一 Leader；A 为少数派被隔离 → 属于情况 1。

**恢复步骤**：
1. **确认多数派健康**：查询 B/C 的 `/raft/leader`，确认两者指向同一 Leader；查询 A 的视图，确认 A 看不到 B/C（区分隔离范围）。
2. **等待或修复网络**：若分区由临时抖动引起，等待自动恢复；若为交换机/防火墙问题，修复 B/C 与 A 的连通性。
3. **观察自动重同步**：网络恢复后，节点 A 重新加入集群，通过 Raft 日志回放补全其在隔离期间缺失的数据，状态从 DOWN 变回 UP。
4. **复核**：用 14.8 节的健康复核命令确认三个节点视图一致、Leader 唯一、Distro 无 FAILED。

该案例说明，多数派健康的情况下，脑裂恢复以"**修复网络 + 等待自愈**"为主，人工介入仅作为备选。

### Trade-off 分析

**自动恢复 vs 人工干预**：

| 维度 | 自动恢复（依赖 Raft 自愈） | 人工干预（双 Leader 场景） |
|------|--------------------------|--------------------------|
| 恢复速度 | 快（秒级自动重同步） | 慢（需人工判断与操作） |
| 数据安全性 | 依赖多数派协商 | 由人工权威决策保底 |
| 出错风险 | 中（自动合并可能不完全） | 低（人工可控） |
| 适用场景 | 情况 1/2（有唯一 Leader） | 情况 3（双 Leader） |

Nacos 设计上优先依赖 Raft 的自愈能力（多数派协商、断点重放）处理情况 1/2，仅在出现真正的双 Leader（情况 3）时才需人工介入。生产运维应**优先给网络分区自愈留时间窗口**，避免过早人工干预造成更多分叉；只有在双 Leader 持续或数据冲突严重时才强制执行人工收敛。

### 恢复后的健康复核与数据对账

脑裂恢复并非"网络通了就完事"，必须完成恢复后的健康复核与数据对账，确认集群真正回到一致状态：

```bash
# 1. 节点视图: 所有节点应从同一视角看到完整成员列表
for n in nodeA nodeB nodeC; do curl -s "http://$n:8848/nacos/v1/core/cluster/nodes" | jq '.nodes|length'; done
# 期望: 三个节点都返回 3

# 2. Leader 唯一性: 所有节点查询同一 group 应返回同一 Leader
for n in nodeA nodeB nodeC; do curl -s "http://$n:8848/nacos/v1/core/raft/leader" | jq -r 'to_entries[].value.leader'; done
# 期望: 三个查询结果指向同一地址

# 3. 临时数据一致: Distro 校验应不再失败
grep -c "DISTRO-FAILED" ${nacos.home}/logs/nacos-cluster.log
# 期望: 恢复后不再新增 FAILED 记录
```

**数据对账要点**：对于持久配置（CP）类数据，恢复后应比对各分区的配置列表，确认无分叉或已按主 Leader 收敛。必要时可通过配置文件一致性校验工具或重新发布配置来强制对齐。只有完成上述复核，才可判定集群真正恢复正常，并结束本次脑裂事件。

### 情况 3（双 Leader）的详细处理

双 Leader 是最危险的脑裂形态，处理顺序直接影响数据完整性。完整步骤如下：

1. **确认两个 Leader 的地址**：分别在两个分区各选一个代表节点执行 `/raft/leader`，记录两个 Leader 的地址，确认是否真的指向不同节点。
2. **评估数据新鲜度**：优先选择"数据更完整、节点数更多"的分区作为权威。可通过对比两分区的配置发布记录、服务注册时间戳判断数据新旧。
3. **降级次级 Leader**：在被淘汰的分区上，通过运维动作暂停其主 Leader 角色（可临时停止该分区 Leader 节点的写入，或将其从集群中隔离），阻止其继续产生新数据。
4. **合并数据**：将次级分区的增量数据与主分区合并。无法自动合并的冲突数据，以主分区为准。
5. **重启收敛**：重启被淘汰分区的所有节点，使其以跟随者身份加入主分区，通过日志重放收敛为单 Leader。
6. **完整复核**：执行健康复核命令，确认全局单 Leader、数据一致、无残留分区。

> **特别提醒**：双 Leader 恢复属于高风险操作，建议在操作窗口、并有回滚预案时进行。若不确定哪个分区数据更权威，宁可先停写两个分区再人工对账，也不要在数据未对齐时贸然合并，以免造成配置/服务数据大面积覆盖。

### 恢复后的监控与预案固化

脑裂恢复后，应将本次事件的处理经验固化为可持续的运维资产：

```bash
# 新增一条监控: 检测 Leader 唯一性（任一 group 出现多个 Leader 即告警）
curl -s 'http://localhost:8848/nacos/v1/core/raft/leader' | jq 'to_entries | length'
# 期望: 等于 Raft 组数量，每个组一个 Leader
```

同时建议建立脑裂专项告警与应急预案文档，明确：
- **告警规则**：Leader 数量异常、节点视图分裂、DISTRO-FAILED 高频等触发条件；
- **处置流程**：按"三问评估 → 判断情况 → 对应处理 → 健康复核"的标准流程；
- **演练计划**：定期通过网络隔离演练验证预案，确保团队熟悉操作。

### 常见恢复陷阱与规避

1. **过早人工干预**：网络分区尚未恢复就强制重启节点，可能触发更多选举，加剧混乱。应先观察等待，确认分区是否自愈。
2. **同时重启多节点**：脑裂恢复时应**逐节点**操作，避免同时重启造成集群瞬间失去足够成员而无法形成多数派。
3. **忽略数据对账**：只恢复网络不核对数据，可能让分叉配置继续存在，遗留隐患。恢复后务必完成数据对账。
4. **选主标准不清**：情况 3 人工选主时，若选了数据较旧的分区为主，会覆盖较新的数据。应优先选"数据较新、节点数较多"的分区为权威。

### 设计模式分析

脑裂恢复场景体现了**状态机模式（State Pattern）**：集群在不同阶段（完整→分区→恢复）处于不同状态，针对各状态采取不同处理策略；同时恢复过程依赖 Raft 的多数派协商与日志重放，本质是分布式共识状态机的状态迁移。运维的 3 种情况处理对应状态机中不同分支的状态转移路径。

### 小结

脑裂恢复按"少数派隔离 / 多数派有 Leader / 双 Leader"3 种情况分策：情况 1/2 优先依赖 Raft 自愈（保留多数派、等待重同步），情况 3（双 Leader）才需人工选定主 Leader 并收敛。核心原则是**先辨状态再动手，能自愈不干预，双 Leader 必须人工裁决**。恢复后需通过 14.7 节的 3 步命令复核单 Leader 与数据一致，并将处理经验固化为监控与应急预案，形成脑裂故障的完整处置闭环。


---

## 14.9 JVM 内存泄漏排查：jstat → jmap HeapDump → Eclipse MAT 分析

### 设计背景

Nacos Server 长期运行后可能出现堆内存持续增长、最终 OutOfMemoryError（OOM）的情况。内存泄漏不同于短暂的 GC 压力——泄漏意味着部分对象被错误持有而无法被回收，堆占用随时间单调上升。排查内存泄漏需要依次回答三问：**内存是否在涨（jstat）→ 堆里是什么（jmap dump）→ 谁持有它们（MAT 分析）**。本节给出完整的三步排查流程。

### 核心类关系图（内存泄漏三步排查）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│            JVM 内存泄漏排查：三步法                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  步骤1: jstat         步骤2: jmap HeapDump     步骤3: Eclipse MAT          │
│  ┌──────────────┐    ┌────────────────────┐   ┌─────────────────────────┐ │
│  │ 观察 O/FGC/   │    │ 导出 hprof 堆快照   │   │ 分析 Dominator Tree     │ │
│  │ O 持续增长?   │    │ (生产慎用,会FGC)    │   │ 定位 Retained Heap 大   │ │
│  │ E/S/O 区占比  │    │ heapdump.hprof      │   │ 对象及 GC Roots 引用链  │ │
│  └──────┬───────┘    └─────────┬──────────┘   └──────────┬──────────────┘ │
│         │ 发现增长/OOM         │ 抓到现场堆              │ 确认泄漏对象      │
│         ▼                      ▼                        ▼                  │
│  确认内存问题            捕获泄漏对象分布              定位持有者/修复点        │
│            图 14-9：内存泄漏三步排查法                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 步骤 1：jstat 确认内存增长趋势

```bash
# 每秒采样一次 GC 统计，连续多次
jstat -gcutil <nacos_pid> 1000 10
# 关注列: O(Old Gen 使用率), FGC(Full GC 次数), FGCT(FGC总时间)
```
判定标准：
- **O 持续上升且 Full GC 后不回落到基线** → 存在泄漏（正常对象在 FGC 后应被回收）。
- O 高位但 FGC 后可回落 → 多为堆内存过小或瞬时压力，调整 `-Xmx` 或优化缓存即可。
- 配合 GC 日志观察 `gc.log` 中的 Old 区回收效果，确认是否"回收不掉"。

### 步骤 2：jmap 导出 HeapDump 抓取现场

```bash
# 导出堆快照（注意: -dump:live 会先触发 Full GC, 生产慎用)
jmap -dump:live,format=b,file=/tmp/nacos_$(date +%Y%m%d_%H%M%S).hprof <nacos_pid>
# 查看堆概况（不触发 FGC）
jmap -histo <nacos_pid> | head -40
```
**抓取时机**：务必在 O 区高位尚未 OOM 时抓取，或配置 `-XX:+HeapDumpOnOutOfMemoryError` 让 JVM 在 OOM 时自动生成快照（推荐生产开启）。抓取过晚（已在 OOM 后）会丢失泄漏现场。

`-histo` 的快速观察：出现大量本该释放的对象（如 `java.util.concurrent.ConcurrentHashMap$Node`、Nacos 内部 Map 集合、`com.alibaba.nacos...` 实例）即提示特定模块有对象未能释放。

### 步骤 2 深入：常用 jmap/jcmd 诊断命令详解

除了基本的 `jmap -histo` 与 `jmap -dump`，以下命令对定位 Nacos 内存问题同样关键：

```bash
# 查看堆各分区实时使用与 GC 配置
jmap -heap <nacos_pid>
# 输出: Heap Configuration 的 -Xmx/-Xms, 各代容量, 以及 Used/Capacity

# 查看已加载类数与内存占用（排查类加载泄漏）
jcmd <nacos_pid> GC.class_histogram | head -40

# 查看 JVM 命令行参数（确认 GC 策略与堆配置是否生效）
jcmd <nacos_pid> VM.flags

# 查看线程与堆信息概览
jcmd <nacos_pid> VM.summary
```

**GC 日志分析**：配合 GC 日志（启动参数 `-Xlog:gc*` 或 `-verbose:gc`）可观察内存趋势：
```bash
# 观察每个 Full GC 后 Old Gen 是否回落到基线
grep "Full GC" ${nacos.home}/logs/gc.log | tail -20
```
若 `Full GC` 后 Old 区占用持续攀升（如 60%→70%→80% 不回降），即为内存泄漏的典型曲线；若 FGC 频繁但每次都能回落到接近初始基线，则更可能是堆容量设置过小。

### Nacos 中常见的几类内存泄漏

结合 Nacos 的运行机制，生产中最常出现的泄漏可归为以下几类，排查时可优先对号入座：

| 泄漏类型 | 典型持有对象 | 症状 | 常见诱因 |
|---------|-------------|------|---------|
| **连接对象泄漏** | `Connection`/`ConnectionManager.connections` Map | Old Gen 缓慢增长 | 客户端非优雅退出，连接关闭回调未触发 |
| **事件订阅泄漏** | `NotifyCenter` 事件总线中的订阅者 | 事件类实例大量堆积 | 业务代码注册监听后未注销 |
| **推送任务队列堆积** | 推送任务对象、阻塞队列 | 队列无界增长 | 推送积压、消费者处理不及时 |
| **缓存集合膨胀** | 缓存 Map、`ConcurrentHashMap$Node` | 内存占用超预期 | 缓存 key 无限增长、未做淘汰 |
| **线程/线程池持有** | 线程对象、`ThreadLocal` | 线程数异常增多 | 线程池未复用、ThreadLocal 未清理 |

判读要点：上述类型的共同特征是**对象随运行时间单调增长、与业务流量不成比例**。若发现某类对象数量远超其对应的在线客户端数/注册订阅数，即为异常，应追溯到其 GC Roots 持有链。

### 内存泄漏 vs 内存不足：先分清问题性质

定位到"内存高"后，还需区分是**泄漏**还是**容量不足**，二者处理方向完全不同：

| 判定维度 | 内存泄漏 | 内存不足 |
|---------|---------|---------|
| Full GC 后 Old 区 | 不回落或回落不明显 | 通常能回落到基线附近 |
| 对象数量 | 某类对象持续单调增长 | 整体对象规模随流量正常波动 |
| 与业务关系 | 与流量不成比例 | 与流量成正相关 |
| 处理方向 | 修复引用链、释放对象 | 调大堆、优化 GC、扩容 |

**判断方法**：连续观测多次 Full GC 后的 Old 区占用。若每次 Full GC 后占用都持续攀升（如 50%→65%→78%），倾向泄漏；若 Full GC 后能明显回落、只是很快又填满，更可能是堆设偏小。二者也可能并存——先解决泄漏，再评估是否需要调堆。

### 步骤 3：Eclipse MAT 分析定位泄漏

用 MAT 打开 hprof 快照，按以下路径定位：

```text
1. Overview → 查看 Total heap / Classes / Unreachable Objects
2. Histogram → 按 Retained Heap 排序，找占用最大的对象类型
3. 右键对象 → "Path To GC Roots" → 选 "with all references"
    → 查看是谁（GC Root）持有了这些对象 → 定位未释放的引用链
4. Dominator Tree → 观察大对象树，确认是否为 Nacos 缓存/连接/推送队列
```

**典型泄漏判定**：若发现 Nacos 的某类集合（如连接管理 Map、推送任务队列）Retained Heap 持续增大，且 GC Roots 链指向了本应释放的生命周期对象（如已注销的客户端连接），即为典型泄漏。

**MAT 关键视图详解**：
- **Histogram**：列出所有类的实例数与 Shallow/Retained Heap。Retained Heap 大的类往往持有大量子对象，是排查重点。
- **Dominator Tree**：展示对象间的引用支配关系，一眼看出"哪个对象支配（间接持有）了大量内存"。
- **Path to GC Roots**：从可疑对象追溯到 GC Root（静态字段、线程栈、JNI 引用等）的完整引用链，揭示对象为何无法被回收。

**常见误判**：MAT 显示的大对象不一定是泄漏，可能是正常缓存（如 Nacos 的服务实例缓存、配置缓存）。需结合业务流量判断该对象规模是否与预期一致，若远超正常规模才判定为异常增长。

### Trade-off 分析

**HeapDump 抓取时机：在线 dump vs OOM 自动 dump**：

| 维度 | 在线 jmap dump | OOM 自动 dump |
|------|---------------|--------------|
| 时机精准度 | 可自主选择高位时刻 | OOM 瞬间（可能延迟） |
| 生产影响 | 高（live dump 触发 FGC，停摆冲击） | 低（非主动干预） |
| 配置成本 | 无 | 需加 JVM 参数 |
| 推荐场景 | 已能复现/运维窗口 | 生产长期运行（保底） |

生产推荐**双通道**：开启 `-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=...` 保底捕获 OOM 现场；在疑似泄漏且可低峰期操作时再用 jmap 主动 dump 获取更精准的高位快照。避免在高峰用 `-dump:live`，其触发的 Full GC 可能加剧服务停顿。

### JVM 内存参数与 GC 策略建议

排查内存问题后，合理配置 JVM 参数能显著降低内存风险。Nacos 生产环境常用参数建议：

```bash
# 启动参数示例（nacos/bin/startup.sh 调整或 JVM_OPT 注入）
-Xms4g -Xmx4g                    # 初始与最大堆（建议相等，避免动态伸缩抖动）
-XX:+UseG1GC                     # G1 收集器，适合大堆与低停顿场景
-XX:+HeapDumpOnOutOfMemoryError  # OOM 自动 dump（推荐必开）
-XX:HeapDumpPath=/opt/nacos/logs # dump 输出目录
-XX:MaxGCPauseMillis=200         # G1 GC 停顿目标（可调）
```

**调优关注点**：
- **堆大小**：需结合实例数、连接数、配置量综合评估，过小触发频繁 FGC，过大则 GC 停顿长。可按"正常运行峰值占用 × 1.5~2"预留。
- **GC 策略**：2.5.3 默认基于 JDK 版本选择收集器，中大型集群推荐 G1，追求更低停顿。
- **OOM dump**：务必开启，这是抓取泄漏现场的最后保障，且应配置独立的 dump 目录避免写满系统盘。

### 真实案例：一次连接管理 Map 泄漏的定位

某 Nacos 集群运行 30 天后出现 OOM。按三步排查：

1. **jstat** 观察：Old Gen 使用率从 40% 缓慢升至 90%，Full GC 后无法回落 → 确认存在泄漏。
2. **jmap -histo**：发现 `com.alibaba.nacos.core.remote.Connection` 相关对象及 `ConcurrentHashMap$Node` 实例数量庞大，远超在线客户端数。
3. **MAT 分析**：对 `Connection` 对象执行 Path to GC Roots，发现其被 `ConnectionManager.connections` Map 持有，但对应客户端早已注销。进一步排查发现，客户端异常退出时未触发连接关闭回调，导致连接对象未从 Map 移除。

**根因**：客户端非优雅退出（进程被 kill -9 / 网络闪断）时服务端未及时清理连接，连接 Map 持续增长引发泄漏。**解决**：调整连接超时清理配置、启用连接健康检测，并对异常断开调用关闭回调从 Map 移除，最终内存恢复平稳。该案例与 14.10 节的"gRPC 连接泄漏"场景相互印证。

### 源码走读：连接对象在哪个环节被持有

以最典型的连接对象泄漏为例，从源码层面理解持有链。Nacos 服务端通过 `ConnectionManager` 统一管理 gRPC 连接：

```java
// core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java (Nacos 2.5.3, 节选)
private Map<String, Connection> connections = new ConcurrentHashMap<>();
// key 为 connectionId, value 为 Connection 对象

public Connection getConnection(String connectionId) {
    return connections.get(connectionId);
}

public void remove(String connectionId) {
    connections.remove(connectionId);
}
```

**关键点**：`connections` 是一个以 `connectionId` 为 key 的 Map。正常情况下，客户端断连后服务端会通过连接关闭流程调用 `remove` 移除该连接。**若客户端异常退出（进程被 kill -9、网络闪断）而未触发关闭回调，对应的 `Connection` 对象就永久滞留在 Map 中**——随着时间推移，Map 中的无效连接越积越多，Old Gen 被持续撑大，最终 OOM。

**排查印证**：在 MAT 中对 `Connection` 对象执行 `Path to GC Roots`，若引用链显示"被 `ConnectionManager.connections` 持有、且对应客户端已不在线"，即可确认该连接为泄漏对象。此时解决的思路是补全异常断开的清理逻辑：可结合连接健康检测与定期扫描，将长时间无心跳的陈旧连接从 `connections` 中移除。

### 泄漏修复的工程化措施

定位到泄漏根因后，从工程上给出可持续的修复与防控：

1. **修复持有链**：补全断开/注销时的清理逻辑，确保生命周期结束的对象从 Map、事件总线、队列中移除。
2. **主动健康检测**：开启并调优连接心跳超时，让服务端能主动剔除"死连接"。
3. **容器无界增长防线**：对缓存、队列等容器设置容量上限与淘汰策略（LRU/TTL），避免极端情况无限膨胀。
4. **持续观测**：将 Old Gen 使用率、Full GC 频率、关键缓存/连接数接入监控，设置增长趋势告警（而非仅阈值告警），提前捕获缓慢泄漏。
5. **回归验证**：修复后通过压测或长期运行观察，确认 Old Gen 保持平稳、不再单调攀升。

### 一次完整的排查 Snippet（汇总）

为便于直接套用，将三类核心命令整合为一个可复用的排查片段：

```bash
# 第 1 步：确认泄漏趋势（连续采集 2~3 次）
jstat -gcutil <pid> 1000 3

# 第 2 步：抓现场（低峰期或开启 OOM 自动 dump）
jmap -dump:live,format=b,file=/tmp/nacos_heap.hprof <pid>
jmap -histo <pid> | head -40

# 第 3 步：记录异常对象类名，供 MAT 追溯 GC Roots
# 在 MAT 中对占用最大类执行 "Path to GC Roots → with all references"
```

此片段覆盖"看趋势→抓现场→找持有链"全流程，运维可直接固化为脚本，配合监控增长趋势告警，形成对内存泄漏的常态防控。

### 设计模式分析

Nacos 内存占用与**观察者/缓存模式**密切相关：发布-订阅（NotifyCenter 事件总线）中，若订阅者未被正确解除注册，事件会被持续推送并持有订阅者引用，形成泄漏。排查时可重点通过 MAT 的引用链分析确认**事件监听者、连接对象、推送任务是否在生命周期结束后仍被事件总线或任务队列持有**。

### 小结

JVM 内存泄漏按"jstat 看趋势 → jmap dump 抓现场 → MAT 找引用链"三步排查。核心判据是**Old Gen 持续增长且 Full GC 后无法回落**。生产务必开启 OOM 自动 dump，抓取现场快照是定位泄漏的前提，MAT 的 GC Roots 引用链分析是确认持有者的关键。

---

## 14.10 常见内存泄漏场景表：gRPC 连接泄漏 / Distro 数据积压 / LongPolling OOM / 推送执行器 OOM（PushExecutorDelegate）

### 设计背景

Nacos Server 的内存泄漏有若干典型场景，了解这些场景有助于快速缩小 MAT 分析范围。本节以表格形式归纳 4 类最常见的 Nacos 内存泄漏场景，并结合 14.9 的排查方法给出各自的判定与处理。

### 核心类关系图（四类泄漏场景）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│               Nacos 常见内存泄漏场景与对应数据结构                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  场景                      泄漏的数据结构              MAT 观察点            │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌───────────────────┐ │
│  │ ① gRPC 连接泄漏       │──▶│ ConnectionManager    │──▶│ connections Map   │ │
│  │   连接未正常关闭       │  │ connections Map      │  │ clientId 持续增长 │ │
│  ├──────────────────────┤  ├──────────────────────┤  ├───────────────────┤ │
│  │ ② Distro 数据积压     │──▶│ Distro 延迟任务队列    │──▶│ 任务队列长度       │ │
│  │   同步失败堆积任务     │  │ / 待同步数据          │  │ 无界增长          │ │
│  ├──────────────────────┤  ├──────────────────────┤  ├───────────────────┤ │
│  │ ③ LongPolling OOM     │──▶│ LongPollingService   │──▶│ ClientLongPolling │ │
│  │   挂起连接堆积         │  │ 挂起任务集合          │  │ 对象数量过多        │ │
│  ├──────────────────────┤  ├──────────────────────┤  ├───────────────────┤ │
│  │ ④ 推送执行器 OOM       │──▶│ PushExecutorDelegate │──▶│ 推送任务队列        │ │
│  │   推送任务积压         │  │ 任务队列             │  │ 积压任务           │ │
│  └──────────────────────┘  └──────────────────────┘  └───────────────────┘ │
│            图 14-10：四类内存泄漏场景与数据结构                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 四类泄漏场景速查表

| # | 场景 | 涉及核心类 | 泄漏机理 | MAT/观察判定 | 处理方向 |
|---|------|-----------|---------|-------------|---------|
| 1 | **gRPC 连接泄漏** | `ConnectionManager`（`core/.../remote/ConnectionManager.java`） | 客户端异常退出但服务端连接未关闭，`connections` Map 中的连接对象持续堆积 | `connections` Map 的 clientId 数量持续增长且不释放 | 排查连接超时配置、客户端断线重连是否规范关闭 |
| 2 | **Distro 数据积压** | `DistroClientDataProcessor`、`DistroDelayTaskProcessor` | Distro 同步失败后任务持续入队且未被消费，队列无界增长 | 延迟任务队列长度持续上升，`[DISTRO-FAILED]` 日志高频 | 修复节点间同步通道，清理积压队列 |
| 3 | **LongPolling OOM** | `LongPollingService`、`ClientLongPolling` | 长轮询挂起请求异常堆积，未超时未释放的挂起连接过多 | `ClientLongPolling` 对象数量暴增、Retained Heap 大 | 检查长轮询线程池配置、客户端数量上限 |
| 4 | **推送执行器 OOM** | `PushExecutorDelegate`（`naming/.../push/v2/executor/PushExecutorDelegate.java`） | 服务变更频繁导致推送任务积压，推送执行器队列 OOM | 推送任务队列积压、`PushExecuteTask` 对象增长 | 检查服务变更频率，优化推送批处理与执行器容量 |

### 场景重点说明

**gRPC 连接泄漏**：`ConnectionManager` 使用 `Map<String, Connection>` 维护客户端连接。当客户端不优雅退出（如进程被 kill -9、网络闪断未触发连接关闭回调）时，服务端连接对象无法及时移除，长期积累即泄漏。排查时对比"客户端真实数量"与"连接 Map 中 clientId 数量"，差距持续扩大即为泄漏。生产可通过 13.2 节的 gRPC 连接数指标监控其增长趋势。

**Distro 数据积压**：`DistroClientDataProcessor` 处理临时实例的跨节点同步，`DistroDelayTaskProcessor` 负责延迟重试。若某目标节点长期不可达，同步任务会持续进入延迟队列而无法成功消费，队列无界增长导致内存膨胀。观察指标可用 `[DISTRO-FAILED]` 日志频率与队列长度。

**LongPolling OOM**：`LongPollingService` 中每个挂起的长轮询请求对应一个 `ClientLongPolling` 对象并被持有至超时或变更。当客户端数量远超服务端承受能力、或挂起任务未及时释放时，对象堆积引发 OOM。这与 14.4 节的长轮询超时同源——超时后应正确释放挂起连接。

**推送执行器 OOM**：`PushExecutorDelegate` 采用 SPI 选择推送实现（`PushExecutorRpcImpl`/`PushExecutorUdpImpl`），将推送任务提交到执行器队列。当服务实例频繁变（大量注册/注销）时，推送任务爆炸式涌入队列，若消费速率跟不上即 OOM。

### 源码走读：LongPollingService 的挂起任务持有

以 LongPolling 泄漏为例深入源码，理解挂起任务为何会越积越多。`LongPollingService` 维护了配置/数据变更的推送逻辑，其中挂起的长轮询请求被持有于专门的集合：

```java
// common/src/main/java/com/alibaba/nacos/common/notify/NotifyCenter.java (Nacos 2.5.3, 节选)
// LongPollingService 通过事件总线注册处理器, 挂起的 ClientLongPolling 交由异步处理
public static void registerToPublisher(Class<? extends Event> eventType,
                                       int queueMaxSize) {
    // 为事件类型分配一个带容量上限的 Publisher
    // queueMaxSize 限制事件队列, 防止事件无界堆积
}
```

关键控制点在事件发布队列的 `queueMaxSize`：若配置不当（或无界），事件发布速率远超处理速率时队列会持续膨胀。`ClientLongPolling` 对象在"请求挂起 → 变更触发 → 推送响应"的完整生命周期结束后必须被移除；若变更从未到来且超时清理失效，挂起对象便会长期滞留形成泄漏。

**对排查的启示**：当 MAT 中 `ClientLongPolling` 与 `NotifyCenter` 相关的事件队列对象过多时，应从两个方向核查——（1）事件队列容量是否过小导致处理积压；（2）挂起请求的超时释放逻辑是否生效。两者结合能同时缓解 CPU 与内存压力。

### 泄漏场景的预防与常态化防控

四类泄漏场景的防控重点可归纳为一张清单：

| 场景 | 核心预防手段 | 关键监控指标 |
|------|-------------|-------------|
| gRPC 连接泄漏 | 开启连接健康检测、规范断连清理 | 在线连接数、`connections` Map 规模 |
| Distro 数据积压 | 保障节点间网络与同步通道稳定 | 延迟队列长度、`[DISTRO-FAILED]` 频次 |
| LongPolling OOM | 控制长轮询规模、缩短超时、容量上限 | 挂起连接数、事件队列积压 |
| 推送执行器 OOM | 限制服务变更频率、批处理推送 | 推送任务队列长度、`PushExecuteTask` 数量 |

**共性的三条防控原则**：一是**所有可增长的容器优先配置容量上限**（有界队列 + 拒绝/背压策略）；二是**生命周期结束的对象必须从持有集合中移除**（连接、订阅、挂起请求）；三是**将上述指标的"增长趋势"接入告警**，在泄漏早期（而非 OOM 时）即发现并干预。这三条贯穿四类场景，也是 14.9 节排查方法落地的延伸。

### 场景间的关联与组合排查

实际生产往往不止出现单类泄漏。四类场景可能相互放大：例如网络抖动先引发 **Distro 同步失败**（场景 2），继而客户端反复重连产生 **gRPC 连接泄漏**（场景 1），配置频繁变更又叠加 **推送任务积压**（场景 4）。因此，MAT 分析时不要孤立看待单类对象，应综合多类对象增长情况，追溯共同根因（多为网络不稳定或客户端异常行为）。这与 14.7/14.8 节"先辨状态再动手"的思路一致。

### Trade-off 分析

**队列有界 vs 无界**（针对上述任务/连接缓存）**：

| 维度 | 有界队列（限流） | 无界队列（无限积压） |
|------|----------------|--------------------|
| OOM 风险 | 低（超限拒绝） | 高（无界增长） |
| 任务丢失风险 | 中（拒绝可能丢任务） | 低（全部排队） |
| 背压机制 | 有（队列满即拒绝） | 无 |
| 适用场景 | 生产（推荐） | 短暂峰值容忍 |

Nacos 内部部分任务队列默认采用无界队列以保任务不丢，这在异常场景下易成 OOM 源头。生产排查到对应队列积压后，应评估是否可配置为有界队列 + 拒绝策略（如告警），或调大处理线程池以加快消费。

### 快照对比法：确认泄漏是否仍在持续

排查四类场景时，单一 HeapDump 只能反映"此刻"的内存状态，无法证明"在持续增长"。工程上推荐**间隔抓取两个快照对比**以确证泄漏：

```bash
# 时刻 T1 抓第一个快照
jmap -dump:format=b,file=/tmp/nacos_heap_t1.hprof <pid>
# 运行一段时间（数小时至 1 天）后, T2 抓第二个快照
jmap -dump:format=b,file=/tmp/nacos_heap_t2.hprof <pid>
```

然后对两份快照分别用 MAT 打开 Histogram，对比**同一类对象**的 Retained Heap 与实例数：

- 若 `Connection`、`ClientLongPolling`、`PushExecuteTask` 等对象的实例数在 T1→T2 间**明显增加**，即确证对应场景存在泄漏；
- 若各对象实例数基本持平、仅总量随流量波动，则更可能是容量/配置问题而非泄漏。

对比法能有效避免"误把正常缓存当泄漏"的误判，是 14.9 单快照分析的重要补充，应作为排查流程的标准动作固化。

### 设计模式分析

`PushExecutorDelegate` 体现了**策略模式 + 工厂模式**：通过 SPI 机制在运行时选择具体推送实现（RPC 推送 `PushExecutorRpcImpl`、UDP 推送 `PushExecutorUdpImpl`），实现推送方式的可插拔扩展。`SpiPushExecutor` 作为抽象策略接口，`SpiImplPushExecutorHolder` 作为策略登记工厂。这种设计带来了灵活性，也要求运维关注不同推送实现的队列容量差异。

### 小结

Nacos 四类典型内存泄漏分别对应连接管理、Distro 同步、长轮询挂起、推送任务积压。排查时结合 14.9 的 MAT 分析，针对性地观察 `connections` Map、任务队列、`ClientLongPolling`/`PushExecuteTask` 对象的 Retained Heap。处理共性方向是**控制无界队列、及时释放生命周期结束的对象、监控对应指标趋势**。工程落地上，建议为所有可增长容器配置容量上限，并将连接数、队列长度、挂起对象规模接入增长趋势告警；结合间隔快照对比法确认泄漏是否持续，实现从"故障处理"到"常态防控"的转变，从而在四类泄漏真正演化为 OOM 之前及时拦截、主动规避，保障集群内存长期稳定与业务连续。运维团队应定期巡检连接数、队列长度等关键指标，并结合压测验证容量上限设计的有效性，形成可持续、可复用的内存健康管理长效机制。

---

## 14.11 CPU 飙高排查：top -H → jstack → async-profiler 火焰图

### 设计背景

Nacos Server CPU 飙高（持续高占用）会拖慢所有请求，是仅次于 OOM 的高影响故障。CPU 高可能源于 GC 频繁（配套内存问题）、线程空转、热点业务逻辑（推送、注册处理）、锁竞争等。排查 CPU 问题遵循"先定位线程，再看线程在干什么"的思路：top -H 找高 CPU 线程 → jstack 看线程栈 → async-profiler 生成火焰图定位热点函数。

### 核心类关系图（CPU 飙高三步定位）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│           CPU 飙高排查：三步法                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  步骤1: top -H          步骤2: jstack           步骤3: async-profiler       │
│  ┌────────────────┐    ┌──────────────────┐    ┌──────────────────────┐    │
│  │ 找高CPU线程TID   │    │ 定位线程栈        │    │ CPU 火焰图            │    │
│  │ %CPU 最高的线程  │    │ 看线程名/执行方法  │    │ 热点函数纵向占比      │    │
│  └───────┬────────┘    └────────┬─────────┘    └──────────┬───────────┘    │
│          │ TID                 │ 线程栈                   │ 调用路径          │
│          ▼                     ▼                         ▼                  │
│  锁定嫌疑线程            初判线程在忙什么             精确定位热点函数/行号     │
│          │                     │                         │                  │
│          └─────────────────────┴─────────────────────────┤                  │
│                     综合判定: GC/业务热点/锁竞争/空转                      │
│            图 14-11：CPU 飙高三步定位法                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 步骤 1：top -H 锁定高 CPU 线程

```bash
# 找到 Nacos 进程 PID
jps -l | grep -i nacos
# 显示该进程内所有线程 CPU 占用（-H 显示线程）
top -H -p <nacos_pid>
# 记录 %CPU 最高的线程 TID（十六进制转换，供 jstack 定位）
```
将高 CPU 线程的 TID 转为十六进制：
```bash
printf '%x
' <thread_tid>
```
得到十六进制 nid 后，在 jstack 输出中按 nid 定位对应线程。

### 步骤 2：jstack 看线程栈

```bash
# 抓取线程快照
jstack <nacos_pid> > /tmp/nacos_$(date +%Y%m%d_%H%M%S).txt
# 按步骤1的 nid 定位高 CPU 线程
grep -A 30 "nid=0x<hex>" /tmp/nacos_thread.txt
```
观察线程栈，通常能初判线程类别：
- **GC 线程**（`GC task thread`、`VM Thread`）高 CPU → 多为内存问题（见 14.9），需配合 jstat 看 GC 频率。
- **gRPC 工作线程**（`grpc-default-worker`）→ 请求处理热点，业务吞吐过大或业务逻辑耗时。
- **推送/调度线程**（`RpcPushService`、`Distro` 相关）→ 变更风暴导致推送/同步密集。
- **锁等待线程**（`BLOCKED`、`parking to wait for <lock>`) → 锁竞争严重，多个线程争抢同一锁。

### 步骤 3：async-profiler 生成火焰图定位热点

```bash
# 采样 CPU 30 秒生成火焰图
async-profiler -d 30 -e cpu -f /tmp/nacos_cpu_flame.html <nacos_pid>
# 打开 firefox/chrome 查看火焰图
```
火焰图纵轴为调用栈，横向宽度表示 CPU 采样占比。**顶部最宽的函数即 CPU 热点**。常见 Nacos 热点函数：
- `DistroClientDataProcessor` / `ClientBeatProcessorV2` → 大量实例心跳/注册处理。
- `RpcPushService.push` → 推送风暴（服务频繁变更）。
- `ConfigCacheService` / `LongPollingService` → 配置高并发读写/长轮询。
- GC 相关（`GC` 标签占大比例）→ 内存回收压力。

### 高频诊断命令补充

除三步主流程外，以下命令在 CPU 排查中高频使用，建议一并掌握：

```bash
# 查看进程 CPU 与内存占用概览
top -p <nacos_pid>
# 按 CPU 排序刷新（M 按内存, P 按 CPU），观察是否持续高位

# 查看 CPU 核数与容器配额（判断是否线程数过多导致上下文切换飙升）
nproc --all

# 观察上下文切换开销（若切换率异常高, 说明线程数过多或锁竞争严重）
vmstat 1 5 | awk '{print "cs:", $12}'

# 抓多份 jstack 对比同一线程是否长期停留在同一栈帧
for i in 1 2 3; do jstack <pid> > /tmp/thread_$i.txt; sleep 3; done
# 若同一线程在多份快照里都停在同一个高 CPU 栈帧 → 确证该处为持续热点
```

**多次采样对比**是 CPU 排查区分"瞬时尖刺"与"持续高占用"的关键：瞬时尖刺多为偶发 GC 或单次大请求，持续热点则是需要定位的瓶颈。连抓多份快照，若高 CPU 线程反复出现在同一栈帧，即可确认热点稳定存在，值得深入。

### CPU 飙高的常见根因归类

结合 Nacos 运行特征，CPU 飙高通常可归为以下几类，排查时可对号入座：

| 根因类别 | 典型表现 | 定位方向 |
|---------|---------|---------|
| **GC 压力** | GC 线程高 CPU、频繁 Full GC | 转 14.9/14.10 内存排查 |
| **gRPC 请求过载** | `grpc-default-worker` 线程高占用 | 检查请求量/吞吐、业务逻辑耗时 |
| **推送/同步风暴** | 推送、Distro 相关线程高占用 | 检查服务变更频率、批量推送优化 |
| **锁竞争** | 大量 `BLOCKED`、`parking to wait` | jstack 找锁持有者，分析临界区 |
| **线程空转/忙等** | 线程 100% CPU 但无实际业务 | 检查死循环、自旋等待逻辑 |

判定时先看高 CPU 线程的**类别**（GC？gRPC worker？推送？锁等待？），再结合线程栈与火焰图定位到具体函数。分类思路能避免盲目钻入细节。

### Trade-off 分析

**jstack 快照 vs async-profiler 采样**：

| 维度 | jstack（瞬时快照） | async-profiler（周期采样） |
|------|------------------|--------------------------|
| 采样方式 | 单点快照 | 高频采样聚合 |
| 热点统计 | 不直观（需人工看栈） | 火焰图直观统计 |
| 性能开销 | 轻微（短暂暂停） | 中（采样开销） |
| 适用场景 | 初步判断线程类别、锁状态 | 精确定位热点函数与调用路径 |

**推荐组合**：先 jstack 快速判断线程类别与是否锁竞争，再用 async-profiler 对高 CPU 线程做细粒度火焰图分析。CPU 问题多为动态热点，单点快照不足以反映，火焰图的聚合统计更具说服力。

### 源码走读：RpcPushService 推送链路与热点成因

CPU 热点往往集中在推送链路，理解其处理逻辑有助于定位。`RpcPushService` 是 Nacos 推送的核心入口：

```java
// naming/src/main/java/com/alibaba/nacos/naming/push/v2/RpcPushService.java (Nacos 2.5.3, 节选)
public void pushToClient(Service service, String clientId, PushDataWrapper dataWrapper) {
    // 将推送任务按 clientId 路由到对应 Client, 通过 gRPC 单向流推送
    Connection connection = ConnectionManager.getInstance().getConnection(clientId);
    if (connection == null) { return; }
    // ...
}
```

**热点成因**：当服务实例频繁变更（注册/注销/心跳）时，`pushToClient` 会被大量调用，按客户端逐一推送。若订阅该服务的客户端数量巨大，单次服务变更即触发大规模推送，形成 CPU 热点。此时火焰图顶部会出现 `RpcPushService.pushToClient` 的宽大柱状。

**优化思路**：从"按客户端逐个推送"转向**批量推送 / 合并推送**，或对推送频率做节流（同服务短时间多次变更合并为一次推送），可显著降低推送链路的 CPU 开销。这也解释了为何高变更频率场景下 CPU 会与内存泄漏（14.10 场景 4 推送积压）同时出现。

### 锁竞争检测与定位

锁竞争是 CPU 高的隐性根因——大量线程在锁上自旋/阻塞，CPU 空转但无实际产出。定位方法：

```bash
# 抓取线程快照, 统计 BLOCKED 线程数
jstack <pid> > /tmp/t.txt; grep -c "java.lang.Thread.State: BLOCKED" /tmp/t.txt

# 查看所有线程等待的锁与持有者
grep -B1 -A5 "parking to wait for" /tmp/t.txt | head -50
```

**判断要点**：
- 若大量线程处于 `BLOCKED` 或 `parking to wait for <lock>`，且都等待**同一个锁对象**，即为锁竞争热点；
- 结合火焰图，锁相关热点常表现为 `synchronized`、`ReentrantLock`、`AbstractQueuedSynchronizer` 相关栈帧占比偏高；
- 定位到锁后，分析临界区是否过长、是否存在可消除的串行化，可通过**缩小锁粒度、读写锁分离、无锁/并发集合替换**等方式缓解。

**与 CPU 的关系**：锁竞争不会直接产生大量计算量，但会让大量线程陷入反复的锁获取/释放与上下文切换，导致系统整体 CPU 与切换开销飙升。排查时"BLOCKED 线程数多 + 上下文切换率高"是锁竞争的有力信号。

### 源码走读：ClientBeatProcessorV2 逐实例处理模型

心跳处理是集群持续运行下的常见 CPU 热点，其处理模型决定了负载的放大效果。`ClientBeatProcessorV2` 负责处理大批量的实例心跳：

```java
// naming/src/main/java/com/alibaba/nacos/naming/remote/rpc/handler/InstanceRequestHandler.java
// 与 ClientBeatProcessorV2 配合处理心跳注册链路, 逐实例更新
// 每个客户端心跳都会进入实例处理流程, 更新其注册记录与健康状态
```

**处理模型的放大效应**：心跳处理采用"逐实例"模型——每个在线实例的每次心跳都产生一次独立处理。当实例规模达数十万、心跳频率较高时，即便单个心跳处理成本很低，总量也会放大为可观的 CPU 消耗。火焰图中 `ClientBeatProcessorV2` 相关栈帧变宽，往往提示在线实例规模已大。

**缓解方向**：其一，通过批处理合并同一客户端的批量心跳；其二，合理配置心跳频率与超时，避免过度频繁或重复的心跳上报；其三，规模超过单节点处理上限时，将服务实例分布到不同节点（垂直/水平扩展）分担心跳处理压力。这也解释了为何心跳热点常与 gRPC 连接管理、内存使用（14.9/14.10）问题相互叠加出现。

### 火焰图的正确解读方法与常见误区

火焰图是 CPU 排查的核心工具，正确解读能事半功倍。关键要点：

- **纵向为调用栈层级**，自底向上为"被调用→调用方"；**横向宽度为采样占比**，宽度越大表示该处占 CPU 时间越多。
- 关注**顶部（栈顶）宽大函数**：它们是实际消耗 CPU 的最内层热点；中间层宽大多为聚合函数（如分发、调度框架），真正热点要看最顶层的叶子函数。
- **同一函数多次出现**：若栈顶某函数以多个同层柱子出现，说明其在多路调用中都占用 CPU，应合并统计其总占比。
- **误区分 GC**：火焰图中大面积的 `GC`、内存分配相关栈帧，说明 CPU 消耗在内存回收上，应转向 14.9/14.10 的内存问题排查，而非业务函数。

**实战技巧**：对热点函数右键查看调用来源，可回溯"哪个上游逻辑触发了这条热点路径"；对比不同时段的火焰图（高峰 vs 低峰）可识别随业务波动或随机出现的异常热点。

### CPU 性能监控与告警配置

排查解决之后，需通过监控提前发现 CPU 异常。建议配置以下指标及告警：

| 指标 | 告警阈值参考 | 用途 |
|------|-------------|------|
| 节点 CPU 使用率 | 持续 > 85% 数分钟 | 识别持续高占用 |
| GC 频率/耗时 | Full GC 频次、耗时超阈值 | 识别 GC 压力型 CPU |
| 上下文切换率（cs/s） | 异常飙升 | 识别锁竞争/线程过多 |
| 关键线程高 CPU 持续时间 | 单线程持续高占用 | 定位热点线程 |

**告警设计**：CPU 瞬时波动常见，建议用"**持续占用**"而非瞬时值触发告警（如连续 5 分钟 > 85%），避免误报；同时区分"业务高峰期正常高占用"与"异常持续高占用"，可结合时间窗口与基线对比。将 CPU 监控与 14.9 内存、14.10 连接数监控联动，形成对节点健康状态的完整观测。

### 设计模式分析

CPU 飙高排查本身并不涉及 Nacos 源码设计模式，但通过热点函数可反推 Nacos 的性能设计取舍。例如 `ClientBeatProcessorV2` 的逐实例心跳处理、`RpcPushService` 的逐个推送，本质是**分而治之**的处理模型；当热点集中在这些处理器时，往往提示"单实例处理成本 × 实例数量"超过节点处理能力，可通过批处理、线程池扩容或水平扩展缓解。这也呼应了第 12 章性能调优中"从单实例优化转向横向扩展"的思路。

### 完整案例：一次推送风暴引发的 CPU 飙高

结合前述方法，演示一次完整排查：

**现象**：某 Nacos 节点 CPU 持续 95%+，且伴随机房网络抖动。

**步骤 1 - top -H**：`top -H -p <pid>` 显示多个线程 CPU 占用偏高，其中两个线程 %CPU 达 60% 和 40%。

**步骤 2 - jstack**：将两个 TID 转十六进制，在 `jstack` 中定位到线程栈，发现二者都停在高频调用 `RpcPushService.pushToClient` 与 `DistroClientDataProcessor` 的栈帧上，线程类型属于推送/同步相关。

**步骤 3 - async-profiler**：生成火焰图（`-d 30 -e cpu`），顶部最宽的函数确认为 `RpcPushService.pushToClient`，其次为 `ClientBeatProcessorV2.process`（心跳处理）。

**综合判定**：网络抖动导致一批服务反复注册/注销，触发**推送风暴**与**心跳处理风暴**——大量客户端重连与实例变更，推送链路成为 CPU 热点。

**处理**：① 排查并稳定触发变更的客户端（修复反复重连/注册循环）；② 调整推送节流与心跳处理参数，合并短时间内的重复变更；③ 若业务规模已超单节点处理能力，做水平扩展，将服务分布到更多节点。

该案例说明 CPU 排查的价值在于**透过热点函数找到业务层的触发源**——火焰图定位到热点后，需回溯"为什么会有如此密集的推送/心跳"，而非仅纠结于单个函数本身的优化。

### CPU 排查后的调优与扩容决策

定位到根因后，需从操作层面给出决策。按影响范围从小到大排列：

| 措施 | 适用情形 | 成本 |
|------|---------|------|
| **代码/配置级优化**（合并推送、节流） | 推送/心跳风暴 | 低 |
| **调优线程池/队列**（扩处理线程、有界队列） | 处理能力不足但规模可预见 | 中 |
| **锁粒度优化**（缩小临界区） | 锁竞争明显 | 中 |
| **水平扩容**（增加节点分散负载） | 业务规模持续增长、单节点饱和 | 高 |

**决策要点**：优先用低成本措施消除"不合理的负载来源"（如反复重连、变更风暴），再评估是否调优线程与锁，最后才考虑扩容。盲目扩容可能在负载根源未除时仍有隐患，也增加了集群维护成本。同时，将 CPU 使用率、GC 频率、上下文切换率等指标接入监控并设置趋势告警，避免 CPU 飙高长期未被发现。

### 小结

CPU 飙高按"top -H 找线程 → jstack 看线程栈 → async-profiler 火焰图定位热点"三步排查。关键区分四类根因：GC 压力（配合 14.9 内存排查）、gRPC 请求热点、推送/同步风暴、锁竞争。火焰图的纵向宽度是定位热点函数的最直观依据，配合线程类别判断可快速收敛到具体模块并制定调优或扩容决策。
