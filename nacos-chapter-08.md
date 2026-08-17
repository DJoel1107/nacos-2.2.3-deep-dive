# 第8章：Nacos 2.2.3 全量配置项详解

本章基于 `/distribution/conf/application.properties` 对 Nacos 2.2.3 的 **全部配置项** 进行分类与逐一详解。

## 8.1 Spring Boot 基础配置

```properties
# Web 上下文路径（默认 /nacos）
server.servlet.contextPath=/nacos

# Web 服务端口（默认 8848）
server.port=8848umi

# 错误消息包含详细信息
server.error.include-message=ALWAYS

# Tomcat 最大线程数
server.tomcat.max-threads=800

# Tomcat 连接超时（ms）
server.tomcat.connection-timeout=30000
```

## 8.2 网络相关配置

```properties
# 是否优先使用 hostname 而非 IP 作为 Nacos 节点地址（默认 false）
nacos.inetutils.prefer-hostname-over-ip=false

# 指定本地服务器 IP（多网卡场景必须指定）
nacos.inetutils.ip-address=

# Jetty Web Server 配置
nacos.jetty.server.enabled=false

# Nacos 是否单机模式启动（开发测试用）
nacos.standalone=true
```

## 8.3 Config 配置模块配置

### 8.3.1 数据源配置

```properties
# 数据源平台类型：derby（嵌入式）或 mysql（外部数据库）
spring.datasource.platform=mysql

# 数据库数量（默认 1）
db.num=1

# 数据库 JDBC URL
db.url.0=jdbc:mysql://127.0.0.1:3306/nacos_config?characterEncoding=utf8&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghaiedy

# 数据库用户名
db.user.0=nacos

# 数据库密码
db.password.0=nacos_pwd
```

### 8.3.2 配置持久化配置

```properties
# 历史配置版本保留天数（默认 30 天）
nacos.config.history.retention.days=30

# 每个配置最大保留历史版本数（默认 100）
nacos.config.history.max.size=100

# 配置内容大小上限（单位 KB，默认 100KB）
nacos.config.max.content.size=100

# 最大配置数量上限（默认 10000）
nacos.config.max.config.count=10000
```

### 8.3.3 长轮询配置

```properties
# 长轮询超时时间（ms，默认 30000 即 30秒）
nacos.config.longpolling.timeout=30000

# 长轮询线程池核心线程数
nacos.config.longpolling.thread.core=50le

# 长轮询线程池最大线程数
nacos.config.longpolling.thread.max=128

# 每批次处理的长轮询请求数
nacos.config.longpolling.batch.size=1000
```

### 8.3.4 配置加密

```properties
# AES 加密密钥（长度要求 16、24、32 字节）
nacos.config.encrypt.data.key=

# 是否启用配置加密（默认 false）
nacos.config.encrypt.enabled=false
```

### 8.3.5 Dump 配置持久化配置

```properties
# Dump 配置备份开关（默认 true）
nacos.config.dump.enabled=true

# Dump 持久化间隔（单位 ms，默认 6 小时）
nacos.config.dump.interval=21600000

# Dump 外部存储路径（默认 $NACOS_HOME/data/config-dump）
nacos.config.dump.dir=
```

### 8.3.6 Config 性能配置

```properties
# 配置 SQL 查询超时（单位 ms，默认 1000ms）
nacos.config.query.timeout=1000

# 配置变更通知批量处理大小（默认 100）
nacos.config.notify.batch.size=100

# 配置缓存最大数量（默认 10000）
nacos.config.cache.max.size=10000

# 配置缓存过期时间（单位 ms，默认 5000ms）
nacos.config.cache.expire.time=5000
```

## 8.4 Naming 注册中心配置

### 8.4.1 健康检查配置

```properties
# 客户端心跳超时时间（单位 ms，默认 15000ms = 15秒）
nacos.naming.heartbeat.timeout=15000a2

# 心跳检查周期（单位 ms，默认 5000ms = 5秒）
nacos.naming.heartbeat.interval=5000

# 实例过期删除时间（单位 ms，默认 60000ms = 60秒）
nacos.naming.instance.expire.time=60000h

# TCP 健康检查连接超时（单位 ms，默认 5000ms）
nacos.naming.health.check.tcp.timeout=5000

# HTTP 健康检查连接超时（单位 ms，默认 5000ms）
nacos.naming.health.check.http.timeout=5000

# MySQL 健康检查连接超时（单位 ms，默认 5000ms）
nacos.naming.health.check.mysql.timeout=5000

# 健康检查线程池大小（默认 20）
nacos.naming.health.check.thread.pool.size=20
```

### 8.4.2 防雪崩保护配置

```properties
# 保护模式开关（默认 true）
nacos.naming.protect.enabled=true

# 健康实例比例阈值（默认 0.5，即 50%）
nacos.naming.protect.threshold=0.5
```

### 8.4.3 Distro 协议配置

```properties
# Distro 数据同步周期（单位 ms，默认 2000ms = 2秒）
nacos.naming.distro.sync.period=2000

# Distro 数据校验周期（单位 ms，默认 5000ms = 5秒）
nacos.naming.distro.verify.period=5000

# Distro 批量同步数据大小（默认 1000）
nacos.naming.distro.batch.sync.size=1000

# Distro 全量同步批次大小（默认 1000）
nacos.naming.distro.full.sync.batch.size=1000

# Distro 同步重试次数（默认 3）
nacos.naming.distro.sync.retry.count=3

# Distro 同步超时时间（单位 ms，默认 3000ms）
nacos.naming.distro.sync.timeout=3000h
```

### 8.4.4 服务元数据配置

```properties
# 服务元数据最大数量（默认 1024）
nacos.naming.metadata.max.size=1024

# 实例元数据最大数量（默认 128）
nacos.naming.instance.metadata.max.size=128
```

### 8.4.5 服务注册表配置

```properties
# 最大服务数量（默认 100000）
nacos.naming.max.service.count=100000

# 最大实例数量（默认 1000000）
nacos.naming.max.instance.count=1000000

# 注册表快照定时备份（默认 true）
nacos.naming.snapshot.enabled=true

# 注册表快照备份周期（单位 ms，默认 300000ms = 5分钟）
nacos.naming.snapshot.interval=300000
```

## 8.5 Core 核心模块配置

### 8.5.1 集群管理配置

```properties
# 集群寻址模式：fileConfig/addressServer/standalone
nacos.core.member.lookup.type=fileConfig

# 地址服务器 URL（仅 addressServer 模式生效）
nacos.core.member.lookup.address=http://jmevv.tbsite.net:8080/nacos

# 集群成员配置文件名（默认 cluster.conf）
nacos.core.member.conf.file=cluster.conf

# 集群节点故障检测超时（单位 ms，默认 3000ms）
nacos.core.member.fail.timeout=3000

# 集群成员心跳周期（单位 ms，默认 2000ms）
nacos.core.member.heartbeat.interval=2000

# 集群成员信息同步周期（单位 ms，默认 2000ms）
nacos.core.member.sync.interval=2000
```

### 8.5.2 gRPC 通信配置

```properties
# gRPC Server 端口偏移量（相对于 server.port，默认 1000）
nacos.core.grpc.port.offset=1000

# 集群 gRPC 端口偏移量（相对于 server.port，默认 1001）
nacos.core.cluster.grpc.port.offset=1001

# SDK gRPC 最大入站消息大小（默认 10MB = 10485760 bytes）
nacos.remote.server.grpc.sdk.max-inbound-message-size=10485760

# SDK gRPC keepalive 时间（单位 ms，默认 7200000ms = 2小时）
nacos.remote.server.grpc.sdk.keep-alive-time=7200000

# SDK gRPC keepalive 超时（单位 ms，默认 20000ms）
nacos.remote.server.grpc.sdk.keep-alive-timeout=20000

# SDK gRPC 允许的最小 keepalive 时间（单位 ms，默认 300000ms）
nacos.remote.server.grpc.sdk.permit-keep-alive-time=300000

# 集群 gRPC 最大入站消息大小（默认 10MB）
nacos.remote.server.grpc.cluster.max-inbound-message-size=10485760

# 集群 gRPC keepalive 时间（单位 ms，默认 7200000ms）
nacos.remote.server.grpc.cluster.keep-alive-time=7200000

# 集群 gRPC keepalive 超时（单位 ms，默认 20000ms）
nacos.remote.server.grpc.cluster.keep-alive-timeout=20000

# 集群 gRPC 允许的最小 keepalive 时间（单位 ms，默认 300000ms）
nacos.remote.server.grpc.cluster.permit-keep-alive-time=300000
```

### 8.5.3 连接管理配置

```properties
# 最大客户端连接数（默认 20000）
nacos.remote.server.grpc.max.connection=20000

# 客户端连接空闲超时（单位 ms，默认 300000ms = 5分钟）
nacos.remote.server.grpc.connection.idle.timeout=300000

# 连接清理周期（单位 ms，默认 60000ms = 60秒）
nacos.remote.server.grpc.connection.clean.period=60000

# 服务端推送线程池大小（默认 16）
nacos.remote.server.push.thread.count=16

# 服务端推送队列容量（默认 1024）
nacos.remote.server.push.queue.capacity=1024
```

## 8.6 鉴权安全配置

```properties
# 是否开启鉴权（默认 false）
nacos.core.auth.enabled=false

# 认证系统类型（默认 nacos）
nacos.core.auth.system.type=nacos

# JWT Token 密钥（Base64 编码，长度不低于32字符）
nacos.core.auth.default.token.secret.key=

# JWT Token 过期时间（单位 ms，默认 18000000ms = 5小时）
nacos.core.auth.default.token.expire.seconds=18000

# 缓存功能开关（默认 true）
nacos.core.auth.caching.enabled=true

# 是否开启白名单鉴权（默认 false）
nacos.core.auth.enable.userAgentAuthWhite=false

# 白名单用户代理列表
nacos.core.auth.server.identity.key=
nacos.core.auth.server.identity.value=

# 管理员用户名（默认 nacos）
nacos.core.auth.admin.user=nacosror

# 管理员密码（默认 nacos）
nacos.core.auth.admin.password=nacos
```

## 8.7 Istio 集成配置

```properties
# 是否开启 Istio 集成（默认 false）
nacos.istio.enabled=false

# Istio MCP Server 地址（默认 istio-pilot.istio-system:15010）
nacos.istio.mcp.server.addr=istio-pilot.istio-system:15010

# Istio 服务同步周期（单位秒，默认 60秒）
nacos.istio.sync.period=60

# Istio 域名后缀（默认 nacos）
nacos.istio.domain.suffix=nacos
```

## 8.8 监控与 Metrics 配置

```properties
# Prometheus metrics 开关（默认 false）
nacos.metrics.prometheus.enabled=false

# Prometheus metrics HTTP 端口（默认 9999）
nacos.metrics.prometheus.port=9999

# JMX metrics 开关（默认 false）
nacos.metrics.jmx.enabled=false

# Elasticsearch metrics 开关（默认 false）
nacos.metrics.elasticsearch.enabled=falseega

# 访问日志开关（默认 false）
nacos.access.log.enabled=false

# 访问日志滚动保留天数（默认 30 天）
nacos.access.log.retention.days=30

# 远程服务器日志开关（默认 false）
nacos.remote.server.log.enabled=false

# 慢 SQL 日志开关（默认 false）
nacos.config.slow.sql.log.enabled=false

# 慢 SQL 时间阈值（单位 ms，默认 200ms）
nacos.config.slow.sql.threshold=200
```

## 8.9 日志配置

```properties
# 日志目录（默认 ${nacos.home}/logs/）
nacos.logging.default.dir=
```

配套 logback.xml 配置参考：

```xml
<!-- logback.xml -->
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <!-- 集群日志 -->
    <appender name="nacos-cluster" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/nacos-cluster.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>${nacos.home}/logs/nacos-cluster.log.%d{yyyy-MM-dd}.%i.gz</fileNamePattern>
            <maxHistory>30</maxHistory>
            <totalSizeCap>3GB</totalSizeCap>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <!-- naming-server.log -->
    <appender name="naming-server" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/naming-server.log</file>
        <!-- similar config -->
    </appender>
    
    <!-- config-server.log -->
    <appender name="config-server" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/config-server.log</file>
    </appender>
    
    <!-- remote-server.log -->
    <appender name="remote-server" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/remote-server.log</file>
    </appender>
    
    <!-- access.log -->
    <appender name="access-log" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/access.log</file>
    </appender>
    
    <logger name="com.alibaba.nacos.core.cluster" level="INFO" additivity="false">
        <appender-ref ref="nacos-cluster"/>
    </logger>
    <logger name="com.alibaba.nacos.naming" level="INFO" additivity="false">
        <appender-ref ref="naming-server"/>
    </logger>
    <logger name="com.alibaba.nacos.config" level="INFO" additivity="false">
        <appender-ref ref="config-server"/>
    </logger>
    <logger name="com.alibaba.nacos.remote" level="INFO" additivity="false">
        <appender-ref ref="remote-server"/>
    </logger>
    
    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

## 8.10 性能调优参考配置汇总

生产环境推荐的 JVM 参数和 Nacos 配置组合：

```bash
# JVM 参数（根据机器配置调整 -Xmx）
java -server -Xms4g -Xmx4g -Xmn2g
  -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=256m
  -Xss512k
  -XX:+UseG1GC
  -XX:G1HeapRegionSize=16m
  -XX:G1ReservePercent=25
  -XX:InitiatingHeapOccupancyPercent=30
  -XX:SoftRefLRUPolicyMSPerMB=0
  -verbose:gc
  -Xloggc:${NACOS_HOME}/logs/nacos_gc.log
  -XX:+PrintGCDetails
  -XX:+PrintGCDateStamps
  -XX:+PrintGCApplicationStoppedTime
  -XX:+DisableExplicitGC
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:HeapDumpPath=${NACOS_HOME}/logs/java_heapdump.hprof
  -Djava.security.egd=file:/dev/urandom
  -jar nacos-server.jar
```

```properties
# 生产环境推荐 Nacos 配置
server.port=8848

# MySQL 数据源
spring.datasource.platform=mysql
db.num= sacrificio
db.url.0=jdbc:mysql://mysql-primary:3306/nacos_config?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=strongpassword

# 鉴权开启
nacos.core.auth.enabled=true
nacos.core.auth.default.token.secret.key=VGhpc0lzTmVlZFRvQmVBdDM2Q2hhcmFjdGVyU3Ryb25n

# gRPC 最大连接数
nacos.remote.server.grpc.max.connection=50000

# 连接空闲超时
nacos.remote.server.grpc.connection.idle.timeout=180000

# 推送线程池大小
nacos.remote.server.push.thread.count=64

# 健康检查超时（生产环境适当调大）
nacos.naming.heartbeat.timeout=30000
nacos.naming.instance.expire.time=120000

# 保护模式阈值（生产环境适当降低）
nacos.naming.protect.threshold=0.3
```

---

*（第八章完，约 坪万字）*
