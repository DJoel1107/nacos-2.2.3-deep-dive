# 第11章：Nacos 2.5.3 性能调优深度分析

## 11.1 JVM 堆内存配置指南

| 集群规模 | 节点数 | 推荐 -Xms / -Xmx | 推荐 -Xmn | 说明 |
|---------|--------|-----------------|-----------|------|
| **小型** | ≤3 | `-Xms2g -Xmx2g` | `-Xmn1g` | 开发/测试环境 |
| **中型** | 3-5 | `-Xms4g -Xmx4g` | `-Xmn2g` | 生产环境（中小规模） |
| **大型** | ≥5 | `-Xms8g -Xmx8g` | `-Xmn4g` | 生产环境（大规模） |

### JAVA_OPT 配置示例

```bash
# bin/startup.sh 中的 JAVA_OPT
JAVA_OPT="${JAVA_OPT} -server -Xms4g -Xmx4g -Xmn2g"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC"
JAVA_OPT="${JAVA_OPT} -XX:MaxGCPauseMillis=200"
JAVA_OPT="${JAVA_OPT} -XX:G1HeapRegionSize=8m"
JAVA_OPT="${JAVA_OPT} -XX:G1ReservePercent=10"
JAVA_OPT="${JAVA_OPT} -XX:InitiatingHeapOccupancyPercent=45"
```

## 11.2 GC 策略选择：G1GC 完整参数详解

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `-XX:+UseG1GC` | 启用 | 使用 G1 GC（推荐） |
| `-XX:MaxGCPauseMillis` | `200` | 最大 GC 暂停目标（毫秒） |
| `-XX:G1HeapRegionSize` | `8m` | G1 Region 大小 |
| `-XX:G1ReservePercent` | `10` | 保留空间百分比 |
| `-XX:InitiatingHeapOccupancyPercent` | `45` | 触发 Mixed GC 的堆占用阈值 |
| `-XX:ConcGCThreads` | `2` | 并发 GC 线程数 |
| `-XX:ParallelGCThreads` | `8` | 并行 GC 线程数 |

## 11.3 GC 调优目标参考表

| 指标 | 小5型集群 | 中型集群 | 大型集群 |
|------|---------|---------|---------|
| Young GC 频率 | 10-20次/小时 | 5-10次/小时 | 3-5次/小时 |
| Young GC 暂停时间 | < 50ms | < ít100ms | < 150ms |
| Mixed GC 频率 | 1-2次/天 | 0-1次/天 | 0-1次/天 |
| Full GC 频率 | **0** | **0** | **0** |
| 堆使用率 | 50-70% | 50-70% | 50-70% |
| 晋升速率 | < 10MB/秒 | < 50MB/秒 | < 100MB/秒 |

## 11.4 GC 日志配置

```bash
JAVA_OPT="${JAVA_OPT} -Xloggc:${NACOS_HOME}/logs/nacos_gc.log"
JAVA_OPT="${JAVA_OPT} -XX:+PrintGCDetails"
JAVA_OPT="${JAVA_OPT} -XX:+PrintGCDateStamps"
JAVA_OPT="${JAVA_OPT} -XX:+PrintGCApplicationStoppedTime"
JAVA_OPT="${JAVA_OPT} -XX:+PrintHeapAtGC"
JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation"
JAVA_OPT="${JAVA_OPT} -XX:NumberOfGCLogFiles=10"
JAVA_OPT="${JAVA_OPT} -XX:GCLogFileSize=100M"
```

## 11.5 线程栈大小优化

| 集群规模 | 推荐 -Xss | 预计线程数 | 线程栈内存占用 |
|---------|-----------|-----------|--------------|
| 小型 | `-Xss512k` | ~500 | ~256MB |
| 中型 | `-Xss512k` | ~1000 | ~512MB |
| 大型 | `-Xss512k` | ~2000 | ~1GB |

## 11.6 gRPC 线程池优化

**★ 2.5.3 gRPC 版本升级（1.50.2→1.75.0）性能影响**：

| 线程池 | 配置项 | SDK Server 默认值 | Cluster Server 默认值 | 推荐（中型） |
|--------|--------|----------------|-------------------|-------------|
| 核心线程 | `*.thread-pool.core-size` | 64 | 8 | **128 / 16** |
| 最大线程 | `*.thread-pool.max-size` | 128 | 16 | **256 / 32** |
| 队列容量 | `*.thread-pool.queue-size` | 1024 | 256 | **2048 / 512** |

**★ 2.5.3 新增 `GrpcServerThreadPoolMonitor`**：gRPC 线程池 Metrics 监控，可实时查看线程池状态。

## 11.7 推送线程池 + 队列容量优化

| 配置项 | 默认值 | 推荐（中型） | 说明 |
|--------|--------|------------|------|
| `nacos.remote.push.thread.count` | 8 | **16** | 推送线程数 |
| `nacos.remote.push.queue.capacity` | 4096 | **8192** | 推送队列容量 |

## 11.8 健康检查参数优化

| 配置项 | 默认值 | 推荐（中型） | 说明 |
|--------|--------|------------|------|
| `heartbeat.timeout` | 15000 | **10000** | 心跳超时（毫秒） |
| `heartbeat.interval` | 5000 | **3000** | 心跳间隔（毫秒） |
| `expire.time` | 30000 | **20000** | 实例过期时间（毫秒） |

## 11.9 防雪崩保护阈值优化

| 配置项 | 默认值 | 推荐 | 说明 |
|--------|--------|------|------|
| `nacos.naming.protect.threshold` | 0.5 | **0.3** | 降低阈值提高保护敏感度 |

## 11.10 MySQL 连接池优化（HikariCP）

**★ 2.5.3 MySQL Connector 8.2.0 HikariCP 参数优化**：

| 参数 | 默认值 | 推荐（中型） | 说明 |
|------|--------|------------|------|
| `maximumPoolSize` | 20 | **50** | 最大连接池大小 |
| `minimumIdle` | 2 | **10** | 最小空闲连接数 |
| `connectionTimeout` | 30000 | **25000** | 连接超时（毫秒） |
| `idleTimeout` | 600000 | **300000** | 空闲超时（毫秒） |
| `maxLifetime` | 1800000 | **1800000** | 最大生命周期（毫秒） |
| `leakDetectionThreshold` | 0 | **60000** | 连接泄露检测阈值（毫秒） |

## 11.11 MySQL 连接数规划

| 节点数 | MySQL `max_connections` | `innodb_buffer_pool_size` | 说明 |
|--------|----------------------|--------------------------|------|
| 3 | 200 | 2GB | 小型集群 |
| 5 | 400 | 4GB | 中型集群 |
| 7 | 600 | 8GB | 大型集群 |

## 11.12 压测工具选择对比

| 工具 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| **JMH** | 微基准测试（单方法） | 精确、JVM 预热 | 只能测试Java API |
| **JMeter** | HTTP/gRPC 压测 | 图形化界面、插件丰富 | 资源消耗大 |
| **gRPC sampler** | gRPC 服务压测 | 原生 gRPC 协议支持 | 需要编写 proto |

## 11.13 JMeter 压测配置示例

```xml
<!-- 配置注册压测 ThreadGroup -->
<ThreadGroup>
    <elementProp name="ThreadGroup.main_controller" 
        elementType="LoopController"/>
    <stringProp name="ThreadGroup.num_threads">100</stringProp>
    <stringProp name="ThreadGroup.ramp_time">10</stringProp>
    <boolProp name="ThreadGroup.scheduler">false</boolProp>
    <stringProp name="ThreadGroup.duration">300</stringProp>
</ThreadGroup>

<!-- HTTP Request 服务注册 -->
<HTTPSamplerProxy>
    <elementProp name="HTTPsampler.Arguments">
        <collectionProp name="Arguments.arguments">
            <elementProp name="serviceName" 
                elementType="HTTPArgument">
                <stringProp name="Argument.value">demo-service</stringProp>
            </elementProp>
            <elementProp name="ip" 
                elementType="HTTPArgument">
                <stringProp name="Argument.value">192.168.1.100</stringProp>
            </elementProp>
            <elementProp name="port" 
                elementType="HTTPArgument">
                <stringProp name="Argument.value">8080</stringProp>
            </elementProp>
        </collectionProp>
    </elementProp>
    <stringProp name="HTTPSampler.domain">nacos.example.com</stringProp>
    <stringProp name="HTTPSampler.port">8848</stringProp>
    <stringProp name="HTTPSampler.path">/nacos/v1/ns/instance</stringProp>
    <stringProp name="HTTPSampler.method">POST</stringProp>
</HTTPSamplerProxy>
```

## 11.14 Nacos 2.5.3 性能基线参考值

| 指标 | 3 节点集群 | 5 节点集群 | 7 节点集群 |
|------|-----------|-----------|-----------|
| **注册 TPS** | ~10,000 | ~15,000 | ~20,000 |
| **发现 QPS** | ~50,000 | ~80,000 | ~100,000 |
| **配置发布 TPS** | ~5,000 | ~8,000 | ~10,000 |
| **配置查询 QPS** | ~50,000 | ~80,000 | ~100,000 |
| **长轮询并发** | ~32,000 | ~64,000 | ~128,000 |
| **gRPC 连接数** | ~10,000 | ~20,000 | ~30,000 |
| **平均响应延迟（P99）** | < 10ms | < 15ms | < 20ms |
| **★ gRPC 1.75.0 提升** | — | **~10-15% 吞吐提升** | **~10-15%** |

## 11.15 OS 内核参数优化

```bash
# /etc/sysctl.conf (★ 2.5.3 新增 gRPC 相关优化)

# TCP 缓冲区优化（gRPC 长连接）
net.core.rmem_default = 262144
net.core.rmem_max = 16777216
net.core.wmem_default = 262144
net.core.wmem_max = 16777216

# TCP KeepAlive（gRPC 长连接保持）
net.ipv4.tcp_keepalive_time = 1200
net.ipv4.tcp_keepalive_probes = 3
net.ipv4.tcp_keepalive_intvl = 30

# TCP Fast Open（★ gRPC 1.75.0 支持 TFO）
net.ipv4.tcp_fastopen = 3

# TCP 半连接队列（高并发 gRPC 连接）
net.ipv4.tcp_max_syn_backlog = 8192

# Socket 重用（TIME_WAIT 快速回收）
net.ipv4.tcp_tw_reuse =  поха1

# 端口范围（大量 gRPC 连接）
net.ipv4.ip_local_port_range = 1024 65535

# 应用
sysctl -p
```

### ★ 2.5.3 gRPC 性能增强

| 增强点 | gRPC 1.50.2 | gRPC 1.75.0 |
|--------|------------|------------|
| TCP Fast Open (TFO) | 不支持 | **支持** |
| HTTP/2 HPACK 压缩 | 基础 | **优化压缩算法** |
| Flow Control | 基础 | **增强 Flow Control 窗口** |
| ThreadPool 监控 | 无 | **GrpcServerThreadPoolMonitor** |

---

> **本章基于 Nacos 2.5.3 性能调优生成。**
