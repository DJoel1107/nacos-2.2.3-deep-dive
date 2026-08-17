# 第10章：高可用架构设计与性能调优

## 10.1 AP vs CP 模式选择策略

### 10.1.1 CAP 理论在 Nacos 中的实践

Nacos 在同一个集群中同时支持 AP 和 CP 模式，通过 `ephemeral` 字段动态选择：

| 属性 | AP 模式（临时实例） | CP 模式（持久化实例） |
|------|---------------------|----------------------|
| `ephemeral` | `true`（默认） | `false` |
| 一致性协议 | Distro（自研） | JRaft（SOFAJRaft） |
| 共识算法 | 去中心化异步复制 | Leader-based Raft |
| 读写延迟 | 低（1~3ms） | 中（5~15ms） |
| 注册速度 | 快（仅内存写入） | 慢（需多数派确认） |
| 数据持久化 | 否（重启丢失） | 是（Raft Log持久化） |
| 最少节点数 | 1（单机可用） | 3（保证多数派） |
| 适用场景 | 微服务注册/发现 | DNS/F5注册/关键元数据 |

### 10.1.2 模式选择决策矩阵

```
                        开始
                         │
                         ▼
                ┌─────────────────┐
                │ 是否需要持久化？ │
                └────────┬────────┘
                 Yes     │     No
                 │       │       │
                 ▼       │       ▼
         ┌───────────┐  │  ┌──────────────┐
         │ CP 模式    │  │  │ AP 模式       │
         │ (JRaft)   │  │  │ (Distro)     │
         └─────┬─────┘  │  └──────┬───────┘
               │         │          │
               ▼         │          ▼
     ┌─────────────────┐  │  ┌──────────────────┐
     │ 低频变更场景   │  │  │ 高频心跳场景    │
     │ DNS/F5注册     │  │  │ 微服务自动注册  │
     │ 关键元数据     │  │  │ 临时状态数据    │
     └─────────────────┘  │  └──────────────────┘
                         │
                         ▼
                   混合部署模式
            （同一集群同时支持 AP + CP）
```

## 10.2 集群脑裂防护

### 10.2.1 脑裂场景分析

```
┌─────────────────────────────────────────────────────────┐
│                    正常集群状态                        │
│  ┌────────┐  ┌────────┐  ┌────────┐                │
│  │ Node A │  │ Node B │  │ Node C │                │
│  │ Leader │  │Follower│  │Follower│                │
│  └───┬────┘  └───┬────┘  └───┬────┘                │
│      └─────────────┼─────────────┘                  │
└─────────────────────────────────────────────────────────┘
                       │
                网络分区发生
                       │
┌──────────────────────────────────────────────────────────┐
│                   脑裂状态                              │
│  ┌─────────────┐              ┌─────────────┐       │
│  │  分区 A     │              │  分区 B     │       │
│  │  Node A     │              │  Node B     │       │
│  │  (原Leader) │              │  Node C     │       │
│  │             │              │  (新Leader) │       │
│  └─────────────┘              └─────────────┘       │
│  两个分区各自选举 Leader → 数据冲突!               │
└──────────────────────────────────────────────────────────┘
```

### 10.2.2 Raft Pre-Vote 防脑裂机制

```java
// Nacos JRaft 集成 Pre-Vote 机制
@Override
public void handlePreVoteRequest(PreVoteRequest request) {
    // 1. term 检查
    if (request.getTerm() <= this.currentTerm) {
        response(false, "term expired");
        return;
    }
    
    // 2. 检查 Leader 是否仍然存活（关键！）
    if (this.leaderAlive() && this.isCurrentLeader()) {
        // Leader 仍然存活，拒绝 Pre-Vote
        response(false, "current leader is alive");
        return;
    }
    
    // 3. 检查日志是否最新
    if (!isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm())) {
        response(false, "log not up-to-date");
        return;
    }
    
    // 4. 同意 Pre-Vote
    response(true, "granted");
}
```

### 10.2.3 脑裂恢复策略

```
Phase 1: 检测脑裂
┌────────────────────────────────────────────┐
│ 通过集群间健康检查检测节点不可达         │
│ - gRPC keepalive 超时                    │
│ - TCP 健康检查失败                       │
└────────────────┬───────────────────────────┘
                │
                ▼
Phase 2: 仲裁确定
┌────────────────────────────────────────────┐
│ JRaft Leader 选举自动恢复                │
│ - 拥有多数派的分区继续服务              │
│ - 少数派分区自动降级为 Follower        │
└────────────────┬───────────────────────────┘
                │
                ▼
Phase 3: 数据合并
┌────────────────────────────────────────────┐
│ 少数派分区重新加入集群                   │
│ - Leader 发送缺失的 Raft Log             │
│ - Follower 追赶日志                     │
│ - 日志追赶完成后恢复正常状态            │
└────────────────────────────────────────────┘
```

### 10.2.4 防止脑裂的配置建议

```properties
# JRaft 选举超时（ms，适当增大以减少误选举）
nacos.core.raft.election.timeout=5000n

# Raft 心跳周期（ms，Leader 发送心跳的频率）
nacos.core.raft.heartbeat.interval=1000r

# Raft 日志复制批量大小（适合高吞吐场景）
nacos.core.raft.log.batch.size=100

# gRPC keepalive 时间（检测节点故障）
nacos.remote.server.grpc.cluster.keep-alive-time=7200000

# gRPC keepalive 超时（快速发现节点不可达）
nacos.remote.server.grpc.cluster.keep-alive-timeout=20000
```

## 10.3 异地多活方案

### 10.3.1 三中心架构

```
┌─────────────────────────────────────────────────────────────┐
│                    全局流量调度层                            │
│              (GeoDNS + 全局负载均衡)                       │
└────┬────────────────────┬────────────────────┬────────────┘
     │                    │                    │
┌────▼─────┐      ┌────▼─────┐      ┌────▼─────┐
│ 中心 A   │      │ 中心 B   │      │ 中心 C   │
│ (主中心) │      │ (同城灾备)│      │ (异地灾备)│
│          │      │          │      │          │
│ Nacos   │      │ Nacos   │      │ Nacos   │
│ 3 Nodes │      │ 3 Nodes │      │ 3 Nodes │
│          │      │          │      │          │
│ MySQL   │      │ MySQL   │      │ MySQL   │
│ Master  │      │ Slave   │      │ Slave   │
└────┬─────┘      └────┬─────┘      └────┬─────┘
     │                  │                  │
     └──────────────────┼──────────────────┘
                        │
              MySQL 半同步复制
```

### 10.3.2 流量切换策略

| 场景 | 操作 | RTO | RPO |
|------|------|-----|-----|
| 单节点故障 | 集群内自动切换 | < 30秒 | 0 |
| 中心A整体故障 | DNS切换至中心B | < 5分钟 | < 1分钟 |
| 同城双中心故障 | DNS切换至异地中心C | < 10分钟 | < 5分钟 |

### 10.3.3 配置同步确保

```bash
# MySQL 半同步复制配置（保证主从数据一致性）
# Master 配置
INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so';
SET GLOBAL rpl_semi_sync_master_enabled = 1;
SET GLOBAL rpl_semi_sync_master_timeout = 30000;  # 30秒超时

# Slave 配置
INSTALL PLUGIN rpl_semi_sync_slave SONAME 'semisync_slave.so';
SET GLOBAL rpl_semi_sync_slave_enabled = 1;

# 验证半同步状态
SHOW STATUS LIKE 'rpl_semi_sync%';
```

## 10.4 JVM 参数调优实战

### 10.4.1 堆内存配置指南

```bash
# 根据机器配置确定 JVM 堆大小

# 物理内存 ≤ 8GB 场景（小型集群）
-Xms2g -Xmx2g -Xmn1g
-XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=128m

# 物理内存 16GB 场景（中型集群）
-Xms4g -Xmx4g -Xmn2g
-XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=256m

# 物理内存 ≥ 32GB 场景（大型集群）
-Xms8g -Xmx8g -Xmn4g
-XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=512m
```

### 10.4.2 GC 策略选择

```bash
# G1 GC（推荐，JDK 8+）
-XX:+UseG1GC
-XX:G1HeapRegionSize=16m
-XX:G1ReservePercent=25
-XX:InitiatingHeapOccupancyPercent=30
-XX:MaxGCPauseMillis=200
-XX:+ParallelRefProcEnabled
-XX:+DisableExplicitGC

# GC 日志配置
-Xloggc:/home/nacos/logs/nacos_gc.log
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-XX:+PrintGCApplicationStoppedTime
-XX:+PrintGCDetails
-XX:+PrintTenuringDistribution

# OOM 时自动 HeapDump
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/home/nacos/logs/
```

### 10.4.3 GC 调优目标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| Young GC 频率 | ≤ 10次/分钟 | 正常负载 |
| Young GC 暂停时间 | ≤ 50ms | G1 GC 优化 |
| Mixed GC 暂停时间 | ≤ 200ms | 可接受 |
| Full GC 频率 | 0次/天 | 严禁频繁 Full GC |
| 堆内存使用率 | 60~80% | 留有20%余量 |
| 晋升到老年代速率 | ≤ 10% | 控制对象晋升速率 |

### 10.4.4 线程栈大小优化

```bash
# 线程栈大小根据实际线程数调整
# Nacos 线程数（保守估计约 500~1000 条）
# 每条线程栈大小 * 线程数 ≈ 内存占用

-Xss512k    # 每条线程 512KB 栈空间
# 500 条线程 × 512KB ≈ 256MB 栈内存占用

-Xss256k    # 每条线程 256KB 栈空间（如果无深递归调用）
# 500 条线程 × 256KB ≈ 128MB 栈内存占用
```

## 10.5 Nacos 核心参数调优

### 10.5.1 gRPC 线程池优化

```properties
# gRPC 服务端线程池大小（根据 CPU 核数调整）
# 推荐：CPU 核数 × 2
nacos.remote.server.grpc.thread.pool.size.core=16
nacos.remote.server.grpc.thread.pool.size.max=32

# gRPC 客户端连接数上限
# 推荐：保守 20000，激进 50000
nacos.remote.server.grpc.max.connection=20000

# gRPC 连接空闲超时（避免连接泄漏）
nacos.remote.server.grpc.connection.idle.timeout=180000
```

### 10.5.2 推送线程池优化

```properties
# 推送线程池大小（影响服务变更推送延迟）
# 推荐：CPU 核数 × 4
nacos.remote.server.push.thread.count=64

# 推送队列容量（避免 OOM）
nacos.remote.server.push.queue.capacity=16384
```

### 10.5.3 健康检查参数优化

```properties
# 心跳超时时间（适当增大减少误判）
nacos.naming.heartbeat.timeout=30000

# 心跳间隔（不要太高，增加网络负担）
nacos.naming.heartbeat.interval=5000

# 实例过期时间（清理长时间不健康实例）
nacos.naming.instance.expire.time=120000

# 健康检查线程池（根据实例数量调整）
nacos.naming.health.check.thread.pool.size=64
```

### 10.5.4 防雪崩保护阈值优化

```properties
# 保护模式阈值（生产环境适当降低）
nacos.naming.protect.threshold=0.3

# 开启保护模式
nacos.naming.protect.enabled=trueayega
```

## 10.6 数据库连接池优化

```properties
# HikariCP 连接池优化
db.pool.config.maximumPoolSize=50
db.pool.config.minimumIdle=10
db.pool.config.connectionTimeout=10000
db.pool.config.idleTimeout=600000
db.pool.config.maxLifetime=1800000
db.pool.config.connectionTestQuery=SELECT 1ridb
db.pool.config.validationTimeout=3000sra
db.pool.config.leakDetectionThreshold=10000
```

### 10.6.1 MySQL 连接数规划

| 集群节点数 | Nacos 连接数/节点 | MySQL 最大连接数 | 推荐配置 |
|-----------|-----------------|-----------------|---------| 
| 3 | 50 | 200 | `max_connections=250` |
| 5 | 50 | 300 | `max_connections=350` |
| 7 | 50 | 400 | `max_connections=450` |

```sql
-- MySQL 配置优化
SET GLOBAL max_connections = 500;
SET GLOBAL innodb_buffer_pool_size = 2147483648;  # 2GB
SET GLOBAL innodb_log_file_size = 524288000;       # 500MB
SET GLOBAL innodb_flush_log_at_trx_commit = 显得有些;
SET GLOBAL sync_binlog = 0;  # 性能优先（配合半同步复制保证可靠性）
```

## 10.7 压测方案与性能基线

### 10.7.1 压测工具选择

| 场景 | 推荐工具 | 说明 |
|------|---------|------|
| 服务注册 | JMH（Java Microbenchmark Harness） | 微基准测试 |
| 配置发布 | Apache JMeter | HTTP 压测 |
| 服务发现 | JMeter + gRPC sampler | 大规模实例拉取 |
| 长轮询 | 自研长轮询压测工具 | 数千并发客户端 |

### 10.7.2 Nacos 2.2.3 官方性能基线

| 指标 | 3节点集群 | 5节点集群 | 说明 |
|------|----------|----------|------|
| 最大客户端连接数 | 20000 | 50000 | gRPC 长连接 |
| 服务注册 TPS | 15000/s | 25000/s | 临时实例 |
| 服务发现 QPS | 50000/s | 100000/s | 本地缓存命中 |
| 配置获取 QPS | 30000/s | 50000/s | 长轮询优化 |
| 配置变更通知延迟 | < 100ms | < 100ms | 集群内同步 |

### 10.7.3 JMeter 配置示例

```xml
<!-- JMeter 压测计划 - Nacos Config API -->
<jmeterTestPlan version="1.2" properties="2.8">
  <hashTree>
    <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" 
        testname="Nacos Config API 压测" enabled="true">
      <stringProp name="ThreadGroup.num_threads">1000</stringProp>
      <stringProp name="ThreadGroup.ramp_time">60</stringProp>
      <boolProp name="ThreadGroup.scheduler">false</boolProp>
      <boolProp name="ThreadGroup.same_user_on_next_iteration">true</boolProp>
    </ThreadGroup>
    
    <!-- HTTP Request - 发布配置 -->
    <HTTPSamplerProxy testname="Publish Config" enabled="true">
      <elementProp name="HTTPsampler.Arguments" elementType="Arguments">
        <collectionProp name="Arguments.arguments">
          <elementProp name="dataId" elementType="HTTPArgument">
            <boolProp name="Argument.always_encode">false</boolProp>
            <stringProp name="Argument.value">${__UUID()}</stringProp>
            <stringProp name="Argument.metadata">=</stringProp>
          </elementProp>
          <elementProp name="group" elementType="HTTPArgument">
            <boolProp name="Argument.always_encode">false</boolProp>
            <stringProp name="Argument.value">DEFAULT_GROUP</stringProp>
          </elementProp>
          <elementProp name="content" elementType="HTTPArgument">
            <boolProp name="Argument.always_encode">false</boolProp>
            <stringProp name="Argument.value">test_content_${__counter(FALSE,)}</stringProp>
          </elementProp>
        </collectionProp>
      </elementProp>
      <stringProp name="HTTPSampler.domain">nacos-server</stringProp>
      <stringProp name="HTTPSampler.port">8848</stringProp>
      <stringProp name="HTTPSampler.path">/nacos/v1/cs/configs</stringProp>
      <stringProp name="HTTPSampler.method">POST</stringProp>
    </HTTPSampler>
  </hashTree>
</jmeterTestPlan>
```

## 10.8 监控指标体系

### 10.8.1 Prometheus Metrics 导出

```properties
# 开启 Prometheus Metrics
nacos.metrics.prometheus.enabled=true
nacos.metrics.prometheus.port=9999

# 访问 http://nacos-server:9999/metrics 查看所有指标
```

### 10.8.2 核心 Prometheus 指标

| 指标名称 | 类型 | 说明 | 告警阈值 |
|---------|------|------|---------|
| `nacos_server_connection_count` | Gauge | 当前客户端连接数 | > 15000 |
| `nacos_naming_service_count` | Gauge | 注册服务数 | > 80000 |
| `nacos_naming_instance_count` | Gauge | 注册实例数 | > 800000 |
| `nacos_config_publish_total` | Counter | 配置发布次数 | — |
| `nacos_config_get_total` | Counter | 配置获取次数 | — |
| `nacos_raft_log_index` | Gauge | Raft 日志最新索引 | 落后 > 1000 |
| `nacos_distro_sync_fail_total` | Counter | Distro 同步失败次数 | > 0 |
| `jvm_memory_used_bytes` | Gauge | JVM 内存使用量 | > 80% of max |
| `jvm_gc_pause_seconds` | Histogram | GC 暂停时间 | > 500ms |
| `process_cpu_seconds_total` | Counter | CPU 使用时间 | — |
| `hikaricp_active_connections` | Gauge | 数据库活跃连接数 | > 40 |

### 10.8.3 Grafana Dashboard 推荐面板

```json
// Nacos Dashboard JSON 模板（核心面板）
{
  "dashboard": {
    "title": "Nacos 2.2.3 Production Monitoring",
    "panels": [
      {
        "title": "Client Connections",
        "targets": [{
          "expr": "nacos_server_connection_count"
        }]
      },
      {
        "title": "Service Count by Namespace",
        "targets": [{
          "expr": "nacos_naming_service_count"
        }]
      },
      {
        "title": "Config Publish Rate",
        "targets": [{
          "expr": "rate(nacos_config_publish_total[5m])"
        }]
      },
      {
        "title": "JVM Heap Usage",
        "targets": [{
          "expr": "jvm_memory_used_bytes{area=\"heap\"} / jvm_memory_max_bytes{area=\"heap\"}"
        }]
      },
      {
        "title": "GC Pause Time",
        "targets": [{
          "expr": "rate(jvm_gc_pause_seconds_sum[5m]) / rate(jvm_gc_pause_seconds_count[5m])"
        }]
      }
    ]
  }
}
```

### 10.8.4 关键告警规则（Prometheus AlertManager）

```yaml
groups:
  - name: nacos_alerts
    rules:
      - alert: NacosHighConnections
        expr: nacos_server_connection_count > 15000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Nacos 客户端连接数过高"
          
      - alert: NacosNodeDown
        expr: nacos_cluster_node_state != 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Nacos 节点故障"
          
      - alert: NacosDistroSyncFailure
        expr: rate(nacos_distro_sync_fail_total[5m]) > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Distro 数据同步失败"
          
      - alert: NacosJVMHighMemoryUsage
        expr: jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Nacos JVM 堆内存使用率超过 85%"
          
      - alert: NacosFullGC
        expr: increase(jvm_gc_pause_seconds_count{action="end of major GC"}[5m]) > 0
        for: 丛m
        labels:
          severity: critical
        annotations:
          summary: "Nacos 发生 Full GC"
```

---

*（第十章完，约 1.6 万字）*
