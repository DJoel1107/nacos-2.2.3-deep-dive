# 第12章：Nacos 2.5.3 监控运维

## 12.1 Prometheus Metrics 导出配置

```properties
# application.properties
management.endpoints.web.exposure.include=*
management.metrics.export.prometheus.enabled=true
```

访问 `http://nacos:8848/nacos/actuator/prometheus` 获取 Prometheus Metrics 数据。

## 12.2 核心 Prometheus 指标表

### 2.5.3 新增指标

| 指标名称 (Metrics Key) | 类型 | 说明 | 告警阈值 |
|------------------------|------|------|---------|
| `nacos_monitor_module_health` | Gauge | **★ 2.5.3 新增：模块健康状态** | 0=DOWN |
| `nacos_persistence_datasource_active` | Gauge | **★ 2.5.3 新增：persistence 数据源活跃连接数** | > 80% |
| `nacos_persistence_datasource_wait` | Gauge | **★ 2.5.3 新增：等待获取连接的线程数** | > 0 |
| `nacos_grpc_server_threadpool_active` | Gauge | **★ 2.5.3 新增：gRPC 线程池活跃线程数** | > 80% |
| `nacos_config_cache_size` | Gauge | **★ 2.5.3 新增：ConfigCache 缓存条目数** | > 80% |

### 已有指标（增强）

| 指标名称 | 类型 | 说明 |
|----------|------|------|
| `nacos_monitor_connections_count` | Gauge | 当前 gRPC 连接数 |
| `nacos_monitor_service_count` | Gauge | 已注册服务总数 |
| `nacos_monitor_instance_count` | Gauge | 已注册实例总数 |
| `nacos_monitor_config_publish_total` | Counter | 配置发布总数 |
| `nacos_monitor_config_get_total` | Counter | 配置查询总数 |
| `nacos_monitor_naming_register_total` | Counter | 服务注册总数 |
| `nacos_monitor_naming_subscribe_total` | Counter | 服务订阅总数 |
| `nacos_monitor_distro_task_total` | Counter | Distro 同步任务总数 |
| `nacos_monitor_raft_leader_count` | Gauge | Raft Leader 数量（应为1） |
| `jvm_memory_used_bytes` | Gauge | JVM 堆内存使用 |
| `jvm_gc_pause_seconds` | Summary | GC 暂停时间 |

## 12.3 Grafana Dashboard 推荐面板

### 5 个核心面板

| 面板 | 指标 | 说明 |
|------|------|------|
| **连接数面板** | `nacos_monitor_connections_count` | gRPC 连接数实时监控 |
| **服务数面板** | `nacos_monitor_service_count` + `nacos_monitor_instance_count` | 注册服务/实例数 |
| **配置速率面板** | `nacos_monitor_config_publish_total` + `config_get_total` | 配置发布/查询 QPS |
| **JVM 堆面板** | `jvm_memory_used_bytes` + `jvm_gc_pause_seconds` | JVM 堆使用 + GC 暂停 |
| **★ 2.5.3 模块健康面板** | `nacos_monitor_module_health` + `nacos_persistence_*` | 模块就绪 + persistence 数据源 |

### Grafana Dashboard JSON 导入

```json
{
  "dashboard": {
    "title": "Nacos 2.5.3 Monitor",
    "panels": [
      {
        "title": "Connections",
        "targets": [{
          "expr": "nacos_monitor_connections_count"
        }]
      },
      {
        "title": "Services & Instances",
        "targets": [
          {"expr": "nacos_monitor_service_count"},
          {"expr": "nacos_monitor_instance_count"}
        ]
      },
      {
        "title": "★ Module Health (2.5.3)",
        "targets": [
          {"expr": "nacos_monitor_module_health"}
        ]
      },
      {
        "title": "★ Persistence Datasource (2.5.3)",
        "targets": [
          {"expr": "nacos_persistence_datasource_active"},
          {"expr": "nacos_persistence_datasource_wait"}
        ]
      },
      {
        "title": "JVM Heap & GC",
        "targets": [
          {"expr": "jvm_memory_used_bytes"},
          {"expr": "jvm_gc_pause_seconds"}
        ]
      }
    ]
  }
}
```

## 12.4 Prometheus AlertManager 告警规则

### 5 条核心告警

```yaml
groups:
  - name: nacos_alerts
    interval: 30s
    rules:
      # 告警 1：高连接数
      - alert: NacosHighConnections
        expr: nacos_monitor_connections_count > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Nacos 高连接数 (> 10000)"

      # 告警 2：节点 Down
      - alert: NacosNodeDown
        expr: nacos_monitor_module_health == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Nacos 节点 DOWN"

      # 告警 3：Distro 同步失败
      - alert: NacosDistroFail
        expr: rate(nacos_monitor_distro_task_total{status="fail"}[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Distro 同步失败"

      # 告警 4：JVM 内存高使用率
      - alert: NacosJVMHighMemory
        expr: jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "JVM 堆内存使用率 > 85%"

      # ★ 告警 5：persistence 数据源连接池告警 (2.5.3 新增)
      - alert: NacosPersistenceDatasourcePoolExhausted
        expr: nacos_persistence_datasource_wait > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "persistence 数据源连接池耗尽"
```

## 12.5 日志分析

### 5 种日志文件详解

| 日志文件 | 内容 | 关注关键词 |
|---------|------|-----------| 
| `nacos-cluster.log` | 集群管理日志 | `MEMBER_CHANGE`, `LEADER_CHANGE` |
| `naming-server.log` | 注册中心日志 | `registerInstance`, `heartbeat`, `distro` |
| `config-server.log` | 配置中心日志 | `publishConfig`, `longPolling`, `dump` |
| `remote-server.log` | gRPC 通信日志 | `connect`, `disconnect`, `push` |
| `access.log` | HTTP 访问日志 | 所有 HTTP 请求 |

### ★ 2.5.3 新增日志内容

| 新增日志 | 模块 | 说明 |
|---------|------|------|
| `persistence datasource initialized` | persistence | persistence 数据源初始化 |
| `module health check` | core | 模块健康检查结果 |
| `ConfigCache hit/miss` | config | ConfigCache 缓存命中/未命中 |
| `ParamChecker validation` | core | ParamChecker 参数校验日志 |

## 12.6 日志滚动策略

```xml
<!-- conf/logback.xml -->
<appender name="SERVER_LOG" 
    class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${nacos.home}/logs/nacos-cluster.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
        <!-- 按天滚动 -->
        <fileNamePattern>${nacos.home}/logs/nacos-cluster.log.%d{yyyy-MM-dd}.%i.gz</fileNamePattern>
        <!-- 保留 90 天 -->
        <maxHistory>90</maxHistory>
        <!-- 总大小上限 10GB -->
        <totalSizeCap>10GB</totalSizeCap>
    </rollingPolicy>
</appender>
```

## 12.7 日常运维巡检清单

| 序号 | 巡检项 | 命令 | 期望结果 |
|------|--------|------|---------|
| 1 | **集群状态** | `curl nacos/v1/core/cluster/nodes` | 所有节点 `state=UP` |
| 2 | **gRPC 连接数** | `grep "connections" logs/remote-server.log` | < 最大连接数 |
| 3 | **JVM 内存** | `jstat -gcutil $(pgrep nacos) 1000` | 堆使用率 < 80% |
| 4 | **DB 连接池** | `curl nacos/v1/console/health/metrics` | `persistence.datasource.active` < 80% |
| 5 | **磁盘空间** | `df -h ${NACOS_HOME}` | 磁盘使用率 < 80% |
| 6 | **★ Raft Leader** | `curl nacos/v1/core/cluster/raft/leader` | 有且仅有一个 Leader |
| 7 | **错误日志** | `grep ERROR logs/*.log | tail -20` | 无严重错误 |

## 12.8 日常运维命令速查表

### curl API 命令

```bash
# 集群节点列表
curl http://localhost:8848/nacos/v1/core/cluster/nodes

# Raft Leader 查询
curl http://localhost:8848/nacos/v1/core/cluster/raft/leader

# 服务列表（分页）
curl "http://localhost:8848/nacos/v1/ns/service/list?pageNo=1&pageSize=1000"

# 实例列表
curl "http://localhost:8848/nacos/v1/ns/instance/list?serviceName=demo-service"

# 配置列表（分页）
curl "http://localhost:8848/nacos/v1/cs/configs?pageNo=1&pageSize=100"

# ★ 2.5.3: 模块健康状态
curl http://localhost:8848/nacos/v1/console/health
```

### JVM 诊断命令

```bash
# 线程堆栈
jstack $(pgrep -f nacos) > jstack.txt

# 堆 Dump（★ 生产环境谨慎使用）
jmap -dump:format=b,file=heap.hprof $(pgrep -f nacos)

# GC 统计
jstat -gcutil $(pgrep -f nacos) 1000 10

# JVM 参数
jinfo -flags $(pgrep -f nacos)
```

### 日志 grep 命令

```bash
# 错误日志
grep 'ERROR' logs/nacos-cluster.log | tail -20

# 慢 SQL（★ 2.5.3: persistence 模块）
grep 'slow' logs/nacos-cluster.log | tail -20

# Distro 同步失败
grep 'distro.*fail' logs/naming-server.log | tail -20

# gRPC 连接异常
grep 'disconnect' logs/remote-server.log | tail -20
```

## 12.9 定期运维任务

| 频率 | 任务 | 命令/操作 |
|------|------|-----------|
| **每日** | 巡检清单检查 | 见 §12.7 |
| **每周** | 日志轮转检查 | `du -sh logs/` |
| **每月** | 历史配置清理 | `DELETE FROM his_config_info WHERE gmt_create < DATE_SUB(NOW(), INTERVAL 90 DAY)` |
| **每月** | 过期实例清理 | 自动通过 ClientBeatCheckTask 清理 |
| **每季度** | Raft Snapshot 压缩 | 自动通过 JRaft Snapshot 机制 |

### ★ 2.5.3 新增运维任务

| 任务 | 说明 |
|------|------|
| **persistence 数据源连接池检查** | 检查 `nacos_persistence_datasource_active` 指标 |
| **模块健康状态检查** | 检查 `nacos_monitor_module_health` 指标 |
| **ConfigCache 缓存命中率检查** | 检查 `nacos_config_cache_hit_total` / `nacos_config_cache_miss_total` |
| **ParamChecker 校验日志检查** | 检查 `grep 'ParamChecker' logs/nacos-cluster.log` |

---

### 2.5.3 监控运维变更总结

| 变更点 | 2.2.3 | 2.5.3 |
|--------|-------|-------|
| 模块健康检查 | 无 | **`ModuleHealthChecker` + `NamingReadinessCheckService`** |
| persistence 监控 | 无 | **`DatasourceMetrics` 数据源 Metrics** |
| gRPC 线程池监控 | 无 | **`GrpcServerThreadPoolMonitor`** |
| ConfigCache 缓存监控 | 无 | **ConfigCache hit/miss Metrics** |
| Raft Leader 查询 | 无专用 API | **`/v1/core/cluster/raft/leader` API** |
| MySQL 驱动包名 | `mysql-connector-java` | **`mysql-connector-j`** |

---

> **本章基于 Nacos 2.5.3 监控运维生成。**
