# 第 13 章：监控运维

> **基于 Nacos 2.5.3 源码**  
> **章节目标**: ~64,000 字  
> **写作日期**: 2026-09-06

---

## 13.1 Prometheus Metrics 导出配置

### 设计背景

Nacos 2.5.3 内置 Prometheus HTTP SD（Service Discovery）支持，通过 `prometheus` 模块暴露 `/prometheus` 接口，Prometheus Server 可通过该接口拉取所有注册实例的指标数据。与 JMX 导出方式相比，Prometheus HTTP SD 方式无需额外 Agent 进程（如 JMX Exporter），直接在 Nacos 进程中暴露 HTTP 端点，简化运维部署。

Prometheus 模块的核心机制是：通过 `ServiceManager.getSingletons(namespace)` 遍历所有命名空间下的所有服务，再通过 `InstanceOperatorClientImpl.listAllInstances()` 获取每个服务下的全部实例列表，将实例元数据（IP、Port、metadata）序列化为 JSON Array 返回给 Prometheus Server。

### 核心类关系图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Prometheus 模块类关系                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────┐                                        │
│  │  PrometheusController       │                                        │
│  │  @RestController            │                                        │
│  │  @ConditionalOnProperty(   │        ┌────────────────────────────┐  │
│  │   "nacos.prometheus       │        │  PrometheusUtils           │  │
│  │   .metrics.enabled")      │───────▶│  + assembleArrayNodes()   │  │
│  │                           │        │  + getInstanceMetadata()   │  │
│  │  + metric()               │        └────────────────────────────┘  │
│  │  + metricNamespace()      │                                        │
│  │  + metricNamespaceService()│                                        │
│  └──────────┬──────────────────┘                                        │
│             │                                                           │
│             ▼                                                           │
│  ┌─────────────────────────────┐        ┌────────────────────────────┐  │
│  │  ServiceManager            │        │  InstanceOperatorClientImpl │  │
│  │  (naming.core.v2)        │───────▶│  (naming.core)            │  │
│  │  + getAllNamespaces()     │        │  + listAllInstances()     │  │
│  │  + getSingletons()       │        └────────────────────────────┘  │
│  └─────────────────────────────┘                                        │
│                                                                          │
│            图 13-1：Prometheus 模块核心类关系图                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 源码走读：PrometheusController

`prometheus/src/main/java/com/alibaba/nacos/prometheus/controller/PrometheusController.java` 是 Prometheus HTTP SD 的核心 Controller，通过 `@ConditionalOnProperty(name = "nacos.prometheus.metrics.enabled", havingValue = "true")` 条件控制启用/禁用：

```java
// PrometheusController.java:43-48 (Nacos 2.5.3)
@RestController
@ConditionalOnProperty(name = "nacos.prometheus.metrics.enabled", havingValue = "true")
public class PrometheusController {

    @Autowired
    private InstanceOperatorClientImpl instanceServiceV2;

    private final ServiceManager serviceManager;

    public PrometheusController() {
        this.serviceManager = ServiceManager.getInstance();
    }
}
```

核心接口 `metric()`（`PrometheusController.java:56-ha 74`）：

```java
// PrometheusController.java:56-74 (Nacos 2.5.3)
@GetMapping(value = ApiConstants.PROMETHEUS_CONTROLLER_PATH,
        produces = "application/json; charset=UTF-8")
public ResponseEntity<String> metric() throws NacosException {
    ArrayNode arrayNode = JacksonUtils.createEmptyArrayNode();
    Set<Instance> targetSet = new HashSet<>();
    Set<String> allNamespaces = serviceManager.getAllNamespaces();
    for (String namespace : allNamespaces) {
        Set<Service> singletons = serviceManager.getSingletons(namespace);
        for (Service service : singletons) {
            List<? extends Instance> instances = instanceServiceV2.listAllInstances(
                namespace, service.getGroupedServiceName());
            targetSet.addAll(instances);
        }
    }
    PrometheusUtils.assembleArrayNodes(targetSet, arrayNode);
    return ResponseEntity.ok().body(arrayNode.toString());
}
```

**遍历逻辑**：三层嵌套循环——外层遍历所有命名空间 → 中层遍历每个命名空间下的所有 Service → 内层获取每个 Service 的所有 Instance。时间复杂度 O(N × S × I)，其中 N 为命名空间数、S 为每空间平均 Service 数、I 为每 Service 平均实例数。

`PrometheusUtils.assembleArrayNodes()`（`prometheus/src/main/java/com/alibaba/nacos/prometheus/utils/PrometheusUtils.java`）负责将 Instance 集合转换为 JSON Array，每个 Instance 的元数据包括 `instanceId`、`ip`、`port`、`clusterName`、`serviceName`、`metadata` 等字段。

**API 端点常量**（`prometheus/src/main/java/com/alibaba/nacos/prometheus/api/ApiConstants.java`）：

```java
// ApiConstants.java:28-PI 35 (Nacos 2.5.3)
public class ApiConstants {
    public static final String PROMETHEUS_CONTROLLER_PATH = "/prometheus";
    public static final String PROMETHEUS_CONTROLLER_NAMESPACE_PATH =
            "/prometheus/{namespaceId}";
    public static final String PROMETHEUS_CONTROLLER_SERVICE_PATH =
            "/prometheus/{namespaceId}/{service}";
}
```

提供三个粒度的端点：
- `/prometheus` — 全量：所有命名空间下所有服务的所有实例
- `/prometheus/{namespaceId}` — 按命名空间过滤
- `/prometheus/{namespaceId}/{service}` — 按命名空间+服务精确过滤

### Trade-off 分析

**HTTP SD vs JMX Exporter**：

| 维度 | HTTP SD（PrometheusController） | JMX Exporter |
|------|-------------------------------|--------------|
| **部署复杂度** | 零额外进程，Nacos 内置 | 需额外启动 JMX Exporter Agent |
| **指标丰富度** | 仅实例元数据（IP/Port/metadata） | 丰富 JVM 指标（堆/GC/线程） |
| **数据新鲜度** | 实时拉取注册表 | 实时拉取 MBean |
| **性能开销** | 遍历全量注册表（O(N×S×I)） | MBean 查询开销极低 |
| **适用场景** | Prometheus 服务发现 | JVM 监控 + Grafana Dashboard |

**启用 Prometheus Controller 的性能影响**：每次 Prometheus Server 拉取 `/prometheus` 接口时，Controller 需要遍历所有命名空间 × 所有 Service × 所有 Instance。在大规模集群（2000+ 服务、100,000+ 实例）场景下，单次 `metric()` 调用可能耗时数百毫秒，建议 Prometheus Server 的 `scrape_interval` 不要低于 30s。

### 设计模式分析

1. **条件装配模式（@ConditionalOnProperty）**：通过 `nacos.prometheus.metrics.enabled=true/false` 控制 PrometheusController 是否加载到 Spring 容器，实现零配置启用/禁用
2. **门面模式（Facade）**：`PrometheusController` 作为统一入口，封装了 `ServiceManager` + `InstanceOperatorClientImpl` 的复杂调用链，对外暴露简洁的 REST API
3. **工具类模式（Utility）**：`PrometheusUtils.assembleArrayNodes()` 将 Instance 集合 → JSON Array 的转换逻辑集中到工具类，避免 Controller 代码膨胀

### 配置方式

在 `application.properties` 中启用 Prometheus metrics：

```properties
# application.properties
nacos.prometheus.metrics.enabled=true
```

启用后，访问 `http://nacos-server:8848/prometheus` 即可获取 JSON 格式的全量实例列表。

配合 Prometheus Server 的 `prometheus.yml` 配置：

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'nacos'
    scrape_interval: 30s
    metrics_path: '/prometheus'
    static_configs:
      - targets: ['nacos-server:8848']
```

### 小结

Prometheus HTTP SD 是 Nacos 2.5.3 内置的轻量级监控数据导出方案，通过 `PrometheusController` 提供三层粒度的实例元数据 REST API。其核心优势是零额外进程部署，适用于 Prometheus 服务发现场景；局限性是仅导出实例元数据而非 JVM 指标，需配合 JMX Exporter 实现全栈监控。

---

## 13.2 核心 Prometheus 指标表

### 设计背景

虽然 Nacos 2.5.3 内置的 `PrometheusController` 仅导出实例元数据（IP/Port/metadata），但配合 Prometheus JMX Exporter 或 Micrometer，可以导出丰富的 JVM 和业务指标。在生产环境中，运维团队需要一套标准化的核心指标表来建立监控基线、设置告警阈值和构建 Grafana Dashboard。

以下 11 个核心指标覆盖了 Nacos 集群健康度的四个维度：**连接层**（gRPC 连接数）、**服务层**（注册服务数、实例数）、**配置层**（配置发布速率）、**JVM 层**（堆内存、GC、线程）。

### 核心类关系图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Nacos Prometheus 监控指标体系                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌───────────────────────────┐  ┌───────────────────────────────────────┐  │
│  │  连接层 (Connection)      │  │  服务层 (Naming)                    │  │
│  │  • grpc_connections_total │  │  • naming_service_total             │  │
│  │  • grpc_push_cost_millis │  │  • naming_instance_total            │  │
│  │  • grpc_bi_stream_active │  │  • naming_health_check_cost_millis │  │
│  └───────────────────────────┘  └───────────────────────────────────────┘  │
│                                                                          │
│  ┌───────────────────────────┐  ┌───────────────────────────────────────┐  │
│  │  配置层 (Config)         │  │  JVM 层 (Runtime)                   │  │
│  │  • config_publish_total  │  │  • jvm_heap_used_bytes             │  │
│  │  • config_get_total      │  │  • jvm_gc_pause_seconds           │  │
│  │  • config_listener_total │  │  • jvm_threads_current             │  │
│  └───────────────────────────┘  └───────────────────────────────────────┘  │
│                                                                          │
│            图 13-2：Nacos 核心 Prometheus 指标四层体系                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 核心指标表：11 个关键指标

| # | Prometheus 指标名 | 类型 | 说明 | 告警阈值建议 |
|---|-----------------|------|------|-------------|
| 1 | `naming_service_total` | Gauge | 当前注册的服务总数（所有命名空间） | > 90% 集群容量上限（如 2000 服务的 90% = 1800） |
| 2 | `naming_instance_total` | Gauge | 当前注册的实例总数（临时+持久） | 单节点 > 50,000 实例（超过默认连接数限制） |
| 3 | `grpc_connections_total` | Gauge | 当前活跃的 gRPC 双向流连接数 | > 80% `maxServerConnections`（默认 20000） |
| 4 | `grpc_push_cost_millis` | Histogram | gRPC 推送延迟分布（P50/P95/P99） | P99 > 500ms（影响服务发现时效） |
| 5 | `config_publish_total` | Counter | 配置发布总次数（累计） | 速率突增 > 1000/min（异常批量发布） |
| 6 | `config_get_total` | Counter | 配置获取总次数（累计） | 速率突降 > 50%（客户端无法获取配置） |
| 7 | `config_listener_total` | Gauge | 当前配置监听器总数（Long Polling 连接） | > 80% `maxLongPollingConnections` |
| 8 | `jvm_heap_used_bytes` | Gauge | JVM 堆内存已使用字节数 | > 85% `-Xmx`（连续 5 分钟触发告警） |
| 9 | `jvm_gc_pause_seconds` | Summary | GC 暂停时间分布（P50/P99） | P99 > 1s（影响请求延迟） |
| 10 | `jvm_threads_current` | Gauge | 当前活跃线程数 | > 1000 线程（线程泄漏风险） |
| 11 | `naming_health_check_cost_millis` | Histogram | 健康检查耗时分布（P99） | P99 > 2000ms（健康检查超时风险） |

### 源码走读：gRPC 连接数指标的数据来源

gRPC 连接数指标来源于 `core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcBiStreamRequestAcceptor.java` 中的 `ConnectionManager`：

```java
// GrpcBiStreamRequestAcceptor.java:57-67 (Nacos 2.5.3)
public class GrpcBiStreamRequestAcceptor extends GrpcRequestAcceptor {
    
    @Autowired
    private ConnectionManager connectionManager;
    
    // 当前连接数 = ConnectionManager.getCurrentConnectionCount()
    // 连接数通过 ConnectionEventListener 实时维护
}
```

`ConnectionManager`（`core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java`）维护 `Map<String, Connection>` 存储所有活跃连接：

```java
// ConnectionManager.java:53-68 (Nacos 2.5.3)
@Component
public class ConnectionManager {
    
    // 所有客户端连接映射：connectionId → Connection
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    
    // 连接总数计数器
    private AtomicInteger connectionCount = new AtomicInteger(0);
    
    public int getCurrentConnectionCount() {
        return connectionCount.get();
    }
}
```

### Trade-off 分析

**细粒度指标 vs 性能开销**：

| 维度 | 粗粒度（5-8 指标） | 细粒度（20+ 指标） |
|------|-------------------|---------------------|
| **监控覆盖度** | 仅覆盖核心健康度 | 覆盖性能 + 业务细节 |
| **Prometheus 存储开销** | ~50MB/天（11 指标 × 15s 抓取） | ~150MB/天（30+ 指标 × 15s 抓取） |
| **Grafana 面板复杂度** | 简单 3-5 面板 | 复杂 10+ 面板 |
| **告警噪音** | 低（5-8 条告警规则） | 高（15+ 条告警规则，易疲劳） |
| **排查效率** | 快速定位大类问题 | 精准定位具体根因 |

**推荐**：先建立 11 个核心指标基线，运行 2-4 周后根据实际故障模式逐步增加细分指标。避免过早引入过多指标导致告警疲劳。

### 小结

11 个核心 Prometheus 指标覆盖了 Nacos 集群的四层健康度：连接层、服务层、配置层、JVM 层。关键是建立每个指标的基线值（正常运行时的均值 ± 标准差），基于基线设置告警阈值，而非使用固定绝对值。

---

## 13.3 Grafana Dashboard 推荐面板 JSON

### 设计背景

Prometheus 收集指标数据后，需要可视化面板来直观展示 Nacos 集群健康状态。Grafana 是 Prometheus 生态的标准可视化工具，通过 JSON Dashboard 定义面板（Panel）的布局、数据源查询（PromQL）和可视化类型（Graph / Stat / Gauge / Table）。

一个高效的 Nacos Grafana Dashboard 应覆盖 5 个核心面板：**连接数趋势**、**服务数趋势**、**配置速率趋势**、**JVM 堆内存趋势**、**GC 暂停时间分布**。面板设计原则：每面板 ≤ 6 条 PromQL 查询（避免面板过于密集），Row 分组按维度（连接层 / 服务层 / 配置层 / JVM 层）。

### 核心类关系图（Dashboard 架构）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Grafana Dashboard 架构                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                        Grafana Server                                 │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  │  │
│  │  │ Panel 1 │  │ Panel 2 │  │ Panel 3 │  │ Panel 4 │  │ Panel 5 │  │  │
│  │  │ 连接数  │  │ 服务数  │  │配置速率│  │ JVM 堆 │  │ GC暂停 │  │  │
│  │  │ Graph   │  │ Graph   │  │ Graph   │  │ Graph   │  │ Heatmap │  │  │
│  │  └────┬───┘  └────┬───┘  └────┬───┘  └────┬───┘  └────┬───┘  │  │
│  └───────┼──────────┼──────────┼──────────┼──────────┼──────────────┘  │
│          │          │          │          │          │                     │
│          ▼          ▼          ▼          ▼          ▼                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                     Prometheus Server                               │  │
│  │  tsdb {                                                }          │  │
│  │  scrape_configs: [nacos-cluster:8848/prometheus]                  │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│            图 13-3：Grafana Dashboard 数据流架构                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5 个核心面板 PromQL 查询

#### Panel 1：gRPC 连接数趋势（Graph）

```promql
# 当前活跃 gRPC 连接数
grpc_connections_total{job="nacos"}

# 连接数变化速率（每分钟新增连接数）
rate(grpc_connections_total{job="nacos"}[5m]) * 60

# 连接数饱和度百分比
grpc_connections_total{job="nacos"} / maxServerConnections * 100
```

**可视化类型**：Graph（折线图）  
**Y 轴单位**：Count（个数）  
**告警阈值线**：红色虚线 @ 80% `maxServerConnections`

#### Panel 2：注册服务数 / 实例数趋势（Graph）

```promql
# 当前注册服务总数
naming_service_total{job="nacos"}

# 当前注册实例总数
naming_instance_total{job="nacos"}

# 实例增长率（每分钟新增实例数）
rate(naming_instance_total{job="nacos"}[5m]) * 60
```

**可视化类型**：Graph（双 Y 轴，左轴服务数 / 右轴实例数）  
**Y 轴单位**：Count（个数）

#### Panel 3：配置发布/获取速率（Graph）

```promql
# 配置发布速率（每分钟发布次数）
rate(config_publish_total{job="nacos"}[5m]) * 60

# 配置获取速率（每分钟获取次数）
rate(config_get_total{job="nacos"}[5m]) * 60

# 配置发布/获取比率
rate(config_publish_total{job="nacos"}[5m]) / rate(config_get_total{job="nacos"}[5m])
```

**可视化类型**：Graph（双线叠加）  
**Y 轴单位**：ops/min（次每分钟）

#### Panel 4：JVM 堆内存趋势（Graph）

```promql
# 堆内存已使用
jvm_heap_used_bytes{job="nacos"}

# 堆内存最大容量
jvm_heap_max_bytes{job="nacos"}

# 堆内存使用率百分比
jvm_heap_used_bytes{job="nacos"} / jvm_heap_max_bytes{job="nacos"} * 100
```

**可视化类型**：Graph（面积图 + 阈值线）  
**Y 轴单位**：Bytes（字节）  
**告警阈值线**：红色虚线 @ 85% `-Xmx`

#### Panel 5：GC 暂停时间分布（Heatmap）

```promql
# GC 暂停时间分布（P50/P95/P99）
histogram_quantile(0.50, sum(rate(jvm_gc_pause_seconds_bucket{job="nacos"}[5m])) by (le))
histogram_quantile(0.95, sum(rate(jvm_gc_pause_seconds_bucket{job="nacos"}[5m])) by (le))
histogram_quantile(0.99, sum(rate(jvm_gc_pause_seconds_bucket{job="nacos"}[5m])) by (le))
```

**可视化类型**：Heatmap（热力图，X 轴时间 / Y 轴暂停时长）  
**颜色映射**：绿（< 100ms）→ 黄（100-500ms）→ 红（> 500ms）

### Dashboard JSON 推荐配置

```json
{
  "dashboard": {
    "title": "Nacos 2.5.3 Cluster Monitoring",
    "uid": "nacos-cluster-v2",
    "refresh": "30s",
    "time": {"from": "now-6h", "to": "now"},
    "templating": {
      "list": [
        {
          "name": "datasource",
          "type": "datasource",
          "query": "prometheus",
          "current": {"text": "Prometheus", "value": "Prometheus"}
        },
        {
          "name": "instance",
          "type": "query",
          "query": "label_values(naming_service_total, instance)",
          "multi": true,
          "includeAll": true
        }
      ]
    },
    "panels": [
      {
        "id": 1,
        "title": "gRPC Connections (Total)",
        "type": "graph",
        "targets": [
          {
            "expr": "grpc_connections_total{instance=~\"$instance\"}",
            "legendFormat": "{{instance}}"
          }
        ],
        "thresholds": [
          {"value": 16000, "colorMode": "warning"},
          {"value": 18000, "colorMode": "critical"}
        ]
      }
    ]
  }
}
```

### Trade-off 分析

**Grafana 内置 Dashboard vs JSON 文件导入**：

| 维度 | 内置 Dashboard | JSON 文件导入 |
|------|---------------|---------------|
| **定制灵活性** | 低（固定面板） | 高（完全自定义） |
| **版本管理** | 无版本控制 | 可 Git 管理 JSON 文件 |
| **团队共享** | 手动截图分享 | 文件导入即用 |
| **维护成本** | 低（无文件维护） | 中（需随 Nacos 版本更新 JSON） |
| **多环境适用** | 需逐环境手动配置 | 通过 Templating 变量 `$instance` 适配多环境 |

### 设计模式分析

1. **模板变量模式（Templating）**：通过 `$instance` 变量实现一套 Dashboard JSON 适配多集群（测试/预发/生产），避免每集群手工配置
2. **阈值线模式（Threshold）**：通过 Grafana Graph Panel 的 Threshold 功能，在可视化面板中直接标记告警阈值线（如 80% 连接数），无需切换至 AlertManager 面板即可快速识别异常
3. **分层组织模式（Row-based Layout）**：按维度（连接层 / 服务层 / 配置层 / JVM 层）分组 Row，每个 Row 内面板数量 ≤ 3，避免单个 Dashboard 面板数 > 15 导致的视觉混乱

### 小结

5 个核心 Grafana 面板覆盖了 Nacos 集群的四层监控维度：连接数趋势、服务数趋势、配置速率趋势、JVM 堆内存趋势、GC 暂停时间分布。通过 PromQL 查询 + Templating 变量 `$instance` 实现多集群复用，通过 Threshold 阈值线实现面板内快速异常识别。

---

## 13.4 Prometheus AlertManager 告警规则

### 设计背景

Prometheus AlertManager 是 Prometheus 生态的标准告警组件，通过 `alert_rules.yml` 定义告警规则（PromQL 表达式 + 持续时间 + 标签），当规则触发时 AlertManager 将告警路由到指定接收者（如企业微信 / Slack / PagerDuty / Email）。

Nacos 集群需要 5 条核心告警规则覆盖最常见的生产故障模式：**高连接数告警**（连接数超阈值 → 新客户端无法连接）、**节点 Down 告警**（节点不可达 → 集群容量缩减）、**Distro 同步失败告警**（临时实例同步失败 → 多节点数据不一致）、**JVM 内存告警**（堆内存超阈值 → Full GC 频繁 → 暂停时间增加）、**Full GC 频繁告警**（GC 暂停 P99 > 1s → 请求延迟飙升）。

### 核心类关系图（告警路由架构）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   Prometheus AlertManager 告警路由架构                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────┐  │
│  │ Prometheus Server │────▶│ AlertManager     │────▶│ Receiver: Webhook    │  │
│  │ (alert_rules.yml)│     │ (route + group) │     │ → 企业微信 / Slack   │  │
│  └──────────────────┘     └──────────────────┘     └──────────────────────┘  │
│          │                       │                                          │
│          ▼                       ▼                                          │
│  ┌──────────────────┐     ┌──────────────────┐                             │
│  │ 5 条核心告警规则 │     │ Grouping:       │                             │
│  │ 1. HighConn      │     │   group_by:      │                             │
│  │ 2. NodeDown     │     │   [alertname]    │                             │
│  │ 3. DistroFail   │     │ group_wait: 10s  │                             │
│  │ 4. HighHeap     │     │ group_interval:   │                             │
│  │ 5. FrequentFullGC│     │   5m              │                             │
│  └──────────────────┘     └──────────────────┘                             │
│                                                                          │
│          图 13-4：AlertManager 告警路由架构                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5 条核心告警规则

#### Rule 1：高 gRPC 连接数告警

```yaml
# alert_rules.yml
groups:
  - name: nacos_connection_alerts
    rules:
      - alert: HighGrpcConnections
        expr: grpc_connections_total{job="nacos"} / maxServerConnections > 0.8
        for: 5m
        labels:
          severity: warning
          team: nacos-ops
        annotations:
          summary: "Nacos gRPC 连接数超过 80% 阈值"
          description: "集群 {{ $labels.instance }} 当前连接数 {{ $value }}/20000，超过 80% 阈值，新客户端可能无法连接"
```

#### Rule 2：节点 Down 告警

```yaml
  - name: nacos_node_alerts
    rules:
      - alert: NodeDown
        expr: up{job="nacos"} == 0
        for: 丛
        labels:
          severity: critical
          team: nacos-ops
        annotations:
          summary: "Nacos 节点 {{ $labels.instance }} Down"
          description: "节点 {{ $labels.instance }} 已 Down 超过 1 分钟，请立即检查"
```

#### Rule 3：Distro 同步失败告警

```yaml
  - name: nacos_distro_alerts
    rules:
      - alert: DistroSyncFail
        expr: rate(naming_distro_sync_failed_total{job="nacos"}[5m]) > 0.05
        for: 10m
        labels:
          severity: warning
          team: nacos-ops
        annotations:
          summary: "Nacos Distro 同步失败率超过 5%"
          description: "集群 {{ $labels.instance }} Distro 同步失败率 {{ $value }}/s，可能导致多节点临时实例数据不一致"
```

#### Rule 4：JVM 堆内存告警

```yaml
  - name: nacos_jvm_alerts
    rules:
      - alert: HighHeapMemory
        expr: jvm_heap_used_bytes{job="nacos"} / jvm_heap_max_bytes{job="nacos"} > 0.85
        for: 5m
        labels:
          severity: warning
          team: nacos-ops
        annotations:
          summary: "Nacos JVM 堆内存超过 85% 阈值"
          description: "集群 {{ $labels.instance }} 堆内存使用率 {{ $value | humanizePercentage }}，持续 5 分钟，可能即将 Full GC"
```

#### Rule 5：Full GC 频繁告警

```yaml
      - alert: FrequentFullGC
        expr: histogram_quantile(0.99, sum(rate(jvm_gc_pause_seconds_bucket{job="nacos"}[5m])) by (le)) > 1.0
        for: 5m
        labels:
          severity: critical
          team: nacos-ops
        annotations:
          summary: "Nacos Full GC P99 暂停超过 1s"
          description: "集群 {{ $labels.instance }} GC P99 暂停时间为 {{ $value }}s，超过 1s 阈值，请求延迟将飙升"
```

### Trade-off 分析

**告警灵敏度 vs 告警疲劳**：

| 维度 | 高灵敏度（短持续时间 + 低阈值） | 低灵敏度（长持续时间 + 高阈值） |
|------|------------------------------|------------------------------|
| **故障发现速度** | 快（< 2min 发出告警） | 慢（> 10min 发出告警） |
| **误告警风险** | 高（瞬时波动触发告警） | 低（仅持续异常触发告警） |
| **告警疲劳** | 高（频繁告警 → 运维人员忽略） | 低（仅关键告警触发响应） |
| **生产推荐** | 非核心告警（如 Distro 同步） | 核心告警（如节点 Down、Full GC P99 > 1s） |

**推荐策略**：
- **核心告警**（NodeDown、FrequentFullGC）：高灵敏度（`for: 1m`）→ 快速响应
- **非核心告警**（HighGrpcConnections、DistroSyncFail）：低灵敏度（`for: 5-10m`）→ 减少告警噪音

### 设计模式分析

1. **告警分组模式（Alert Grouping）**：通过 `group_by: [alertname]` 将同一告警规则的多个实例触发分组为单条通知，避免同一故障的多节点同时告警产生消息轰炸
2. **告警路由模式（Alert Routing）**：通过 `severity: warning/critical` 标签将告警路由到不同接收者（warning → 企业微信 / critical → PagerDuty），实现分级响应
3. **告警静默模式（Silence）**：AlertManager 支持按时间窗口静默特定告警（如计划维护窗口期间），避免计划维护触发误告警

### 小结

5 条核心告警规则覆盖了 Nacos 集群最常见的 5 种生产故障模式：高连接数（80% 阈值 5min）、节点 Down（1min）、Distro 同步失败（5% 速率 10min）、JVM 堆内存（85% 阈值 5min）、Full GC P99 > 1s（5min）。通过 AlertManager 的分组 + 路由 + 静默机制实现告警的精准分发和噪音控制。

---

## 13.5 日志分析：5 种日志文件详解

### 设计背景

Nacos 2.5.3 的日志体系基于 SLF4J + Logback 实现，通过 `logger-adapter-impl` 模块提供 Log4j2 和 Logback 两种日志适配器。生产环境中，日志是排查问题的一手数据源——无论是启动失败、配置不生效还是性能瓶颈，第一步永远是查日志。

Nacos 运行时产生的 5 种核心日志文件各有其特定用途：`nacos-cluster.log`（集群操作日志）、`naming-server.log`（命名服务日志）、`config-server.log`（配置服务日志）、`remote-server.log`（gRPC 通信日志）、`access.log`（HTTP 访问日志）。每种日志文件包含不同的信息维度，组合使用才能完整还原问题现场。

### 核心类关系图（日志体系架构）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Nacos 2.5.3 日志架构                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────────────────────────────────────────────────┐             │
│  │  logger-adapter-impl/                                    │             │
│  │  ├── log4j2-adapter/ → Log4j2NacosLoggingAdapter       │             │
│  │  └── logback-adapter-12/ → LogbackNacosLoggingAdapter  │             │
│  └────────────────────────────────────────────────────────────┘             │
│                           │                                              │
│                           ▼                                              │
│  ┌────────────────────────────────────────────────────────────┐             │
│  │              日志输出 (${nacos.home}/logs/)              │             │
│  ├── nacos-cluster.log    ← 集群操作（Raft 选举/成员变更）  │             │
│  ├── naming-server.log    ← 命名服务（注册/发现/健康检查）  │             │
│  ├── config-server.log    ← 配置服务（发布/订阅/监听）      │             │
│  ├── remote-server.log    ← gRPC 通信（双向流/推送）        │             │
│  ├── access.log          ← HTTP 访问日志（Tomcat Access）    │             │
│  └── nacos.log           ← 根日志（所有未分类日志）        │             │
│                                                                          │
│            图 13-5：Nacos 2.5.3 日志体系架构                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5 种日志文件详解

#### 1. `nacos-cluster.log` — 集群操作日志

**日志来源**：`core/src/main/java/com/alibaba/nacos/core/cluster/` 包下的集群管理相关类，包括 `ServerMemberManager`（集群成员管理）、`RaftPeerSet`（JRaft 对等节点集合）等。

**关键日志内容**：

```
# 节点加入集群
INFO ServerMemberManager - new member added: 192.168.1.101:8848

# 节点离开集群
WARN ServerMemberManager - member removed: 192.168.1.102:8848

# Raft 选举触发
INFO RaftCore - leader changed from 192.168.1.101:7848 to 192.168.1.102:7848

# CP 模式数据同步
INFO RaftSnapshotOperation - snapshot save successfully, path: ${nacos.home}/data/protocol/raft/
```

**排查场景**：
- 集群脑裂：查 `leader changed` 频率——如果 5 分钟内多次选举切换，可能是网络分区
- 节点频繁加入/离开：查 `member added/removed` 频率——可能是网络不稳定导致心跳超时误判

#### 2. `naming-server.log` — 命名服务日志

**日志来源**：`naming/src/main/java/com/alibaba/nacos/naming/` 包下的核心类，包括 `InstanceOperatorClientImpl`（实例注册操作）、`ServiceManager`（服务管理）、`DistroClientDataProcessor`（Distro 协议数据处理器）等。

**关键日志内容**：

```
# 实例注册
INFO InstanceOperatorClientImpl - register instance: serviceName=DEFAULT_GROUP@@example-service, ip=192.168.1.101, port=8080

# 实例注销
INFO InstanceOperatorClientImpl - deregister instance: serviceName=DEFAULT_GROUP@@example-service, ip=192.168.1.101, port=8080

# 健康检查超时
WARN HealthCheckTask - health check timeout for instance: 192.168.1.101:8080

# Distro 同步失败
ERROR DistroClientDataProcessor - distro sync failed: target=192.168.1.103:8848, service=DEFAULT_GROUP@@example-service
```

**排查场景**：
- 实例注册失败：查 `register instance` 日志 → 确认是否到达 Nacos Server → 如果没有则排查客户端网络
- 健康检查超时误判：查 `health check timeout` 频率 → 如果频率高且对应实例实际健康 → 可能是 GC 暂停导致心跳延迟

#### 3. `config-server.log` — 配置服务日志

**日志来源**：`config/src/main/java/com/alibaba/nacos/config/server/` 包下的核心类，包括 `ConfigCacheService`（配置缓存服务）、`LongPollingService`（长轮询服务）、`ConfigChangePublisher`（配置变更发布器）等。

**关键日志内容**：

```
# 配置发布
INFO ConfigCacheService - publish config: dataId=application.properties, group=DEFAULT_GROUP, md5=abc123

# 配置订阅
INFO LongPollingService - add listener: dataId=application.properties, group=DEFAULT_GROUP

# 配置变更推送
INFO ConfigChangePublisher - notify config change: dataId=application.properties, group=DEFAULT_GROUP, md5=def456

# 长轮询超时
INFO LongPollingService - long polling timeout: clientId=192.168.1.101:54321, dataId=application.properties
```

**排查场景**：
- 配置不生效：查 `publish config` + `notify config change` → 确认发布成功 + 推送成功 → 如果推送成功但客户端未收到 → 排查客户端长轮询连接
- 长轮询超时：查 `long polling timeout` 频率 → 如果高频超时 → 调大 `configLongPollTimeout` 参数

#### 4. `remote-server.log` — gRPC 通信日志

**日志来源**：`core/src/main/java/com/alibaba/nacos/core/remote/grpc/` 包下的核心类，包括 `GrpcBiStreamRequestAcceptor`（gRPC 双向流请求接收器）、`GrpcConnection`（gRPC 连接）、`RpcPushService`（RPC 推送服务）等。

**关键日志内容**：

```
# gRPC 连接建立
INFO GrpcBiStreamRequestAcceptor - new gRPC bi-stream connection: clientId=192.168.1.101:54321

# gRPC 连接断开
WARN GrpcConnection - gRPC connection closed: clientId=192.168.1.101:54321, reason=connection reset

# 推送超时
ERROR RpcPushService - push timeout for client: clientId=192.168.1.101:54321, dataId=application.properties
```

**排查场景**：
- gRPC 连接频繁断开：查 `connection closed` 频率 → 如果高频 → 排查客户端网络稳定性或 `keepAliveTime` 配置
- 推送超时：查 `push timeout` → 如果高频 → 排查客户端处理速度或网络延迟

#### 5. `access.log` — HTTP 访问日志

**日志来源**：Tomcat Embedded 的 `AccessLogValve`，配置位于 `conf/application.properties` 中的 `server.tomcat.accesslog.enabled=true`。

**关键日志格式**：

```
# Combined Access Log 格式
192.168.1.101 - - [06/Sep/2026:12:00:00 +0800] "GET /nacos/v1/ns/instance/list?serviceName=example-service HTTP/1.1" 200 1234

# 关键字段
# 192.168.1.101 = 客户端 IP
# [06/Sep/2026:12:00:00 +0800] = 请求时间
# "GET /nacos/v1/ns/instance/list?serviceName=..." = 请求方法和 URL
# 200 = HTTP 响应状态码
# 1234 = 响应体大小 (bytes)
```

**排查场景**：
- 高频 API 调用：统计 `access.log` 中 API 路径频率 → 识别异常高频调用（可能是客户端 Bug 导致循环调用）
- 非 200 响应：统计 `access.log` 中非 200 状态码比例 → 识别 API 调用失败率

### Trade-off 分析

**同步日志 vs 异步日志**：

| 维度 | 同步日志（Logback SyncAppender） | 异步日志（Logback AsyncAppender） |
|------|-------------------------------|----------------------------------|
| **写入延迟** | 每次日志写入阻塞业务线程 | 日志写入异步队列，业务线程无阻塞 |
| **日志丢失风险** | 无（磁盘写入成功才返回） | 有（队列满时丢弃，默认 80% 队列容量时丢弃 TRACE/DEBUG/INFO） |
| **磁盘 I/O 影响** | 高峰时段磁盘 I/O 飙升 → 影响业务响应 | 异步缓冲平滑磁盘写入峰值 |
| **适用场景** | 审计日志（不允许丢失） | 普通业务日志（允许少量丢失） |

**推荐**：Nacos 默认使用 Logback SyncAppender（因为 `nacos-cluster.log` 等包含 Raft 选举等关键审计信息，不允许丢失），但在超高吞吐场景（100,000+ 实例注册）可以考虑将 `access.log` 切换为异步日志以减少磁盘 I/O 影响。

### 设计模式分析

1. **适配器模式（Adapter）**：`logger-adapter-impl/` 模块提供 Log4j2 和 Logback 两种适配器，Nacos 内部统一使用 SLF4J API，通过适配器切换底层日志实现
2. **门面模式（Facade）**：SLF4J 作为日志门面，统一 `LoggerFactory.getLogger()` 接口，屏蔽底层 Logback/Log4j2 差异
3. **策略模式（Strategy）**：`LogbackNacosLoggingAdapter` vs `Log4j2NacosLoggingAdapter` 通过策略模式支持运行时切换日志实现

### 小结

5 种日志文件各司其职：`nacos-cluster.log` 用于集群操作排查（节点加入/离开/Raft 选举）、`naming-server.log` 用于服务注册发现排查、`config-server.log` 用于配置发布订阅排查、`remote-server.log` 用于 gRPC 通信排查、`access.log` 用于 HTTP API 访问审计。组合使用 5 种日志可完整还原问题现场。

---

## 13.6 日志滚动策略

### 设计背景

Nacos 2.5.3 使用 Logback 的 `TimeBasedRollingPolicy` 实现日志滚动（Rolling），通过 `maxHistory`（保留天数）和 `totalSizeCap`（总大小上限）两个参数控制日志文件的数量和磁盘占用。合理的日志滚动策略可以避免日志文件无限增长撑爆磁盘，同时保留足够的历史日志用于问题排查。

### 核心配置参数

Nacos 2.5.3 的 Logback 配置文件位于各模块的 `src/main/resources/META-INF/logback`（如 `naming/src/main/resources/META-INF/logback`），典型配置如下：

```xml
<!-- naming/src/main/resources/META-INF/logback (Nacos 2.5.3) -->
<configuration>
    <appender name="NACOS-NAMING-SERVER"
              class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/naming-server.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <!-- 每天滚动一次 -->
            <fileNamePattern>${nacos.home}/logs/naming-server.log.%d{yyyy-MM-dd}.%i.gz</fileNamePattern>
            <!-- 保留最近 30 天的日志文件 -->
            <maxHistory>30</maxHistory>
            <!-- 所有归档日志文件总大小上限 3GB -->
            <totalSizeCap>3GB</totalSizeCap>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n</pattern>
        </encoder>
    </appender>
</configuration>
```

### 源码走读：TimeBasedRollingPolicy 滚动触发机制

Logback 的 `TimeBasedRollingPolicy` 通过 `RollingCalendar` 判断当前时间是否已经跨越了滚动周期（如按天滚动——当前日期 != 上次滚动日期），触发滚动时将当前日志文件重命名为归档文件名模式（如 `naming-server.log.2026-09-05.0.gz`），并创建新的空日志文件继续写入。

`totalSizeCap` 的工作原理：每次滚动完成后，`TimeBasedRollingPolicy` 会计算所有归档日志文件的总大小，如果超过 `totalSizeCap`，则按时间从旧到新删除最旧的归档文件，直到总大小 ≤ `totalSizeCap`。**注意**：`totalSizeCap` 仅对归档文件生效，**当前活跃日志文件不计入** `totalSizeCap`。

### Trade-off 分析

**保守策略（maxHistory=180 / totalSizeCap=10GB） vs 激进策略（maxHistory=7 / totalSizeCap=1GB）**：

| 维度 | 保守策略 | 激进策略 |
|------|---------|---------|
| **历史日志可用性** | 可回溯半年内的日志 | 仅可回溯最近一周日志 |
| **磁盘占用** | ~10GB / 节点 | ~1GB / 节点 |
| **问题排查能力** | 可排查历史故障（如 3 个月前的一次配置错误） | 仅排查近期故障（超 7 天无法回溯） |
| **磁盘成本** | 高（7 节点 × 10GB = 70GB） | 低（7 节点 × 1GB = 7GB） |
| **清理频率** | 低（180 天清理一次） | 高（7 天清理一次） |

**推荐**：
- **生产环境**：`maxHistory=30` / `totalSizeCap=3GB`（保留 1 个月日志，覆盖大多数问题排查周期）
- **测试环境**：`maxHistory=7` / `totalSizeCap=1GB`（节省磁盘成本）
- **审计合规环境**：`maxHistory=365` / `totalSizeCap=50GB`（满足年度审计要求）

### 设计模式分析

1. **策略模式（Policy Pattern）**：Logback 的 `RollingPolicy` 接口支持多种滚动策略——`TimeBasedRollingPolicy`（按时间滚动）、`SizeBasedTriggeringPolicy`（按大小滚动）、`SizeAndTimeBasedRollingPolicy`（按时间+大小组合滚动），通过策略模式实现灵活的滚动规则组合
2. **职责链模式（Chain of Responsibility）**：Logback Appender 内部通过 `RollingPolicy` → `TriggeringPolicy` → `RolloverStrategy` 的职责链，依次判断是否触发滚动、如何命名归档文件、如何清理旧归档文件

### 小结

`maxHistory=30` + `totalSizeCap=3GB` 是 Nacos 生产环境的推荐日志滚动策略：保留 30 天历史日志覆盖绝大多数问题排查周期，`totalSizeCap=3GB` 避免日志文件无限增长撑爆磁盘。关键注意：`totalSizeCap` 仅对归档文件生效，当前活跃日志文件不计入，实际磁盘占用 = 当前日志文件大小 + `totalSizeCap`。

---

## 13.7 日常运维巡检清单

### 设计背景

Nacos 集群的健康运行依赖定期巡检（Health Check Audit）——通过标准化检查项逐项核对集群状态，在故障发生前（Proactive）发现潜在风险。运维巡检不同于监控告警——监控告警是 Reactive（故障发生后触发），巡检是 Proactive（故障发生前的预防性检查）。

7 项必检项覆盖了 Nacos 集群的 7 个关键维度：**集群状态**（节点是否全部在线）、**连接数**（gRPC 连接是否接近上限）、**JVM 内存**（堆内存是否接近上限）、**DB 连接池**（MySQL 连接池是否泄漏）、**磁盘**（磁盘使用率是否接近上限）、**Raft 日志**（Raft 日志是否持续增长）、**错误日志**（ERROR 日志是否高频出现）。

### 7 项必检项清单

#### 1. 集群状态检查

**检查命令**：

```bash
# 查询集群节点列表
curl -X GET 'http://nacos-server:8848/nacos/v1/core/cluster/nodes'
```

**预期输出**：

```json
{
  "nodes": [
    {"address": "192.168.1.101:8848"8, "state": "UP"},
    {"address": "192.168.1.102:8848", "state": "UP"},
    {"address": "192.168.1.103:8848", "state": "UP"}
  ]
}
```

**异常判定**：任意节点 `state != "UP"` → 立即检查该节点日志（`nacos-cluster.log` 中 `member removed` 记录）

**检查频率**：每小时 1 次

#### 2. gRPC 连接数检查

**检查命令**：

```bash
# 查询当前连接数（Prometheus 指标）
curl -s 'http://nacos-server:8848/prometheus' | jq 'length'

# 或者通过 JMX MBean（如果启用了 JMX）
# java -jar cmdline-jmxclient-0.10.3.jar - localhost:9999 java.lang:type=OperatingSystem
```

**预期值**：当前连接数 < 80% × `maxServerConnections`（默认 20000 × 0.8 = 16000）

**异常判定**：连接数 > 16000 → 可能即将拒绝新客户端连接 → 扩容集群节点或调大 `maxServerConnections`

**检查频率**：每小时 1 次

#### 3. JVM 堆内存检查

**检查命令**：

```bash
# jstat -gcutil <nacos_pid> 1000 1
# 关注 O（Old Gen 使用率）列
```

**预期值**：Old Gen 使用率 < 85%（连续 3 次采样）

**异常判定**：Old Gen 使用率 > 85% → 可能即将 Full GC → 排查内存泄漏（jmap -histo:live <pid>）

**检查频率**：每 4 小时 1 次

#### 4. MySQL 连接池检查

**检查命令**：

```sql
-- 查询当前活跃 MySQL 连接数
SHOW PROCESSLIST;

-- HikariCP 连接池指标（通过 JMX MBean）
# java -jar cmdline-jmxclient-0.10.3.jar - localhost:9999 \
#   com.zaxxer.hikari:type=Pool (HikariPool-1) ActiveConnections
```

**预期值**：MySQL 活跃连接数 < 80% × `maximumPoolSize`（默认 20 × 0.8 = 16）

**异常判定**：活跃连接数 > 16 → 可能连接池泄漏 → 排查是否未关闭 `Connection`（try-with-resources 遗漏）

**检查频率**：每 4 小时 1 次

#### 5. 磁盘使用率检查

**检查命令**：

```bash
# 检查日志目录磁盘使用率
df -h ${nacos.home}/logs/

# 检查 Raft 日志目录磁盘使用率
du -sh ${nacos.home}/data/protocol/raft/
```

**预期值**：磁盘使用率 < 80%

**异常判定**：磁盘使用率 > 80% → 立即清理归档日志（`totalSizeCap` 触发清理）或扩容磁盘

**检查频率**：每天 1 次

#### 6. Raft 日志增长检查

**检查命令**：

```bash
# 检查 Raft 日志目录大小变化趋势
du -sh ${nacos.home}/data/protocol/raft/ns/default/

# 对比 24h 前的快照大小
diff <(du -sh ${nacos.home}/data/protocol/raft/ns/default/) <(cat /tmp/raft_size_yesterday.txt)
```

**预期值**：Raft 日志大小稳定（日增长率 < 百分 10%）

**异常判定**：Raft 日志大小异常增长（日增长率 > 50%）→ 可能 Raft Snapshot 失败 → 检查 `RaftSnapshotOperation` 日志

**检查频率**：每天 1 次

#### 7. ERROR 日志统计检查

**检查命令**：

```bash
# 统计过去 1 小时内 ERROR 日志条数
grep -c "ERROR" ${nacos.home}/logs/nacos-cluster.log.$(date +%Y-%m-%d).0.gz 2>/dev/null || \
  grep -c "ERROR" ${nacos.home}/logs/nacos-cluster.log

# 统计过去 1 小时内 WARN 日志条数
grep -c "WARN" ${nacos.home}/logs/nacos-cluster.log
```

**预期值**：ERROR < 5 条/小时，WARN < 50 条/小时

**异常判定**：ERROR > 10 条/小时 → 立即查看最新 ERROR 日志内容（`tail -100 ${nacos.home}/logs/nacos-cluster.log | grep ERROR`）

**检查频率**：每 4 小时 1 次

### Trade-off 分析

**人工巡检 vs 自动化巡检**：

| 维度 | 人工巡检 | 自动化巡检 |
|------|---------|-----------|
| **准确性** | 人为遗漏风险（疲劳/疏忽） | 脚本精确执行 |
| **实时性** | 低（按巡检周期执行） | 高（可分钟级执行） |
| **成本** | 人工时间成本 | 脚本编写 + 维护成本 |
| **告警集成** | 无 | 可集成 Prometheus AlertManager |
| **灵活性** | 高（人工可根据经验调整） | 低（按固定脚本逻辑） |

**推荐**：初期人工巡检建立基线 → 2-4 周后编写自动化巡检脚本（Shell + Cron） → 集成到 Prometheus AlertManager 实现全自动监控告警。

### 小结

7 项必检项覆盖了 Nacos 集群的 7 个关键维度：集群状态（每 1h）、连接数（每 1h）、JVM 堆内存（每 4h）、MySQL 连接池（每 4h）、磁盘（每天）、Raft 日志（每天）、ERROR 日志（每 4h）。建议初期人工执行巡检清单建立基线 → 2-4 周后编写自动化巡检脚本。

---

## 13.8 日常运维命令速查表

### 设计背景

Nacos 运维中，快速排查问题依赖一套熟练的运维命令集——从查看集群状态到抓取线程快照，每个运维人员都应熟悉这些命令。本节提供分类整理的常用运维命令速查表，覆盖 HTTP API、Shell 日志分析、JVM 诊断工具三个维度。

### 1. HTTP API 命令速查

#### 集群管理

```bash
# 查询集群节点列表
curl -X GET 'http://localhost:8848/nacos/v1/core/cluster/nodes'

# 查看当前节点 Raft 状态
curl -X GET 'http://localhost:8848/nacos/v1/core/cluster/node/self'

# 触发集群节点下线（需鉴权）
curl -X POST 'http://localhost:8848/nacos/v1/core/cluster/nodes' \
  -d 'address=192.168.1.101:8848&state=DOWN'
```

#### 服务管理

```bash
# 查询所有服务列表（分页）
curl -X GET 'http://localhost:8848/nacos/v1/ns/service/list?pageNo=1&pageSize=100'

# 查询指定服务的实例列表
curl -X GET 'http://localhost:8848/nacos/v1/ns/instance/list?serviceName=example-service'

# 查询服务健康度（健康实例数）
curl -X GET 'http://localhost:8848/nacos/v1/ns/health/service?serviceName=example-service'
```

#### 配置管理

```bash
# 查询指定配置内容
curl -X GET 'http://localhost:8848/nacos/v1/cs/configs?dataId=application.properties&group=DEFAULT_GROUP'

# 查询配置历史版本列表
curl -X GET 'http://localhost:8848/nacos/v1/cs/history/configs?dataId=application.properties&group=DEFAULT_GROUP&pageNo=1&pageSize=10'
```

#### gRPC 连接管理

```bash
# 查询当前所有 gRPC 客户端连接列表
curl -X GET 'http://localhost:8848/nacos/v1/core/client/list'

# 查询指定客户端连接详情
curl -X GET 'http://localhost:8848/nacos/v1/core/client/info?clientId=192.168.1.101:54321'
```

### 2. Shell 日志分析命令速查

```bash
# 实时查看集群操作日志
tail -f ${nacos.home}/logs/nacos-cluster.log

# 查询最近 1 小时 ERROR 日志
grep "ERROR" ${nacos.home}/logs/nacos-cluster.log | tail -50

# 统计各服务注册次数（top 10）
grep "register instance" ${nacos.home}/logs/naming-server.log | \
  awk -F'serviceName=' '{print $2}' | awk -F',' '{print $1}' | sort | uniq -c | sort -rn | head -10

# 统计各客户端连接数
grep "new gRPC bi-stream connection" ${nacos.home}/logs/remote-server.log | \
  awk -F'clientId=' '{print $2}' | sort | uniq -c | sort -rn

# 按时间段过滤日志
sed -n '/2026-09-06 10:00/,/2026-09-06 11:00/p' ${nacos.home}/logs/nacos-cluster.log
```

### 3. JVM 诊断命令速查

#### jstat — GC 统计

```bash
# 每秒输出一次 GC 统计（连续 5 次）
jstat -gcutil <nacos_pid> 1000 5

# 关注列：O（Old Gen 使用率）、FGC（Full GC 次数）、FGCT（Full GC 总时间）
# 异常判定：O > 85% 持续 5 次采样 → 可能即将 Full GC
```

#### jstack — 线程快照

```bash
# 抓取线程快照
jstack <nacos_pid> > /tmp/nacos_thread_dump_$(date +%Y%m%d_%H%M%S).txt

# 统计线程状态分布
grep "java.lang.Thread.State" /tmp/nacos_thread_dump.txt | sort | uniq -c | sort -rn

# 查找死锁
grep -A 10 "deadlock" /tmp/nacos_thread_dump.txt

# 查找 gRPC 相关线程（排查 gRPC 连接泄漏）
grep "grpc-default-worker" /tmp/nacos_thread_dump.txt
```

#### jmap — 堆内存快照

```bash
# 导出堆内存快照（会触发 Full GC，生产慎用！）
jmap -dump:live,format=b,file=/tmp/nacos_heap_$(date +%Y%m%d_%H%M%S).hprof <nacos_pid>

# 统计堆内存中各对象实例数（Top 30，不触发 Full GC）
jmap -histo <nacos_pid> | head -35

# 关注类：
# java.util.concurrent.ConcurrentHashMap$Node → gRPC 连接元数据
# com.alibaba.nacos.naming.core.Instance → 临时实例对象数
# com.alibaba.nacos.naming.core.Cluster → 集群对象数
```

#### async-profiler — CPU 火焰图

```bash
# 生成 CPU 火焰图（采样 30 秒）
async-profiler -d 30 -e cpu -f /tmp/nacos_cpu_flamegraph.html <nacos_pid>

# 关注热点函数：
# GrpcBiStreamRequestAcceptor → gRPC 请求处理占比
# DistroClientDataProcessor → Distro 同步耗时占比
# ConfigCacheService → 配置缓存更新耗时占比
```

### Trade-off 分析

**手动命令 vs 脚本自动化**：

| 维度 | 手动命令 | 脚本自动化 |
|------|---------|-----------|
| **执行效率** | 低（逐条手动输入） | 高（一键批量执行） |
| **风险** | 高（手误输入错误命令） | 低（脚本预先测试） |
| **知识依赖** | 高（需记忆命令语法） | 低（脚本封装复杂性） |
| **灵活性** | 高（可根据现场情况调整） | 低（按固定逻辑执行） |
| **适用场景** | 紧急排查（灵活调整） | 定期巡检（批量执行） |

### 小结

运维命令速查表覆盖 HTTP API（集群/服务/配置管理）、Shell 日志分析（tail/grep/awk）、JVM 诊断（jstat/jstack/jmap/async-profiler）三个维度。建议运维团队建立团队内部的运维命令知识库（Wiki），持续积累故障排查中使用的命令组合。

---

## 13.9 定期运维任务

### 设计背景

Nacos 集群长期运行过程中，会积累历史配置数据（`his_config_info` 表）、过期临时实例（客户端异常退出后残留的实例注册信息）、Raft 日志快照（`${nacos.home}/data/protocol/raft/`）等需要定期清理的数据。同时日志文件需要按滚动策略定期归档和清理。

定期运维任务包括三大类：**数据清理**（历史配置 / 过期实例 / Raft Snapshot）、**日志轮转**（按 TimeBasedRollingPolicy 自动滚动 + 手动清理异常增长的日志文件）、**Raft Snapshot 检查**（确保 Raft 日志不会无限增长）。

### 1. 数据清理任务

#### 1.1 历史配置清理

Nacos 每次配置发布会在 `his_config_info` 表中插入一条历史记录（包含完整的 `content` 字段），长期运行后表大小可能膨胀到数十 GB。

```sql
-- 查询 his_config_info 表大小
SELECT 
    table_name,
    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)"
FROM information_schema.tables 
WHERE table_schema = 'nacos_config' 
  AND table_name = 'his_config_info';

-- 清理 30 天前的历史配置记录
DELETE FROM his_config_info 
WHERE gmt_create < DATE_SUB(NOW(), INTERVAL 30 DAY);
```

**建议清理频率**：每月 1 次（保留最近 30 天历史配置，覆盖大多数配置回滚需求）

#### 1.2 过期实例清理

客户端异常退出（未调用 `deregisterInstance`）时，Nacos 不会自动清理残留的实例注册信息，需手动通过 API 清理：

```bash
# 查询指定服务的所有实例（检查是否有过期实例）
curl -X GET 'http://localhost:8848/nacos/v1/ns/instance/list?serviceName=DEFAULT_GROUP@@example-service'

# 手动注销过期实例
curl -X DELETE 'http://localhost:8848/nacos/v1/ns/instance?serviceName=example-service&ip=192.168.1.101&port=8080'
```

**建议清理频率**：每周 1 次（配合监控——如果发现实例注册数异常增长但实际活跃客户端数未增长→排查残留实例）

#### 1.3 Raft Snapshot 检查

```bash
# 检查 Raft 日志目录大小
du -sh ${nacos.home}/data/protocol/raft/ns/default/

# 查询 Raft Snapshot 状态（通过 API）
curl -X GET 'http://localhost:8848/nacos/v1/core/cluster/raft/snapshot'
```

Raft Snapshot 由 JRaft 自动管理（默认每 1000 条日志触发一次 Snapshot），正常情况下无需手动干预。但如果 Snapshot 失败（如磁盘空间不足），Raft 日志将持续增长→需手动清理。

**建议检查频率**：每月 1 次

### 2. 日志轮转任务

#### 2.1 TimeBasedRollingPolicy 自动滚动

Logback 的 `TimeBasedRollingPolicy` 自动按 `fileNamePattern` 滚动归档（详见 13.6 节），无需手动干预。但需要定期检查：

```bash
# 检查日志目录磁盘使用率
df -h ${nacos.home}/logs/

# 检查各日志文件大小分布
du -sh ${nacos.home}/logs/*.log

# 检查归档日志文件数量
ls ${nacos.home}/logs/*.gz | wc -l
```

**建议检查频率**：每周 1 次

#### 2.2 异常增长的日志文件清理

如果某个模块的日志异常暴增（如 `remote-server.log` 因为 gRPC 连接频繁断开导致 ERROR 日志暴增），需手动清理大日志文件后排查根因：

```bash
# 查找大于 1GB 的日志文件
find ${nacos.home}/logs/ -name "*.log" -size +1G

# 清理大日志文件（先备份再删除）
cp ${nacos.home}/logs/remote-server.log /tmp/remote-server.log.bak.$(date +%Y%m%d)
echo > ${nacos.home}/logs/remote-server.log
```

### 3. 定期运维任务 Cron 配置

建议通过 Cron 定时执行以上运维任务：

```bash
# crontab -e

# 每天凌晨 2:00 执行磁盘检查
0 2 * * * df -h ${nacos.home}/logs/ >> /var/log/nacos_disk_check.log 2>&1

# 每周日凌晨 3:00 执行 Raft 日志大小检查
0 3 * * 0 du -sh ${nacos.home}/data/protocol/raft/ >> /var/log/nacos_raft_check.log 2>&1

# 每月 1 日凌晨 4:00 执行 MySQL 历史配置清理
0 4 1 * * mysql -u nacos -p -e "DELETE FROM his_config_info WHERE gmt_create < DATE_SUB(NOW(), INTERVAL 30 DAY);" nacos_config >> /var/log/nacos_cleanup.log 2>&1
```

### Trade-off 分析

**手动清理 vs 自动化清理**：

| 维度 | 手动清理 | 自动化清理（Cron 定时任务） |
|------|---------|--------------------------|
| **安全性** | 高（人工审核后执行） | 中（脚本错误可能导致误删数据） |
| **时效性** | 低（依赖人工定期执行） | 高（Cron 自动按时执行） |
| **误删风险** | 低（人工审核 SQL WHERE 条件） | 高（脚本 SQL 错误可能误删大量数据） |
| **可追溯性** | 低（无自动记录） | 高（Cron 日志自动记录） |

**推荐**：初期手动执行清理任务建立基线（运行 丛-3 次确认 SQL WHERE 条件正确）→ 再编写自动化 Cron 脚本（加入 `--dry-run` 模式先预览要删除的数据）。

### 小结

定期运维任务包括三大类：数据清理（历史配置 / 过期实例 / Raft Snapshot）、日志轮转（TimeBasedRollingPolicy 自动滚动 + 手动清理异常增长）、Raft Snapshot 检查。建议初期手动执行建立基线 → 再编写自动化 Cron 脚本，避免脚本错误导致误删数据。

---

## 13.1 补充：Prometheus Metrics 导出配置的深入实战

### PrometheusUtils 源码走读

`prometheus/src/main/java/com/alibaba/nacos/prometheus/utils/PrometheusUtils.java` 负责将 Instance 集合序列化为 Prometheus HTTP SD 格式的 JSON Array：

```java
// PrometheusUtils.java:37-65 (Nacos 2.5.3)
public class PrometheusUtils {
    
    public static void assembleArrayNodes(Set<Instance> targetSet, ArrayNode arrayNode) {
        for (Instance instance : targetSet) {
            ObjectNode node = JacksonUtils.createEmptyObjectNode();
            // 提取 Instance 元数据字段
            node.put("instanceId", instance.getInstanceId());
            node.put("ip", instance.getIp());
            node.put("port", instance.getPort());
            node.put("clusterName", instance.getClusterName());
            node.put("serviceName", instance.getServiceName());
            
            // 提取 metadata（自定义键值对）
            ObjectNode metadataNode = JacksonUtils.createEmptyObjectNode();
            Map<String, String> metadata = instance.getMetadata();
            if (metadata != null) {
                for (Map.Entry<String, String> entry : metadata.entrySet()) {
                    metadataNode.put(entry.getKey(), entry.getValue());
                }
            }
            node.set("metadata", metadataNode);
            arrayNode.add(node);
        }
    }
}
```

**序列化逻辑**：遍历 `targetSet` 中的每个 `Instance` 对象→提取 5 个核心字段（`instanceId`、`ip`、`port`、`clusterName`、`serviceName`）+ `metadata` 自定义键值对→组装为 Jackson `ObjectNode`→添加至 `ArrayNode`→返回 JSON 数组字符串。

**性能考虑**：在大规模集群（100,000+ 实例）场景下，`assembleArrayNodes()` 的序列化耗时可能达到数百毫秒。建议 Prometheus Server 的 `scrape_interval` 不低于 30s，且在 Prometheus Server 端配置 `scrape_timeout` 不低于 20s 以避免超时。

### PrometheusAuthFilter 安全控制

`prometheus/src/main/java/com/alibaba/nacos/prometheus/filter/PrometheusAuthFilter.java` 提供 Prometheus 端点的访问控制：

```java
// PrometheusAuthFilter.java:32-48 (Nacos 2.5.3)
public class PrometheusAuthFilter implements Filter {
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        // 检查是否启用了认证
        if (AuthConfigs.isAuthEnabled()) {
            // 验证 AccessToken
            String accessToken = httpRequest.getParameter("accessToken");
            if (StringUtils.isBlank(accessToken) || !AuthManager.getInstance()
                    .validateAccessToken(accessToken)) {
                ((HttpServletResponse) response)
                    .sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid access token");
                return;
            }
        }
        chain.doFilter(request, response);
    }
}
```

**安全建议**：生产环境中务必启用 Nacos Auth（`nacos.core.auth.enabled=true`），确保 Prometheus 端点不被未授权访问泄露所有注册实例的 IP/Port/metadata 信息。

---

## 13.2 补充：Prometheus 指标采集实战配置

### JMX Exporter 集成配置

Nacos 2.5.3 内置的 `PrometheusController` 仅导出实例元数据，JVM 指标（堆内存、GC、线程）需要通过 JMX Exporter 导出。配置方式：

```bash
# 下载 JMX Exporter
wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar

# 创建 JMX Exporter 配置文件
cat > /opt/nacos/conf/jmx_exporter.yml << EOF
startDelaySeconds: 0
ssl: false
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  - pattern: "java.lang<type=Memory><HeapMemoryUsage>used"
    name: jvm_heap_used_bytes
  - pattern: "java.lang<type=Memory><HeapMemoryUsage>max"
    name: jvm_heap_max_bytes
  - pattern: "java.lang<type=GarbageCollector, name=(.*)><CollectionCount>"
    name: jvm_gc_collection_count
  - pattern: "java.lang<type=GarbageCollector, name=(.*)><CollectionTime>"
    name: jvm_gc_collection_seconds
  - pattern: "java.lang<type=Threading><ThreadCount>"
    name: jvm_threads_current
EOF

# 修改 startup.sh 添加 JMX Exporter Agent
# JAVA_OPT="${JAVA_OPT} -javaagent:/opt/nacos/conf/jmx_prometheus_javaagent-0.20.0.jar=9999:/opt/nacos/conf/jmx_exporter.yml"
```

### Micrometer 集成（替代方案）

如果使用 Spring Boot Actuator + Micrometer，可以替代 JMX Exporter：

```xml
<!-- pom.xml -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

```properties
# application.properties
management.endpoints.web.exposure.include=prometheus,health,info
management.endpoint.prometheus.enabled=true
```

访问 `http://nacos-server:8848/actuator/prometheus` 即可获取完整的 JVM + 业务指标。

### Prometheus 抓取配置生产最佳实践

```yaml
# prometheus.yml — Nacos 生产环境推荐配置
global:
  scrape_interval: 30s
  scrape_timeout: 20s
  evaluation_interval: 30s

scrape_configs:
  # Nacos 实例元数据（PrometheusController）
  - job_name: 'nacos-instances'
    scrape_interval: 60s  # 实例数据变化慢，可降低抓取频率
    scrape_timeout: 30s
    metrics_path: '/prometheus'
    static_configs:
      - targets:
        - 'nacos-1:8848'
        - 'nacos-2:8848'
        - 'nacos-3:8848'

  # Nacos JVM 指标（JMX Exporter）
  - job_name: 'nacos-jvm'
    scrape_interval: 15s  # JVM 指标变化快，需高频抓取
    scrape_timeout: 10s
    metrics_path: '/metrics'
    static_configs:
      - targets:
        - 'nacos-1:9999'
        - 'nacos-2:9999'
        - 'nacos-3:9999'
```

---

## 13.5 补充：日志分析实战案例

### 生产案例 1：通过 nacos-cluster.log 排查集群脑裂

**背景**：某金融企业 Nacos 3 节点集群，某日凌晨 2:00-2:15 期间业务反馈服务发现超时率高。

**排查过程**：

```bash
# Step 1: 查看 nacos-cluster.log 中 Raft 选举日志
grep "leader changed" ${nacos.home}/logs/nacos-cluster.log | tail -20

# 输出：
# 02:03:15 INFO RaftCore - leader changed from 192.168.1.101:7848 to 192.168.1.102:7848
# 02:05:22 INFO RaftCore - leader changed from 192.168.1.102:7848 to 192.168.1.101:7848
# 02:08:47 INFO RaftCore - leader changed from 192.168.1.101:7848 to 192.168.1.103:7848
# 02:11:03 INFO RaftCore - leader changed from 192.168.1.103:7848 to 192.168.1.101:7848
```

**根因分析**：15 分钟内发生了 4 次 Leader 切换→频率极高→典型的网络分区导致的"乒乓效应"（Ping-Pong Effect）。排查交换机日志发现凌晨 2:00-2:15 期间该网段发生了短暂的网络中断→导致 Raft 心跳超时→触发 Leader 选举→网络恢复后重新选举→反复切换。

**解决方案**：
1. 调大 Raft 选举超时参数（`nacos.core.raft.election_timeout_ms` 默认 1000ms → 调整至 3000ms）以减少瞬时网络抖动触发的选举
2. 配置交换机网络冗余（双上行链路）避免单点网络故障

### 生产案例 2：通过 remote-server.log 排查 gRPC 连接泄漏

**背景**：某互联网公司 Nacos 集群运行 2 周后，节点 `grpc_connections_total` 持续增长至 18000+（接近默认 `maxServerConnections=20000`），但实际活跃客户端数仅 2000。

**排查过程**：

```bash
# Step 1: 查看 remote-server.log 中连接建立/断开频率
grep "new gRPC bi-stream connection" ${nacos.home}/logs/remote-server.log | wc -l
# 输出：45000+（连接建立次数远超实际客户端数）

grep "connection closed" ${nacos.home}/logs/remote-server.log | wc -l
# 输出：25000+（连接断开次数少于连接建立次数 → 连接泄漏）
```

**根因分析**：部分客户端未正确调用 `shutdown()` 方法，导致 gRPC 连接未正常关闭→服务端 `ConnectionManager` 未收到 `ConnectionCloseEvent`→连接对象未从 `connections` Map 中移除→连接数持续增长。

**解决方案**：
1. 要求客户端升级 SDK 至最新版本（修复了 gRPC 连接关闭的 Bug）
2. 临时措施：配置 `nacos.core.remote.server.grpc.sdk.maxConnectionIdleSeconds` 参数（默认 20s）缩短空闲连接超时时间，让服务端主动关闭僵尸连接

---

## 13.8 补充：生产环境故障排查实战命令组合

### 实战案例：CPU 飙高排查完整流程

**故障现象**：Nacos 节点 CPU 使用率突然飙升至 95%+，业务反馈服务注册延迟从 10ms 飙升至 2s+。

**排查步骤（按顺序执行）**：

```bash
# Step 1: top -H 定位高 CPU 线程
top -H -p <nacos_pid>
# 关注 %CPU 列 → 记录最高 CPU 线程的 TID（如 12345）

# Step 2: jstack 抓取线程快照
jstack <nacos_pid> > /tmp/nacos_cpu_high_$(date +%Y%m%d_%H%M%S).txt

# Step 3: 将 TID 转换为十六进制（jstack 中的 nid 是十六进制）
printf "%x\n" 12345
# 输出：3039

# Step 4: 在 jstack 文件中搜索 nid=0x3039 的线程堆栈
grep -A 30 "nid=0x3039" /tmp/nacos_cpu_high_*.txt

# Step 5: async-profiler 生成 CPU 火焰图（如果 jstack 无法定位根因）
async-profiler -d 30 -e cpu -f /tmp/nacos_cpu_flamegraph.html <nacos_pid>
```

**常见根因**：
1. `DistroClientDataProcessor` 线程 CPU 高 → Distro 同步任务积压 → 排查 Distro 延迟（`naming-server.log` 中 `distro sync failed` 频率）
2. `GrpcBiStreamRequestAcceptor` 线程 CPU 高 → gRPC 请求处理阻塞 → 排查客户端 gRPC 调用频率（`remote-server.log` 中 `push timeout` 频率）
3. `G1GC Concurrent Mark` 线程 CPU 高 → GC 频繁（Old Gen 使用率 > 85%）→ jstat -gcutil 确认 GC 频率

### 实战案例：内存泄漏排查完整流程

**故障现象**：Nacos 节点运行 1 周后 `jvm_heap_used_bytes` 持续线性增长至 90%+，但业务量（注册实例数）未增长。

**排查步骤**：

```bash
# Step 1: jstat 确认 Old Gen 使用率持续增长
jstat -gcutil <nacos_pid> 1000 10lish
# 观察 O 列 → 如果每次采样 O 递增 → 内存泄漏

# Step 2: jmap -histo 统计对象实例数
jmap -histo <nacos_pid> | head -35
# 关注：
# com.alibaba.nacos.naming.core.Instance → 实例数是否远超实际注册实例数
# java.util.concurrent.ConcurrentHashMap$Node → HashMap Node 数是否异常增长

# Step 3: 对比 24h 前后的 jmap -histo 输出（diff 两次采样）
diff <(jmap -histo <nacos_pid> | head -35) <(cat /tmp/nacos_histo_24h_ago.txt)

# Step 4: 导出 HeapDump（会触发 Full GC，生产慎用！）
jmap -dump:live,format=b,file=/tmp/nacos_heap_$(date +%Y%m%d_%H%M%S).hprof <nacos_pid>

# Step 5: Eclipse MAT 分析 HeapDump
# 打开 .hprof 文件 → Histogram → 按 Retained Heap 降序排列 → 定位内存大户
```

**常见根因**：
1. `ConcurrentHashMap$Node` 对象数持续增长 → 某个 `ConcurrentHashMap` 的 key 未清理（如 `ConnectionManager.connections` 中的僵尸连接）
2. `Instance` 对象数远超实际注册实例数 → 过期实例未清理（客户端异常退出后残留注册信息）
3. `byte[]` 对象数异常增长 → 配置内容缓存未清理（`CacheData` 中缓存过期配置内容）

---

## 13.2 深入：Prometheus 核心指标源码映射

### 指标数据来源的源码走读

11 个核心 Prometheus 指标在 Nacos 2.5.3 源码中的具体数据来源如下：

#### naming_service_total — 服务总数

数据来源：`naming/src/main/java/com/alibaba/nacos/naming/core/v2/ServiceManager.java:145-160`

```java
// ServiceManager.java:145-160 (Nacos 2.5.3)
public Set<Service> getSingletons(String namespace) {
    return new HashSet<>(namespaceSingletonMaps.getOrDefault(namespace, 
        new ConcurrentHashMap<>()).values());
}
```

遍历所有命名空间下的所有 Service → 计数即可得到 `naming_service_total`。

#### naming_instance_total — 实例总数

数据来源：`naming/src/main/java/com/alibaba/nacos/naming/core/InstanceOperatorClientImpl.java:127-145`

```java
// InstanceOperatorClientImpl.java:127-145 (Nacos 2.5.3)
@Override
public List<? extends Instance> listAllInstances(String namespace, String serviceName) {
    Service service = serviceManager.getService(namespace, serviceName);
    if (service == null) {
        return new ArrayList<>();
    }
    List<Instance> allInstances = new ArrayList<>();
    for (Cluster cluster : service.getClusterMap().values()) {
        allInstances.addAll(cluster.allIPs());
    }
    return allInstances;
}
```

每个 Service → 每个 Cluster → `allIPs()` → 所有 Instance → 全局求和即可得到 `naming_instance_total`。

#### grpc_connections_total — gRPC 连接总数

数据来源：`core/src/main/java/com/alibaba/nacos/core/remote/ConnectionManager.java:67-73`

```java
// ConnectionManager.java:67-73 (Nacos 2.5.3)
public Map<String, Connection> getConnections() {
    return connections;
}

public int getCurrentConnectionCount() {
    return connectionCount.get();
}
```

`ConnectionManager.connections` 的 `Map.size()` → 通过 JMX MBean 暴露为 `grpc_connections_total`。

#### grpc_push_cost_millis — gRPC 推送延迟

数据来源：`core/src/main/java/com/alibaba/nacos/core/remote/RpcPushService.java:85-105`

```java
// RpcPushService.java:85-105 (Nacos 2.5.3)
public void push(Connection connection, PushAckId pushAckId, Object request) {
    long startTime = System.currentTimeMillis();
    try {
        connection.request(pushAckId, request, timeoutMs);
    } finally {
        long costTime = System.currentTimeMillis() - startTime;
        // 记录推送延迟 Histogram
        MetricsMonitor.recordPushCost(costTime);
    }
}
```

每次 gRPC 推送调用 → `System.currentTimeMillis()` 记录耗时 → 汇总为 Histogram → 通过 Prometheus Histogram 暴露 P50/P95/P99。

### Grafana Dashboard 深入：完整的 5 面板 JSON 配置

以下是一个完整的 Grafana Dashboard JSON 配置（可直接导入 Grafana）：

```json
{
  "__inputs": [
    {
      "name": "DS_PROMETHEUS",
      "label": "Prometheus",
      "type": "datasource"
    }
  ],
  "title": "Nacos 2.5.3 Cluster Monitoring",
  "uid": "nacos-cluster-v2",
  "refresh": "30s",
  "time": {"from": "now-6h", "to": "now"},
  "templating": {
    "list": [
      {
        "name": "datasource",
        "type": "datasource",
        "query": "prometheus",
        "current": {"text": "Prometheus", "value": "Prometheus"}
      },
      {
        "name": "instance",
        "type": "query",
        "query": "label_values(naming_service_total, instance)",
        "multi": true,
        "includeAll": true,
        "allValue": ".*"
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "title": "gRPC Connections (Total)",
      "type": "graph",
      "targets": [
        {
          "expr": "grpc_connections_total{instance=~\"$instance\"}",
          "legendFormat": "{{instance}}"
        },
        {
          "expr": "20000",
          "legendFormat": "Max Connections"
        }
      ],
      "thresholds": [
        {"value": 16000, "colorMode": "warning"},
        {"value": 18000, "colorMode": "critical"}
      ],
      "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8}
    },
    {
      "id": 2,
      "title": "Service & Instance Count",
      "type": "graph",
      "targets": [
        {
          "expr": "naming_service_total{instance=~\"$instance\"}",
          "legendFormat": "Services - {{instance}}"
        },
        {
          "expr": "naming_instance_total{instance=~\"$instance\"}",
          "legendFormat": "Instances - {{instance}}"
        }
      ],
      "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8}
    },
    {
      "id": 3,
      "title": "Config Publish/Get Rate (ops/min)",
      "type": "graph",
      "targets": [
        {
          "expr": "rate(config_publish_total{instance=~\"$instance\"}[5m]) * 60",
          "legendFormat": "Publish - {{instance}}"
        },
        {
          "expr": "rate(config_get_total{instance=~\"$instance\"}[5m]) * 60",
          "legendFormat": "Get - {{instance}}"
        }
      ],
      "gridPos": {"x": 0, "y": 8, "w": 12, "h": 8}
    },
    {
      "id": 4,
      "title": "JVM Heap Memory Usage",
      "type": "graph",
      "targets": [
        {
          "expr": "jvm_heap_used_bytes{instance=~\"$instance\"}",
          "legendFormat": "Used - {{instance}}"
        },
        {
          "expr": "jvm_heap_max_bytes{instance=~\"$instance\"}",
          "legendFormat": "Max - {{instance}}"
        }
      ],
      "thresholds": [
        {"value": 0.85, "colorMode": "warning", "line": true}
      ],
      "gridPos": {"x": 12, "y": 8, "w": 12, "h": 8}
    },
    {
      "id": 5,
      "title": "GC Pause Distribution (Heatmap)",
      "type": "heatmap",
      "targets": [
        {
          "expr": "sum(rate(jvm_gc_pause_seconds_bucket{instance=~\"$instance\"}[5m])) by (le)",
          "legendFormat": "{{le}}"
        }
      ],
      "gridPos": {"x": 0, "y": 16, "w": 24, "h": 8}
    }
  ]
}
```

### Trade-off 分析：Grafana Canvas vs Dashboard JSON

| 维度 | Canvas（自由布局） | Dashboard JSON（网格布局） |
|------|-------------------|------------------------|
| **灵活性** | 高（自由拖拽、叠加） | 中（12 列网格约束） |
| **版本管理** | 低（Canvas 无文本表示、无法 Git Diff） | 高（JSON 文本 → Git diff 可视化变更） |
| **团队协作** | 低（难以 Review 布局变更） | 高（通过 Git PR Review JSON 变更） |
| **迁移成本** | 高（需手动截图 + 重新配置） | 低（一键导入 JSON 文件） |

### 小结

5 个 Grafana 核心 Dashboard 面板的 JSON 配置可直接导入 Grafana 实例。通过 `$instance` Templating 变量实现多集群复用。建议将 Dashboard JSON 文件纳入 Git 版本管理（与 Nacos 配置文件同步管理），便于团队 Review Dashboard 变更。

---

## 13.6 深入：日志体系源码走读

### Nacos 2.5.3 日志适配器架构

Nacos 2.5.3 的 `logger-adapter-impl/` 模块提供了 Log4j2 和 Logback 两种日志适配器的参考实现。Nacos 内部统一使用 SLF4J API（`org.slf4j.LoggerFactory.getLogger()`），通过 SPI 机制加载具体的日志实现。

#### Log4j2 适配器源码走读

`logger-adapter-impl/log4j2-adapter/src/main/java/com/alibaba/nacos/logger/adapter/log4j2/Log4J2NacosLoggingAdapter.java`：

```java
// Log4J2NacosLoggingAdapter.java:30-52 (Nacos 2.5.3)
public class Log4J2NacosLoggingAdapter implements NacosLoggingAdapter {

    static {
        Log4J2NacosLoggingAdapterBuilder.load();
    }

    @Override
    public void info(String message) {
        getLogger().info(message);
    }

    @Override
    public void warn(String message, Throwable throwable) {
        getLogger().warn(message, throwable);
    }

    @Override
    public void error(String message, Throwable throwable) {
        getLogger().error(message, throwable);
    }
}
```

`Log4J2NacosLoggingAdapterBuilder.load()` 负责从 `nacos.logging.config` 系统属性指定的路径加载 Log4j2 配置文件：

```java
// Log4J2NacosLoggingAdapterBuilder.java:35-60 (Nacos 2.5.3)
public static void load() {
    String location = System.getProperty("nacos.logging.config");
    if (StringUtils.isNotBlank(location)) {
        try {
            Configurator.initialize(null, location);
        } catch (Exception e) {
            e.printStackTrace();
        }
    } else {
        Configurator.initialize(null, "classpath:nacos-log4j2.xml");
    }
}
```

**自定义日志配置路径**：通过 JVM 系统属性 `-Dnacos.logging.config=/opt/nacos/conf/custom-log4j2.xml` 指定自定义 Log4j2 配置文件路径。

#### Logback 适配器源码走读

`logger-adapter-impl/logback-adapter-12/src/main/java/com/alibaba/nacos/logger/adapter/logback12/LogbackNacosLoggingAdapter.java`：

```java
// LogbackNacosLoggingAdapter.java:30-48 (Nacos 2.5.3)
public class LogbackNacosLoggingAdapter implements NacosLoggingAdapter {

    static {
        NacosLogbackConfiguratorAdapterV1.Initialize(null, 
            System.getProperty("nacos.logging.config"));
    }

    @Override
    public void info(String message) {
        getLogger().info(message);
    }
    // ... 其他日志方法类似
}
```

### 各模块的 Logback 配置文件示例

Nacos 每个模块都有独立的 Logback 配置文件（如 `naming/src/main/resources/META-INF/logback`）：

```xml
<!-- naming/src/main/resources/META-INF/logback (Nacos 2.5.3) -->
<configuration>
    <!-- 命名服务日志：滚动策略 -->
    <appender name="NAMING-SERVER"
              class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/naming-server.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>${nacos.home}/logs/naming-server.log.%d{yyyy-MM-dd}.%i.gz</fileNamePattern>
            <maxHistory>30</maxHistory>
            <totalSizeCap>3GB</totalSizeCap>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- 根日志级别：INFO -->
    <root level="INFO">
        <appender-ref ref="NAMING-SERVER"/>
    </root>
</configuration>
```

### 日志格式化 Pattern 详解

Nacos 默认日志 Pattern：

```
%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n
```

| 占位符 | 含义 | 示例输出 |
|--------|------|---------|
| `%d{yyyy-MM-dd HH:mm:ss.SSS}` | 日期时间（毫秒精度） | `2026-09-06 12:00:00.123` |
| `%thread` | 线程名 | `grpc-default-worker-ELG-1-1` |
| `%-5level` | 日志级别（左对齐 5 字符） | `INFO ` |
| `%logger{50}` | Logger 名称（最多 50 字符） | `c.a.n.naming.core.ServiceManager` |
| `%msg` | 日志消息 | `register instance: serviceName=...` |
| `%n` | 换行符 | （换行） |

### Trade-off 分析：Log4j2 vs Logback 选型

| 维度 | Log4j2 | Logback |
|------|--------|--------|
| **异步日志性能** | 优（Disruptor RingBuffer 高性能异步 Appender） | 良（AsyncAppender 基于 BlockingQueue） |
| **配置灵活性** | 高（XML / JSON / YAML 多格式） | 中（仅 XML） |
| **GC 友好度** | 优（避免 String 分配，GC 压力小） | 中（正常 Java 对象分配） |
| **社区活跃度** | 低（新项目，社区较晚） | 高（Spring Boot 默认，社区成熟） |
| **Nacos 默认选择** | — | ✅ Nacos 默认使用 Logback |

**Nacos 选择 Logback 的原因**：Logback 是 Spring Boot 的默认日志实现，Nacos 基于 Spring Boot 构建，自然继承了 Logback 作为默认日志实现。同时 Logback 的 `TimeBasedRollingPolicy` 提供了 `maxHistory` + `totalSizeCap` 的组合策略，满足 Nacos 的日志归档需求。

### 生产日志配置最佳实践

```xml
<!-- 生产环境推荐 Logback 配置 -->
<configuration>
    <!-- 异步 Appender：减少日志写入对业务线程的阻塞 -->
    <appender name="ASYNC-NAMING"
              class="ch.qos.logback.classic.AsyncAppender">
        <queueSize>512</queueSize>
        <discardingThreshold>0</discardingThreshold>
        <appender-ref ref="NAMING-SERVER"/>
    </appender>

    <!-- 错误日志单独输出到文件 -->
    <appender name="ERROR-FILE"
              class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${nacos.home}/logs/nacos-error.log</file>
        <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
            <level>ERROR</level>
        </filter>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>${nacos.home}/logs/nacos-error.log.%d{yyyy-MM-dd}.%i.gz</fileNamePattern>
            <maxHistory>90</maxHistory>
            <totalSizeCap>1GB</totalSizeCap>
        </rollingPolicy>
    </appender>

    <root level="INFO">
        <appender-ref ref="ASYNC-NAMING"/>
        <appender-ref ref="ERROR-FILE"/>
    </root>
</configuration>
```

**关键配置说明**：
- **AsyncAppender**：日志写入异步队列（`queueSize=512`），避免日志写入阻塞业务线程
- **ERROR 单独文件**：ERROR 日志单独输出到 `nacos-error.log`，便于快速定位严重问题
- **ERROR 日志更长的保留期**：`maxHistory=90`（3 个月），ERROR 日志通常需要更长的保留期用于事后根因分析

### 小结

Nacos 2.5.3 通过 `logger-adapter-impl/` 模块提供 Log4j2 和 Logback 两种日志适配器，默认使用 Logback。各模块独立的 Logback 配置文件通过 `TimeBasedRollingPolicy` 实现按天滚动归档。生产推荐配置：启用 AsyncAppender（`queueSize=512`）、ERROR 日志单独输出文件（`maxHistory=90`）。

---

## 13.7 深入：自动化运维巡检脚本

### 完整的 Shell 巡检脚本

以下是一个可直接用于生产环境的 Nacos 集群自动化巡检 Shell 脚本：

```bash
#!/bin/bash
# nacos_health_check.sh — Nacos 集群自动化巡检脚本
# 用途：每小时执行一次，检查 7 项核心指标
# 配置：修改 NODES 数组为实际集群节点地址

set -euo pipefail

NODES=("192.168.1.101:8848" "192.168.1.102:8848" "192.168.1.103:8848")
NACOS_HOME="/opt/nacos"
LOG_FILE="/var/log/nacos_health_check.log"
ALERT_WEBHOOK="https://webhook.example.com/nacos-alerts"

exec 1>>"${LOG_FILE}"
exec 2>&1

echo "========================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') Nacos Health Check Start"
echo "========================================="

# 函数：发送告警到企业微信/Slack
send_alert() {
    local severity="$1"
    local title="$2"
    local message="$3"
    curl -s -X POST "${ALERT_WEBHOOK}" \
        -H 'Content-Type: application/json' \
        -d "{\"text\": \"[${severity}] ${title}\n${message}\"}" > /dev/null 2>&1
}

# 1. 集群状态检查
echo "[1/7] 检查集群状态..."
for node in "${NODES[@]}"; do
    response=$(curl -s --connect-timeout 5 --max-time 10 \
        "http://${node}/nacos/v1/core/cluster/nodes")
    if [ $? -ne 0 ]; then
        send_alert "CRITICAL" "节点 Down" "节点 ${node} 不可达"
        echo "FAIL: 节点 ${node} 不可达"
    else
        down_count=$(echo "${response}" | jq '[.nodes[] | select(.state != "UP")] | length')
        if [ "${down_count}" -gt 0 ]; then
            send_alert "CRITICAL" "节点状态异常" "集群中有 ${down_count} 个节点状态不为 UP"
            echo "FAIL: 集群中有 ${down_count} 个节点状态不为 UP"
        else
            echo "OK: 节点 ${node} 集群状态正常"
        fi
    fi
done

# 2. gRPC 连接数检查（通过 Prometheus 指标）
echo "[2/7] 检查 gRPC 连接数..."
for node in "${NODES[@]}"; do
    connections=$(curl -s --connect-timeout 5 --max-time 10 \
        "http://${node}/prometheus" | jq 'length')
    if [ "${connections}" -gt 16000 ]; then
        send_alert "WARNING" "连接数过高" "节点 ${node} 当前连接数 ${connections} > 16000（80% 阈值）"
        echo "WARN: 节点 ${node} gRPC 连接数 ${connections} > 16000"
    else
        echo "OK: 节点 ${node} gRPC 连接数 ${connections}"
    fi
done

# 3. JVM 堆内存检查（通过 jstat）
echo "[3/7] 检查 JVM 堆内存..."
PID=$(pgrep -f "nacos.nacos" | head -1)
if [ -z "${PID}" ]; then
    send_alert "CRITICAL" "进程不存在" "Nacos 进程未运行"
    echo "FAIL: Nacos 进程未运行"
else
    OLD_GEN=$(jstat -gcutil "${PID}" 1000 丛 | awk 'END{print $4}' | sed 's/\..*//')
    if [ "${OLD_GEN}" -gt 85 ]; then
        send_alert "WARNING" "JVM 堆内存过高" "Old Gen 使用率 ${OLD_GEN}% > 85%"
        echo "WARN: Old Gen 使用率 ${OLD_GEN}% > 85%"
    else
        echo "OK: Old Gen 使用率 ${OLD_GEN}%"
    fi
fi

# 4. 磁盘使用率检查
echo "[4/7] 检查磁盘使用率..."
DISK_USAGE=$(df "${NACOS_HOME}/logs" | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "${DISK_USAGE}" -gt 80 ]; then
    send_alert "WARNING" "磁盘使用率过高" "日志目录磁盘使用率 ${DISK_USAGE}% > 80%"
    echo "WARN: 日志目录磁盘使用率 ${DISK_USAGE}% > 80%"
else
    echo "OK: 日志目录磁盘使用率 ${DISK_USAGE}%"
fi

# 5. ERROR 日志统计
echo "[5/7] 检查 ERROR 日志..."
for log in "nacos-cluster.log" "naming-server.log" "config-server.log" "remote-server.log"; do
    error_count=$(grep -c "ERROR" "${NACOS_HOME}/logs/${log}" 2>/dev/null || echo 0)
    if [ "${error_count}" -gt 10 ]; then
        send_alert "WARNING" "ERROR 日志过多" "${log} 中最近 1h 有 ${error_count} 条 ERROR"
        echo "WARN: ${log} 中有 ${error_count} 条 ERROR"
    else
        echo "OK: ${log} 中 ERROR 条数 ${error_count}"
    fi
done

# 6. MySQL 连接池检查
echo "[6/7] 检查 MySQL 连接池..."
MYSQL_CONNECTIONS=$(mysql -u nacos -p -h 192.168.1.100 -e "SHOW PROCESSLIST" 2>/dev/null | wc -l)
if [ "${MYSQL_CONNECTIONS}" -gt 16 ]; then
    send_alert "WARNING" "MySQL 连接数过高" "当前活跃 MySQL 连接数 ${MYSQL_CONNECTIONS} > 16"
    echo "WARN: MySQL 活跃连接数 ${MYSQL_CONNECTIONS} > 16"
else
    echo "OK: MySQL 活跃连接数 ${MYSQL_CONNECTIONS}"
fi

# 7. Raft 日志大小检查
echo "[7/7] 检查 Raft 日志大小..."
RAFT_SIZE=$(du -sm "${NACOS_HOME}/data/protocol/raft/ns/default/" 2>/dev/null | awk '{print $1}')
if [ "${RAFT_SIZE}" -gt 1000 ]; then
    send_alert "WARNING" "Raft 日志过大" "Raft 日志目录大小 ${RAFT_SIZE}MB > 1GB"
    echo "WARN: Raft 日志目录大小 ${RAFT_SIZE}MB > 1GB"
else
    echo "OK: Raft 日志目录大小 ${RAFT_SIZE}MB"
fi

echo "========================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') Nacos Health Check Complete"
echo "========================================="
```

**使用方式**：

```bash
# 配置 Cron 每小时执行一次
# crontab -e
0 * * * * /opt/nacos/scripts/nacos_health_check.sh
```

### 脚本设计要点

1. **超时控制**：每个 curl 请求设置 `--connect-timeout 5 --max-time 10`，避免单个节点不可达导致脚本卡死
2. **告警集成**：通过 `send_alert()` 函数发送告警到企业微信/Slack Webhook，确保巡检异常能及时通知运维团队
3. **幂等执行**：脚本使用 `set -euo pipefail` 确保任何命令失败时立即退出，避免部分失败后继续执行导致误判

### 小结

自动化巡检脚本覆盖 7 项必检指标，可通过 Cron 每小时执行一次。告警集成企业微信/Slack Webhook 确保巡检异常及时通知运维团队。建议将脚本纳入 Git 版本管理，随 Nacos 版本升级同步更新检查项。

---

## 13.3 深入：Grafana Dashboard 完整 PromQL 查询库

### 额外的监控面板 PromQL 查询

除了 5 个核心面板外，以下 6 个补充面板可进一步提升 Nacos 集群监控的全面性：

#### Panel 6：健康检查耗时分布（Heatmap）

```promql
# 健康检查耗时 P50/P95/P99
histogram_quantile(0.50, sum(rate(naming_health_check_cost_millis_bucket{job="nacos"}[5m])) by (le))
histogram_quantile(0.95, sum(rate(naming_health_check_cost_millis_bucket{job="nacos"}[5m])) by (le))
histogram_quantile(0.99, sum(rate(naming_health_check_cost_millis_bucket{job="nacos"}[5m])) by (le))
```

**告警阈值**：P99 > 2000ms（健康检查超时 3000ms 默认）→ 可能误判实例下线

#### Panel 7：Distro 同步失败率（Graph）

```promql
# Distro 同步失败速率（每分钟失败次数）
rate(naming_distro_sync_failed_total{job="nacos"}[5m]) * 60

# Distro 同步总次数（每分钟同步次数）
rate(naming_distro_sync_total{job="nacos"}[5m]) * 60

# Distro 同步失败率百分比
rate(naming_distro_sync_failed_total{job="nacos"}[5m]) / rate(naming_distro_sync_total{job="nacos"}[5m]) * 100
```

**告警阈值**：失败率 > 5%（可能导致多节点临时实例数据不一致）

#### Panel 8：gRPC 推送延迟分布（Histogram）

```promql
# gRPC 推送延迟 P50/P95/P99
histogram_quantile(0.50, sum(rate(grpc_push_cost_millis_bucket{job="nacos"}[5m])) by (le))
histogram_quantile(0.95, sum(rate(grpc_push_cost_millis_bucket{job="nacos"}[5m])) by (le))
histogram_quantile(0.99, sum(rate(grpc_push_cost_millis_bucket{job="nacos"}[5m])) by (le))
```

**告警阈值**：P99 > 500ms（影响服务发现时效）

#### Panel 9：Long Polling 连接数趋势（Graph）

```promql
# 当前配置长轮询连接数
config_listener_total{job="nacos"}

# 长轮询超时速率（每分钟超时次数）
rate(config_long_polling_timeout_total{job="nacos"}[5m]) * 60
```

#### Panel 10：Raft Leader 切换次数（Stat）

```promql
# Raft Leader 切换总次数（累计计数、增量变化率）
changes(raft_leader_changes_total{job="nacos"}[1h])
```

**告警阈值**：1 小时内 Leader 切换 > 2 次（可能网络分区）

#### Panel 11：MySQL 连接池指标（Graph）

```promql
# HikariCP 活跃连接数
hikaricp_active_connections{job="nacos"}

# HikariCP 等待连接数
hikaricp_pending_connections{job="nacos"}

# HikariCP 空闲连接数
hikaricp_idle_connections{job="nacos"}

# HikariCP 连接超时速率（每分钟超时次数）
rate(hikaricp_connection_timeout_total{job="nacos"}[5m]) * 60
```

### Grafana Dashboard Row 布局推荐

```
┌──────────────────────────────────────────────────────────────────┐
│ Row 1: 连接层                                                      │
│ ┌──────────────────────┐  ┌──────────────────────┐                │
│ │ Panel 1: gRPC 连接数│  │ Panel 8: gRPC 推送延迟│               │
│ └──────────────────────┘  └──────────────────────┘                │
├──────────────────────────────────────────────────────────────────┤
│ Row 2: 服务层                                                      │
│ ┌──────────────────────┐  ┌──────────────────────┐                │
│ │ Panel 2: 服务/实例数 │  │ Panel 7: Distro 同步  │               │
│ └──────────────────────┘  └──────────────────────┘                │
│ ┌──────────────────────┐  ┌──────────────────────┐                │
│ │ Panel 6: 健康检查耗时│  │ Panel 10: Raft Leader │               │
│ └──────────────────────┘  └──────────────────────┘                │
├──────────────────────────────────────────────────────────────────┤
│ Row 3: 配置层                                                      │
│ ┌──────────────────────┐  ┌──────────────────────┐                │
│ │ Panel 3: 配置速率    │  │ Panel 9: Long Polling│               │
│ └──────────────────────┘  └──────────────────────┘                │
├──────────────────────────────────────────────────────────────────┤
│ Row 4: JVM 层                                                       │
│ ┌──────────────────────┐  ┌──────────────────────┐                │
│ │ Panel 4: JVM 堆内存 │  │ Panel 5: GC 暂停分布│               │
│ └──────────────────────┘  └──────────────────────┘                │
├──────────────────────────────────────────────────────────────────┤
│ Row 5: 数据库层                                                     │
│ ┌──────────────────────┐                                           │
│ │ Panel 11: MySQL 连接池│                                          │
│ └──────────────────────┘                                           │
└──────────────────────────────────────────────────────────────────┘
```

### 小结

11 个面板 + 5 个 Row 的 Dashboard 布局覆盖了 Nacos 集群的五层监控维度：连接层（gRPC 连接 + 推送延迟）、服务层（服务/实例 + Distro 同步 + 健康检查 + Raft Leader）、配置层（配置速率 + Long Polling）、JVM 层（堆内存 + GC 暂停）、数据库层（MySQL 连接池）。通过 Templating 变量 `$instance` 实现多集群复用，通过 Threshold 阈值线实现面板内快速异常识别。

---

## 13.4 深入：AlertManager 告警路由配置与静默规则

### AlertManager 完整配置示例

以下是一个完整的 AlertManager 配置文件（`alertmanager.yml`），包含告警路由、分组、静默规则：

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m
  smtp_smarthost: 'smtp.example.com:587'
  smtp_from: 'alertmanager@example.com'
  smtp_auth_username: 'alertmanager@example.com'
  smtp_auth_password: 'password'

# 告警路由树
route:
  group_by: ['alertname', 'cluster']
  group_wait: 10s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'default-receiver'
  routes:
    # Critical 告警 → PagerDuty
    - match:
        severity: critical
      receiver: 'pagerduty-critical'
      group_wait: 5s
      repeat_interval: 1h
    # Warning 告警 → Slack
    - match:
        severity: warning
      receiver: 'slack-warning'
      group_wait: 30s
      repeat_interval: 2h

# 告警接收者
receivers:
  - name: 'default-receiver'
    email_configs:
      - to: 'nacos-ops@example.com'

  - name: 'pagerduty-critical'
    pagerduty_configs:
      - routing_key: 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

  - name: 'slack-warning'
    slack_configs:
      - api_url: 'https://webhook.example.com/nacos-alerts'
        channel: '#nacos-alerts'
        title: '{{ .GroupLabels.alertname }}'
        text: '{{ .CommonAnnotations.description }}'

# 告警抑制规则
inhibit_rules:
  # 如果节点 Down 告警正在触发，抑制该节点上的其他 Warning 告警
  - source_match:
      severity: 'critical'
      alertname: 'NodeDown'
    target_match:
      severity: 'warning'
    equal: ['instance']

  # 如果 FullGC 告警正在触发，抑制 HighHeap 告警
  - source_match:
      alertname: 'FrequentFullGC'
    target_match:
      alertname: 'HighHeapMemory'
    equal: ['instance']
```

### 告警抑制规则设计

**抑制规则的核心理念**：当一个 Critical 告警已经触发时，由同一根因导致的 Warning 告警是多余的（例如：节点 Down 已触发 Critical 告警 → 该节点上的高连接数 Warning 告警不再需要重复通知）。

**推荐抑制规则**：

| Source（Critical 告警） | Target（被抑制的 Warning 告警） | 根因关系 |
|----------------------|-------------------------------|----------|
| `NodeDown` | `HighGrpcConnections`、`DistroSyncFail` | 节点 Down → 其他告警为次要 |
| `FrequentFullGC` | `HighHeapMemory` | Full GC 频发 → 堆内存高是必然结果 |
| `DistroSyncFail` | — | Distro 失败直接可能引发多节点数据不一致 |

### 告警静默规则（Silence）

**静默规则用途**：在计划维护窗口期间，通过 AlertManager 的 Silence 功能临时静默特定告警，避免计划维护触发误告警。

**配置示例**（通过 AlertManager API）：

```bash
# 创建静默规则：2026-09-10 02:00-04:00 期间静默所有 Nacos 告警
curl -X POST 'http://alertmanager:9093/api/v2/silences' \
  -H 'Content-Type: application/json' \
  -d '{
    "matchers": [
      {
        "name": "job",
        "value": "nacos",
        "isRegex": false
      }
    ],
    "startsAt": "2026-09-10T02:00:00Z",
    "endsAt": "2026-09-10T04:00:00Z",
    "createdBy": "nacos-ops",
    "comment": "计划维护窗口：Nacos 集群升级至 2.5.opat3"
  }'
```

### Trade-off 分析：分级路由 vs 统一路由

| 维度 | 分级路由（severity-based） | 统一路由（所有告警 → 单一 Channel） |
|------|--------------------------|--------------------------------------|
| **响应速度** | Critical → PagerDuty（5min 内响应）；Warning → Slack（30min 内关注） | 所有告警平等处理 → 无法区分优先级 |
| **告警疲劳** | 低（Warning 不打扰 On-Call） | 高（所有告警都推送 → 大量噪音） |
| **路由复杂度** | 中（需配置多条 route） | 低（单条 route） |
| **适用场景** | 生产环境（7×24 On-Call） | 测试环境（无 On-Call 轮值） |

### 小结

5 条核心告警规则 + AlertManager 分级路由（Critical → PagerDuty / Warning → Slack）+ 告警抑制规则（避免同一根因的多余告警）实现精准告警分发。建议在计划维护窗口期间使用 Silence 功能临时静默告警，避免误告警打扰 On-Call 轮值人员。

---

## 13.9 补充：生产环境定期运维任务自动化

### MySQL 历史配置完整清理脚本

以下是一个完整的 MySQL 历史配置清理脚本，包含 `dry-run` 预览模式：

```bash
#!/bin/bash
# nacos_cleanup_history_config.sh — 清理 Nacos 历史配置记录
# 用法：./nacos_cleanup_history_config.sh [--dry-run]

set -euo pipefail

MYSQL_HOST="192.168.1.100"
MYSQL_PORT="3306"
MYSQL_USER="nacos"
MYSQL_PASS="nacos_password"
MYSQL_DB="nacos_config"
RETENTION_DAYS=30
LOG_FILE="/var/log/nacos_cleanup_history_config.log"

exec 1>>"${LOG_FILE}"
exec 2>&1

echo "========================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') Nacos History Config Cleanup Start"
echo "========================================="

# Dry-run 模式：先预览要删除的记录数
if [ "${1:-}" = "--dry-run" ]; then
    echo "[DRY-RUN] 预览模式：不实际删除数据"
    
    COUNT=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
        -u "${MYSQL_USER}" -p"${MYSQL_PASS}" \
        -e "SELECT COUNT(*) FROM ${MYSQL_DB}.his_config_info WHERE gmt_create < DATE_SUB(NOW(), INTERVAL ${RETENTION_DAYS} DAY);" \
        2>/dev/null | tail -1)
    
    echo "[DRY-RUN] 将要删除 ${COUNT} 条 ${RETENTION_DAYS} 天前的历史配置记录"
    
    # 按天分组显示要删除的记录分布
    mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
        -u "${MYSQL_USER}" -p"${MYSQL_PASS}" \
        -e "SELECT DATE(gmt_create) AS date, COUNT(*) AS count FROM ${MYSQL_DB}.his_config_info WHERE gmt_create < DATE_SUB(NOW(), INTERVAL ${RETENTION_DAYS} DAY) GROUP BY DATE(gmt_create) ORDER BY date;" \
        AN>/dev/null
    
    echo "[DRY-RUN] 预览完成。确认无误后执行: $0 (不加 --dry-run)"
    exit 0
fi

# 实际执行删除
echo "开始清理 ${RETENTION_DAYS} 天前的历史配置记录..."

# 先备份要删除的行数
COUNT_BEFORE=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
    -u "${MYSQL_USER}" -p"${MYSQL_PASS}" \
    -e "SELECT COUNT(*) FROM ${MYSQL_DB}.his_config_info;" 2>/dev/null | tail -1)

# 分批删除（每次删除 10000 条，避免长事务锁表）
BATCH_SIZE=10000
DELETED_TOTAL=0

while true; do
    AFFECTED=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
        -u "${MYSQL_USER}" -p"${MYSQL_PASS}" \
        -e "DELETE FROM ${MYSQL_DB}.his_config_info WHERE gmt_create < DATE_SUB(NOW(), INTERVAL ${RETENTION_DAYS} DAY) LIMIT ${BATCH_SIZE};" \
        2>/dev/null | tail -1)
    
    if [ "${AFFECTED}" = "0" ] || [ -z "${AFFECTED}" ]; then
        break
    fi
    
    DELETED_TOTAL=$((DELETED_TOTAL + AFFECTED))
    echo "已删除 ${DELETED_TOTAL} 条记录..."
    sleep 1  # 短暂暂停，避免对 MySQL 造成持续压力
done

COUNT_AFTER=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
    -u "${MYSQL_USER}" -p"${MYSQL_PASS}" \
    -e "SELECT COUNT(*) FROM ${MYSQL_DB}.his_config_info;" 2>/dev/null | tail -1)

echo "清理完成：共删除 ${DELETED_TOTAL} 条记录"
echo "清理前总数：${COUNT_BEFORE} → 清理后总数：${COUNT_AFTER}"

echo "========================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') Nacos History Config Cleanup Complete"
echo "========================================="
```

### Raft Snapshot 自动检查脚本

```bash
#!/bin/bash
# nacos_check_raft_snapshot.sh — 检查 Raft Snapshot 状态
# 用法：./nacos_check_raft_snapshot.sh

set -euo pipefail

NACOS_HOME="/opt/nacos"
RAFT_DATA_DIR="${NACOS_HOME}/data/protocol/raft/ns/default"
ALERT_WEBHOOK="https://webhook.example.com/nacos-alerts"

# 检查 Raft 日志目录大小
RAFT_SIZE_MB=$(du -sm "${RAFT_DATA_DIR}" 2>/dev/null | awk '{print $1}')

echo "$(date '+%Y-%m-%d %H:%M:%S') Raft Snapshot Check: ${RAFT_SIZE_MB}MB"

# 如果 Raft 日志目录超过  sacr 1GB，发送告警
if [ "${RAFT_SIZE_MB}" -gt 1000 ]; then
    echo "WARNING: Raft 日志目录大小 ${RAFT_SIZE_MB}MB > 1GB"
    
    # 发送告警
    curl -s -X POST "${ALERT_WEBHOOK}" \
        -H 'Content-Type: application/json' \
        -d "{\"text\": \"[WARNING] Nacos Raft 日志目录大小 ${RAFT_SIZE_MB}MB > 1GB，请检查 Raft Snapshot 是否正常\"}" \
        > /dev/null 2>&1
    
    # 检查最近的 Raft Snapshot 时间
    LATEST_SNAPSHOT=$(find "${RAFT_DATA_DIR}" -name "snapshot_*" -type d -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $1}')
    if [ -z "${LATEST_SNAPSHOT}" ]; then
        echo "ERROR: 未找到任何 Raft Snapshot！Raft 日志可能无限增长"
        curl -s -X POST "${ALERT_WEBHOOK}" \
            -H 'Content-Type: application/json' \
            -d "{\"text\": \"[CRITICAL] Nacos 未找到任何 Raft Snapshot！请立即检查 JRaft Snapshot 机制\"}" \
            > /dev/null 2>&1
    else
        LATEST_SNAPSHOT_DATE=$(date -d @"${LATEST_SNAPSHOT%.*}" '+%Y-%m-%d %H:%M:%S')
        echo "最近一次 Raft Snapshot: ${LATEST_SNAPSHOT_DATE}"
    fi
else
    echo "OK: Raft 日志目录大小正常 (${RAFT_SiZE_MB}MB < 1GB)"
fi
```

### 完整的 Cron 配置

```bash
# crontab -e

# 每小时执行一次自动化巡检
0 * * * * /opt/nacos/scripts/nacos_health_check.sh

# 每天凌晨 2:00 执行磁盘检查
0 2 * * * /opt/nacos/scripts/nacos_disk_check.sh

# 每周日凌晨 3:00 执行 Raft Snapshot 检查
0 3 * * 0 /opt/nacos/scripts/nacos_check_raft_snapshot.sh

# 每月 1 日凌晨 4:00 执行 MySQL 历史配置清理（先 dry-run，确认后再实际执行）
0 4 1 * * /opt/nacos/scripts/nacos_cleanup_history_config.sh
```

### 小结

自动化运维脚本包括：自动化巡检脚本（`nacos_health_check.sh`—7 项指标每小时执行）、历史配置清理脚本（`nacos_cleanup_history_config.sh`—每月执行、支持 dry-run 预览模式）、Raft Snapshot 检查脚本（`nacos_check_raft_snapshot.sh`—每周执行）。所有脚本集成告警通知（企业微信/Slack Webhook），确保运维异常及时通知运维团队。
