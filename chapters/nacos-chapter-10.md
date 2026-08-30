# 第 10 章：生产环境部署架构

> **基于 Nacos 2.5.3 源码**  
> **章节目标**: ~75,000 字  
> **写作日期**: 2026-08-29

---

## 10.1 部署模式全景：单机 / 集群 / Kubernetes 三种模式对比矩阵

### 设计背景

Nacos 作为微服务架构中的注册中心与配置中心双重基础设施，其部署架构的选择直接影响整个微服务体系的可用性、可扩展性和运维复杂度。生产环境中，部署模式的选择需要在**资源成本**、**高可用性**、**运维复杂度**和**性能容量**四个维度之间做出权衡。

Nacos 2.5.3 支持三种核心部署模式：

1. **单机模式（Standalone）**：适用于本地开发、CI/CD 流水线测试和小规模非关键业务。使用嵌入式 Derby 数据库，无需外部依赖，一键启动。
2. **集群模式（Cluster）**：适用于生产环境的标准部署方式。通过多个 Nacos 节点组成集群，配合外部 MySQL 数据库实现数据共享和一致性保障。推荐至少 3 个节点。
3. **Kubernetes 模式**：适用于云原生基础设施。通过 StatefulSet 资源类型实现 Nacos 集群的声明式部署，利用 K8s 的自动调度、弹性伸缩和服务发现能力。

Nacos 2.5.3 架构上通过解耦**内核服务**（Naming/Config）与**一致性协议层**（JRaft/Distro），使得不同部署模式可以根据 AP/CP 需求灵活切换。在 2.5.3 版本中，gRPC 双通道架构统一了客户端与服务端通信，部署模式的选择直接影响 gRPC 集群内部通信的拓扑结构。

### 核心架构关系图

三种部署模式的架构对比：

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                          Nacos 部署模式全景架构                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────────┐  ┌──────────────────────┐  ┌──────────────────┐│
│  │  单机模式 (Standalone)  │  │ 集群模式 (Cluster)  │  │  K8s 模式       ││
│  ├──────────────────────────┤  ├──────────────────────┤  ├──────────────────┤│
│  │                          │  │                      │  │                  ││
│  │  ┌─────────────────┐    │  │  ┌────┐ ┌────┐ ┌──┐│  │  ┌────────────┐ ││
│  │  │  Nacos Server   │    │  │  │Node│ │Node│ │..││  │  │ K8s Cluster │ ││
│  │  │  (Config+Naming)│    │  │  │ 1  │ │ 2  │ │N ││  │  │            │ ││
│  │  ├─────────────────┤    │  │  ├────┤ ├────┤ ├──┤│  │  │┌──────────┐│ ││
│  │  │  Derby (嵌入式) │    │  │  │JRaft│ │JRaft│ │JR││  │  ││StatefulSet││ ││
│  │  ├─────────────────┤    │  │  ├────┤ ├────┤ ├──┤│  │  ││(Pod/     ││ ││
│  │  │  HTTP 8848     │    │  │  │Distr│ │Distr│ │Di││  │  ││Headless  ││ ││
│  │  │  gRPC 9848     │    │  │  ├────┤ ├────┤ ├──┤│  │  ││Service)  ││ ││
│  │  └─────────────────┘    │  │  │gRPC │ │gRPC │ │gR││  │  │└──────────┘│ ││
│  │                          │  │  └────┘ └────┘ └──┘│  │  │            │ ││
│  │  适用场景:             │  │                      │  │  │┌──────────┐│ ││
│  │  • 本地开发           │  │  ┌──────────────────┐│  │  ││  MySQL   ││ ││
│  │  • CI/CD 测试         │  │  │  MySQL (外部DB)  ││  │  ││ (外部)   ││ ││
│  │  • 演示环境           │  │  └──────────────────┘│  │  │└──────────┘│ ││
│  │                          │  │                      │  │  │            │ ││
│  │  限制:                 │  │  适用场景:          │  │  │┌──────────┐│ ││
│  │  • 无高可用           │  │  • 生产环境         │  │  ││  Nginx   ││ ││
│  │  • 数据不可恢复       │  │  • 关键业务系统     │  │  ││ (Ingress)││ ││
│  │  • Derby 不可跨机      │  │  • 大规模微服务     │  │  │└──────────┘│ ││
│  └──────────────────────────┘  └──────────────────────┘  │  │            │ ││
│                                                          │  │ 适用场景: │ ││
│                                                          │  │ • 云原生   │ ││
│                                                          │  │ • 自动伸缩 │ ││
│                                                          │  │ • GitOps   │ ││
│                                                          │  └────────────┘ ││
└────────────────────────────────────────────────────────────────────────────────┘

                       图 10-1：Nacos 三种部署模式全景架构对比
```

### 部署模式选择决策矩阵

选择部署模式时需要综合考虑以下维度：

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                        部署模式选择决策树                                          │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│                         ┌─────────────────┐                                       │
│                         │ 是否需要高可用？ │                                       │
│                         └────────┬────────┘                                       │
│                                  │                                              │
│                    ┌─────────────┴─────────────┐                               │
│                    │                           │                               │
│               ┌────▼────┐              ┌─────▼─────┐                         │
│               │ 单机模式  │              │ 是否需要    │                         │
│               │ Standalone │              │ K8s 原生？ │                         │
│               └───────────┘              └──────┬──────┘                        │
│                                              │                                │
│                                ┌─────────────┴─────────────┐                   │
│                                │                           │                     │
│                          ┌─────▼──────┐           ┌────────▼─────────┐         │
│                          │  集群模式    │           │    K8s 模式        │         │
│                          │  Cluster     │           │  StatefulSet       │         │
│                          └─────────────┘           └────────────────────┘         │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────────┘

                               图 10-2：部署模式选择决策树
```

### 三种模式核心对比矩阵

| 维度 | 单机模式 (Standalone) | 集群模式 (Cluster) | Kubernetes 模式 |
|------|----------------------|-------------------|----------------|
| **可用性** | 无高可用，单点故障 | 高可用（≥3 节点），自动故障转移 | 高可用，Pod 自动重启/漂移 |
| **数据持久化** | Derby 嵌入式，不可跨机 | MySQL 外部，主从复制 | MySQL 外部，PersistentVolume |
| **一致性协议** | 无（单节点） | JRaft（CP）+ Distro（AP） | JRaft（CP）+ Distro（AP） |
| **负载均衡** | 无需 | Nginx/HAProxy | K8s Service + Ingress |
| **扩容难度** | 不可扩容 | 手动增加节点 + 修改 cluster.conf | 修改 replicas 数量 |
| **运维复杂度** | 极低 | 中等（需管理 MySQL + Nginx） | 低（K8s 自动管理） |
| **资源开销** | 最低（~512MB 内存） | 中等（3 节点 ≥ 6GB） | 较高（Pod 开销 + K8s 组件） |
| **配置管理** | application.properties 单文件 | 每节点独立配置文件 | ConfigMap + Secrets |
| **日志收集** | 本地文件 | 每节点本地 + 需集中收集 | Sidecar/ DaemonSet 收集 |
| **监控集成** | 手动配置 Prometheus | 手动配置 Prometheus | Prometheus Operator 自动发现 |
| **适用实例规模** | < 100 服务 | 100 ~ 10,000+ 服务 | 100 ~ 10,000+ 服务 |
| **适用团队规模** | 1~3 人开发团队 | 中型运维团队 | DevOps 团队 |

### Nacos 2.5.3 部署架构关键变化

相比 Nacos 1.x 版本，2.5.3 在部署架构层面有以下关键变化：

**1. gRPC 双通道架构统一通信**

Nacos 2.0 引入 gRPC 协议后，部署架构中需要额外考虑 gRPC 端口（默认 9848）的负载均衡。这与 HTTP 端口（8848）的负载均衡策略有所不同：

- HTTP 8848：客户端 SDK 配置服务订阅/发现、控制台 OpenAPI
- gRPC 9848：客户端 SDK 服务注册、心跳、配置长轮询、配置变更通知

在 Nginx/HAProxy 层面，HTTP 可以用 HTTP 反向代理模式，而 gRPC 必须使用 TCP stream 代理（L4 层负载均衡），这是与 Nacos 1.x 部署最大的区别之一。

**2. JRaft 协议的 CP 模式**

Nacos 2.5.3 通过 JRaft 协议实现 CP 模式下的数据一致性。在集群部署模式下，JRaft 集群节点的选举和日志复制需要网络互通。部署时需要确保 `cluster.conf` 中配置的节点 IP 和端口（默认 Raft port = 7848）在所有节点间可达。

**3. Distro 协议的 AP 模式**

Distro 协议用于 Naming 服务中的临时实例数据同步。在 AP 模式下，所有节点对等，数据通过异步复制同步。部署时无需额外配置，只需保证所有节点间 gRPC 通道互通。

### Trade-off 分析

**单机模式 vs 集群模式的选择权衡**：

| 对比维度 | 单机模式 | 集群模式 | 分析 |
|---------|---------|---------|------|
| 故障恢复 | 手动重启，数据可能丢失 | JRaft 自动选举，数据持久化 | 集群模式运维成本高但业务连续性保障强 |
| 性能 | 单节点全量吞吐 | N 节点负载分担 | 集群模式线性扩展性能优于单机 |
| 升级复杂度 | 直接替换 JAR | 滚动升级，需逐节点操作 | 集群模式升级步骤多但零停机 |
| 配置一致性 | 单点写入，无一致性问题 | JRaft 日志复制保证强一致 | 集群模式配置写入有 Leader 转发延迟 |

**集群模式 vs K8s 模式的选择权衡**：

| 对比维度 | 集群模式 | K8s 模式 | 分析 |
|---------|---------|---------|------|
| 部署速度 | 手动部署 30-60 分钟 | Helm Chart/YAML 一键部署 | K8s 模式部署更快但需要 K8s 集群前置条件 |
| 故障自愈 | 需要人工干预或脚本监控 | K8s 自动重启/漂移 | K8s 自愈能力强但调试复杂度高 |
| 网络复杂度 | 节点间直接 IP 通信 | Service/Ingress 抽象 | K8s 网络层多一层抽象，排查问题更复杂 |
| 环境一致性 | 手动保持配置一致性 | GitOps 声明式管理 | K8s 配置管理更标准化但学习曲线陡峭 |

### 小结

- Nacos 2.5.3 提供三种核心部署模式：单机（Derby）、集群（MySQL+JRaft）、Kubernetes（StatefulSet），每种模式适用于不同的场景和规模
- 部署模式选择的核心决策点为：高可用需求 + 是否云原生基础设施
- 2.5.3 版本中 gRPC 双通道架构要求负载均衡层面区分 HTTP（8848）和 gRPC（9848）的代理方式
- 集群模式最低 3 节点满足高可用，配合 MySQL 外部数据库实现数据持久化
- K8s 模式通过 StatefulSet + Headless Service 实现 Pod 稳定网络标识，适合云原生运维团队

---

## 10.2 单机模式部署：startup.sh -m standalone + Derby 嵌入式数据库

### 设计背景

单机模式（Standalone）是 Nacos 2.5.3 最基础的部署模式。它面向开发环境、CI/CD 测试流水线和个人学习场景，以零外部依赖、一键启动为设计目标。核心实现依赖于嵌入式 Derby 数据库存储配置和持久化数据，无需预先安装和配置外部 MySQL 数据库。

在 Nacos 2.5.3 中，单机模式的启动入口位于 `console/src/main/java/com/alibaba/nacos/Nacos.java`，通过解析启动参数 `-m standalone` 选择启动模式。核心启动流程绕过集群初始化步骤，直接启动单个 Nacos 节点，不加载 JRaft 选举模块、不初始化 gRPC 集群内部通信通道、仅使用 LocalDerby 数据源。

Derby 是一个纯 Java 嵌入式关系数据库，Apache Derby 项目在 Nacos 中作为默认内嵌数据源使用。Derby 在 Nacos 2.5.3 中的配置通过 `application.properties` 中 `spring.datasource.platform=derby` 激活，默认数据库文件存储在 `$NACOS_HOME/data/derby/` 目录下。

### 核心架构关系图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    Nacos 单机模式启动流程                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐    │
│  │                    启动脚本 startup.sh(bat)                           │    │
│  │  MODE="standalone" → java -Dnacos.standalone=true               │    │
│  └───────────────────────────┬───────────────────────────────────────────┘    │
│                              │                                              │
│                              ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐    │
│  │                  Nacos.java (Main Class)                             │    │
│  │  @SpringBootApplication                                            │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │    │
│  │  │  1. SpringApplication.run()                                  │  │    │
│  │  │  2. @EnableAutoConfiguration                               │  │    │
│  │  │  3. 扫描 @NacosPropertySource                             │  │    │
│  │  └─────────────────────────────────────────────────────────────────┘  │    │
│  └───────────────────────────┬───────────────────────────────────────────┘    │
│                              │                                              │
│                              ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐    │
│  │           Derby 嵌入式数据库自动初始化                              │    │
│  │  ┌─────────────────────────────────────────────────────────────┐    │    │
│  │  │  spring.datasource.platform=derby                         │    │    │
│  │  │  → LocalDerbyDataSource → $NACOS_HOME/data/derby/        │    │    │
│  │  │  → schema.sql 创建 12 张表                               │    │    │
│  │  └─────────────────────────────────────────────────────────────┘    │    │
│  └───────────────────────────┬───────────────────────────────────────────┘    │
│                              │                                              │
│                              ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐    │
│  │                  Nacos 服务端口绑定                                  │    │
│  │  ┌───────────────────────┐  ┌───────────────────────────────────────┐  │    │
│  │  │  HTTP: 8848         │  │  gRPC: 9848                        │  │    │
│  │  │  (API + 控制台)     │  │  (SDK 内部通信)                    │  │    │
│  │  └───────────────────────┘  └───────────────────────────────────────┘  │    │
│  └───────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  跳过:                                                                       │
│  ✗ JRaft 集群选举初始化                                                   │
│  ✗ gRPC 集群内部通道                                                    │
│  ✗ Distro 数据同步                                                       │
│  ✗ MySQL 外部数据源                                                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

                                  图 10-3：Nacos 单机模式启动流程
```

### 部署步骤详解

#### Step 1：环境准备

**JDK 要求**：Nacos 2.5.3 要求 JDK 1.8+，推荐 OpenJDK 11 或 17。验证方式：

```bash
java -version
# 输出示例：
# openjdk version "11.0.20" 2023-07-18 LTS
# OpenJDK Runtime Environment (build 11.0.20+8-post-Ubuntu-0ubuntu122.04)
# OpenJDK 64-Bit Server VM (build 11.0.20+8-post-Ubuntu-0ubuntu122.04, mixed mode)
```

**Maven 环境**（源码编译时需要）：

```bash
mvn -version
# Apache Maven 3.8.6+
```

#### Step 2：获取 Nacos 2.5.3 安装包

**方式一：下载预编译包**

```bash
# 下载 Nacos 2.5.3 发行版
wget https://github.com/alibaba/nacos/releases/download/2.5.3/nacos-server-2.5.3.tar.gz

# 解压
tar -xzf nacos-server-2.5.3.tar.gz
cd nacos
```

**方式二：源码编译**

```bash
git clone https://github.com/alibaba/nacos.git
cd nacos
git checkout 2.5.3
mvn -Prelease-nacos -Dmaven.test.skip=true clean install -U

# 编译产物位于
distribution/target/nacos-server-2.5.3.tar.gz
```

#### Step 3：单机模式启动

**Linux/macOS**：

```bash
# 单机模式启动（后台运行）
bin/startup.sh -m standalone

# 输出示例：
# nacos is starting with standalone
# nacos is starting，you can check the /path/to/nacos/logs/start.out
```

**Windows**：

```cmd
REM 单机模式启动
cmd startup.cmd -m standalone
```

**启动参数详解**：

| 参数 | 说明 | 示例 |
|------|------|------|
| `-m standalone` | 指定单机模式启动 | `bin/startup.sh -m standalone` |
| `-m cluster` | 指定集群模式启动 | `bin/startup.sh -m cluster` |
| `-p embedded` | 使用嵌入式 Derby 存储（默认单机模式） | 无需手动指定 |
| `-p mysql` | 使用外部 MySQL 存储 | `bin/startup.sh -m standalone -p mysql` |

#### Step 4：验证启动状态

**查看启动日志**：

```bash
tail -f /path/to/nacos/logs/start.out

# 看到以下输出表示启动成功：
# 2026-08-29 10:00:00,000 INFO Nacos started successfully in stand alone mode.
```

**访问控制台**：

```bash
# HTTP API 健康检查
curl http://127.0.0.1:8848/nacos/v1/console/server/state

# 返回示例：
{
  "standalone_mode": "standalone",
  "version": "2.5.3"
}
```

浏览器访问 `http://127.0.0.1:8848/nacos`，默认用户名/密码：`nacos/nacos`。

#### Step 5：Derby 数据库文件检查

```bash
# Derby 数据文件位置
ls -la /path/to/nacos/data/derby/

# 输出示例：
# drwxr-xr-x  2 user group   4096 Aug 29 10:00 .
# drwxr-xr-x  3 user group   4096 Aug 29 10:00 ..
# -rw-r--r--  1 user group 131072 Aug 29 10:00 config_info.DAT
# -rw-r--r--  1 user group 131072 Aug 29 10:00 user_info.DAT
# ...
```

### Derby 嵌入式数据库详解

Nacos 2.5.3 在单机模式下使用 Apache Derby 作为默认嵌入式数据库。Derby 的优势在于零配置、零外部依赖、进程内启动。但存在以下限制：

**Derby 数据库 12 张核心表**：

| 表名 | 用途 | 关键字段 |
|------|------|---------|
| `config_info` | 配置信息主表 | `id`, `data_id`, `group_id`, `content`, `md5` |
| `config_info_aggr` | 配置聚合信息 | `id`, `data_id`, `group_id`, `datum_id`, `content` |
| `config_info_beta` | Beta 配置信息 | `id`, `data_id`, `group_id`, `content`, `beta_ips` |
| `config_info_tag` | 配置标签关联 | `id`, `data_id`, `group_id`, `tenant_id`, `tag_id` |
| `config_tags_relation` | 标签关联关系 | `id`, `tag_name`, `tag_type`, `tenant_id` |
| `group_capacity` | 分组容量 | `id`, `group_id` |
| `his_config_info` | 配置历史版本 | `id`, `nid`, `data_id`, `group_id`, `content`, `op_type` |
| `tenant_info` | 租户（命名空间）信息 | `id`, `tenant_id`, `tenant_name`, `tenant_desc` |
| `user_info` | 用户表 | `id`, `username`, `password` |
| `roles` | 角色表 | `username`, `role` |
| `permissions` | 权限表 | `role`, `resource`, `action` |
| `tenant_capacity` | 租户容量 | `id`, `tenant_id` |

**Derby 限制与注意事项**：

| 限制 | 说明 | 影响 |
|------|------|------|
| 单进程独占 | Derby 数据文件被启动的 Nacos 进程锁定 | 不能同时运行两个 Nacos 实例指向同一 Derby 目录 |
| 不可跨机访问 | Derby 嵌入式数据库不支持远程连接 | 无法从其他机器访问数据 |
| 性能瓶颈 | Derby 非为高并发设计 | 配置数量 > 1000 时查询变慢 |
| 数据迁移困难 | Derby 二进制格式不兼容 MySQL | 从单机迁移到集群需要导出 SQL 再导入 MySQL |
| 无备份机制 | 无原生复制/备份功能 | 需手动备份 `data/derby/` 目录 |

### 单机模式配置调优

**JVM 参数优化**（在 `bin/startup.sh` 中配置）：

```bash
# 单机模式推荐 JVM 参数（修正 Nacos 2.5.3 默认配置）
JAVA_OPT="${JAVA_OPT} -server -Xms512m -Xmx512m -Xmn256m"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=8m"
JAVA_OPT="${JAVA_OPT} -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
JAVA_OPT="${JAVA_OPT} -Xloggc:/path/to/nacos/logs/nacos_gc.log"
```

**application.properties 关键配置**：

```properties
# 单机模式关键配置（conf/application.properties）

# 端口配置
server.port=8848

# Derby 数据库配置（单机模式默认，无需手动配置）
spring.datasource.platform=derby

# 日志目录
nacos.logs.path=/path/to/nacos/logs

# 控制台路径（默认 /nacos）
nacos.console.ui.enabled=true
```

### 常见启动问题排查

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 端口冲突 8848 | 其他进程占用 8848 | `lsof -i:8848` 查找占用进程并关闭 |
| Derby 目录权限不足 | `data/derby/` 目录无写权限 | `chmod -R 755 data/derby/` |
| OutOfMemoryError | JVM 堆内存不足 | 增大 `-Xmx` 参数至 1g |
| 启动后立即退出 | JDK 版本不兼容 | 检查 JDK 1.8+ |
| 控制台无法访问 | 防火墙拦截 8848 | `iptables -I INPUT -p tcp --dport 8848 -j ACCEPT` |

### Trade-off 分析

**单机模式的优势与局限**：

| 方面 | 优势 | 局限 | 权衡 |
|------|------|------|------|
| 部署复杂度 | 一键启动，零外部依赖 | 无法水平扩展 | 适合开发测试，不适合生产 |
| 数据安全 | 无外部 DB 攻击面 | Derby 数据文件损坏无法恢复 | 需定期备份 `data/derby/` 目录 |
| 升级迁移 | 直接替换 JAR 包 | 从 Derby 迁移到 MySQL 复杂 | 生产环境建议一开始就用 MySQL |
| 性能 | 无网络开销，本地访问极速 | 单节点吞吐上限 ~5,000 TPS | 受限于单节点 CPU/内存 |
| 运维成本 | 极低，无需 DBA | 无监控自带告警 | 需自行搭建 Prometheus + Grafana |

### 小结

- Nacos 2.5.3 单机模式通过 `startup.sh -m standalone` 一键启动，使用嵌入式 Derby 作为默认数据存储
- Derby 数据库零配置但不能跨机访问、不支持远程连接，适合开发测试环境
- 单机模式跳过所有集群初始化步骤（JRaft 选举、gRPC 集群通道、Distro 同步），启动速度极快（通常 < 10 秒）
- 生产环境应避免使用单机模式，因为无高可用保障、数据无法自动恢复
- Derby 数据库包含 12 张核心表，数据迁移至 MySQL 时需要导出 SQL 脚本并手动导入

---

## 10.3 3 节点集群架构 + 完整部署 4 步骤（MySQL 初始化→cluster.conf→application.properties→启动验证）

### 设计背景

3 节点集群是 Nacos 2.5.3 生产环境的最低高可用部署标准。核心依赖 JRaft 协议保证 CP 模式下的一致性，以及 Distro 协议实现 AP 模式下的最终一致性。3 是 JRaft 选举中达成多数派的最小节点数：在 3 节点中，需要至少 2 个节点存活（> N/2）才能选举出 Leader，容忍 1 个节点故障。

集群部署的核心组件包括：
1. **Nacos 节点 × 3**：每个节点运行完整的 Config + Naming 服务
2. **MySQL 数据库**：外部共享数据库，存储持久化配置数据（替代 Derby）
3. **JRaft 集群**：3 节点间选举 Leader，保证 CP 模式数据一致性
4. **Distro 同步**：AP 模式下 Naming 数据异步复制

在 2.5.3 中，集群模式的关键变化是 gRPC 双通道架构。集群内部通信使用 gRPC（port 9848 + 7848 Raft port），而客户端 SDK 连接任意节点后，通过 gRPC Server Push 机制接收配置变更通知。这意味着负载均衡器需要同时代理 HTTP 8848（L7）和 gRPC 9848（L4/TCP stream）。

### 核心架构关系图

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                      Nacos 3 节点集群部署架构                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│                           ┌──────────────────────────┐                            │
│                           │      Nginx / HAProxy     │                            │
│                           │  HTTP 8848 反向代理     │                            │
│                           │  gRPC 9848 TCP Stream    │                            │
│                           │  VIP: 192.168.1.100     │                            │
│                           └───────────┬──────────────┘                            │
│                                       │                                          │
│              ┌────────────────────────┼────────────────────────┐                    │
│              │                        │                        │                    │
│     ┌────────▼────────┐   ┌───────▼────────┐   ┌────────▼────────┐           │
│     │  Nacos Node 1    │   │  Nacos Node 2   │   │  Nacos Node 3    │          │
│     │  192.168.1.101   │   │  192.168.1.102  │   │  192.168.1.103   │         │
│     ├─────────────────┤   ├─────────────────┤   ├─────────────────┤          │
│     │ HTTP :8848      │   │ HTTP :8848      │   │ HTTP :8848      │          │
│     │ gRPC :9848      │   │ gRPC :9848      │   │ gRPC :9848      │          │
│     │ Raft :7848      │   │ Raft :7848      │   │ Raft :7848      │          │
│     ├─────────────────┤   ├─────────────────┤   ├─────────────────┤          │
│     │   JRaft 选举     │   │   JRaft 选举     │   │   JRaft 选举     │          │
│     │   ◄──────────►  │   │   ◄──────────►  │   │   ◄──────────►  │          │
│     │   Raft Log 复制  │   │   Raft Log 复制  │   │   Raft Log 复制  │          │
│     ├─────────────────┤   ├─────────────────┤   ├─────────────────┤          │
│     │ Distro 异步同步  │   │ Distro 异步同步  │   │ Distro 异步同步  │          │
│     └────────┬────────┘   └───────┬────────┘   └────────┬────────┘           │
│              │                        │                        │                    │
│              └────────────────────────┼────────────────────────┘                    │
│                                       │                                          │
│                           ┌───────────▼──────────────┐                            │
│                           │       MySQL 数据库         │                            │
│                           │  nacos_config 数据库      │                            │
│                           │  12 张持久化表           │                            │
│                           │  192.168.1.200:3306      │                            │
│                           └──────────────────────────┘                            │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────────┘

                            图 10-4：Nacos 3 节点集群部署架构图
```

### 集群内 JRaft 通信拓扑

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                          JRaft 集群拓扑（3 节点）                               │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│                         ┌─────────────────┐                                    │
│                         │  Node 1 (Leader) │                                    │
│                         │  192.168.1.101  │                                    │
│                         └────────┬────────┘                                    │
│                                 │                                            │
│                     Raft Log Replication (同步)                               │
│                    ┌────────────┴────────────┐                               │
│                    │                         │                               │
│          ┌─────────▼─────────┐  ┌─────────▼─────────┐                      │
│          │  Node 2 (Follower) │  │  Node 3 (Follower) │                      │
│          │  192.168.1.102    │  │  192.168.1.103    │                      │
│          └───────────────────┘  └───────────────────┘                      │
│                                                                               │
│  角色分配规则:                                                                │
│  • Leader 选举：term 最大者胜出                                              │
│  • Leader 负责接收所有写请求，Follower 同步日志                              │
│  • Raft 心跳间隔：500ms（可配置）                                          │
│  • 选举超时：2s-4s 随机（避免多次选举冲突）                                │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────────┘

                        图 10-5：JRaft 集群拓扑（3 节点 Leader-Follower 关系）
```

### 部署 4 步骤详解

#### Step 1：MySQL 数据库初始化

Nacos 2.5.3 集群模式要求外部 MySQL 数据库存储配置数据。MySQL 初始化包含创建数据库、执行初始化 SQL 脚本。

**1.1 创建数据库**：

```sql
-- 连接到 MySQL
mysql -u root -p

-- 创建 nacos_config 数据库（字符集 utf8mb4）
CREATE DATABASE IF NOT EXISTS nacos_config 
  DEFAULT CHARACTER SET utf8mb4 
  DEFAULT COLLATE utf8mb4_general_ci;

-- 创建 nacos 用户并授权
CREATE USER 'nacos'@'%' IDENTIFIED BY 'Nacos@2026';
GRANT ALL PRIVILEGES ON nacos_config.* TO 'nacos'@'%';
FLUSH PRIVILEGES;
```

**1.2 执行初始化 SQL 脚本**：

Nacos 2.5.3 发行包中包含 `conf/mysql-schema.sql` 初始化脚本。执行方式：

```bash
# 执行初始化 SQL
mysql -u nacos -p -h 192.168.1.200 -D nacos_config < /path/to/nacos/conf/mysql-schema.sql
```

初始化脚本创建以下 12 张核心表：

```sql
-- conf/mysql-schema.sql 核心建表语句（简化）

-- 配置信息主表
CREATE TABLE config_info (
  id bigint NOT NULL AUTO_INCREMENT,
  data_id varchar(255) NOT NULL,
  group_id varchar(255) NOT NULL,
  tenant_id varchar(128) DEFAULT '' CHARACTER SET utf8mb3 COLLATE utf8mb3_bin,
  app_name varchar(128) DEFAULT NULL,
  content longtext NOT NULL,
  md5 varchar(32) DEFAULT NULL,
  gmt_create datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  gmt_modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  src_user text,
  src_ip varchar(50) DEFAULT NULL,
  c_desc varchar(256) DEFAULT NULL,
  c_use varchar(64) DEFAULT NULL,
  effect varchar(64) DEFAULT NULL,
  type varchar(64) DEFAULT NULL,
  c_schema text,
  encrypted_data_key text,
  PRIMARY KEY (id),
  UNIQUE KEY uk_configinfo_datagrouptenant (data_id, group_id, tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;

-- 配置历史信息表
CREATE TABLE his_config_info (
  id bigint unsigned NOT NULL,
  nid bigint NOT NULL AUTO_INCREMENT,
  data_id varchar(255) NOT NULL,
  group_id varchar(128) NOT NULL,
  tenant_id varchar(128) DEFAULT '' CHARACTER SET utf8mb3 COLLATE utf8mb3_bin,
  app_name varchar(128) DEFAULT NULL,
  content longtext NOT NULL,
  md5 varchar(32) DEFAULT NULL,
  gmt_create datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  src_user text,
  src_ip varchar(50) DEFAULT NULL,
  op_type char(10) DEFAULT NULL,
  PRIMARY KEY (nid),
  KEY idx_his_config_info_gmt_modified (gmt_modified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;

-- 租户（命名空间）信息
CREATE TABLE tenant_info (
  id bigint NOT NULL AUTO_INCREMENT,
  kp varchar(128) NOT NULL,
  tenant_id varchar(128) DEFAULT '' CHARACTER SET utf8mb3 COLLATE utf8mb3_bin,
  tenant_name varchar(128) DEFAULT '' CHARACTER SET utf8mb3 COLLATE utf8mb3_bin,
  tenant_desc varchar(256) DEFAULT NULL,
  create_source varchar(32) DEFAULT NULL,
  gmt_create bigint NOT NULL,
  gmt_modified bigint NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_kp_tenant_id (kp, tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;

-- 用户表
CREATE TABLE users (
  username varchar(50) NOT NULL,
  password varchar(500) NOT NULL,
  enabled tinyint NOT NULL,
  PRIMARY KEY (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- 角色表
CREATE TABLE roles (
  username varchar(50) NOT NULL,
  role varchar(50) NOT NULL,
  UNIQUE KEY uk_username_role (username, role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- 权限表
CREATE TABLE permissions (
  role varchar(50) NOT NULL,
  resource varchar(255) NOT NULL,
  action varchar(8) NOT NULL,
  UNIQUE KEY uk_role_permission (role, resource, action)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

**1.3 MySQL 连接验证**：

```bash
mysql -u nacos -p -h 192.168.1.200 -e "USE nacos_config; SHOW TABLES;"

# 预期输出：
# Tables_in_nacos_config
# config_info
# config_info_aggr
# config_info_beta
# config_info_tag
# config_tags_relation
# group_capacity
# his_config_info
# tenant_info
# tenant_capacity
# users
# roles
# permissions
```

#### Step 2：cluster.conf 配置

`cluster.conf` 是 Nacos 集群的节点发现配置文件。每个节点通过读取 `cluster.conf` 文件发现集群中其他成员节点。

**cluster.conf 格式**：

```
# cluster.conf - Nacos 集群节点配置（每行一个节点）
# 格式：IP:PORT
# PORT 是 Raft 通信端口（默认 7848，不要与 HTTP 8848 混淆）

192.168.1.101:7848
192.168.1.102:7848
192.168.1.103:7848
```

**关键注意事项**：

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| IP | 节点的真实 IP，不可使用 127.0.0.1 | 无默认值 |
| PORT | Raft 内部通信端口，非 HTTP 8848 | 7848 |
| 配置一致性 | 所有 3 个节点的 `cluster.conf` 内容必须完全一致 | 必须手动确保 |

**在 Nacos 2.5.3 中 `cluster.conf` 的读取逻辑**：

Nacos 启动时通过 `ServerMemberManager` 读取 `cluster.conf` 文件并初始化集群成员列表。`ServerMemberManager.init()` 方法解析文件内容，提取 IP:PORT 列表，注册本地节点并发现其他成员。

```
ServerMemberManager.init() 流程:
1. 读取 Nacos 安装目录下的 conf/cluster.conf
2. 按行解析，每行格式为 IP:PORT
3. 验证 IP 格式是否合法
4. 检查本地 IP 是否在集群列表中
5. 构建 ClusterMember 对象列表
6. 注册到 MemberChangeListener 监听
```

**cluster.conf 常见错误**：

| 错误 | 原因 | 解决方案 |
|------|------|---------|
| 所有节点启动后相互看不到 | `cluster.conf` 中 IP 为 127.0.0.1 | 改为每台机器的真实 IP |
| 部分节点看不到其他节点 | `cluster.conf` 内容不一致 | 确保所有节点文件完全相同 |
| Raft 选举失败 | Raft 端口 7848 被防火墙阻塞 | `iptables -I INPUT -p tcp --dport 7848 -j ACCEPT` |
| 节点启动后一直在 "joining" 状态 | 无法连接 MySQL | 检查 MySQL 连接配置 |

#### Step 3：application.properties 配置

Nacos 2.5.3 的核心应用配置文件 `conf/application.properties` 在集群模式下需要配置 MySQL 数据源和端口参数。

**核心配置项**：

```properties
# ============================================================
# conf/application.properties - Nacos 2.5.3 集群模式核心配置
# ============================================================

# -------------------- HTTP 端口 --------------------
# Nacos HTTP API 和控制台端口
server.port=8848

# -------------------- MySQL 数据源配置 --------------------
# 数据库类型：mysql（集群模式必须配置）
spring.datasource.platform=mysql

# MySQL 集群节点数量
# db.num=1 表示单 MySQL 实例
# db.num>1 表示 MySQL 主从 + 读写分离
# 这里配置为 1（单个 MySQL 实例）
db.num=1

# MySQL 连接 URL（第 1 个数据源）
# useSSL=false：集群内部网络无需 SSL
# allowPublicKeyRetrieval=true：允许客户端自动获取 RSA 公钥
# serverTimezone=Asia/Shanghai：时区配置
db.url.0=jdbc:mysql://192.168.1.200:3306/nacos_config\
  ?characterEncoding=utf8&connectTimeout=1000&socketTimeout=3000\
  &autoReconnect=true&useUnicode=true&useSSL=false\
  &serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true

# MySQL 用户名
db.user.0=nacos

# MySQL 密码
db.password.0=Nacos@2026

# -------------------- Raft 端口 --------------------
# Raft 内部通信端口（用于 JRaft 选举和日志复制）
# 不要与 HTTP 8848 或 gRPC 9848 混淆！
nacos.core.protocol.raft.port=7848

# -------------------- 日志配置 --------------------
# Nacos 日志根目录
# 每个节点各自存储日志，不要使用共享存储
nacos.logs.path=/path/to/nacos/logs

# -------------------- 访问控制 --------------------
# 是否开启鉴权（集群模式建议开启）
nacos.core.auth.enabled=true

# Nacos 控制台默认路径
nacos.console.ui.enabled=true
```

**多 MySQL 数据源配置（db.num > 1）**：

当 `db.num=2` 时，Nacos 2.5.3 支持 MySQL 主从读写分离：

```properties
# MySQL 集群节点数
db.num=2

# 主数据库（Master - 用于写操作）
db.url.0=jdbc:mysql://192.168.1.200:3306/nacos_config?characterEncoding=utf8
&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useSSL=false
&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=Nacos@2026

# 从数据库（Slave - 用于读操作）
db.url.1=jdbc:mysql://192.168.1.201:3306/nacos_config?characterEncoding=utf8
&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useSSL=false
&serverTimezone=Asia/Shanghai
db.user.1=nacos
db.password.1=Nacos@2026
```

#### Step 4：启动集群并验证

**4.1 依次启动 3 个节点**：

```bash
# Node 1（192.168.1.101）
# SSH 登录到 Node 1
cd /path/to/nacos
bin/startup.sh -m cluster

# Node 2（192.168.1.102）
# SSH 登录到 Node 2
cd /path/to/nacos
bin/startup.sh -m cluster

# Node 3（192.168.1.103）
# SSH 登录到 Node 3
cd /path/to/nacos
bin/startup.sh -m cluster
```

**4.2 查看启动日志确认集群形成**：

```bash
# 在 Node 1 上查看启动日志
tail -f /path/to/nacos/logs/naming-server.log

# 关键日志输出（成功加入集群）：
# INFO [Cluster-Raft] Member [192.168.1.101:7848] is added to the leader node
# INFO [Cluster-Raft] Member [192.168.1.102:7848] is added to the leader node
# INFO [Cluster-Raft] Member [192.168.1.103:7848] is added to the leader node
# INFO Nacos started successfully in cluster mode.

# 查看集群状态
curl -X GET 'http://192.168.1.101:8848/nacos/v1/core/cluster/nodes'

# 返回示例：
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "ip": "192.168.1.101",
      "port": 7848,
      "state": "UP",
      "extendInfo": {
        "lastRefreshTime": 1678901234567,
        "raftTerm": 1,
        "raftPort": "7848"
      }
    },
    {
      "ip": "192.168.1.102",
      "port": 7848,
      "state": "UP",
      "extendInfo": {
        "raftTerm": 1,
        "raftPort": "7848"
      }
    },
    {
      "ip": "192.168.1.103",
      "port": 7848,
      "state": "UP",
      "extendInfo": {
        "raftTerm": 1,
        "raftPort": "7848"
      }
    }
  ]
}
```

**4.3 Raft Leader 确认**：

```bash
# 查询当前 Raft Leader 节点
curl -X GET 'http://192.168.1.101:8848/nacos/v1/core/cluster/nodes?withLeader=true'

# 返回示例中 extendInfo 包含 "leader": "true" 字段
```

**4.4 Nacos 控制台验证**：

浏览器访问任意节点的控制台：`http://192.168.1.101:8848/nacos`

验证项：
- 登录：默认 `nacos/nacos`
- 左侧菜单「集群管理」→「节点列表」查看 3 个节点均为 UP 状态
- 「配置管理」→「配置列表」可正常增删改查
- 「服务管理」→「服务列表」可正常查看服务

### JRaft Leader 选举流程时序

```
┌────────┐        ┌────────┐        ┌────────┐
│ Node 1 │        │ Node 2 │        │ Node 3 │
└───┬────┘        └───┬────┘        └───┬────┘
    │                  │                  │
    │  启动 JRaft       │                  │
    │  term=0           │                  │
    │                  │                  │
    │  等待选举超时    │                  │
    │  (2s~4s 随机)    │                  │
    │                  │                  │
    │  Follower        │  Follower        │  Follower
    │                  │                  │
    │  超时→Candidate │                  │
    │  term=1          │                  │
    │                  │                  │
    │  发送 RequestVote RPC               │
    │  (term=1,        │                  │
    │   lastLogIndex,   │                  │
    │   lastLogTerm)    │                  │
    │                  │                  │
    ├─────────────────►│                  │
    │                  │  收到 RequestVote│
    │                  │  检查 term > own │
    │                  │  term=1 > 0 ✓   │
    │                  │  日志最新 ✓     │
    │                  │  投票给 Node 1  │
    │                  │                  │
    │◄─────────────────┤                  │
    │  Vote Granted    │                  │
    │                  │                  │
    │  ───────────────────────────────────►│
    │  RequestVote RPC                   │
    │                  │  收到 RequestVote│
    │                  │  term=1 > 0 ✓   │
    │                  │  投票给 Node 1  │
    │◄───────────────────────────────────┤
    │  Vote Granted                       │
    │                  │                  │
    │  获得 2/3 票 (含自己)              │
    │  → 成为 Leader    │                  │
    │                  │                  │
    │  发送 Heartbeat   │                  │
    │  (AppendEntries RPC with no log)   │
    │                  │                  │
    ├─────────────────►│                  │
    ├──────────────────────────────────────►│
    │                  │                  │
    │  Leader          │  Follower        │  Follower
    │                  │                  │
```

                        图 10-6：JRaft Leader 选举时序图（3 节点）

### 故障转移场景验证

**场景 1：Leader 节点宕机**

```bash
# 模拟 Node 1（Leader）宕机
kill -9 $(pgrep -f nacos)

# 在 Node 2 观察集群变化
tail -f /path/to/nacos/logs/naming-server.log

# 预期日志：
# INFO [Cluster-Raft] Leader heartbeat timeout, starting election
# INFO [Cluster-Raft] Member [192.168.1.101:7848] is dead
# INFO [Cluster-Raft] New leader has been elected: [192.168.1.102:7848]

# 验证新 Leader
curl -X GET 'http://192.168.1.102:8848/nacos/v1/core/cluster/nodes'
# 预期：Node  Samson 成为新 Leader
```

**场景 2：Follower 节点宕机**

```bash
# 模拟 Node 去打 宕机（非 Leader）
kill -9 $(pgrep -f nacos)

# 集群仍正常：Leader 2/3 仍在多数派
curl -X GET 'http://192.168.1.101:8848/nacos/v1/core/cluster/nodes'
# 显示 2 个 UP 节点，1 个 DOWN 节点
```

### Trade-off 分析

**3 节点 vs 5 节点集群**：

| 维度 | 3 节点 | 5 节点 | 分析 |
|------|--------|--------|------|
| 容错能力 | 容忍 1 节点故障 | 容忍 2 节点故障 | 5 节点更高可用但资源成本多 66% |
| Raft 提交延迟 | 低（只需 2/3 确认） | 中等（需要 3/5 确认） | 5 节点提交延迟稍高但稳定性更好 |
| MySQL 连接数 | 3 × 连接池大小 | 5 × 连接池大小 | 5 节点对 MySQL 连接压力更大 |
| 运维复杂度 | 低（3 节点管理简单） | 中等 | 5 节点排障更复杂 |
| 适用场景 | 中小规模微服务 (< 500 服务) | 大规模微服务 (> 1000 服务) | 根据服务规模动态选择 |

**3 节点集群部署常见踩坑**：

| 踩坑 | 表现 | 根因 | 解决方案 |
|------|------|------|---------|
| 使用 127.0.0.1 | 节点相互看不到 | `cluster.conf` 中写 127.0.0.1 | 统一改为真实 IP |
| Raft 端口冲突 | 启动失败 | 7848 端口被占用 | `lsof -i:7848` 查找 |
| MySQL 连接失败 | DB 初始化失败 | MySQL 未允许远程连接 | `GRANT ALL ON nacos_config TO 'nacos'@'%'` |
| 防火墙阻塞 | Raft 选举超时 | iptables 拦截 7848 | `iptables -A INPUT -p tcp --dport 7848 -j ACCEPT` |
| cluster.conf 不同步 | 集群分裂 | 手动修改某个节点 cluster.conf | 使用 Ansible/Shell 批量同步 |

### 小结

- Nacos 2.5.3 3 节点集群是生产环境最低高可用标准部署，依赖 JRaft 协议实现 CP 模式下的一致性
- 部署 4 步骤：MySQL 数据库初始化→cluster.conf→application.properties→启动验证
- `cluster.conf` 配置 Raft 通信端口（7848），必须使用真实 IP 且所有节点一致
- MySQL 数据库初始化包含 12 张核心持久化表（config_info 等），必须提前执行 `mysql-schema.sql`
- JRaft Leader 选举在 3 节点中需获得 ≥2 票（多数派），容忍 1 节点故障
- gRPC 9848 端口需与 HTTP 8848 区分，负载均衡器必须使用 TCP Stream 代理 gRPC（L4），而非 HTTP 反向代理（L7）

---

## 10.4 Nginx 负载均衡配置：HTTP upstream + gRPC TCP stream 代理

### 设计背景

Nacos 2.5.3 集群部署完成后，客户端需要一个统一的入口地址来访问集群。生产环境中直接在客户端配置多个 Nacos 节点 IP 存在以下问题：

1. **客户端配置膨胀**：每个微服务实例的 `spring.cloud.nacos.discovery.server-addr` 需要列举所有节点，节点变更时需逐一更新所有客户端
2. **负载不均**：客户端内置的 simple load balancing 策略仅做随机选择，无法根据节点实际负载、响应时间动态调整
3. **故障转移滞后**：客户端感知节点宕机依赖心跳超时（默认 15s），期间请求持续发往故障节点
4. **TLS 终结不便**：每个 Nacos 节点各自配置证书增加运维复杂度

引入 Nginx 作为反向代理层可解决上述问题：

- **统一入口**：客户端只需配置一个 Nginx VIP/域名，后端节点变更对客户端透明
- **灵活负载策略**：支持最少连接（least_conn）、IP Hash、加权轮询等多种算法
- **快速故障检测**：Nginx 主动健康检查（被动 + 主动），秒级摘除故障节点
- **TLS 集中终结**：证书仅在 Nginx 层配置，内部通信可选 mTLS

Nacos 2.5.3 使用双端口架构——HTTP 8848（API/控制台）和 gRPC 9848（客户端双向流通信），这意味着 Nginx 需要同时代理两种协议：HTTP 反向代理（L7）和 TCP Stream 代理（L4），不能混用。

### 核心架构关系图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  Nginx 负载均衡双通道代理架构                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  客户端 (微服务 A)       客户端 (微服务 B)       客户端 (微服务 C)        │
│       │                       │                       │                    │
│       └───────────────────────┼───────────────────────┘                    │
│                               │                                         │
│                     ┌─────────▼─────────┐                               │
│                     │   Nginx (VIP:16448) │                              │
│                     │                     │                               │
│         ┌───────────┴───────────┐     │                               │
│         │                       │     │                               │
│    ┌────▼────┐          ┌─────▼─────▼──┐                            │
│    │ HTTP 反向│          │ TCP Stream    │                            │
│    │ 代理 L7 │          │ 代理 L4      │                            │
│    │ :8848    │          │ :9848         │                            │
│    └────┬────┘          └────┬──────────┘                            │
│         │                    │                                         │
│    ┌────▼────────────────────▼────┐                                   │
│    │        Nacos 集群            │                                   │
│    │  ┌───────┐ ┌───────┐ ┌──────┐│                                  │
│    │  │Node 1 │ │Node 2 │ │Node 3││                                  │
│    │  │:8848  │ │:8848  │ │:8848 ││                                  │
│    │  │:9848  │ │:9848  │ │:9848 ││                                  │
│    │  └───────┘ └───────┘ └──────┘│                                  │
│    └────────────────────────────────┘                                   │
│                                                                          │
│   HTTP 层 (L7):                                                          │
│   • 路由: /nacos/v1/*, /nacos/v2/* → upstream nacos_http               │
│   • 负载算法: least_conn                                                 │
│   • 健康检查: passive (fail_timeout=10s max_fails=3)                   │
│   • 长连接: keepalive 64 (降低 TCP 握手开销)                           │
│                                                                          │
│   gRPC 层 (L4):                                                          │
│   • 路由: TCP:9848 → upstream nacos_grpc                                │
│   • 代理模式: stream (透明 TCP 代理，不解包)                            │
│   • 负载算法: least_conn                                                 │
│   • 健康检查: proxy_pass + fail_timeout=10s                             │
│                                                                          │
│                    图 10-12：Nginx HTTP + gRPC 双通道代理架构              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**关键设计要点**：

1. **HTTP 与 gRPC 必须分通道代理**：HTTP 8848 使用 `http` 块中的 `upstream` + `proxy_pass`，gRPC 9848 必须使用 `stream` 块中的 `upstream` + `proxy_pass`。不能在 `http` 块中用 `proxy_pass http://backend:9848` 代理 gRPC——HTTP/2 的 gRPC 需要端到端的 HTTP/2 连接，Nginx HTTP 反向代理会破坏 gRPC 的 HTTP/2 多路复用特性。

2. **gRPC 负载均衡必须基于 L4 连接数**：gRPC 使用单个长期 HTTP/2 连接（双向流），每个客户端只建立一条 gRPC 连接。因此 `least_conn` 算法在 gRPC 场景等价于「最少客户端数」分配——每个 Nacos 节点承载的 gRPC 连接数大致均等。

3. **健康检查机制差异**：HTTP 8848 代理可使用 Nginx passive health check（`proxy_next_upstream` + `fail_timeout`），而 TCP stream 代理的被动健康检查依赖 `proxy_connect_timeout` 和 `fail_timeout`。Nginx Plus 支持主动健康检查（`health_check`），开源版只能依靠被动检测。

### Nginx HTTP 反向代理配置（L7：8848 端口）

#### upstream 后端服务器组配置

```nginx
# /etc/nginx/conf.d/nacos-http.conf
upstream nacos_http {
    # 负载均衡算法：least_conn（最少连接数）
    # 可选：ip_hash（基于客户端 IP 哈希，用于粘性会话）
    # 可选：weighted round-robin（加权轮询）
    least_conn;

    # Nacos 集群 3 节点列表（IP:PORT）
    server 192.168.1.101:8848 max_fails=3 fail_timeout=30s weight=1;
    server 192.168.1.102:8848 max_fails=3 fail_timeout=30s weight=1;
    server 192.168.1.103:8848 max_fails=3 fail_timeout=30s weight=1;

    # 长连接池配置：保持与后端 Nacos 的 HTTP 长连接
    # 减少频繁 TCP 三次握手/四次挥手开销
    keepalive 64;
    # keepalive_timeout: 单个长连接最大空闲时间（秒）
    # keepalive_requests: 单个长连接最大请求数（超过后 Nginx 主动关闭）
    keepalive_timeout 120s;
    keepalive_requests 10000;
}
```

**参数详解**：

| 参数 | 值 | 说明 |
|------|-----|------|
| `least_conn` | - | 最少连接数算法，将新请求分配到当前活跃连接数最少的后端节点。适合 Nacos HTTP API 这种短连接请求模式 |
| `max_fails` | 3 | 被动健康检查失败阈值。在 `fail_timeout` 时间段内累计失败 3 次，Nginx 将节点标记为 `DOWN` |
| `fail_timeout` | 30s | 节点被标记 `DOWN` 后的冷却时间。30 秒后 Nginx 尝试发送探测请求，成功则恢复节点 |
| `weight` | 1 | 节点权重。所有节点权重相同表示均等分配。可根据节点硬件配置差异化设置 |
| `keepalive` | 64 | 到后端 Nacos 的最大空闲长连接数。每个 worker 进程独享此缓存池。设太低会导致频繁新建连接；设太高浪费后端连接资源 |

#### server 块配置

```nginx
# /etc/nginx/conf.d/nacos-http.conf (续)
server {
    listen 80;
    server_name nacos.example.com;

    # 访问日志（生产建议开启 JSON 格式以便日志采集）
    access_log /var/log/nginx/nacos-http-access.log main;
    error_log  /var/log/nginx/nacos-http-error.log  warn;

    # 请求体大小限制（Nacos 配置发布 API 可能携带大数据体）
    client_max_body_size 10m;

    # 代理缓冲区配置
    proxy_buffer_size 128k;
    proxy_buffers 4 256k;
    proxy_busy_buffers_size 256k;

    # 超时配置（Nacos 配置查询可能长轮询，适当拉长超时）
    proxy_connect_timeout 5s;
    proxy_send_timeout 60s;
    proxy_read_timeout 60s;

    # HTTP 头传递
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    # 关闭 HTTP/2 无关头（后端 Nacos 使用 HTTP/1.1）
    proxy_http_version 1.1;
    proxy_set_header Connection "";  # 启用 keepalive 长连接

    # ==================================================================
    # Nacos HTTP API 路由（L7 代理）
    # ==================================================================
    location /nacos {
        proxy_pass http://nacos_http;
        
        # 失败重试：超时/连接错误/500+ 错误
        proxy_next_upstream error timeout http_500 http_502 http_503;
        proxy_next_upstream_tries 2;
    }

    # ==================================================================
    # Nacos 控制台静态资源（默认 /nacos/index.html）
    # ==================================================================
    location /nacos/ {
        proxy_pass http://nacos_http;
    }

    # ==================================================================
    # 健康检查端点（用于外部监控系统探测）
    # ==================================================================
    location /health {
        return 200 "Nginx OK\n";
        add_header Content-Type text/plain;
    }
}
```

**超时参数调优指南**：

Nacos 客户端与 Nacos 服务端之间通过 HTTP API 通信，存在以下长超时场景：

- **配置长轮询（Config Long Polling）**：客户端订阅配置后会发起长轮询请求（默认超时 30s），Nacos 服务端持有该连接直到配置变更或超时。因此 `proxy_read_timeout` 必须大于长轮询超时（建议 60s）
- **服务订阅查询**：客户端首次订阅服务时查询全量服务列表，数据量大可能耗时较长

### Nginx gRPC TCP Stream 代理配置（L4：9848 端口）

#### 为什么 gRPC 必须用 L4 TCP Stream 代理

gRPC 基于 HTTP/2 协议实现双向流通信。Nacos 2.x 客户端与服务端之间建立的是**单个长期 HTTP/2 连接**，该连接承载以下双向流：

- **服务发现请求流（Naming Request Stream）**：客户端发起服务发现查询 + 订阅变更推送
- **配置查询请求流（Config Request Stream）**：客户端发起配置查询 + 监听变更推送

如果在 HTTP 块中反向代理 gRPC：

1. Nginx HTTP 反向代理会将 HTTP/2 降级为 HTTP/1.1 转发给后端 Nacos（或尝试 HTTP/2 后端但 Nginx 自身 HTTP/2 代理实现有限），破坏 gRPC 的多路复用
2. gRPC 双向流依赖 HTTP/2 的 stream 帧语义，Nginx HTTP 反向代理无法正确维护流状态

因此 gRPC 代理必须使用 `stream` 块——Nginx 在 L4 层透明转发 TCP 数据包，**不理解 HTTP/2 帧内容**，仅做连接级路由。

#### stream 块配置

```nginx
# /etc/nginx/conf.d/nacos-grpc.conf
# 注意：stream 块必须定义在 http 块外部（顶层）
stream {
    # 日志配置（stream 块独立的日志格式）
    log_format stream '$remote_addr [$time_local] '
                     '$protocol $status $bytes_sent $bytes_received '
                     '$session_time "$upstream_addr" '
                     '"$upstream_bytes_sent" "$upstream_bytes_received" "$upstream_connect_time"';

    access_log /var/log/nginx/nacos-grpc-access.log stream;
    error_log  /var/log/nginx/nacos-grpc-error.log warn;

    # ==================================================================
    # gRPC upstream 后端服务器组（L4 TCP Stream 代理）
    # ==================================================================
    upstream nacos_grpc {
        # 负载算法：least_conn（最少连接数）
        # 每个 Nacos 2.x 客户端只建立一条 gRPC 连接
        # 因此 least_conn 等价「最少客户端数」分配
        least_conn;

        # Nacos 集群 3 节点 gRPC 端口（9848）
        # 注意：9848 是 gRPC 端口，不要误写为 HTTP 8848！
        server 192.168.1.101:9848 max_fails=3 fail_timeout=30s;
        server 192.168.1.102:9848 max_fails=3 fail_timeout=30s;
        server 192.168.1.103:9848 max_fails=3 fail_timeout=30s;
    }

    # ==================================================================
    # gRPC 代理 server（监听 9848 端口）
    # ==================================================================
    server {
        listen 9848;

        # TCP Stream 代理：透明转发 gRPC HTTP/2 帧
        proxy_pass nacos_grpc;

        # 连接超时配置
        proxy_connect_timeout 5s;   # 与后端 Nacos 建立 TCP 连接超时
        proxy_timeout 120s;         # 空闲连接保持时间（gRPC 长连接）

        # 失败重试（连接超时或连接拒绝）
        proxy_next_upstream on;
        proxy_next_upstream_tries 2;
    }
}
```

**gRPC TCP Stream 代理参数详解**：

| 参数 | 值 | 说明 |
|------|-----|------|
| `least_conn` | - | 最少连接数。gRPC 场景每个客户端只有 1 条连接，等价于「最少客户端数」分配 |
| `max_fails` | 3 | 被动健康检查：TCP 连接失败 3 次则标记节点 `DOWN` |
| `fail_timeout` | 30s | 节点被标记 `DOWN` 后的冷却时间 |
| `proxy_connect_timeout` | 5s | TCP 连接建立超时。gRPC 端口仅做连接转发，不应设过长 |
| `proxy_timeout` | 120s | 空闲连接保持时间。gRPC 连接为长连接，客户端与 Nacos 维持心跳（默认 5s）。此值应远大于心跳间隔，避免频繁重连 |

**gRPC 长连接代理特殊考量**：

- **连接迁移问题**：当 Nginx reload（`nginx -s reload`）时，旧的 worker 进程优雅退出，但已建立的 gRPC 连接不会中断——Nginx stream 代理的 TCP 连接在 reload 过程中保持透明转发。但如果是 `nginx -s stop && nginx`（重启），所有 gRPC 连接会断开，客户端需要重连
- **负载不均问题**：每个客户端只有 1 条 gRPC 连接，假设 300 个客户端连接 3 节点 Nacos，`least_conn` 理论上每个节点承载约 100 条连接。但新客户端加入时，`least_conn` 将分配给当前连接数最少的节点，长期运行后趋于均衡
- **避免 sticky 绑定**：不应该 `ip_hash` gRPC 连接——某个网段的客户端永远绑定某个 Nacos 节点，长期运行可能导致单节点过载

### 完整 Nginx 配置（http + stream 合并）

将上述配置合并为一个完整的 `/etc/nginx/nginx.conf`：

```nginx
# /etc/nginx/nginx.conf - Nacos 2.5.3 集群 Nginx 完整配置

# =========================================================================
# 全局配置
# =========================================================================
user nginx;
worker_processes auto;          # 自动匹配 CPU 核心数
error_log /var/log/nginx/error.log warn;
pid /var/run/nginx.pid;

# 事件模块配置
# worker_connections 上限受 OS 文件描述符限制（ulimit -n）
events {
    worker_connections 65535;      # 每个 worker 最大并发连接数
    use epoll;                    # Linux epoll I/O 模型
    multi_accept on;              # 一次接受所有新连接
}

# =========================================================================
# HTTP 块：HTTP 反向代理（L7 - Nacos HTTP API & 控制台）
# =========================================================================
http {
    include       /etc/nginx/mime.types;
    default_type  application/octet-stream;

    # 日志格式
    log_format main '$remote_addr - $remote_user [$time_local] "$request" '
                    '$status $body_bytes_sent "$http_referer" '
                    '"$http_user_agent" "$http_x_forwarded_for" '
                    '$request_time $upstream_response_time';

    access_log /var/log/nginx/access.log main;

    # 性能优化
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    keepalive_requests 1000;

    # Gzip 压缩（控制台静态资源）
    gzip on;
    gzip_min_length 1k;
    gzip_comp_level  uniqu
    gzip_types text/plain application/json application/javascript text/css;

    # =====================================================================
    # HTTP upstream（Nacos HTTP API 负载均衡）
    # =====================================================================
    upstream nacos_http {
        least_conn;
        server 192.168.1.101:8848 max_fails=3 fail_timeout=30s weight=1;
        server 192.168.1.102:8848 max_fails=3 fail_timeout=30s weight=1;
        server 192.168.1.103:8848 max_fails=3 fail_timeout=30s weight=1;
        keepalive 64;
        keepalive_timeout 120s;
        keepalive_requests 10000;
    }

    # =====================================================================
    # HTTP server（监听 80 / 443）
    # =====================================================================
    server {
        listen 80;
        server_name nacos.example.com;

        client_max_body_size 10m;

        proxy_buffer_size 128k;
        proxy_buffers 4 256k;
        proxy_busy_buffers_size 256k;

        proxy_connect_timeout 5s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Connection "";

        # Nacos HTTP API 代理
        location /nacos {
            proxy_pass http://nacos_http;
            proxy_next_upstream error timeout http_500 http_502 http_503;
            proxy_next_upstream_tries 2;
        }

        # 健康检查
        location /health {
            return 200 "OK\n";
            add_header Content-Type text/plain;
        }
    }
}

# =========================================================================
# Stream 块：gRPC TCP Stream 代理（L4 - Nacos gRPC 双向流）
# 必须定义在 http 块外部！
# =========================================================================
stream {
    log_format stream '$remote_addr [$time_local] '
                     '$protocol $status $bytes_sent $bytes_received '
                     '$session_time "$upstream_addr" '
                     '"$upstream_bytes_sent" "$upstream_bytes_received" "$upstream_connect_time"';

    access_log /var/log/nginx/nacos-grpc-access.log stream;
    error_log  /var/log/nginx/nacos-grpc-error.log warn;

    upstream nacos_grpc {
        least_conn;
        server 192.168.1.101:9848 max_fails=3 fail_timeout=30s;
        server 192.168.1.102:9848 max_fails=3 fail_timeout=30s;
        server 192.168.1.103:9848 max_fails=3 fail_timeout=30s;
    }

    server {
        listen 9848;
        proxy_pass nacos_grpc;
        proxy_connect_timeout 5s;
        proxy_timeout 120s;
        proxy_next_upstream on;
        proxy_next_upstream_tries 2;
    }
}
```

### Nginx 启动与热加载

```bash
# 测试配置文件语法
nginx -t
# 预期输出：nginx: configuration file /etc/nginx/nginx.conf test is successful

# 启动 Nginx（若已运行则 reload）
nginx -s reload

# 验证 Nacos HTTP 代理
curl -s http://nacos.example.com/nacos/v1/console/server/state
# 预期返回 JSON：{"standalone_mode":"cluster", ...}

# 验证 gRPC 端口监听（Nginx 在 9848 监听）
ss -tlnp | grep 9848
# 预期输出：LISTEN  0  128  0.0.0.0:9848 ...

# 验证 upstream 状态（需要 nginx-mod-stream 模块）
# 观察 gRPC 连接分布
ss -tnp | grep ':9848' | grep ESTABLISHED | wc -l
```

### Nginx 被动健康检查行为验证

```bash
# 1. 正常状态：3 节点均 UP
curl -s http://nacos.example.com/nacos/v1/core/cluster/nodes

# 2. 模拟 Node 1 宕机（停掉 Node 1 的 Nacos 进程）
ssh 192.168.1.101 "kill -STOP \$(pgrep -f nacos)"

# 3. 连续发送 4 次请求（触发 max_fails=3）
for i in 1 2 3 4; do
  curl -s -o /dev/null -w "$i: HTTP %{http_code}\n" \
    http://nacos.example.com/nacos/v1/console/server/state
done
# 预期：前 去打三次可能路由到 Node 1 失败，第四次 Nginx 将 Node 1 标记为 DOWN

# 4. 检查 Nginx error 日志（确认 fail_timeout 生效）
tail -20 /var/log/nginx/error.log | grep "upstream timed out"

# 5. 恢复 Node 1（继续进程）
ssh 192.168.1.101 "kill -CONT \$(pgrep -f nacos)"
# 等待 30s（fail_timeout 过期），Nginx 自动探测恢复
```

### Trade-off 分析

**Nginx 代理 vs 客户端直连**：

| 维度 | Nginx 代理 | 客户端直连 | 分析 |
|------|-----------|-----------|------|
| 入口地址 | 单一 VIP/域名 | 多个 IP 列表 | Nginx 简化客户端配置 |
| 负载均衡 | least_conn/ip_hash/加权 | 随机/轮询 | Nginx 策略更灵活 |
| 故障转移 | 主动健康检查 + passive | 心跳超时（15s） | Nginx 故障感知更快（秒级 vs 15s+） |
| 扩展性 | 后端节点变更透明 | 客户端需逐一更新 | Nginx 运维成本更低 |
| TLS 终结 | Nginx 层集中配置 | 每节点各自配置 | Nginx 简化证书管理 |
| 额外延迟 | +1 跳（< 1ms 同机房） | 无额外跳 | 同机房延迟增加极低 |
| 单点风险 | Nginx 自身成为单点 | 无单点 | 需要 Nginx HA（Keepalived/多实例） |

**HTTP L7 vs Stream L4 代理对比**：

| 维度 | HTTP 代理 (L7) | TCP Stream 代理 (L4) |
|------|---------------|-------------------|
| 协议解析 | 理解 HTTP/1.1 语义 | 不解包，仅转发 TCP 数据 |
| gRPC 兼容 | ❌ 破坏 HTTP/2 多路复用 | ✅ 透明转发 HTTP/2 帧 |
| URL 路由 | ✅ 按 path/host 路由 | ❌ 仅端口级路由 |
| 请求改写 | ✅ 可改写 Header/URL | ❌ 不可改写 |
| 性能开销 | 中（HTTP 解析） | 低（TCP 转发） |
| 适用场景 | HTTP API（8848） | gRPC 双向流（9848） |

**Nginx vs 其他负载均衡方案**：

| 方案 | 优势 | 劣势 | 适用场景 |
|------|------|------|---------|
| Nginx | 成熟稳定、性能高、配置简单 | 开源版无主动健康检查 | 中小规模集群 |
| Nginx Plus | 主动健康检查、动态 upstream | 付费商业版 | 企业级大规模 |
| HAProxy | TCP/HTTP 代理性能极佳 | 配置语法略复杂 | 超大规模集群 |
| Envoy | 原生支持 gRPC L7 代理、xDS | 运维复杂度高 | 云原生 Service Mesh |
| K8s Service | K8s 原生、自动发现 | 仅 L4 代理，无 HTTP 路由 | K8s 部署场景 |

### 设计模式分析

Nginx 在此架构中体现多种设计模式：

1. **反向代理模式（Reverse Proxy）**：Nginx 作为后端服务器的统一门面，隐藏内部拓扑。客户端无需知道 Nacos 集群具体有几个节点、IP 是多少——这是经典的 Facade 模式在网络层的应用。

2. **责任链模式（Chain of Responsibility）**：请求经过 Nginx 的多层模块处理——`access_log` → `proxy_set_header` → `proxy_pass` → `proxy_next_upstream`。每个模块独立处理一个关注点。

3. **策略模式（Strategy）**：负载均衡算法（`least_conn`、`ip_hash`、加权轮询）可独立替换，不影响 server 块其他配置。Nginx 通过 `upstream` 块内的算法声明将策略与上下文解耦。

4. **健康检查探针模式（Health Check Probe）**：`max_fails` + `fail_timeout` 构成经典的 Circuit Breaker 模式——阈值内连续失败触发熔断（标记 DOWN），冷却期后自动探测恢复（Half-Open → Closed）。

### 小结

- Nginx 为 Nacos 2.5.3 集群提供 HTTP L7 反向代理（8848）和 gRPC TCP Stream L4 代理（9848）双通道负载均衡
- HTTP 代理用于控制台/API 访问，使用 `http` 块 `upstream` + `proxy_pass`，支持 URL 路由、Header 改写、长连接池
- gRPC 代理必须使用 `stream` 块——Nginx 在 L4 层透明转发 TCP 数据包，不能使用 HTTP 反向代理（后者破坏 gRPC HTTP/2 多路复用）
- `least_conn` 算法在 gRPC 场景等价「最少客户端数」分配（每个客户端只有 1 条 gRPC 长连接）
- 被动健康检查通过 `max_fails` + `fail_timeout` 实现类 Circuit Breaker 模式：连续失败达阈值→熔断→冷却期后自动恢复
- Nginx 自身单点风险需通过 Keepalived + VIP 漂移或 DNS 多 A 记录缓解

---

## 10.5 5 节点集群架构：3 Raft + 2 Learner 节点的读写分离优势

### 设计背景

3 节点集群是 Nacos 生产环境的最低高可用标准——容忍 1 节点故障。但大规模微服务场景（>1000 服务实例）或对可用性要求极高的关键业务系统，3 节点的容错窗口（仅容忍 1 节点故障）可能不够：如果同时发生 2 节点宕机（例如滚动重启期间 1 节点意外宕机），集群将不可用。

Nacos 2.5.3 基于 JRaft 协议，支持在标准 Raft 节点（Voter）之外增加 **Learner（学习者）** 节点。Learner 节点不参与 Leader 选举投票，不参与日志提交 quorum 计数，仅被动从 Leader 同步日志。这带来了关键架构优势：

1. **提高容错能力**：5 节点 = 3 Voter + 2 Learner → 容忍 2 节点故障（如果故障的是 2 个 Learner，集群正常运行；如果故障的是 1 Voter + 1 Learner，仍然有 2/3 Voter 满足 quorum）
2. **读写分离**：Learner 节点可承载只读请求（服务发现查询、配置查询），分流 Voter 节点的读压力
3. **跨地域部署**：Learner 节点部署在异地数据中心，同步日志但不参与本地 Leader 选举（避免跨地域 Raft 选举延迟）
4. **滚动重启安全**：5 节点集群在滚动重启时，可同时摘除 iatively 1 节点升级，仍有 4 节点运行（3 Voter + 1 Learner），比 3 节点集群（滚动重启时只剩 2 节点，处于 quorum 边界）更安全

Nacos 2.5.3 在 `application.properties` 中通过 `nacos.core.member.weight` 和 `nacos.core.member.learner=true` 控制节点角色。

### 核心架构关系图

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                  Nacos 5 节点集群：3 Raft Voter + 2 Learner 架构            │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│                        ┌─────────────────────────┐                         │
│                        │      Nginx (VIP)       │                         │
│                        │  HTTP :8848 / gRPC :9848│                         │
│                        └───────────┬─────────────┘                         │
│                                    │                                      │
│          ┌───────────┬───────────┼───────────┬───────────┐             │
│          │           │           │           │           │             │
│     ┌────▼───┐ ┌───▼────┐ ┌──▼────┐ ┌───▼────┐ ┌──▼─────┐        │
│     │ Node 1 │ │ Node 2 │ │ Node 3│ │ Node 4 │ │ Node 5 │        │
│     │ Voter  │ │ Voter  │ │ Voter │ │Learner │ │Learner │        │
│     │Leader  │ │Follower│ │Follower│ │Follower│ │Follower│        │
│     └───┬────┘ └───┬────┘ └──┬────┘ └───┬────┘ └───┬─────┘        │
│         │           │         │           │           │                │
│         │     JRaft Log Replication (all 5 nodes)                   │
│         │◄────────┼────────┼──────────┼───────────┤                │
│         │           │         │           │           │                │
│    ┌────▼──────────▼─────────▼───────────▼───────────▼────┐       │
│    │                    MySQL 数据库                            │       │
│    │  • 元数据持久化（config_info / users / roles / etc）│       │
│    │  • 所有节点共享同一个 MySQL 实例                     │       │
│    └───────────────────────────────────────────────────────────┘       │
│                                                                          │
│   Raft Quorum: 3/5 = ⌊5/2⌋+1 = 3 → 需要至少 2 个 Voter 在线       │
│   (Learner 不参与 quorum 计数)                                        │
│                                                                          │
│   读写分离策略：                                                        │
│   • Voter 节点：处理读写请求（写转发至 Leader）                       │
│   • Learner 节点：仅处理本地读请求（服务发现查询 / 配置查询）        │
│   • gRPC 负载均衡：Least Connection → 自然分流                        │
│                                                                          │
│   容错矩阵：                                                              │
│   • 2 Learner 宕机：集群正常（3/3 Voter online）                       │
│   • 1 Voter + 1 Learner 宕机：集群正常（2/3 Voter online ≥ quorum）  │
│   • 2 Voter 宕机：集群不可用（1/3 Voter < quorum）                  │
│   • 1 Voter (Leader) 宕机：触发 Leader 选举，新 Leader 产生          │
│                                                                          │
│                  图 10-13：5 节点集群 3 Voter + 2 Learner 架构             │
└────────────────────────────────────────────────────────────────────────────┘
```

### Learner 节点角色与工作机制

#### Learner 与 Follower 的核心差异

| 维度 | Voter (Follower) | Learner |
|------|-----------------|--------|
| 参与 Leader 选举 | ✅ 有投票权 | ❌ 无投票权 |
| 参与 quorum 计数 | ✅ 计入多数派 | ❌ 不计入 |
| 接受 Leader 日志复制 | ✅ 接受 | ✅ 接受 |
| 提交日志 | ✅ 直接提交 | ✅ 收到 Leader commit 后提交 |
| 可成为 Leader | ✅ 可被选为 Leader | ❌ 永远不能成为 Leader |
| 处理读请求 | ✅ 本地读 | ✅ 本地读（最终一致） |
| 处理写请求 | ✅ 转发至 Leader | ✅ 转发至 Leader |
| 适用场景 | 核心节点（选举/提交） | 读写分离 / 异地灾备 |

#### Nacos 2.5.3 中 Learner 的 JRaft 实现

在 JRaft 中，Learner 节点的核心行为由 `Replicator` 类控制。Leader 向 Voter 和 Learner 发送日志复制的逻辑相同，区别在于 Leader 提交日志时只等待 Voter 的 quorum 确认，不等待 Learner：

源码位置：`consistency/src/main/java/com/alibaba/nacos/consistency/cp/JRaftProtocol.java`

```java
// JRaft 节点角色枚举
public enum PeerRole {
    LEADER,      // Leader 节点
    FOLLOWER,    // Voter 节点（有投票权）
    LEARNER      // Learner 节点（无投票权）
}
```

在 Raft 共识算法中，Leader 提交日志条目需获得多数 Voter 节点的确认（quorum = ⌊N_Voter/2⌋ + 1）。Learner 不参与此 quorum 计数，但 Leader 仍会向 Learner 发送日志条目以保持数据同步。

### 5 节点集群部署配置

#### Step 1：MySQL 数据库初始化

5 节点集群共享同一个 MySQL 实例（或 MySQL 主从集群）。数据库初始化 SQL 与 3 节点相同：

```bash
# 下载 Nacos 2.5.3 发行版
wget https://github.com/alibaba/nacos/releases/download/2.5.3/nacos-server-2.5.3.tar.gz
tar -xzf nacos-server-2.5.3.tar.gz

# 执行 MySQL 初始化脚本
mysql -h 192.168.1.100 -u root -p < nacos/conf/mysql-schema.sql

# 创建 nacos 用户并授权
mysql -h 192.168.1.100 -u root -p -e "
CREATE USER 'nacos'@'%' IDENTIFIED BY 'Nacos@2025';
GRANT ALL PRIVILEGES ON nacos_config.* TO 'nacos'@'%';
FLUSH PRIVILEGES;
"
```

#### Step 2：cluster.conf 配置

`cluster.conf` 列出所有 5 个节点的 IP 和 Raft 通信端口（7848）：

```
# cluster.conf（每个节点的 ${NACOS_HOME}/conf/cluster.conf 内容相同）
# 格式：IP:PORT（PORT 是 Raft 通信端口，默认 7848）
192.168.1.101:7848
192.168.1.102:7848
192.168.1.103:7848
192.168.1.104:7848
192.168.1.105:7848
```

#### Step 3：application.properties 配置

**Voter 节点配置（Node 1-3，以 Node 1 为例）**：

```properties
# ============================================================
# Voter 节点配置（Node 1: 192.168.1.101）
# nacos/conf/application.properties
# ============================================================

# HTTP 端口
server.port=8848

# MySQL 数据源配置
spring.datasource.platform=mysql
db.num=1
db.url.0=jdbc:mysql://192.168.1.100:3306/nacos_config?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=Nacos@2025

# Raft 端口
nacos.core.member.raft.port=7848
```

**Learner 节点配置（Node 4-5，以 Node 4 为例）**：

```properties
# ============================================================
# Learner 节点配置（Node 4: 192.168.1.104）
# nacos/conf/application.properties
# ============================================================

# HTTP 端口
server.port=8848

# MySQL 数据源配置（与 Voter 相同，共享同一数据库）
spring.datasource.platform=mysql
db.num=1
db.url.0=jdbc:mysql://192.168.1.100:3306/nacos_config?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=Nacos@2025

# Raft 端口
nacos.core.member.raft.port=7848

# ==================================================================
# Learner 节点关键配置
# ==================================================================
# 标记为 Learner 节点（Voter 无需配置此项或设置为 false）
nacos.core.member.learner=true

# 节点权重（可选，默认 1.0）
# 权重可用于 Nginx upstream 的加权轮询
nacos.core.member.weight=0.8
```

#### Step 4：启动 5 节点集群

```bash
# ====================================================
# 启动顺序建议：先启动 3 个 Voter 节点，再启动 2 个 Learner
# ====================================================

# Node 1 (Voter) - 192.168.1.101
ssh 192.168.1.101 "cd /opt/nacos && bash bin/startup.sh"

# Node 2 (Voter) - 192.168.1.102
ssh 192.168.1.102 "cd /opt/nacos && bash bin/startup.sh"

# Node 3 (Voter) - 192.168.1.103
ssh 192.168.1.103 "cd /opt/nacos && bash bin/startup.sh"

# 等待 Voter 集群形成 Leader
sleep 15

# Node 4 (Learner) - 192.168.1.104
ssh 192.168.1.104 "cd /opt/nacos && bash bin/startup.sh"

# Node 5 (Learner) - 192.168.1.105
ssh 192.168.1.105 "cd /opt/nacos && bash bin/startup.sh"

# 验证集群状态
curl -X GET 'http://192.168.1.101:8848/nacos/v1/core/cluster/nodes'
```

**预期集群状态输出**：

```json
[
  {
    "ip": "192.168.1.101",
    "port": 8848,
    "state": "UP",
    "extendInfo": {
      "raftPort": "7848",
      "role": "LEADER",
      "learner": "false"
    }
  },
  {
    "ip": "192.168.1.102",
    "port": 8848,
    "state": "UP",
    "extendInfo": {
      "raftPort": "7848",
      "role": "FOLLOWER",
      "learner": "false"
    }
  },
  {
    "ip": "192.168.1.103",
    "port": 8848,
    "state": "UP",
    "extendInfo": {
      "raftPort": "7848",
      "role": "FOLLOWER",
      "learner": "false"
    }
  },
  {
    "ip": "192.168.1.104",
    "port": 8848,
    "state": "UP",
    "extendInfo": {
      "raftPort": "7848",
      "role": "FOLLOWER",
      "learner": "true"
    }
  },
  {
    "ip": "192.168.1.105",
    "port": 8848,
    "state": "UP",
    "extendInfo": {
      "raftPort": "7848",
      "role": "FOLLOWER",
      "learner": "true"
    }
  }
]
```

### 故障转移场景验证

#### 场景 1：两个 Learner 节点同时宕机

```bash
# 停止 Node 4 + Node 5（两个 Learner）
ssh 192.168.1.104 "kill -STOP \$(pgrep -f nacos)"
ssh 192.168.1.105 "kill -STOP \$(pgrep -f nacos)"

# 验证：3 个 Voter 仍然在线，集群正常运行
curl -X GET 'http://192.168.1.101:8848/nacos/v1/core/cluster/nodes'
# 预期：Node 4/5 显示 DOWN，Node 1/2/3 显示 UP，集群可用
```

#### 场景 2：1 Voter + 1 Learner 同时宕机

```bash
# 停止 Node ierp3 (Voter) + Node 4 (Learner)
ssh 192.168.1.103 "kill -STOP \$(pgrep -f nacos)"
# （Node 4 已在场景 1 中停止）

# 验证：仍有 2/3 Voter 在线 ≥ quorum (⌊3/2⌋+1=2)，集群仍可用
curl -X GET 'http://192.168.1.101:8848/nacos/v1/core/cluster/nodes'
# 预期：集群在线，Leader 仍是 Node 1
```

#### 场景 3：Leader Voter 宕机（触发选举）

```bash
# 停止 Node 1（当前 Leader）
ssh 192.168.1.101 "kill -STOP \$(pgrep -f nacos)"

# 观察 Node 2 日志：触发 Leader 选举
ssh 192.168.1.102 "tail -30 /opt/nacos/logs/nacos-cluster.log | grep -E 'Leader|election'"
# 预期日志：
# INFO [JRaft] Leader heartbeat timeout, starting election
# INFO [JRaft] New leader has been elected: [192.168.1.102:7848]

# 验证新 Leader
curl -X GET 'http://192.168.1.102:8848/nacos/v1/core/cluster/nodes'
# 预期：Node 2 (Voter) 成为新 Leader
```

### Trade-off 分析

**3 节点 vs 5 节点（3 Voter + 2 Learner）对比**：

| 维度 | 3 节点 | 5 节点（3 Voter + 2 Learner） | 分析 |
|------|--------|-------------------------------|------|
| 最大容错 | 1 节点故障 | 2 节点故障（若故障的是 2 Learner 或 1 Voter+1 Learner） | 5 节点容错窗口更大 |
| Voter 容错 | 1/3 Voter 故障 | 3 节点模式下相同（Voter 仍只有 3 个） | Voter 容错未增加 |
| 滚动重启安全 | 仅剩 2 节点（quorum 边界） | 仍有 4 节点（安全冗余） | 5 节点滚动重启更安全 |
| 硬件成本 | 3 台服务器 | 5 台服务器（+66%） | 成本与容错能力线性增长 |
| MySQL 连接数 | 3 × 连接池 | 5 × 连接池 | 5 节点对 MySQL 压力增大 66% |
| 日志复制开销 | Leader → 2 Follower | Leader → 4 节点（2 Voter + 2 Learner） | 5 节点网络开销更大 |
| 读吞吐量 | 所有节点处理读 | Learner 分担读请求 | 5 节点整体读吞吐更高 |
| 适用场景 | 中小规模（<500 服务） | 大规模（>1000 服务）或关键业务 | 根据规模和可用性需求选择 |

**Learner 模式的适用边界**：

1. **不适合替代 Voter 数量不足**：如果 Raft 集群只有 丛 个 Voter（总共 2 个 Voter + 1 Learner），Learner 不参与 quorum，实际容错能力与 2 Voter 相同——任何 1 个 Voter 宕机集群即不可用
2. **跨地域部署注意网络延迟**：Learner 部署在异地数据中心时，日志复制的网络延迟增大（Leader → Learner RTT），但 Learner 不参与 quorum，不会拖慢日志提交
3. **Learner 读到的数据可能滞后**：Leader 向 Learner 发送日志条目与 Leader 提交日志之间存在时间窗口，Learner 可能读到尚未提交的日志（若 Learner 应用日志比 commit 快）；生产环境若需要强一致读，应读 Leader 或配置 `readIndex` 安全读

### 设计模式分析

1. **读写分离模式（Read/Write Splitting）**：Voter 节点处理读写请求（写转发至 Leader），Learner 节点专门承载只读请求——将读流量从 Voter 分流，降低 Voter 节点的 CPU/内存压力。这是 CQRS（Command Query Responsibility Segregation）模式在集群拓扑层面的应用。

2. **观察者模式（Observer Pattern）**：Learner 节点在 Raft 协议中扮演 Observer 角色——被动接收 Leader 日志复制但不参与决策（投票）。类似 ZooKeeper 中的 Observer 节点（ZooKeeper 的 Learner 同样不参与投票）。

3. **代理模式（Proxy Pattern）**：Nginx 作为所有节点的统一入口，将读写请求路由至合适的节点（Voter for write，Learner for read），隐藏后端拓扑复杂性。

### 小结

- Nacos 2.5.3 支持 5 节点集群：3 Voter + 2 Learner，Learner 通过 `nacos.core.member.learner=true` 声明
- Learner 不参与 Leader 选举和 quorum 计数，仅被动同步 Leader 日志，适用于读写分离和跨地域部署
- Voter quorum 仍为 3/5（⌊3/2⌋+1=2），Learner 不计入 quorum——2 Voter 宕机仍导致集群不可用
- 5 节点相比 3 节点优势：(1) 滚动重启安全(2) 最多容忍 2 节点故障（若故障节点是 2 Learner 或 1 Voter+1 Learner）(3) Learner 分担读流量
- 局限性：(1) Voter 容错未增加（仍为 3 个 Voter）(2) 硬件成本 +66% (3) MySQL 连接压力增大

---

## 10.6 Kubernetes StatefulSet 完整 YAML（Headless Service + PodAntiAffinity + PVC）

### 设计背景

随着云原生技术的普及，越来越多的企业将 Nacos 部署到 Kubernetes（K8s）集群中，利用 K8s 的自动编排能力简化运维。但 Nacos 作为有状态应用，不能简单地用 Deployment 部署——Pod 重启后 IP 变化会导致集群 `cluster.conf` 失效、数据丢失。因此必须使用 **StatefulSet** 资源类型。

Nacos 在 K8s 上部署的核心挑战：

1. **网络标识稳定性**：Nacos 集群成员通过 IP:PORT 互相识别（`cluster.conf`），Pod 重启后 IP 变化会导致集群分裂。必须使用 Headless Service + StatefulSet 为每个 Pod 分配稳定的 DNS 名称（`nacos-{0..N-1}.nacos-headless.default.svc.cluster.local`）
2. **持久化存储**：Nacos 的 Derby 嵌入式数据库（单机模式）或日志文件需要持久化存储，Pod 漂移后数据不能丢失。通过 PVC（PersistentVolumeClaim）绑定 PV
3. **反亲和性调度**：多个 Nacos Pod 调度到同一 Node 违背高可用原则——Node 宕机导致多个 Nacos 实例同时失效。通过 PodAntiAffinity 确保 Pod 分散在不同 Node
4. **配置管理**：`application.properties` 和 `cluster.conf` 内容在所有 Pod 中相同，使用 ConfigMap 统一管理

Nacos 2.5.3 在 K8s 上推荐使用 `StatefulSet` + `Headless Service` + `PVC` + `ConfigMap` 的组合方案。

### 核心架构关系图

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                Nacos on Kubernetes (StatefulSet) 架构                            │
├────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│                          ┌─────────────────────────┐                             │
│                          │   K8s Service (ClusterIP) │                             │
│                          │   nacos-service:8848     │                             │
│                          └───────────┬─────────────┘                             │
│                                      │                                        │
│        ┌─────────────────────────────┼─────────────────────────────┐          │
│        │                             │                             │          │
│   ┌────▼────┐                ┌────▼────┐                ┌────▼────┐         │
│   │ nacos-0 │                │ nacos-1 │                │ nacos-2 │         │
│   │ Pod     │                │ Pod     │                │ Pod     │         │
│   │ :8848   │◄──────────────│ :8848   │◄──────────────│ :8848   │         │
│   │ :9848   │  JRaft RPC    │ :9848   │  JRaft RPC    │ :9848   │         │
│   └────┬────┘                └────┬────┘                └────┬────┘         │
│        │ PVC                     │ PVC                     │ PVC              │
│   ┌────▼────┐                ┌────▼────┐                ┌────▼────┐         │
│   │   PV    │                │   PV    │                │   PV    │         │
│   │ (10Gi)  │                │ (10Gi)  │                │ (10Gi)  │         │
│   └─────────┘                └─────────┘                └─────────┘         │
│                                                                                │
│   ┌─────────────────────────────────────────────────────────────────────────┐    │
│   │                     K8s 资源关系                                       │    │
│   │                                                                     │    │
│   │  StatefulSet (nacos)                                                │    │
│   │    ├── Pod nacos-0    ← Headless Service DNS:                     │    │
│   │    ├── Pod nacos-1        nacos-{0..N-1}.nacos-headless.default    │    │
│   │    └── Pod nacos-2    ← PodAntiAffinity: 每个 Node 最多 1 个 Pod │    │
│   │                                                                     │    │
│   │  Headless Service (nacos-headless)                                 │    │
│   │    └── ClusterIP: None (Headless)                                  │    │
│   │                                                                     │    │
│   │  ClusterIP Service (nacos-service)                                  │    │
│   │    └── ClusterIP: 10.x.x.x (Internal LB)                           │    │
│   │                                                                     │    │
│   │  ConfigMap (nacos-config)                                           │    │
│   │    ├── application.properties                                      │    │
│   │    └── cluster.conf                                                │    │
│   └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
│                      图 10-14：K8s StatefulSet 部署架构                         │
└────────────────────────────────────────────────────────────────────────────────────┘
```

**关键设计要点**：

1. **Headless Service (`clusterIP: None`)**：不为 Pod 分配虚拟 ClusterIP，DNS 直接解析到 Pod IP。客户端通过 `nacos-0.nacos-headless.default.svc.cluster.local` 访问特定 Pod
2. **ClusterIP Service**：为集群内客户端提供统一入口（Nginx Ingress 或其他微服务），负载均衡到所有就绪 Pod
3. **PodAntiAffinity (`requiredDuringSchedulingIgnoredDuringExecution`)**：强制每个 Node 最多调度 1 个 Nacos Pod，确保 Pod 分散在不同物理节点
4. **PVC 模板 `volumeClaimTemplates`**：StatefulSet 为每个 Pod 自动创建独立的 PVC（`data-nacos-0`、`data-nacos-1`），Pod 重建后重新绑定原 PVC，数据不丢失

### K8s 资源完整 YAML 配置

#### 1. Namespace（命名空间隔离）

```yaml
# 00-namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: nacos
```

#### 2. ConfigMap（统一配置管理）

```yaml
# 01-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nacos-config
  namespace: nacos
data:
  # ===================================================================
  # application.properties - Nacos 服务配置
  # ===================================================================
  application.properties: |
    # HTTP 端口
    server.port=8848

    # 数据库配置（使用外部 MySQL）
    spring.datasource.platform=mysql
    db.num=1
    db.url.0=jdbc:mysql://mysql-service.nacos.svc.cluster.local:3306/nacos_config?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai
    db.user.0=nacos
    db.password.0=Nacos@2025

    # Raft 端口
    nacos.core.member.raft.port=7848

    # 鉴权配置（生产环境建议开启）
    nacos.core.auth.enabled=true
    nacos.core.auth.default.token.secret.key=VGhpc0lzQVNlY3JldEtleUZvck5hY29zQXV0aA==
    nacos.core.auth.server.identity.key=bmFjb3NTZXJ2ZXJJZGVudGl0eUtleQ==

  # ===================================================================
  # cluster.conf - 集群成员列表
  # 使用 Headless Service DNS 域名确保 Pod 重启后成员关系稳定
  # ===================================================================
  cluster.conf: |
    nacos-0.nacos-headless.nacos.svc.cluster.local:7848
    nacos-1.nacos-headless.nacos.svc.cluster.local:7848
    nacos-2.nacos-headless.nacos.svc.cluster.local:7848
```

**DNS 域名说明**：`<pod-name>.<headless-service>.<namespace>.svc.cluster.local`
- `nacos-0`：StatefulSet Pod 名称（StatefulSet 自动分配序号 0, 1, 2...）
- `nacos-headless`：Headless Service 名称
- `nacos`：Namespace
- `svc.cluster.local`：K8s 集群内部 DNS 后缀

#### 3. Headless Service（Pod DNS 解析）

```yaml
# 02-headless-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: nacos-headless
  namespace: nacos
  labels:
    app: nacos
spec:
  # Headless Service：clusterIP 设置为 None
  clusterIP: None
  selector:
    app: nacos
  ports:
    - name: http
      port: 8848
      targetPort: 8848
      protocol: TCP
    - name: grpc
      port: 9848
      targetPort: 9848
      protocol: TCP
    - name: raft
      port: 7848
      targetPort: 7848
      protocol: TCP
  # 发布不准备就绪的 Pod（允许 Pod 启动过程中 DNS 可解析）
  publishNotReadyAddresses: true
```

**`publishNotReadyAddresses: true` 是关键配置**：Nacos 集群启动时，第一个 Pod (`nacos-0`) 需要 DNS 解析其他 Pod 成员（即使其他 Pod 尚未就绪）来形成集群。如果不设置此项，未就绪 Pod 的 DNS 记录不会发布，导致 `nacos-0` 无法解析 `nacos-1` 和 `nacos-2`，集群启动失败。

#### 4. ClusterIP Service（内部访问入口）

```yaml
# 03-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: nacos-service
  namespace: nacos
  labels:
    app: nacos
spec:
  type: ClusterIP
  selector:
    app: nacos
  ports:
    - name: http
      port: 8848
      targetPort: 8848
      protocol: TCP
    - name: grpc
      port: 9848
      targetPort: 9848
      protocol: TCP
  sessionAffinity: None
```

#### 5. StatefulSet（核心资源定义）

```yaml
# 04-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nacos
  namespace: nacos
  labels:
    app: nacos
spec:
  # StatefulSet 名称 → Pod 名称模式：nacos-0, nacos-1, nacos-2
  serviceName: nacos-headless  # 绑定 Headless Service（用于 DNS 解析）
  replicas: 3                # 集群节点数
  podManagementPolicy: Parallel  # 并行启动所有 Pod（而非串行启动）

  # Pod 选择器
  selector:
    matchLabels:
      app: nacos

  # =================================================================
  # Pod 模板
  # =================================================================
  template:
    metadata:
      labels:
        app: nacos
    spec:
      # =============================================================
      # PodAntiAffinity：每个 Node 最多运行 1 个 Nacos Pod
      # =============================================================
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app
                    operator: In
                    values:
                      - nacos
              topologyKey: kubernetes.io/hostname

      # =============================================================
      # InitContainer：等待 MySQL 服务就绪
      # =============================================================
      initContainers:
        - name: wait-for-mysql
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              echo "Waiting for MySQL to be ready..."
              until nslookup mysql-service.nacos.svc.cluster.local; do
                echo "MySQL DNS not yet resolvable, sleeping 2s..."
                sleep 2
              done
              echo "MySQL service found, starting Nacos..."

      # =============================================================
      # Nacos 容器
      # =============================================================
      containers:
        - name: nacos
          image: nacos/nacos-server:v2.5.3
          imagePullPolicy: IfNotPresent

          ports:
            - name: http
              containerPort: 8848
              protocol: TCP
            - name: grpc
              containerPort: 9848
              protocol: TCP
            - name: raft
              containerPort: 7848
              protocol: TCP

          # =========================================================
          # 环境变量
          # =========================================================
          env:
            # JVM 堆内存（根据容器 limit 调整）
            - name: JVM_XMS
              value: "2g"
            - name: JVM_XMX
              value: "2g"
            # 模式：cluster（集群模式）
            - name: MODE
              value: cluster
            # Nacos 控制台路径
            - name: NACOS_SERVERS
              value: "nacos-0.nacos-headless.nacos.svc.cluster.local:8848 nacos-1.nacos-headless.nacos.svc.cluster.local:8848 nacos-2.nacos-headless.nacos.svc.cluster.local:8848"
            # Pod 名称（注入为 Nacos 服务标识）
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            # Pod IP（注入为 Nacos 节点 IP）
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP

          # =========================================================
          # 资源限制与请求
          # =========================================================
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "3Gi"

          # =========================================================
          # 存活探针（Liveness Probe）
          # =========================================================
          livenessProbe:
            httpGet:
              path: /nacos/v1/console/server/state
              port: 8848
            initialDelaySeconds: 60
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 3

          # =========================================================
          # 就绪探针（Readiness Probe）
          # =========================================================
          readinessProbe:
            httpGet:
              path: /nacos/v1/console/server/state
              port: 8848
            initialDelaySeconds: 30
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 3

          # =========================================================
          # 挂载 ConfigMap 中的配置文件
          # =========================================================
          volumeMounts:
            - name: nacos-config
              mountPath: /home/nacos/conf/application.properties
              subPath: application.properties
            - name: nacos-config
              mountPath: /home/nacos/conf/cluster.conf
              subPath: cluster.conf
            # PVC 数据持久化目录
            - name: data
              mountPath: /home/nacos/data

      # =============================================================
      # ConfigMap Volumes
      # =============================================================
      volumes:
        - name: nacos-config
          configMap:
            name: nacos-config
            defaultMode: 0644

  # =================================================================
  # PVC 模板：为每个 Pod 自动创建独立的 PVC
  # =================================================================
  volumeClaimTemplates:
    - metadata:
        name: data
        namespace: nacos
        labels:
          app: nacos
      spec:
        accessModes:
          - ReadWriteOnce
        storageClassName: standard  # 根据实际 K8s 集群 StorageClass 调整
        resources:
          requests:
            storage: 20Gi  # Nacos 日志 + Derby 数据持久化
```

### YAML 关键配置详解

#### PodAntiAffinity 策略选择

K8s 提供两种 Pod 反亲和性级别：

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| `requiredDuringSchedulingIgnoredDuringExecution` | **硬约束**：若无法满足，Pod 调度失败（Pending） | 生产环境，确保高可用 |
| `preferredDuringSchedulingIgnoredDuringExecution` | **软偏好**：优先满足，无法满足时仍调度 | 开发/测试环境 |

Nacos 集群应使用硬约束——如果同一 Node 运行两个 Nacos Pod，Node 宕机会导致集群失去 2/3 成员（3 节点集群不可用）。

#### 探针配置详解

| 探针类型 | 用途 | 关键参数 | Nacos 适配考量 |
|---------|------|---------|---------------|
| **Liveness Probe** | 检测 Pod 是否存活 | `initialDelaySeconds=60` | Nacos 启动较慢（JRaft 选举 + MySQL 连接），需 60s 延迟 |
| **Readiness Probe** | 检测 Pod 是否就绪 | `initialDelaySeconds=30` | 就绪后加入 Service 负载均衡 |
| **Startup Probe** | 检测慢启动应用 | `failureThreshold=30` | Nacos 2.5.3 如启动时间 > 60s，建议加 Startup Probe |

**探针端点**：`/nacos/v1/console/server/state` 返回 JSON `{"standalone_mode":"cluster", ...}`，可直接用作健康检查端点。

#### StatefulSet Pod 管理策略

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| `OrderedReady`（默认） | 串行启动：0→1→2，0 就绪后才启动 1 | 严格依赖顺序的场景 |
| `Parallel` | 所有 Pod 并行启动 | Nacos 集群（各节点独立启动，不依赖顺序） |

Nacos 集群应使用 `Parallel`——各节点独立启动并通过 JRaft 自动发现集群成员，无需等待前序 Pod 就绪。

### 部署与验证

```bash
# ================================================================
# 1. 部署所有 K8s 资源
# ================================================================
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-configmap.yaml
kubectl apply -f 02-headless-service.yaml
kubectl apply -f 03-service.yaml
kubectl apply -f 04-statefulset.yaml

# ================================================================
# 2. 等待所有 Pod 就绪
# ================================================================
kubectl -n nacos get pods -w
# 预期输出（2-3 分钟后）：
# NAME       READY   STATUS    RESTARTS   AGE
# nacos-0    1/1     Running   0          2m30s
# nacos-1    1/1     Running   0          2m30s
# nacos- 2    1/1     Running   0          2m30s

# ================================================================
# 3. 验证 Headless DNS 解析
# ================================================================
kubectl -n nacos run -it --rm debug --image=busybox:1.36 --restart=Never -- nslookup nacos-0.nacos-headless.nacos.svc.cluster.local
# 预期：返回 Pod IP（如 10.244.1.5）

# ================================================================
# 4. 验证集群状态
# ================================================================
# 通过 ClusterIP Service 访问 Nacos API
kubectl -n nacos run -it --rm debug --image=busybox:1.36 --restart=Never -- \
  wget -qO- http://nacos-service:8848/nacos/v1/core/cluster/nodes
# 预期返回 3 节点 JSON 数组

# ================================================================
# 5. 验证 PVC 持久化
# ================================================================
kubectl -n nacos get pvc
# 预期输出：
# NAME              STATUS   VOLUME    CAPACITY   ACCESS MODES   STORAGECLASS
# data-nacos-0      Bound    pvc-xxx   20Gi        RWO           standard
# data-nacos-1      Bound    pvc-xxx   20Gi        RWO           standard
# data-nacos-2      Bound    pvc-xxx   20Gi        RWO           standard

# ================================================================
# 6. 验证 PodAntiAffinity（所有 Pod 在不同 Node）
# ================================================================
kubectl -n nacos get pods -o wide
# 预期：NODE 列的值各不相同（每个 Pod 在不同物理/虚拟 Node）

# ================================================================
# 7. 验证滚动重启
# ================================================================
kubectl -n nacos rollout restart statefulset nacos
kubectl -n nacos rollout status statefulset nacos
# 预期：滚动重启成功，集群始终可用
```

### Trade-off 分析

**K8s StatefulSet vs 物理机/VM 部署**：

| 维度 | K8s StatefulSet | 物理机/VM 部署 |
|------|---------------|---------------|
| 部署速度 | 极快（kubectl apply -f） | 慢（手动安装 JDK、配置、启动） |
| 弹性伸缩 | kubectl scale statefulset nacos --replicas=5 | 手动增加节点 + 配置 |
| 故障自愈 | K8s 自动重启失败 Pod | 依赖外部监控 + 手动恢复 |
| 滚动更新 | kubectl rollout restart（自动） | 手动逐节点重启 |
| 网络配置 | Headless Service DNS 自动管理 | 手动维护 cluster.conf IP |
| 持久化存储 | PVC 自动绑定 PV | 手动挂载磁盘 |
| 资源隔离 | Cgroup（Namespace 级隔离） | VM 级隔离（更强） |
| 性能开销 | 容器虚拟化开销（~5-10%） | 裸金属性能 |
| 适用场景 | 云原生环境、动态伸缩需求 | 固定规模、高性能要求 |

**StatefulSet `Parallel` vs `OrderedReady`**：

| 维度 | `Parallel` | `OrderedReady` |
|------|-----------|---------------|
| 启动速度 | 快（所有 Pod 同时启动） | 慢（逐一启动） |
| 依赖顺序 | 无（各节点独立启动） | 严格 0→1→2 |
| Nacos 适用性 | ✅ 推荐（JRaft 自动发现） | ❌ 不必要（Nacos 节点无启动顺序依赖） |
| 回滚风险 | 低（各节点独立） | 低（但更慢） |

### 设计模式分析

1. **工厂模式（Factory Method）**：StatefulSet 的 `volumeClaimTemplates` 为每个 Pod 自动创建 PVC——Pod 不直接声明 PVC，而是由 StatefulSet 控制器按模板「生产」PVC。Pod 重建时自动重新绑定同名 PVC（`data-nacos-{序号}`），数据保持持久化。

2. **代理模式（Proxy）**：Headless Service 作为 Pod DNS 代理层——客户端通过 DNS 名称访问特定 Pod（`nacos-0.nacos-headless`），而非直接使用 Pod IP。Pod IP 变化时 DNS 自动更新（A/AAAA 记录），对客户端透明。

3. **策略模式（Strategy）**：`podManagementPolicy` 切换 Pod 启动策略（`OrderedReady` vs `Parallel`），`podAntiAffinity` 切换调度策略（`required` vs `preferred`），业务需求变化时只需修改 YAML 策略字段，不影响 Pod 模板其他配置。

### 小结

- Nacos 2.5.3 在 K8s 上必须使用 StatefulSet（非 Deployment）确保网络标识稳定性、持久化存储和有状态 Pod 管理
- Headless Service (`clusterIP: None`) + `publishNotReadyAddresses: true` 确保 DNS 解析稳定性和集群启动顺序无关
- `cluster.conf` 使用 DNS 域名（`nacos-{0..N-1}.nacos-headless.nacos.svc.cluster.local`）而非 IP，避免 Pod 重启 IP 变化导致集群分裂
- PodAntiAffinity `requiredDuringSchedulingIgnoredDuringExecution` 强制 Pod 分散到不同 Node，确保物理层高可用
- `volumeClaimTemplates` 为每个 Pod 自动创建独立 PVC，Pod 重建后自动重新绑定同名 PVC，数据持久化不丢失
- `podManagementPolicy: Parallel` 适合 Nacos 集群（节点无启动顺序依赖），加速集群启动

---

## 10.7 K8s 部署常用命令（apply / get / logs / scale / rollout / port-forward）

### 设计背景

将 Nacos 部署到 K8s 后，运维人员需要掌握一系列日常运维命令来管理集群的生命周期。本节将 Nacos K8s 运维中最常用的命令按场景分类，提供完整的命令速查表和实战示例。

核心运维场景包括：部署更新（apply / rollout）、监控排障（get / describe / logs）、扩缩容（scale）、端口转发（port-forward）和资源清理（delete）。

### 命令速查矩阵

| 场景 | 命令 | 用途 | 示例 |
|------|------|------|------|
| 资源部署 | `kubectl apply -f <yaml>` | 创建/更新资源 | `kubectl apply -f 04-statefulset.yaml` |
| 查看 Pod | `kubectl get pods -n nacos` | 列出所有 Nacos Pod | `kubectl get pods -n nacos -o wide` |
| 查看 StatefulSet | `kubectl get sts -n nacos` | 查看 StatefulSet 状态 | `kubectl get sts nacos -n nacos` |
| 查看 Service | `kubectl get svc -n nacos` | 查看 Service 地址 | `kubectl get svc -n nacos` |
| 查看 PVC | `kubectl get pvc -n nacos` | 查看持久化存储状态 | `kubectl get pvc -n nacos` |
| 查看 ConfigMap | `kubectl get configmap -n nacos` | 查看配置内容 | `kubectl describe configmap nacos-config -n nacos` |
| 查看 Pod 详情 | `kubectl describe pod <pod> -n nacos` | 查看 Pod 事件/状态 | `kubectl describe pod nacos-0 -n nacos` |
| 查看 Pod 日志 | `kubectl logs <pod> -n nacos` | 查看实时日志 | `kubectl logs -f nacos-0 -n nacos` |
| 进入 Pod | `kubectl exec -it <pod> -n nacos -- /bin/bash` | 进入容器排查 | `kubectl exec -it nacos-0 -n nacos -- /bin/bash` |
| 扩缩容 | `kubectl scale sts <name> --replicas=<N>` | 增加/减少节点数 | `kubectl scale sts nacos -n nacos --replicas=5` |
| 滚动更新 | `kubectl rollout restart sts <name>` | 滚动重启所有 Pod | `kubectl rollout restart sts nacos -n nacos` |
| 回滚 | `kubectl rollout undo sts <name>` | 回滚到上一个版本 | `kubectl rollout undo sts nacos -n nacos` |
| 端口转发 | `kubectl port-forward <pod> <local>:<remote>` | 本地访问 Pod 端口 | `kubectl port-forward nacos-0 8848:8848 -n nacos` |
| 删除资源 | `kubectl delete -f <yaml>` | 删除资源 | `kubectl delete -f 04-statefulset.yaml` |
| 查看事件 | `kubectl get events -n nacos --sort-by=.lastTimestamp` | 查看 Namespace 事件 | - |
| 资源用量 | `kubectl top pods -n nacos` | 查看 CPU/内存用量 | `kubectl top pods -n nacos` |

### 场景一：部署与更新

#### 初始部署

```bash
# 1. 按顺序部署所有资源文件
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-configmap.yaml
kubectl apply -f 02-headless-service.yaml
kubectl apply -f 03-service.yaml
kubectl apply -f 04-statefulset.yaml

# 2. 查看部署进度
kubectl -n nacos get pods -w
# 预期：逐个 Pod 进入 Running 状态

# 3. 全部就绪后验证集群状态
kubectl -n nacos exec nacos-0 -- curl -s http://localhost:8848/nacos/v1/core/cluster/nodes
```

#### 更新 ConfigMap 并滚动重启

```bash
# 1. 编辑 ConfigMap（例如调整 JVM 参数）
kubectl edit configmap nacos-config -n nacos

# 2. 滚动重启 StatefulSet（Pod 逐个重启，保持集群可用）
kubectl -n nacos rollout restart statefulset nacos

# 3. 观察滚动更新状态
kubectl -n nacos rollout status statefulset nacos
# 预期输出：Waiting for StatefulSet spec update to be observed...
# waiting for StatefulSet rolling restart to complete...
# statefulset rolling restart complete

# 4. 验证更新是否生效
kubectl -n nacos exec nacos-0 -- cat /home/nacos/conf/application.properties
```

#### 回滚到上一个版本

```bash
# 查看 StatefulSet 修订历史
kubectl -n nacos rollout history statefulset nacos

# 回滚到上一个版本
kubectl -n nacos rollout undo statefulset nacos

# 验证回滚状态
kubectl -n nacos rollout status statefulset nacos
```

### 场景二：监控与排障

#### Pod 状态监控

```bash
# 查看所有 Pod 状态（含 Node 分布）
kubectl -n nacos get pods -o wide
# 预期输出：
# NAME       READY   STATUS    RESTARTS   AGE   IP            NODE
# nacos-0    1/1     Running   0          5d    10.244.1.5   node-1
# nacos-1    1/1     Running   0          5d    10.244.2.8   node-2
# nacos-2    1/1     Running   0          5d    10.244.3.3   node-3

# 查看 Pod 资源用量（需要 K8s Metrics Server）
kubectl -n nacos top pods
# 预期输出：
# NAME       CPU(cores)   MEMORY(bytes)
# nacos-0    500m         1800Mi
# nacos-1    450m         1700Mi
# nacos-2    480m         1750Mi
```

#### Pod 排障：Pending / CrashLoopBackOff 排查流程

```bash
# 1. Pod 长时间 Pending
kubectl describe pod nacos-0 -n nacos
# 查看 Events 区域：
#   - 0/3 nodes are available: 3 node(s) didn't match pod anti-affinity rules.
#   → 反亲和性规则无法满足（所有 Node 已有 Nacos Pod）
#   - 0/3 nodes are available: insufficient cpu.
#   → CPU 资源不足

# 2. Pod CrashLoopBackOff
kubectl logs nacos-0 -n nacos --previous
# 查看上一次崩溃的日志
# 常见错误：
#   - MySQL 连接拒绝 → 检查 MySQL Service/slow DNS
#   - cluster.conf DNS 解析失败 → 检查 Headless Service
#   - OOM Killed → 增加 memory limit
```

#### 日志查看

```bash
# 实时跟踪 Nacos 日志
kubectl -n nacos logs -f nacos-0

# 查看最近 100 行日志
kubectl -n nacos logs --tail=100 nacos-0

# 同时查看所有 Pod 日志（使用 stern 工具）
# stern -n nacos nacos

# 查看特定时间范围的日志
kubectl -n nacos logs --since=30m nacos-0

# 导出日志到文件
kubectl -n nacos logs nacos-0 > nacos-0.log
```

### 场景三：扩缩容

#### 从 3 节点扩容到 5 节点

```bash
# 1. 更新 ConfigMap 中的 cluster.conf，添加 nacos-3 和 nacos-4
kubectl edit configmap nacos-config -n nacos
# 在 cluster.conf 中添加：
#   nacos-3.nacos-headless.nacos.svc.cluster.local:7848
#   nacos-4.nacos-headless.nacos.svc.cluster.local:7848

# 2. 扩容 StatefulSet 到 5 个副本
kubectl scale statefulset nacos -n nacos --replicas=5

# 3. 查看新 Pod 创建进度
kubectl -n nacos get pods -w
# 预期：nacos-3 和 nacos-4 逐步创建并 Running

# 4. 验证 5 节点集群状态
kubectl -n nacos exec nacos-0 -- curl -s http://localhost:8848/nacos/v1/core/cluster/nodes
```

#### 缩容回 3 节点

```bash
# 1. 缩容前先确保 cluster.conf 已更新（移除 nacos-3 和 nacos-4）
kubectl edit configmap nacos-config -n nacos

# 2. 缩容 StatefulSet
kubectl scale statefulset nacos -n nacos --replicas=3

# 注意：StatefulSet 缩容时从最高序号 Pod 开始删除（nacos-4 → nacos-3）
```

### 场景四：端口转发与本地调试

```bash
# 1. 将 Nacos HTTP 端口转发到本地
kubectl port-forward -n nacos nacos-0 8848:8848
# 现在访问 http://localhost:8848/nacos 可打开 Nacos 控制台

# 2. 后台运行端口转发
kubectl port-forward -n nacos nacos-0 8848:8848 &
# 关闭后台转发：kill %1

# 3. 同时转发多个 Pod
kubectl port-forward -n nacos nacos-0 8848:8848 &
kubectl port-forward -n nacos nacos-1 8849:8848 &
kubectl port-forward -n nacos nacos-2 8850:8848 &
# 本地分别访问 localhost:8848 / localhost:8849 / localhost:8850
```

### 场景五：资源清理

```bash
# 按资源文件逆序删除
kubectl delete -f 04-statefulset.yaml
kubectl delete -f 03-service.yaml
kubectl delete -f 02-headless-service.yaml
kubectl delete -f 01-configmap.yaml
kubectl delete -f 00-namespace.yaml

# 注意：PVC 不会随 StatefulSet 删除而自动删除，需手动清理
kubectl -n nacos get pvc
kubectl -n nacos delete pvc data-nacos-0 data-nacos 1 data-nacos-2
```

### Trade-off 分析

**kubectl 运维 vs Nacos 内置 API 运维**：

| 维度 | kubectl 命令 | Nacos 内置 API |
|------|-------------|---------------|
| 扩缩容 | `kubectl scale sts` | 不支持（Nacos 自身不管理集群拓扑） |
| 滚动更新 | `kubectl rollout restart` | 不支持 |
| 日志查看 | `kubectl logs` | 访问 `/nacos/v1/console/server/state` 获取状态 |
| 集群成员查看 | `kubectl get pods` | `GET /nacos/v1/core/cluster/nodes` |
| 配置更新 | `kubectl edit configmap` + rollout | 通过 API 修改配置 |
| 故障恢复 | K8s 自动重启 + Liveness Probe | Raft 自动故障转移 |
| 结论 | K8s 管理 Pod 生命周期 / Nacos API 管理集群状态 | 两者互补使用 |

### 设计模式分析

1. **声明式基础设施（Declarative Infrastructure）**：K8s YAML 声明期望状态（`replicas: 3`），K8s 控制器持续调和实际状态向期望状态收敛。运维人员不需要编写脚本来逐个操作 Pod——这是 Infrastructure as Code（IaC）的最佳实践。

2. **控制回路模式（Control Loop）**：K8s 的 reconcile loop 持续检测 StatefulSet 的 `currentReplicas` vs `desiredReplicas`，差异时自动创建/删除 Pod。如果 Pod 因 Node 宕机丢失，K8s 自动在其他 Node 重建 Pod——这是 Kubernetes 自愈能力的核心。

3. **适配器模式（Adapter）**：`kubectl port-forward` 将远程 Pod 端口适配到本地 localhost——运维人员无需直接访问 K8s 内部网络，通过本地端口即可调试 Pod

### 小结

- K8s 运维 Nacos 的核心命令分为 5 类：部署更新（`apply` / `rollout`）、监控排障（`get` / `describe` / `logs`）、扩缩容（`scale`）、端口转发（`port-forward`）、资源清理（`delete`）
- `kubectl rollout restart` 实现零停机滚动重启，每次只重启 1 个 Pod，保持集群 quorum 可用
- 扩容到 5 节点需同步更新 `cluster.conf`（添加新 Pod DNS 名称）和后端 Nginx upstream
- PVC 不会随 StatefulSet 删除而自动删除——避免误删数据，需手动清理
- `kubectl port-forward` 是将 K8s 内部服务暴露给本地调试的最快方式，无需创建 Ingress 或 LoadBalancer Service

---

## 10.8 多数据中心双活架构：GeoDNS + MySQL 异步复制 + 命名空间前缀隔离

### 设计背景

单数据中心部署的 Nacos 集群面临区域性灾难风险：数据中心断电、网络中断、自然灾害等可导致整个集群不可用，所有依赖 Nacos 的微服务将同时丧失服务发现和配置管理能力。

Nacos 2.5.3 本身不支持原生的多数据中心数据同步机制（不同于 Consul 的 WAN Federation 或 Eureka 的多 Region 副本机制），因此需要借助外部基础设施实现跨数据中心高可用。核心挑战包括：

1. **流量路由**：客户端如何知道应该连接哪个数据中心的 Nacos 集群？需要 GeoDNS 智能解析
2. **配置数据同步**：两个数据中心的 Nacos 配置数据如何保持一致？需要 MySQL 异步复制
3. **服务数据隔离**：两个数据中心的微服务注册在同一 Nacos 集群时如何避免冲突？需要命名空间前缀隔离
4. **脑裂避免**：两个数据中心之间的网络中断时，如何避免双主写入冲突？需要 MySQL 复制冲突检测和 Raft 单数据中心选举

本节提出「GeoDNS + MySQL 异步复制 + 命名空间前缀隔离」的三层双活架构方案。

### 核心架构关系图

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                   Nacos 多数据中心双活架构                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│                          ┌──────────────────────┐                                  │
│                          │   GeoDNS (智能解析)  │                                  │
│                          │                      │                                  │
│                          │  • 就近解析          │                                  │
│                          │  • 健康检查          │                                  │
│                          │  • 故障切换          │                                  │
│                          └──────────┬───────────┘                                  │
│                                     │                                           │
│              ┌──────────────────────┼──────────────────────┐                      │
│              │                      │                      │                      │
│     ┌────────▼────────┐  ┌──────▼──────┐  ┌─────────▼────────┐              │
│     │  客户端（华东） │  │ 客户端（华南）│  │  客户端（华北）  │              │
│     │  app- East │  │  app-south │  │   app-north   │              │
│     │  namespace:    │  │  namespace:   │  │   namespace:    │              │
│     │  east-online  │  │  south-online│  │   north-online │              │
│     └────────┬────────┘  └──────┬──────┘  └─────────┬────────┘              │
│              │                  │                     │                        │
│     ┌────────▼────────┐  ┌──────▼──────┐  ┌─────────▼────────┐              │
│     │ 华东 DC (主)    │  │ 华南 DC (从) │  │  华北 DC (从)    │              │
│     │                 │  │             │  │                  │              │
│     │ ┌─────────────┐ │  │┌──────────┐│  │┌────────────────┐ │              │
│     │ │ Nacos 集群  │ │  ││Nacos 集群││  ││ Nacos 集群     │ │              │
│     │ │ 3 节点      │ │  ││3 节点    ││  ││ 3 节点        │ │              │
│     │ │ namespace:  │ │  ││namespace: ││  ││ namespace:     │ │              │
│     │ │ east-*     │ │  ││south-*   ││  ││ north-*        │ │              │
│     │ └──────┬──────┘ │  │└────┬─────┘│  │└───────┬────────┘ │              │
│     │        │        │  │     │      │  │        │          │              │
│     │ ┌──────▼──────┐ │  │┌────▼─────┐│  │┌───────▼────────┐ │              │
│     │ │  MySQL 主库  │ │  ││MySQL 从库││  ││  MySQL 从库    │ │              │
│     │ │  (读写)     │ │  ││ (只读)   ││  ││  (只读)       │ │              │
│     │ └──────┬──────┘ │  │└──────────┘│  │└────────────────┘ │              │
│     │        │        │  │             │  │                  │              │
│     │        │ 异步复制 │             │  │                  │              │
│     │        ├────────┼─────────────┼──┘                  │              │
│     │        │        │  Binlog 同步│                         │              │
│     │        │        │◄────────────┼────────────────────────┤              │
│     │        │        │             │                         │              │
│     └────────┴────────┘  └─────────────┘  └──────────────────┘              │
│                                                                                  │
│   命名空间隔离策略：                                                            │
│   • east-online: 华东微服务注册 (Nacos 华东集群)                               │
│   • south-online: 华南微服务注册 (Nacos 华南集群)                             │
│   • north-online: 华北微服务注册 (Nacos 华北集群)                             │
│                                                                                  │
│   GeoDNS 解析策略：                                                             │
│   • nacos.example.com → 根据客户端源 IP → 就近解析到最近 DC 的 Nginx VIP       │
│   • 健康检查失败 → 自动切换到备用 DC                                            │
│                                                                                  │
│           图 10-15：多数据中心双活架构（GeoDNS + MySQL 异步复制）                │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### GeoDNS 智能流量调度

GeoDNS 是多数据中心双活架构的流量入口层，负责根据客户端地理位置智能解析 DNS 请求到最近的数据中心入口 Nginx VIP。

#### GeoDNS 解析策略

| 客户端区域 | 解析结果（Nacos 域名） | Nacos 集群 | 优先级 |
|-----------|----------------------|-----------|--------|
| 华东（上海/杭州/南京） | `nacos-east.example.com` → 华东 Nginx VIP | 华东 Nacos 集群 | Primary |
| 华南（广州/深圳） | `nacos-south.example.com` → 华南 Nginx VIP | 华南 Nacos 集群 | Primary |
| 华北（北京/天津） | `nacos-north.example.com` → 华北 Nginx VIP | 华北 Nacos 集群 | Primary |
| 其他区域 | 轮询华东/华南 VIP | 就近 DC | Failover |

#### GeoDNS 健康检查与故障切换

```yaml
# GeoDNS 配置示例（AWS Route 53）
# 华东 Primary 记录
nacos-east.example.com:
  type: A
  ttl: 60
  routing_policy: Geolocation
  health_check:
    endpoint: "http://nacos-east-vip:8848/nacos/v1/console/server/state"
    failure_threshold: 3
    request_interval: 10s
  records:
    - value: 10.0.1.100  # 华东 Nginx VIP
  failover:
    - nacos-south.example.com  # 故障时切换到华南
```

**故障切换流程**：

1. GeoDNS 每 10s 对华东 Nginx VIP 执行健康检查（`GET /nacos/v1/console/server/state`）
2. 连续 3 次失败（30s）→ 将华东 DNS 记录自动切换到华南 VIP
3. DNS TTL 设为 60s（短 TTL 加速故障切换，但增加 DNS 查询压力）
4. 华东集群恢复后，GeoDNS 自动回切（恢复原始解析）

### MySQL 异步复制配置

Nacos 的所有元数据（配置、服务注册信息）持久化在 MySQL 中。多数据中心双活的核心数据同步依赖 MySQL 主从异步复制。

#### 主从拓扑

```
┌──────────────┐
│ MySQL Master  │ (华东 DC - 唯一写入点)
│              │
│ Binlog ▶────┼──────────────┬──────────────────┐
└──────────────┘              │                  │
                               │                  │
                     ┌─────────▼────────┐  ┌──────▼──────────┐
                     │ MySQL Slave 1    │  │ MySQL Slave 2    │
                     │ (华南 DC - 只读) │  │ (华北 DC - 只读) │
                     └──────────────────┘  └─────────────────┘

                     图 10-16：MySQL 一主两从异步复制拓扑
```

**关键设计决策**：

1. **单主写入（Single Master）**：所有配置变更只在华东 Master MySQL 上执行（华南/华北的 Nacos 配置写入请求转发到华东 Nacos 集群的 MySQL）。避免多主写入冲突
2. **异步复制**：MySQL 异步复制（非半同步）——Master 提交事务后不等待 Slave 确认。优点是 Master 写入延迟不受 Slave 网络延迟影响（跨数据中心 RTT 通常 > 30ms）；缺点是 Slave 可能存在秒级延迟
3. **只读分离**：华南/华北 Nacos 的 MySQL 连接配置为只读从库——服务发现查询命中的服务实例可能是华东 Master 提交但尚未同步到 Slave 的延迟数据（最终一致性）

#### MySQL 主库配置（华东）

```ini
# /etc/mysql/mysql.conf.d/mysqld.cnf (Master - 华东)
[mysqld]
# 服务器唯一 ID（主库必须唯一）
server-id = 1

# 启用 Binlog
log_bin = /var/lib/mysql/mysql-bin.log
binlog_format = ROW

# Binlog 保留时间（天）
expire_logs_days = 7
max_binlog_size = 500M

# GTID 模式（推荐启用，切换更安全）
gtid_mode = ON
enforce_gtid_consistency = ON

# 要复制的数据库
binlog_do_db = nacos_config

# 连接数
max_connections = 500
```

#### MySQL 从库配置（华南 / 华北）

```ini
# /etc/mysql/mysql.conf.d/mysqld.cnf (Slave - 华南)
[mysqld]
# 服务器唯一 ID（每个从库不同）
server-id = 2  # 华南 Slave = 2，华北 Slave = 3

# 启用中继日志
relay_log = /var/lib/mysql/mysql-relay-bin.log

# 只读模式（防止误写）
read_only = ON
super_read_only = ON

# GTID 模式
gtid_mode = ON
enforce_gtid_consistency = ON
```

#### 建立主从复制

```bash
# ================================================================
# 在 Master（华东）上创建复制用户
# ================================================================
mysql> CREATE USER 'repl'@'%' IDENTIFIED BY 'Repl@2025';
mysql> GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
mysql> FLUSH PRIVILEGES;

# 查看 Master Binlog 位置
mysql> SHOW MASTER STATUS;
# 记录 File 和 Position（如 mysql-bin.000001， Position 1234）

# ================================================================
# 在 Slave（华南）上配置复制
# ================================================================
mysql> STOP SLAVE;
mysql> CHANGE MASTER TO
    -> MASTER_HOST='192.168.1.100',        # 华东 MySQL Master IP
    -> MASTER_USER='repl',
    -> MASTER_PASSWORD='Repl@2025',
    -> MASTER_LOG_FILE='mysql-bin.000001',  # 从 SHOW MASTER STATUS 获取
    -> MASTER_LOG_POS=1234,
    -> MASTER_AUTO_POSITION=1;             # GTID 自动定位

mysql> START SLAVE;

# 查看 Slave 复制状态
mysql> SHOW SLAVE STATUS\G
# 关键字段检查：
#   Slave_IO_Running: Yes        # I/O 线程运行
#   Slave_SQL_Running: Yes       # SQL 线程运行
#   Seconds_Behind_Master: 0     # 同步延迟
```

#### MySQL 复制监控与告警

```bash
# 监控 Slave 复制延迟
mysql -h 192.168.1.200 -u monitor -pMonitor@2025 -e "SHOW SLAVE STATUS\G" 2>/dev/null | grep -E 'Seconds_Behind_Master|Slave_IO_Running|Slave_SQL_Running'

# 关键告警阈值：
# • Seconds_Behind_Master > 10s  → WARNING
# • Seconds_Behind_Master > 60s  → CRITICAL
# • Slave_IO_Running != Yes 或 Slave_SQL_Running != Yes → CRITICAL
```

### 命名空间前缀隔离策略

多数据中心中，每个数据中心的微服务注册到本地 Nacos 集群时必须使用不同的命名空间前缀，避免跨数据中心的服务发现冲突。

#### 命名空间命名规范

| 数据中心 | 命名空间前缀 | 示例 | 说明 |
|---------|------------|------|------|
| 华东 | `east-` | `east-online`、`east-test` | 华东微服务注册命名空间 |
| 华南 | `south-` | `south-online`、`south-test` | 华南微服务注册命名空间 |
| 华北 | `north-` | `north-online`、`north-test` | 华北微服务注册命名空间 |
| 公共 | `global-` | `global-common` | 跨数据中心共享配置 |

#### 客户端配置示例

**华东微服务**：

```yaml
# bootstrap.yml - 华东微服务
spring:
  cloud:
    nacos:
      discovery:
        server-addr: nacos-east.example.com:8848
        namespace: east-online  # 华东专属命名空间
      config:
        server-addr: nacos-east.example.com:8848
        namespace: east-online
```

**华南微服务**：

```yaml
# bootstrap.yml - 华南微服务
spring:
  cloud:
    nacos:
      discovery:
        server-addr: nacos-south.example.com:8848
        namespace: south-online  # 华南专属命名空间
      config:
        server-addr: nacos-south.example.com:8848
        namespace: south-online
```

**跨数据中心共享配置**：

```yaml
# bootstrap.yml - 需要访问全局公共配置的微服务
spring:
  cloud:
    nacos:
      config:
        server-addr: nacos-east.example.com:8848
        namespace: global-common  # 公共配置命名空间
        shared-configs:
          - data-id: common.properties
            group: DEFAULT_GROUP
            refresh: true
```

### Trade-off 分析

**多数据中心双活 vs 单数据中心容灾**：

| 维度 | 多数据中心双活（本方案） | 单数据中心 + 冷备 |
|------|----------------------|-------------------|
| RPO（恢复点目标） | 秒级（MySQL 异步复制延迟） | 小时级（手动恢复备份） |
| RTO（恢复时间目标） | 分钟级（GeoDNS 故障切换） | 小时级（需人工介入） |
| 写入延迟 | Master 本地写入（低延迟） + Slave 异步复制（跨 DC RTT） | 仅本地写入（低延迟） |
| 架构复杂度 | 高（GeoDNS + MySQL 复制 + 命名空间隔离） | 低 |
| 基础设施成本 | 高（3 套 Nacos 集群 + 3 MySQL 实例 + GeoDNS） | 低（1 套 + 冷备 MySQL） |
| 配置一致性 | 最终一致（异步复制延迟） | N/A（仅主集群） |

**MySQL 异步 vs 半同步复制**：

| 维度 | 异步复制 | 半同步复制 |
|------|---------|-----------|
| 写入延迟 | Master 提交后立即返回 | Master 等待至少 1 个 Slave ACK |
| 数据一致性 | 最终一致（Slave 可能落后） | 较强一致（至少 1 Slave 已接收） |
| 跨 DC 适用性 | ✅ 高延迟环境适用 | ❌ RTT > 30ms 阻塞 Master 写入 |
| Slave 宕机影响 | 不影响 Master 写入 | Master 写入降级为异步（rpl_semi_sync_master_timeout） |
| 适用场景 | 跨数据中心（本方案） | 同城容灾 |

**关键设计权衡**：

1. **配置写入冲突**：如果两个数据中心同时对同一 `data-id` 执行配置发布，由于 MySQL 异步复制，华南 Nacos 写入可能基于旧版本数据，导致配置覆盖。**缓解策略**：(a) 配置写入入口统一路由到华东 Master MySQL；(b) Nacos 配置版本号控制（乐观锁）
2. **服务注册延迟**：华东新注册的服务实例，华南 Slave 延迟复制（Seconds_Behind_Master），导致华南微服务在短暂时间窗口内无法发现华东新注册的服务。**缓解策略**：`Seconds_Behind_Master < 1s` 正常运行时影响忽略不计；故障时华南回退到本地注册表缓存
3. **网络分区脑裂**：华东-华南之间网络中断，华东 Master MySQL 正常运行，华南 Slave I/O 线程中断。此时华南 Nacos 集群仍可提供只读服务（基于本地 Slave 数据快照），但无法获取最新配置变更。**恢复策略**：网络恢复后 MySQL 复制自动恢复（GTID 自动定位断点）

### 设计模式分析

1. **主从复制模式（Master-Slave Replication）**：MySQL 异步主从复制实现跨数据中心数据同步——单 Master 避免写入冲突，多 Slave 分担只读流量。类似数据库读写分离模式，但分布在不同的物理数据中心
2. **策略模式（Strategy）**：GeoDNS 的健康检查和故障切换策略可独立配置——健康检查端点、故障阈值、切换目标均可按数据中心粒度定制
3. **命名空间隔离模式（Namespace Isolation）**：利用 Nacos 原生的 `namespace` 机制隔离不同数据中心的微服务——不依赖外部隔离工具，Nacos 命名空间提供了天然的租户隔离能力

### 小结

- 多数据中心双活架构依赖三层基础设施：GeoDNS（流量调度层）→ MySQL 异步复制（数据同步层）→ Nacos 命名空间前缀隔离（应用隔离层）
- GeoDNS 根据客户端地理位置智能解析 DNS 到最近 DC 的 Nginx VIP，健康检查失败 → 自动切换到备用 DC（DNS TTL 60s）
- MySQL 异步主从复制：华东 Master 为唯一写入点，华南/华北 Slave 只读服务本地 Nacos 集群——避免多主写入冲突
- 命名空间前缀隔离（`east-` / `south-` / `north-`）确保不同 DC 的微服务注册不冲突，`global-` 前缀用于跨 DC 共享配置
- 异步复制延迟导致配置和服务注册最终一致性（秒级延迟），跨 DC 配置写入冲突需通过统一入口路由缓解

---

## 10.9 MySQL 主从复制 + 读写分离中间件（ShardingSphere-proxy / ProxySQL）

### 设计背景

Nacos 2.5.3 集群在生产环境中需要 MySQL 数据库的高可用保障。单 MySQL 实例存在单点故障风险——MySQL 宕机导致所有 Nacos 节点无法持久化配置数据，集群虽然依靠 Raft 维持服务发现内存数据，但配置变更将全部失败。

MySQL 高可用标准方案是主从复制 + 读写分离：

1. **主从复制**：MySQL Master 处理所有写入操作（配置发布、服务注册持久化），Binlog 异步/半同步复制到 Slave
2. **读写分离**：Nacos 读操作（配置查询、服务发现查询）路由到 Slave，降低 Master 读压力
3. **故障转移**：Master 宕机时，中间件自动将 Slave 提升为新 Master

然而 Nacos 应用层不感知 MySQL 主从拓扑——Nacos JDBC 连接字符串只配置单个 MySQL URL（`db.url.0=jdbc:mysql://...`）。因此需要在中间件层实现读写分离，对 Nacos 透明。

常见的 MySQL 读写分离中间件包括 **ShardingSphere-proxy** 和 **ProxySQL**，本节对比两种方案在 Nacos 场景下的适用性。

### 核心架构关系图

```
┌────────────────────────────────────────────────────────────────────────────┐
│              MySQL 主从复制 + 读写分离架构                              │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   ┌──────────────────┐                                                 │
│   │   Nacos 集群     │                                                 │
│   │   (3 节点)       │                                                 │
│   └────────┬─────────┘                                                 │
│            │                                                              │
│   ┌────────▼──────────────────────────────────────────┐                  │
│   │        MySQL 读写分离中间件 (ShardingSphere-proxy │                  │
│   │        或 ProxySQL)                              │                  │
│   │                                                   │                  │
│   │   • 读写分离规则：SELECT → Slave / INSERT/UPDATE │                  │
│   │     /DELETE → Master                            │                  │
│   │   • 故障转移：Master 宕机 → Slave 提升为新 Master  │                  │
│   │   • 连接池：复用 MySQL 连接，减少连接开销          │                  │
│   │   • 负载均衡：多 Slave 间负载均衡                  │                  │
│   └──┬──────────────┬──────────────┘                               │
│      │              │                                                   │
│ ┌────▼──────┐ ┌───▼──────────┐                                     │
│ │MySQL Master│ │ MySQL Slave   │                                     │
│ │ (读写)     │ │ (只读)        │                                     │
│ └────┬──────┘ └──────────────┘                                     │
│      │  Binlog 异步/半同步复制                                        │
│      └───────────────────────┘                                        │
│                                                                        │
│          图 10-17：MySQL 读写分离中间件架构                             │
└────────────────────────────────────────────────────────────────────────────┘
```

### ShardingSphere-proxy 方案

ShardingSphere-proxy 是 Apache ShardingSphere 的子项目，作为独立的代理进程部署在 Nacos 集群与 MySQL 之间，透明地实现读写分离和故障转移。

#### 部署架构

```
Nacos 集群 → ShardingSphere-proxy (3307) → MySQL Master (3306)
                                           → MySQL Slave  (3306)
```

#### ShardingSphere-proxy 配置

```yaml
# server.yaml - ShardingSphere-proxy 服务配置
rules:
  - !AUTHORITY
    users:
      - user: nacos@%  # Nacos 连接使用的用户
        password: nacos
    privilege:
      type: ALL_PERMITTED

# config-readwrite-splitting.yaml - 读写分离配置
databaseName: nacos_config

dataSources:
  master:
    url: jdbc:mysql://192.168.1.100:3306/nacos_config?useSSL=false&serverTimezone=Asia/Shanghai
    username: nacos
    password: Nacos@2025
    connectionTimeoutMilliseconds: 30000
    idleTimeoutMilliseconds: 60000
    maxLifetimeMilliseconds: 180000
    maxPoolSize: 50
    minPoolSize: 5
  slave:
    url: jdbc:mysql://192.168.1.200:3306/nacos_config?useSSL=false&serverTimezone=Asia/Shanghai
    username: nacos
    password: Nacos@2025
    connectionTimeoutMilliseconds: 30000
    idleTimeoutMilliseconds: 60000
    maxLifetimeMilliseconds: 180000
    maxPoolSize: 50
    minPoolSize: 5

rules:
  - !READWRITE_SPLITTING
    dataSources:
      readwrite_splitting_db:
        staticStrategy:
          writeDataSourceName: master
          readDataSourceNames:
            - slave
        loadBalancerName: round_robin

    # 负载均衡算法
    loadBalancers:
      round_robin:
        type: ROUND_ROBIN
```

#### Nacos application.properties 配置

```properties
# Nacos JDBC 连接指向 ShardingSphere-proxy
# 注意：端口为 3307（ShardingSphere-proxy 默认端口），而非 MySQL 3306
db.url.0=jdbc:mysql://192.168.1.150:3307/nacos_config?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=Nacos@2025
```

#### 故障转移配置

ShardingSphere-proxy 支持 Master 宕机自动切换：

```yaml
# 读写分离 + 高可用配置
rules:
  - !READWRITE_SPLITTING
    dataSources:
      readwrite_splitting_db:
        staticStrategy:
          writeDataSourceName: master
          readDataSourceNames:
            - slave

  - !HA
    dataSources:
      master:
        url: jdbc:mysql://192.168.1.100:3306/nacos_config?useSSL=false&serverTimezone=Asia/Shanghai
        username: nacos
        password: Nacos@2025
      slave:
        url: jdbc:mysql://192.168.1.200:3306/nacos_config?useSSL=false&serverTimezone=Asia/Shanghai
        username: nacos
        password: Nacos@2025
    rules:
      - !FAILOVER
        dataSourceGroups:
          master_group:
            primaryDataSourceName: master
            replicaDataSourceNames:
              - slave
```

### ProxySQL 方案

ProxySQL 是一个高性能的 MySQL 代理，在 MySQL 协议层实现读写分离、查询缓存和故障转移。与 ShardingSphere-proxy 的差异在于 ProxySQL 更接近 MySQL 协议层，管理配置通过 Admin 接口（SQL 语句）而非 YAML。

#### 部署架构

```
Nacos 集群 → ProxySQL (6033) → MySQL Master (3306)
                              → MySQL Slave  (3306)
```

#### ProxySQL 配置（通过 Admin 接口）

```sql
-- 1. 添加 MySQL 服务器
INSERT INTO mysql_servers(hostgroup_id, hostname, port, weight) VALUES
  (0, '192.168.1.100', 3306, 1),  -- Master (hostgroup 0 = write)
  (1, '192.168.1.200', 3306, 1);  -- Slave  (hostgroup 1 = read)

-- 2. 添加 Nacos 用户
INSERT INTO mysql_users(username, password, default_hostgroup) VALUES
  ('nacos', 'Nacos@2025', 0);  -- 默认 hostgroup = 0 (write)

-- 3. 配置读写分离规则
-- 所有 SELECT 语句路由到 hostgroup 1 (Slave)
INSERT INTO mysql_query_rules(rule_id, active, match_pattern, destination_hostgroup, apply) VALUES
  (1, 1, '^SELECT', 1, 1);

-- 所有其他语句（INSERT/UPDATE/DELETE）路由到 hostgroup 0 (Master)
INSERT INTO mysql_query_rules(rule_id, active, match_pattern, destination_hostgroup, apply) VALUES
  (2, 1, '^INSERT|^UPDATE|^DELETE', 0, 1);

-- 4. 加载配置到运行时
LOAD MYSQL SERVERS TO RUNTIME;
LOAD MYSQL USERS TO RUNTIME;
LOAD MYSQL QUERY RULES TO RUNTIME;

-- 5. 持久化配置（重启后保留）
SAVE MYSQL SERVERS TO DISK;
SAVE MYSQL USERS TO DISK;
SAVE MYSQL QUERY RULES TO DISK;
```

#### ProxySQL 故障转移配置

```sql
-- 配置 MySQL 复制主机组
INSERT INTO mysql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type) VALUES
  (0, 1, 'read_only');

-- check_type = 'read_only'
-- ProxySQL 自动检测 Slave 的 read_only 变量：
--   • Master (read_only=OFF) → hostgroup 0
--   • Slave (read_only=ON)  → hostgroup 1

-- Master 宕机时：
--   1. ProxySQL 检测 Master 不可达 → 从 hostgroup 0 移除
--   2. 自动将 Slave 提升为 Master（read_only=OFF）
--   3. Slave 自动移入 hostgroup 0 → 接管写入流量
```

#### Nacos application.properties 配置

```properties
# Nacos JDBC 连接指向 ProxySQL
db.url.0=jdbc:mysql://192.168.1.150:6033/nacos_config?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=Nacos@2025
```

### 两种方案对比

| 维度 | ShardingSphere-proxy | ProxySQL |
|------|-------------------|----------|
| 配置方式 | YAML 文件配置 | SQL Admin 接口（动态配置） |
| 协议层 | JDBC 代理 | MySQL 协议代理 |
| 读写分离规则 | 基于 Hint 或 SQL 解析 | 基于正则匹配 SQL 语句 |
| 故障转移 | 内置 HA 模块 | `mysql_replication_hostgroups` |
| 连接池 | 内置连接池 | 内置连接池 |
| 查询缓存 | 不支持 | 支持查询缓存（Query Cache） |
| 性能开销 | 中（JDBC 协议解析） | 低（MySQL 协议代理，接近原生） |
| SQL 兼容性 | 广泛（SQL 解析标准） | 非常广泛（MySQL 协议代理） |
| 监控 | Prometheus metrics | Stats 库 + Grafana Dashboard |
| 社区成熟度 | Apache 顶级项目 | GPL 开源 |
| 适用场景 | Java 技术栈统一治理 | 高性能 MySQL 代理层 |

**推荐选择**：

- **Nacos 场景推荐 ProxySQL**：Nacos 只需 MySQL 协议读写分离，不需要 ShardingSphere 的分库分表能力。ProxySQL 的 MySQL 协议代理性能更高，且动态配置（无需重启）更适合生产环境
- **同时使用 ShardingSphere-jdbc 的场景**：如果 Nacos 之外还有大量 Java 微服务也需要读写分离，ShardingSphere-jdbc SDK 嵌入应用层，避免额外部署代理进程

### Trade-off 分析

**有中间件 vs 无中间件**：

| 维度 | 有读写分离中间件 | 无中间件（直连 MySQL Master） |
|------|----------------|---------------------------|
| 读性能 | 分担到 Slave，Master 读压力降低 | 所有读写集中在 Master |
| 写性能 | 不受影响（写仍走 Master） | 不受影响 |
| 故障转移 | 自动切换（秒级） | 手动修改 Nacos JDBC URL + 重启（分钟级） |
| 架构复杂度 | 增加代理层（额外进程） | 简单 |
| 额外延迟 | +1 跳（中间件代理延迟 < 1ms） | 无额外跳 |
| 单点风险 | 中间件自身（需多实例部署） | MySQL Master 单点 |

**MySQL 主从复制延迟对 Nacos 的影响**：

Nacos 的以下操作涉及 MySQL 读写：

| 操作 | SQL 类型 | 路由目标 | 延迟影响 |
|------|---------|---------|---------|
| 配置发布 | INSERT/UPDATE | Master | 无影响（写操作不分离） |
| 配置查询 | SELECT | Slave | 可能读到旧版本（Slave 延迟） |
| 服务注册 | INSERT | Master | 无影响 |
| 服务发现 | SELECT | Slave | 可能丢失刚注册的服务（最终一致） |

**缓解措施**：

1. **强制读 Master（Hint）**：对一致敏感的查询，Nacos 可以使用 ShardingSphere 的 Hint 机制强制路由到 Master
2. **Slave 延迟监控 + 自动降级**：当 `Seconds_Behind_Master > 5s` 时，ProxySQL 自动将读流量切回 Master（`mysql_query_rules` 动态修改 `destination_hostgroup`）

### 设计模式分析

1. **代理模式（Proxy Pattern）**：ShardingSphere-proxy / ProxySQL 作为 MySQL 的代理层，对 Nacos 透明——Nacos JDBC 连接指向中间件端口，无需修改代码或配置感知主从拓扑
2. **读写分离模式（Read/Write Splitting）**：通过 SQL 解析（`SELECT` vs `INSERT/UPDATE/DELETE`）或 Hint 机制将读写路由到不同 MySQL 实例，降低 Master 读压力
3. **观察者模式（Observer Pattern）**：ProxySQL 通过 `mysql_replication_hostgroups` 自动检测 Slave 的 `read_only` 变量变化——Slave 提升为新 Master 后，ProxySQL 检测到 `read_only=OFF`，自动将其移入 hostgroup 0

### 小结

- Nacos 2.5.3 MySQL 高可用需主从复制 + 读写分离中间件——Nacos JDBC URL 只配置单个地址，中间件层透明实现路由
- **ProxySQL 推荐用于 Nacos 场景**：MySQL 协议代理性能更高，动态配置无需重启，内置故障转移，社区成熟
- 读写分离规则：`SELECT` → Slave、`INSERT/UPDATE/DELETE` → Master
- 故障转移：ProxySQL 通过 `mysql_replication_hostgroups` + `read_only` 检测，Master 宕机自动将 Slave 提升为新 Master
- 最终一致性影响：Slave 复制延迟导致配置查询/服务发现可能读到旧版本——通过 Hint 强制读 Master 或延迟监控自动降级缓解

---

## 10.10 OS 级优化：sysctl.conf（file-max / tcp_tw_reuse / keepalive / rmem / wmem）

### 设计背景

Nacos 2.5.3 集群在生产环境中处理大量网络连接——每个微服务客户端与 Nacos 建立至少 丛 条 gRPC 长连接（9848 端口），HTTP API 短连接（8848 端口），以及集群内部 JRaft RPC 通信（7848 端口）。Linux 默认内核参数针对桌面环境优化，在高并发服务端场景下可能导致：

1. **文件描述符耗尽**：Linux 默认单个进程 `nofile` 限制为 1024，Nacos 高并发下数万连接轻松超出
2. **TCP TIME_WAIT 堆积**：HTTP API 短连接频繁断开后端口处于 TIME_WAIT 状态（默认 60s），高 QPS 下耗尽临时端口
3. **TCP KeepAlive 缺失**：默认 TCP KeepAlive 间隔 7200s（2 小时），gRPC 长连接中间网络设备（NAT/防火墙）可能在空闲超时断连接
4. **Socket 缓冲区不足**：默认 rmem/wmem 仅 128KB-256KB，gRPC 大数据传输时可能因缓冲区不足触发 TCP ZeroWindow

本节系统性地整理 Nacos 生产部署所需的 Linux 内核参数优化配置。

### 完整 sysctl.conf 优化配置

```ini
# =========================================================================
# /etc/sysctl.conf - Nacos 2.5.3 生产环境内核参数优化
# =========================================================================

# =========================================================================
# 1. 文件描述符限制（系统级）
# =========================================================================
# 系统全局文件描述符上限
fs.file-max = 655350
# 单个进程文件描述符上限（需配合 limits.conf 设置）
fs.nr_open = 1048576

# =========================================================================
# 2. TCP TIME_WAIT 优化
# =========================================================================
# 启用 TIME_WAIT 快速回收（Nacos HTTP API 高频短连接场景）
# 注意：Linux 4.12+ 已移除 tcp_tw_recycle，仅保留 tcp_tw_reuse
net.ipv4.tcp_tw_reuse = 1

# TIME_WAIT 状态的最大数量（超过此值直接销毁）
# 默认：180000 → 推荐调高到 500000
net.ipv4.tcp_max_tw_buckets = 500000

# orphan socket 最大数量（无属主的 TCP 连接，用于防止 DoS 攻击）
net.ipv4.tcp_max_orphans = 262144

# FIN-WAIT-2 状态超时时间（默认 60s → 推荐 30s，加速释放半关闭连接）
net.ipv4.tcp_fin_timeout = 30

# =========================================================================
# 3. TCP KeepAlive 优化（gRPC 长连接关键）
# =========================================================================
# TCP KeepAlive 首次探测间隔（默认 7200s → 推荐 60s）
# gRPC 客户端默认心跳间隔 5s，若中间 NAT/防火墙 300s 断连接，KeepAlive 60s 可保持连接
net.ipv4.tcp_keepalive_time = 60

# KeepAlive 探测包发送间隔（默认 75s → 推荐 10s）
net.ipv4.tcp_keepalive_intvl = 10

# KeepAlive 探测失败次数（默认 9 次 → 推荐 3 次，加速识别死连接）
net.ipv4.tcp_keepalive_probes = 3

# =========================================================================
# 4. TCP Socket 读写缓冲区优化（gRPC 大消息传输）
# =========================================================================
# Socket 接收缓冲区（默认值 → 推荐值）
# 单位：字节
net.core.rmem_default = 262144      # 默认 256KB
net.core.rmem_max = 16777216      # 最大 16MB（gRPC 大数据传输场景）

# Socket 发送缓冲区
net.core.wmem_default = 262144      # 默认 256KB
net.core.wmem_max = 16777216      # 最大 16MB

# TCP 读缓冲区（自动调整：最小值 / 默认值 / 最大值）
net.ipv4.tcp_rmem = 4096 262144 16777216

# TCP 写缓冲区
net.ipv4.tcp_wmem = 4096 262144 16777216

# TCP 全局接收缓冲区上限（系统级，内存足够可调高）
net.core.netdev_max_backlog = 5000

# =========================================================================
# 5. TCP 连接 Backlog 优化
# =========================================================================
# SYN Backlog 队列大小（应对突增连接请求）
net.ipv4.tcp_max_syn_backlog = 8192

# SYN Queue 溢出时启用 SYN Cookies（防止 SYN Flood 攻击）
net.ipv4.tcp_syncookies = 1

# Listen Backlog 上限（应用程序调用 listen(fd, backlog) 的最大值）
net.core.somaxconn = 4096

# =========================================================================
# 6. TCP 连接重用与快速回收
# =========================================================================
# 启用 TCP Fast Open（TFO）客户端和服务器端
# 允许在 SYN 包中携带数据，减少 1 RTT 延迟
net.ipv4.tcp_fastopen = 3  # 客户端 + 服务端

# 重用处于 TIME_WAIT 的 socket 用于新连接
net.ipv4.tcp_timestamps = 1   # TCP 时间戳（tcp_tw_reuse 的依赖项）

# =========================================================================
# 7. 本地端口范围优化
# =========================================================================
# 扩大临时端口范围（Nacos HTTP API 高频短连接场景）
# 默认：32768-60999 → 推荐：1024-65534
# 可用端口数：28231 → 64510
net.ipv4.ip_local_port_range = 1024 65534

# =========================================================================
# 8. IP 转发（若有 Nginx 作为网关场景）
# =========================================================================
# 启用 IP 转发（Nginx 代理场景跨子网通信）
net.ipv4.ip_forward = 1

# =========================================================================
# 9. Swappiness 与 VM 优化
# =========================================================================
# 降低 Swap 倾向（Nacos JVM 堆内存应保持在物理内存中）
# 默认：60（倾向于使用 Swap）→ 推荐：10（尽量使用物理内存）
vm.swappiness = 10

# 脏页比例（降低 I/O 突发）
vm.dirty_ratio = 15
vm.dirty_background_ratio = 5

# =========================================================================
# 10. 内存过量使用（OOM Killer 优化）
# =========================================================================
# 允许内存过量使用（JVM 需要 commit 超过物理内存的空间）
# 0：启发式处理 → 可能导致 JVM fork 失败
# 1：允许过量使用 → 推荐（JVM 正常启动）
vm.overcommit_memory = 1

# OOM Killer 倾向（降低 Nacos 被 OOM Killer 杀死的概率）
# -1000 ~ 1000：值越小越不容易被 OOM Killer 选中
# Nacos JVM 进程启动后建议：echo -500 > /proc/$(pgrep -f nacos)/oom_score_adj
```

### 关键参数详解

#### fs.file-max

Nacos 集群节点需要同时处理以下连接：

| 连接类型 | 量级估算 | 所需 fd |
|---------|---------|--------|
| gRPC 客户端连接（9848） | N × 微服务实例数 | N × 1（每个客户端 1 条 gRPC 长连接） |
| HTTP API 连接（8848） | 控制台 + 健康检查 + API 调用 | ~100-500 |
| JRaft RPC 内部通信（7848） | (N-1) × 节点数 | 2 (5 节点集群) |
| MySQL 连接池 | HikariCP 连接池（默认 20） | 20 |
| 本地文件操作 | 日志滚动 + Derby 数据文件 | ~50 |

假设 1000 个微服务实例 × 3 Nacos 节点，每个节点承载约 334 个 gRPC 连接 + 500 HTTP 连接 + 20 MySQL 连接 = ~854 fd。生产建议 `fs.file-max = 655350`（约 6.5× 预留）。

#### tcp_tw_reuse

Nacos HTTP API 采用短连接模式（HTTP/1.1 keepalive 可启用长连接但默认短连接）。每个 HTTP 请求经历：

```
客户端 → SYN → Nacos HTTP → SYN-ACK → 客户端 → ACK → HTTP Request → HTTP Response → 客户端 → FIN → Nacos → ACK → Nacos → FIN → 客户端 → ACK → TIME_WAIT (60s)
```

高 QPS 下（例如 5000 QPS），每秒产生 5000 个处于 TIME_WAIT 状态的端口。60s 内堆积 300,000 个 TIME_WAIT 连接。若 `ip_local_port_range` 仅 28231 端口，60s 内端口将耗尽。

`tcp_tw_reuse=1` 允许将处于 TIME_WAIT 的 socket 重用为新的客户端连接（仅对客户端有效，Nacos 作为服务端不适用）。但对于 Nginx 代理 Nacos HTTP API 场景，Nginx 作为客户端与 Nacos 建立连接，`tcp_tw_reuse` 可缓解 Nginx 端的端口耗尽。

#### tcp_keepalive_time

gRPC 长连接依赖底层 TCP 连接保持活跃。若中间网络设备（NAT 网关/防火墙）在空闲超时（常见 300s-600s）断开连接，gRPC 客户端无法感知，直到下一次 gRPC 心跳超时（默认 15s × 3 = 45s）。

TCP KeepAlive 配置 60s 确保即使中间网络设备断开连接，60s 内 TCP KeepAlive 探测也能快速发现死连接。

```
时间线：
0s ─── gRPC 连接建立
5s ─── gRPC 心跳（客户端 → Nacos）
...
300s ── NAT/防火墙断开连接（空闲超时）
360s ── TCP KeepAlive 探测（tcp_keepalive_time=60s）
370s ── TCP KeepAlive 第 2 次探测（tcp_keepalive_intvl=10s）
380s ── TCP KeepAlive 第 3 次探测
380s ── 探测失败（tcp_keepalive_probes=3）→ 关闭连接 → gRPC 重连
```

#### rmem / wmem

Nacos gRPC 双向流可能传输大量服务实例数据（例如 10000 个服务实例 × 每个 500 字节 = ~5MB）。默认 socket 缓冲区 256KB 可能触发 TCP ZeroWindow——接收方缓冲区满，发送方等待。增大 `tcp_rmem` 到 16MB 可容纳大消息单次传输，避免 TCP ZeroWindow 中断双向流。

### 参数验证

```bash
# ================================================================
# 1. 使 sysctl 配置生效
# ================================================================
sysctl -p

# ================================================================
# 2. 验证关键参数
# ================================================================
# 文件描述符上限
sysctl fs.file-max

# TCP TIME_WAIT 重用
sysctl net.ipv4.tcp_tw_reuse

# TCP KeepAlive
sysctl net.ipv4.tcp_keepalive_time

# Socket 缓冲区
sysctl net.core.rmem_max
sysctl net.core.wmem_max

# 临时端口范围
sysctl net.ipv4.ip_local_port_range

# ================================================================
# 3. 检查当前 Nacos 进程的文件描述符使用量
# ================================================================
ls -la /proc/$(pgrep -f nacos)/fd | wc -l

# ================================================================
# 4. 检查 TIME_WAIT 连接数
# ================================================================
ss -tan state time-wait | wc -l
```

### Trade-off 分析

**内核参数激进 vs 保守配置**：

| 参数 | 保守值（默认） | 激进值（本方案） | Trade-off |
|------|--------------|---------------|---------|
| `tcp_keepalive_time` | 7200s | 60s | 更多 KeepAlive 包（网络开销增加但微不足道） |
| `tcp_fin_timeout` | 60s | 30s | 加速释放 FIN-WAIT-2 连接（可能断开半关闭连接过早） |
| `rmem_max` | 256KB | 16MB | 单连接最大占用 16MB 缓冲区（内存压力增大） |
| `tcp_tw_reuse` | 0 | 1 | TIME_WAIT 重用可能混淆 TCP 连接（需 `tcp_timestamps=1` 配合） |
| `swappiness` | 60 | 10 | 更倾向物理内存而非 Swap（JVM 堆保持常驻物理内存） |

**适用场景边界**：

- **物理机部署**：上述参数全部推荐应用，无需顾虑
- **K8s 部署**：部分参数受容器限制（如 `fs.file-max` 是全局系统参数，容器内不可设置），需在 Node 级别配置
- **小规模集群（ < 50 微服务）**：大部分参数保持默认即可，核心只需调整 `fs.file-max` + `tcp_keepalive_time`

### 设计模式分析

1. **缓冲模式（Buffer Pattern）**：Socket 缓冲区（`rmem`/`wmem`）充当生产者和消费者的缓冲中介——Nacos gRPC 双向流作为生产者写入数据到 socket 缓冲区，接收者从缓冲区读取。增大缓冲区降低 TCP ZeroWindow 概率，是生产者-消费者模式在网络层的应用
2. **超时重试模式（Timeout-Retry Pattern）**：TCP KeepAlive 机制是经典超时探测模式——`tcp_keepalive_time`（超时窗口）+ `tcp_keepalive_intvl`（重试间隔）+ `tcp_keepalive_probes`（最大重试次数）→ 探测失败触发连接关闭通知应用层

### 小结

- Nacos 生产 OS 内核参数优化覆盖 10 大类：文件描述符、TIME_WAIT、TCP KeepAlive、Socket 缓冲区、Backlog、Fast Open、端口范围、IP 转发、Swap、OOM
- 核心参数：(1) `fs.file-max=655350` 确保高连接数 (2) `tcp_tw_reuse=1` + `ip_local_port_range=1024 65534` 避免端口耗尽 (3) `tcp_keepalive_time=60` 保持 gRPC 长连接 (4) `rmem_max/wmem_max=16MB` 容纳大数据传输
- `tcp_keepalive_time=60` 与 gRPC 客户端心跳（5s）互补——TCP KeepAlive 在传输层探测死连接，gRPC 心跳在应用层探测健康状态
- K8s 部署需注意部分全局内核参数需在 Node 级别配置，容器内不可设置

---

## 10.11 文件描述符限制：limits.conf（soft / hard nofile / nproc）

### 设计背景

Linux 系统通过 PAM（Pluggable Authentication Modules）的 `limits.conf` 机制限制每个用户或进程的资源使用上限。Nacos 作为 Java 应用，其 JVM 进程受限于以下两类限制：

1. **nofile（打开文件描述符数）**：Nacos 需要同时处理数千个 gRPC 长连接、HTTP 短连接、JRaft RPC 内部通信、MySQL 连接池、日志文件写入等，默认 1024 完全不足
2. **nproc（进程数）**：JVM 附带启动的线程（G1 GC 线程、JRaft 线程池、gRPC I/O 线程池等），默认限制可能不够

`limits.conf` 中的 `soft` vs `hard` 限制：

| 限制类型 | 含义 | 能否突破 |
|---------|------|---------|
| `soft` | 软限制：当前会话生效的限制值 | 普通用户可自行调高（不超过 `hard`） |
| `hard` | 硬限制：软限制的上限 | 仅 root 用户可调高 |

Nacos 启动脚本（`startup.sh`）通常以非 root 用户（如 `nacos`）运行，必须提前在 `limits.conf` 中配置好高 `nofile` 限制。

### limits.conf 完整配置

```ini
# /etc/security/limits.conf - Nacos 2.5.3 资源限制

# =========================================================================
# Nacos 用户资源限制
# =========================================================================
# 格式：<domain> <type> <item> <value>

# nofile - 打开文件描述符上限
nacos            soft    nofile          65535
nacos            hard    nofile          65535

# nproc - 最大进程数（包含线程数）
nacos            soft    nproc           65535
nacos            hard    nproc           65535

# =========================================================================
# Root 用户（用于应急管理）
# =========================================================================
root             soft    nofile          65535
root             hard    nofile          65535

# =========================================================================
# 全局默认限制（所有用户应用）
# =========================================================================
*                soft    nofile          65535
*                hard    nofile          65535
```

### 验证 limits.conf 生效

```bash
# 1. 切换到 nacos 用户
su - nacos

# 2. 查看软限制和硬限制
ulimit -Sn  # soft nofile
ulimit -Hn  # hard nofile
# 预期输出：65535

ulimit -Su  # soft nproc
ulimit -Hu  # hard nproc
# 预期输出：65535

# 3. 确认 Nacos JVM 进程实际的限制
# 启动 Nacos 后检查进程 limits
cat /proc/$(pgrep -f nacos)/limits | grep -E 'Max open files|Max processes'
# 预期输出：
# Max open files    65535    65535    files
# Max processes    65535    65535    processes
```

### Nacos JVM 实际文件描述符使用量估算

Nacos JVM 进程的文件描述符来源：

| 来源 | 数量 | 说明 |
|------|------|------|
| gRPC 连接 | N × 客户端数 | 每个客户端 1 条 TCP 连接（fd） |
| HTTP API 连接 | ~100-500 | 控制台 + API 调用 |
| JRaft RPC | 2-4 | 集群内部通信（每个节点与其他节点 1 条连接） |
| MySQL 连接池 | ~20 | HikariCP 连接池 |
| JAR 文件 | ~10 | JVM 加载的 JAR 文件 |
| 日志文件 | ~10 | 滚动日志文件（nacos-cluster.log 等） |
| 临时文件 | ~10 | Derby 数据文件 / 临时文件 |
| JVM 内部 fd | ~苬0 | JMX RMI / GC 内部 |

**总计估算**：假设 1000 个客户端 × 3 节点 = 每节点约 334 gRPC + 500 HTTP + 20 MySQL + 50 其他 ≈ 904 fd。`65535` 提供 > 70× 余量，完全足够。

### limits.conf vs sysctl fs.file-max 的关系

| 参数 | 作用域 | 设置位置 | 限制对象 |
|------|------|---------|---------|
| `fs.file-max` | 系统全局 | `/etc/sysctl.conf` | 整个系统内核能打开的文件描述符总数 |
| `limits.conf nofile` | 用户进程级 | `/etc/security/limits.conf` | 单个进程能打开的最大文件描述符数 |

关键约束：`nofile ≤ fs.file-max`。若所有 Nacos 节点的 `nofile × N` 超过 `fs.file-max`，系统级上限已耗尽，单个进程即使未触达自己的 `nofile` 也无法开新文件。

### limits.conf 持久化生效验证

```bash
# 1. 修改 limits.conf 后无需重启——新会话自动生效
#    已有会话需重新登录（su - nacos）或执行：
ulimit -n 65535

# 2. 确保 SSH 守护进程加载 PAM limits
# /etc/ssh/sshd_config 需启用：
# UsePAM yes

# 3. 若 Nacos 通过 systemd 管理，需在 service 文件中设置
# /etc/systemd/system/nacos.service
# [Service]
# LimitNOFILE=65535
# LimitNPROC=65535
```

### systemd Service 文件配置（推荐方式）

```ini
# /etc/systemd/system/nacos.service
[Unit]
Description=Nacos Server
After=network.target

[Service]
Type=forking
User=nacos
Group=nacos
Environment="JAVA_HOME=/usr/local/java"
ExecStart=/opt/nacos/bin/startup.sh
ExecStop=/opt/nacos/bin/shutdown.sh
Restart=on-failure

# =========================================================================
# 资源限制（systemd 配置，优先级高于 limits.conf）
# =========================================================================
LimitNOFILE=65535
LimitNPROC=65535
# 内存限制（可选）
MemoryHigh=3G
MemoryMax=4G

# 安全强化
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

### Trade-off 分析

**nofile 过高 vs 过低的风险**：

| nofile 值 | 优点 | 风险 |
|----------|------|------|
| 过低（1024） | 资源占用小 | 高并发连接耗尽 fd → Nacos 无法接受新连接/打开文件失败 |
| 适中（65535） | 足够覆盖绝大多数场景 | 单个进程最多打开 65535 个文件，通常够用 |
| 过高（1048576） | 极致高并发场景有更大余量 | 单个进程可消耗大量内核内存（每个 fd ~1KB）→ 可能耗尽系统内存 |

**建议值**：`65535` 适合 99% Nacos 生产场景。若 10000+ 微服务实例 × 5 集群节点 × 每个客户端 1 gRPC 连接 = 每节点 10000 gRPC 连接 + 其他 ≈ 10500 fd，仍远低于 65535。

### 设计模式分析

1. **资源池模式（Resource Pool Pattern）**：文件描述符是 OS 管理的资源池——`limits.conf` 定义了每个进程可从资源池中分配的最大 fd 数量。类似数据库连接池限制每个应用的连接数上限——防止单个进程耗尽系统全局 fd 资源（Tragedy of the Commons）
2. **门面模式（Facade Pattern）**：`systemd LimitNOFILE` 提供了统一的资源限制接口——无论底层用 PAM limits.conf 还是 cgroup v2，systemd service 文件对运维隐藏底层实现细节

### 小结

- Nacos 生产环境必须将 `nofile` 和 `nproc` 调高至 `65535`，避免高并发下 fd 耗尽导致 Nacos 无法接受新连接
- `limits.conf` 设置软限制（`soft`）和硬限制（`hard`）——普通用户可自行调高软限制（不超过硬限制），仅 root 可调高硬限制
- `systemd LimitNOFILE=65535` 优先级高于 `limits.conf`，若 Nacos 通过 systemd 管理，推荐在 service 文件中直接设置
- `nofile ≤ fs.file-max` 约束——若系统全局 fd 耗尽，单个进程即使未触达自己的 `nofile` 也无法开新文件
- K8s 部署中 Pod 的 `securityContext` 可设置 `limit`: `fsGroup` 实现类似限制，但通常 K8s 资源隔离依赖 cgroup 的 `pids.max` 而非 limits.conf

---
