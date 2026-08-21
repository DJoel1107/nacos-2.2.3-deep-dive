# 第9章：Nacos 2.5.3 生产环境部署架构

## 9.1 部署模式全景

Nacos 2.5.3 支持三种部署模式：

| 部署模式 | 数据源 | 集群节点数 | 适用场景 |
|---------|--------|----------|---------|
| **单机模式** | Derby（嵌入式） | 1 | 本地开发、测试 |
| **集群模式** | MySQL（外部） | ≥3 | 生产环境 |
| **Kubernetes** | MySQL（外部） | ≥3 | 云原生环境 |

### 2.5.3 部署变更

| 变更点 | 2.2.3 | 2.5.3 |
|--------|-------|-------|
| Derby 管理 | 嵌入 `config` 模块（`EmbeddedStorageContextHolder`） | **persistence 模块统一管理** |
| MySQL 驱动 | `mysql-connector-java` 8.0.28 | **`mysql-connector-j` 8.2.0** |
| MySQL URL 格式 | 同 | `jdbc:mysql://host:port/nacos?...&useSSL=false` |

## 9.2 单机模式部署

```bash
# 1. 解压发行包
tar -xzf nacos-server-2.5.3.tar.gz
cd nacos

# 2. 单机模式启动（默认使用 Derby 嵌入式数据库）
bin/startup.sh -m standalone

# 3. 验证启动
curl http://localhost:8848/nacos/v1/console/health
```

**★ 2.5.3 变更**：单机模式下，Derby 数据库管理由 `persistence` 模块的 `LocalDataSourceServiceImpl` 实现，配置条件注解 `ConditionStandaloneEmbedStorage` 也已移至 persistence 模块。

## 9.3 3 节点集群架构图 + 完整部署

### 架构拓扑

```
         ┌──────────────┐
         │   Nginx LB   │
         │  HTTP 80/443 │
         │  TCP 8848    │
         │  TCP 9848(gRPC)│
         └──────┬───────┘
                │
    ┌───────────┼───────────┐
    │           │           │
┌───▼───┐  ┌──▼────┐  ┌───▼───┐
│Nacos 1 │  │Nacos 2│  │Nacos 3 │
│ :8848  │  │ :8848 │  │ :8848  │
│ :9848  │  │ :9848 │  │ :9848  │
│ :9849  │  │ :9849 │  │ :9849  │
└───┬───┘  └──┬────┘  └───┬───┘
    │           │           │
    └───────────┼───────────┘
                │
         ┌──────▼──────┐
         │   MySQL 主   │
         │   :3306     │
         └─────────────┘
```

### 部署 4 步骤

#### Step 1：MySQL 初始化

```sql
-- 创建 nacos_config 数据库
CREATE DATABASE IF NOT EXISTS nacos_config DEFAULT CHARACTER SET utf8mb4;

-- 导入初始化 SQL（2.5.3）
source conf/mysql-schema.sql;
```

**★ 2.5.3 变更**：`mysql-schema.sql` 新增 `persistence` 模块相关表的初始化语句。

#### Step 2：cluster.conf 配置

```bash
# conf/cluster.conf
192.168.1.1:8848
192.168.1.2:8848
192.168.1.3:8848
```

#### Step 3：application.properties 配置

```properties
# ★★★ 2.5.3 持久化配置 ★★★
spring.datasource.platform=mysql棉
db.num=1
db.url.0=jdbc:mysql://192.168.1.100:3306/nacos_config?characterEncoding=utf8&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useSSL=false
db.user.0=nacos
db.password.0=nacos_2.5.3_secret

# ★ 2.5.3: 驱动类（包名变更）
# 无需手动配置，Spring Boot 自动检测 com.mysql.cj.jdbc.Driver
```

#### Step 4：启动验证

```bash
# 在所有3个节点上依次启动
bin/startup.sh

# 验证集群状态
curl http://192.168.1.1:8848/nacos/v1/core/cluster/nodes
# 期望响应：
{
  "code": 200,
  "message": "success",
  "data": [
    {"ip": "192.168.1.1", "port": 8848, "state": "UP"},
    {"ip": "192.168.1.2", "port": 8848, "state": "UP"},
    {"ip": "192.168.1.3", "port": 8848, "state": "UP"}
  ]
}
```

## 9.4 Nginx 负载均衡配置

### gRPC 端口说明

| 端口 | 计算公式 | 用途 |
|------|---------|------|
| HTTP 主端口 | `server.port`（默认 8848） | HTTP REST API |
| gRPC SDK Server | `server.port + 1000`（默认 9848） | 客户端 SDK ↔ 服务端 gRPC |
| gRPC Cluster Server | `server.port + 1001`（默认 9849） | 集群节点间 gRPC |

### Nginx 配置

```nginx
upstream nacos-backend {
    server 192.168.1.1:8848 weight=1 max_fails=iat2 fail_timeout=10s;
    server 192.168.1.2:8848 weight=1 max_fails=2 fail_timeout=10s;
    server 192.168.1.3:8848 weight=1 max_fails=2 fail_timeout=10s;
}

upstream nacos-grpc-sdk {
    # ★ gRPC 需要 TCP Stream 代理（不能 HTTP）
    server 192.168.1.1:9848 weight=1 max_fails=2 fail_timeout=10s;
    server 192.168.1.2:9848 weight=1 max_fails=2 fail_timeout=10s;
    server 192.168.1.3:9848 weight=1 max_fails=2 fail_timeout=10s;
}

server {
    listen 80;
    server_name nacos.example.com;
    location /nacos/ {
        proxy_pass http://nacos-backend/nacos/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}

# gRPC TCP Stream 代理（需要 Nginx stream 模块）
stream {
    upstream nacos-grpc {
        server 192.168.1.1:9848;
        server 192.168.1.2:9848;
        server 192.168.1.3:9848;
    }
    server {
        listen 9848;
        proxy_pass nacos-grpc;
    }
}
```

## 9.5 5 节点集群架构

5 节点集群 = 3 Raft 节点 + 2 Learner 节点：

| 节点角色 | 数量 | 职责 |
|---------|------|------|
| **Raft Leader** | 1 | 处理所有 CP 写请求 |
| **Raft Follower** | 2 | 接收 Log Replication |
| **Learner** | 2 | 只读（不参与 Leader 选举） |

### 读写分离优势

- **写请求**：仅 Raft Leader 处理
- **读请求**：Learner 节点可分担读压力
- **可用性**：5 节点可容忍最多 2 节点故障

## 9.6 Kubernetes StatefulSet 部署

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nacos-headless
spec:
  clusterIP: None
  selector:
    app: nacos
  ports:
    - name: http
      port: 8848
    - name: grpc-sdk
      port: 9848
    - name: grpc-cluster
      port: 9849

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nacos
spec:
  serviceName: nacos-headless
  replicas: 3
  selector:
    matchLabels:
      app: nacos
  template:
    metadata:
      labels:
        app: nacos
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: nacos
              topologyKey: kubernetes.io/hostname
      containers:
        - name: nacos
          image: nacos/nacos-server:v2.5.3
          ports:
            - containerPort: 8848
              name: http
            - containerPort: 9848
              name: grpc-sdk
            - containerPort: 9849
              name: grpc-cluster
          env:
            - name: NACOS_SERVERS
              value: "nacos-0.nacos-headless:8848,nacos-1.nacos-headless:8848,nacos-2.nacos-headless:8848"
            - name: MYSQL_SERVICE_HOST
              value: "mysql-svc"
            - name: MYSQL_SERVICE_DB_NAME
              value: "nacos_config"
            - name: MYSQL_SERVICE_USER
              value: "nacos"
            - name: MYSQL_SERVICE_PASSWORD
              value: "nacos_password"
          volumeMounts:
            - name: nacos-data
              mountPath: /home/nacos/data
  volumeClaimTemplates:
    - metadata:
        name: nacos-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
```

## 9.7 K8s 部署常用命令

```bash
# 应用 YAML
kubectl apply -f nacos-statefulset.yaml

# 查看 Pod 状态
kubectl get pods -l app=nacos

# 查看日志
kubectl logs nacos-0 -f

# 扩缩容
kubectl scale statefulset nacos --replicas=5

# 滚动更新
kubectl rollout restart statefulset nacos

# 端口转发（本地调试）
kubectl port-forward nacos-0 8848:8848
```

## 9.8 多数据中心双活架构

### 架构拓扑

```
                ┌─────────────┐
                │   GeoDNS   │
                │ 智能 DNS   │
                └──────┬──────┘
                       │
          ┌────────────┼────────────┐
          │            │            │
    ┌─────▼─────┐ ┌──▼───────┐ │
    │ 中心 A     │ │ 中心 B    │ │
    │ (Region A) │ │ (Region B)│ │
    │            │ │           │ │
    │ Nacos × 3 │ │ Nacos × 3│ │
    │ MySQL主   │ │ MySQL从   │ │
    │ namespace: │ │ namespace:│ │
    │  RegionA_  │ │  RegionB_ │ │
    └────────────┘ └───────────┘ │
            │            │        │
            └────────────┼────────┘
                         │
                  ┌──────▼──────┐
                  │   MySQL主-从 │
                  │   异步复制   │
                  └─────────────┘
```

### 关键配置

| 配置 | 中心 A | 中心 B |
|------|--------|--------|
| 命名空间前缀 | `RegionA_` | `RegionB_` |
| MySQL 角色 | Master | Slave（只读） |
| 服务发现策略 | 就近+兜底 | 就近+兜底 |

## 9.9 MySQL 主从复制

### 主库配置

```sql
-- my.cnf
[mysqld]
server-id=1
log-bin=mysql-bin
binlog-format=ROW
sync-binlog=1
```

### 从库配置

```sql
-- my.cnf
[mysqld]
server-id=2
relay-log=mysql-relay-bin
read-only=1
```

### 建立主从复制

```sql
-- 从库执行
CHANGE MASTER TO
  MASTER_HOST='192.168.1.100',
  MASTER_USER='repl',
  MASTER_PASSWORD='repl_password',
  MASTER_LOG_FILE='mysql-bin.000001',
  MASTER_LOG_POS=107;
START SLAVE;
```

## 9.10 OS 级优化

```bash
# /etc/sysctl.conf

# 最大文件描述符数
fs.file-max = 655360

# TCP 连接复用
net.ipv4.tcp_tw_reuse = 1

# TCP KeepAlive
net.ipv4.tcp_keepalive_time = 1200
net.ipv4.tcp_keepalive_probes = 3
net.ipv4.tcp_keepalive_intvl = 30

# Socket 缓冲区
net.core.rmem_default = 262144
net.core.rmem_max = 16777216
net.core.wmem_default = 262144
net.core.wmem_max = 16777216

# TCP 半连接队列
net.ipv4.tcp_max_syn_backlog = 8192

# 本地端口范围
net.ipv4.ip_local_port_range = 1024 65535

# 应用配置
sysctl -p
```

## 9.11 文件描述符限制

```bash
# /etc/security/limits.confverted
* soft nofile 655360
* hard nofile 655360
* soft nproc 40960
* hard nproc 40960
```

验证：

```bash
# 查看 Nacos 进程限制
cat /proc/$(pgrep -f nacos)/limits | grep "Max open files"
```

---

### 本章关键部署版本信息

| 组件 | 2.2.3 | 2.5.3 |
|------|-------|-------|
| MySQL 驱动 | `mysql-connector-java` 8.0.28 | **`mysql-connector-j` 8.2.0** |
| Derby 管理 | `config` 模块 | **`persistence` 模块统一管理** |
| Spring Boot | 2.6.14 | **2.7.18** |
| Nacos 镜像 | `nacos/nacos-server:v2.2.3` | **`nacos/nacos-server:v2.5.3`** |

---

> **本章基于 Nacos 2.5.3 部署架构生成。**
