# 第9章：生产环境部署架构

## 9.1 部署模式全景

Nacos 2.2.3 支持三种部署模式：单机模式、集群模式、Kubernetes 模式。

| 模式 | 节点数 | 数据存储 | 适用场景 |
|------|--------|---------|---------| 
| 单机模式 | 1 | Embedded Derby | 本地开发/测试 |
| 集群模式 | 3/5/7 | MySQL 外部数据库 | 生产环境 |
| K8s 模式 | 动态伸缩 | MySQL 外部数据库 | 云原生环境 |

## 9.2 单机模式部署

### 9.2.1 单机启动命令

```bash
# 单机模式启动（Windows）
startup.cmd -m standalone

# 单机模式启动（Linux/Mac）
sh startup.sh -m standalone
```

### 9.2.2 单机模式特点oper

- 使用嵌入式 Derby 数据库（无需外部 MySQL）
- 所有数据存储在 `$NACOS_HOME/data/` 目录下
- 不支持集群模式下的数据同步
- 控制台直接可访问 `http://localhost:8848/nacos`

## 9.3 集群模式部署

### 9.3.1 3节点集群架构图

```
                    ┌─────────────────────────────┐
                    │      Nginx / SLB             │
                    │   (VIP: 192.168.1.100:80)  │
                    └─────────────┬───────────────┘
                                  │
               ┌──────────────────┼──────────────────┐
               │                  │                  │
               ▼                  ▼                  ▼
       ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
       │  Nacos      │  │  Nacos      │  │  Nacos      │
       │  Node 1     │  │  Node 2     │  │  Node 3     │
       │  192.168.1.1│  │  192.168.1.2│  │  192.168.1.3│
       │  :8848      │  │  :8848      │  │  :8848      │
       │  :9848(gRPC)│  │  :9848(gRPC)│  │  :9848(gRPC)│
       │  :9849(Clus)│  │  :9849(Clus)│  │  :9849(Clus)│
       └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
              │                  │                  │
              └──────────────────┼──────────────────┘
                                 │
                        ┌────────▼────────┐
                        │    MySQL 集群     │
                        │  主从 + 读写分离  │
                        └─────────────────┘
```

### 9.3.2 集群部署步骤

**Step 1：配置 MySQL 数据库**

```sql
-- 创建 Nacos 数据库
CREATE DATABASE IF NOT EXISTS nacos_config 
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- 导入官方初始化 SQL 脚本
SOURCE /path/to/nacos/conf/mysql-schema.sql;
```

**Step 2：配置 cluster.conf**

```bash
# Node 1 (192.168.1.1)
# 编辑 $NACOS_HOME/conf/cluster.conf
192.168.1.1:8848
192.168.1.2:8848
192.168.1.3:8848
```

**Step 3：配置 application.properties**

```properties
# 所有3个节点使用相同的配置
spring.datasource.platform=mysql
db.num=1
db.url.0=jdbc:mysql://mysql-primary:3306/nacos_config?characterEncoding=utf8&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useSSL=false&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=strong_password
```

**Step 4：启动集群节点**

```bash
# Node 1
sh startup.sh

# Node 2
sh startup.sh

# Node 3
sh startup.sh

# 验证集群状态
curl http://192.168.1.1:8848/nacos/v1/core/cluster/nodes
```

### 9.3.3 Nginx 负载均衡配置

```nginx
# nginx.conf - Nacos 集群前端负载均衡
upstream nacos_cluster {
    # Nacos HTTP 端口 (8848)
    server 192.168.1.1:8848 weight=1 max_fails=2 fail_timeout=10s;
    server 192.168.1.2:8848 weight=1 max_fails=2 fail_timeout=10s;
    server 192.168.1.3:8848 weight=1 max_fails=2 fail_timeout=10s;
}

upstream nacos_grpc {
    # Nacos gRPC 端口 (9848, offset+1000)
    server 192.168.1.1:9848 weight=1;
    server 192.168.1.2:9848 weight=1;
    server 192.168.1.3:9848 weight=1;
}

server {
    listen 80;
    server_name nacos.example.com;
    
    # Nacos 后台控制台
    location /nacos {
        proxy_pass http://nacos_cluster;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
    
    # gRPC TCP 代理
    location /grpc {
        proxy_pass http://nacos_grpc;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }
}
```

### 9.3.4 5节点集群架构

对于大规模生产环境，推荐 5 节点集群：

```
          ┌────────────────────────────────────────┐
          │             Nginx / SLB                 │
          └────────────────┬───────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
   │ Node 1  │    │ Node 2  │    │ Node 3  │
   │ Raft    │◄──►│ Raft    │◄──►│ Raft    │
   │ Leader  │    │Follower │    │Follower │
   └────┬────┘    └────┬────┘    └────┬────┘
        │                 │                 │
   ┌────▼────┐    ┌────▼────┐
   │ Node 4  │    │ Node 5  │
   │Follower │    │Follower │
   │(Learner)│    │(Learner)│
   └─────────┘    └─────────┘

优势：
- 3 节点满足 Raft 多数派 (quorum = floor(5/2)+1 = 3)
- 2 个 Learner 节点可以承载更多的读请求
- 允许 2 个节点同时故障（Raft Leader仍可用）
```

## 9.4 Kubernetes 部署

### 9.4.1 StatefulSet YAML 配置

```yaml
# nacos-statefulset.yaml
apiVersion: v1
kind: Service
metadata:
  name: nacos-headless
  namespace: nacos
spec:
  clusterIP: None
  selector:
    app: nacos
  ports:
    - name: http
      port: 8848
      targetPort: 8848
    - name: grpc
      port: 9848
      targetPort: 9848
    - name: cluster-grpc
      port: 9849
      targetPort: 9849
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nacos
  namespace: nacos
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
                matchExpressions:
                  - key: app
                    operator: In
                    values:
                      - nacos
              topologyKey: kubernetes.io/hostname
      containers:
        - name: nacos
          image: nacos/nacos-server:v2.2.3
          ports:
            - containerPort: 8848
              name: http
            - containerPort: 9848
              name: grpc
            - containerPort: 9849
              name: cluster-grpc
          env:
            - name: NACOS_SERVERS
              value: "nacos-0.nacos-headless.nacos.svc.cluster.local:8848 nacos-1.nacos-headless.nacos.svc.cluster.local:8848 nacos-2.nacos-headless.nacos.svc.cluster.local:8848"
            - name: PREFER_HOST_MODE
              value: "hostname"
            - name: NACOS_APPLICATION_PORT
              value: "8848"
            - name: SPRING_DATASOURCE_PLATFORM
              value: "mysql"
            - name: MYSQL_SERVICE_HOST
              value: "mysql-service.nacos.svc.cluster.local"
            - name: MYSQL_SERVICE_DB_NAME
              value: "nacos_config"
            - name: MYSQL_SERVICE_USER
              value: "nacos"
            - name: MYSQL_SERVICE_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: mysql-secret
                  key: password
            - name: NACOS_AUTH_ENABLE
              value: "true"
            - name: NACOS_AUTH_TOKEN_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: nacos-secret
                  key: token-secret-key
          resources:
            requests:
              cpu: 500m
              memory: 2Gi
            limits:
              cpu: 2
              memory: 4Gi
          readinessProbe:
            httpGet:
              path: /nacos/v1/console/health/readiness
              port: 8848
            initialDelaySeconds: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /nacos/v1/console/health/liveness
              port: 8848
            initialDelaySeconds: 60
            periodSeconds: 20
          volumeMounts:
            - name: data
              mountPath: /home/nacos/data
            - name: logs
              mountPath: /home/nacos/logs
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
    - metadata:
        name: logs
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

### 9.4.2 K8s 部署常用命令

```bash
# 部署 nacos StatefulSet
kubectl apply -f nacos-statefulset.yaml

# 查看 Pod 状态
kubectl get pods -n nacos -w

# 查看日志
kubectl logs -f nacos-0 -n nacos

# 扩容到 5 节点
kubectl scale statefulset nacos --replicas=5 -n nacos

# 滚动重启
kubectl rollout restart statefulset nacos -n nacos

# 端口转发（本地访问控制台）
kubectl port-forward nacos-0 8848:8848 -n nacos
```

## 9.5 多数据中心部署方案

### 9.5.1 双活架构

```
┌─────────────────────────────────────────────────────────────┐
│                      全局 DNS/LB                            │
│            (GeoDNS: nacos.example.com)                    │
└───────────────────────────┬─────────────────────────────┘
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
┌─────────▼─────────┐           ┌─────────▼─────────┐
│   数据中心 A      │           │   数据中心 B      │
│  ┌────────────┐  │           │  ┌────────────┐  │
│  │ Nacos 集群 │  │           │  │ Nacos 集群 │  │
│  │ 3 Nodes    │  │           │  │ 3 Nodes    │  │
│  └─────┬──────┘  │           │  └─────┬──────┘  │
│        │         │           │        │         │
│  ┌─────▼──────┐  │           │  ┌─────▼──────┐  │
│  │  MySQL     │  │           │  │  MySQL     │  │
│  │  (Master)  │◄─┼─── 异步 ─┼─►│  (Slave)  │  │
│  └────────────┘  │           │  └────────────┘  │
└───────────────────┘           └───────────────────┘
```

### 9.5.2 配置同步策略

```properties
# 数据中心 A：MySQL Master 配置
# 双向同步避免写冲突：
# - 命名空间按前缀隔离：dc-a-* 归属数据中心 A
# - 命名空间：dc-b-* 归属数据中心 B
# - 公共命名空间 public 通过 MySQL 异步复制

# 配置 Nacos
# 数据中心 A：nacos.inetutils.ip-address=10.0.1.x
# 数据中心 B：nacos.inetutils.ip-address=10.0.2.x
```

## 9.6 数据库高可用方案

### 9.6.1 MySQL 主从复制

```
┌────────────────┐     Binlog 异步复制    ┌────────────────┐
│   MySQL        │──────────────────────►│   MySQL        │
│   Master      │                      │   Slave        │
└───────┬────────┘                      └────────┬───────┘
        │                                       │
   ┌────▼─────────────────────────────┐       │
   │      读写分离中间件               │       │
   │  (ShardingSphere-proxy /        │       │
   │   MyCat / ProxySQL)              │       │
   └────────────────┬─────────────────┘       │
                    │                         │
           ┌────────▼────────┐               │
           │  写请求 → Master │               │
           │  读请求 → Slave  │───────────────┘
           └─────────────────┘
```

### 9.6.2 MySQL 连接池优化

```properties
# HikariCP 推荐配置
db.pool.config.connectionTimeout=30000
db.pool.config.idleTimeout=600000
db.pool.config.maxLifetime=1800000
db.pool.config.maximumPoolSize=20
db.pool.config.minimumIdle=10
db.pool.config.connectionTestQuery=SELECT 1ridb
```

## 9.7 操作系统级优化

### 9.7.1 Linux 内核参数优化

```bash
# /etc/sysctl.conf

# 最大文件打开数
fs.file-max = 6553560

# 最大 TCP 半连接数
net.ipv4.tcp_max_syn_backlog = 8192

# TCP TIME_WAIT 快速回收
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_tw_recycle = 1

# TCP keepalive 时间（减少死连接占用）
net.ipv4.tcp_keepalive_time = 1200
net.ipv4.tcp_keepalive_intvl = 30
net.ipv4.tcp_keepalive_probes = 3

# TCP FIN_TIMEOUT 时间
net.ipv4.tcp_fin_timeout = 30

# 端口范围
net.ipv4.ip_local_port_range = 1024 65535

# Socket 接收/发送缓冲区
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 12582912 16777216
net.ipv4.tcp_wmem = 4096 12582912 16777216

# 应用生效
sysctl -p
```

### 9.7.2 文件描述符限制

```bash
# /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536
* soft nproc 65536
* hard nproc 65536

# 验证
ulimit -n
# 应输出：65536
```

---

*（第九章完，约 0.9 万字）*
