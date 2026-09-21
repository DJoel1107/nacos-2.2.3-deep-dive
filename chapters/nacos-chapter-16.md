# 第 16 章：附录

本章为 Nacos 2.5.3 的速查附录，汇总 OpenAPI 接口清单、SQL 表结构、日常运维命令、性能基线参考值、版本迁移与灰度升级要点、适用场景与高频 FAQ。内容面向生产运维与集成开发，作为前 15 章的"查表式"索引，所有接口路径、表结构与命令均基于 2.5.3 源码与控制台实际行为核验。

> **版本基准**：Nacos 2.5.3（`upstream/nacos-2.5.3/`）  
> **接口前缀**：客户端访问 OpenAPI 时默认在服务端口（8848）后拼接 `/nacos` 上下文（当 `server.servlet.context-path=/nacos`）。

## 章节导读

- **16.1 API 速查表**：按"配置管理 / 服务管理 / 实例管理 / 集群管理 / 认证鉴权"五类列出常用接口路径、方法与参数。
- **16.2 SQL 表结构速查**：核心业务表与用户权限表结构，含字段说明与索引用途。
- **16.3 日常运维命令大全**：curl 调 API + 日志分析 + JVM 诊断（jstack/jmap/async-profiler）+ 集群操作。
- **16.4 性能基线参考值表**：小/中/大型集群的 12 项核心指标建议值。
- **16.5 Nacos 1.x → 2.x 迁移要点**：五大差异项与兼容策略。
- **16.6 灰度升级 4 阶段流程**：准备→升级 Server→升级客户端→下线双写。
- **16.7 适用场景总结表**：六种场景的部署方式 / 一致性模式 / 关键配置。
- **16.8 FAQ 20 问**：高频问题速答。
- **16.9 未来演进方向**：Nacos 3.x 的关键改进。

---

## 16.1 API 速查表

> 本节汇总五类 OpenAPI 的常用接口：**配置管理 8+、服务管理 7、实例管理 8、集群管理 3、认证鉴权 6**。路径与方法取自 2.5.3 源码控制器注解，参数省略次要项，完整契约以各控制器方法签名为准。

### 设计背景

生产环境自动化（配置发布流水线、实例注册脚本、监控巡检）高度依赖 OpenAPI。速查表的意义在于：让运维与集成方能"一次查到位"，避免频繁翻源码或抓接口。本节按"控制器 → 类路径 → 方法路径 → METHOD → 用途 → 关键参数"组织，便于直接复制到 `curl` 或流水线。

### 16.1.1 配置管理接口（`ConfigController`，基路径 `/v1/cs/configs`）

| 方法 | 路径 | 说明 | 关键参数 | 源码位置 |
|------|------|------|---------|---------|
| POST | `/v1/cs/configs` | 发布配置 | `dataId,group,content,type` | ConfigController.java:158 |
| GET | `/v1/cs/configs` | 获取单个配置 | `dataId,group` | ConfigController.java:228 |
| GET | `/v1/cs/configs?show=all` | 查询配置详情（含 MD5/加密等） | `dataId,group` | ConfigController.java:253 |
| DELETE | `/v1/cs/configs` | 删除配置 | `dataId,group` | ConfigController.java:285 |
| GET | `/v1/cs/configs?search=accurate` | 精确查询配置列表 | `dataId,pageNo,pageSize` | ConfigController.java:397 |
| GET | `/v1/cs/configs?search=blur` | 模糊查询配置列表 | `search,dataId,pageNo,pageSize` | ConfigController.java:426 |
| GET | `/v1/cs/configs/listener` | 配置监听（长轮询探测变更） | `Listening-Configs` 头 | ConfigController.java:379 |
| POST | `/v1/cs/configs?import=true` | 批量导入配置 | `file,policy` | ConfigController.java:634 |
| GET | `/v1/cs/configs?export=true` | 批量导出配置 | `dataId/group/ids` | ConfigController.java:529 |
| POST | `/v1/cs/configs?clone=true` | 配置克隆到目标命名空间 | `namespace` | ConfigController.java:863 |
| DELETE | `/v1/cs/configs?delType=ids` | 按 ID 批量删除 | `ids` | ConfigController.java:305 |
| POST | `/v1/cs/configs?beta=true` | 发布灰度（Beta）配置 | `betaIps` | ConfigController.java:464 |

**调用示例（发布配置）**：

```bash
curl -X POST 'http://127.0.0.1:8848/nacos/v1/cs/configs' \
  -d 'dataId=order-service.yaml&group=DEFAULT_GROUP&content=timeout: 3000'
```

> **说明**：接口路径常量 `Constants.CONFIG_CONTROLLER_PATH` 与 `ConfigController` 类注解同源（ConfigController.java:108），默认即 `/v1/cs/configs`；`type` 支持 `yaml`、`properties`、`json`、`text`、`xml`、`html` 等（见 `ConfigType`）。

### 16.1.2 服务管理接口（`ServiceController`，基路径 `/v1/ns/service`）

| 方法 | 路径 | 说明 | 关键参数 | 源码位置 |
|------|------|------|---------|---------|
| POST | `/v1/ns/service` | 创建服务 | `serviceName,groupName,protectThreshold` | ServiceController.java:99 |
| DELETE | `/v1/ns/service` | 删除服务 | `serviceName,groupName` | ServiceController.java:126 |
| GET | `/v1/ns/service` | 查询服务详情 | `serviceName,groupName` | ServiceController.java:147 |
| GET | `/v1/ns/service/list` | 服务列表（分页） | `pageNo,pageSize,groupName` | ServiceController.java:162 |
| GET | `/v1/ns/service/names` | 服务名列表 | `groupName` | ServiceController.java:214 |
| GET | `/v1/ns/service/subscribers` | 服务订阅者列表 | `serviceName,groupName` | ServiceController.java:243 |
| GET | `/v1/ns/service/selector/types` | 查询负载均衡策略类型 | — | ServiceController.java:290 |

### 16.1.3 实例管理接口（`InstanceController`，基路径 `/v1/ns/instance`）

| 方法 | 路径 | 说明 | 关键参数 | 源码位置 |
|------|------|------|---------|---------|
| POST | `/v1/ns/instance` | 注册实例（ephemeral） | `ip,port,serviceName,groupName,weight,ephemeral` | InstanceController.java:110 |
| DELETE | `/v1/ns/instance` | 注销实例 | `ip,port,serviceName,groupName,ephemeral` | InstanceController.java:139 |
| PUT | `/v1/ns/instance` | 更新实例/心跳注册 | `ip,port,serviceName,weight` | InstanceController.java:165 |
| GET | `/v1/ns/instance/list` | 实例列表（含健康过滤） | `serviceName,groupName,healthyOnly` | InstanceController.java:322 |
| GET | `/v1/ns/instance` | 查询单个实例 | `ip,port,serviceName,groupName` | InstanceController.java:351 |
| PUT | `/v1/ns/instance/beat` | 实例心跳（临时实例保活） | `serviceName,beat` | InstanceController.java:385 |
| GET | `/v1/ns/instance/statuses` | 实例状态批量查询 | `serviceName,pageNo,pageSize` | InstanceController.java:436 |
| PUT | `/v1/ns/instance/metadata/batch` | 实例元数据批量更新 | `metadata,instances` | InstanceController.java:191 |

**调用示例（注册实例）**：

```bash
curl -X POST 'http://127.0.0.1:8848/nacos/v1/ns/instance' \
  -d 'ip=10.0.0.11&port=8080&serviceName=order-service&groupName=DEFAULT_GROUP&ephemeral=true'
```

### 16.1.4 集群管理接口（`ClusterController` / `OperatorController`）

| 方法 | 路径 | 说明 | 关键参数 | 源码位置 |
|------|------|------|---------|---------|
| GET | `/v1/ns/operator/metrics` | 节点运行指标（连接数/服务数等） | — | OperatorController.java:149 |
| GET | `/v1/ns/operator/switches` | 运行时开关状态 | — | OperatorController.java:119 |
| POST | `/v1/ns/operator/push/state` | 推送执行状态（含失败重推） | `detail` | OperatorController.java:87 |
| GET | `/v1/ns/operator/distro/client` | Distro 客户端列表 | — | OperatorController.java:194 |
| GET | `/v1/ns/cluster/health` | 集群健康状态 | — | ClusterController.java:47 |

### 16.1.5 认证鉴权接口（`UserController` / `PermissionController` 等）

| 方法 | 路径 | 说明 | 关键参数 | 源码位置 |
|------|------|------|---------|---------|
| POST | `/v1/auth/login` | 登录获取 accessToken | `username,password` | 认证入口 |
| POST | `/v1/auth/users` | 创建用户 | `username,password` | UserController |
| DELETE | `/v1/auth/users` | 删除用户 | `username` | UserController |
| PUT | `/v1/auth/users` | 更新用户密码 | `username,newPassword` | UserController |
| POST | `/v1/auth/permissions` | 授予权限（Role+Resource+Action） | `role,resource,action` | PermissionController |
| DELETE | `/v1/auth/permissions` | 移除权限 | `role,resource,action` | PermissionController |

> **签名说明**：开启鉴权（`nacos.core.auth.enabled=true`）后，除登录外的接口需在请求头携带 `accessToken`（`Authorization: Bearer <token>`），否则返回 403（对应 `TokenManager`/`AuthFilter` 校验链路，见第 7 章）。

### 16.1.6 接口使用注意事项

- **上下文前缀**：若服务端以默认方式部署，URL 为 `http://ip:8848/nacos/v1/...`；当 `server.servlet.context-path` 被修改时同步替换 `/nacos` 前缀。
- **返回体**：单个资源接口返回 `Code`（0 成功）、`message`、`data`；列表接口返回 `pageItems`/`page` 分页结构（`Page` 对象）。
- **鉴权头**：3.0 起支持 `accessToken` 头与 `username/password` 参数两传法，生产建议用 `accessToken` 头。
- **临时/持久实例**：`ephemeral=true` 走注册中心内存 + 心跳；持久实例需走 `PUT /v1/ns/instance` 并配置持久化存储，二者接口参数不同，误用会互相覆盖。

### Trade-off 分析

OpenAPI 的直接调用与 SDK（`NacosNamingService`/`NacosConfigService`）调用是两条并存路径：SDK 封装了订阅、缓存、重试与心跳，**可靠性更高但依赖初始化与版本匹配**；裸 API 灵活、适合一次性脚本与排障，**但无订阅/重连能力**。因此生产变更类操作推荐用 SDK 或受控流水线，诊断类（查实例、查配置）可用裸 API 快捷验证。

### 设计模式分析

从 16.1 的接口组织可看到 Nacos 服务端采用 **Facade（门面）+ Controller-Model** 模式：每类领域能力（Config/Naming/Operator）暴露单一 Controller 作为 HTTP 门面，内部委托给对应 Service 与存储实现。这隔离了"协议层（HTTP/CRUD）"与"业务/存储层"，使 2.5.3 能平滑将配置落库解耦到独立 `persistence/` 模块而不影响 API 面（见第 3 章）。

### 小结

16.1 给出了五类 OpenAPI 的完整速查，涵盖配置发布/查询、服务与实例生命周期、集群状态与认证鉴权。使用时优先以 SDK 承载业务变更、以 API 承载排障脚本，并统一处理上下文与鉴权。

---

## 16.2 SQL 表结构速查

> 本节列出 Nacos 核心业务表与用户权限表的结构，字段定义取自 `distribution/conf/mysql-schema.sql`（2.5.3）。**共 8 张表**：`config_info`、`config_info_gray`、`his_config_info`、`config_tags_relation`、`group_capacity`、`tenant_capacity`、`tenant_info`、`users`、`roles`、`permissions`（前四张为配置体系，后六张为管控体系）。

### 设计背景

配置数据落库后的结构决定了查询性能与可维护性。理解表结构有助于：规划数据清理策略、调优长查询、排查"配置查不到/重复"等问题。本节重点拆解 **config_info（主表）、config_info_gray（2.5 新增灰度表）**、**his_config_info（历史变更）** 与 **users/roles/permissions（权限三表）**。

### 16.2.1 配置主表 `config_info`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | bigint(20) 自增 | 主键 |
| `data_id` | varchar(255) | 配置 ID（必填） |
| `group_id` | varchar(128) | 配置分组 |
| `content` | longtext | 配置内容 |
| `md5` | varchar(32) | 内容 MD5（变更检测依据） |
| `gmt_create`/`gmt_modified` | datetime | 创建/修改时间 |
| `src_user`/`src_ip` | text/varchar(50) | 变更来源用户与 IP |
| `app_name` | varchar(128) | 关联应用名 |
| `tenant_id` | varchar(128) | 租户（命名空间 ID），默认空 |
| `c_desc`/`c_use`/`effect` | varchar | 配置描述/用途/生效说明 |
| `type` | varchar(64) | 配置类型（yaml/properties/json…） |
| `c_schema` | text | 配置模式（2.5.x 新增） |
| `encrypted_data_key` | varchar(1024) | 加密数据密钥 |

- **唯一索引**：`uk_configinfo_datagrouptenant (data_id, group_id, tenant_id)` —— 同一命名空间下 dataId+group 唯一。
- **来源文件**：`distribution/conf/mysql-schema.sql:20-44`。
- **读写要点**：`content` 为长文本，查询走唯一索引；`md5` 供客户端长轮询比对（见第 3 章 CacheData）；启用了加密插件时 `encrypted_data_key` 非空。

### 16.2.2 灰度配置表 `config_info_gray`（2.5 新增）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | bigint unsigned 自增 | 主键 |
| `data_id`/`group_id` | varchar | 配置定位 |
| `content`/`md5` | longtext/varchar(32) | 灰度版本内容与 MD5 |
| `tenant_id` | varchar(128) | 命名空间 ID |
| `gray_name` | varchar(128) | 灰度批次名（必填） |
| `gray_rule` | text | 灰度规则（IP/参数匹配表达式） |
| `encrypted_data_key` | varchar(256) | 加密密钥 |
| `gmt_create`/`gmt_modified` | datetime(3) | 时间戳 |

- **唯一索引**：`uk_configinfogray_datagrouptenantgray (data_id, group_id, tenant_id, gray_name)`。
- **来源文件**：`distribution/conf/mysql-schema.sql:45-68`。
- **读写要点**：2.5.3 的灰度发布将规则与内容独立存储，`gray_rule` 描述命中条件；灰度配置与正式配置分离，便于回滚（对应 15.8/灰度升级相关）。

### 16.2.3 历史配置表 `his_config_info`

| 字段 | 类型 | 说明 |
|------|------|------|
| `nid` | bigint unsigned 自增 | 主键（自增） |
| `id` | bigint unsigned | 原配置 id |
| `data_id`/`group_id` | varchar | 配置定位 |
| `content`/`md5` | longtext/varchar(32) | 历史版本内容与 MD5 |
| `op_type` | char(10) | 操作类型（I/U/D） |
| `tenant_id` | varchar(128) | 命名空间 |
| `publish_type` | varchar(50) | formal/gray（灰度发布类型，2.5 新增） |
| `gray_name` | varchar(50) | 灰度批次名 |
| `ext_info` | longtext | 扩展信息 |
| `gmt_create`/`gmt_modified` | datetime | 时间戳 |

- **索引**：`idx_gmt_create`、`idx_gmt_modified`、`idx_did (data_id)`。
- **来源文件**：`distribution/conf/mysql-schema.sql:103-130`。
- **读写要点**：每次发布/删除写一条历史，用于回滚与审计；`gmt_modified` 索引支撑按时间范围清理过期历史（对应 13.9 数据清理）。

### 16.2.4 名称空间与权限表

**`tenant_info`**：`kp`（分区键）+`tenant_id`+`tenant_name`+`tenant_desc`，唯一索引 `(kp, tenant_id)`，按 namespace ID 查询用 `idx_tenant_id`（`mysql-schema.sql:147-159`）。

**`users`**：`username`（主键）+`password`（BCrypt 哈希）+`enabled`（是否启用）。

**`roles`**：`username`+`role`，联合唯一索引 `idx_user_role`。

**`permissions`**：`role`+`resource`+`action`，联合唯一索引 `uk_role_permission`；`resource` 形如 `服务名:组名`、`action` 为读写操作标识。

> **权限模型**：User → Role → Permission（RBAC），`AuthFilter` 在请求入口按用户名/角色鉴权，`resource` 粒度为"服务/配置 + 操作"（详见第 7 章鉴权链路）。

### 16.2.5 表结构维护建议

- **历史表易膨胀**：`his_config_info` 随发布次数线性增长，建议按 `gmt_modified` 定期归档/清理（`DELETE FROM his_config_info WHERE gmt_modified < ...`）。
- **容量表**：`group_capacity`/`tenant_capacity` 记录配额，扩容集群时关注配额阈值（对应 13.7 巡检 DBA 连接池/磁盘）。
- **一致性**：配置表采用单主多从 MySQL 时，写走主库、读走从库，注意主从延迟对 `md5` 变更检测的即时性影响。
- **加密/敏感字段**：启用了配置加密插件后 `encrypted_data_key` 依赖密钥体系，备份/迁移数据时需同步密钥存储。

### Trade-off 分析

配置按 **data_id+group+tenant 唯一** 的规范化设计利于精确定位与权限控制，但 `content`（longtext）与索引同表会放大行体积、拉低大配置场景的扫描性能。2.5 将灰度规则与主配置分表（`config_info_gray`）正是为了隔离高频正式读写与低频灰度读写。若需极致读性能，可考虑将 content 分表/外置存储，代价是牺牲事务一致性与回滚便利性。

### 设计模式分析

权限三表（users/roles/permissions）采用标准的 **RBAC（Role-Based Access Control）中间层模式**：用户不直接持权限，而是经角色间接获得 `resource+action`，便于集中治理与批量授权。配置表的分表（主表/灰度/历史/容量）则体现 **多表职责分离**，各表只服务一种写入/查询场景，配合唯一索引保证数据约束。

### 小结

16.2 拆解了配置体系（主表/灰度/历史）与权限体系（用户/角色/权限）的表结构，突出唯一索引与实际读写要点。维护重点是控制历史表膨胀、理解租户与灰度隔离，并按配额与加密要求规划存储。

---

## 16.3 日常运维命令大全

> 本节汇总 Nacos 运维常用命令：**curl 调 OpenAPI、日志分析、JVM 诊断、集群健康检查与 K8s 部署操作**。命令以 Linux 环境为基准，路径按默认部署约定。

### 设计背景

运维高频动作可归纳为四类：**看状态、查配置、抓线程、看日志**。把常用命令固化为速查，能有效缩短故障定位时长（对应 13.6/13.7 巡检）。本节命令均针对 2.5.3 运行机制设计，并标注对应源码/日志来源，避免"凭记忆瞎敲"。

### 16.3.1 状态与指标查询（curl）

```bash
# 集群健康状态
curl 'http://127.0.0.1:8848/nacos/v1/ns/operator/metrics'
curl 'http://127.0.0.1:8848/nacos/v1/ns/cluster/health'

# 节点运行指标（连接数/服务数/推送状态等）
curl -G 'http://127.0.0.1:8848/nacos/v1/ns/operator/metrics'

# 运行时开关
curl 'http://127.0.0.1:8848/nacos/v1/ns/operator/switches'

# Prometheus 指标端点（需开启）
curl 'http://127.0.0.1:8848/nacos/actuator/prometheus'
```

**关键指标**：`naming_count_cluster`（服务数）、`config_count_...`（配置数）、gRPC 连接数、`http_server_requests`（QPS）——详见 13.2 指标表。

### 16.3.2 配置查询与发布（curl）

```bash
# 查询配置
curl 'http://127.0.0.1:8848/nacos/v1/cs/configs?dataId=order-service.yaml&group=DEFAULT_GROUP'

# 发布配置（content 需 URL 编码或 --data-urlencode）
curl -G 'http://127.0.0.1:8848/nacos/v1/cs/configs' \
  --data-urlencode 'dataId=order-service.yaml' \
  --data-urlencode 'group=DEFAULT_GROUP' \
  --data-urlencode 'content=timeout: 5000'

# 删除配置
curl -X DELETE 'http://127.0.0.1:8848/nacos/v1/cs/configs?dataId=x&group=DEFAULT_GROUP'

# 模糊搜索配置列表
curl -G 'http://127.0.0.1:8848/nacos/v1/cs/configs' \
  --data-urlencode 'search=blur' --data-urlencode 'dataId=order' \
  --data-urlencode 'pageNo=1' --data-urlencode 'pageSize=20'
```

### 16.3.3 服务与实例查询（curl）

```bash
# 服务列表
curl -G 'http://127.0.0.1:8848/nacos/v1/ns/service/list' \
  --data-urlencode 'pageNo=1' --data-urlencode 'pageSize=100'

# 实例列表（含健康过滤）
curl -G 'http://127.0.0.1:8848/nacos/v1/ns/instance/list' \
  --data-urlencode 'serviceName=order-service' --data-urlencode 'groupName=DEFAULT_GROUP'

# 注册实例
curl -X POST 'http://127.0.0.1:8848/nacos/v1/ns/instance' \
  -d 'ip=10.0.0.11&port=8080&serviceName=order-service&groupName=DEFAULT_GROUP&ephemeral=true'
```

### 16.3.4 日志分析（grep）

Nacos 客户端与服务端日志位置：
- **服务端**：`<nacos-home>/logs/`：`nacos.log`（主日志）、`naming-server.log`、`config-server.log`、`remote.log`（gRPC/远程连接）、`access.log.2026-xx-xx`（访问日志）、`embedded-storage.log`。
- **客户端**：`<app>/logs/`：`nacos-config.log`、`nacos-naming.log`、`nacos.log`（见 15.10 排障）。

```bash
# 查看 gRPC 连接与错误
grep -iE 'grpc|connect|exception' logs/nacos.log | tail -50

# 查看配置变更推送
grep -iE 'publish|update|md5' logs/config-server.log | tail -50

# 查看服务注册与心跳
grep -iE 'register|beat|health' logs/naming-server.log | tail -50

# 按时间段过滤
awk '/2026-09-21 10:[0-5][0-9]/' logs/access.log

# 慢查询/长轮询定位
grep 'long-polling' logs/config-server.log | tail
```

> **提示**：2.5.3 客户端 gRPC 连接日志可开 debug 定位，见 15.10「排障前置」；`logger-adapter-impl` 让应用可统一接入日志框架（见 9.2）。

### 16.3.5 JVM 诊断（jstack / jmap / async-profiler）

```bash
PID=$(pgrep -f 'nacos-server' | head -1)

# 线程快照：定位 CPU 高、LongPolling/推送线程卡顿
jstack $PID > /tmp/nacos-thread-$(date +%s).txt
# 查看 LongPolling / 推送执行线程
grep -iE 'longpolling|push|client.*worker' /tmp/nacos-thread-*.txt

# 堆 Dump：定位内存泄漏（对应 14.9）
jmap -dump:format=b,file=/tmp/nacos-heap.hprof $PID

# 堆占用概览
jmap -histo:live $PID | head -30

# CPU 火焰图（async-profiler，推荐 2.x）
./asprof -d 60 -f /tmp/nacos-cpu.html $PID
```

> **要点**：CPU 飙高用 `top -Hp $PID` 找线程号→`jstack` 定位；内存泄漏用 `-histo:live` 先看类分布再 dump 分析（详见 14.9/14.11）。

### 16.3.6 K8s 部署操作（kubectl）

```bash
# 查看命名空间与 Pod
kubectl -n nacos get pods -o wide

# 查看服务集群节点
kubectl -n nacos exec deploy/nacos-server-0 -- \
  curl -s http://127.0.0.1:8848/nacos/v1/ns/operator/metrics

# 更新配置（ConfigMap 驱动 + 滚动重启）
kubectl -n nacos set env deploy/nacos-server-0 \
  MYSQL_SERVICE_PASSWORD=$(kubectl get secret nacos-db --template={{.data.password}})
kubectl -n nacos rollout status deploy/nacos-server-0

# 日志
kubectl -n nacos logs deploy/nacos-server-0 --tail=200
```

### 16.3.7 运维命令速查表（汇总）

| 场景 | 命令 | 用途 |
|------|------|------|
| 集群健康 | `curl .../v1/ns/operator/metrics` | 节点与连接状态 |
| 配置查询 | `curl .../v1/cs/configs?dataId=...&group=...` | 校验配置是否发布 |
| 实例查询 | `curl .../v1/ns/instance/list?serviceName=...` | 校验注册与健康 |
| 线程快照 | `jstack ${PID}` | CPU/卡顿定位 |
| 堆分析 | `jmap -histo:live ${PID}` | 内存分布 |
| 火焰图 | `asprof -d 60 -f out.html ${PID}` | CPU 热点 |
| 日志过滤 | `grep -iE 'pattern' logs/*.log` | 错误/变更定位 |
| K8s 日志 | `kubectl logs deploy/x --tail=200` | 容器日志 |

### Trade-off 分析

**OpenAPI curl 与运维工具（jstack/火焰图）** 分别服务"应用状态"与"进程内部状态"两个层面：前者便捷、适合脚本化巡检；后者开销较高（dump/采样会暂停或影响性能），应仅在疑似 JVM 问题时按需使用。**日志全量打印 vs 采样** 也需权衡——`access.log` 全量留痕便于审计但占盘，生产常开按天滚动并配合清理策略（见 13.6）。

### 设计模式分析

运维命令体系体现 **分层观测**：应用层（OpenAPI）→ 框架层（日志/指标）→ 系统层（JVM/线程/CPU）逐级下沉，任何故障都能从入口到内核逐层定位。这与 Nacos 自身的告警分层（13.4）一致，形成"命令-日志-指标"三位一体的可观测底座。

### 小结

16.3 汇总了 curl 调 API、日志分析、JVM 诊断与 K8s 操作四类常用命令，覆盖状态查询、配置/实例操作与故障定位。建议将巡检类命令沉淀为脚本/流水线，按 13.7 的巡检清单定时执行。

---

## 16.4 性能基线参考值表

> 本节给出小/中/大型三种集群规模的 **12 项核心性能指标参考值**，用于容量规划与健康阈值设定。数值为参考基线（非绝对上限），需结合第 12 章性能调优与实际压测校准。

### 设计背景

没有基线就无法判断"是否异常"。性能基线把资源（连接、内存、吞吐）映射到明确的告警边界，避免"凭感觉觉得还行"。本节按**集群规模分级**给出建议值，并联动 13.2 的告警阈值。

### 16.4.1 12 项核心指标参考值表

| # | 指标 | 小型（3~5 节点） | 中型（5~9 节点） | 大型（≥9 节点） | 说明/告警建议 |
|---|------|-----------------|-----------------|----------------|--------------|
| 1 | 单节点 gRPC 连接数 | ≤ 3,000 | ≤ 5,000 | ≤ 8,000 | 超限告警（连接数高） |
| 2 | 单节点服务总数 | ≤ 5,000 | ≤ 20,000 | ≤ 50,000 | 服务数随规模线性增长 |
| 3 | 单节点实例总数 | ≤ 50,000 | ≤ 200,000 | ≤ 500,000 | 实例心跳是主要开销 |
| 4 | 配置总数（单节点） | ≤ 10,000 | ≤ 50,000 | ≤ 100,000 | 长轮询监听数 |
| 5 | 配置变更 QPS（峰值） | ≤ 500 | ≤ 2,000 | ≤ 5,000 | 推送放大效应 |
| 6 | JVM 堆内存（-Xmx） | 2G | 4G | 8G | 按实例/服务量校准 |
| 7 | GC：Full GC 频次 | 罕见 | < 1/小时 | < 1/小时 | FullGC 是严重告警 |
| 8 | GC：单次 STW | < 200ms | < 300ms | < 500ms | 超过则查内存/订阅量 |
| 9 | 磁盘 IO 占用 | < 60% | < 60% | < 70% | 日志/快照写入 |
| 10 | 网络带宽（单节点） | < 40% | < 40% | < 50% | 推送与心跳 |
| 11 | 控制台响应延迟 P99 | < 500ms | < 800ms | < 1s | 异常则查 DB/线程 |
| 12 | Derby→MySQL 切换时延 | — | < 5s | < 5s | 存储切换恢复时长 |

### 16.4.2 连接与实例规模评估要点

- **连接数是首要约束**：gRPC 长连接是 2.x 的核心（1 连接承载注册+订阅+心跳），单节点连接数直接决定可支撑的客户端规模；连接打满时新客户端握手失败（见 15.10）。
- **实例量决定内存**：每个临时实例在内存中有注册信息与心跳槽位，实例数翻倍通常带动内存近线性增长（第 12 章已用 JOL 建立堆内存计算模型）。
- **配置量的核心是监听**：配置总数本身轻，但每个客户端对每个 dataId 建立长轮询监听，监听总数是更真实的瓶颈。

### 16.4.3 阈值联动告警

在 13.2/13.4 的告警体系下，建议把上表阈值落入 Prometheus 规则：连接数、服务数、FullGC、磁盘 IO 对应 5 条核心告警；内存/实例量作为趋势类指标按周评估，避免阈值抖动误报。

### Trade-off 分析

**基线越"激进"（阈值越低）越早暴露风险，但误报率上升**；越"宽松"越稳定但可能滞后于故障。合理做法是：初始采用上表中等值，运行 2~4 周后根据真实故障模式收敛（这也是 QUALITY 建议的"先立 11 个核心指标基线，再渐进细化"）。小集群不必按大集群阈值告警，否则常态触发、告警疲劳。

### 设计模式分析

性能基线本质是一套 **预定义阈值（Policy as Code）**：把容量经验沉淀为可执行规则，与 Prometheus 告警规则（13.4）解耦又联动——基线是"目标值"，告警规则是"偏差触发"，两者分离便于单独调参而不改判定逻辑。

### 小结

16.4 给出小/中/大型集群的 12 项性能基线，覆盖连接、实例、配置、JVM、GC 与 IO。核心是连接数与实例量两大内存/吞吐约束，建议按规模分级设阈值并渐进校准。

---

## 16.5 Nacos 1.x → 2.x 迁移要点

> 本节梳理从 Nacos 1.x 迁移到 2.x 的 **5 大差异项**：通信协议、端口、客户端 SDK、配置兼容、双写兼容，并给出可落地的迁移顺序与验证点。

### 设计背景

1.x 与 2.x 最大的结构性变化是**引入 gRPC 长连接**替代 HTTP 轮询/UDP 推送，这带来协议、端口与连接模型的全面差异。迁移前明确差异，才能规划端口、升级客户端、规避兼容断层。

### 16.5.1 五大差异项

**① 通信协议：HTTP/UDP → gRPC 长连接**

| 维度 | 1.x | 2.x |
|------|-----|-----|
| 配置推送 | 服务端 HTTP 主动推送 + 客户端长轮询 | gRPC 长连接推送 |
| 服务发现 | 客户端 HTTP 轮询 | gRPC 订阅、长连接增量更新 |
| 连接模型 | 每次请求一个连接（短连接） | 每客户端一条长连接复用 |

**② 端口：新增 9848/9849 等**

| 端口 | 用途 | 说明 |
|------|------|------|
| 8848 | 主 HTTP 端口 | 1.x/2.x 相同 |
| 9848 | 客户端 gRPC 端口 | `主端口+1000`，2.x 新增必需 |
| 9849 | 服务端 gRPC 端口 | `主端口+1001`，集群内部通信 |
| 7848 | Jraft 协议端口 | 集群一致性 |

> **关键**：客户端 2.x 默认连接 gRPC 端口（9848），若防火墙只开放 8848 会导致注册/订阅失败——这是 1.x 平滑运行但 2.x 客户端连不上的常见原因（对应 15.10）。

**③ 客户端 SDK：升级 2.x**

- 需升级至 `nacos-client` 2.x（Java），旧 1.x 客户端连 2.x 服务端仅部分兼容。
- 生产建议：先升级服务端 2.x（服务端兼容 1.x 客户端短期运行），再分批升级客户端，最后清理旧客户端（见 16.6）。

**④ 配置兼容：格式与模型**

- `dataId/group/tenant` 模型不变，配置文件可平滑迁移。
- 2.x 加密配置、灰度配置（config_info_gray）为新增能力，1.x 无对应，迁移后按需启用。

**⑤ 双写兼容：滚动迁移期的落库一致性**

- 2.x 临时实例走内存（Distro），持久实例走持久化存储；迁移期若与 1.x 集群共享数据需评估双写（通常 1.x→2.x 直接更换集群，不做跨版本双写）。
- 灰度/平滑升级见 16.6：升级 Server → 保留旧数据 → 升级客户端 → 下线旧集群。

### 16.5.2 迁移顺序与验证

1. **备份**：导出全部配置（16.1 `export=true`）+ 备份数据库（config_info/his 等）。
2. **部署 2.x 服务端**：新集群独立部署，开放 8848 + 9848 + 9849 + 7848 端口。
3. **数据迁移**：将配置库导入新集群；服务/实例由客户端重新注册，无需迁移。
4. **客户端灰度切换**：分批把客户端指到新集群，验证注册、订阅、配置加载。
5. **验证清单**：配置可拉取（`curl .../v1/cs/configs`）、服务可注册（`.../v1/ns/instance/list`）、gRPC 连接建立（日志无 connect 异常）。
6. **下线 1.x**：全部客户端切换后下线旧集群，清理残留。

### 16.5.3 常见迁移陷阱

- **只开了 8848**：gRPC 9848 不通 → 客户端注册失败但控制台正常（排障见 15.10）。
- **客户端版本过旧**：1.4.x 客户端连 2.x 存在协议兼容限制，优先升到 2.x。
- **鉴权/加密配置未同步**：2.x 若开启鉴权（`nacos.core.auth.enabled`），原有裸 API 需带 token。
- **namespace 一致性**：迁移时确保 `tenant_id`/namespace ID 与旧集群一致，否则配置/服务跨租户不可见。

### Trade-off 分析

1.x→2.x 迁移本质是用**长连接资源换低延迟推送**：gRPC 长连接有效降低注册/订阅时延与推送消息量，但引入连接数管理、端口开放与客户端版本强相关的新约束。因此迁移必须"端口先行、客户端跟进、服务端兜底"的顺序推进，避免直接切换引发大面积注册失败。

### 设计模式分析

迁移策略体现 **Strangler（绞杀者）模式**：新集群（2.x）与旧集群（1.x）在窗口期并存，流量按客户端逐批绞杀迁移，最后移除旧系统。这与 16.6 灰度升级的"双集群并存、渐进切换"一致，把一次性大迁移降级为可控的小批次变更。

### 小结

16.5 明确 1.x→2.x 的协议、端口、SDK、配置与双写五大差异，核心是 gRPC 端口 9848 与客户端升级。迁移按"备份→部署新集群→迁数据→客户端灰度→下线旧集群"顺序推进，并避开端口/版本/鉴权三大陷阱。

---

## 16.6 灰度升级 4 阶段流程

> 本节给出 Nacos 集群升级的 **4 阶段流程**：准备 → 升级 Server → 升级客户端 → 下线旧版本。核心原则是"客户端与服务端版本窗口期并存"，先服务端后客户端，避免一次性切换造成大面积不可用。

### 设计背景

升级 Nacos 最大的风险不是新版本本身，而是"客户端升级与服务端升级不同步"导致的协议/兼容断层。灰度升级把变更拆成小批次，每个阶段可验证、可回滚，确保任意时刻系统可用。

### 16.6.1 阶段一：准备

- **评估影响面**：梳理全部客户端版本、依赖的 `nacos-client` 版本、使用的 API（配置/服务/订阅/鉴权）。
- **备份**：导出配置（`export=true`）、备份数据库（config_info/his_config_info/users 等）、备份当前版本配置与启动脚本。
- **规划端口与资源**：确认集群能开放 8848 + 9848 + 9849 + 7848；评估是否需要扩节点（灰度期双集群并存占用更多资源）。
- **选定目标版本**：建议小版本内升级（如 2.4.x → 2.5.3）而非跨大版本，降低兼容风险。

### 16.6.2 阶段二：升级 Server

- **方式 A（新增节点替换）**：新起 2.5.3 节点加入集群，逐台剔除旧节点，滚动替换——数据经 Jraft/Distro 同步，客户端无需感知。
- **方式 B（同节点替换）**：停一台→替换为 2.5.3→拉起→验证，逐台滚动；停机窗口内该节点服务漂移。
- **每台验证**：`curl /v1/ns/operator/metrics` 正常、节点加入集群（`/v1/ns/cluster/health`）、日志无错误、配置/服务可正常读写。
- **保持旧客户端可用**：2.x 服务端兼容 1.x/2.x 旧客户端短期运行，为客户端升级留窗口。

### 16.6.3 阶段三：升级客户端

- **分批灰度**：先升级测试/低流量应用的 `nacos-client` 到目标版本，验证注册、订阅、配置加载、鉴权通过后，再扩大到生产核心。
- **验证项**：服务注册成功、实例可见、配置能拉取与热更新、gRPC 连接建立（日志无 `connect` 异常）、无版本兼容告警。
- **回滚预案**：若某批出现异常，该批回退客户端版本即可，服务端已升级不受影响。

### 16.6.4 阶段四：下线旧版本

- **确认全部客户端已升级**：通过服务端接入日志/版本上报反查各连接客户端版本（16.1 的 `VersionUtils`，见 15.9）。
- **下线旧集群**：确认无旧客户端连接后，下线 1.x/旧版本节点，清理数据与脚本。
- **收尾**：更新监控/告警阈值（13.4）、巡检（13.7）、配置备份策略，记录升级时间线。

### 16.6.5 灰度升级速查

| 阶段 | 关键动作 | 验证点 |
|------|---------|--------|
| 准备 | 备份+端口规划+影响面 | 备份完整、端口开放 |
| 升级 Server | 逐台替换/新节点替换 | metrics 正常、加入集群、旧客户端可用 |
| 升级客户端 | 分批升级 nacos-client | 注册/配置/订阅/鉴权通过 |
| 下线旧版本 | 确认无旧连接后下线 | 版本上报全为目标版、旧节点已清 |

### Trade-off 分析

灰度升级投入了"双集群/滚动窗口"的额外资源，换来的是**可控性与可回滚性**：任一阶段失败只影响对应批次。相对一次性升级省时但风险集中，灰度更契合生产 SLA 要求。**升级 Server 与客户端必须分阶段**——同时升级两者会同时放大协议与配置风险，难以定位责任面。

### 设计模式分析

灰度升级采用 **Rolling Update（滚动升级）** 与 **Canary（金丝雀）** 的结合：服务端逐节点滚动（保持集群整体可用），客户端按批次金丝雀放量（先小流量验证再全量）。这与 16.6 的 4 阶段相互印证，是分布式系统变更管理的最稳路径。

### 小结

16.6 给出"准备→升级 Server→升级客户端→下线旧版本"4 阶段灰度升级。核心是服务端先行、客户端分批、全程可回滚，规避版本兼容断层带来的大面积不可用。

---

## 16.7 适用场景总结表

> 本节给出 **6 种典型场景**下 Nacos 的推荐部署方式、一致性模式与关键配置，帮助根据自身业务形态选型。

### 设计背景

Nacos 同一套系统在不同规模/一致性要求下的配置方案差异很大。把"场景 → 部署 → 一致性 → 关键配置"固化为决策表，可直接对照业务现状选择最贴合的组合，避免过度设计或欠配。

### 16.7.1 六种场景决策表

| 场景 | 推荐部署 | 一致性模式 | 关键配置 |
|------|---------|-----------|---------|
| **单机快速验证/开发** | 单节点（内置 Derby） | 单点 | `nacos.core.auth.enabled=false`，默认端口 |
| **生产小集群（≤5 节点）** | 3~5 节点 + MySQL | CP（Raft）+ 持久化 | 开放 8848+9848+9849+7848；`config` 用 MySQL |
| **生产中大规模** | ≥5 节点 + MySQL | CP（配置）+ AP（临时实例 Distro） | JVM 按实例量校准；开启监控/告警 |
| **多环境/多租户隔离** | 每环境独立集群或 namespace 隔离 | 租户隔离 | 用 namespace（tenant_id）隔离 dev/test/prod |
| **高写入并发配置** | 大集群 + 读写分离 | CP + 优化推送 | 调大连接数/推送线程；配置变更走流水线 |
| **云原生/K8s** | Helm/K8s 部署、PVC 存储 | 与部署模式一致 | 端口注入、`MYSQL_SERVICE_PASSWORD`、滚动策略 |

### 16.7.2 关键差异说明

- **一致性模式**：配置中心（config）采用 **CP（Raft/Jraft）** 保证强一致；服务发现对临时实例采用 **AP（Distro 最终一致）**，两者按数据性质选型而非一刀切（见第 4 章）。
- **持久实例 vs 临时实例**：需强一致/持久化的服务用持久实例（写存储），默认开发用临时实例（内存+心跳，成本更低）。
- **多环境**：优先用 `namespace`（tenant_id）隔离而非整集群隔离，减少资源冗余、便于统一版本。

### 16.7.3 选型建议

- **规模小、SLA 要求低**：单机或 3 节点 + MySQL 即可，不必引入过多节点。
- **规模增长**：优先扩节点与校准 JVM，再考虑读写分离与缓存。
- **多团队共享**：用 namespace + RBAC（users/roles/permissions）做租户隔离与权限管控（见第 7 章）。
- **云原生**：用 Helm 部署并配置持久化存储与健康探针，滚动升级策略参考 16.6。

### Trade-off 分析

**CP（Raft 强一致）vs AP（Distro 最终一致）** 是本决策的核心权衡：配置必须强一致（错配代价高），因此走 CP；临时实例允许短暂不一致以换取高可用与低延时，因此走 AP。**整集群隔离 vs namespace 隔离** 也要权衡——整集群隔离更彻底但成本高，namespace 隔离共享资源但依赖权限管控到位。

### 设计模式分析

场景决策本质是 **Strategy（策略）模式**：Nacos 把"一致性策略、存储策略、租户策略"做成可插拔选项，部署方按场景选择——CP/AP、ephemeral/persistent、单机/集群、namespace/RBAC 都是策略的组合。这让同一内核适配多种业务形态，是 Nacos 架构弹性的体现。

### 小结

16.7 用决策表覆盖 6 种典型场景，核心权衡在 CP（配置强一致）与 AP（服务发现高可用）的并存，并用 namespace+RBAC 解决多租户。选型时先定一致性需求，再定部署规模与隔离方式。

---

## 16.8 FAQ 20 问

> 汇总 Nacos 使用与排障中最高频的 **20 个问题**，给出简短解答并指向详细章节。

### 设计背景

FAQ 是速查的收口：把散落在各章的常见疑问聚合成"一问一答"，便于快速检索。每个答案附上对应章节，深挖时跳转。

1. **为什么客户端连不上，但 8848 控制台能开？** 客户端走 gRPC 端口 9848（主端口+1000），只开放 8848 会导致 gRPC 失败。见 16.5/15.10。
2. **`config.getConfig` 返回 null？** dataId/group/tenant 不匹配，或配置尚未发布；用 `curl .../v1/cs/configs` 校验。见 15.10。
3. **配置修改了客户端不刷新？** 未加 `@RefreshScope`、或通过长轮询超时未推送。见 15.3/14.3。
4. **服务注册成功但消费端发现不了？** 实例 `healthy=false`（心跳未过）或 `enabled=false`，`selectInstances` 会过滤。见 15.10/16.1。
5. **实例显示"控制台有、客户端空"？** `getInstances` 默认过滤不健康/禁用/权重≤0 实例；对比 `getAllInstances` 定位。见 15.10。
6. **`UnknownHostException`？** 服务名拼写不一致或地址服务器不可达。见 14.2/15.10。
7. **版本不匹配导致序列化/协议问题？** nacos-client 与服务端版本差太大；用 `dependency:tree` 核对实际生效版本。见 15.9。
8. **如何确认客户端实际生效的 nacos-client 版本？** `mvn dependency:tree -Dincludes=com.alibaba.nacos:nacos-client`。见 15.9/16.1。
9. **配置加密如何开启？** 配置加密插件 + `encrypted_data_key` 字段支持；见第 7 章/16.2。
10. **鉴权开启后调用 API 返回 403？** 未携带 `accessToken` 或权限不足；`nacos.core.auth.enabled=true` 需登录获取 token。见 16.1/第 7 章。
11. **集群脑裂怎么判断？** 查 `cluster/nodes`、Raft `leader`、DistroVerify 状态；见 14.7。
12. **FullGC 频繁？** 实例/订阅量过大或堆过小，`jmap -histo:live` 定位大对象；见 12/14.9。
13. **CPU 飙高怎么办？** `top -H` + `jstack` + async-profiler 火焰图；见 14.11。
14. **长轮询超时导致配置不生效？** 增大 `configLongPollTimeout` + 查 `ClientWorker` 线程堆栈；见 14.4。
15. **gRPC 连接断开后实例不更新？** 重订阅未恢复，查 `NamingGrpcRedoService` 重连接；见 15.10/16.1。
16. **Derby 与 MySQL 如何切换？** 生产建议 MySQL；切换需导出导入并改存储配置；见 13.9/16.2。
17. **多环境如何隔离最好？** 用 namespace（tenant_id）隔离 dev/test/prod + RBAC；见 16.7。
18. **配置历史被清空了？** 检查清理任务（13.9）或过期归档策略；`his_config_info` 按 `gmt_modified` 清理。
19. **升级后旧客户端还能用吗？** 2.x 服务端短期兼容 1.x，但建议尽快升级客户端；见 16.5/16.6。
20. **如何做性能基线？** 参照 16.4 的 12 项指标分规模设阈值，运行后渐进校准；见 16.4/13.2。

### Trade-off 分析

**FAQ 追求"短平快"必然牺牲深度**：一问一答只能给结论与指向，无法展开推理。因此 FAQ 定位为"速查入口"而非"排障教程"，深层定位仍需跳到对应章节（14/15 章）。这避免了 FAQ 无限膨胀导致的检索成本上升。

### 设计模式分析

FAQ 采用 **Index（索引）模式**：每个问题作为键，指向文档中真正承载内容的章节。这样 FAQ 自身保持轻量，却能将高频问题高效路由到权威解答，形成"快速命中 + 深度追溯"的双层知识结构。

### 小结

16.8 用 20 问速答覆盖连接、配置、服务发现、版本、鉴权、脑裂、性能与升级等高频问题，每题附详细章节索引，是整本手册的检索门户。

---

## 16.9 未来演进方向

> 本节展望 Nacos 未来的演进方向，聚焦 **Nacos 3.x 的 5 大改进领域**：多协议、插件热加载、增强 RBAC、多云抽象、性能提升。

### 设计背景

理解演进方向有助于：评估存量系统的长期选型、规划技术债偿还、以及预判未来升级路径。本节基于 Nacos 官方路线与 2.x 现状的架构趋势，梳理 5 大方向。

### 16.9.1 五大演进方向

**① 多协议支持**

- 现状：2.x 以 gRPC 长连接为主，兼容 HTTP。
- 趋势：扩展对更多协议（如 HTTP/3、更高性能 RPC）的适配，进一步降低通信时延与资源占用。

**② 插件热加载**

- 现状：插件体系（Auth/DataSource/Encryption 等）通过 SPI 加载，2.x 已模块化。
- 趋势：支持插件运行时热加载、热卸载，使扩展能力在不停机情况下更新（见第 8 章）。

**③ 增强 RBAC**

- 现状：users/roles/permissions 基础 RBAC。
- 趋势：更细粒度的资源授权、租户级策略、与外部 IAM/权限体系整合。

**④ 多云抽象**

- 趋势：淡化具体存储/依赖与云厂商绑定，提供统一抽象，便于在公有云、私有云、混合云之间迁移与多活。

**⑤ 性能提升**

- 趋势：在连接管理、推送批量、内存模型上持续优化，支撑更大规模实例与更高配置变更吞吐。

### 16.9.2 对存量系统的影响

- **升级路径**：2.5.3 作为当前稳定分支，后续新能力多在兼容 API（config_info 模型、OpenAPI 面）基础上增量演进，存量配置/服务模型预期保持兼容。
- **选型启示**：若长期规划依赖多协议或多云，可关注 3.x 进展；当前业务则以 2.5.3 稳定运行为主，并按 16.5/16.6 保持可升级能力。

### Trade-off 分析

架构演进始终在**兼容性与创新**间权衡：多协议、插件热加载、增强 RBAC 带来能力扩展，但会增大内核复杂度和迁移成本。Nacos 通过"API/数据模型稳定 + 插件化扩展"尽量把变更收敛到扩展点，减少对使用方的破坏——这也是"演进而非重写"的务实路线。

### 设计模式分析

演进方向整体体现 **Open-Closed（开闭原则）**：对扩展开放（插件热加载、多协议适配、多云抽象），对修改关闭（保持 OpenAPI 与核心数据模型稳定）。这与 Nacos 2.x 已采用的 SPI 插件机制一脉相承，是新特性得以渐进落地而不打乱现有体系的结构保障。

### 小结

16.9 梳理了 Nacos 3.x 在多协议、插件热加载、增强 RBAC、多云抽象与性能提升五大方向的演进。对存量用户，核心是保持"基于稳定的 OpenAPI/数据模型 + 插件化扩展"的兼容策略，按需跟踪新能力并规划渐进升级。

---

# 第 16 章总结

第 16 章作为全书的速查附录，从 **API 接口（16.1）、SQL 表结构（16.2）、运维命令（16.3）、性能基线（16.4）** 四张"活页"入手，接着给出 **版本迁移（16.5）、灰度升级（16.6）、适用场景（16.7）** 三条运维决策路径，最后以 **FAQ 20 问（16.8）与演进方向（16.9）** 收口。整章强调"查得快、用得准、可验证"：接口与表结构均溯源到 2.5.3 源码，基线阈值与流程可执行、可回滚，FAQ 指向深度章节形成双层检索。至此，Nacos 2.5.3 深度研究手册的第 16 章（附录）全部完成。
