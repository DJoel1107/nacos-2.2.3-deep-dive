# 第7章：Nacos 2.5.3 认证安全、控制台、周边模块深度分析

## 7.1 认证流程全链路

Nacos 2.5.3 的认证流程全链路基于 `username/password → BCrypt → AccessToken → JWT` 四步：

```
┌──────────────────────────────────────────────────────┐
│              Nacos 认证流程全链路 (2.5.3)              │
├──────────────────────────────────────────────────────┤
│                                                       │
│  客户端                        Nacos Server            │
│    │                              │                   │
│    │── POST /v1/auth/login ────▶│                   │
│    │   {username, password}      │                   │
│    │                              │── BCrypt验证密码  │
│    │                              │                   │
│    │                              │── 查询users表    │
│    │                              │                   │
│    │                              │── 生成JWT Token │
│    │                              │                   │
│    │◀─ 200 OK {accessToken} ───│                   │
│    │                              │                   │
│    │── GET /v1/cs/configs ─────▶│                   │
│    │   Header: Authorization:     │                   │
│    │   Bearer {accessToken}      │── AuthFilter验证Token│
│    │                              │                   │
│    │                              │── 权限验证      │
│    │                              │   (RBAC角色检查) │
│    │                              │                   │
│    │◀─ 200 OK 或 403 Forbidden ─│                   │
│                                                       │
└──────────────────────────────────────────────────────┘
```

### JWT Token 结构

```json
{
  "sub": "nacos",
  "exp": 1700000000,
  "username": "admin",
  "role": "ROLE_ADMIN"
}
```

| JWT Claim | 说明 |
|-----------|------|
| `sub` | JWT 主体标识（固定为 "nacos"） |
| `exp` | Token 过期时间（Unix 时间戳） |
| `username` | 用户名 |
| `role` | 用户角色 |

### 2.5.3 认证配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.core.auth.enabled` | `false` | 是否启用认证 |
| `nacos.core.auth.system.type` | `nacos` | 认证系统类型 |
| `nacos.core.auth.token.secret.key` | 无默认值 | JWT Token 签名密钥 |
| `nacos.core.auth.token.expire.seconds` | 18000 | Token 过期时间（秒） |

## 7.2 RBAC 权限模型实战

### 3 种预设角色

| 角色 | 权限范围 |
|------|---------|
| **ROLE_ADMIN** | 全局管理员：用户管理、角色管理、权限管理、命名空间管理、配置管理、服务管理 |
| **ROLE_OPERATOR** | 运维操作员：命名空间管理、配置管理、服务管理（不可管理用户/角色/权限） |
| **ROLE_VIEWER** | 只读用户：查看配置、查看服务（不可修改） |

### 权限验证 SQL 示例

```sql
-- 查询用户是否具有某个资源的操作权限
SELECT COUNT(*) FROM permissions p
INNER JOIN roles r ON p.role = r.role
WHERE r.username = 'admin'
  AND p.resource = '/nacos/v1/cs/configs'
  AND p.action = 'w';
```

## 7.3 AuthFilterChain 过滤器链完整源码走读

2.5.3 的 `AuthFilterChain` 包含以下过滤器顺序：

| 顺序 | 过滤器 | 说明 |
|------|--------|------|
| 1 | `JwtAuthenticationFilter` | JWT Token 验证 |
| 2 | `RbacAuthorizationFilter` | RBAC 权限验证 |
| 3 | `RequestContextFilter` | **★ 2.5.3 新增：请求上下文设置** |
| 4 | `ParamCheckerFilter` | **★ 2.5.3 新增：参数校验过滤器** |

## 7.4 ConsoleController：控制台后端 API 全览

`ConsoleController`（路径：`nacos-2.5.3/console/src/main/java/com/alibaba/nacos/console/controller/ConsoleController.java`）：

| API 端点 | HTTP 方法 | 说明 |
|----------|-----------|------|
| `/v1/auth/login` | POST | 用户登录，返回 JWT Token |
| `/v1/auth/users` | POST | 创建用户 |
| `/v1/auth/users/{username}` | PUT | 修改用户 |
| `/v1/auth/users/{username}` | DELETE | 删除用户 |
| `/v1/auth/roles` | POST | 创建角色 |
| `/v1/auth/roles/{role}` | DELETE | 删除角色 |
| `/v1/auth/permissions` | POST | 添加权限 |
| `/v1/auth/permissions/{permissionId}` | DELETE | 删除权限 |
| `/v1/console/health` | GET | 系统健康检查 |
| `/v1/console/health/metrics` | GET | Metrics 指标 |
| `/v1/console/namespaces` | POST | 创建命名空间 |
| `/v1/console/namespaces/{namespaceId}` | GET | 查询命名空间 |
| `/v1/console/namespaces/{namespaceId}` | PUT | 修改命名空间 |
| `/v1/console/namespaces/{namespaceId}` | DELETE | 删除命名空间 |

## 7.5 用户登录 API

```java
// ConsoleController.login() (2.5.3)
@PostMapping("/login")
public Result<String> login(
    @RequestParam("username") String username,
    @RequestParam("password") String password) {
    
    // Step 1: 参数校验★ 2.5.3 新增ParamChecker
    ParamChecker.requireNonNull(username, "username不能为空");
    ParamChecker.requireNonNull(password, "password不能为空");
    
    // Step 2: 认证验证
    LoginIdentityContext context = new LoginIdentityContext();
    context.setParameter("username", username);
    context.setParameter("password", password);
    authPluginService.login(context);
    
    // Step 3: 返回 JWT Token
    String token = context.getToken();
    return Result.success(token);
}
```

## 7.6 命名空间管理

2.5.3 中，命名空间管理已从 Console 模块独立到 **persistence 模块** 统一管理：

| API 端点 | 说明 | 2.5.3 变更 |
|----------|------|------------|
| `POST /v1/console/namespaces` | 创建命名空间 | 数据持久化调用 `NamespacePersistService` |
| `GET /v1/console/namespaces` | 查询命名空间列表 | 同上 |
| `PUT /v1/console/namespaces/{namespaceId}` | 修改命名空间 | 同上 |
| `DELETE /v1/console/namespaces/{namespaceId}` | 删除命名空间 | 同上 |

### namespaceId 生成规则

```java
// namespaceId 生成规则 (2.5.3)
public class NamespaceIdGenerator {
    /**
     ★ namespaceId 生成规则:
     - 用户自定义 namespaceId: 直接使用
     - 自动生成 namespaceId: UUID (无连字符)
     */
    public static String generateNamespaceId(String customNamespaceId) {
        if (StringUtils.isNotBlank(customNamespaceId)) {
            return customNamespaceId;
        }
        return UUID.randomUUID().toString().replace("-", "");
    }
}
```

## 7.7 系统健康检查 API

### 基础健康检查

```
GET /v1/console/health
Response: "UP" or "DOWN"
```

### Metrics 指标

```
GET /v1/console/health/metrics
Response: {
    "status": "UP",
    "details": {
        "naming": {"status": "UP", "instances": 150},
        "config": {"status": "UP", "configs": 320},
        "persistence": {"status": "UP", "dataSource": "MySQL"},
        "raft": {"status": "UP", "isLeader": true}
    }
}
```

**★ 2.5.3 新增**：`persistence` 模块健康状态、"raft" Leader 状态。

## 7.8 Istio MCP 集成

Nacos 2.5.3 的 Istio 集成模块（路径：`nacos-2.5.3/istio/`）支持将 Nacos 注册的服务自动同步到 Istio Service Mesh：

| 类 | 职责 |
|----|------|
| `IstioServiceEntryRegistry` | Istio ServiceEntry 注册器 |
| `McpClient` | MCP（Mesh Configuration Protocol）客户端 |
| `NacosServiceDiscovery` | Nacos 服务发现适配器 |

### Istio 集成配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `nacos.istio.enabled` | `false` | 是否启用 Istio 集成 |
| `nacos.istio.mcp.server.addr` | — | MCP Server 地址 |
| `nacos.istio.sync.period` | 30 | 同步周期（秒） |
| `nacos.istio.domain.suffix` | `svc.cluster.local` | 服务域名后缀 |

## 7.9 CMDB 标签数据管理

`CmdbService`（路径：`nacos-2.5.3/cmdb/src/main/java/com/alibaba/nacos/cmdb/core/CmdbService.java`）提供基于 CMDB 标签的实例匹配：

```java
// CmdbService 标签数据管理 (2.5.3)
public class CmdbService {
    
    /** 按标签匹配实例 */
    public List<Instance> matchInstances(String labelKey, String labelValue) {
        return instanceList.stream()
            .filter(instance -> labelValue.equals(
                instance.getMetadata().get(labelKey)))
            .collect(Collectors.toList());
    }
}
```

## 7.10 AddressServer：独立部署的地址服务器模式

`AddressServer`（路径：`nacos-2.5.3/address/src/main/java/com/alibaba/nacos/address/`）提供独立部署的地址服务器模式：

| API | HTTP 方法 | 说明 |
|-----|-----------|------|
| `/nacos/v1/as/serverlist` | GET | 获取 Nacos Server 列表 |
| `/nacos/v1/as/local/serverlist` | GET | 获取本地 Server 列表 |

### AddressServer 地址服务器集群模式

```
┌──────────────────────────────────────────────────────┐
│           AddressServer 集群模式                     │
├──────────────────────────────────────────────────────┤
│                                                       │
│  Nacos Client ──GET /serverlist──▶ AddressServer    │
│                                    │                │
│                                    │── MySQL/File   │
│                                    │   (Server列表) │
│                                    │                │
│  Nacos Client ◀── [server1:8848,     │                │
│                    server2:8848, ...]  │                │
│                                                       │
│  (然后客户端直连 Nacos Server)                      │
│  Nacos Client ──gRPC──▶ Nacos Server 1 (8848)      │
│                                                       │
└──────────────────────────────────────────────────────┘
```

## 7.11 2.5.3 新增：模块启用过滤器

2.5.3 新增模块级启用过滤器，允许精细控制模块启用/禁用：

| 过滤器 | 模块 | 说明 |
|--------|------|------|
| `ConfigEnabledFilter` | Config | 当 Config 模块禁用时拒绝请求 |
| `NamingEnabledFilter` | Naming | 当 Naming 模块禁用时拒绝请求 |

### 配置示例

```properties
# 禁用 Config 模块（仅启动 Naming 模块）
nacos.config.enabled=false
# 禁用 Naming 模块（仅启动 Config 模块）
nacos.naming.enabled=false
```

## 7.12 2.5.3 新增：Ability 系统对认证的控制

2.5.3 新增的 Ability 系统对认证插件有影响：

| Ability 能力 | 认证相关影响 |
|------------|-------------|
| `AbilityKey.AUTH_ENABLED` | 控制认证是否启用 |
| `AbilityMode.CLIENT_AUTH` | 客户端认证模式 |
| `AbilityStatus.ACTIVE` | 认证状态是否激活 |

---

### 本章统计数据

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| auth 模块 Java 文件 | 22 | **27** | +5 |
| console 模块 Java 文件 | 14 | 12 | -2 |
| 命名空间持久化 | 分散实现 | **persistence 模块统一管理** | ★架构调整 |
| 模块启用过滤器 | 无 | **ConfigEnabledFilter + NamingEnabledFilter** | ★新增 |
| 系统健康检查 | 基础健康检查 | **模块级 + persistence + raft 状态** | ★增强 |

---

> **本章基于 Nacos 2.5.3 源码分析生成。**
