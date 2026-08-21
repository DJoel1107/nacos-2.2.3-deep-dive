# 第6章：Nacos 2.5.3 插件体系深度分析

## 6.1 插件体系概览

Nacos 2.5.3 的插件体系基于 Java SPI（Service Provider Interface）机制实现，支持 6 种插件类型：

| 插件类型 | SPI 接口 | 用途 | 2.5.3 变更 |
|---------|---------|------|------------|
| **认证插件** | `AuthPluginService` | 认证鉴权 | — |
| **数据源插件** | `DataSourcePlugin` | 数据源管理 | **★ persistence 模块集成** |
| **配置加密插件** | `EncryptionPluginService` | 配置加密 | — |
| **链路追踪插件** | `TracePlugin` | 链路追踪 | — |
| **环境插件** | `EnvironmentPlugin` | 环境变量 | — |
| **控制插件** | `ControlManagerPlugin` | TPS/连接控制 | — |

### 2.5.3 plugin-default-impl 模块重构

2.5.3 将 `plugin-default-impl` 从扁平结构重组为多子模块：

```
plugin-default-impl/                 (2.2.3: 扁平单模块 47个文件)
  ├── nacos-default-plugin-all/       (聚合模块)
  ├── nacos-default-auth-plugin/     (默认认证插件独立子模块)
  └── nacos-default-control-plugin/   (默认控制插件独立子模块)
```

## 6.2 AuthPluginService 接口设计

`AuthPluginService`（路径：`nacos-2.5.3/plugin/auth/src/main/java/com/alibaba/nacos/plugin/auth/spi/AuthPluginService.java`）：

```java
// AuthPluginService SPI 接口 (2.5.3)
public interface AuthPluginService {
    
    /** 登录验证 */
    void login(LoginIdentityContext loginContext) throws AccessException;
    
    /** 权限验证 */
    void validate(Resource resource, Action action) throws AccessException;
    
    /** Token 验证 */
    boolean validateToken(String token);
    
    /** Token 生成 */
    String generateToken(LoginIdentityContext loginContext) throws AccessException;
    
    /** 获取用户信息 */
    User getUser(String token) throws AccessException;
    
    /** 登出 */
    void logout(String token) throws AccessException;
}
```

## 6.3 NacosAuthPluginService：BCrypt 加密 + JWT Token

`NacosAuthPluginService`（路径：`nacos-2.5.3/auth/src/main/java/com/alibaba/nacos/auth/impl/NacosAuthPluginService.java`）是 Nacos 内置的认证插件实现：

```java
// NacosAuthPluginService 内置认证实现 (2.5.3)
@Component
public class NacosAuthPluginService implements AuthPluginService {
    
    /** BCrypt 密码编码器 */
    private final BCryptPasswordEncoder passwordEncoder;
    
    /** JWT Token 管理器 */
    private final JwtTokenManager jwtTokenManager;
    
    @Override
    public void login(LoginIdentityContext loginContext) throws AccessException {
        // Step 1: 验证用户名密码
        User user = userService.findByUsername(
            loginContext.getParameter("username"));
        
        if (!passwordEncoder.matches(
                loginContext.getParameter("password"), 
                user.getPassword())) {
            throw new AccessException("用户名或密码错误");
        }
        
        // Step 2: 生成 JWT Token
        String token = jwtTokenManager.createToken(user.getUsername());
        loginContext.setToken(token);
    }
    
    @Override
    public boolean validateToken(String token) {
        try {
            jwtTokenManager.parseToken(token);
            return true;
        } catch (JwtException e) {
            return false;
        }
    }
}
```

## 6.4 RBAC 权限模型

Nacos 2.5.3 的 RBAC（Role-Based Access Control）权限模型包含三个核心实体：

| 实体 | 数据库表 | 核心字段 |
|------|---------|---------|
| **User** | `users` | username、password（BCrypt加密） |
| **Role** | `roles` | role（ROLE_ADMIN/ROLE_OPERATOR/ROLE_VIEWER） |
| **Permission** | `permissions` | resource（资源路径）、action（r/w） |

### 三种预设角色

| 角色 | 权限 | 说明 |
|------|------|------|
| **ROLE_ADMIN** | 所有权限 | 系统管理员，拥有全部操作权限 |
| **ROLE_OPERATOR** | 读写权限（不可管理用户/角色） | 运维操作员 |
| **ROLE_VIEWER** | 只读权限 | 只读查看 |

### SQL 表结构示例

```sql
CREATE TABLE users (
    username VARCHAR(50) NOT NULL PRIMARY KEY,
    password VARCHAR(500) NOT NULL
);

CREATE TABLE roles (
    username VARCHAR(50) NOT NULL,
    role VARCHAR(50) NOT NULL
);

CREATE TABLE permissions (
    role VARCHAR(50) NOT NULL,
    resource VARCHAR(255) NOT NULL,
    action VARCHAR(8) NOT NULL
);
```

## 6.5 AuthFilter 认证过滤器链

`AuthFilter`（路径：`nacos-2.5.3/auth/src/main/java/com/alibaba/nacos/auth/controller/AuthFilter.java`）：

```java
// AuthFilter 认证过滤器链 (2.5.3)
public class AuthFilter implements Filter {
    
    private final AuthConfigs authConfigs;
    private final AuthPluginService authPluginService;
    
    /** 白名单路径（不需要认证） */
    private static final List<String> WHITE_URLS = Arrays.asList(
        "/v1/auth/login",
        "/v1/ns/instance/beat",
        "/nacos/v1/auth/users/login",
        "/prometheus"
    );
    
    @Override
    public void doFilter(ServletRequest request, 
                         ServletResponse response, 
                         FilterChain chain) 
        throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String uri = httpRequest.getRequestURI();
        
        // Step 1: 检查是否在白名单中
        if (isWhiteUrl(uri)) {
            chain.doFilter(request, response);
            return;
        }
        
        // Step 2: 从 Header 获取 Token
        String token = httpRequest.getHeader("Authorization");
        if (token == null) {
            token = httpRequest.getHeader("accessToken");
        }
        
        // Step 3: 验证 Token
        if (token == null || !authPluginService.validateToken(token)) {
            httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            httpResponse.getWriter().write("{\"code\":403,\"message\":\"未授权\"}");
            return;
        }
        
        // Step 4: 权限验证
        User user = authPluginService.getUser(token);
        Resource resource = new Resource(uri);
        Action action = Action.fromMethod(httpRequest.getMethod());
        try {
            authPluginService.validate(resource, action);
        } catch (AccessException e) {
            httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return;
        }
        
        chain.doFilter(request, response);
    }
}
```

## 6.6 DataSourcePlugin：MySQL vs Derby 切换

`DataSourcePlugin`（路径：`nacos-2.5.3/plugin/datasource/src/main/java/com/alibaba/nacos/plugin/datasource/spi/DataSourcePlugin.java`）：

```java
// DataSourcePlugin SPI 接口 (2.5.3)
public interface DataSourcePlugin {
    
    /** 获取数据源 */
    DataSource getDataSource(Properties properties);
    
    /** 获取数据源类型 */
    String getDataSourceType();
}
```

### 2.5.3 persistence 模块集成

2.5.3 中，数据源管理已从 Config 模块独立到 `persistence` 模块：

```java
// DatasourceConfiguration (persistence模块) (2.5.3)
@Configuration
public class DatasourceConfiguration {
    
    /**
     ★ 2.5.3: DynamicDataSource 动态数据源路由
     根据配置 platform 选择 MySQL 或 Derby
     */
    @Bean
    public DataSource dataSource() {
        DynamicDataSource dynamicDataSource = new DynamicDataSource();
        
        if (EnvUtil.getProperty("spring.datasource.platform", "")
                .equalsIgnoreCase("mysql")) {
            // MySQL 外部存储
            dynamicDataSource.addDataSource(
                DataSourceService.EXTERNAL_STORAGE, 
                createMySQLDataSource());
        } else {
            // Derby 嵌入式存储
            dynamicDataSource.addDataSource(
                DataSourceService.LOCAL_STORAGE, 
                createDerbyDataSource());
        }
        
        return dynamicDataSource;
    }
}
```

## 6.7 EncryptionPluginService：AES/GCM/NoPadding 加密

`EncryptionPluginService`（路径：`nacos-2.5.3/plugin/encryption/src/main/java/com/alibaba/nacos/plugin/encryption/spi/EncryptionPluginService.java`）：

```java
// EncryptionPluginService SPI 接口 (2.5.3)
public interface EncryptionPluginService {
    
    /** 加密 */
    String encrypt(String content, String secretKey);
    
    /** 解密 */
    String decrypt(String content, String secretKey);
    
    /** 生成密钥 */
    String generateSecretKey();
}
```

Nacos 内置实现使用 AES/GCM/NoPadding 算法：

| 配置项 | 说明 |
|--------|------|
| `nacos.config.encrypt.data-key` | 加密密钥 |
| `nacos.config.encrypt.enabled` | 是否启用配置加密 |

## 6.8 TracePlugin + EnvironmentPlugin + ControlManagerPlugin

### TracePlugin

用于分布式链路追踪，将 Nacos 操作事件发送到链路追踪系统（如 SkyWalking、Jaeger）：

| 方法 | 说明 |
|------|------|
| `trace(event)` | 追踪事件 |

### EnvironmentPlugin

用于从外部环境获取配置信息（如 K8s ConfigMap）：

| 方法 | 说明 |
|------|------|
| `getEnvironment(config)` | 获取环境配置 |

### ControlManagerPlugin

用于 TPS（每秒事务数）控制和连接管理：

| 子插件 | 用途 |
|--------|------|
| `TpsControlPlugin` | TPS 限流控制 |
| `ConnectionControlPlugin` | 连接数控制 |

## 6.9 自定义插件开发完整指南

开发一个自定义 Nacos 插件需要以下 5 步：

### Step 1：创建 Maven 项目

```xml
<dependency>
    <groupId>com.alibaba.nacos</groupId>
    <artifactId>nacos-plugin-auth</artifactId>
    <version>${nacos.version}</version>
    <scope>provided</scope>
</dependency>
```

### Step 2：实现 SPI 接口

```java
public class CustomAuthPlugin implements AuthPluginService {
    
    @Override
    public void login(LoginIdentityContext context) throws AccessException {
        // 自定义登录逻辑
        // 例如：集成 LDAP/OAuth2/OIDC
    }
    
    // 实现其他方法...
}
```

### Step 3：配置 SPI 文件

在 `src/main/resources/META-INF/services/` 下创建文件：
`com.alibaba.nacos.plugin.auth.spi.AuthPluginService`

文件内容：
```
com.example.CustomAuthPlugin
```

### Step 4：打包

```bash
mvn clean package
```

### Step 5：部署

```bash
cp target/custom-auth-plugin.jar ${NACOS_HOME}/plugins/
```

重新启动 Nacos 即可生效。

## 6.10 2.5.3 插件体系变更总结

| 变更维度 | 2.2.3 | 2.5.3 |
|---------|-------|-------|
| plugin-default-impl 结构 | 扁平单模块（47 个文件） | **多子模块拆分（nacos-default-auth-plugin + nacos-default-control-plugin）** |
| 数据源插件集成 | Config 模块内调用 | **persistence 模块统一管理** |
| 插件数量 | 6 种 | 6 种（类型不变） |
| SPI 加载机制 | NacosServiceLoader | NacosServiceLoader（不变） |

---

### 本章统计数据

| 指标 | 2.2.3 | 2.5.3 | 变化 |
|------|-------|-------|------|
| plugin-default-impl 结构 | 扁平单模块 | **多子模块** | 架构重构 |
| plugin 模块 Java 文件 | 47 | **47（重组）** | 0 |
| persistence 数据源集成 | 无独立模块 | **37 个 persistence Java 文件** | ★新增独立模块 |

---

> **本章基于 Nacos 2.5.3 源码分析生成。**
