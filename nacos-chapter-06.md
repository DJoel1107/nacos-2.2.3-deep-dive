# 第6章：插件体系与认证安全深度分析

## 6.1 插件体系概览

Nacos 2.2.3 的插件体系基于 Java SPI（Service Provider Interface）机制，支持以下插件类型：

| 插件类型 | 接口 | 默认实现 | 用途 |
|---------|------|---------|------|
| 鉴权插件 | `AuthPluginService` | `NacosAuthPluginService` | 用户认证与授权 |
| 数据源插件 | `DataSourcePlugin` | Derby/MySQL | 数据库连接管理 |
| 加密插件 | `EncryptionPluginService` | AES 加密 | 配置内容加密 |
| 追踪插件 | `TracePlugin` | — | 全链路追踪 |
| 环境插件 | `EnvironmentPlugin` | — | 环境变量注入 |
| 控制插件 | `ControlManagerPlugin` | `RemoteControlPlugin` | 连接控制管理 |

## 6.2 鉴权插件（AuthPluginService）

### 6.2.1 鉴权架构

```
HTTP/gRPC Request
    │
    ▼
┌────────────────────────────────────────────────────┐
│              AuthFilterChain                     │
│  ┌──────────────────────────────────────────┐   │
│  │  AuthFilter (JWT/Token验证)            │   │
│  └──────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────┐   │
│  │  PermissionFilter (RBAC权限校验)        │   │
│  └──────────────────────────────────────────┘   │
└────────────────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────────────────┐
│             AuthPluginService                      │
│  ┌──────────────────────────────────────────┐   │
│  │  NacosAuthPluginService (默认实现)     │   │
│  │  - username/password 验证               │   │
│  │  - AccessToken 生成/验证                │   │
│  │  - JWT Token 生成/验证                  │   │
│  └──────────────────────────────────────────┘   │
└────────────────────────────────────────────────────┘
```

### 6.2.2 AuthPluginService 接口定义

```java
// AuthPluginService.java - 鉴权插件接口
public interface AuthPluginService {
    
    /**
     * 验证用户名密码
     */
    boolean validateIdentity(String username, String password);
    
    /**
     * 验证 AccessToken
     */
    boolean validateAccessToken(String accessToken);
    
    /**
     * 生成 AccessToken
     */
    String generateAccessToken(String username);
    
    /**
     * 获取用户的角色列表
     */
    List<String> getRoles(String username);
    
    /**
     * 验证用户是否有权限执行操作
     */
    boolean hasPermission(String username, String resource, String action);
    
    /**
     * 是否开启鉴权
     */
    boolean isAuthEnabled();
}
```

### 6.2.3 NacosAuthPluginService——默认鉴权实现

```java
// NacosAuthPluginService.java - 默认鉴权实现
@Component
public class NacosAuthPluginService implements AuthPluginService {
    
    @Autowired
    private UserMapper userMapper;
    
    @Autowired
    private RoleMapper roleMapper;
    
    @Autowired
    private PermissionMapper permissionMapper;
    
    // BCrypt 密码加密
    private final PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
    
    // JWT Token 密钥（可配置）
    @Value("${nacos.core.auth.default.token.secret.key:}")
    private String tokenSecretKey;
    
    // AccessToken 有效期（默认 18000秒 = 5小时）
    @Value("${nacos.core.auth.default.token.expire.seconds:18000}")
    private long tokenExpireSeconds;
    
    @Override
    public boolean validateIdentity(String username, String password) {
        User user = userMapper.findByUsername(username);
        if (user == null) {
            return false;
        }
        return passwordEncoder.matches(password, user.getPassword());
    }
    
    @Override
    public String generateAccessToken(String username) {
        // JWT Token 生成
        Map<String, Object> claims = new HashMap<>();
        claims.put("sub", username);
        claims.put("iat", System.currentTimeMillis() / 1000);
        claims.put("exp", System.currentTimeMillis() / 1000 + tokenExpireSeconds);
        
        return JWT.create()
            .setClaims(claims)
            .sign(Algorithm.HMAC256(tokenSecretKey));
    }
    
    @Override
    public boolean validateAccessToken(String accessToken) {
        try {
            JWT.require(Algorithm.HMAC256(tokenSecretKey))
                .build()
                .verify(accessToken);
            return true;
        } catch (JWTVerificationException e) {
            return false;
        }
    }
    
    @Override
    public boolean hasPermission(String username, String resource, String action) {
        return permissionMapper.hasPermission(username, resource, action);
    }
    
    @Override
    public boolean isAuthEnabled() {
        return StringUtils.isNotBlank(tokenSecretKey);
    }
}
```

### 6.2.4 RBAC 权限模型

Nacos 的 RBAC（Role-Based Access Control）权限模型包含三个实体：
- **User（用户）**：登录 Nacos 控制台的操作人员
- **Role（角色）**：权限的集合，如 ROLE_ADMIN（管理员）、ROLE_USER（普通用户）
- **Permission（权限）**：资源 + 操作的组合，如 `namespace/public:*:*` 表示对 public 命名空间的所有操作

```sql
-- 用户表
CREATE TABLE users (
    username varchar(50) NOT NULL PRIMARY KEY,
    password varchar(500) NOT NULL
);

-- 角色表
CREATE TABLE roles (
    username varchar(50) NOT NULL,
    role varchar(50) NOT NULL
);

-- 权限表
CREATE TABLE permissions (
    role varchar(50) NOT NULL,
    resource varchar(512) NOT NULL,
    action varchar(8) NOT NULL
);
```

**权限格式：`namespace/resource:action`**

| 示例 | 说明 |
|------|------|
| `public:*:*` | 对 public 命名空间所有资源的所有操作 |
| `prod:*:r` | 对 prod 命名空间所有资源的只读操作 |
| `*:*:*` | 对所有命名空间所有资源的所有操作（全局管理员） |

### 6.2.5 AuthFilter 认证过滤器链

```java
// AuthFilter.java - 认证过滤器
@WebFilter(urlPatterns = "/*")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class AuthFilter implements Filter {
    
    @Autowired
    private AuthPluginService authPluginService;
    
    // 无需认证的路径白名单
    private static final List<String> EXCLUDED_URL_PREFIX = Arrays.asList(
        "/v1/auth/login",
        "/v1/console/ui/",
        "/v1/ms/",
        "/static/"
    );
    
    @Override
    public void doFilter(ServletRequest servletRequest, 
            ServletResponse servletResponse, FilterChain filterChain) 
            throws IOException, ServletException {
        
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        
        // 1. 检查是否开启鉴权
        if (!authPluginService.isAuthEnabled()) {
            filterChain.doFilter(request, response);
            return;
        }
        
        // 2. 检查是否在白名单
        String path = request.getRequestURI();
        if (isExcludedPath(path)) {
            filterChain.doFilter(request, response);
            return;
        }
        
        // 3. 验证 AccessToken
        String accessToken = request.getHeader("Authorization");
        if (StringUtils.isBlank(accessToken)) {
            accessToken = request.getParameter("accessToken");
        }
        
        if (StringUtils.isBlank(accessToken) || 
                !authPluginService.validateAccessToken(accessToken)) {
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.getWriter().write("{\"code\":403,\"message\":\"accessToken invalid\"}");
            return;
        }
        
        // 4. 权限校验（仅非 GET 请求）
        if (!"GET".equals(request.getMethod())) {
            String username = JWTUtils.getUsernameFromToken(accessToken);
            String resource = parseResource(path);
            String action = parseAction(request.getMethod());
            
            if (!authPluginService.hasPermission(username, resource, action)) {
                response.setStatus(HttpStatus.FORBIDDEN.value());
                response.getWriter().write(
                    "{\"code\":403,\"message\":\"permission denied\"}");
                return;
            }
        }
        
        filterChain.doFilter(request, response);
    }
    
    private boolean isExcludedPath(String path) {
        return EXCLUDED_URL_PREFIX.stream().anyMatch(path::startsWith);
    }
}
```

## 6.3 数据源插件（DataSourcePlugin）

### 6.3.1 数据源插件的两种模式

Nacos 支持两种数据源插件模式：

```java
// DataSourcePlugin.java - 数据源插件接口
public interface DataSourcePlugin {
    
    /**
     * 获取数据源类型（mysql/derby）
     */
    String getDataSourceType();
    
    /**
     * 获取数据源连接池（HikariCP）
     */
    HikariDataSource getDataSource();
    
    /**
     * 初始化数据源
     */
    void init(Properties properties);
}
```

**切换方式：**

```properties
# 使用 MySQL 外部数据源（生产环境）
spring.datasource.platform=mysql
db.num=1
db.url.0=jdbc:mysql://127.0.0.1:3306/nacos_config?characterEncoding=utf8&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai
db.user.0=nacos
db.password.0=nacos_pwd

# 使用 Derby 嵌入式数据源（单机测试）
spring.datasource.platform=derby
```

### 6.3.2 HikariCP 连接池配置优化

```properties
# HikariCP 连接池配置（适用于 MySQL 外部数据源）
db.pool.config.maximumPoolSize=20
db.pool.config.minimumIdle=5
db.pool.config.connectionTimeout=30000
db.pool.config.idleTimeout=600000
db.pool.config.maxLifetime=1800000
db.pool.config.leakDetectionThreshold=3000
```

## 6.4 加密插件（EncryptionPluginService）

```java
// EncryptionPluginService.java - 加密插件接口
public interface EncryptionPluginService {
    
    /**
     * 加密配置内容
     */
    String encrypt(String content, String secretKey);
    
    /**
     * 解密配置内容
     */
    String decrypt(String content, String secretKey);
    
    /**
     * 生成 SecretKey
     */
    String generateSecretKey();
}
```

默认使用 AES 加密：

```java
// DefaultEncryptionPluginServiceImpl.java - 默认 AES 加密实现
@Component
public class DefaultEncryptionPluginServiceImpl implements EncryptionPluginService {
    
    private static final String AES_ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_IV_LENGTH = 12;
    
    @Override
    public String encrypt(String content, String secretKey) {
        try {
            SecretKey key = new SecretKeySpec(
                Hex.decodeHex(secretKey), "AES");
            
            byte[] iv = generateIV();
            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            GCMParameterSpec gcmParameterSpec = 
                new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, gcmParameterSpec);
            
            byte[] cipherText = cipher.doFinal(
                content.getBytes(StandardCharsets.UTF_8));
            
            // IV + CipherText 合并后用 Base64 编码
            byte[] combined = new byte[GCM_IV_LENGTH + cipherText.length];
            System.arraycopy(iv, 0, combined, 0, GCM_IV_LENGTH);
            System.arraycopy(cipherText, 0, combined, GCM_IV_LENGTH, 
                cipherText.length);
            
            return Base64.encodeBase64String(combined);
        } catch (Exception e) {
            throw new RuntimeException("AES encrypt failed", e);
        }
    }
    
    @Override
    public String generateSecretKey() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(256, new SecureRandom());
            SecretKey key = keyGenerator.generateKey();
            return Hex.encodeHexString(key.getEncoded());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to generate secret key", e);
        }
    }
}
```

## 6.5 自定义插件开发指南

### 6.5.1 开发步骤

1. **创建新插件模块**
```
nacos-plugin-custom/
├── pom.xml
├── src/main/java/
│   └── com/alibaba/nacos/plugin/custom/
│       └── CustomPlugin.java
└── src/main/resources/
    └── META-INF/services/
        └── com.alibaba.nacos.plugin.custom.spi.CustomPluginService
```

2. **实现自定义插件接口**

```java
@NacosPlugin
public class CustomPlugin implements CustomPluginService {
    @Override
    public void doSomething() {
        // 自定义实现
    }
    
    @Override
    public String getPluginName() {
        return "custom-plugin";
    }
}
```

3. **配置 SPI 文件**

在 `META-INF/services/com.alibaba.nacos.plugin.custom.spi.CustomPluginService` 中指定实现类：
```
com.alibaba.nacos.plugin.custom.CustomPlugin
```

4. **打包并部署**

```bash
# 编译打包
mvn clean package -DskipTestsear

# 将 JAR 复制到 Nacos 的 plugins 目录
cp target/nacos-plugin-custom-1.0.0.jar $NACOS_HOME/plugins/
```

---

*（第六章完，约 1.3 万字）*
