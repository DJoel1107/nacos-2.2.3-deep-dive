# 第10章：Nacos 2.5.3 高可用架构设计

## 10.1 CAP 理论在 Nacos 中的实践

Nacos 2.5.3 同时支持 AP 和 CP 两种一致性模式，根据 CAP 理论灵活权衡：

| 属性 | AP 模式（Distro） | CP 模式（JRaft 1.3.14） |
|------|-------------------|--------------------------|
| **一致性** | 最终一致性 | 强一致性 |
| **可用性** | 高（容忍少数分区） | 中（需要多数派存活） |
| **分区容忍性** | 高（去中心化） | 中（需要 Leader） |
| **核心协议** | Distro Protocol | Raft Protocol |
| **数据量级** | 临时实例（百万级心跳） | 持久化实例（万级持久） |
| **适10用场景** | 高频心跳、临时实例 | 数据持久化要求高 |

### 2.5.3 AP/CP 增强

| 增强点 | 说明 |
|--------|------|
| JRaft 1.3.14 | Leader 选举 Pre-Vote 超时参数优化，减少误触发 |
| Distro 优化 | 批量同步 Key 数量、数据大小参数可配置 |
| 脑裂检测 | **★新增 `NamingReadinessCheckService` 提前检测模块就绪** |
| 健康检查 | **★新增 `AbstractModuleHealthChecker` 模块级健康检查** |

## 10.2 AP vs CP 模式选择决策树

```
┌────────────────────────────────────┐
│      AP vs CP 模式选择决策树      │
├────────────────────────────────────┤
│                                    │
│  实例是否需要持久化？              │
│   ├── 否 ──▶ AP (Distro)        │
│   │           临时实例（高频心跳） │
│   │                                │
│   └── 是 ──▶ 需要强一致性？     │
│               ├── 是 ──▶ CP (Raft)│
│               │       持久化实例   │
│               └── 否 ──▶ AP (Distro)│
│                       异步最终一致性│
│                                    │
│  实例心跳频率？                  │
│   ├── 高（≤5秒）──▶ AP (Distro)│
│   └── 低（≥10秒）──▶ CP (Raft │
│                        也可AP)      │
└────────────────────────────────────┘
```

## 10.3 集群脑裂场景分析

### 网络分区导致双 Leader 完整时序图

```
┌────────────────────────────────────────────────────┐
│           Raft 集群脑裂场景时序图                  │
├────────────────────────────────────────────────────┤
│                                                    │
│  初始状态: Node1 (Leader), Node2 (Follower), Node3 (Follower)   │
│                                                    │
│  T1: 网络分区发生                                  │
│  ┌─────────────┐     ┌──────────────┐            │
│  │ 分区 A:    │     │ 分区 B:      │            │
│  │ Node1 × 1  │     │ Node2 + Node3 │            │
│  │ (原Leader)  │     │ (多数派)     │            │
│  └─────────────┘     └──────────────┘            │
│                                                    │
│  T2: 分区 B 选举新 Leader                         │
│   Node2 超时未收到 Leader 心跳                     │
│   → Pre-Vote (★ 2.5.3: 优化超时参数)            │
│   → RequestVote (获得多数派投票)                  │
│   → Node2 当选为新 Leader                          │
│                                                    │
│  T3: 脑裂双 Leader 状态                           │
│   Node1: 仍认为自己是 Leader                       │
│   Node2: 已被多数派选举为 Leader                   │
│   ★★ 此时出现双 Leader = 脑裂 ★★               │
│                                                    │
│  T4: 分区恢复                                     │
│   Node1 收到更高 term 的消息                       │
│   → 发现自己的 term 落后                          │
│   → 自动降级为 Follower                           │
│   → 丢弃未提交的 Log Entry                       │
│                                                    │
│  T5: 恢复正常状态                                 │
│   Node2 (Leader) + Node1 (Follower) + Node3 (Follower)│
│                                                    │
└────────────────────────────────────────────────────┘
```

## 10.4 Raft Pre-Vote 防脑裂机制

**★ 2.5.3 JRaft 1.3.14 Pre-Vote 优化**：

| 检查层 | 机制 | 2.5.3 优化 |
|--------|------|-----------|
| **1. term 检查** | Candidate 的 term 必须 > Follower 的 term | 超时参数优化 |
| **2. Leader 存活检测** | Follower 在过去 `electionTimeout` 内收到过 Leader 心跳，则拒绝 Pre-Vote | **增强心跳检测机制** |
| **3. 日志最新检查** | Candidate 的 lastLogIndex ≥ Follower 的 lastLogIndex | — |

### Pre-Vote 流程

```
Follower ──超时未收到Leader心跳──▶ 转变为Candidate
                                          │
                                          ├─ 1. term++
                                          ├─ 2. Pre-Vote RPC
                                          │    └─ 询问其他节点："我能否赢得选举？"
                                          │
                                          ├─ 3. 收集Pre-Vote赞同票
                                          │    └─ 多数派赞同？
                                          │         ├─ Yes ──▶ 4. RequestVote RPC（正式投票）
                                          │         └─ No  ──▶ 退回到Follower
                                          │
                                          └─ 5. 成为Leader
```

## 10.5 脑裂恢复 3 阶段策略

| 阶段 | 操作 | 工具/命令 |
|------|------|-----------|
| **1. 检测** | 检查集群健康状态：`curl nacos/v1/core/cluster/nodes` | 检查双 Leader、Raft Leader 状态 |
| **2. 仲裁** | 通过 `RaftCore` Majority Vote 重新选举 | `curl nacos/v1/core/cluster/raft/leader` |
| **3. 数据合并** | 丢弃未提交的 Log + 从 Snapshot 恢复 | Jraft Node `resetPeers()` |

## 10.6 异地三中心多活架构

| 中心 | 角色 | MySQL | Nacos 部署 |
|------|------|------|-----------|
| **中心 A** | 主中心 | MySQL Master | Nacos × 3（Raft 3节点） |
| **中心 B** | 灾备中心 | MySQL Slave（半同步复制） | Nacos × iat2（Learner 2节点） |
| **中心 C** | 异地灾备 | MySQL Slave（异步复制） | Nacos × 1（Learner 1节点，可选） |

### 流量切换策略

| 故障场景 | 切换时间 | 操作 |
|---------|---------|------|
| 单节点故障 | < 30 秒 | 自动故障转移（Raft Leader 重选举） |
| 中心 A 故障 | < 5 分钟 | DNS 切换 + MySQL 主从切换 |
| 双中心故障 | < 10 分钟 | DNS 切换到中心 C + MySQL 灾备恢复 |

## 10.7 MySQL 半同步复制配置

```sql
-- 主库配置
INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so';
SET GLOBAL rpl_semi_sync_master_enabled = 1;
SET GLOBAL rpl_semi_sync_master_timeout = 10000; -- 10秒超时

-- 从库配置
INSTALL PLUGIN rpl_semi_sync_slave SONAME 'semisync_slave.so';
SET GLOBAL rpl_semi_sync_slave_enabled = 1;

-- 验证半同步状态
SHOW STATUS LIKE 'Rpl_semi_sync%';
```

## 10.8 优雅停机流程

```
┌────────────────────────────────────────────────────┐
│            Nacos 优雅停机流程                      │
├────────────────────────────────────────────────────┤
│                                                    │
│  Step 1: 从 LB 摘除（如 Nginx upstream 标记 DOWN）│
│  │ curl -X PUT "http://nginx:80/admin" -d {}      │
│  │                                                 │
│  Step 2: 等待当前连接处理完成（默认 30秒）       │
│  │ curl nacos/v1/core/cluster/self/health         │
│  │                                                 │
│  Step 3：执行 shutdown.sh（内部调用 kill -15）     │
│  │ bin/shutdown.sh                                │
│  │                                                 │
│  Step 4: 等待进程退出（最多 60秒）                │
│  │ kill -0 $(pgrep nacos) 2>/dev/null           │
│  │                                                 │
└────────────────────────────────────────────────────┘
```

## 10.9 滚动重启流程

```
┌────────────────────────────────────────────────────┐
│            滚动重启 Standard流程                     │
├────────────────────────────────────────────────────┤
│                                                    │
│  for node in node1 node2 node3:                    │
│    1. 从 LB 摘除 node                             │
│       curl PUT "http://nginx/admin" -d {}         │
│    2. 等待集群共识稳定（5-10秒）                 │
│    3. 优雅停机 node                               │
│    4. 更新配置/Nacos版本（如需要）               │
│    5. 启动 node                                   │
│    6. 等待 node UP + Raft 重新加入               │
│       curl nacos/v1/core/cluster/nodes            │
│    7. 验证 node 功能正常                          │
│       curl nacos/v1/core/cluster/health           │
│    8. 恢复 LB node                               │
│                                                    │
│  注意事项：                                       │
│  - 每次只重启 1 个节点                            │
│  - 等待 Raft Leader 稳定后再重启下一个            │
│  - ★ 2.5.3: 增加模块健康检查确保persistence模块就绪│
│                                                    │
└────────────────────────────────────────────────────┘
```

## 10.10 2.5.3 高可用增强总结

| 增强点 | 说明 |
|--------|------|
| JRaft 1.3.14 | Leader 选举 Pre-Vote 超时优化 |
| ModuleHealthChecker | 模块级健康检查（★新增） |
| ReadinessResult | 就绪检查结果标准化（★新增） |
| NamingReadinessCheckService | Naming 模块就绪检查（★新增） |
| persistence 健康检查 | persistence 模块数据源状态监控（★新增） |
| ConfigCache 容错 | 配置缓存容错机制（★新增） |

---

> **本章基于 Nacos 2.5.3 高可用架构设计生成。**
