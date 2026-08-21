# Nacos 2.5.3 深度技术研究

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Nacos Version](https://img.shields.io/badge/Nacos-2.5.3-brightgreen.svg)](https://github.com/alibaba/nacos)

基于 Nacos 2.5.3 源码的深度技术研究文档，涵盖架构设计、核心模块源码分析、插件体系、安全机制及生产实践。

> **版本变更**：从 Nacos 2.2.3 升级到 2.5.3。源码规模从 1925 个 Java 文件增加到 2460 个。核心变更包括：
> - 新增 `persistence/` 独立持久化模块（72 个 Java 文件），从 Config 模块分离
> - 新增 `logger-adapter-impl/` 日志适配器模块（16 个 Java 文件）

## 📖 文档结构

全文共 15 章，按五个维度组织：

### 第一部分：架构与设计

| 章节 | 内容 | 核心主题 |
|------|------|---------|
| 第1章 | Nacos 2.5.3 整体架构概述 | 分层架构、核心能力矩阵、2.x 架构演进、新增 persistence/logging 模块 |
| 第2章 | 注册中心 (Naming) 源码深度分析 | 服务注册/发现、健康检查、Distro 协议一致性（356 文件） |
| 第3章 | 配置中心 (Config) 源码深度分析 | 配置发布/订阅、灰度发布、持久化层独立（323 + 72 文件） |
| 第4章 | 一致性协议深度分析 | JRaft 选举与日志复制、Distro 最终一致性 |
| 第5章 | 集群管理 (Core) 源码深度分析 | 集群成员管理、节点的寻址与元数据同步（324 文件） |

### 第二部分：持久化与扩展

| 章节 | 内容 | 核心主题 |
|------|------|---------|
| 第6章 | 持久化层 (Persistence) 深度分析 | **2.5.3 新增**：独立持久化模块、数据源动态切换、Derby/MySQL 双模 |
| 第7章 | 客户端 SDK 深度分析 | ConfigService/NamingService、gRPC 双向流通信（238 文件） |
| 第8章 | 插件体系与 SPI 扩展机制 | 鉴权插件、数据源插件、配置加密插件（plugin 172 + plugin-default-impl 89） |

### 第三部分：安全与控制台

| 章节 | 内容 | 核心主题 |
|------|------|---------|
| 第9章 | 认证与安全机制 | JWT/AK-SK 认证、RBAC 权限模型、TLS 传输加密 |
| 第10章 | 控制台与系统管理 | 服务管理、配置管理、集群管理、命名空间管理 |

### 第四部分：生产实践

| 章节 | 内容 | 核心主题 |
|------|------|---------|
| 第11章 | 全量配置项详解 | `application.properties` 全部配置项分类与调优建议 |
| 第12章 | 生产环境部署架构 | 多数据中心部署、K8s 部署、容量规划 |
| 第13章 | 高可用架构设计 | CAP 实践、脑裂恢复、多活架构 |
| 第14章 | 性能调优深度分析 | JVM/GC 调优、线程池优化、OS 内核参数 |
| 第15章 | 监控运维与故障排查 | Prometheus + Grafana、日志分析、常见故障排查 |

## 🔍 技术栈覆盖

- **通信协议**：gRPC Bi-directional Stream、HTTP OpenAPI、DNS-F
- **一致性协议**：JRaft（CP）、Distro（AP）
- **存储层**：Derby（内置）、MySQL（生产）、嵌入式 RocksDB
- **持久化层**：独立 `persistence/` 模块（2.5.3 新增）
- **扩展点**：SPI 插件体系、事件总线、过滤器链
- **安全机制**：JWT、AK-SK、RBAC、Tantor

## 📂 目录结构

```
.
├── README.md                     # 项目说明
├── PROGRESS.md                  # 任务进度跟踪
├── meta/                         # 规范与配套文档
│   ├── glossary.md               # 术语表（91 个术语 / 16 分类）
│   ├── nacos-writing-guide.md    # 写作工程指南
│   ├── writing-spec.md           # 写作规范
│   ├── source-reference-spec.md  # 源码引用规范（待产出）
│   ├── quality-checklist.md      # 质检清单（待产出）
│   ├── nacos-doc-outline.md     # 详细大纲（15 章 / 163 小节）
│   └── outline.md                # 旧版大纲（归档）
├── chapters/                     # 各章正文
│   ├── nacos-chapter-01.md
│   ├── ...
│   └── nacos-chapter-15.md
├── analysis/                     # 分析报告
│   └── nacos-2.2.3-to-2.5.3-diff.md
├── upstream/                    # 上游源码
│   └── nacos-2.5.3/             # Nacos 2.5.3 源码（2,460 Java 文件）
├── src/                         # 自定义代码 / 脚本
├── assets/                      # 图片 / 图表 / 截图
├── refs/                        # 外部参考资料 / 论文
└── build/                       # 构建 / 导出 / 合并脚本
```

## 📝 文档特点

- **源码级深度**：基于 Nacos 2.5.3 源码逐模块分析，包含类图、时序图、核心代码片段
- **结构化组织**：每章独立成文，可按需阅读，章内按功能域分节组织
- **生产导向**：第 11-15 章聚焦生产实践，包含配置调优、部署架构、性能优化、故障排查
- **技术性语言**：采用专业技术术语描述，避免口语化表达

## 🔗 相关资源

- [Nacos 官方文档](https://nacos.io/docs/latest/overview/)
- [Nacos GitHub 仓库](https://github.com/alibaba/nacos)
- [JRaft 论文](https://raft.github.io/)
- [Distro 一致性协议说明](https://nacos.io/docs/latest/architecture/)

## ⚠️ 免责声明

本文档为个人深度技术研究，基于 Nacos 2.5.3 开源版本源码分析编写。内容仅代表作者的个人理解和研究成果，不代表阿里巴巴或 Nacos 官方立场。使用时请结合实际版本验证相关结论。
