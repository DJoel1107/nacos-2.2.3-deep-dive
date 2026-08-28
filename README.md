# Nacos 2.5.3 深度技术研究

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Nacos Version](https://img.shields.io/badge/Nacos-2.5.3-brightgreen.svg)](https://github.com/alibaba/nacos)

基于 Nacos 2.5.3 源码的深度技术研究文档，涵盖架构设计、核心模块源码分析、插件体系、安全机制及生产实践。

> **版本变更**：从 Nacos 2.2.3 升级到 2.5.3。源码规模从 1925 个 Java 文件增加到 2460 个。核心变更包括：
> - 新增 `persistence/` 独立持久化模块（72 个 Java 文件），从 Config 模块分离
> - 新增 `logger-adapter-impl/` 日志适配器模块（16 个 Java 文件）

## 📖 文档结构

全文共 16 章，按五个维度组织：

### 第一部分：架构与设计

| 章号 | 文件 | 内容 | 状态 |
|------|------|------|------|
| 第1章 | `nacos-chapter-01.md` | Nacos 2.5.3 整体架构概述：分层架构、核心能力矩阵、2.x 架构演进 | ✅ |
| 第2章 | `nacos-chapter-02.md` | 注册中心 (Naming) 源码深度分析：服务注册/发现、健康检查、Distro 协议（356 文件） | ✅ |
| 第3章 | `nacos-chapter-03.md` | 配置中心 (Config) 源码深度分析：配置发布/订阅、灰度发布、持久化层（323+72 文件） | ✅ |
| 第4章 | `nacos-chapter-04.md` | 一致性协议深度分析：JRaft 选举与日志复制、Distro 最终一致性 | ✅ |
| 第5章 | `nacos-chapter-05.md` | 集群管理 (Core) + 客户端 SDK 深度分析：集群成员管理、gRPC 双向流通信（324+238 文件） | ✅ |

### 第二部分：持久化与扩展

| 章号 | 文件 | 内容 | 状态 |
|------|------|------|------|
| 第6章 | `nacos-chapter-08.md` | 插件体系与 SPI 扩展机制：鉴权插件、数据源插件、配置加密插件（plugin 172 + plugin-default-impl 89） | ✅ |
| 第7章 | `nacos-chapter-07.md` | 认证安全 + 控制台 + 周边模块：JWT/AK-SK 认证、RBAC、TLS、服务/配置/集群管理 | ✅ |
| 第8章 | `nacos-chapter-06.md` | 持久化层 (Persistence) 深度分析：**2.5.3 新增**：独立持久化模块、Derby/MySQL 双模 | ✅ |

### 第三部分：配置与部署

| 章号 | 文件 | 内容 | 状态 |
|------|------|------|------|
| 第9章 | `nacos-chapter-09.md` | 全量配置项详解：~200+ 配置项分类、调优建议、源码引用 | ✅ |
| 第10章 | `nacos-chapter-10.md` | 生产环境部署架构：多数据中心部署、K8s 部署、容量规划 | ⬜ |
| 第11章 | `nacos-chapter-11.md` | 高可用架构设计：CAP 实践、脑裂恢复、多活架构 | ⬜ |
| 第12章 | `nacos-chapter-12.md` | 性能调优深度分析：JVM/GC 调优、线程池优化、OS 内核参数 | ⬜ |

### 第四部分：运维与实践

| 章号 | 文件 | 内容 | 状态 |
|------|------|------|------|
| 第13章 | `nacos-chapter-13.md` | 监控运维：Prometheus + Grafana、日志分析、告警配置 | ⬜ |
| 第14章 | `nacos-chapter-14.md` | 故障排查指南：常见故障诊断流程、日志分析工具 | ⬜ |
| 第15章 | — | Spring Cloud Alibaba 集成最佳实践 | ⬜ |
| 第16章 | — | 附录：术语表、配置速查表、API 参考 | ⬜ |

> **编号说明**：章节编号与 `nacos-doc-outline.md` 新大纲一致。由于旧版大纲将部分章节拆分（如认证安全/控制台分开），新大纲合并后章节总数从 15 章变为 16 章。文件编号沿用旧版序号，因此第6章和第8章的文件编号与内容章节号交叉（`nacos-chapter-06.md` = 持久化层第8章，`nacos-chapter-08.md` = 插件体系第6章）。

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
│   ├── source-reference-spec.md  # 源码引用规范 ✅
│   ├── quality-checklist.md      # 质检清单（55项/42必检） ✅
│   ├── nacos-doc-outline.md     # 详细大纲（16 章）
│   └── outline.md                # 旧版大纲（归档）
├── chapters/                     # 各章正文（按新大纲编号）
│   ├── nacos-chapter-01.md      ✅ 第1章 整体架构概述（~66K字）
│   ├── nacos-chapter-02.md      ✅ 第2章 注册中心源码分析
│   ├── nacos-chapter-03.md      ✅ 第3章 配置中心源码分析
│   ├── nacos-chapter-04.md      ✅ 第4章 一致性协议深度分析
│   ├── nacos-chapter-05.md      ✅ 第5章 集群管理+客户端SDK
│   ├── nacos-chapter-06.md      ✅ 第8章 持久化层 ⚠️
│   ├── nacos-chapter-07.md      ✅ 第7章 认证安全+控制台+周边
│   ├── nacos-chapter-08.md      ✅ 第6章 插件体系 ⚠️
│   ├── nacos-chapter-09.md      ✅ 第9章 全量配置项详解
│   ├── nacos-chapter-10.md      ⬜ 第10章 生产部署架构
│   ├── nacos-chapter-11.md      ⬜ 第11章 高可用架构设计
│   └── nacos-chapter-12.md      ⬜ 第12章 性能调优
├── chapter-html/                 # HTML 格式输出
│   ├── nacos-chapter-01.html     ✅ Swiss Style 排版
│   └── .gitkeep
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
- **生产导向**：第 9-16 章聚焦生产实践，包含配置调优、部署架构、性能优化、故障排查、集成最佳实践
- **技术性语言**：采用专业技术术语描述，避免口语化表达

## 🔗 相关资源

- [Nacos 官方文档](https://nacos.io/docs/latest/overview/)
- [Nacos GitHub 仓库](https://github.com/alibaba/nacos)
- [JRaft 论文](https://raft.github.io/)
- [Distro 一致性协议说明](https://nacos.io/docs/latest/architecture/)

## ⚠️ 免责声明

本文档为个人深度技术研究，基于 Nacos 2.5.3 开源版本源码分析编写。内容仅代表作者的个人理解和研究成果，不代表阿里巴巴或 Nacos 官方立场。使用时请结合实际版本验证相关结论。
