# Nacos 2.2.3 深度技术研究

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Nacos Version](https://img.shields.io/badge/Nacos-2.2.3-brightgreen.svg)](https://github.com/alibaba/nacos)

基于 Nacos 2.2.3 源码的深度技术研究文档，涵盖架构设计、核心模块源码分析、插件体系、安全机制及生产实践。

## 📖 文档结构

全文共 12 章，按四个维度组织：

### 第一部分：架构与设计

| 章节 | 内容 | 核心主题 |
|------|------|---------|
| 第1章 | Nacos 整体架构概述 | 分层架构、核心能力矩阵、2.x vs 1.x 架构演进 |
| 第2章 | 注册中心 (Naming) 源码深度分析 | 服务注册/发现、健康检查、Distro 协议一致性 |
| 第3章 | 配置中心 (Config) 源码深度分析 | 配置发布/订阅、灰度发布、历史版本管理 |
| 第4章 | 一致性协议深度分析 | JRaft 选举与日志复制、Distro 最终一致性 |
| 第5章 | 集群管理 (Core) 源码深度分析 | 集群成员管理、节点的寻址与元数据同步 |

### 第二部分：客户端与扩展

| 章节 | 内容 | 核心主题 |
|------|------|---------|
| 第6章 | 客户端 SDK 深度分析 | ConfigService/NamingService、gRPC 双向流通信 |
| 第7章 | 公共组件与 SPI 扩展机制 | EventDispatcher、NotifyCenter、SPI 加载器 |
| 第8章 | 插件体系深度分析 | 鉴权插件、限流插件、数据源插件、配置加密插件 |

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

## 🔍 技术栈覆盖

- **通信协议**：gRPC Bi-directional Stream、HTTP OpenAPI、DNS-F
- **一致性协议**：JRaft（CP）、Distro（AP）
- **存储层**：Derby（内置）、MySQL（生产）、嵌入式 RocksDB
- **扩展点**：SPI 插件体系、事件总线、过滤器链
- **安全机制**：JWT、AK-SK、RBAC、TLS

## 📂 文件说明

```
.
├── README.md                   # 本文件
├── outline.md                  # 简要大纲
├── nacos-doc-outline.md      # 详细大纲（含各章小节规划）
├── writing-spec.md            # 写作规范
├── nacos-writing-guide.md     # 写作指南
├── nacos-chapter-01.md       # 第1章：Nacos 整体架构概述
├── nacos-chapter-02.md       # 第2章：注册中心源码分析
├── nacos-chapter-03.md       # 第3章：配置中心源码分析
├── nacos-chapter-04.md       # 第4章：一致性协议分析
├── nacos-chapter-05.md       # 第5章：集群管理源码分析
├── nacos-chapter-06.md       # 第6章：客户端SDK分析
├── nacos-chapter-07.md       # 第7章：SPI扩展机制
├── nacos-chapter-08.md       # 第8章：插件体系分析
├── nacos-chapter-09.md       # 第9章：认证与安全机制
├── nacos-chapter-10.md       # 第10章：控制台与系统管理
├── nacos-chapter-11.md       # 第11章：全量配置项详解
└── nacos-chapter-12.md       # 第12章：生产环境部署
```

## 📝 文档特点

- **源码级深度**：基于 Nacos 2.2.3 源码逐模块分析，包含类图、时序图、核心代码片段
- **结构化组织**：每章独立成文，可按需阅读，章内按功能域分节组织
- **生产导向**：第 11-12 章聚焦生产实践，包含配置调优、部署架构、容量规划
- **技术性语言**：采用专业技术术语描述，避免口语化表达

## 🔗 相关资源

- [Nacos 官方文档](https://nacos.io/docs/latest/overview/)
- [Nacos GitHub 仓库](https://github.com/alibaba/nacos)
- [JRaft 论文](https://raft.github.io/)
- [Distro 一致性协议说明](https://nacos.io/docs/latest/architecture/)

## ⚠️ 免责声明

本文档为个人深度技术研究，基于 Nacos 2.2.3 开源版本源码分析编写。内容仅代表作者的个人理解和研究成果，不代表阿里巴巴或 Nacos 官方立场。使用时请结合实际版本验证相关结论。
