#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 Nacos 2.5.3 深度研究 HTML 总索引页"""
import re, os, html

REPO = "/home/sandbox/.openclaw/workspace/repo"
OUT  = os.path.join(REPO, "chapter-html")

# 章节元数据：(文件号, 显示章号, 标题, 分组)
CHAPTERS = [
    ("01", "第 1 章",  "Nacos 2.5.3 整体架构概述",              "第一部分 · 架构与设计"),
    ("02", "第 2 章",  "注册中心（Naming）源码深度分析",          "第一部分 · 架构与设计"),
    ("03", "第 3 章",  "配置中心（Config）源码深度分析",          "第一部分 · 架构与设计"),
    ("04", "第 4 章",  "一致性协议（JRaft & Distro）深度分析",    "第一部分 · 架构与设计"),
    ("05", "第 5 章",  "集群管理（Core）+ 客户端 SDK 深度分析",   "第一部分 · 架构与设计"),
    ("06", "第 6 章",  "持久化层（persistence）深度分析",         "第二部分 · 持久化与扩展"),
    ("07", "第 7 章",  "认证安全、控制台与周边模块",              "第二部分 · 持久化与扩展"),
    ("08", "第 8 章",  "插件体系与 SPI 扩展机制",                 "第二部分 · 持久化与扩展"),
    ("09", "第 9 章",  "全量配置项详解",                          "第三部分 · 配置与部署"),
    ("10", "第 10 章", "生产环境部署架构",                        "第三部分 · 配置与部署"),
    ("11", "第 11 章", "高可用架构设计",                          "第三部分 · 配置与部署"),
    ("12", "第 12 章", "性能调优深度分析",                        "第三部分 · 配置与部署"),
    ("13", "第 13 章", "监控运维",                                "第四部分 · 运维与实践"),
    ("14", "第 14 章", "故障排查指南",                            "第四部分 · 运维与实践"),
    ("15", "第 15 章", "Spring Cloud Alibaba 集成最佳实践",       "第四部分 · 运维与实践"),
    ("16", "第 16 章", "附录",                                    "第四部分 · 运维与实践"),
]

def count_chars(path):
    """统计正文字数（剔除代码块与 Markdown 格式符），与项目口径一致"""
    t = open(path, encoding="utf-8").read()
    t = re.sub(r"```.*?```", "", t, flags=re.S)
    t = re.sub(r"[#*|\-\s]", "", t)
    return len(t)

def first_h2(path):
    """取第一个 ## 小节的标题"""
    for line in open(path, encoding="utf-8"):
        if line.startswith("## "):
            return line[3:].strip()
    return ""

rows = []
groups = []
for fid, disp, title, group in CHAPTERS:
    md  = os.path.join(REPO, "chapters", f"nacos-chapter-{fid}.md")
    htmlf = f"nacos-chapter-{fid}.html"
    exists = os.path.exists(os.path.join(OUT, htmlf))
    n = count_chars(md) if os.path.exists(md) else 0
    if group not in groups:
        groups.append(group)
    rows.append((group, fid, disp, title, n, htmlf, exists))

# 统计
total_chars = sum(r[4] for r in rows)
total_kb = sum(os.path.getsize(os.path.join(OUT, r[5])) for r in rows if r[6]) / 1024

cards = []
for group in groups:
    items = [r for r in rows if r[0] == group]
    inner = "\n".join(
        f'''      <a class="card" href="{r[5]}">
        <span class="num">{r[1]}</span>
        <span class="body">
          <span class="ct">{html.escape(r[3])}</span>
          <span class="meta">{r[2]} · {r[4]:,} 字</span>
        </span>
      </a>''' for r in items
    )
    cards.append(f'''    <section class="grp">
      <h2>{html.escape(group)}</h2>
      <div class="grid">
{inner}
      </div>
    </section>''')

page = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nacos 2.5.3 深度技术研究 · 目录</title>
<style>
/* === Swiss Style Typography System === */
:root {{
  --sans: "Inter", "Helvetica Neue", "Helvetica", "Arial", system-ui, sans-serif;
  --sans-zh: "Noto Sans SC", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
  --mono: "JetBrains Mono", "SF Mono", "Cascadia Code", "Consolas", monospace;
  --paper: #fafaf8; --ink: #0a0a0a;
  --text-secondary: #525252; --text-helper: #737373;
  --accent: #002FA7; --accent-bright: #5B7BFF;
  --border-subtle: #e0e0e0; --grey-1: #f0f0ee;
  --sp-3:8px; --sp-5:16px; --sp-7:32px; --sp-8:40px; --sp-10:64px; --sp-12:96px;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: var(--sans-zh), var(--sans);
  font-weight: 300; font-size: 17px; line-height: 1.75;
  color: var(--ink); background: var(--paper);
  max-width: 1080px; margin: 0 auto;
  padding: var(--sp-12) var(--sp-8);
  -webkit-font-smoothing: antialiased;
}}
header {{ border-bottom: 2px solid var(--ink); padding-bottom: var(--sp-7); margin-bottom: var(--sp-10); }}
.kicker {{ font-family: var(--mono); font-size: 13px; font-weight: 500; letter-spacing: .12em;
  text-transform: uppercase; color: var(--accent); display: block; margin-bottom: var(--sp-5); }}
h1 {{ font-weight: 900; font-size: 2.6em; line-height: 1.1; letter-spacing: -.02em; }}
.sub {{ color: var(--text-secondary); margin-top: var(--sp-5); font-size: 1.05em; }}
.stats {{ display: flex; gap: var(--sp-10); margin-top: var(--sp-7); flex-wrap: wrap; }}
.stat b {{ display: block; font-size: 2.2em; font-weight: 200; line-height: 1; letter-spacing: -.02em; color: var(--accent); }}
.stat span {{ font-family: var(--mono); font-size: 12px; letter-spacing: .1em; text-transform: uppercase; color: var(--text-helper); }}
.grp {{ margin-bottom: var(--sp-10); }}
h2 {{ font-weight: 700; font-size: 1.15em; letter-spacing: .02em; color: var(--text-helper);
  text-transform: uppercase; padding-bottom: var(--sp-5); margin-bottom: var(--sp-7);
  border-bottom: 1px solid var(--border-subtle); }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 1px; background: var(--border-subtle); border: 1px solid var(--border-subtle); }}
.card {{ display: flex; gap: var(--sp-5); align-items: flex-start; background: var(--paper);
  padding: var(--sp-7); text-decoration: none; color: inherit; transition: background .15s; }}
.card:hover {{ background: var(--grey-1); }}
.card:hover .ct {{ color: var(--accent); }}
.num {{ font-family: var(--mono); font-size: 13px; font-weight: 600; color: var(--accent);
  border: 1px solid var(--accent); padding: 1px 7px; flex-shrink: 0; margin-top: 3px; }}
.body {{ display: block; min-width: 0; }}
.ct {{ display: block; font-weight: 600; font-size: 1.02em; line-height: 1.45; margin-bottom: 4px; }}
.meta {{ display: block; font-family: var(--mono); font-size: 12px; color: var(--text-helper); }}
footer {{ margin-top: var(--sp-10); padding-top: var(--sp-7); border-top: 1px solid var(--border-subtle);
  font-size: .88em; color: var(--text-helper); }}
footer code {{ font-family: var(--mono); background: var(--grey-1); padding: 1px 5px; color: var(--accent); }}
@media (max-width: 840px) {{ body {{ padding: var(--sp-8) var(--sp-5); }} h1 {{ font-size: 1.9em; }} .stats {{ gap: var(--sp-7); }} }}
</style>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@200;300;400;500;600;700;900&family=JetBrains+Mono:wght@400;500;600&family=Noto+Sans+SC:wght@200;300;400;500;700;900&display=swap" rel="stylesheet">
</head>
<body>
<header>
  <span class="kicker">Nacos 2.5.3 · Source Code Deep Dive</span>
  <h1>Nacos 2.5.3 深度技术研究</h1>
  <p class="sub">基于 Nacos 2.5.3 源码的逐模块分析文档 · 共 16 章 · Swiss Style 排版</p>
  <div class="stats">
    <div class="stat"><b>16</b><span>章节</span></div>
    <div class="stat"><b>{total_chars:,}</b><span>正文字数</span></div>
    <div class="stat"><b>{total_kb:,.0f} KB</b><span>HTML 体积</span></div>
  </div>
</header>
{chr(10).join(cards)}
<footer>
  <p>本文档基于 Nacos 2.5.3 开源版本源码分析编写，内容仅代表作者的个人理解和研究成果，不代表阿里巴巴或 Nacos 官方立场。</p>
  <p style="margin-top:8px">排版：guizang-ppt-skill Swiss Style 规范 · 转换工具链：<code>pandoc 3.6.2</code></p>
</footer>
</body>
</html>
'''

open(os.path.join(OUT, "index.html"), "w", encoding="utf-8").write(page)
print(f"✅ index.html 已生成")
print(f"   章节数: {len(rows)}")
print(f"   正文字数合计: {total_chars:,}")
print(f"   HTML 体积合计: {total_kb:,.0f} KB")
