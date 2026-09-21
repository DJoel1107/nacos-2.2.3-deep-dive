#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""第15章全面质检(基于 quality-checklist.md)"""
import re, sys

PATH='chapters/nacos-chapter-15.md'
s=open(PATH,encoding='utf-8').read()
lines=s.splitlines()

print("========== 0. 章节结构(## 顶格小节标题) ==========")
for i,l in enumerate(lines,1):
    if re.match(r'^#{1,3}\s', l):
        print(f"{i}: {l}")

print("\n========== 1. 禁用表达/主观副词/口语化/空洞 扫描 ==========")
# 主观副词 & 口语化 & 空洞 & 无依据断言(基于 quality-checklist 7.x)
banned = ['非常','特别地','很高效','很稳定','比较好','比较快','挺','比较','特别','强大','优雅','巧妙','有意思','很好用','性能最好','效率最高','最稳定','绝对最优','完美','最佳实践之外','非常好']
for b in banned:
    idx=[i+1 for i,l in enumerate(lines) if b in l]
    if idx:
        print(f"  命中'{b}' -> 行{idx}")

print("\n========== 2. 无实质空洞描述 扫描 ==========")
for i,l in enumerate(lines,1):
    for m in ['功能强大','设计优雅','非常好','很方便','简单高效','性能很好','扩展性强(未展开)','可扩展性强','很优秀']:
        if m in l:
            print(f"  行{i}: {m}")

print("\n========== 3. 源码引用格式(file:line 检查) ==========")
refs=re.findall(r'[`]?[\w./\-]+\.java:\d+(?:-\d+)?[`]?', s)
print(f"  含行号的 java 引用数: {len(refs)}")
# 找出 .java 后无行号的引用
bad=[]
for m in re.finditer(r'([\w./]+\.java)(?!:\d)', s):
    p=m.group(1)
    if not p.startswith('source:'):
        bad.append((m.start(),p))
seen=set()
for pos,p in bad:
    line=lines[s[:pos].count('\n')]
    key=(p,line[:40])
    seen.add(key)
for p,line in seen:
    print(f"  ⚠ 无行号引用: {p} | 上下文: ...{line}")

print("\n========== 4. '在代码中可以看到'等模糊描述 ==========")
for i,l in enumerate(lines,1):
    if re.search(r'在.*(源码|代码|类).*(可以看到|可见|就能看到|通过.*可看)|从.*可以看出', l):
        print(f"  行{i}: {l.strip()[:80]}")

print("\n========== 5. 交叉引用格式(须'参见第X章') ==========")
refs_x=re.findall(r'参见第\s*\d+(?:\.\d+)?\s*章|见第\s*\d+章|第\s*\d+章', s)
print(f"  交叉引用计数: {len(refs_x)}")
for i,l in enumerate(lines,1):
    if '见上文' in l or '见下文' in l or '如上图' in l or '见下图' in l.lower():
        print(f"  ⚠ 行{i}: {l.strip()[:70]}")

print("\n========== 6. 版本号 2.5.3 检查 ==========")
v253=len(re.findall(r'2\.5\.3', s))
print(f"  '2.5.3' 出现: {v253} 次")
# 找非2.5.3的其他版本号可能误用
other=set(re.findall(r'\b2\.(?!5\.3)\d+\.\d+\b', s))
print(f"  其他 2.x.y 版本(排除2.5.3): {sorted(other)}")

print("\n========== 7. 表格编号(表 X-Y)与图编号(图 X-Y) ==========")
tabs=re.findall(r'表\s*\d+-\d+', s)
figs=re.findall(r'图\s*\d+-\d+', s)
print(f"  表格编号引用: {len(tabs)} 个 {set(tabs)}")
print(f"  图编号引用: {len(figs)} 个 {set(figs)}")
# ASCII 图数量
ascii_boxes=sum(1 for l in lines if any(c in l for c in "┌┐└┘├┤┬┴┼─│"))
print(f"  ASCII框图字符行≈: {ascii_boxes}")

print("\n========== 8. 英中混杂/术语全称 ==========")
# 简表：检查常见缩写是否给出全称(首次出现附近)
for abbr,full in [('gRPC','gRPC Remote Procedure Call'),('BOM','Bill of Materials'),('SPI','Service Provider Interface'),('JDK','Java Development Kit'),('CAS','Compare-And-Swap'),('LTS','Long Term Support')]:
    if abbr in s and full not in s:
        # gRPC/BOM 可能已给
        print(f"  提示: 缩写 {abbr} 存在但未搜到全称 '{full}'")

print("\n========== 9. 每节是否有 ASCII 图(≥1) ==========")
# 按 ## 分段统计框图行
cur=None; secbox={}
for l in lines:
    m=re.match(r'^##\s+(15\.\d+)', l)
    if m: cur=m.group(1); secbox.setdefault(cur,0)
    if cur and any(c in l for c in "┌┐└┘├┤┬┴┼─│"): secbox[cur]=secbox.get(cur,0)+1
for k,v in secbox.items(): print(f"  {k}: ASCII框图行数={v}")
