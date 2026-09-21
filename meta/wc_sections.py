#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按节统计 Nacos 文档字数（遵循 quality-checklist 字数规则）。
规则：
- 计：汉字/英文字母/数字/代码符号 每字符 1 字；空白不计
- 不计：Markdown 格式符（**、#、表格|、列表- 等纯格式符）；mermaid 代码块；ASCII 框图代码块
"""
import re
import sys

def is_ascii_box(text: str) -> bool:
    """是否 ASCII 框图：含制表符字符"""
    box_chars = set("┌┐└┘├┤┬┴┼─│╔╗╚╝║═")
    if any(c in box_chars for c in text):
        return True
    return False

def count_text(line: str) -> int:
    """统计一行（非代码块、非框图、非mermaid）计入的字数，剔除 Markdown 格式符"""
    s = line
    # 剔除行首标题/列表/引用/表格标记
    s = re.sub(r'^#{1,6}\s*', '', s)
    s = re.sub(r'^\s*[-*+]\s+', ' ', s)      # 列表项
    s = re.sub(r'^\s*\|', ' ', s)            # 表格行首
    s = re.sub(r'\|\s*$', ' ', s)            # 表格行尾
    # 剔除行内加粗/反引号
    s = s.replace('**', '').replace('`', '')
    # 剔除表格内分隔列
    s = s.replace('|', ' ')
    # 删除空白
    s = re.sub(r'\s+', '', s)
    return len(s)

def count_code(code: str) -> int:
    """统计代码块字数（非mermaid、非ASCII框图）：去掉空白后计字符数"""
    clean = re.sub(r'\s+', '', code)
    return len(clean)

def count_section(text: str) -> int:
    total = 0
    in_code = False
    code_is_mermaid = False
    code_is_box = False
    code_buf = []
    for raw in text.splitlines(keepends=True):
        line = raw.rstrip('\n')
        if line.strip().startswith('```'):
            if not in_code:
                # 进入代码块
                in_code = True
                lang = line.strip()[3:].strip()
                code_is_mermaid = lang.startswith('mermaid')
                code_is_box = False
                code_buf = []
            else:
                # 退出代码块
                in_code = False
                if not code_is_mermaid and not code_is_box:
                    total += count_code(''.join(code_buf))
                code_buf = []
            continue
        if in_code:
            code_buf.append(line)
            if not code_is_mermaid and not code_is_box and line.strip():
                if is_ascii_box(line):
                    code_is_box = True
            continue
        total += count_text(line)
    return total

def main():
    path = sys.argv[1]
    # 节分隔符
    starts = sys.argv[2].split(',')
    ends = sys.argv[3].split(',')
    with open(path, encoding='utf-8') as f:
        lines = f.readlines()
    total = 0
    for i, (s, e) in enumerate(zip(starts, ends)):
        sec = ''.join(lines[int(s)-1:int(e)-1])
        c = count_section(sec)
        total += c
        print(f"节[{i+1}] {sec.splitlines()[0].strip()[:30]}... => {c} 字")
    print(f"三节合计: {total} 字")

if __name__ == '__main__':
    main()
