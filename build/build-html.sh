#!/usr/bin/env bash
# 批量将 Nacos 深度研究 MD 章节转为 guizang Swiss Style HTML
set -euo pipefail

REPO="/home/sandbox/.openclaw/workspace/repo"
TPL="$(dirname "$0")/pandoc-swiss-template.html"
OUT="$REPO/chapter-html"
ASSETS="$REPO/assets"
TMPD="$(dirname "$0")/.tmp-build"

mkdir -p "$OUT" "$TMPD"

# mermaid 注入片段（仅含 mermaid 图的章节注入）
cat > "$TMPD/mermaid-snippet.html" <<'MERMAID'
<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
  mermaid.initialize({
    startOnLoad: true,
    theme: 'neutral',
    securityLevel: 'loose',
    fontFamily: '"Inter","Noto Sans SC","Helvetica Neue",Arial,sans-serif',
    themeVariables: {
      primaryColor: '#f0f0ee',
      primaryTextColor: '#0a0a0a',
      primaryBorderColor: '#002FA7',
      lineColor: '#525252',
      secondaryColor: '#e8eaf6',
      tertiaryColor: '#fafaf8',
      noteBkgColor: '#f0f0ee',
      noteBorderColor: '#002FA7',
      noteTextColor: '#0a0a0a',
      actorBkg: '#f0f0ee',
      actorBorder: '#002FA7',
      actorTextColor: '#0a0a0a',
      signalColor: '#525252',
      signalTextColor: '#0a0a0a',
      labelBoxBkgColor: '#e8eaf6',
      labelBoxBorderColor: '#002FA7',
      labelTextColor: '#0a0a0a',
      loopTextColor: '#0a0a0a',
      sequenceNumberColor: '#ffffff'
    },
    sequence: { useMaxWidth: true, wrap: false, diagramMarginX: 8, diagramMarginY: 8 }
  });
</script>
MERMAID

TITLES="第 1 章|第 2 章|第 3 章|第 4 章|第 5 章|第 6 章|第 7 章|第 8 章|第 9 章|第 10 章|第 11 章|第 12 章|第 13 章|第 14 章|第 15 章|第 16 章"
IFS='|' read -ra TARR <<< "$TITLES"

idx=0
for n in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16; do
  md="$REPO/chapters/nacos-chapter-${n}.md"
  html="$OUT/nacos-chapter-${n}.html"
  title="Nacos 2.5.3 深度研究 — ${TARR[$idx]}"
  idx=$((idx+1))

  [ -f "$md" ] || { echo "SKIP $n (md 不存在)"; continue; }

  pandoc "$md" \
    -f gfm -t html5 --standalone \
    --template="$TPL" \
    --metadata title="$title" \
    --resource-path="$ASSETS:$REPO" \
    -o "$html"

  # 含 mermaid 图的章节：在 </body> 前注入 mermaid 渲染脚本
  if grep -q '```mermaid' "$md"; then
    python3 - "$html" "$TMPD/mermaid-snippet.html" <<'PY'
import sys
html_path, snip_path = sys.argv[1], sys.argv[2]
h = open(html_path, encoding='utf-8').read()
s = open(snip_path, encoding='utf-8').read()
h = h.replace('</body>', s + '\n</body>', 1)
open(html_path, 'w', encoding='utf-8').write(h)
PY
    tag=" [+mermaid]"
  else
    tag=""
  fi

  echo "OK  $n -> $(basename "$html")  $(stat -c%s "$html") bytes${tag}"
done

echo "--- 完成 ---"
