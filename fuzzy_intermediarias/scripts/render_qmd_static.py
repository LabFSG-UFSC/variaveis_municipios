#!/usr/bin/env python3
"""Renderiza o QMD como HTML estatico, sem depender do Quarto."""

from __future__ import annotations

from pathlib import Path

import markdown


ROOT = Path(__file__).resolve().parents[1]
QMD = ROOT / "relatorio_fuzzy_capitais_intermediarias.qmd"
HTML = ROOT / "relatorio_fuzzy_capitais_intermediarias.html"


def main() -> int:
    text = QMD.read_text(encoding="utf-8")
    if text.startswith("---"):
        partes = text.split("---", 2)
        if len(partes) >= 3:
            text = partes[2]

    corpo = markdown.markdown(
        text,
        extensions=["fenced_code", "tables", "sane_lists", "nl2br"],
    )

    html = f"""<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Fuzzy das Capitais Intermediarias</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
      line-height: 1.6;
      max-width: 1100px;
      margin: 40px auto;
      padding: 0 24px;
      color: #1f2937;
    }}
    h1, h2, h3 {{ line-height: 1.2; }}
    code {{
      background: #f3f4f6;
      padding: 0.15rem 0.35rem;
      border-radius: 4px;
    }}
    pre {{
      background: #0b1020;
      color: #e5e7eb;
      padding: 16px;
      border-radius: 10px;
      overflow-x: auto;
    }}
    pre code {{
      background: transparent;
      padding: 0;
    }}
    blockquote {{
      border-left: 4px solid #d1d5db;
      margin: 1rem 0;
      padding-left: 1rem;
      color: #4b5563;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      margin: 1rem 0;
    }}
    th, td {{
      border: 1px solid #d1d5db;
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ background: #f9fafb; }}
  </style>
</head>
<body>
{corpo}
</body>
</html>
"""
    HTML.write_text(html, encoding="utf-8")
    print(HTML)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
