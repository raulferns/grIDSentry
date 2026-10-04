import json
import os
import html

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
NB_PATH = os.path.join(PROJECT_ROOT, "BDA_MiniProject.ipynb")
HTML_OUT = os.path.join(PROJECT_ROOT, "dashboard", "notebook.html")

def render_notebook_to_html():
    with open(NB_PATH, "r", encoding="utf-8") as f:
        nb = json.load(f)

    cells_html = []
    cell_idx = 1

    for cell in nb.get("cells", []):
        cell_type = cell.get("cell_type")
        raw_source = "".join(cell.get("source", []))

        if cell_type == "markdown":
            # Simple markdown to HTML conversion
            lines = raw_source.split("\n")
            formatted_lines = []
            in_list = False
            for line in lines:
                line_str = line.strip()
                if line_str.startswith("# "):
                    formatted_lines.append(f"<h1 class='nb-h1'>{html.escape(line_str[2:])}</h1>")
                elif line_str.startswith("## "):
                    formatted_lines.append(f"<h2 class='nb-h2'>{html.escape(line_str[3:])}</h2>")
                elif line_str.startswith("### "):
                    formatted_lines.append(f"<h3 class='nb-h3'>{html.escape(line_str[4:])}</h3>")
                elif line_str.startswith("- ") or line_str.startswith("* "):
                    if not in_list:
                        formatted_lines.append("<ul class='nb-ul'>")
                        in_list = True
                    formatted_lines.append(f"<li>{html.escape(line_str[2:])}</li>")
                elif line_str.startswith("1. ") or line_str.startswith("2. ") or line_str.startswith("3. ") or line_str.startswith("4. ") or line_str.startswith("5. ") or line_str.startswith("6. ") or line_str.startswith("7. "):
                    formatted_lines.append(f"<div class='nb-num-item'><b>{line_str[:3]}</b> {html.escape(line_str[3:])}</div>")
                elif line_str == "---":
                    if in_list:
                        formatted_lines.append("</ul>")
                        in_list = False
                    formatted_lines.append("<hr class='nb-hr'/>")
                elif line_str:
                    if in_list:
                        formatted_lines.append("</ul>")
                        in_list = False
                    formatted_lines.append(f"<p class='nb-p'>{html.escape(line_str)}</p>")
                else:
                    if in_list:
                        formatted_lines.append("</ul>")
                        in_list = False
            if in_list:
                formatted_lines.append("</ul>")

            md_content = "\n".join(formatted_lines)
            cells_html.append(f"""
            <div class="nb-cell nb-markdown-cell">
              <div class="nb-prompt"></div>
              <div class="nb-rendered-markdown">{md_content}</div>
            </div>
            """)

        elif cell_type == "code":
            escaped_code = html.escape(raw_source)
            cell_outputs_html = []
            for out in cell.get("outputs", []):
                otype = out.get("output_type")
                if otype == "stream":
                    stream_text = "".join(out.get("text", []))
                    escaped_stream = html.escape(stream_text)
                    cell_outputs_html.append(f"""
                    <div class="nb-output-stream"><pre>{escaped_stream}</pre></div>
                    """)
                elif otype in ["display_data", "execute_result"]:
                    data = out.get("data", {})
                    if "image/png" in data:
                        img_b64 = data["image/png"]
                        cell_outputs_html.append(f"""
                        <div class="nb-output-image"><img src="data:image/png;base64,{img_b64}" alt="Cell Plot" /></div>
                        """)
                    elif "text/plain" in data:
                        plain_text = "".join(data["text/plain"])
                        cell_outputs_html.append(f"""
                        <div class="nb-output-stream"><pre>{html.escape(plain_text)}</pre></div>
                        """)

            outputs_rendered = "".join(cell_outputs_html)
            cells_html.append(f"""
            <div class="nb-cell nb-code-cell">
              <div class="nb-input-area">
                <div class="nb-prompt">In [{cell_idx}]:</div>
                <div class="nb-code-box"><pre><code class="language-python">{escaped_code}</code></pre></div>
              </div>
              {f'<div class="nb-output-area">{outputs_rendered}</div>' if outputs_rendered.strip() else ''}
            </div>
            """)
            cell_idx += 1

    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>BDA_MiniProject.ipynb | Interactive Spark Notebook View</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css">
  <style>
    body {{
      background: #0d1117;
      color: #c9d1d9;
      font-family: 'Inter', sans-serif;
      margin: 0;
      padding: 2rem;
    }}
    .nb-container {{
      max-width: 1080px;
      margin: 0 auto;
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 12px;
      padding: 2rem;
      box-shadow: 0 8px 24px rgba(0,0,0,0.5);
    }}
    .top-bar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid #30363d;
      padding-bottom: 1rem;
      margin-bottom: 2rem;
    }}
    .nb-title-tag {{
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      font-weight: 700;
      color: #58a6ff;
      font-size: 1.1rem;
    }}
    .btn-back {{
      background: #238636;
      color: #fff;
      padding: 0.5rem 1rem;
      border-radius: 6px;
      text-decoration: none;
      font-weight: 600;
      font-size: 0.85rem;
    }}
    .btn-back:hover {{ background: #2ea043; }}
    .nb-cell {{ margin-bottom: 1.5rem; }}
    .nb-h1 {{ font-size: 1.8rem; color: #58a6ff; margin-bottom: 0.5rem; border-bottom: 1px solid #30363d; padding-bottom: 0.4rem; }}
    .nb-h2 {{ font-size: 1.35rem; color: #79c0ff; margin-top: 1.5rem; margin-bottom: 0.5rem; }}
    .nb-h3 {{ font-size: 1.1rem; color: #d2a8ff; margin-top: 1rem; margin-bottom: 0.5rem; }}
    .nb-p {{ font-size: 0.95rem; line-height: 1.6; margin-bottom: 0.75rem; color: #8b949e; }}
    .nb-ul {{ padding-left: 1.5rem; margin-bottom: 1rem; color: #8b949e; font-size: 0.95rem; line-height: 1.6; }}
    .nb-num-item {{ font-size: 0.95rem; line-height: 1.6; color: #8b949e; margin-bottom: 0.35rem; }}
    .nb-hr {{ border: 0; border-top: 1px solid #30363d; margin: 1.5rem 0; }}
    .nb-input-area {{ display: flex; gap: 1rem; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 0.75rem 1rem; }}
    .nb-prompt {{ font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: #ff7b72; min-width: 65px; padding-top: 4px; user-select: none; }}
    .nb-code-box {{ flex: 1; overflow-x: auto; }}
    .nb-code-box pre {{ margin: 0; }}
    .nb-code-box code {{ font-family: 'JetBrains Mono', monospace; font-size: 0.88rem; }}
    .nb-output-area {{ margin-top: 0.75rem; padding-left: 75px; }}
    .nb-output-stream {{ background: #090d13; border: 1px solid #21262d; border-radius: 6px; padding: 0.75rem 1rem; margin-bottom: 0.75rem; overflow-x: auto; }}
    .nb-output-stream pre {{ margin: 0; font-family: 'JetBrains Mono', monospace; font-size: 0.82rem; color: #7ee787; line-height: 1.5; }}
    .nb-output-image {{ margin-top: 0.75rem; text-align: center; }}
    .nb-output-image img {{ max-width: 100%; border-radius: 8px; border: 1px solid #30363d; box-shadow: 0 4px 16px rgba(0,0,0,0.4); }}
  </style>
</head>
<body>
  <div class="nb-container">
    <div class="top-bar">
      <div class="nb-title-tag">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
        BDA_MiniProject.ipynb (Interactive Web View)
      </div>
      <a href="index.html" class="btn-back">← Back to grIDSentry Dashboard</a>
    </div>
    {"".join(cells_html)}
  </div>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
</body>
</html>
"""

    with open(HTML_OUT, "w", encoding="utf-8") as f:
        f.write(full_html)
    print(f"[SUCCESS] Exported standalone HTML notebook view to: {HTML_OUT}")

if __name__ == "__main__":
    render_notebook_to_html()
