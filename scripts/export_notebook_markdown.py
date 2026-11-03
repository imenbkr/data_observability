"""Export the notebook as Markdown: docs/detect-data-volume-drops.md.

Markdown cells are copied as-is, code cells become fenced code blocks, panels become their
PromQL queries, and embedded screenshots link to grafana/notebooks/images/ so the file
stays small and renders on GitHub.

Usage: python scripts/export_notebook_markdown.py
"""
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "grafana" / "notebooks" / "detect-data-volume-drops.json"
OUT = ROOT / "docs" / "detect-data-volume-drops.md"
IMAGES = ROOT / "grafana" / "notebooks" / "images"


def image_path(data_uri):
    """Find the screenshot file whose content matches an embedded data URI."""
    import base64
    data = base64.b64decode(data_uri.split("base64,", 1)[1])
    for path in IMAGES.glob("*.jpg"):
        if path.read_bytes() == data:
            return "../" + path.relative_to(ROOT).as_posix()
    return None


def markdown_cell(text):
    def replace(match):
        alt, uri = match.group(1), match.group(2)
        path = image_path(uri) if uri.startswith("data:") else uri
        return f"![{alt}]({path})" if path else f"*[{alt}]*"
    return re.sub(r"!\[([^\]]*)\]\((data:[^)]+|[^)]+)\)", replace, text)


def panel_cell(spec):
    queries = [q["spec"]["query"] for q in spec["data"]["spec"]["queries"]]
    ml = any(q["datasource"]["name"] == "__ML_DATASOURCE_UID__" for q in queries)
    lines = [f"**Panel: {spec['title']}** ({spec['vizConfig']['group']}"
             + (", data source `grafanacloud-ml-metrics`" if ml else "") + ")"]
    if spec.get("description"):
        lines.append(f"\n> {spec['description']}")
    lines.append("\n```promql")
    for q in queries:
        legend = q["spec"].get("legendFormat")
        lines.append((f"# {legend}\n" if legend and legend != "__auto" else "") + q["spec"]["expr"])
    lines.append("```")
    return "\n".join(lines)


def main():
    notebook = json.loads(SRC.read_text())
    spec = notebook["spec"]
    parts = [f"<!-- Exported from the Grafana notebook \"{spec['title']}\" by scripts/export_notebook_markdown.py -->"]
    for item in spec["layout"]["spec"]["cells"]:
        el = spec["elements"][item["spec"]["element"]["name"]]
        if el["kind"] == "Cell":
            content = el["spec"]["content"]
            if content["kind"] == "Markdown":
                parts.append(markdown_cell(content["spec"]["text"]))
            else:
                parts.append(f"```{content['spec']['language']}\n{content['spec']['code']}\n```")
        else:
            parts.append(panel_cell(el["spec"]))
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text("\n\n".join(parts) + "\n")
    print(f"wrote {OUT} ({OUT.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
