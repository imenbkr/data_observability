"""Generate grafana/dashboards/volume-forecasting.json from the notebook.

Grafana Cloud stacks without the dashboard.notebooks feature can't show notebooks, so this
converts grafana/notebooks/detect-data-volume-drops.json into a regular dashboard with the same
content: markdown and code cells become text panels, panel cells become dashboard panels.

Usage: python scripts/build_notebook.py && python scripts/build_forecast_dashboard.py
"""
import base64
import html
import io
import json
import math
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "grafana" / "notebooks" / "detect-data-volume-drops.json"
OUT = ROOT / "grafana" / "dashboards" / "volume-forecasting.json"
DS = {"type": "prometheus", "uid": "${datasource}"}
ML_DS = {"type": "prometheus", "uid": "grafanacloud-ml-metrics"}
CHARS_PER_LINE = 170  # roughly, for a full-width text panel
ROW_PX = 38           # one grid unit: 30 px row + 8 px margin
PANEL_WIDTH_PX = 1350


def image_height(text):
    """Grid units for an embedded screenshot, keeping its aspect ratio."""
    from PIL import Image
    data = base64.b64decode(re.search(r"base64,([A-Za-z0-9+/=]+)", text).group(1))
    w, h = Image.open(io.BytesIO(data)).size
    return math.ceil(h * PANEL_WIDTH_PX / w / ROW_PX) + 2


def text_height(text):
    """Grid units for a full-width text panel."""
    if text.startswith("!["):
        return image_height(text)
    lines = sum(max(1, math.ceil(len(line) / CHARS_PER_LINE)) for line in text.splitlines() if line.strip())
    headings = len(re.findall(r"^#", text, re.M))
    return max(2, math.ceil(lines * 0.62 + headings * 0.7) + 1)


def code_block(language, code):
    """Fenced code block, or HTML <pre> when the code contains {{ … }}.

    Recent Grafana versions run text panels through a template engine that fails on
    {{ $value }} (alert annotation templates). Encoded braces display the same but aren't parsed.
    """
    if "{{" not in code:
        return f"```{language}\n{code}\n```"
    escaped = html.escape(code).replace("{", "&#123;").replace("}", "&#125;")
    return f"<pre><code>{escaped}</code></pre>"


def datasource(uid):
    return ML_DS if uid == "__ML_DATASOURCE_UID__" else DS


def convert(notebook):
    elements = notebook["spec"]["elements"]
    panels, y = [], 0
    for item in notebook["spec"]["layout"]["spec"]["cells"]:
        el = elements[item["spec"]["element"]["name"]]
        pid = len(panels) + 1
        if el["kind"] == "Cell":
            content = el["spec"]["content"]
            if content["kind"] == "Markdown":
                text = content["spec"]["text"]
            else:  # Code cell → fenced code block
                text = code_block(content["spec"]["language"], content["spec"]["code"])
            h = text_height(text)
            panels.append({"type": "text", "id": pid, "title": "", "transparent": True,
                           "gridPos": {"h": h, "w": 24, "x": 0, "y": y},
                           "options": {"mode": "markdown", "content": text}})
        else:  # Panel
            spec = el["spec"]
            queries = [q["spec"] for q in spec["data"]["spec"]["queries"]]
            ds = datasource(queries[0]["query"]["datasource"]["name"])
            viz = spec["vizConfig"]
            h = 6 if viz["group"] == "stat" else 9
            panels.append({
                "type": viz["group"], "id": pid, "title": spec["title"], "description": spec.get("description", ""),
                "datasource": ds, "gridPos": {"h": h, "w": 24, "x": 0, "y": y},
                "targets": [{"refId": q["refId"], "datasource": ds,
                             "expr": q["query"]["spec"]["expr"],
                             "legendFormat": q["query"]["spec"].get("legendFormat", "")} for q in queries],
                "options": viz["spec"].get("options", {}),
                "fieldConfig": viz["spec"].get("fieldConfig", {"defaults": {}, "overrides": []}),
            })
        y += panels[-1]["gridPos"]["h"]
    return panels


def main():
    notebook = json.loads(SRC.read_text())
    dashboard = {
        "uid": "volume-forecasting",
        "title": notebook["spec"]["title"],
        "description": notebook["spec"].get("description", ""),
        "tags": notebook["spec"]["tags"],
        "timezone": "browser", "refresh": "5m", "schemaVersion": 41, "editable": True, "graphTooltip": 1,
        "time": {"from": notebook["spec"]["timeSettings"]["from"], "to": notebook["spec"]["timeSettings"]["to"]},
        "templating": {"list": [{
            "name": "datasource", "label": "Data source", "type": "datasource", "query": "prometheus",
            "hide": 0, "current": {}, "refresh": 1, "includeAll": False, "multi": False,
            # local data source is "Prometheus"; on Grafana Cloud it is "grafanacloud-<stack>-prom"
            "regex": "/^(Prometheus|grafanacloud-.+-prom)$/"}]},
        "panels": convert(notebook),
    }
    OUT.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {len(dashboard['panels'])} panels to {OUT}")


if __name__ == "__main__":
    main()
