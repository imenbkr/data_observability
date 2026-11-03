"""Create or update the "Detect data volume drops" notebook on a Grafana instance.

Usage:
  python scripts/deploy_notebook.py                                    # local Grafana (http://localhost:3000)
  GRAFANA_URL=https://<stack>.grafana.net GRAFANA_TOKEN=glsa_... DATASOURCE_UID=grafanacloud-prom \\
      NAMESPACE=stacks-<id> python scripts/deploy_notebook.py

The instance needs the dashboard.notebooks feature toggle.
"""
import json
import os
import pathlib
import urllib.error
import urllib.request

SRC = pathlib.Path(__file__).resolve().parent.parent / "grafana" / "notebooks" / "detect-data-volume-drops.json"
URL = os.getenv("GRAFANA_URL", "http://localhost:3000").rstrip("/")
TOKEN = os.getenv("GRAFANA_TOKEN", "")
DS_UID = os.getenv("DATASOURCE_UID", "prom")
# Grafana ML forecasts exist on Grafana Cloud only; locally those panels stay empty
ML_DS_UID = os.getenv("ML_DATASOURCE_UID", DS_UID)
NAMESPACE = os.getenv("NAMESPACE", "default")


def call(method, path, body=None):
    req = urllib.request.Request(URL + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    if TOKEN:
        req.add_header("Authorization", f"Bearer {TOKEN}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def main():
    text = SRC.read_text().replace("__ML_DATASOURCE_UID__", ML_DS_UID).replace("__DATASOURCE_UID__", DS_UID)
    notebook = json.loads(text)
    name = notebook["metadata"]["name"]
    base = f"/apis/dashboard.grafana.app/v2beta1/namespaces/{NAMESPACE}/notebooks"
    try:
        current = call("GET", f"{base}/{name}")
        notebook["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"]
        call("PUT", f"{base}/{name}", notebook)
        action = "updated"
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise SystemExit(f"{e.code}: {e.read().decode()[:300]}")
        call("POST", base, notebook)
        action = "created"
    print(f"{action}: {URL}/notebooks/{name}")


if __name__ == "__main__":
    main()
