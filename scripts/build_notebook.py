"""Generate grafana/notebooks/detect-data-volume-drops.json, a Grafana Notebook.

step by step tutorial on detecting volume changes, with Prometheus queries on the Tunisia flights pipeline.

The data source UID is left as __DATASOURCE_UID__ and filled in at deploy time
("prom" locally, "grafanacloud-prom" on Grafana Cloud).

Usage: python scripts/build_notebook.py
"""
import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "grafana" / "notebooks" / "detect-data-volume-drops.json"
DS_UID = "__DATASOURCE_UID__"
RULES = "prometheus/rules.yml"
ALERTS = "prometheus/alerts.yml"

elements = {}
layout = []


def add(name, element):
    elements[name] = element
    layout.append({"kind": "NotebookLayoutItem",
                   "spec": {"source": "user", "element": {"kind": "ElementReference", "name": name}}})


def md(name, text):
    add(name, {"kind": "Cell", "spec": {"content": {"kind": "Markdown", "spec": {"text": text.strip()}}}})


def code(name, language, text):
    add(name, {"kind": "Cell", "spec": {"content": {"kind": "Code",
                                                    "spec": {"language": language, "code": text.strip()}}}})


def panel(name, title, viz, queries, description="", options=None, defaults=None, overrides=None):
    add(name, {"kind": "Panel", "spec": {
        "id": len(elements) + 1, "title": title, "description": description, "links": [],
        "data": {"kind": "QueryGroup", "spec": {
            "queries": [{"kind": "PanelQuery", "spec": {
                "refId": chr(65 + i), "hidden": False,
                "query": {"kind": "DataQuery", "group": "prometheus", "version": "v0",
                          "datasource": {"name": DS_UID},
                          "spec": {"expr": expr, "legendFormat": legend, "range": True, "instant": False}}}}
                for i, (expr, legend) in enumerate(queries)],
            "transformations": [], "queryOptions": {}}},
        "vizConfig": {"kind": "VizConfig", "group": viz, "version": "", "spec": {
            "options": options or {},
            "fieldConfig": {"defaults": defaults or {}, "overrides": overrides or []}}},
    }})


def by_name(name, **props):
    return {"matcher": {"id": "byName", "options": name},
            "properties": [{"id": k.replace("__", "."), "value": v} for k, v in props.items()]}


# --- Introduction -----------------------------------------------------------------------
md("intro", """
# Detect data volume drops

A drop in data volume leaves gaps in the information that decisions are based on, and it rarely
throws an error. This notebook rebuilds the Dynatrace tutorial
[*Detect data volume drops*](https://docs.dynatrace.com/docs/observe/data-observability/use-cases/detect-data-volume-drops)
with Grafana, Prometheus and the **Tunisia flights** pipeline:

> adsb.lol (live ADS-B) → producer → Kafka `tunisia.flights` → quality checker → Prometheus → Grafana

How you detect a drop depends on **how often data arrives**, so there are two parts:

1. **High ingestion frequency:** a continuous stream, compared with a baseline.
2. **Low ingestion frequency:** batches, compared with a one-step-ahead forecast.
""")

# --- Part 1: high frequency -----------------------------------------------------------------
md("high-title", """
## 1. High ingestion frequency: baseline comparison

Our source is a continuous stream: every minute, adsb.lol returns about 1,000 aircraft over
Tunisia and Europe. Dynatrace learns a baseline automatically. In Prometheus we use the same moment
**yesterday** as the baseline, which absorbs the daily rhythm of air traffic for free.
""")
panel("high-panel", "Aircraft reported by adsb.lol: today vs. yesterday", "timeseries",
      [("sum(adsb_aircraft_in_areas)", "today"),
       ("0.5 * sum(adsb_aircraft_in_areas offset 1d)", "alert threshold (50% of yesterday)")],
      description="The alert fires when 'today' stays below the dashed line for 15 minutes.",
      defaults={"unit": "short"},
      overrides=[by_name("alert threshold (50% of yesterday)",
                         custom__lineStyle={"fill": "dash", "dash": [10, 10]},
                         color={"mode": "fixed", "fixedColor": "red"})])
md("high-rules", f"""
Two alert rules cover this case (in `{ALERTS}`, group `volume-drop-alerts`):

- **Volume drop:** less than half of yesterday's volume for 15 minutes.
- **Missing data:** no data at all for 10 minutes, the worst case of a drop.
""")
code("high-code", "yaml", """
- alert: SourceVolumeBelowBaseline
  expr: sum(adsb_aircraft_in_areas) < 0.5 * sum(adsb_aircraft_in_areas offset 1d)
  for: 15m
  labels: { pillar: volume, team: data-engineering, severity: warning }

- alert: SourceDataMissing
  expr: absent_over_time(adsb_aircraft_in_areas[10m])
  labels: { pillar: volume, team: data-engineering, severity: page }
""")
md("high-tune", """
**Tune before you trust it.** Dynatrace has an alert preview. In Grafana, open the rule in
**Alerting → Alert rules → Edit** and use **Preview** to see how often it would have fired over the
last days. Raise or lower the `0.5` factor until it only fires on real incidents.
""")

# --- Part 2: low frequency ------------------------------------------------------------------
md("low-title", """
## 2. Low ingestion frequency: one-step-ahead forecast

For batch data, a fixed threshold is too blunt and a rate is meaningless between batches. The
Dynatrace tutorial forecasts the **next hour's record count** and checks whether the actual count lands
inside the prediction interval. We treat the **number of valid flight positions per hour** as our batch.

| Dynatrace | Grafana / Prometheus |
|---|---|
| Workflow with cron trigger `0 * * * *` | Recording rules evaluated by the ruler on a schedule |
| Generic Forecast Analysis | `avg_over_time` ± `stddev_over_time` over the history |
| Learning period 72 h, end time `now-1h` | Subquery `[72h:1h] offset 1h`: 72 hourly samples, current hour excluded |
| Coverage probability 0.9 | ± 1.645 standard deviations (normal approximation) |
| Data points to predict: 1 | One band value per evaluation, compared with the current hour |
| Run JavaScript: compare and alert | An alert rule comparing current count and lower bound |
""")

md("step1", f"""
### Step 1: Schedule the analysis

Instead of a workflow, the Prometheus ruler evaluates the group `volume-forecast` in `{RULES}`
every 5 minutes. On Grafana Cloud, the same file is uploaded to the stack with `mimirtool rules load`.
""")
code("step1-code", "yaml", """
- name: volume-forecast
  interval: 5m
  rules:
    - record: flights:positions:count1h
      expr: sum(increase(flights_valid_positions_total[1h]))
""")

md("step2", """
### Step 2: Analyze the data (forecast)

The forecast is the mean of the previous 72 hourly counts, with a band of ± 1.645 standard
deviations around it: about **90%** of normal hours should fall inside it.
""")
code("step2-code", "yaml", """
- record: flights:positions:lower_bound
  expr: |
    clamp_min(
      avg_over_time(flights:positions:count1h[72h:1h] offset 1h)
      - 1.645 * stddev_over_time(flights:positions:count1h[72h:1h] offset 1h), 0)
- record: flights:positions:upper_bound
  expr: |
    avg_over_time(flights:positions:count1h[72h:1h] offset 1h)
    + 1.645 * stddev_over_time(flights:positions:count1h[72h:1h] offset 1h)
""")
panel("step2-panel", "Hourly flight positions and the 90% prediction band", "timeseries",
      [("flights:positions:count1h", "actual (last hour)"),
       ("flights:positions:upper_bound", "upper bound"),
       ("flights:positions:lower_bound", "lower bound"),
       ("flights:positions:forecast_mean", "forecast")],
      description="A point of 'actual' below the shaded band is a volume drop. The band appears once a few hours of history exist.",
      defaults={"unit": "short"},
      overrides=[
          by_name("upper bound", custom__fillBelowTo="lower bound", custom__lineWidth=0, custom__fillOpacity=20,
                  color={"mode": "fixed", "fixedColor": "blue"}),
          by_name("lower bound", custom__lineWidth=0, color={"mode": "fixed", "fixedColor": "blue"}),
          by_name("forecast", custom__lineStyle={"fill": "dash", "dash": [10, 10]},
                  color={"mode": "fixed", "fixedColor": "blue"}),
          by_name("actual (last hour)", custom__lineWidth=2, color={"mode": "fixed", "fixedColor": "green"}),
      ])
md("step2-season", """
**Seasonality matters here.** Air traffic has a strong daily cycle (almost nothing at 03:00, peaks in
the morning), so a flat 72-hour band is wide during the day and misfires at night. The Dynatrace
tutorial gives the same advice: for seasonal data, train on **at least two weeks**. Two practical
options:

- compare with the **same hour on previous days** (`offset 1d`, `offset 7d`), as in part 1;
- on Grafana Cloud, use **Machine Learning → Metric forecasts**, which learns daily and weekly
  seasonality and plays the role of the Dynatrace forecast analyzer.
""")

md("step3", """
### Step 3: Retrieve the current records

The equivalent of the tutorial's `fetch events, from:now()-60min | summarize count()`:
""")
code("step3-code", "promql", "sum(increase(flights_valid_positions_total[1h]))")
panel("step3-panel", "Flight positions received in the last hour", "stat",
      [("flights:positions:count1h", "last hour"), ("flights:positions:lower_bound", "lower bound")],
      options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area",
               "textMode": "value_and_name"},
      defaults={"unit": "short", "decimals": 0})

md("step4", f"""
### Step 4: Compare prediction and current volume, then alert

Dynatrace runs a JavaScript step that compares the count with the forecast's lower bound and
creates an *Abnormal data volume drop* event. Here it is a single alert rule (in `{ALERTS}`).

The last line is a lesson from real data: without it, the rule fires every night, because the
forecast still expects daytime traffic. It only alerts when the same hour yesterday had real traffic.
""")
code("step4-code", "yaml", """
- alert: FlightVolumeBelowForecast
  expr: |
    flights:positions:count1h < flights:positions:lower_bound
    and on() flights:positions:count1h offset 1d > 100
  for: 10m
  labels: { pillar: volume, team: data-engineering, severity: warning }
  annotations:
    summary: "Only {{ $value | humanize }} flight positions in the last hour, below the forecast's lower bound"
""")
panel("step4-panel", "Headroom above the lower bound", "timeseries",
      [("flights:positions:count1h - flights:positions:lower_bound", "actual − lower bound")],
      description="Below zero (red area) the current hour is under the forecast band.",
      defaults={"unit": "short", "custom": {"thresholdsStyle": {"mode": "area"}},
                "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None},
                                                             {"color": "transparent", "value": 0}]}})

md("step5", """
### Step 5: Observe the low data volume alerts

Firing and pending volume alerts are listed below and in **Alerting → Alert rules** (filter
`pillar=volume`). To get notified, add a contact point (email, Slack) and a notification policy
matching `team=data-engineering`.
""")
panel("step5-panel", "Volume alerts", "state-timeline",
      [('max by (alertname) (ALERTS{pillar="volume", alertstate="firing"})', "{{alertname}}"),
       ('vector(0) unless on() count(ALERTS{pillar="volume", alertstate="firing"})', "all volume checks")],
      options={"showValue": "never", "mergeValues": True},
      defaults={"mappings": [{"type": "value", "options": {
          "0": {"text": "OK", "color": "green", "index": 0},
          "1": {"text": "Firing", "color": "red", "index": 1}}}]})

md("summary", """
## Summary

| Data arrives… | Detect drops with | Rule |
|---|---|---|
| Continuously (stream) | Comparison with the same time yesterday, plus a missing-data check | `SourceVolumeBelowBaseline`, `SourceDataMissing` |
| In batches (hourly) | One-step-ahead forecast with a 90% prediction band | `FlightVolumeBelowForecast` |

Start with volume: it is the cheapest check and catches the most incidents.
""")

notebook = {
    "apiVersion": "dashboard.grafana.app/v2beta1",
    "kind": "Notebook",
    "metadata": {"name": "detect-data-volume-drops"},
    "spec": {
        "title": "Detect data volume drops · Tunisia flights",
        "description": "Grafana version of the Dynatrace data-observability tutorial, on live flight data.",
        "tags": ["data-observability", "volume", "tutorial"],
        "timeSettings": {"from": "now-24h", "to": "now", "autoRefresh": "", "autoRefreshIntervals": [],
                         "hideTimepicker": False, "fiscalYearStartMonth": 0},
        "elements": elements,
        "layout": {"kind": "NotebookLayout", "spec": {"cells": layout}},
    },
}

OUT.parent.mkdir(exist_ok=True)
OUT.write_text(json.dumps(notebook, indent=2, ensure_ascii=False) + "\n")
print(f"wrote {len(layout)} cells to {OUT}")
