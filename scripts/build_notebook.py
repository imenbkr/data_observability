"""Generate grafana/notebooks/detect-data-volume-drops.json, a Grafana Notebook.

A step-by-step tutorial on detecting data volume drops, with Prometheus queries and Grafana
Machine Learning forecasts on the Tunisia flights pipeline.

Data source UIDs are placeholders filled in at deploy time:
  __DATASOURCE_UID__     "prom" locally, "grafanacloud-prom" on Grafana Cloud
  __ML_DATASOURCE_UID__  "grafanacloud-ml-metrics" on Grafana Cloud (ML forecasts are Cloud only)

Usage: python scripts/build_notebook.py
"""
import base64
import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "grafana" / "notebooks" / "detect-data-volume-drops.json"
DS_UID = "__DATASOURCE_UID__"
ML_DS_UID = "__ML_DATASOURCE_UID__"
ML_METRIC = "tunisia_flight_positions"
RULES = "prometheus/rules.yml"
ALERTS = "prometheus/alerts.yml"
IMAGES = OUT.parent / "images"  # taken by scripts/take_screenshots.py

elements = {}
layout = []


def add(name, element):
    elements[name] = element
    layout.append({"kind": "NotebookLayoutItem",
                   "spec": {"source": "user", "element": {"kind": "ElementReference", "name": name}}})


def md(name, text):
    add(name, {"kind": "Cell", "spec": {"content": {"kind": "Markdown", "spec": {"text": text.strip()}}}})


def screenshot(name, file, caption):
    """Markdown cell with an embedded screenshot (data URI), skipped if the image is missing."""
    path = IMAGES / f"{file}.jpg"
    if not path.exists():
        return
    uri = "data:image/jpeg;base64," + base64.b64encode(path.read_bytes()).decode()
    md(name, f"![{caption}]({uri})\n\n*{caption}*")


def code(name, language, text):
    add(name, {"kind": "Cell", "spec": {"content": {"kind": "Code",
                                                    "spec": {"language": language, "code": text.strip()}}}})


def panel(name, title, viz, queries, description="", options=None, defaults=None, overrides=None, datasource=DS_UID):
    add(name, {"kind": "Panel", "spec": {
        "id": len(elements) + 1, "title": title, "description": description, "links": [],
        "data": {"kind": "QueryGroup", "spec": {
            "queries": [{"kind": "PanelQuery", "spec": {
                "refId": chr(65 + i), "hidden": False,
                "query": {"kind": "DataQuery", "group": "prometheus", "version": "v0",
                          "datasource": {"name": datasource},
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


def band_overrides(actual, upper, lower, center):
    """Shade the area between upper and lower; dashed center line; bold actual."""
    return [
        by_name(upper, custom__fillBelowTo=lower, custom__lineWidth=0, custom__fillOpacity=20,
                color={"mode": "fixed", "fixedColor": "blue"}),
        by_name(lower, custom__lineWidth=0, color={"mode": "fixed", "fixedColor": "blue"}),
        by_name(center, custom__lineStyle={"fill": "dash", "dash": [10, 10]}, color={"mode": "fixed", "fixedColor": "blue"}),
        by_name(actual, custom__lineWidth=2, color={"mode": "fixed", "fixedColor": "green"}),
    ]


# --- Introduction -----------------------------------------------------------------------
md("intro", """
# Detect data volume drops

A drop in data volume leaves gaps in the information that decisions are based on, and it rarely
throws an error. This notebook shows how to catch volume drops with Grafana, Prometheus and
Grafana Machine Learning on a live pipeline that tracks every flight to and from Tunisia:

> adsb.lol (live ADS-B) → producer → Kafka `tunisia.flights` → quality checker → Prometheus → Grafana

How you detect a drop depends on **how often data arrives**:

1. **High ingestion frequency:** a continuous stream, compared with a baseline.
2. **Low ingestion frequency:** batches, compared with a one-step-ahead forecast.
3. **Better forecasts:** seasonal baselines, PromQL trend functions and Grafana Machine Learning.
""")

# --- Part 1: high frequency -----------------------------------------------------------------
md("context", """
### The data

The live dashboard tracks every flight to and from Tunisia, and each position is one record on the
Kafka topic `tunisia.flights`. These record counts are what we monitor below.
""")
screenshot("context-map", "flight-map", "Flights to and from Tunisia, live from the Tunisia Flights dashboard")
screenshot("context-table", "flights-table", "The same flights as a table: route, status and estimated arrival")
screenshot("context-kafka", "kafka-messages", "Raw records on the Kafka topic tunisia.flights (Kafka UI)")

md("high-title", """
## 1. High ingestion frequency: baseline comparison

Our source is a continuous stream: every minute, adsb.lol returns about 1,000 aircraft over
Tunisia and Europe. The simplest robust baseline is the same moment **yesterday**: it absorbs the
daily rhythm of air traffic for free.
""")
panel("high-panel", "Aircraft reported by adsb.lol: today vs. yesterday", "timeseries",
      [("sum(adsb_aircraft_in_areas)", "today"),
       ("0.5 * sum(adsb_aircraft_in_areas offset 1d)", "alert threshold (50% of yesterday)")],
      description="The alert fires when 'today' stays below the dashed line for 15 minutes. The dashed line is the same data shifted by 24 hours (offset 1d), so it only appears once the pipeline has run for a day.",
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
**Tune before you trust it.** Open the rule in **Alerting → Alert rules → Edit** and use
**Preview** to see how often it would have fired over the last days. Raise or lower the `0.5`
factor until it only fires on real incidents.
""")

# --- Part 2: low frequency ------------------------------------------------------------------
md("low-title", """
## 2. Low ingestion frequency: one-step-ahead forecast

For batch data, a fixed threshold is too blunt and a rate is meaningless between batches. Instead,
forecast the **next hour's record count** and check whether the actual count lands inside the
prediction interval. We treat the **number of valid flight positions per hour** as our batch.

| Forecast setting | Prometheus implementation |
|---|---|
| Runs on a schedule | Recording rules evaluated by the ruler every 5 minutes |
| Training period: 72 hours, current hour excluded | Subquery `[72h:1h] offset 1h`: 72 hourly samples |
| Coverage probability: 90% | ± 1.645 standard deviations (normal approximation) |
| Points to predict: 1 | One band value per evaluation, compared with the current hour |
""")

md("step1", f"""
### Step 1: Schedule the analysis

The Prometheus ruler evaluates the group `volume-forecast` in `{RULES}` every 5 minutes. On Grafana
Cloud, the same file is uploaded to the stack with `mimirtool rules load`.
""")
code("step1-code", "yaml", """
- name: volume-forecast
  interval: 5m
  rules:
    - record: flights:positions:count1h
      expr: sum(increase(flights_valid_positions_total[1h]))
""")

md("step2", """
### Step 2: Forecast

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
      overrides=band_overrides("actual (last hour)", "upper bound", "lower bound", "forecast"))

md("step3", """
### Step 3: Retrieve the current records

Records received in the last hour:
""")
code("step3-code", "promql", "sum(increase(flights_valid_positions_total[1h]))")
panel("step3-panel", "Flight positions received in the last hour", "stat",
      [("flights:positions:count1h", "last hour"), ("flights:positions:lower_bound", "lower bound")],
      options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area",
               "textMode": "value_and_name"},
      defaults={"unit": "short", "decimals": 0})

md("step4", f"""
### Step 4: Compare prediction and current volume, then alert

A single alert rule (in `{ALERTS}`) compares the current count with the forecast's lower bound.

The last line is a lesson from real data: without it, the rule fires every night, because the flat
forecast still expects daytime traffic. It only alerts when the same hour yesterday had real traffic.
Part 3 shows forecasts that understand the daily cycle and don't need this guard.
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
      description="Below zero (red area) the current hour is under the forecast band. Needs more than 1 hour of history (the bound uses offset 1h); meaningful after about 3 hours.",
      defaults={"unit": "short", "custom": {"thresholdsStyle": {"mode": "area"}},
                "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None},
                                                             {"color": "transparent", "value": 0}]}})

md("step5", """
### Step 5: Observe the low data volume alerts

Firing volume alerts are listed below and in **Alerting → Alert rules** (filter `pillar=volume`).
To get notified, add a contact point (email, Slack) and a notification policy matching
`team=data-engineering`.
""")
screenshot("step5-rules", "volume-alert-rules",
           "Alerting → Alert rules filtered on pillar=volume: the five volume rules from this notebook")
panel("step5-panel", "Volume alerts", "state-timeline",
      [('max by (alertname) (ALERTS{pillar="volume", alertstate="firing"})', "{{alertname}}"),
       ('vector(0) unless on() count(ALERTS{pillar="volume", alertstate="firing"})', "all volume checks")],
      options={"showValue": "never", "mergeValues": True},
      defaults={"mappings": [{"type": "value", "options": {
          "0": {"text": "OK", "color": "green", "index": 0},
          "1": {"text": "Firing", "color": "red", "index": 1}}}]})

# --- Part 3: better forecasts ---------------------------------------------------------------
md("ml-title", """
## 3. Better forecasts with Grafana

Air traffic has a strong daily cycle: almost nothing at 03:00, peaks in the morning. A flat
72-hour average is too wide during the day and misfires at night. For seasonal data, a model needs
to learn the cycle, ideally from **two weeks or more** of history. Grafana offers three levels:

| Approach | Learns the daily cycle | Where it runs | Needs |
|---|---|---|---|
| **A. Seasonal baseline** (PromQL) | Yes, same hour on the last 7 days | Any Prometheus | 2+ days of data |
| **B. Trend extrapolation** (`predict_linear`) | No, short-term trend only | Any Prometheus | A few hours |
| **C. Grafana Machine Learning** forecast | Yes, daily and weekly, automatically | Grafana Cloud | 2+ days (daily), 2+ weeks (weekly) |
""")

md("seasonal", f"""
### A. Seasonal baseline: same hour on previous days

The forecast for this hour is the mean of the **same hour on each of the last 7 days**, with a
± 1.645 sd band. At night the expected value is close to zero, so the band no longer misfires. The
rules are in the group `volume-forecast-seasonal` (`{RULES}`). The `or` with a `day` label keeps
the result valid while fewer than 7 days of history exist.
""")
code("seasonal-code", "yaml", """
- record: flights:positions:seasonal_mean
  expr: |
    avg without (day) (
        label_replace(flights:positions:count1h offset 1d, "day", "1", "", "")
     or label_replace(flights:positions:count1h offset 2d, "day", "2", "", "")
     # … up to offset 7d
    )
""")
panel("seasonal-panel", "Seasonal baseline: same hour on the last 7 days", "timeseries",
      [("flights:positions:count1h", "actual (last hour)"),
       ("flights:positions:seasonal_upper", "upper bound"),
       ("flights:positions:seasonal_lower", "lower bound"),
       ("flights:positions:seasonal_mean", "seasonal forecast")],
      description="Appears after 1 day of history; the band needs at least 2 days.",
      defaults={"unit": "short"},
      overrides=band_overrides("actual (last hour)", "upper bound", "lower bound", "seasonal forecast"))

md("trend", """
### B. Short-term trend with `predict_linear`

PromQL's `predict_linear(v[range], seconds)` fits a straight line through recent samples and
extrapolates it. Here: where will the hourly count be **one hour from now**, based on the last
3 hours? It reacts quickly to a sudden decline but knows nothing about the daily cycle, so it is
best used together with A or C, not alone.
""")
code("trend-code", "promql", "predict_linear(flights:positions:count1h[3h], 3600)")
panel("trend-panel", "Linear trend: expected hourly count in 1 hour", "timeseries",
      [("flights:positions:count1h", "actual (last hour)"),
       ("clamp_min(predict_linear(flights:positions:count1h[3h], 3600), 0)", "predicted in 1 h")],
      defaults={"unit": "short"},
      overrides=[by_name("predicted in 1 h", custom__lineStyle={"fill": "dash", "dash": [10, 10]},
                         color={"mode": "fixed", "fixedColor": "purple"}),
                 by_name("actual (last hour)", custom__lineWidth=2, color={"mode": "fixed", "fixedColor": "green"})])

md("ml", f"""
### C. Grafana Machine Learning: metric forecast

**Grafana Cloud Machine Learning** trains a forecasting model on the history of a query, detects
daily and weekly seasonality automatically, and writes the prediction and a 95% interval back as
metrics in the `grafanacloud-ml-metrics` data source:

- `{ML_METRIC}:predicted{{ml_forecast="yhat"}}`: the forecast
- `{ML_METRIC}:predicted{{ml_forecast="yhat_lower"}}` and `…="yhat_upper"}}`: the interval
- `{ML_METRIC}:actual`: the real values, for comparison

The forecast job and its alert are managed as code in `terraform/main.tf`. The model retrains every
24 hours, and its seasonality improves as history grows. The model needs at least **100 data
points** before its first training succeeds (about 3.5 hours at one point every 2 minutes).
*Available on Grafana Cloud only.*
""")
code("ml-code", "hcl", f"""
resource "grafana_machine_learning_job" "flight_positions_forecast" {{
  name            = "Tunisia flight positions per hour"
  metric          = "{ML_METRIC}"
  datasource_type = "prometheus"
  datasource_uid  = var.prometheus_uid
  query_params    = {{ expr = "flights:positions:count1h" }}
  interval        = 120  # seconds between training points
}}

resource "grafana_machine_learning_alert" "flight_positions_drop" {{
  job_id            = grafana_machine_learning_job.flight_positions_forecast.id
  title             = "Flight positions below ML forecast"
  anomaly_condition = "low"
  for               = "15m"
  labels            = {{ pillar = "volume", team = "data-engineering" }}
}}
""")
panel("ml-panel", "Grafana ML forecast with 95% interval", "timeseries",
      [(f'{ML_METRIC}:actual', "actual"),
       (f'{ML_METRIC}:predicted{{ml_forecast="yhat_upper"}}', "upper bound"),
       (f'{ML_METRIC}:predicted{{ml_forecast="yhat_lower"}}', "lower bound"),
       (f'{ML_METRIC}:predicted{{ml_forecast="yhat"}}', "ML forecast")],
      description="Grafana Cloud Machine Learning. Points of 'actual' outside the band are anomalies.",
      defaults={"unit": "short"},
      overrides=band_overrides("actual", "upper bound", "lower bound", "ML forecast"),
      datasource=ML_DS_UID)
md("ml-alert", """
The ML alert fires when actual values stay **below** the forecast interval for 15 minutes. The
equivalent PromQL condition, if you prefer a regular alert rule on the `grafanacloud-ml-metrics`
data source:
""")
code("ml-alert-code", "promql", f"""
{ML_METRIC}:actual
  < bool ignoring(ml_forecast)
{ML_METRIC}:predicted{{ml_forecast="yhat_lower"}}
""")

md("summary", """
## Summary

| Data arrives… | Detect drops with | Rule |
|---|---|---|
| Continuously (stream) | Comparison with the same time yesterday, plus a missing-data check | `SourceVolumeBelowBaseline`, `SourceDataMissing` |
| In batches (hourly) | One-step-ahead forecast with a 90% prediction band | `FlightVolumeBelowForecast` |
| With a daily or weekly cycle | Seasonal baseline, or a Grafana Machine Learning forecast | `flights:positions:seasonal_*`, ML alert |

Start with volume: it is the cheapest check and catches the most incidents. Then move from
simple baselines to seasonal and ML forecasts as history accumulates.
""")

notebook = {
    "apiVersion": "dashboard.grafana.app/v2beta1",
    "kind": "Notebook",
    "metadata": {"name": "detect-data-volume-drops"},
    "spec": {
        "title": "Detect data volume drops · Tunisia flights",
        "description": "Detecting data volume drops with baselines, forecasts and Grafana Machine Learning, on live flight data.",
        "tags": ["data-observability", "volume", "forecasting", "machine-learning"],
        "timeSettings": {"from": "now-24h", "to": "now", "autoRefresh": "", "autoRefreshIntervals": [],
                         "hideTimepicker": False, "fiscalYearStartMonth": 0},
        "elements": elements,
        "layout": {"kind": "NotebookLayout", "spec": {"cells": layout}},
    },
}

OUT.parent.mkdir(exist_ok=True)
OUT.write_text(json.dumps(notebook, indent=2, ensure_ascii=False) + "\n")
print(f"wrote {len(layout)} cells to {OUT}")
