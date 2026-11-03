"""Generate grafana/dashboards/tunisia-flights.json.

Usage: python scripts/build_dashboard.py   (then: docker compose restart grafana)
"""
import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "grafana" / "dashboards" / "tunisia-flights.json"
# A data source variable lets the same JSON work self-hosted and on Grafana Cloud
DS = {"type": "prometheus", "uid": "${datasource}"}
G, O, R = "green", "orange", "red"
STAT = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "area", "textMode": "value"}

panels = []
y = 0


def steps(*pairs):
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in pairs]}


def q(expr, legend="", instant=False, fmt=None):
    d = {"expr": expr, "legendFormat": legend or "__auto"}
    if instant:
        d.update(instant=True, range=False)
    if fmt:
        d["format"] = fmt
    return d


def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": len(panels) + 1, "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1


def panel(kind, title, targets, x, w, h, unit=None, desc="", options=None, custom=None, thresholds=None,
          overrides=None, transformations=None, defaults=None):
    fd = {"defaults": dict(defaults or {}), "overrides": overrides or []}
    if unit:
        fd["defaults"]["unit"] = unit
    if custom:
        fd["defaults"]["custom"] = custom
    if thresholds:
        fd["defaults"]["thresholds"] = thresholds
    p = {"type": kind, "title": title, "description": desc, "id": len(panels) + 1, "datasource": DS,
         "gridPos": {"h": h, "w": w, "x": x, "y": y}, "fieldConfig": fd,
         "targets": [dict(datasource=DS, refId=chr(65 + i), **t) for i, t in enumerate(targets)]}
    if options:
        p["options"] = options
    if transformations:
        p["transformations"] = transformations
    panels.append(p)


def text(title, content, x, w, h):
    panels.append({"type": "text", "title": title, "id": len(panels) + 1,
                   "gridPos": {"h": h, "w": w, "x": x, "y": y},
                   "options": {"mode": "markdown", "content": content}})


def by_name(name, *props):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": k, "value": v} for k, v in props]}


# --- Live flights: one instant query per attribute, merged into one row per flight ---------
FLIGHT_BY = "flight, airline, direction, origin, destination, aircraft, registration"
FILTER = '{airline=~"$airline", direction=~"$direction"}'
ATTRIBUTES = [  # (metric suffix, column name)
    ("latitude", "latitude"), ("longitude", "longitude"), ("heading_degrees", "Heading"),
    ("phase", "Status"), ("altitude_feet", "Altitude"), ("speed_kmh", "Speed"),
    ("distance_left_km", "Distance left"), ("eta_timestamp_ms", "Arrival (est.)"),
    ("tracked_since_timestamp_ms", "Tracked since"), ("last_update_timestamp_ms", "Last update"),
]
FLIGHT_QUERIES = [q(f"max by ({FLIGHT_BY}) (flight_{m}{FILTER})", instant=True, fmt="table") for m, _ in ATTRIBUTES]
COLUMNS = ["Flight", "Airline", "Direction", "From", "To", "Status", "Aircraft", "Altitude", "Speed",
           "Distance left", "Arrival (est.)", "Tracked since", "Last update", "Registration"]
rename = {f"Value #{chr(65 + i)}": col for i, (_, col) in enumerate(ATTRIBUTES)}
rename.update({"flight": "Flight", "airline": "Airline", "direction": "Direction", "origin": "From", "destination": "To", "aircraft": "Aircraft",
               "registration": "Registration"})
FLIGHT_TRANSFORMS = [
    {"id": "merge", "options": {}},
    {"id": "organize", "options": {
        "excludeByName": {"Time": True},
        "renameByName": rename,
        "indexByName": {**{k: COLUMNS.index(v) for k, v in rename.items() if v in COLUMNS}},
    }},
]
ALTITUDE_COLORS = steps((None, "#8AB8FF"), (10000, "#5794F2"), (25000, "#1F60C4"), (33000, "#FF9830"))
PHASES = {"0": ("On ground", "text"), "1": ("Climbing ↑", "green"), "2": ("Cruising", "blue"), "3": ("Descending ↓", "orange")}
DIRECTIONS = {"Arrival": ("Arrival ↘", "green"), "Departure": ("Departure ↗", "blue"),
              "Domestic": ("Domestic", "purple"), "Via Tunisia": ("Via Tunisia", "text"), "Unknown": ("Unknown", "orange")}
FLIGHT_OVERRIDES = [
    by_name("Direction", ("mappings", [{"type": "value", "options": {k: {"text": t, "color": c, "index": i}
                                                                       for i, (k, (t, c)) in enumerate(DIRECTIONS.items())}}]),
            ("custom.cellOptions", {"type": "color-text"})),
    by_name("Status", ("mappings", [{"type": "value", "options": {k: {"text": t, "color": c, "index": i}
                                                                    for i, (k, (t, c)) in enumerate(PHASES.items())}}]),
            ("custom.cellOptions", {"type": "color-text"})),
    by_name("Altitude", ("unit", "lengthft"), ("decimals", 0), ("thresholds", ALTITUDE_COLORS),
            ("custom.cellOptions", {"type": "color-text"})),
    by_name("Speed", ("unit", "velocitykmh"), ("decimals", 0)),
    by_name("Distance left", ("unit", "suffix: km"), ("decimals", 0)),
    by_name("Arrival (est.)", ("unit", "time:HH:mm")),
    by_name("Tracked since", ("unit", "time:HH:mm")),
    by_name("Last update", ("unit", "dateTimeFromNow")),
    by_name("latitude", ("custom.hidden", True)),
    by_name("longitude", ("custom.hidden", True)),
    by_name("Heading", ("custom.hidden", True)),
    by_name("Registration", ("custom.hidden", True)),
] + [by_name(col, ("custom.width", w)) for col, w in [
    ("Flight", 80), ("Airline", 130), ("Direction", 105), ("Status", 110), ("Aircraft", 135), ("Altitude", 95), ("Speed", 95),
    ("Distance left", 105), ("Arrival (est.)", 110), ("Tracked since", 110), ("Last update", 120)]]

text("How to read this dashboard", """
**What you see:** every plane flying **to, from or within Tunisia** right now (Tunisair, Nouvelair, Transavia,
easyJet, TUI, Air France and 120+ other airlines), updated every minute. Use the **Airline** and **Direction**
filters at the top to focus, e.g. on Nouvelair arrivals only.

**Where the data comes from:** aircraft broadcast their position by radio (called **ADS-B**).
Thousands of volunteers run small receivers that pick up these signals and share them through
**adsb.lol**, a free public flight-tracking network. Our pipeline asks adsb.lol every 60 seconds
for planes over Tunisia, the Mediterranean and Europe, keeps the ones whose flight number is a known
Tunisia route (plus every Tunisair, Nouvelair and Tunisair Express plane), adds the route
(e.g. *Paris → Tunis*), sends them through **Kafka** and checks their quality.

**The rows below** answer five questions about the data: *Is enough arriving? Is it recent?
Is its format correct? Do the values look normal? Does everything make it through the pipeline?*
Times are shown in your browser's time zone. *Estimated arrival* is based on the straight-line
distance and current speed, so treat it as an approximation.
""", 0, 24, 8)
y += 8

row("Live · Flights to and from Tunisia right now")
panel("geomap", "Map", FLIGHT_QUERIES, 0, 24, 13,
      desc="Each plane icon is a flight to or from Tunisia seen in the last 10 minutes. It points in its direction of travel; colour shows altitude. Hover for details.",
      transformations=FLIGHT_TRANSFORMS, thresholds=ALTITUDE_COLORS, defaults={"color": {"mode": "thresholds"}},
      overrides=FLIGHT_OVERRIDES[:7],
      options={
          "view": {"id": "coords", "lat": 43, "lon": 7, "zoom": 4},
          "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True},
          "basemap": {"type": "default", "name": "Basemap", "config": {}},
          "layers": [{
              "type": "markers", "name": "Flights", "tooltip": True,
              "location": {"mode": "coords", "latitude": "latitude", "longitude": "longitude"},
              "config": {"showLegend": False, "style": {
                  "size": {"fixed": 16},
                  "color": {"field": "Altitude", "fixed": "blue"},
                  "opacity": 1,
                  "symbol": {"mode": "fixed", "fixed": "img/icons/marker/plane.svg"},
                  "symbolAlign": {"horizontal": "center", "vertical": "center"},
                  "rotation": {"field": "Heading", "fixed": 0, "min": -360, "max": 360, "mode": "mod"},
                  "text": {"mode": "field", "field": "Flight", "fixed": ""},
                  "textConfig": {"fontSize": 11, "offsetX": 0, "offsetY": -16, "textAlign": "center", "textBaseline": "middle"},
              }}}],
          "tooltip": {"mode": "details"},
      })
y += 13
panel("table", "Flights", FLIGHT_QUERIES, 0, 24, 13, transformations=FLIGHT_TRANSFORMS, overrides=FLIGHT_OVERRIDES,
      desc="One row per flight. 'Tracked since' is when our pipeline first saw it (not the scheduled departure).",
      options={"showHeader": True, "sortBy": [{"displayName": "Arrival (est.)"}], "cellHeight": "sm"})
y += 13

row("Arrivals at Tunisian airports")
ARRIVAL_FILTER = '{airline=~"$airline", direction=~"Arrival|Domestic"}'
ARRIVAL_QUERIES = [q(f"max by ({FLIGHT_BY}) (flight_{m}{ARRIVAL_FILTER})", instant=True, fmt="table")
                   for m, _ in ATTRIBUTES]
panel("bargauge", "Expected in the next hour", [q('max by (airport) (airport_expected_arrivals{within="1h"}) > 0',
                                                  "{{airport}}", instant=True)],
      0, 6, 9, "short", desc="Tracked flights whose estimated arrival is within 1 hour. Airports with none are hidden.",
      options={"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}}, defaults={"decimals": 0, "min": 0, "noValue": "None expected"},
      thresholds=steps((None, "blue")))
panel("bargauge", "Expected in the next 2 hours", [q('max by (airport) (airport_expected_arrivals{within="2h"}) > 0',
                                                     "{{airport}}", instant=True)],
      6, 6, 9, "short", options={"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                                 "reduceOptions": {"calcs": ["lastNotNull"]}}, defaults={"decimals": 0, "min": 0, "noValue": "None expected"},
      thresholds=steps((None, "purple")))
panel("table", "Next arrivals", ARRIVAL_QUERIES, 12, 12, 9, transformations=FLIGHT_TRANSFORMS,
      overrides=FLIGHT_OVERRIDES + [by_name(c, ("custom.hidden", True))
                                    for c in ("Airline", "Direction", "Status", "Aircraft", "Speed", "Tracked since")],
      desc="Flights to Tunisian airports, soonest first. Estimated arrival = distance left ÷ current speed.",
      options={"showHeader": True, "sortBy": [{"displayName": "Arrival (est.)"}], "cellHeight": "sm"})
y += 9

row("Data health at a glance")
panel("stat", "Flights tracked now", [q("flights_active")], 0, 4, 5, "short", options=STAT, thresholds=steps((None, "blue")))
panel("stat", "Positions received / min", [q("flights:messages:per_minute")], 4, 4, 5, "short", options=STAT, thresholds=steps((None, "blue")))
panel("stat", "Data age (95% under)", [q("flights:position_age:p95")], 8, 4, 5, "s", options=STAT,
      desc="How old a plane position is when we process it. 95% of positions are newer than this.",
      thresholds=steps((None, G), (120, O), (180, R)))
panel("stat", "Rejected messages", [q("flights:invalid:ratio")], 12, 4, 5, "percentunit", options=STAT,
      thresholds=steps((None, G), (0.02, O), (0.05, R)))
panel("stat", "Messages delivered", [q("flights:completeness:ratio")], 16, 4, 5, "percentunit", options=STAT,
      desc="Messages received by the quality checker / messages sent to Kafka.",
      thresholds=steps((None, R), (0.95, O), (0.99, G)))
panel("stat", "Active alerts", [q('count(ALERTS{alertstate="firing", team="data-engineering"}) or vector(0)', instant=True)],
      20, 4, 5, "short", options=STAT, thresholds=steps((None, G), (1, R)))
y += 5

row("1 · Volume: are we receiving as much data as usual?")
panel("timeseries", "Flights tracked: today vs. yesterday", [
    q("flights_active", "today"), q("flights:active:yesterday", "same time yesterday")], 0, 12, 8, "short",
    desc="Tunisian air traffic has a daily rhythm (quiet at night, busy in the morning). Yesterday is the baseline; a big gap means a problem.",
    overrides=[by_name("same time yesterday", ("custom.lineStyle", {"fill": "dash", "dash": [10, 10]}),
                       ("color", {"mode": "fixed", "fixedColor": "text"}))])
panel("timeseries", "Planes seen by the tracking network", [
    q("adsb_aircraft_in_areas", "all aircraft"), q("producer_tunisia_aircraft", "Tunisia flights")], 12, 12, 8, "short",
    desc="All aircraft adsb.lol reports over our areas, and how many are flying to/from Tunisia. If 'all aircraft' drops too, the problem is the source, not Tunisian traffic.",
    overrides=[by_name("Tunisia flights", ("custom.axisPlacement", "right"))])
y += 8

row("2 · Freshness: how recent is the data?")
panel("timeseries", "Data age when processed", [q("flights:position_age:p50", "typical (median)"),
                                                q("flights:position_age:p95", "worst 5%")], 0, 8, 8, "s",
      desc="Time between the plane sending its position and our pipeline processing it. Mostly the 60 s polling interval.")
panel("timeseries", "Where the time goes (95th percentile)", [q("flights:stage_latency:p95", "{{stage}}")], 8, 8, 8, "s",
      desc="source_to_api: radio receiver → adsb.lol · api_to_kafka: our producer · kafka_to_consumer: Kafka → quality checker.",
      custom={"stacking": {"mode": "normal"}, "fillOpacity": 40})
panel("timeseries", "Time since last data pull", [q("time() - (producer_last_poll_timestamp_seconds > 0)", "seconds")], 16, 8, 8, "s",
      desc="Should stay around 60 s. Above 5 minutes the 'PipelineStalled' alert fires.",
      thresholds=steps((None, G), (300, R)), custom={"thresholdsStyle": {"mode": "line+area"}})
y += 8

row("3 · Schema: is the data in the expected format?")
panel("timeseries", "Rejected messages by reason", [q("sum by (reason) (increase(flights_invalid_messages_total[10m]))", "{{reason}}")],
      0, 8, 8, "short", desc="e.g. missing_position: the plane was heard but without coordinates.",
      custom={"drawStyle": "bars", "fillOpacity": 70, "stacking": {"mode": "normal"}})
panel("timeseries", "Fields per plane record (min / max)", [
    q('flights_raw_field_count{stat="min"}', "fewest fields"), q('flights_raw_field_count{stat="max"}', "most fields")], 8, 8, 8, "short",
    desc="Different transponders send different fields, so some variation is normal. A sudden jump means adsb.lol changed its format.",
    custom={"lineInterpolation": "stepAfter"})
panel("timeseries", "Flights with unknown route", [q("flights:unknown_route:ratio", "unknown route")], 16, 8, 8, "percentunit",
      desc="Flight numbers missing from our route list (data/routes.csv). If this grows, refresh it with 'make routes'.")
y += 8

row("4 · Distribution: do the values look normal?")
BAR = {"orientation": "horizontal", "displayMode": "gradient", "reduceOptions": {"calcs": ["lastNotNull"]},
       "showUnfilled": True}
DONUT = {"pieType": "donut", "reduceOptions": {"calcs": ["lastNotNull"]},
         "legend": {"displayMode": "table", "placement": "right", "values": ["value"]}}
panel("bargauge", "Top 10 airlines in the air", [q("topk(10, flights_active_by_airline)", "{{airline}}", instant=True)],
      0, 8, 10, "short", options=BAR, desc="If one airline suddenly disappears, check its flight numbers in the route list.")
panel("bargauge", "Countries connected to Tunisia right now", [q("topk(10, flights_active_by_country)", "{{country}}", instant=True)],
      8, 8, 10, "short", options=BAR, desc="For arrivals: the country of departure. For departures: the destination country.")
panel("piechart", "Arrivals vs. departures", [q("flights_active_by_direction", "{{direction}}", instant=True)],
      16, 8, 5, "short", options=DONUT,
      overrides=[by_name(k, ("color", {"mode": "fixed", "fixedColor": c})) for k, (_, c) in DIRECTIONS.items()])
y += 5
panel("piechart", "Aircraft models in the air", [q("topk(8, flights_active_by_type)", "{{aircraft}}", instant=True)],
      16, 8, 5, "short", options=DONUT)
y += 5
panel("bargauge", "Altitude of positions (last 24h)", [q("sum by (le) (increase(flights_altitude_feet_bucket[24h]))", "{{le}}", instant=True, fmt="heatmap")],
      0, 24, 7, "short", desc="Most positions should be at cruising altitude (30,000–40,000 ft).",
      options={"orientation": "vertical", "displayMode": "basic", "reduceOptions": {"calcs": ["lastNotNull"]}})
y += 7

row("5 · Lineage: where does the data come from, and does it all arrive?")
FLOW = [("① Planes seen by adsb.lol", "sum(sum_over_time(adsb_aircraft_in_areas[1h:1m]))"),
        ("② Tunisia flights kept", "sum(sum_over_time(producer_tunisia_aircraft[1h:1m]))"),
        ("③ Sent to Kafka", "sum(increase(producer_messages_sent_total[1h]))"),
        ("④ Received by quality checker", "sum(increase(flights_messages_total[1h]))"),
        ("⑤ Passed all checks", "sum(increase(flights_valid_positions_total[1h]))")]
for i, (title, expr) in enumerate(FLOW):
    panel("stat", title, [q(expr, instant=True)], i * 4, 4, 5, "short", desc="Last hour",
          options={**STAT, "graphMode": "none", "colorMode": "value"}, thresholds=steps((None, "blue")))
text("Data sources", "**Positions:** [adsb.lol](https://adsb.lol) (ODbL)  \n**Routes:** VRS standing data (CC0)  \n**Airports:** OurAirports (public domain)",
     20, 4, 5)
y += 5
panel("timeseries", "Requests to adsb.lol by area", [q("sum by (area, status) (increase(adsb_api_requests_total[10m]))", "{{area}} · {{status}}")],
      0, 12, 8, "short", desc="Status 200 = OK. Anything else (or 'error') means adsb.lol did not answer correctly.",
      custom={"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 60})
panel("timeseries", "adsb.lol response time (95th percentile)", [
    q("histogram_quantile(0.95, sum by (le, area) (rate(adsb_api_request_seconds_bucket[10m])))", "{{area}}")], 12, 12, 8, "s")
y += 8

VARIABLES = [
    {"name": "datasource", "label": "Data source", "type": "datasource", "query": "prometheus", "hide": 0,
     "current": {}, "refresh": 1, "includeAll": False, "multi": False,
     # local data source is "Prometheus"; on Grafana Cloud it is "grafanacloud-<stack>-prom"
     "regex": "/^(Prometheus|grafanacloud-.+-prom)$/"},
] + [
    {"name": name, "label": label, "type": "query", "datasource": DS, "query": {"query": query, "refId": "var"},
     "definition": query, "refresh": 2, "includeAll": True, "multi": True, "allValue": ".*", "sort": 1,
     "current": {"selected": True, "text": ["All"], "value": ["$__all"]}}
    for name, label, query in [("airline", "Airline", "label_values(flight_phase, airline)"),
                               ("direction", "Direction", "label_values(flight_phase, direction)")]]

dashboard = {"uid": "tunisia-flights", "title": "Tunisia Flights · Data Observability",
             "tags": ["data-observability", "kafka", "flights", "tunisia"], "templating": {"list": VARIABLES},
             "timezone": "browser", "refresh": "1m", "time": {"from": "now-6h", "to": "now"}, "schemaVersion": 41,
             "editable": True, "graphTooltip": 1, "panels": panels}
OUT.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
print(f"wrote {len(panels)} panels to {OUT}")
