# Tunisia Flights Data Observability

Live tracking of every flight **to, from and within Tunisia** (Tunisair, Nouvelair, Transavia,
easyJet, TUI, Air France and 120+ other airlines), streamed through **Kafka** and
monitored with **Grafana** using the five pillars of data observability: **volume,
freshness, schema, distribution and lineage**.

All data is real: aircraft positions come from public ADS-B receivers through the free
[adsb.lol](https://adsb.lol) API, and routes come from open aviation datasets.

```mermaid
flowchart LR
  A[adsb.lol<br/>live ADS-B API] -->|poll every 60 s| P[producer<br/>keep Tunisia flights · add route]
  R[(data/routes.csv<br/>3,306 Tunisia routes<br/>126 airlines)] --> P
  P -->|tunisia.flights| K[(Kafka)]
  K --> C[consumer<br/>quality checks]
  P -. metrics .-> M[Prometheus]
  C -. metrics .-> M
  M --> G[Grafana<br/>map · five pillars · alerts]
```

| Service | What it does |
|---|---|
| `producer` | Every 60 s, queries 10 areas (Tunisia, Libya & Malta, Algeria, Mediterranean, France, Benelux, UK, Germany, Poland). Keeps aircraft whose flight number is a known Tunisia route, plus every Tunisair / Nouvelair / Tunisair Express plane. Adds airline, direction and route (e.g. `Paris → Tunis`), then publishes one message per aircraft |
| `kafka` | Single-node Kafka 3.9 (KRaft) with the topic `tunisia.flights` |
| `consumer` | Validates every message and exposes the data-quality metrics and live flight positions |
| `prometheus` | Stores metrics; recording and alerting rules are in [`prometheus/`](prometheus/) |
| `grafana` | Provisioned **Tunisia Flights · Data Observability** dashboard with Notebooks enabled |
| `kafka-ui` | Browse the topic and read the messages |

## Quick start

Requirements: Docker with Compose v2, and an internet connection.

```bash
git clone https://github.com/imenbkr/data_observability.git
cd data_observability
cp .env.example .env        # COMPOSE_PROFILES=local runs Prometheus + Grafana in Docker
docker compose up -d --build
```

| UI | URL |
|---|---|
| Grafana dashboard | http://localhost:3000 |
| Kafka UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |

The first flights show up on the map after about 1–2 minutes. Run `make help` for shortcuts
such as `make flights` (flights found in the latest polls) or `make topic` (read raw Kafka messages).

> Most Tunisia flights operate during the day, so expect few flights at night (Tunisia is UTC+1).

## Run with Grafana Cloud instead

Kafka, the producer and the consumer stay in Docker. A [Grafana Alloy](https://grafana.com/docs/alloy/)
container replaces local Prometheus and Grafana and sends the metrics to your Grafana Cloud stack.

1. In your stack, open **Prometheus → Details** and copy the remote write URL and the instance ID.
   Create an access-policy token with `metrics:write`, `rules:read` and `rules:write`.
2. In `.env`, set `COMPOSE_PROFILES=cloud`, `GRAFANA_CLOUD_PROM_URL`, `GRAFANA_CLOUD_PROM_USER`
   and `GRAFANA_CLOUD_API_TOKEN` (see [`.env.example`](.env.example)), then run `docker compose up -d`.
3. Upload the recording and alerting rules to the stack:

   ```bash
   set -a && . ./.env && set +a && docker run --rm -v "$PWD/prometheus:/rules:ro" grafana/mimirtool:2.14.0 \
     rules load /rules/rules.yml /rules/alerts.yml \
     --address="${GRAFANA_CLOUD_PROM_URL%/api/prom/push}" --id="$GRAFANA_CLOUD_PROM_USER" --key="$GRAFANA_CLOUD_API_TOKEN"
   ```

4. Deploy the dashboard as code with Terraform ([`terraform/main.tf`](terraform/main.tf)). Create a
   service account with the **Admin** role in your stack's Grafana, add `GRAFANA_CLOUD_URL` and
   `GRAFANA_CLOUD_SA_TOKEN` to `.env`, then review and apply the plan:

   ```bash
   set -a && . ./.env && set +a
   TF="docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work/terraform -e HOME=/tmp \
     -e TF_VAR_grafana_url=$GRAFANA_CLOUD_URL -e TF_VAR_grafana_token=$GRAFANA_CLOUD_SA_TOKEN \
     hashicorp/terraform:1.9.8"
   $TF init && $TF plan -out=tfplan && $TF apply tfplan
   ```

   After changing the dashboard (`python3 scripts/build_dashboard.py`), run `plan` and `apply` again.
   Pick `grafanacloud-…-prom` in the dashboard's **Data source** dropdown if it isn't selected.

## The dashboard

- **How to read this dashboard:** a plain-language intro to the data and where it comes from.
- **Filters:** **Airline** and **Direction** (arrival / departure / domestic) dropdowns at the top,
  e.g. to show only Nouvelair arrivals.
- **Map:** every plane flying to or from Tunisia, pointing in its direction of travel and coloured by altitude.
- **Flights table:** flight, airline, direction, from → to, status (climbing / cruising / descending), aircraft,
  altitude, speed, distance left, **estimated arrival**, **tracked since**, **last update**.
- **Five rows, one per pillar:** each one answers a single question about the data.

ADS-B only carries position, not schedules. So *tracked since* is when the pipeline first
saw the flight, and *estimated arrival* is calculated from the straight-line distance and the
current speed.

## The data

A message on the `tunisia.flights` topic:

```json
{
  "hex": "02a1b0", "callsign": "LBT719", "airline_code": "LBT", "airline": "Nouvelair",
  "direction": "Arrival", "registration": "TS-INQ", "aircraft_type": "A320",
  "lat": 39.12, "lon": 9.29, "altitude_ft": 36025, "ground_speed_kt": 441.4, "track_deg": 143.8,
  "origin": "LFBD", "origin_city": "Bordeaux", "origin_country": "France",
  "destination": "DTTA", "destination_city": "Tunis", "destination_country": "Tunisia",
  "position_ts": 1793722012.4, "fetched_at": 1793722013.1,
  "source": "adsb.lol", "source_area": "Central Mediterranean", "raw_field_count": 47
}
```

| Data | Source | License |
|---|---|---|
| Live positions | [adsb.lol API](https://www.adsb.lol/docs/open-data/api/) (community ADS-B network) | [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/) |
| Flight number → route, airline names | [VRS standing data](https://github.com/vradarserver/standing-data) | CC0 |
| Airports (city, country) | [OurAirports](https://ourairports.com/data/) | Public domain |

**Polling rate:** the producer polls every 60 s, spreading its 10 requests evenly over the
minute (one every 6 s) and backing off when adsb.lol answers `429 Too Many Requests`. A cruising
airliner moves about 14 km per minute, which is plenty for a live map and for data-quality
checks, and stays gentle on a free community API. Don't go below about 15 s (`POLL_SECONDS`).

To refresh the routes, run `make routes` (it calls `scripts/build_routes.py`). To track
another country, change `TUNISIA = "DT"` in the script to its ICAO airport prefix (e.g. `GM` for
Morocco), and update `AREAS` in the producer.

## The five pillars

Real data is messy, so the checks catch real problems and nothing is simulated.

| Pillar | Question | What's measured | Alert |
|---|---|---|---|
| **Volume** | Is the expected amount of data arriving? | Active flights vs. the same time yesterday; aircraft returned by the API | `NoFlightsWhileExpected`, `SourceApiFailing` |
| **Freshness** | How old is the data when it lands? | Position age (aircraft broadcast → consumer), latency per pipeline stage, time since the last poll | `PipelineStalled`, `PositionsStale` |
| **Schema** | Has the structure changed? | Rejected messages by reason (`missing_position`, `bad_altitude`, …), ADS-B fields per record, unknown routes | `InvalidMessages`, `RouteReferenceDataOutdated` |
| **Distribution** | Do the values look normal? | Flights per airline, per connected country, arrivals vs. departures, aircraft models, altitude | — |
| **Lineage** | Where does the data come from, and does all of it arrive? | Counts at each stage (API → filter → Kafka → consumer → valid), API status per area | `MessagesLostInPipeline` |

Two real quirks this pipeline handles:

- **Missing fields:** ADS-B transponders report different sets of fields, so the field count per record varies.
- **Ground altitude:** `alt_baro` is the string `"ground"` instead of a number when an aircraft is on the ground.

Every alert has `pillar` and `team=data-engineering` labels, ready for notification routing.

## Notebook: detect data volume drops

[`grafana/notebooks/detect-data-volume-drops.json`](grafana/notebooks/detect-data-volume-drops.json)
is a step-by-step Grafana Notebook on detecting data volume drops in this pipeline's live data. It
mixes explanations, PromQL / YAML / HCL cells and live panels:

1. **High ingestion frequency:** the adsb.lol stream compared with the same time yesterday, plus a
   missing-data check (`SourceVolumeBelowBaseline`, `SourceDataMissing`).
2. **Low ingestion frequency:** the hourly count of flight positions compared with a one-step-ahead
   forecast. That's the mean of the last 72 hourly samples ± 1.645 sd (90% coverage), recorded by the
   `volume-forecast` rule group and alerted on by `FlightVolumeBelowForecast`.
3. **Better forecasts with Grafana:**
   - **A. Seasonal baseline:** the same hour on the last 7 days (`volume-forecast-seasonal`).
   - **B. Short-term trend:** `predict_linear`.
   - **C. Grafana Machine Learning:** a metric forecast (`grafana_machine_learning_job`) with an ML alert,
     both managed in Terraform (Grafana Cloud only).

Some panels need history before they show anything:

| Panel | Needs |
|---|---|
| 72-hour forecast band, headroom | > 1 hour (meaningful after ~3 hours) |
| "Yesterday" lines, seasonal baseline | 1 day (seasonal band: 2 days) |
| Grafana ML forecast | 100 training points (~3.5 hours), then retrains every 24 hours |

A Markdown export of the notebook is in [`docs/detect-data-volume-drops.md`](docs/detect-data-volume-drops.md)
(regenerated by `make notebook`).

Screenshots of the live map, the flights table, the Kafka topic and the alert rules are embedded in
the notebook as images (`grafana/notebooks/images/`). Refresh them from the running stack with
`make screenshots`, which needs Google Chrome or Chromium and Pillow.

Notebooks need the `dashboard.notebooks` feature toggle (on in the local Grafana; on Grafana Cloud,
ask Grafana support to enable it). A notebook is a `Notebook` resource in the
`dashboard.grafana.app/v2beta1` API, so it is deployed as code:

```bash
make notebook   # build the JSON and create/update it in local Grafana → http://localhost:3000/notebooks/detect-data-volume-drops
```

On Grafana Cloud, Terraform deploys it with `grafana_apps_generic_resource` once the feature is
enabled: add `-e TF_VAR_enable_notebook=true` to the Terraform commands above.

Until then, the same content is deployed as a regular dashboard in the **Forecasting** folder
([`grafana/dashboards/volume-forecasting.json`](grafana/dashboards/volume-forecasting.json), generated
from the notebook by `scripts/build_forecast_dashboard.py`; `make notebook` rebuilds both).

## Project layout

```
.
├── docker-compose.yml
├── Makefile
├── data/routes.csv              # flight number → route for every Tunisia flight
├── scripts/build_routes.py      # rebuilds routes.csv from public sources
├── scripts/build_dashboard.py   # generates the Grafana dashboard JSON
├── producer/producer.py         # adsb.lol → Kafka
├── consumer/consumer.py         # Kafka → quality checks → metrics
├── prometheus/                  # scrape config, recording rules, alerts
├── alloy/config.alloy           # ships metrics to Grafana Cloud (profile "cloud")
├── terraform/main.tf            # dashboard, ML forecast + alert (and notebook) on Grafana Cloud as code
├── grafana/notebooks/           # "Detect data volume drops" notebook (scripts/build_notebook.py)
├── docs/                        # the notebook exported as Markdown
└── grafana/                     # data source, dashboard provisioning, dashboard JSON
```

## Ideas to extend it

- Store every position in a data lake (S3/MinIO + Parquet) for history and replay.
- Add a Schema Registry (Avro) between the producer and the consumer.
- Add the whole adsb.lol stream (every aircraft, ~1.4 million messages a day) as a second topic.

## Acknowledgements

Based on the article *Data Observability with Grafana: The Five Pillars in Practice* by
Imen Bakir. Flight data © adsb.lol contributors (ODbL).

## License

Code: [MIT](LICENSE). Data under `data/` is derived from CC0 and public-domain sources.
