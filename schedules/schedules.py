"""Fetches scheduled arrivals at Tunisian airports from AeroDataBox and publishes them to Kafka.

AeroDataBox's free plan (400 units/month) only has scheduled times for Tunisian airports, not
actual ones. The consumer joins these schedules with the landings it detects from ADS-B to
compute arrival delays.

Budget: one 12-hour arrivals call costs 2 units. Each airport is fetched once per 12-hour window
(2 calls a day), so 3 airports use 360 units a month. Fetched windows are remembered in
STATE_FILE so restarts don't spend units twice, and fetching stops below MIN_UNITS_LEFT.
"""
import json
import os
import pathlib
import time
from datetime import datetime, timedelta, timezone

import requests
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

KAFKA = os.getenv("KAFKA", "kafka:9092")
TOPIC = os.getenv("TOPIC", "tunisia.schedules")
API_KEY = os.getenv("RAPIDAPI_KEY", "")
AIRPORTS = os.getenv("SCHEDULE_AIRPORTS", "DTTA,DTNH,DTMB").split(",")
WINDOW_HOURS = 12  # longest window the free plan allows per call
TUNISIA_UTC_OFFSET = timedelta(hours=1)  # the API takes local airport time; Tunisia is UTC+1, no DST
MIN_UNITS_LEFT = int(os.getenv("MIN_UNITS_LEFT", "20"))
STATE_FILE = pathlib.Path(os.getenv("STATE_FILE", "/data/state.json"))
API = "https://aerodatabox.p.rapidapi.com/flights/airports/icao/{airport}/{start}/{end}"

requests_total = Counter("schedules_api_requests_total", "Requests to AeroDataBox", ["airport", "status"])
units_left = Gauge("schedules_api_units_remaining", "AeroDataBox units left this month")
published = Counter("schedules_flights_published_total", "Scheduled arrivals sent to Kafka", ["airport"])
last_fetch = Gauge("schedules_last_fetch_timestamp_seconds", "Unix time of the last successful fetch", ["airport"])


def load_state():
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return {"fetched": {}, "units_left": None}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=1))


def window_start(now):
    """Start of the current 12-hour window (00:00 or 12:00 UTC)."""
    return now.replace(hour=0 if now.hour < WINDOW_HOURS else WINDOW_HOURS, minute=0, second=0, microsecond=0)


def callsign(flight):
    """'TU 0719' + airline ICAO 'TAR' -> 'TAR719' (the ADS-B callsign)."""
    icao = (flight.get("airline") or {}).get("icao")
    number = (flight.get("number") or "").split(" ")[-1].lstrip("0")
    return f"{icao}{number}" if icao and number else None


def parse_utc(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%MZ").replace(tzinfo=timezone.utc).timestamp()


def fetch(session, airport, start, end):
    local = lambda t: (t + TUNISIA_UTC_OFFSET).strftime("%Y-%m-%dT%H:%M")
    url = API.format(airport=airport, start=local(start), end=local(end))
    resp = session.get(url, params={"direction": "Arrival", "withCodeshared": "false", "withCargo": "false",
                                    "withPrivate": "false", "withLocation": "false", "withLeg": "false",
                                    "withCancelled": "true"}, timeout=40)
    requests_total.labels(airport, str(resp.status_code)).inc()
    remaining = resp.headers.get("x-ratelimit-api-units-remaining")
    if remaining is not None:
        units_left.set(int(remaining))
    resp.raise_for_status()
    return (resp.json() if resp.content else {}).get("arrivals", []), int(remaining) if remaining else None


def publish(producer, airport, flights):
    count = 0
    for f in flights:
        cs = callsign(f)
        scheduled = ((f.get("movement") or {}).get("scheduledTime") or {}).get("utc")
        if not cs or not scheduled:
            continue
        msg = {
            "airport": airport,
            "callsign": cs,
            "flight": f.get("number"),
            "airline": (f.get("airline") or {}).get("name"),
            "origin": ((f.get("movement") or {}).get("airport") or {}).get("name"),
            "scheduled_ts": parse_utc(scheduled),
            "status": f.get("status"),
            "source": "aerodatabox",
            "fetched_at": time.time(),
        }
        producer.produce(TOPIC, json.dumps(msg).encode(), key=cs)
        count += 1
    producer.flush(10)
    published.labels(airport).inc(count)
    return count


def main():
    start_http_server(8002)
    if not API_KEY:
        raise SystemExit("RAPIDAPI_KEY is not set")
    state = load_state()
    if state.get("units_left") is not None:
        units_left.set(state["units_left"])
    producer = Producer({"bootstrap.servers": KAFKA, "acks": "all"})
    session = requests.Session()
    session.headers.update({"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"})
    print(f"fetching scheduled arrivals for {', '.join(AIRPORTS)} every {WINDOW_HOURS} h", flush=True)

    while True:
        now = datetime.now(timezone.utc)
        start = window_start(now)
        key = start.strftime("%Y-%m-%dT%H")
        for airport in AIRPORTS:
            if key in state["fetched"].get(airport, []):
                continue
            if state.get("units_left") is not None and state["units_left"] < MIN_UNITS_LEFT:
                print(f"budget guard: {state['units_left']} units left, skipping {airport}", flush=True)
                continue
            try:
                flights, remaining = fetch(session, airport, start, start + timedelta(hours=WINDOW_HOURS))
            except requests.RequestException as e:
                print(f"{airport}: {e}", flush=True)
                continue
            count = publish(producer, airport, flights)
            state["fetched"].setdefault(airport, []).append(key)
            state["fetched"][airport] = state["fetched"][airport][-10:]
            state["units_left"] = remaining
            save_state(state)
            last_fetch.labels(airport).set(time.time())
            print(f"{airport} {key}: {count} scheduled arrivals, {remaining} units left", flush=True)
            time.sleep(2)  # free plan: 1 request per second
        time.sleep(300)


if __name__ == "__main__":
    main()
