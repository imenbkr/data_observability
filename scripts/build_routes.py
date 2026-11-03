"""Build data/routes.csv: every known flight (callsign) to, from or within Tunisia, any airline.

Sources:
  - Routes:   VRS standing data (CC0), mirrored by adsb.lol  https://github.com/vradarserver/standing-data
  - Airlines: VRS standing data (CC0)
  - Airports: OurAirports (public domain)                     https://ourairports.com/data/

Usage: python scripts/build_routes.py
"""
import csv
import gzip
import io
import pathlib
import urllib.request

ROUTES_URL = "https://vrs-standing-data.adsb.lol/routes.csv.gz"
AIRLINES_URL = "https://raw.githubusercontent.com/vradarserver/standing-data/main/airlines/schema-01/airlines.csv"
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
COUNTRIES_URL = "https://davidmegginson.github.io/ourairports-data/countries.csv"
OUT = pathlib.Path(__file__).resolve().parent.parent / "data" / "routes.csv"
TUNISIA = "DT"  # ICAO prefix of Tunisian airports

# Airports whose OurAirports "municipality" is the suburb, not the city travellers know
CITY_OVERRIDES = {
    "DAOO": "Oran", "DTTJ": "Djerba", "EBBR": "Brussels", "EDDK": "Cologne", "EDDP": "Leipzig",
    "EDDT": "Berlin Tegel", "EDLP": "Paderborn", "EGCN": "Doncaster", "EPRZ": "Rzeszów",
    "LDZA": "Zagreb", "LFBO": "Toulouse", "LFJL": "Metz", "LFLL": "Lyon", "LFML": "Marseille",
    "LFQQ": "Lille", "LFSB": "Basel-Mulhouse", "LFTH": "Toulon", "LIMC": "Milan",
    "LJLJ": "Ljubljana", "LWSK": "Skopje", "UKBB": "Kyiv", "EDDF": "Frankfurt",
    "EGNT": "Newcastle", "ESGG": "Gothenburg", "LFPO": "Paris Orly", "LFPG": "Paris",
    "EGKK": "London Gatwick", "EGLL": "London Heathrow", "EGSS": "London Stansted",
    "LIRF": "Rome", "LIPZ": "Venice", "ELLX": "Luxembourg",
}


# Short names as travellers know them
AIRLINE_OVERRIDES = {"LBT": "Nouvelair", "TAR": "Tunisair", "TUX": "Tunisair Express"}


def fetch(url):
    with urllib.request.urlopen(url, timeout=120) as resp:
        data = resp.read()
    if url.endswith(".gz"):
        data = gzip.decompress(data)
    return list(csv.DictReader(io.StringIO(data.decode("utf-8-sig"))))


def direction(origin, destination):
    if origin.startswith(TUNISIA) and destination.startswith(TUNISIA):
        return "Domestic"
    if origin.startswith(TUNISIA):
        return "Departure"
    if destination.startswith(TUNISIA):
        return "Arrival"
    return "Via Tunisia"


def main():
    routes = [r for r in fetch(ROUTES_URL)
              if any(code.startswith(TUNISIA) for code in r["AirportCodes"].split("-"))]
    airlines = {a["ICAO"]: a["Name"] for a in fetch(AIRLINES_URL) if a["ICAO"]}
    airlines.update(AIRLINE_OVERRIDES)
    countries = {c["code"]: c["name"] for c in fetch(COUNTRIES_URL)}
    airports = {}
    for a in fetch(AIRPORTS_URL):
        code = a["icao_code"] or a["gps_code"] or a["ident"]
        airports[code] = {
            # "Paris (Roissy-en-France, Val-d'Oise)" -> "Paris"
            "city": CITY_OVERRIDES.get(code) or (a["municipality"] or a["name"]).split(" (")[0].split(",")[0],
            "country": countries.get(a["iso_country"], a["iso_country"]),
            "lat": a["latitude_deg"],
            "lon": a["longitude_deg"],
        }

    unknown = {"city": "", "country": "", "lat": "", "lon": ""}
    rows = []
    for r in routes:
        stops = r["AirportCodes"].split("-")
        origin, destination = stops[0], stops[-1]
        o, d = airports.get(origin, unknown), airports.get(destination, unknown)
        rows.append({
            "callsign": r["Callsign"],
            "airline_code": r["AirlineCode"],
            "airline": airlines.get(r["AirlineCode"], r["AirlineCode"]),
            "direction": direction(origin, destination),
            "origin": origin, "origin_city": o["city"], "origin_country": o["country"],
            "origin_lat": o["lat"], "origin_lon": o["lon"],
            "destination": destination, "destination_city": d["city"], "destination_country": d["country"],
            "destination_lat": d["lat"], "destination_lon": d["lon"],
        })

    OUT.parent.mkdir(exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(sorted(rows, key=lambda x: x["callsign"]))
    print(f"wrote {len(rows)} routes ({len({r['airline_code'] for r in rows})} airlines) to {OUT}")


if __name__ == "__main__":
    main()
