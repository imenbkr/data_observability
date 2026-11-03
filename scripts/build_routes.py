"""Build data/routes.csv: every known Nouvelair (ICAO: LBT) callsign with its route.

Sources (both public domain):
  - Routes:   VRS standing data (CC0)  https://github.com/vradarserver/standing-data
  - Airports: OurAirports              https://ourairports.com/data/

Usage: python scripts/build_routes.py
"""
import csv
import io
import pathlib
import urllib.request

ROUTES_URL = "https://raw.githubusercontent.com/vradarserver/standing-data/main/routes/schema-01/L/LBT-all.csv"
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
COUNTRIES_URL = "https://davidmegginson.github.io/ourairports-data/countries.csv"
OUT = pathlib.Path(__file__).resolve().parent.parent / "data" / "routes.csv"


# Airports whose OurAirports "municipality" is the suburb, not the city travellers know
CITY_OVERRIDES = {
    "DAOO": "Oran", "DTTJ": "Djerba", "EBBR": "Brussels", "EDDK": "Cologne", "EDDP": "Leipzig",
    "EDDT": "Berlin Tegel", "EDLP": "Paderborn", "EGCN": "Doncaster", "EPRZ": "Rzeszów",
    "LDZA": "Zagreb", "LFBO": "Toulouse", "LFJL": "Metz", "LFLL": "Lyon", "LFML": "Marseille",
    "LFQQ": "Lille", "LFSB": "Basel-Mulhouse", "LFTH": "Toulon", "LIMC": "Milan",
    "LJLJ": "Ljubljana", "LWSK": "Skopje", "UKBB": "Kyiv", "EDDF": "Frankfurt",
    "EGNT": "Newcastle", "ESGG": "Gothenburg",
}


def fetch_csv(url):
    with urllib.request.urlopen(url, timeout=60) as resp:
        return list(csv.DictReader(io.StringIO(resp.read().decode("utf-8-sig"))))


def main():
    routes = fetch_csv(ROUTES_URL)
    countries = {c["code"]: c["name"] for c in fetch_csv(COUNTRIES_URL)}
    airports = {}
    for a in fetch_csv(AIRPORTS_URL):
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
    print(f"wrote {len(rows)} routes to {OUT}")


if __name__ == "__main__":
    main()
