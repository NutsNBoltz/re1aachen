"""Diagnostic only: does /trip work for trips that /plan returns right now?
Touches no database. Run:  python probe_trip.py
"""
import requests

BASE = "https://api.transitous.org/api"
HEADERS = {"User-Agent": "RE-Tracker/1.0 (Student Project)"}
FROM, TO = "50.7678,6.0914", "50.9432,6.9587"  # Aachen Hbf -> Köln Hbf

r = requests.get(f"{BASE}/v1/plan", headers=HEADERS, timeout=20,
                 params={"fromPlace": FROM, "toPlace": TO, "numItineraries": 10})
print("plan status:", r.status_code)
legs = [l for it in r.json().get("itineraries", []) for l in it.get("legs", [])
        if l.get("mode") == "REGIONAL_RAIL" and l.get("tripId")]
print("regional rail legs:", len(legs))

for leg in legs[:3]:
    tid = leg["tripId"]
    print("\n---", leg.get("displayName") or leg.get("routeShortName"),
          "| start:", leg.get("scheduledStartTime"), "| realTime:", leg.get("realTime"))
    print("tripId:", repr(tid))
    for version in ("v1", "v2", "v5"):
        try:
            t = requests.get(f"{BASE}/{version}/trip", params={"tripId": tid},
                             headers=HEADERS, timeout=20)
        except requests.RequestException as exc:
            print(f"  {version}: request failed: {exc}")
            continue
        body = t.text.replace("\n", " ")
        n_stops = ""
        if t.ok:
            try:
                tl = [x for x in t.json().get("legs", []) if x.get("mode") == "REGIONAL_RAIL"]
                if tl:
                    n_stops = f" | stops: {len(tl[0].get('intermediateStops') or []) + 2}"
            except ValueError:
                pass
        print(f"  {version}: HTTP {t.status_code}{n_stops} | {body[:160]}")
        if version == "v1":
            print("  url:", t.url[:200])