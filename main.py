from datetime import datetime

import requests


url = "https://api.transitous.org/api/v1/geocode"


def cityStationfinder(stationName: str) -> str:
    response = requests.get(url, params={"text": stationName} , headers={"User-Agent":"RE-1-Delay-Tracker"})

    data = response.json()

    dict = data[0]

    latitude = dict.get("lat")
    longitude = dict.get("lon")

    return f"{latitude},{longitude}"


HEADERS = {"User-Agent": "RE-Tracker/1.0 (Student Project)"}
PLAN_URL = "https://api.transitous.org/api/v1/plan"

CORRIDORS = [
    {
        "line": "RE1",
        "from_name": "Aachen Hbf",
        "to_name": "Hamm (Westf) Hbf",
        "from_coords": "50.7678,6.0914",
        "to_coords": "51.6784,7.8077",
    },
    {
        "line": "RE4",
        "from_name": "Aachen Hbf",
        "to_name": "Dortmund Hbf",
        "from_coords": "50.7678,6.0914",
        "to_coords": "51.5177,7.4588",
    },
]

for corridor in CORRIDORS:
    target_line = corridor["line"]

    response = requests.get(
        PLAN_URL,
        params={
            "fromPlace": corridor["from_coords"],
            "toPlace": corridor["to_coords"],
        },
        headers=HEADERS,
    )
    data = response.json()

    for journey in data.get("itineraries", []):

        if journey.get("transfers", 0) > 0:
            continue

        for leg in journey.get("legs", []):
            if leg.get("mode") != "REGIONAL_RAIL":
                continue


            name = leg.get("displayName") or leg.get("routeShortName", "")

            # Filter: Check if this specific leg matches the target line (e.g., starts with "RE1" or "RE4")
            if not name.startswith(target_line):
                continue

            # Leg matches your target line -> process/save to database
            trip_id = leg.get("tripId")
            print(f"Captured {target_line}: ({leg.get("from")["name"]} -> {leg.get("to")['name']})")
            stops = leg.get("intermediateStops", [])
            for stop in stops:
                print(stop["name"], end= " -> ")
            print(leg.get("to")["name"])