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



AachenCoords = cityStationfinder("Aachen HBF")
DortmundCoords = cityStationfinder("Dortmund HBF")

url2 = "https://api.transitous.org/api/v1/plan"

plan = requests.get(url2, params={"fromPlace":AachenCoords, "toPlace":DortmundCoords}, headers={"User-Agent":"RE-1-Delay-Tracker"})
routeData = plan.json()


itineraries = routeData.get("itineraries", [])

print(f"Found {len(itineraries)} different routes")

for i, journey in enumerate(itineraries):

    print(f"=== Option{i+1} ===== Transfers: {journey.get("transfers")} ===")

    for leg in journey.get("legs", []):
        mode = leg.get("mode")

        if mode == "WALK":
            continue

        trainName =leg.get("displayName")
        origin = leg.get("from").get("name")
        destination = leg.get("to").get("name")

        schedDep = leg.get("scheduledStartTime")
        actDep = leg.get("startTime")

        timeSched = datetime.fromisoformat(schedDep.replace("Z", "+00:00"))
        timeActual = datetime.fromisoformat(actDep.replace("Z", "+00:00"))

        timeDiff = int((timeActual-timeSched).total_seconds() / 60)

        timeDelay = f"+{timeDiff} minutes late" if timeDiff > 0 else "On time"

        print(f"TrainName: {trainName}: {origin} -> {destination}")
        print(f" Departs {actDep}, {timeDelay}")

    print("-" * 40)