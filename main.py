#!/usr/bin/env python3
"""RE1 / RE4 live delay archiver (Transitous / MOTIS -> SQLite).

Polls the /plan endpoint for corridor pairs, keeps only direct
REGIONAL_RAIL legs of the target lines, and upserts every stop of each leg
so repeated polls update live predictions instead of creating duplicates.

Usage:
    python re_tracker.py --once
    python re_tracker.py --interval 120
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

import requests

PLAN_URL = "https://api.transitous.org/api/v1/plan"
HEADERS = {"User-Agent": "RE-Tracker/1.0 (Student Project)"}
DB_PATH = "re_tracker.db"
REQUEST_TIMEOUT = 20          # seconds
DELAYED_THRESHOLD_MIN = 3     # >= this many minutes late counts as DELAYED

log = logging.getLogger("re_tracker")


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Corridor:
    line: str
    from_name: str
    to_name: str
    from_coords: str
    to_coords: str


AACHEN = ("Aachen Hbf", "50.7678,6.0914")
KOELN = ("Köln Hbf", "50.9432,6.9587")
HAMM = ("Hamm (Westf) Hbf", "51.6784,7.8077")
DORTMUND = ("Dortmund Hbf", "51.5177,7.4588")


def _pair(line: str, a: tuple[str, str], b: tuple[str, str]) -> list[Corridor]:
    """Both directions of a corridor."""
    return [
        Corridor(line, a[0], b[0], a[1], b[1]),
        Corridor(line, b[0], a[0], b[1], a[1]),
    ]


CORRIDORS: list[Corridor] = (
    _pair("RE1", AACHEN, KOELN)
    + _pair("RE1", AACHEN, HAMM)
    + _pair("RE4", AACHEN, DORTMUND)
)


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS trips (
    trip_id              TEXT PRIMARY KEY,
    line                 TEXT NOT NULL,
    origin               TEXT,
    destination          TEXT,
    headsign             TEXT,
    scheduled_start      TEXT,             -- ISO 8601 UTC
    cancelled            INTEGER NOT NULL DEFAULT 0,  -- whole leg / all stops cancelled
    partially_cancelled  INTEGER NOT NULL DEFAULT 0,  -- some stops cancelled
    first_seen_at        TEXT NOT NULL,
    last_seen_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stop_events (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    trip_id              TEXT NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    station_name         TEXT NOT NULL,
    stop_sequence        INTEGER NOT NULL,  -- position within the latest observed leg
    scheduled_arrival    TEXT,
    actual_arrival       TEXT,
    scheduled_departure  TEXT,
    actual_departure     TEXT,
    delay_minutes        INTEGER,           -- NULL when cancelled / no comparable times
    delay_delta_minutes  INTEGER,           -- delay change vs previous non-cancelled stop
    status               TEXT NOT NULL
        CHECK (status IN ('ON_TIME', 'DELAYED', 'CANCELLED', 'SCHEDULED')),
    last_updated_at      TEXT NOT NULL,
    UNIQUE (trip_id, station_name)
);

CREATE INDEX IF NOT EXISTS idx_stop_events_station ON stop_events(station_name);
CREATE INDEX IF NOT EXISTS idx_trips_line_start ON trips(line, scheduled_start);

-- Legs returned by /plan can be slices of a longer run (boarding -> alighting),
-- so stored sequence numbers are only relative. This view recomputes deltas
-- over the merged data, ordered by scheduled time.
CREATE VIEW IF NOT EXISTS stop_delay_deltas AS
SELECT
    se.*,
    delay_minutes - LAG(delay_minutes) OVER (
        PARTITION BY trip_id
        ORDER BY COALESCE(scheduled_arrival, scheduled_departure)
    ) AS merged_delta_minutes
FROM stop_events se
WHERE status != 'CANCELLED';
"""

UPSERT_TRIP = """
INSERT INTO trips (trip_id, line, origin, destination, headsign, scheduled_start,
                   cancelled, partially_cancelled, first_seen_at, last_seen_at)
VALUES (:trip_id, :line, :origin, :destination, :headsign, :scheduled_start,
        :cancelled, :partially_cancelled, :now, :now)
ON CONFLICT(trip_id) DO UPDATE SET
    origin              = excluded.origin,
    destination         = excluded.destination,
    headsign            = excluded.headsign,
    scheduled_start     = COALESCE(excluded.scheduled_start, trips.scheduled_start),
    cancelled           = excluded.cancelled,
    partially_cancelled = MAX(trips.partially_cancelled, excluded.partially_cancelled),
    last_seen_at        = excluded.last_seen_at
"""

UPSERT_STOP = """
INSERT INTO stop_events (trip_id, station_name, stop_sequence,
                         scheduled_arrival, actual_arrival,
                         scheduled_departure, actual_departure,
                         delay_minutes, delay_delta_minutes, status, last_updated_at)
VALUES (:trip_id, :station_name, :stop_sequence,
        :scheduled_arrival, :actual_arrival,
        :scheduled_departure, :actual_departure,
        :delay_minutes, :delay_delta_minutes, :status, :now)
ON CONFLICT(trip_id, station_name) DO UPDATE SET
    stop_sequence       = excluded.stop_sequence,
    scheduled_arrival   = COALESCE(excluded.scheduled_arrival,   stop_events.scheduled_arrival),
    actual_arrival      = excluded.actual_arrival,
    scheduled_departure = COALESCE(excluded.scheduled_departure, stop_events.scheduled_departure),
    actual_departure    = excluded.actual_departure,
    delay_minutes       = excluded.delay_minutes,
    delay_delta_minutes = excluded.delay_delta_minutes,
    status              = excluded.status,
    last_updated_at     = excluded.last_updated_at
"""


def init_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------
def parse_ts(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 ('Z' or +hh:mm offsets) into an aware UTC datetime."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        log.warning("Unparseable timestamp: %r", value)
        return None
    if dt.tzinfo is None:  # treat naive as UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def delay_minutes(scheduled: Optional[datetime], actual: Optional[datetime]) -> Optional[int]:
    if scheduled is None or actual is None:
        return None
    return round((actual - scheduled).total_seconds() / 60)


def classify(cancelled: bool, delay: Optional[int], realtime: bool) -> str:
    if cancelled:
        return "CANCELLED"
    if delay is None or not realtime:
        return "SCHEDULED"          # no live data (yet) for this stop
    return "DELAYED" if delay >= DELAYED_THRESHOLD_MIN else "ON_TIME"


def leg_places(leg: dict[str, Any]) -> list[dict[str, Any]]:
    """Ordered stations of a leg: from, intermediateStops, to."""
    places = [leg.get("from") or {}]
    places += leg.get("intermediateStops") or []
    places.append(leg.get("to") or {})
    return [p for p in places if p.get("name")]


def build_stop_rows(trip_id: str, leg: dict[str, Any], now: str) -> list[dict[str, Any]]:
    realtime = bool(leg.get("realTime"))
    leg_cancelled = bool(leg.get("cancelled"))
    rows: list[dict[str, Any]] = []
    prev_delay: Optional[int] = None

    for seq, place in enumerate(leg_places(leg)):
        s_arr = parse_ts(place.get("scheduledArrival"))
        a_arr = parse_ts(place.get("arrival"))
        s_dep = parse_ts(place.get("scheduledDeparture"))
        a_dep = parse_ts(place.get("departure"))

        cancelled = leg_cancelled or bool(place.get("cancelled"))

        if cancelled:
            delay = None            # never pollute averages
            delta = None            # and don't use it as a baseline
        else:
            # Prefer arrival delay; origin stops only have a departure.
            delay = delay_minutes(s_arr, a_arr)
            if delay is None:
                delay = delay_minutes(s_dep, a_dep)
            delta = delay - prev_delay if (delay is not None and prev_delay is not None) else None
            if delay is not None:
                prev_delay = delay

        rows.append({
            "trip_id": trip_id,
            "station_name": place["name"],
            "stop_sequence": seq,
            "scheduled_arrival": iso(s_arr),
            "actual_arrival": iso(a_arr),
            "scheduled_departure": iso(s_dep),
            "actual_departure": iso(a_dep),
            "delay_minutes": delay,
            "delay_delta_minutes": delta,
            "status": classify(cancelled, delay, realtime),
            "now": now,
        })
    return rows


def matches_line(leg: dict[str, Any], line: str) -> bool:
    name = leg.get("displayName") or leg.get("routeShortName") or ""
    return name.replace(" ", "").upper().startswith(line)


# --------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------
def fetch_itineraries(session: requests.Session, corridor: Corridor) -> list[dict[str, Any]]:
    resp = session.get(
        PLAN_URL,
        params={
            "fromPlace": corridor.from_coords,
            "toPlace": corridor.to_coords,
            "numItineraries": 10,
            # NOTE: no maxTransfers here. On /api/v1/plan it counts transit legs,
            # so maxTransfers=0 returns no train itineraries at all. Direct
            # trips are filtered below via journey["transfers"] instead.
        },
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("itineraries", [])


def iter_matching_legs(itineraries: list[dict[str, Any]], line: str) -> Iterator[dict[str, Any]]:
    for journey in itineraries:
        if journey.get("transfers", 0) > 0:
            continue
        for leg in journey.get("legs", []):
            if leg.get("mode") == "REGIONAL_RAIL" and leg.get("tripId") and matches_line(leg, line):
                yield leg


def store_leg(conn: sqlite3.Connection, line: str, leg: dict[str, Any]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    trip_id = leg["tripId"]
    stops = build_stop_rows(trip_id, leg, now)
    if not stops:
        return

    n_cancelled = sum(s["status"] == "CANCELLED" for s in stops)
    trip = {
        "trip_id": trip_id,
        "line": line,
        "origin": stops[0]["station_name"],
        "destination": stops[-1]["station_name"],
        "headsign": leg.get("headsign"),
        "scheduled_start": iso(parse_ts(leg.get("scheduledStartTime"))),
        "cancelled": int(bool(leg.get("cancelled")) or n_cancelled == len(stops)),
        "partially_cancelled": int(0 < n_cancelled < len(stops)),
        "now": now,
    }
    with conn:  # one transaction per leg
        conn.execute(UPSERT_TRIP, trip)
        conn.executemany(UPSERT_STOP, stops)


def poll_once(conn: sqlite3.Connection, session: requests.Session) -> int:
    saved = 0
    for corridor in CORRIDORS:
        try:
            itineraries = fetch_itineraries(session, corridor)
        except (requests.RequestException, ValueError) as exc:
            log.error("%s %s -> %s failed: %s", corridor.line,
                      corridor.from_name, corridor.to_name, exc)
            continue
        seen: set[str] = set()
        for leg in iter_matching_legs(itineraries, corridor.line):
            if leg["tripId"] in seen:
                continue
            seen.add(leg["tripId"])
            store_leg(conn, corridor.line, leg)
            saved += 1
        log.info("%s %s -> %s: %d legs stored", corridor.line,
                 corridor.from_name, corridor.to_name, len(seen))
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--once", action="store_true", help="poll a single time and exit")
    parser.add_argument("--interval", type=int, default=120, help="seconds between polls")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = init_db(args.db)
    with requests.Session() as session:
        while True:
            poll_once(conn, session)
            if args.once:
                break
            time.sleep(args.interval)


if __name__ == "__main__":
    main()