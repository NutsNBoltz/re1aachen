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
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional

import requests

PLAN_URL = "https://api.transitous.org/api/v1/plan"
TRIP_URL = "https://api.transitous.org/api/v1/trip"
HEADERS = {"User-Agent": "RE-Tracker/1.0 (Student Project)"}
DB_PATH = "re_tracker.db"
REQUEST_TIMEOUT = 20          # seconds
DELAYED_THRESHOLD_MIN = 3     # >= this many minutes late counts as DELAYED
ACTIVE_GRACE_MIN = 20         # keep refreshing a trip this long after its (actual) final arrival
START_HORIZON_MIN = 15        # start refreshing a trip this long before it departs
SNAPSHOT_WINDOW_MIN = 120     # a new trip gets a one-off full-route snapshot within this window
REFRESH_PAUSE_S = 0.3         # politeness delay between /trip requests

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
    partially_cancelled  INTEGER NOT NULL DEFAULT 0,  -- some stops cancelled (ever)
    planned_origin       TEXT,             -- first full-route snapshot, never overwritten
    planned_destination  TEXT,             -- first full-route snapshot, never overwritten
    planned_stop_count   INTEGER,
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

CREATE TABLE IF NOT EXISTS trip_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    trip_id     TEXT NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,   -- STOPS_DISAPPEARED | DESTINATION_CHANGED
    detail      TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    UNIQUE (trip_id, kind, detail)
);
"""

# Views are dropped and recreated on every start so edits to them always apply.
VIEWS = """
DROP VIEW IF EXISTS stop_delay_deltas;
CREATE VIEW stop_delay_deltas AS
SELECT
    se.*,
    delay_minutes - LAG(delay_minutes) OVER (
        PARTITION BY trip_id
        ORDER BY COALESCE(scheduled_arrival, scheduled_departure)
    ) AS merged_delta_minutes
FROM stop_events se
WHERE status != 'CANCELLED';

DROP VIEW IF EXISTS trip_outcomes;
CREATE VIEW trip_outcomes AS
SELECT *,
    CASE WHEN last_served_station IS NOT NULL AND planned_destination IS NOT NULL
          AND last_served_station <> planned_destination THEN 1 ELSE 0 END AS terminated_early,
    CASE WHEN first_served_station IS NOT NULL AND planned_origin IS NOT NULL
          AND first_served_station <> planned_origin THEN 1 ELSE 0 END AS started_late,
    CASE WHEN cancelled = 0
          AND datetime(last_seen_at) < datetime(final_sched_time, '-10 minutes')
          AND datetime('now') > datetime(final_sched_time, '+30 minutes')
         THEN 1 ELSE 0 END AS lost_before_end
FROM (
    SELECT t.trip_id, t.line, t.planned_origin, t.planned_destination,
           t.cancelled, t.partially_cancelled, t.scheduled_start, t.last_seen_at,
           (SELECT station_name FROM stop_events s
             WHERE s.trip_id = t.trip_id AND s.status != 'CANCELLED'
             ORDER BY COALESCE(s.scheduled_arrival, s.scheduled_departure) DESC LIMIT 1)
             AS last_served_station,
           (SELECT station_name FROM stop_events s
             WHERE s.trip_id = t.trip_id AND s.status != 'CANCELLED'
             ORDER BY COALESCE(s.scheduled_departure, s.scheduled_arrival) ASC LIMIT 1)
             AS first_served_station,
           (SELECT COUNT(*) FROM stop_events s
             WHERE s.trip_id = t.trip_id AND s.status = 'CANCELLED') AS cancelled_stops,
           (SELECT COUNT(*) FROM stop_events s WHERE s.trip_id = t.trip_id) AS total_stops,
           (SELECT MAX(COALESCE(s.scheduled_arrival, s.scheduled_departure))
              FROM stop_events s WHERE s.trip_id = t.trip_id) AS final_sched_time
    FROM trips t
);
"""

MIGRATIONS = [  # (table, column, type) - lets an existing database pick up new columns
    ("trips", "planned_origin", "TEXT"),
    ("trips", "planned_destination", "TEXT"),
    ("trips", "planned_stop_count", "INTEGER"),
]

UPSERT_TRIP = """
INSERT INTO trips (trip_id, line, origin, destination, headsign, scheduled_start,
                   cancelled, partially_cancelled,
                   planned_origin, planned_destination, planned_stop_count,
                   first_seen_at, last_seen_at)
VALUES (:trip_id, :line, :origin, :destination, :headsign, :scheduled_start,
        :cancelled, :partially_cancelled,
        :planned_origin, :planned_destination, :planned_stop_count, :now, :now)
ON CONFLICT(trip_id) DO UPDATE SET
    -- only a full-route observation (/trip) knows the real endpoints of the run;
    -- /plan legs are slices, so they must not overwrite these
    origin          = CASE WHEN :full THEN excluded.origin          ELSE trips.origin          END,
    destination     = CASE WHEN :full THEN excluded.destination     ELSE trips.destination     END,
    scheduled_start = CASE WHEN :full THEN COALESCE(excluded.scheduled_start, trips.scheduled_start)
                                      ELSE trips.scheduled_start END,
    cancelled       = CASE WHEN :full THEN excluded.cancelled       ELSE trips.cancelled       END,
    headsign        = COALESCE(excluded.headsign, trips.headsign),
    partially_cancelled = MAX(trips.partially_cancelled, excluded.partially_cancelled),
    planned_origin      = COALESCE(trips.planned_origin,      excluded.planned_origin),
    planned_destination = COALESCE(trips.planned_destination, excluded.planned_destination),
    planned_stop_count  = COALESCE(trips.planned_stop_count,  excluded.planned_stop_count),
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


def migrate(conn: sqlite3.Connection) -> None:
    for table, column, col_type in MIGRATIONS:
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    conn.commit()


def init_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    migrate(conn)
    conn.executescript(VIEWS)
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


def record_event(conn: sqlite3.Connection, trip_id: str, kind: str, detail: str, now: str) -> None:
    cur = conn.execute(
        "INSERT OR IGNORE INTO trip_events (trip_id, kind, detail, observed_at) VALUES (?,?,?,?)",
        (trip_id, kind, detail, now),
    )
    if cur.rowcount:
        log.warning("%s %s: %s", trip_id, kind, detail)


def reconcile_full_trip(conn: sqlite3.Connection, trip_id: str,
                        names: list[str], now: str) -> None:
    """A full-route response is authoritative. Stops we stored earlier that are
    missing from it AND still lie ahead of us were dropped by the operator
    (short-turn / partial cancellation), so mark them CANCELLED instead of
    leaving stale 'SCHEDULED' rows behind. Stops already passed are left alone."""
    marks = ",".join("?" * len(names))
    gone = [r[0] for r in conn.execute(
        f"""SELECT station_name FROM stop_events
            WHERE trip_id = ? AND status != 'CANCELLED'
              AND station_name NOT IN ({marks})
              AND COALESCE(actual_arrival, actual_departure,
                           scheduled_arrival, scheduled_departure) > ?
            ORDER BY COALESCE(scheduled_arrival, scheduled_departure)""",
        [trip_id, *names, now])]
    if not gone:
        return
    conn.executemany(
        """UPDATE stop_events SET status = 'CANCELLED', delay_minutes = NULL,
               delay_delta_minutes = NULL, last_updated_at = ?
           WHERE trip_id = ? AND station_name = ?""",
        [(now, trip_id, n) for n in gone])
    conn.execute("UPDATE trips SET partially_cancelled = 1 WHERE trip_id = ?", (trip_id,))
    record_event(conn, trip_id, "STOPS_DISAPPEARED", ", ".join(gone), now)


def store_leg(conn: sqlite3.Connection, line: str, leg: dict[str, Any],
              full: bool = False) -> None:
    """full=True: the leg comes from /trip and covers the whole run.
    full=False: the leg comes from /plan and is only a boarding->alighting slice."""
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
        "cancelled": int(bool(leg.get("cancelled")) or (full and n_cancelled == len(stops))),
        "partially_cancelled": int(n_cancelled > 0 and (not full or n_cancelled < len(stops))),
        "planned_origin": stops[0]["station_name"] if full else None,
        "planned_destination": stops[-1]["station_name"] if full else None,
        "planned_stop_count": len(stops) if full else None,
        "full": int(full),
        "now": now,
    }
    with conn:  # one transaction per leg
        previous = conn.execute(
            "SELECT planned_destination FROM trips WHERE trip_id = ?", (trip_id,)).fetchone()
        conn.execute(UPSERT_TRIP, trip)
        if full:
            reconcile_full_trip(conn, trip_id, [s["station_name"] for s in stops], now)
            planned = previous[0] if previous else None
            if planned and planned != trip["destination"]:
                record_event(conn, trip_id, "DESTINATION_CHANGED",
                             f"{planned} -> {trip['destination']}", now)
        conn.executemany(UPSERT_STOP, stops)


# --------------------------------------------------------------------------
# Live refresh of trips already in the database
# --------------------------------------------------------------------------
ACTIVE_TRIPS_SQL = """
SELECT t.trip_id, t.line
FROM trips t
WHERE
    -- (a) newly discovered trip: take one full-route snapshot as early as possible,
    --     so the planned route is recorded before anything gets shortened
    (t.planned_destination IS NULL AND t.first_seen_at >= :recent)
 OR
    -- (b) running or about to run, and not yet finished
    (t.cancelled = 0
     AND t.scheduled_start <= :horizon
     AND (
        SELECT MAX(COALESCE(s.actual_arrival, s.actual_departure,
                            s.scheduled_arrival, s.scheduled_departure))
        FROM stop_events s
        WHERE s.trip_id = t.trip_id AND s.status != 'CANCELLED'
     ) >= :cutoff)
"""


def active_trips(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Trips that have started (or are about to) and haven't finished yet."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    params = {
        "horizon": (now + timedelta(minutes=START_HORIZON_MIN)).isoformat(),
        "cutoff": (now - timedelta(minutes=ACTIVE_GRACE_MIN)).isoformat(),
        "recent": (now - timedelta(minutes=SNAPSHOT_WINDOW_MIN)).isoformat(),
    }
    return [(r[0], r[1]) for r in conn.execute(ACTIVE_TRIPS_SQL, params)]


def fetch_trip(session: requests.Session, trip_id: str) -> Optional[dict[str, Any]]:
    resp = session.get(TRIP_URL, params={"tripId": trip_id},
                       headers=HEADERS, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 404:      # purged / unknown to the server
        return None
    resp.raise_for_status()
    return resp.json()


def pick_trip_leg(data: dict[str, Any], trip_id: str) -> Optional[dict[str, Any]]:
    legs = [l for l in data.get("legs", []) if l.get("mode") == "REGIONAL_RAIL"]
    exact = [l for l in legs if l.get("tripId") == trip_id]
    chosen = exact or legs[:1]
    return chosen[0] if chosen else None


def refresh_active_trips(conn: sqlite3.Connection, session: requests.Session) -> int:
    """Re-query every unfinished trip so rows keep receiving live delays
    after the train has left the origin of our /plan queries."""
    refreshed = 0
    for trip_id, line in active_trips(conn):
        try:
            data = fetch_trip(session, trip_id)
        except (requests.RequestException, ValueError) as exc:
            log.error("trip refresh %s failed: %s", trip_id, exc)
            continue
        leg = data and pick_trip_leg(data, trip_id)
        if not leg:
            log.debug("no usable leg for trip %s", trip_id)
            continue
        # force the stored id so rows merge even if the server echoes a variant
        store_leg(conn, line, {**leg, "tripId": trip_id}, full=True)
        refreshed += 1
        time.sleep(REFRESH_PAUSE_S)
    log.info("refreshed %d active trips", refreshed)
    return refreshed


def probe(trip_id: str) -> None:
    """Debug helper: show what /trip returns for one tripId."""
    with requests.Session() as session:
        data = fetch_trip(session, trip_id)
    if not data:
        print("404 / empty response")
        return
    for leg in data.get("legs", []):
        print(leg.get("mode"), leg.get("displayName") or leg.get("routeShortName"),
              "tripId match:", leg.get("tripId") == trip_id,
              "stops:", len(leg_places(leg)), "realTime:", leg.get("realTime"))


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
    refresh_active_trips(conn, session)
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--once", action="store_true", help="poll a single time and exit")
    parser.add_argument("--interval", type=int, default=120, help="seconds between polls")
    parser.add_argument("--probe", metavar="TRIP_ID", help="print what /trip returns for one trip and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.probe:
        probe(args.probe)
        return
    conn = init_db(args.db)
    with requests.Session() as session:
        while True:
            poll_once(conn, session)
            if args.once:
                break
            time.sleep(args.interval)


if __name__ == "__main__":
    main()