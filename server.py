import flask
import requests
import pymysql
import datetime
import threading
import time as _time
import json
import os

FLIGHTS_SOURCE = "file"          # "db" or "file"
FLIGHTS_FILE   = "flights.json" 

DB_HOST      = "localhost"
DB_USER      = "root"
DB_PASS      = ""
FLIGHTS_DB   = "xuivao_rfe"
AIRPORT_ICAO = "EGLL"
WHAZZUP_URL  = "https://api.ivao.aero/v2/tracker/whazzup"
ROWS_PER_COL = 22

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = flask.Flask(__name__)

airports_cache = {}
whazzup_pilots = {}
whazzup_lock   = threading.Lock()
flights_cache  = []
flights_ts     = 0
banner_messages = ["Welcome to the Heathrow RFE 2026!",
                   "Departures: 0 | Arrivals: 0",
                   "Please keep your luggage and belongings with you at all times",
                   "Book a slot at rfe.xu.ivao.aero",
                   "Watch live at twitch.tv/ivao_official"]


def load_airports():
    global airports_cache
    path = os.path.join(BASE_DIR, "airports.json")
    with open(path, encoding="utf-8") as f:
        airports_cache = json.load(f)


def airport_name(icao):
    if not icao:
        return ""
    r = airports_cache.get(icao)
    if r:
        return (r.get("name") or r.get("city") or icao).replace(".", "").strip()
    return icao


def get_db():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4"
    )


def _parse_datetime(s):
    """Parse a datetime string from JSON into a datetime object."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def load_flights_from_file():
    """Load flights from flights.json (phpMyAdmin export format)."""
    global flights_cache, flights_ts
    now = _time.time()
    if now - flights_ts < 5:
        return flights_cache
    path = os.path.join(BASE_DIR, FLIGHTS_FILE)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for item in data:
        if item.get("type") == "table" and item.get("name") == "flights":
            for r in item.get("data", []):
                if r.get("origin_icao") == AIRPORT_ICAO or r.get("destination_icao") == AIRPORT_ICAO:
                    rows.append({
                        "id":               int(r["id"]),
                        "flight_number":    r.get("flight_number"),
                        "callsign":         r.get("callsign"),
                        "origin_icao":      r.get("origin_icao"),
                        "destination_icao": r.get("destination_icao"),
                        "departure_time":   _parse_datetime(r.get("departure_time")),
                        "arrival_time":     _parse_datetime(r.get("arrival_time")),
                        "aircraft_icao":    r.get("aircraft_icao"),
                        "terminal":         r.get("terminal"),
                        "gate":             r.get("gate"),
                    })
            break
    rows.sort(key=lambda r: r.get("departure_time") or r.get("arrival_time") or datetime.datetime.max)
    flights_cache = rows
    flights_ts = now
    return flights_cache


def load_flights_from_db():
    """Cached DB query, refreshes every 5 seconds."""
    global flights_cache, flights_ts
    now = _time.time()
    if now - flights_ts < 5:
        return flights_cache
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, flight_number, callsign, origin_icao, destination_icao, "
                f"       departure_time, arrival_time, aircraft_icao, terminal, gate "
                f"FROM `{FLIGHTS_DB}`.`flights` "
                f"WHERE origin_icao = %s OR destination_icao = %s "
                f"ORDER BY COALESCE(departure_time, arrival_time)",
                (AIRPORT_ICAO, AIRPORT_ICAO)
            )
            flights_cache = cur.fetchall()
            flights_ts = now
            return flights_cache
    finally:
        conn.close()


def load_flights():
    if FLIGHTS_SOURCE == "file":
        return load_flights_from_file()
    return load_flights_from_db()


def fetch_whazzup():
    """Pull live pilot data from IVAO whazzup API, filtered to EGLL."""
    global whazzup_pilots
    try:
        r = requests.get(WHAZZUP_URL, timeout=15)
        data = r.json()
        pilots = {}
        for p in data.get("clients", {}).get("pilots", []):
            fp = p.get("flightPlan") or {}
            dep_id = fp.get("departureId") or ""
            arr_id = fp.get("arrivalId") or ""
            if dep_id == AIRPORT_ICAO or arr_id == AIRPORT_ICAO:
                cs = p.get("callsign", "")
                track = p.get("lastTrack") or {}
                pilots[cs] = {
                    "state":          track.get("state", ""),
                    "groundSpeed":    track.get("groundSpeed", 0),
                    "arrivalDistance": track.get("arrivalDistance"),
                    "departureId":    dep_id,
                    "arrivalId":      arr_id,
                    "departureTime":  fp.get("departureTime"),
                    "eet":            fp.get("eet"),
                    "onGround":       track.get("onGround", True),
                }
        with whazzup_lock:
            whazzup_pilots = pilots
    except Exception as e:
        print(f"[Whazzup] {e}")


def whazzup_loop():
    while True:
        fetch_whazzup()
        _time.sleep(15)


def fmt_time(dt):
    if isinstance(dt, datetime.datetime):
        return dt.strftime("%H:%M")
    if dt is None:
        return ""
    return str(dt)[:5]


def calc_eta(live):
    """ETA from distance/speed, falling back to departure + EET."""
    d = live.get("arrivalDistance")
    s = live.get("groundSpeed")
    if d and s and s > 0:
        eta = datetime.datetime.utcnow() + datetime.timedelta(hours=d / s)
        return eta.strftime("%H:%M")
    dep_t = live.get("departureTime")
    eet = live.get("eet")
    if dep_t is not None and eet is not None:
        total = dep_t + eet
        return f"{(total // 3600) % 24:02d}:{(total % 3600) // 60:02d}"
    return None


def calc_scheduled_arrival(live):
    """Planned arrival time from departure + EET."""
    dep_t = live.get("departureTime")
    eet = live.get("eet")
    if dep_t is not None and eet is not None:
        total = dep_t + eet
        return f"{(total // 3600) % 24:02d}:{(total % 3600) // 60:02d}"
    return None


def minutes_until(dt):
    if isinstance(dt, datetime.datetime):
        return (dt - datetime.datetime.utcnow()).total_seconds() / 60
    return 999


def sort_key(dt):
    """Sort by time-of-day, wrapping times before 06:00 to end of list."""
    if isinstance(dt, datetime.datetime):
        total = dt.hour * 60 + dt.minute
        if total < 360:
            total += 1440
        return total
    return 9999


# IVAO API states: Boarding, Departing, Departed, Initial Climb,
#                  En Route, Approach, Landed, On Blocks

def dep_status(flight, live):
    """Return (status_text, colour) for a departure."""
    dt = flight.get("departure_time")
    mins = minutes_until(dt)

    if not live:
        if mins < -30:
            return "Departed", "white"
        if mins < 0:
            return "Delayed", "red"
        return "Scheduled", "white"

    st = live["state"]

    if st in ("Boarding", "On Blocks"):
        if mins < 0:
            return "Delayed", "red"
        if mins <= 10:
            return "Boarding", "green"
        return "On Time", "green"

    if st == "Departing":
        spd = live.get("groundSpeed", 0)
        if spd and spd > 5:
            return "Taxiing", "yellow"
        return "Pushback", "yellow"

    if st in ("Departed", "Initial Climb"):
        return "Departed", "white"

    # Beyond departure phase - remove from board
    if st in ("En Route", "Approach", "Landed"):
        return None, None

    return "Scheduled", "white"


def arr_status(flight, live):
    """Return (status_text, colour) for an arrival."""
    dt = flight.get("arrival_time")
    sched_mins = minutes_until(dt) if dt else 999

    if not live:
        if sched_mins < -30:
            return "Arrived", "white"
        if sched_mins < 0:
            return "Delayed", "red"
        return "Scheduled", "white"

    st = live["state"]

    # Still at departure airport
    if st in ("Boarding", "On Blocks", "Departing"):
        if live.get("departureId") != AIRPORT_ICAO:
            if sched_mins < 0:
                return "Delayed", "red"
            return "Scheduled", "white"

    if st == "Departing":
        return "Taxiing", "white"

    # Airborne - show ETA if available, colour by delay
    if st in ("Departed", "Initial Climb", "En Route"):
        eta = calc_eta(live)
        if eta:
            if dt and isinstance(dt, datetime.datetime):
                try:
                    eta_h, eta_m = map(int, eta.split(":"))
                    eta_total = eta_h * 60 + eta_m
                    sched_total = dt.hour * 60 + dt.minute
                    if eta_total - sched_total > 15:
                        return f"Exp {eta}", "red"
                    return f"Exp {eta}", "green"
                except ValueError:
                    pass
            return f"Exp {eta}", "white"
        return "En Route", "white"

    if st == "Approach":
        eta = calc_eta(live)
        if eta:
            return f"Exp {eta}", "green"
        return "Approach", "green"

    # Landed or at gate - remove from board
    if st in ("Landed", "On Blocks"):
        return None, None

    return "Scheduled", "white"


def build_board():
    flights = load_flights()
    with whazzup_lock:
        pilots = whazzup_pilots.copy()

    deps = []
    arrs = []

    for f in flights:
        cs = f["callsign"]
        live = pilots.get(cs)

        if f["origin_icao"] == AIRPORT_ICAO:
            status, colour = dep_status(f, live)
            if status is None:
                continue
            gate = f["gate"] or ""
            gate = "".join(c for c in gate if c.isdigit())[:3]
            if status in ("Departed", "En Route", "Taxiing", "Departing"):
                gate = ""
            deps.append({
                "time":          fmt_time(f["departure_time"]),
                "destination":   airport_name(f["destination_icao"]),
                "flight_number": f["flight_number"] or cs,
                "status":        status,
                "colour":        colour,
                "gate":          gate,
                "_sort":         sort_key(f["departure_time"]),
            })

        if f["destination_icao"] == AIRPORT_ICAO:
            status, colour = arr_status(f, live)
            if status is None:
                continue
            arrs.append({
                "time":          fmt_time(f["arrival_time"]),
                "origin":        airport_name(f["origin_icao"]),
                "flight_number": f["flight_number"] or cs,
                "status":        status,
                "colour":        colour,
                "_sort":         sort_key(f["arrival_time"]),
            })

    deps.sort(key=lambda x: x["_sort"])
    arrs.sort(key=lambda x: x["_sort"])
    for d in deps:
        del d["_sort"]
    for a in arrs:
        del a["_sort"]

    return {"departures": deps, "arrivals": arrs}


def build_planner():
    """Full flight list for the planner UI (includes all states)."""
    flights = load_flights()
    with whazzup_lock:
        pilots = whazzup_pilots.copy()

    deps = []
    arrs = []

    for f in flights:
        cs = f["callsign"]
        live = pilots.get(cs)
        live_state = live["state"] if live else "Offline"

        if f["origin_icao"] == AIRPORT_ICAO:
            status, colour = dep_status(f, live)
            if status is None:
                status, colour = "En Route", "white"
            deps.append({
                "id":            f["id"],
                "time":          fmt_time(f["departure_time"]),
                "destination":   airport_name(f["destination_icao"]),
                "dest_icao":     f["destination_icao"],
                "flight_number": f["flight_number"] or cs,
                "callsign":      cs,
                "aircraft":      f["aircraft_icao"] or "",
                "status":        status,
                "colour":        colour,
                "live_state":    live_state,
                "terminal":      f["terminal"] or "",
                "gate":          f["gate"] or "",
                "_sort":         sort_key(f["departure_time"]),
            })

        if f["destination_icao"] == AIRPORT_ICAO:
            status, colour = arr_status(f, live)
            arrs.append({
                "id":            f["id"],
                "time":          fmt_time(f["arrival_time"]),
                "origin":        airport_name(f["origin_icao"]),
                "origin_icao":   f["origin_icao"],
                "flight_number": f["flight_number"] or cs,
                "callsign":      cs,
                "aircraft":      f["aircraft_icao"] or "",
                "status":        status,
                "colour":        colour,
                "live_state":    live_state,
                "terminal":      f["terminal"] or "",
                "gate":          f["gate"] or "",
                "_sort":         sort_key(f["arrival_time"]),
            })

    deps.sort(key=lambda x: x["_sort"])
    arrs.sort(key=lambda x: x["_sort"])
    for d in deps:
        del d["_sort"]
    for a in arrs:
        del a["_sort"]

    return {"departures": deps, "arrivals": arrs}


@app.route("/")
def board():
    return flask.render_template("board.html", max_rows=ROWS_PER_COL)


@app.route("/planner")
def planner():
    return flask.render_template("planner.html")


@app.route("/api/board")
def api_board():
    data = build_board()
    now = datetime.datetime.utcnow()
    data["time"] = f"{now.strftime('%H:%M')} | 7th March"
    data["max_rows"] = ROWS_PER_COL
    data["banner"] = banner_messages
    return flask.jsonify(data)


@app.route("/api/planner")
def api_planner():
    data = build_planner()
    now = datetime.datetime.utcnow()
    data["time"] = f"{now.strftime('%H:%M')} | 7th March"
    return flask.jsonify(data)


@app.route("/api/banner", methods=["GET"])
def api_banner_get():
    return flask.jsonify({"messages": banner_messages})


@app.route("/api/banner", methods=["POST"])
def api_banner_post():
    global banner_messages
    body = flask.request.get_json()
    action = body.get("action", "set")
    if action == "add":
        msg = (body.get("message") or "").strip()
        if msg:
            banner_messages.append(msg)
    elif action == "remove":
        idx = body.get("index")
        if isinstance(idx, int) and 0 <= idx < len(banner_messages):
            banner_messages.pop(idx)
    elif action == "set":
        msgs = body.get("messages")
        if isinstance(msgs, list):
            banner_messages = [m for m in msgs if m and m.strip()]
    return flask.jsonify({"ok": True, "messages": banner_messages})


@app.route("/api/update-gate", methods=["POST"])
def api_update_gate():
    body = flask.request.get_json()
    flight_id = body.get("id")
    gate = body.get("gate", "")
    terminal = body.get("terminal", "")
    if not flight_id:
        return flask.jsonify({"error": "Missing flight id"}), 400
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{FLIGHTS_DB}`.`flights` SET gate = %s, terminal = %s WHERE id = %s",
                (gate, terminal, flight_id)
            )
            conn.commit()
        global flights_ts
        flights_ts = 0
        return flask.jsonify({"ok": True})
    finally:
        conn.close()


def init():
    load_airports()
    print(f"[+] {len(airports_cache)} airports loaded")
    fetch_whazzup()
    with whazzup_lock:
        print(f"[+] {len(whazzup_pilots)} EGLL pilots tracked")
    threading.Thread(target=whazzup_loop, daemon=True).start()


if __name__ == "__main__":
    init()
    print("[+] Server: http://127.0.0.1:6767")
    print("[+] Board:   http://127.0.0.1:6767/")
    print("[+] Planner: http://127.0.0.1:6767/planner")
    app.run("0.0.0.0", port=6767, debug=False)
