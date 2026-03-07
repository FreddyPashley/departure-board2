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
                   "Please keep your luggage and belongings with you at all times",
                   "",
                   "Book a slot at rfe.xu.ivao.aero",
                   "Watch live at twitch.tv/ivao_official",
                   "ATC Feedback: bit.ly/XUFeedback"]

def updateBanner():
    with open(os.path.join(BASE_DIR, "stats.json")) as f:
        s = json.load(f)
    banner_messages[2] = f"Departures: {len(s['departures'])} Arrivals: {len(s['arrivals'])}"

def getGmc(gate):
    # GMC 1
    gate = int(gate[:3])
    if gate in range(209, 221) or gate in range(231, 259) or gate in ["336", "334", "332", "330", "328", "326", "357", "355", "353", "351", "701", "701"]:
        return "1"
    if gate in range(221, 227) or gate in range(301, 308) or gate in range(590, 596) or gate in range(363, 366) or str(gate).startswith("4") or str(gate).startswith("6") or gate in ["340", "342", "335", "331", "329", "327", "325", "323", "318", "320", "322", "332", "321", "319", "317", "316", "313", "311", "309"]:
        return "2"
    if gate in range(501, 584):
        return "3"
    return "?"

def load_airports():
    global airports_cache
    path = os.path.join(BASE_DIR, "airports.json")
    with open(path, encoding="utf-8") as f:
        airports_cache = json.load(f)

def newDep(cs):
    with open(os.path.join(BASE_DIR, "stats.json")) as f:
        stats = json.load(f)
    if cs not in stats["departures"]:
        stats["departures"].append(cs)
    with open(os.path.join(BASE_DIR, "stats.json"), "w") as f:
        json.dump(stats, f, skipkeys=True, indent=4)
    
    with open(os.path.join(BASE_DIR, "override_gates.json")) as f:
        overrides = json.load(f)
    if cs in overrides:
        del overrides[cs]
    with open(os.path.join(BASE_DIR, "override_gates.json"), "w") as f:
        json.dump(overrides, f, skipkeys=True, indent=4)

def newArr(cs):
    with open(os.path.join(BASE_DIR, "stats.json")) as f:
        stats = json.load(f)
    if cs not in stats["arrivals"]:
        stats["arrivals"].append(cs)
    with open(os.path.join(BASE_DIR, "stats.json"), "w") as f:
        json.dump(stats, f, skipkeys=True, indent=4)


def newArrived(cs):
    with open(os.path.join(BASE_DIR, "arrived.json")) as f:
        arrived = json.load(f)
    if cs not in arrived:
        arrived[cs] = str(datetime.datetime.now())
    with open(os.path.join(BASE_DIR, "arrived.json"), "w") as f:
        json.dump(arrived, f, skipkeys=True, indent=4)


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
                rows.append({
                    "id":               int(r["id"]),
                    "flight_number":    r.get("flight_number"),
                    "callsign":         r.get("callsign"),
                    "origin_icao":      r.get("origin_icao"),
                    "destination_icao": r.get("destination_icao"),
                    "departure_time":   _parse_datetime(r.get("departure_time")),
                    "target_time":      _parse_datetime(r.get("departure_time")) - datetime.timedelta(minutes=5) if r.get("departure_time") else None,
                    "arrival_time":     _parse_datetime(r.get("arrival_time")),
                    "aircraft_icao":    r.get("aircraft_icao"),
                    "gate":             r.get("gate"),
                    "gmc":              getGmc(r.get("gate"))
                })
            break
    rows.sort(key=lambda r: r.get("departure_time") or r.get("arrival_time") or datetime.datetime.max)
    flights_cache = rows
    flights_ts = now
    return flights_cache


def fetch_whazzup():
    """Pull live pilot data from IVAO whazzup API, filtered to AIRPORT_ICAO."""
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
                    "cs":             cs,
                }
        with whazzup_lock:
            whazzup_pilots = pilots
    except Exception as e:
        print(f"[Whazzup] {e}")


def whazzup_loop():
    while True:
        updateBanner()
        fetch_whazzup()
        _time.sleep(15)


def fmt_time(dt):
    if isinstance(dt, datetime.datetime):
        return dt.strftime("%H:%M")

    if isinstance(dt, datetime.timedelta):
        total = int(dt.total_seconds())
        h = (total // 3600) % 24
        m = (total % 3600) // 60
        return f"{h:02d}:{m:02d}"

    if isinstance(dt, str):
        return dt[:5]

    return ""



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
    now = datetime.datetime.now()
    if isinstance(dt, datetime.datetime):
        return (dt - now).total_seconds() / 60
    elif isinstance(dt, datetime.timedelta):
        now_seconds = now.hour * 3600 + now.minute * 60 + now.second
        return (dt.total_seconds() - now_seconds) / 60
    return 999


def sort_key(dt):
    """Sort by time-of-day, wrapping times before 06:00 to end of list."""
    if dt == "":
        return True
    if isinstance(dt, datetime.datetime):
        total = dt.hour * 60 + dt.minute
        if total < 360:
            total += 1440
        return total
    else:
        h = int(dt.split(":")[0])
        return dt == "18:00" or h in range(12, 18) or True


# IVAO API states: Boarding, Departing, Departed, Initial Climb,
#                  En Route, Approach, Landed, On Blocks

def dep_status(flight=None, live=None):
    """
    Return (status_text, colour) for a departure.
    Handles both scheduled flights and live departures.
    """

    # Determine the departure time as a datetime object
    if flight and flight.get("departure_time"):
        dt = flight["departure_time"]  # datetime.datetime
    elif live and live.get("departureTime") is not None:
        # live.get("departureTime") is seconds since midnight UTC?
        # Convert it to today's datetime
        now = datetime.datetime.utcnow()
        dt = datetime.datetime.combine(now.date(), datetime.time()) + datetime.timedelta(seconds=live["departureTime"])
    else:
        dt = None

    # Calculate minutes until departure
    def minutes_until(dt):
        if not dt:
            return 999
        now = datetime.datetime.utcnow()
        return (dt - now).total_seconds() / 60

    mins = minutes_until(dt)

    # Determine status based on live state
    st = live["state"] if live else None

    if not st:
        return "Scheduled", "white"

    if st == "Boarding":
        with open(os.path.join(BASE_DIR, "departed.json")) as f:
            deps = json.load(f)
        if live["cs"] in deps["departures"]:
            deps["departures"].remove(live["cs"])
        with open(os.path.join(BASE_DIR, "departed.json"), "w") as f:
            json.dump(deps, f, skipkeys=True, indent=4)
        if not flight:
            return "Enquire airline", "white"
        if mins > 5:
            return "Go to gate", "green"
        else:
            return "Final call", "yellow"

    if st == "Departing":
        with open(os.path.join(BASE_DIR, "departed.json")) as f:
            deps = json.load(f)
        if live["cs"] not in deps["departures"]:
            deps["departures"].append(live["cs"])
            with open(os.path.join(BASE_DIR, "departed.json"), "w") as f:
                json.dump(deps, f, skipkeys=True, indent=4)
            return "Gate closed", "red"

    if st == "Initial Climb":
        if live and live.get("cs"):
            newDep(live["cs"])

    # Beyond departure phase - remove from board
    if st in ("Departed", "Initial Climb", "En Route", "Approach", "Landed", "On Blocks"):
        return None, None

    return "Scheduled", "white"



def arr_status(live):
    """Return (status_text, colour) for an arrival."""
    if live is None:
        return "Scheduled", "white"
    st = live["state"]

    # Still at departure airport
    if st in ("Boarding", "Departing"):
        return "Scheduled", "white"

    if st == "Landed":
        newArr(live["cs"])
        return "Landed", "white"
    
    if st == "On Blocks":
        newArrived(live["cs"])
        return "Arrived", "green"
    
    # Airborne - show ETA if available, colour by delay
    eta = calc_eta(live)
    if eta:
        return f"Expected {eta}", "white"
    return "En Route", "white"


def build_board():
    flights = load_flights_from_file()
    slot_map = {f["callsign"]: f for f in flights}

    with whazzup_lock:
        pilots = whazzup_pilots.copy()

    deps = []
    arrs = []

    for cs, live in pilots.items():

        slot = slot_map.get(cs)

        dep = live.get("departureId")
        arr = live.get("arrivalId")

        flight_number = slot["flight_number"] if slot else cs
        gate = slot["gate"][:3] if slot else ""

        if dep == AIRPORT_ICAO:

            status, colour = dep_status(slot or {}, live)

            gate = None

            if status:
                with open(os.path.join(BASE_DIR, "override_gates.json")) as f:
                    overrides = json.load(f)
                if flight_number in overrides:
                    gate = overrides[flight_number]
                    if gate == "\n": gate = None
                deps.append({
                    "time": fmt_time(slot.get("departure_time") if slot else datetime.timedelta(seconds=live.get("departureTime"))),
                    "destination": airport_name(arr)[:12],
                    "flight_number": flight_number,
                    "callsign": cs,
                    "status": status,
                    "target_time": "XX:XX",
                    "colour": colour,
                    "gate": gate if gate else (slot.get("gate") if (slot is not None and ("shown" not in status or "closed" not in status)) else ""),
                    "pln_gate": gate if gate else (slot.get("gate") if slot else ""),
                    "gmc": getGmc(gate) if gate else (getGmc(slot.get("gate")) if slot else "")
                })
                if "XX:XX" in deps[-1]["status"]:
                    deps[-1]["status"] = deps[-1]["status"].replace("XX:XX", fmt_time((slot.get("departure_time") if slot else datetime.timedelta(seconds=live.get("departureTime"))) - datetime.timedelta(minutes=30)))
                deps[-1]["target_time"] = datetime.datetime.strftime(datetime.datetime.strptime(deps[-1]["time"], "%H:%M") - datetime.timedelta(minutes=5), "%H:%M")

                tobt_time = datetime.datetime.strptime(deps[-1]["time"], "%H:%M").time()
                today = datetime.datetime.today()
                tobt = datetime.datetime.combine(today, tobt_time)

                now = datetime.datetime.now()

                if now < tobt - datetime.timedelta(minutes=10):
                    tobt_colour = "white"        # More than 10 mins before TOBT
                elif tobt - datetime.timedelta(minutes=10) <= now <= tobt:
                    tobt_colour = "green"        # 10 mins before TOBT up to TOBT
                elif tobt < now <= tobt + datetime.timedelta(minutes=10):
                    tobt_colour = "yellow"       # After TOBT up to 10 mins past
                else:
                    tobt_colour = "red"          # More than 10 mins past TOBT

                deps[-1]["target_colour"] = tobt_colour

                """
                White before 10mins before tobt
                Green 10mins before tobt
                @ TOBT still green
                after TOBT, yellow until 5 after scheduled
                red
                """

        if arr == AIRPORT_ICAO:

            status, colour = arr_status(live)

            add_arr = True

            if status == "Arrived":
                with open(os.path.join(BASE_DIR, "arrived.json")) as f:
                    arrived = json.load(f)
                if live["cs"] in arrived:
                    arr_time = _parse_datetime(arrived[live["cs"]].split(".")[0])
                    if datetime.datetime.now() >= arr_time + datetime.timedelta(minutes=4):
                        add_arr = False
            
            if add_arr:
                arrs.append({
                    "time": fmt_time(slot.get("arrival_time") if slot else calc_scheduled_arrival(live)),
                    "origin": airport_name(dep)[:12],
                    "flight_number": flight_number,
                    "status": status,
                    "colour": colour,
                    "gate": gate
                })

    deps2 = []
    arrs2 = []
    for dep in deps:
        if sort_key(dep["time"]):
            deps2.append(dep)
    for arr in arrs:
        if sort_key(arr["time"]):
            arrs2.append(arr)
    deps2 = sorted(deps2, key=lambda x: x["time"])
    arrs2 = sorted(arrs2, key=lambda x: x["time"])
    return {"departures": deps2, "arrivals": arrs2}



@app.route("/")
def board():
    return flask.render_template("board.html", max_rows=ROWS_PER_COL)


@app.route("/planner")
def planner():
    return flask.render_template("planner2.html", max_rows=ROWS_PER_COL)


@app.route("/api/board")
def api_board():
    data = build_board()
    now = datetime.datetime.utcnow()
    data["time"] = f"{now.strftime('%H:%M')} | 7th March"
    data["max_rows"] = ROWS_PER_COL
    data["banner"] = banner_messages
    return flask.jsonify(data)


@app.route("/api/banner", methods=["GET"])
def api_banner_get():
    return flask.jsonify({"messages": banner_messages})

@app.route("/update_gate", methods=["POST"])
def update_gate():
    data = flask.request.json

    id = data["id"]
    text = data["text"]

    path = "override_gates.json"

    with open(os.path.join(BASE_DIR, path)) as f:
        overrides = json.load(f)

    overrides[id] = text

    with open(os.path.join(BASE_DIR, path), "w") as f:
        json.dump(overrides, f, skipkeys=True, indent=4)

    return flask.jsonify({"status": "ok"})

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
        print(f"[+] {len(whazzup_pilots)} {AIRPORT_ICAO} pilots tracked")
    threading.Thread(target=whazzup_loop, daemon=True).start()


if __name__ == "__main__":
    init()
    print("[+] Server: http://127.0.0.1:6767")
    print("[+] Board:   http://127.0.0.1:6767/")
    print("[+] Planner: http://127.0.0.1:6767/planner")
    app.run("0.0.0.0", port=6767, debug=False)
