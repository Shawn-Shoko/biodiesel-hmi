"""
================================================================================
 WEB-BASED REMOTE MONITORING & CONTROL SYSTEM
 Siemens S7-200 PLC - Biodiesel Production Plant
--------------------------------------------------------------------------------
 Author      : Joel Musekiwa
 Institution : Midlands State University
 Drawing Ref : SASOL DWG BAEC-SASB-103-0010-001
 File        : app.py  (Flask backend / PLC data server)
 Python      : 3.9+
 Run         : pip install flask  ->  python app.py
================================================================================
"""

from __future__ import annotations

import random
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

LOCK = threading.RLock()

# ==============================================================================
# 1. PLC TAG DATABASE  (Siemens S7-200 CPU 224, PPI / RS-485)
# ==============================================================================

OUTPUTS = {
    "P_101": {"addr": "Q0.0", "name": "Raw Oil Feed Pump",       "group": "pump",   "screen": 1},
    "P_102": {"addr": "Q0.1", "name": "Circulation Pump",        "group": "pump",   "screen": 1},
    "P_103": {"addr": "Q0.2", "name": "Mixing Pump",             "group": "pump",   "screen": 2},
    "P_104": {"addr": "Q0.3", "name": "Water Pump",              "group": "pump",   "screen": 1},
    "P_105": {"addr": "Q0.4", "name": "Settling Transfer Pump",  "group": "pump",   "screen": 2},
    "P_106": {"addr": "Q0.5", "name": "Wash Outlet Pump",        "group": "pump",   "screen": 3},
    "P_107": {"addr": "Q0.6", "name": "Product Transfer Pump",   "group": "pump",   "screen": 3},
    "P_108": {"addr": "Q0.7", "name": "Cleaning Water Pump",     "group": "pump",   "screen": 3},
    "H_101": {"addr": "Q1.0", "name": "Pre-Treatment Heater",    "group": "heater", "screen": 1},
    "H_102": {"addr": "Q1.1", "name": "Reaction Heater",         "group": "heater", "screen": 2},
    "H_103": {"addr": "Q1.2", "name": "Dry Heater",              "group": "heater", "screen": 3},
    "H_104": {"addr": "Q1.3", "name": "Water Heater",            "group": "heater", "screen": 3},
    "XV_101": {"addr": "Q1.4", "name": "Reactor Transfer Valve", "group": "valve",  "screen": 2},
    "XV_102": {"addr": "Q1.5", "name": "Mixing Transfer Valve",  "group": "valve",  "screen": 2},
    "XV_103": {"addr": "Q1.6", "name": "Settling Feed Valve",    "group": "valve",  "screen": 2},
    "XV_104": {"addr": "Q1.7", "name": "Glycerine Drain Valve",  "group": "valve",  "screen": 2},
    "XV_105": {"addr": "Q2.0", "name": "Water Supply Valve",     "group": "valve",  "screen": 3},
    "XV_106": {"addr": "Q2.1", "name": "Spray Valve",            "group": "valve",  "screen": 3},
    "XV_107": {"addr": "Q2.2", "name": "Drain Valve",            "group": "valve",  "screen": 3},
    "L101": {"addr": "Q2.3", "name": "LHS Indicator Light",      "group": "light",  "screen": 1},
    "L102": {"addr": "Q2.4", "name": "RHS Indicator Light",      "group": "light",  "screen": 1},
    "L103": {"addr": "Q2.5", "name": "Settling Light",           "group": "light",  "screen": 2},
}

INPUTS = {
    "LT_101":   {"addr": "I0.0", "name": "Raw Oil Storage Level Switch (FULL)"},
    "LT_102":   {"addr": "I0.1", "name": "Wash Tank Level Switch (FULL)"},
    "START_PB": {"addr": "I0.2", "name": "Start Pushbutton"},
    "STOP_PB":  {"addr": "I0.3", "name": "Stop Pushbutton"},
}

ANALOG = {
    "TK_101_LEVEL": {"name": "Raw Oil Storage Level",     "unit": "%",     "min": 0.0, "max": 100.0, "value": 82.0},
    "TK_101_TEMP":  {"name": "Raw Oil Temperature",       "unit": "°C",    "min": 0.0, "max": 120.0, "value": 26.4},
    "TK_102_LEVEL": {"name": "Pre-Treatment Level",       "unit": "%",     "min": 0.0, "max": 100.0, "value": 34.0},
    "TK_102_TEMP":  {"name": "Pre-Treatment Temperature", "unit": "°C",    "min": 0.0, "max": 120.0, "value": 24.6},
    "TK_103_LEVEL": {"name": "Mixing Reactor Level",      "unit": "%",     "min": 0.0, "max": 100.0, "value": 8.0},
    "TK_103_TEMP":  {"name": "Reactor Temperature",       "unit": "°C",    "min": 0.0, "max": 120.0, "value": 25.2},
    "TK_104_LEVEL": {"name": "Settling Tank Level",       "unit": "%",     "min": 0.0, "max": 100.0, "value": 5.0},
    "TK_104_TEMP":  {"name": "Settling Tank Temperature", "unit": "°C",    "min": 0.0, "max": 120.0, "value": 24.1},
    "TK_105_LEVEL": {"name": "Methoxide Dosing Level",    "unit": "%",     "min": 0.0, "max": 100.0, "value": 68.0},
    "TK_106_LEVEL": {"name": "Warm Water Supply Level",   "unit": "%",     "min": 0.0, "max": 100.0, "value": 76.0},
    "TK_106_TEMP":  {"name": "Warm Water Temperature",    "unit": "°C",    "min": 0.0, "max": 120.0, "value": 28.9},
    "TK_107_LEVEL": {"name": "Glycerine Tank Level",      "unit": "%",     "min": 0.0, "max": 100.0, "value": 12.0},
    "TK_108_LEVEL": {"name": "Wash & Dry Tank Level",     "unit": "%",     "min": 0.0, "max": 100.0, "value": 4.0},
    "TK_108_TEMP":  {"name": "Wash & Dry Temperature",    "unit": "°C",    "min": 0.0, "max": 140.0, "value": 24.4},
    "TK_110_LEVEL": {"name": "Drainage Sump Level",       "unit": "%",     "min": 0.0, "max": 100.0, "value": 3.0},
    "TK_111_LEVEL": {"name": "Biodiesel Product Level",   "unit": "%",     "min": 0.0, "max": 100.0, "value": 24.0},
    "PH":           {"name": "Process pH",                "unit": "pH",    "min": 0.0, "max": 14.0,  "value": 6.9},
    "FLOW":         {"name": "Process Flow",              "unit": "L/min", "min": 0.0, "max": 40.0,  "value": 0.0},
    "PRESSURE":     {"name": "Line Pressure",             "unit": "bar",   "min": 0.0, "max": 10.0,  "value": 1.4},
}

THERMAL = {
    "TK_101_TEMP": {"heat": None,    "ambient": 26.0, "rate": 0.000, "max": 60.0},
    "TK_102_TEMP": {"heat": "H_101", "ambient": 24.5, "rate": 0.020, "max": 62.0},
    "TK_103_TEMP": {"heat": "H_102", "ambient": 24.5, "rate": 0.022, "max": 60.0},
    "TK_104_TEMP": {"heat": "H_102", "ambient": 24.0, "rate": 0.008, "max": 58.0},
    "TK_106_TEMP": {"heat": "H_104", "ambient": 24.0, "rate": 0.025, "max": 82.0},
    "TK_108_TEMP": {"heat": "H_103", "ambient": 24.0, "rate": 0.028, "max": 108.0},
}

STEPS = [
    {"id": 1, "name": "Start Up", "duration": 10,
     "description": "Verify L101 / L102 indicator lamps and confirm LT_101 raw oil storage is FULL.",
     "outputs": ["L101", "L102"], "pulses": []},
    {"id": 2, "name": "Transfer Oil to Reactor", "duration": 60,
     "description": "P_101 transfers raw oil from TK_101 into the mixing reactor TK_103.",
     "outputs": ["P_101"], "pulses": []},
    {"id": 3, "name": "Oil Heating Process", "duration": 7200,
     "description": "P_102 circulation + H_101 pre-treatment heater + XV_101 open.",
     "outputs": ["P_102", "H_101", "XV_101"], "pulses": []},
    {"id": 4, "name": "Dosing & Mixing", "duration": 7200,
     "description": "P_103 mixing pump and XV_103 settling feed valve.",
     "outputs": ["P_103", "XV_103"], "pulses": []},
    {"id": 5, "name": "Settling Phase", "duration": 43200,
     "description": "L103 settling lamp ON. XV_104 glycerine drain pulsed for 30 s.",
     "outputs": ["L103"], "pulses": [{"tag": "XV_104", "on": 30}]},
    {"id": 6, "name": "Transfer to Wash Tank", "duration": 300,
     "description": "P_105 transfers crude biodiesel from TK_104 to TK_108.",
     "outputs": ["P_105"], "pulses": []},
    {"id": 7, "name": "Washing & Drying", "duration": 43200,
     "description": "XV_106 spray 45 s, XV_107 drain 50 s, then H_103 + P_107.",
     "outputs": ["H_103", "P_107"],
     "pulses": [{"tag": "XV_106", "on": 45}, {"tag": "XV_107", "on": 50}]},
    {"id": 8, "name": "Cleaning Sequence", "duration": 1380,
     "description": "P_108, P_102, P_103 and XV_101 flush loop.",
     "outputs": ["P_108", "P_102", "P_103", "XV_101"], "pulses": []},
]

LT_101_LOW_THRESHOLD = 25.0
LT_102_LOW_THRESHOLD = 25.0


# ==============================================================================
# 3. PLC IMAGE / RUNTIME STATE
# ==============================================================================
def _initial_state() -> dict:
    return {
        "estop": False,
        "mode": "AUTO",
        "started_at": time.time(),
        "connection": {
            "ip": "192.168.1.100",
            "port": 102,
            "protocol": "PPI / RS-485",
            "latency_ms": 124,
            "vpn_role": "ENGINEER",
            "online": True,
        },
        "sequence": {"step": 0, "running": False, "elapsed": 0.0,
                     "batch": 1, "batch_started": None, "cycles": 0},
        "outputs": {tag: False for tag in OUTPUTS},
        "manual": {},
        "inputs": {tag: {"state": tag == "LT_101"} for tag in INPUTS},
        "analog": {tag: cfg["value"] for tag, cfg in ANALOG.items()},
        "alarms": [],
        "events": [],
    }


STATE = _initial_state()


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _log(message: str, level: str = "INFO") -> None:
    STATE["events"].insert(0, {"message": message, "level": level, "ts": _now_iso()})
    del STATE["events"][60:]


def _raise_alarm(code: str, message: str, severity: str = "WARNING") -> None:
    if any(al["code"] == code for al in STATE["alarms"]):
        return
    STATE["alarms"].insert(0, {"code": code, "message": message, "severity": severity, "ts": _now_iso()})
    del STATE["alarms"][40:]


def _clear_alarm(code: str) -> None:
    STATE["alarms"] = [al for al in STATE["alarms"] if al["code"] != code]


def _enter_step(number: int) -> None:
    number = int(number)
    if number < 0 or number > len(STEPS):
        raise ValueError("Step %s out of range (0-%d)" % (number, len(STEPS)))
    st = STATE
    seq = st["sequence"]
    seq["step"] = number
    seq["elapsed"] = 0.0
    seq["running"] = number > 0
    st["manual"].clear()
    for tag in st["outputs"]:
        st["outputs"][tag] = False
    if number > 0:
        if not seq["batch_started"]:
            seq["batch_started"] = time.time()
        _log("SFC transition -> STEP %d : %s" % (number, STEPS[number - 1]["name"]), "INFO")
    else:
        seq["batch_started"] = None
        _log("SFC returned to IDLE", "INFO")


def _finish_sequence() -> None:
    seq = STATE["sequence"]
    seq["cycles"] += 1
    completed = seq["batch"]
    seq["batch"] = completed + 1
    _log("Batch #%d completed successfully" % completed, "SUCCESS")
    _enter_step(0)


def _run_sequence(dt: float) -> None:
    st = STATE
    seq = st["sequence"]
    if not seq["running"] or not (1 <= seq["step"] <= len(STEPS)):
        for tag in st["outputs"]:
            st["outputs"][tag] = False
        return
    step = STEPS[seq["step"] - 1]
    seq["elapsed"] += dt
    for tag in st["outputs"]:
        st["outputs"][tag] = False
    for tag in step["outputs"]:
        st["outputs"][tag] = True
    for pulse in step["pulses"]:
        if seq["elapsed"] < pulse["on"]:
            st["outputs"][pulse["tag"]] = True
    if seq["elapsed"] >= step["duration"]:
        if seq["step"] >= len(STEPS):
            _finish_sequence()
        else:
            _enter_step(seq["step"] + 1)


def _apply_manual_overrides() -> None:
    for tag, value in STATE["manual"].items():
        if tag in STATE["outputs"]:
            STATE["outputs"][tag] = bool(value)


def _apply_interlocks() -> None:
    st = STATE
    if st["inputs"]["LT_101"]["state"]:
        _clear_alarm("ILK_LT101")
        return
    tripped = []
    for tag, cfg in OUTPUTS.items():
        if cfg["group"] == "heater" and st["outputs"][tag]:
            st["outputs"][tag] = False
            st["manual"].pop(tag, None)
            tripped.append(tag)
    if tripped:
        _raise_alarm("ILK_LT101",
                     "SAFETY INTERLOCK: LT_101 raw oil storage LOW - heater(s) %s tripped" % ", ".join(tripped),
                     "CRITICAL")
        _log("Interlock trip: heater inhibited (LT_101 LOW)", "CRITICAL")


def _update_inputs() -> None:
    st = STATE
    a = st["analog"]
    st["inputs"]["LT_101"]["state"] = a["TK_101_LEVEL"] >= LT_101_LOW_THRESHOLD
    st["inputs"]["LT_102"]["state"] = a["TK_108_LEVEL"] >= LT_102_LOW_THRESHOLD


def _update_process(dt: float) -> None:
    st = STATE
    o = st["outputs"]
    a = st["analog"]

    def transfer(src, dst, rate, ratio=1.0):
        moved = min(a[src], rate * dt)
        a[src] = _clamp(a[src] - moved, 0.0, 100.0)
        a[dst] = _clamp(a[dst] + moved * ratio, 0.0, 100.0)
        return moved

    flow = 0.0
    if o["P_101"]:
        transfer("TK_101_LEVEL", "TK_103_LEVEL", 0.55, 0.90); flow += 18.0
    if o["P_102"]:
        transfer("TK_102_LEVEL", "TK_103_LEVEL", 0.12, 0.98)
        transfer("TK_103_LEVEL", "TK_102_LEVEL", 0.10, 0.98); flow += 12.0
    if o["P_103"]:
        transfer("TK_103_LEVEL", "TK_104_LEVEL", 0.45, 0.96)
        a["TK_105_LEVEL"] = _clamp(a["TK_105_LEVEL"] - 0.10 * dt, 0.0, 100.0); flow += 16.0
    if o["P_104"]:
        transfer("TK_106_LEVEL", "TK_108_LEVEL", 0.50, 0.95); flow += 14.0
    if o["P_105"]:
        transfer("TK_104_LEVEL", "TK_108_LEVEL", 0.40, 0.92); flow += 15.0
    if o["P_106"]:
        transfer("TK_108_LEVEL", "TK_110_LEVEL", 0.45, 1.0); flow += 13.0
    if o["P_107"]:
        transfer("TK_108_LEVEL", "TK_111_LEVEL", 0.35, 0.97); flow += 11.0
    if o["P_108"]:
        transfer("TK_106_LEVEL", "TK_110_LEVEL", 0.30, 1.0); flow += 10.0
    if o["XV_104"]:
        transfer("TK_104_LEVEL", "TK_107_LEVEL", 0.25, 0.85)
    if o["XV_106"]:
        transfer("TK_106_LEVEL", "TK_108_LEVEL", 0.30, 0.95); flow += 8.0
    if o["XV_107"]:
        transfer("TK_108_LEVEL", "TK_110_LEVEL", 0.50, 1.0); flow += 9.0
    a["TK_110_LEVEL"] = _clamp(a["TK_110_LEVEL"] - 0.15 * dt, 0.0, 100.0)
    if a["TK_101_LEVEL"] < 15.0 and not o["P_101"]:
        a["TK_101_LEVEL"] = _clamp(a["TK_101_LEVEL"] + 0.40 * dt, 0.0, 85.0)
    for tag, cfg in THERMAL.items():
        current = a[tag]
        if cfg["heat"] and o.get(cfg["heat"]):
            current += cfg["rate"] * dt
        else:
            current -= (current - cfg["ambient"]) * 0.0009 * dt
        current += random.uniform(-0.03, 0.03)
        a[tag] = _clamp(current, -10.0, cfg["max"])
    a["FLOW"] = _clamp(a["FLOW"] + (flow - a["FLOW"]) * min(1.0, dt * 2.0) + random.uniform(-0.6, 0.6), 0.0, 40.0)
    a["PRESSURE"] = _clamp(1.2 + 0.05 * (a["FLOW"] / 10.0) + random.uniform(-0.05, 0.05), 0.5, 8.0)
    a["PH"] = _clamp(a["PH"] + random.uniform(-0.012, 0.012), 6.2, 7.6)


def _update_connection() -> None:
    conn = STATE["connection"]
    conn["latency_ms"] = int(_clamp(conn["latency_ms"] + random.uniform(-4, 4), 108, 142))


def _tick(dt: float) -> None:
    st = STATE
    if st["estop"]:
        st["sequence"]["running"] = False
        for tag in st["outputs"]:
            st["outputs"][tag] = False
        st["manual"].clear()
    else:
        _run_sequence(dt)
        _apply_manual_overrides()
        _apply_interlocks()
    _update_inputs()
    _update_process(dt)
    _update_connection()


def _simulation_loop() -> None:
    last = time.time()
    while True:
        time.sleep(0.5)
        now = time.time()
        dt = min(now - last, 2.0)
        last = now
        try:
            with LOCK:
                _tick(dt)
        except Exception as exc:
            app.logger.exception("Simulation tick failed: %s", exc)


def _snapshot() -> dict:
    st = STATE
    seq = st["sequence"]
    a = st["analog"]
    step_no = seq["step"]
    step = STEPS[step_no - 1] if 1 <= step_no <= len(STEPS) else None
    if step:
        duration = float(step["duration"])
        elapsed = min(float(seq["elapsed"]), duration)
        remaining = max(0.0, duration - elapsed)
        progress = round(elapsed / duration * 100.0, 1) if duration else 0.0
        step_name = step["name"]
    else:
        duration = elapsed = remaining = 0.0
        progress = 0.0
        step_name = "IDLE"
    outputs = {tag: {"addr": cfg["addr"], "name": cfg["name"], "group": cfg["group"],
                     "screen": cfg["screen"], "state": bool(st["outputs"][tag]),
                     "forced": tag in st["manual"]} for tag, cfg in OUTPUTS.items()}
    inputs = {tag: {"addr": cfg["addr"], "name": cfg["name"], "state": bool(st["inputs"][tag]["state"])}
              for tag, cfg in INPUTS.items()}
    analog = {tag: {"name": cfg["name"], "unit": cfg["unit"], "min": cfg["min"],
                    "max": cfg["max"], "value": round(a[tag], 2)} for tag, cfg in ANALOG.items()}
    steps = []
    for s in STEPS:
        status = "pending"
        if step_no != 0:
            if s["id"] < step_no: status = "done"
            elif s["id"] == step_no: status = "active"
        steps.append({"id": s["id"], "name": s["name"], "duration": s["duration"],
                      "description": s["description"], "status": status})
    kpis = {
        "batch": seq["batch"], "cycles": seq["cycles"],
        "active_step": step_no, "step_name": step_name,
        "progress": progress, "remaining": round(remaining, 1),
        "uptime_s": int(time.time() - st["started_at"]),
        "feed_level": round(a["TK_101_LEVEL"], 1),
        "reactor_temp": round(a["TK_103_TEMP"], 1),
        "reactor_level": round(a["TK_103_LEVEL"], 1),
        "product_level": round(a["TK_111_LEVEL"], 1),
        "glycerine_level": round(a["TK_107_LEVEL"], 1),
        "water_temp": round(a["TK_106_TEMP"], 1),
        "dry_temp": round(a["TK_108_TEMP"], 1),
        "flow": round(a["FLOW"], 1),
        "pressure": round(a["PRESSURE"], 2),
        "ph": round(a["PH"], 2),
        "estop": st["estop"],
        "alarm_count": len(st["alarms"]),
        "outputs_on": sum(1 for v in st["outputs"].values() if v),
    }
    return {
        "ts": _now_iso(),
        "plc": {
            "model": "Siemens S7-200 CPU 224",
            "protocol": st["connection"]["protocol"],
            "ip": st["connection"]["ip"],
            "port": st["connection"]["port"],
            "latency_ms": st["connection"]["latency_ms"],
            "vpn_role": st["connection"]["vpn_role"],
            "online": st["connection"]["online"],
        },
        "estop": st["estop"],
        "mode": st["mode"],
        "sequence": {
            "step": step_no, "name": step_name, "running": bool(seq["running"]),
            "elapsed": round(elapsed, 1), "duration": duration,
            "remaining": round(remaining, 1), "progress": progress,
            "batch": seq["batch"], "cycles": seq["cycles"], "total_steps": len(STEPS),
        },
        "inputs": inputs,
        "outputs": outputs,
        "analog": analog,
        "steps": steps,
        "alarms": st["alarms"][:20],
        "events": st["events"][:20],
        "kpis": kpis,
    }


# ==============================================================================
# 9. ROUTES
# ==============================================================================
@app.after_request
def _no_cache(response):
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        response.headers["Pragma"] = "no-cache"
    return response


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/telemetry", methods=["GET"])
def api_telemetry():
    with LOCK:
        return jsonify(_snapshot())


@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify({"ok": True, "service": "biodiesel-hmi", "ts": _now_iso()})


@app.route("/api/command", methods=["POST"])
def api_command():
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).strip().upper()
    if not action:
        return jsonify({"ok": False, "message": "Missing 'action' field"}), 400

    with LOCK:
        st = STATE
        seq = st["sequence"]

        if action == "EMERGENCY_STOP":
            st["estop"] = True
            seq["running"] = False
            st["manual"].clear()
            for tag in st["outputs"]:
                st["outputs"][tag] = False
            _raise_alarm("ESTOP", "EMERGENCY STOP ACTIVATED - all outputs de-energised", "CRITICAL")
            _log("EMERGENCY STOP pressed by remote operator", "CRITICAL")
            return jsonify({"ok": True, "message": "EMERGENCY STOP executed - all outputs cleared",
                            "state": _snapshot()})

        if action == "RESET_ESTOP":
            st["estop"] = False
            _clear_alarm("ESTOP")
            _clear_alarm("ILK_LT101")
            try:
                _enter_step(0)
            except ValueError:
                pass
            _log("Emergency stop reset - plant in IDLE", "INFO")
            return jsonify({"ok": True, "message": "E-Stop reset. Plant is IDLE.",
                            "state": _snapshot()})

        if st["estop"]:
            return jsonify({"ok": False,
                            "message": "Command rejected - EMERGENCY STOP is latched. Reset first."}), 409

        if action in ("TOGGLE_OUTPUT", "SET_OUTPUT"):
            tag = str(payload.get("tag", "")).strip().upper()
            if tag not in OUTPUTS:
                return jsonify({"ok": False, "message": "Unknown output tag '%s'" % tag}), 400
            desired = not st["outputs"][tag] if action == "TOGGLE_OUTPUT" else bool(payload.get("value", False))
            if desired and OUTPUTS[tag]["group"] == "heater" and not st["inputs"]["LT_101"]["state"]:
                _raise_alarm("ILK_LT101",
                             "Heater %s rejected - LT_101 raw oil storage is LOW" % tag, "CRITICAL")
                return jsonify({"ok": False,
                                "message": "SAFETY INTERLOCK: heater %s rejected - LT_101 (Raw Oil Storage) is LOW (%.1f%%)" % (tag, st["analog"]["TK_101_LEVEL"]),
                                "state": _snapshot()}), 409
            st["manual"][tag] = desired
            st["outputs"][tag] = desired
            _log("Manual %s -> %s (%s)" % (tag, "ON" if desired else "OFF", OUTPUTS[tag]["addr"]), "INFO")
            return jsonify({"ok": True,
                            "message": "%s (%s) forced %s" % (tag, OUTPUTS[tag]["addr"], "ON" if desired else "OFF"),
                            "state": _snapshot()})

        if action == "SET_STEP":
            try:
                target = int(payload.get("step", 0))
            except (TypeError, ValueError):
                return jsonify({"ok": False, "message": "Invalid step number"}), 400
            try:
                _enter_step(target)
            except ValueError as exc:
                return jsonify({"ok": False, "message": str(exc)}), 400
            return jsonify({"ok": True,
                            "message": "SFC forced to %s" % ("IDLE" if target == 0 else "STEP %d" % target),
                            "state": _snapshot()})

        if action == "START_SEQUENCE":
            if seq["step"] == 0:
                _enter_step(1)
            else:
                seq["running"] = True
            _log("Sequence START command accepted", "SUCCESS")
            return jsonify({"ok": True, "message": "Sequence started", "state": _snapshot()})

        if action == "STOP_SEQUENCE":
            seq["running"] = False
            st["manual"].clear()
            for tag in st["outputs"]:
                st["outputs"][tag] = False
            _log("Sequence STOP command accepted - outputs cleared", "WARNING")
            return jsonify({"ok": True, "message": "Sequence stopped - outputs cleared",
                            "state": _snapshot()})

        if action == "ACK_ALARMS":
            st["alarms"] = []
            _log("Alarms acknowledged by operator", "INFO")
            return jsonify({"ok": True, "message": "Alarms acknowledged", "state": _snapshot()})

        return jsonify({"ok": False, "message": "Unknown action '%s'" % action}), 400


if __name__ == "__main__":
    _sim = threading.Thread(target=_simulation_loop, name="plc-sim", daemon=True)
    _sim.start()
    _log("HMI server started - S7-200 PPI link @ 192.168.1.100", "SUCCESS")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)