import os
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import requests
import serial
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

# ---------------------------
# 기본 설정
# ---------------------------
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

load_dotenv(dotenv_path=os.path.join(basedir, ".env"), override=False)

app.config["SQLALCHEMY_DATABASE_URI"] = (
    f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASS')}"
    f"@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}?charset=utf8mb4"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

KST = timezone(timedelta(hours=9))
SERVER_START_KST = None

# ---------------------------
# DB 모델
# ---------------------------
class WeatherReading(db.Model):
    __table_args__ = (db.UniqueConstraint("source", "timestamp", name="uniq_source_ts"),)
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(10), nullable=False)  # Arduino, KMA10, KMA
    temperature = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    timestamp = db.Column(db.DateTime(timezone=True), nullable=False)

# ---------------------------
# 유틸 함수
# ---------------------------
def env(key: str, default: str = "") -> str:
    v = os.environ.get(key)
    return v if v not in [None, ""] else default

def ensure_kst(dt):
    return dt.astimezone(KST) if dt.tzinfo else dt.replace(tzinfo=KST)

def to_minute(dt):
    dt = ensure_kst(dt)
    return dt.replace(second=0, microsecond=0)

def to_10min_floor(dt):
    dt = ensure_kst(dt)
    m = dt.minute - (dt.minute % 10)
    return dt.replace(minute=m, second=0, microsecond=0)

def ten_min_range(start, end):
    cur = to_10min_floor(start)
    end = to_10min_floor(end)
    out = []
    while cur <= end:
        out.append(cur)
        cur += timedelta(minutes=10)
    return out

def kst_floor_10(dt):
    dt = ensure_kst(dt)
    m = dt.minute - (dt.minute % 10)
    return dt.replace(minute=m, second=0, microsecond=0).replace(tzinfo=None)

def linear_fill(src, timeline):
    if not src:
        return [None] * len(timeline)
    keys = sorted(src.keys())
    res = []
    i = 0
    for t in timeline:
        while i + 1 < len(keys) and keys[i + 1] <= t:
            i += 1
        if t <= keys[0]:
            res.append(src[keys[0]])
            continue
        if t >= keys[-1]:
            res.append(src[keys[-1]])
            continue
        left, right = keys[i], keys[i + 1]
        v1, v2 = src[left], src[right]
        if v1 is None or v2 is None:
            res.append(None)
            continue
        span = (right - left).total_seconds()
        prog = (t - left).total_seconds() / span if span else 0
        res.append(round(v1 + prog * (v2 - v1), 2))
    return res

def mape_series(arduino_vals, kma_vals):
    out = []
    acc = 0.0
    cnt = 0
    for a, k in zip(arduino_vals, kma_vals):
        if a is None or k is None or k == 0:
            out.append(None)
        else:
            err = abs(a - k) / abs(k) * 100.0
            out.append(round(err, 2))
            acc += err
            cnt += 1
    avg = round(acc / cnt, 2) if cnt else None
    latest = next((v for v in reversed(out) if v is not None), None)
    return out, avg, latest

# ---------------------------
# KMA 초단기 T H 10분
# ---------------------------
KMA_ULTRA_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

def _session():
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def fetch_kma_ultra():
    raw_key = env("KMA_SERVICE_KEY")
    key = unquote(raw_key).strip().strip('"').strip("'")
    if not key:
        return
    nx = int(env("KMA_NX", "98"))
    ny = int(env("KMA_NY", "76"))

    now = ensure_kst(datetime.now())
    base = to_10min_floor(now - timedelta(minutes=1))
    candidates = [base - timedelta(minutes=10 * i) for i in range(6)]
    s = _session()

    for b in candidates:
        p = {
            "serviceKey": key,
            "numOfRows": 60,
            "pageNo": 1,
            "dataType": "JSON",
            "base_date": b.strftime("%Y%m%d"),
            "base_time": b.strftime("%H%M"),
            "nx": nx,
            "ny": ny,
        }
        try:
            r = s.get(KMA_ULTRA_URL, params=p, timeout=(5, 30))
            j = r.json()
            items = j.get("response", {}).get("body", {}).get("items", {}).get("item", [])
            if not items:
                continue

            bucket = {}
            for it in items:
                bdate, btime = it.get("baseDate"), it.get("baseTime")
                ts = datetime.strptime(bdate + btime, "%Y%m%d%H%M").replace(tzinfo=KST)
                bucket.setdefault(ts, {"T1H": None, "REH": None})
                cat, val = it.get("category"), it.get("obsrValue")
                if cat in ("T1H", "REH"):
                    try:
                        bucket[ts][cat] = float(val)
                    except:
                        pass

            with app.app_context():
                for ts, v in bucket.items():
                    t, h = v["T1H"], v["REH"]
                    if t is None and h is None:
                        continue
                    row = WeatherReading.query.filter_by(source="KMA10", timestamp=ts).first()
                    if row:
                        if t is not None:
                            row.temperature = t
                        if h is not None:
                            row.humidity = h
                    else:
                        db.session.add(
                            WeatherReading(
                                source="KMA10", temperature=t, humidity=h, pressure=None, timestamp=ts
                            )
                        )
                    db.session.commit()
            break
        except:
            pass

def kma_ultra_thread():
    fetch_kma_ultra()
    time.sleep(3)
    fetch_kma_ultra()
    while True:
        now = ensure_kst(datetime.now())
        wait = 600 - ((now.minute % 10) * 60 + now.second)
        time.sleep(max(5, wait))
        fetch_kma_ultra()

# ---------------------------
# KMA ASOS 1시간 T H P
# ---------------------------
KMA_ASOS_URL = "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"

def fetch_kma_asos():
    key = env("KMA_AUTH_KEY")
    if not key:
        return
    now = ensure_kst(datetime.now())
    params = {
        "authKey": key,
        "tm1": now.strftime("%Y%m%d0000"),
        "tm2": now.strftime("%Y%m%d2300"),
        "stn": "159",
        "help": "0",
    }
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.get(KMA_ASOS_URL, params=params, timeout=25, verify=False)
        lines = r.text.strip().splitlines()
        header = None
        data = []
        for ln in lines:
            s = ln.strip()
            if s.startswith("# YYMMDDHHMI"):
                header = s.replace("#", "").strip()
            elif s and not s.startswith("#"):
                data.append(s)
        if not header or not data:
            return
        h = header.split()
        idx = {h[i]: i for i in range(len(h))}
        ti, ta, hm, pa = idx["YYMMDDHHMI"], idx["TA"], idx["HM"], idx["PA"]

        def conv(v):
            return None if v in ["", "-999.0", "-999"] else float(v)

        with app.app_context():
            for row in data:
                p = row.split()
                ts = datetime.strptime(p[ti], "%Y%m%d%H%M").replace(tzinfo=KST)
                t, hmd, pres = conv(p[ta]), conv(p[hm]), conv(p[pa])
                r2 = WeatherReading.query.filter_by(source="KMA", timestamp=ts).first()
                if r2:
                    if t is not None:
                        r2.temperature = t
                    if hmd is not None:
                        r2.humidity = hmd
                    if pres is not None:
                        r2.pressure = pres
                else:
                    db.session.add(
                        WeatherReading(source="KMA", temperature=t, humidity=hmd, pressure=pres, timestamp=ts)
                    )
                db.session.commit()
    except:
        pass

def asos_thread():
    fetch_kma_asos()
    while True:
        now = ensure_kst(datetime.now())
        wait = 3600 - (now.minute * 60 + now.second)
        time.sleep(max(5, wait))
        fetch_kma_asos()

# ---------------------------
# Arduino thread
# ---------------------------
def arduino_thread():
    port = env("ARDUINO_PORT", "COM3")
    baud = int(env("ARDUINO_BAUD", "9600"))
    last_min = None
    ser = None

    while True:
        try:
            ser = serial.Serial(port, baud, timeout=2)
            while True:
                line = ser.readline().decode("utf-8", "ignore").strip()
                if not line:
                    continue
                vals = line.split(",")
                if len(vals) < 2:
                    continue
                try:
                    t = float(vals[0])
                    h = float(vals[1])
                    p = float(vals[2]) if len(vals) >= 3 else None
                except:
                    continue

                minute = to_minute(datetime.now())
                if minute != last_min:
                    with app.app_context():
                        try:
                            db.session.add(
                                WeatherReading(
                                    source="Arduino",
                                    temperature=t,
                                    humidity=h,
                                    pressure=p,
                                    timestamp=minute,
                                )
                            )
                            db.session.commit()
                            last_min = minute
                        except IntegrityError:
                            db.session.rollback()
        except:
            time.sleep(5)
        finally:
            try:
                if ser and ser.is_open:
                    ser.close()
            except:
                pass

# ---------------------------
# 내부 쿼리 헬퍼
# ---------------------------
FIELD_MAP = {"temperature": "temperature", "humidity": "humidity", "pressure": "pressure"}

def compute_time_bounds_for_session(now_kst):
    day_start = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    session_start = SERVER_START_KST or day_start
    first = (
        WeatherReading.query.filter(
            WeatherReading.source == "Arduino",
            WeatherReading.timestamp >= session_start,
            WeatherReading.timestamp <= now_kst,
            )
        .order_by(WeatherReading.timestamp.asc())
        .first()
    )
    if first:
        start = to_10min_floor(first.timestamp)
        if now_kst - start < timedelta(minutes=10):
            start = start - timedelta(minutes=10)
    else:
        base = max(session_start, now_kst - timedelta(hours=1))
        start = to_10min_floor(base)
    return start, now_kst

def fetch_series_for_category(category, start, now_kst):
    field = FIELD_MAP.get(category)
    if not field:
        return None, None, None, None, None

    labels_aw = ten_min_range(start, now_kst)
    labels_dt = [t.replace(tzinfo=None) for t in labels_aw]
    labels = [t.strftime("%H:%M") for t in labels_aw]

    if category == "pressure":
        kma_recs = (
            WeatherReading.query.filter(
                WeatherReading.source == "KMA",
                WeatherReading.timestamp.between(start, now_kst),
                )
            .order_by(WeatherReading.timestamp)
            .all()
        )
    else:
        kma_recs = (
            WeatherReading.query.filter(
                WeatherReading.source == "KMA10",
                WeatherReading.timestamp.between(start - timedelta(hours=2), now_kst),
                )
            .order_by(WeatherReading.timestamp)
            .all()
        )

    kma_map = {}
    for r in kma_recs:
        v = getattr(r, field)
        if v is not None:
            slot = kst_floor_10(r.timestamp)
            kma_map[slot] = v
    kma_vals = linear_fill(kma_map, labels_dt)

    ard_recs = (
        WeatherReading.query.filter(
            WeatherReading.source == "Arduino",
            WeatherReading.timestamp.between(start, now_kst),
            )
        .order_by(WeatherReading.timestamp)
        .all()
    )
    bucket = {}
    for r in ard_recs:
        slot = kst_floor_10(r.timestamp)
        bucket[slot] = getattr(r, field)
    ard_vals = [bucket.get(t) for t in labels_dt]

    return labels, labels_dt, ard_vals, kma_vals, field

# ---------------------------
# Web
# ---------------------------
@app.route("/")
def dash():
    now = ensure_kst(datetime.now())
    return render_template("index.html", display_date=now.strftime("%Y-%m-%d %H:%M"))

@app.route("/api/chart_data/<cat>")
def chart(cat):
    now = ensure_kst(datetime.now())
    start, now_kst = compute_time_bounds_for_session(now)
    res = fetch_series_for_category(cat, start, now_kst)
    if not res:
        return jsonify({"error": "bad field"}), 404
    labels, _, ard_vals, kma_vals, _ = res
    return jsonify({"labels": labels, "kma_values": kma_vals, "arduino_values": ard_vals})

@app.route("/api/error_data/<cat>")
def error_data(cat):
    now = ensure_kst(datetime.now())
    start, now_kst = compute_time_bounds_for_session(now)
    res = fetch_series_for_category(cat, start, now_kst)
    if not res:
        return jsonify({"error": "bad field"}), 404
    labels, _, ard_vals, kma_vals, _ = res
    errors, avg, latest = mape_series(ard_vals, kma_vals)
    return jsonify(
        {"labels": labels, "errors": errors, "avg_error": avg, "latest_error": latest, "unit": "%"}
    )

# 새 엔드포인트 추가
@app.route("/api/error_data_all")
def error_data_all():
    now = ensure_kst(datetime.now())
    start, now_kst = compute_time_bounds_for_session(now)

    # 온도
    labels, _, a_t, k_t, _ = fetch_series_for_category("temperature", start, now_kst)
    err_t, avg_t, latest_t = mape_series(a_t, k_t)

    # 습도
    _, _, a_h, k_h, _ = fetch_series_for_category("humidity", start, now_kst)
    err_h, avg_h, latest_h = mape_series(a_h, k_h)

    # 기압
    _, _, a_p, k_p, _ = fetch_series_for_category("pressure", start, now_kst)
    err_p, avg_p, latest_p = mape_series(a_p, k_p)

    return jsonify(
        {
            "labels": labels,
            "temperature": {"errors": err_t, "avg": avg_t, "latest": latest_t},
            "humidity": {"errors": err_h, "avg": avg_h, "latest": latest_h},
            "pressure": {"errors": err_p, "avg": avg_p, "latest": latest_p},
            "unit": "%"
        }
    )

@app.route("/api/latest-data")
def latest():
    now = ensure_kst(datetime.now())
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    kma = (
        WeatherReading.query.filter(
            WeatherReading.source.in_(["KMA10", "KMA"]), WeatherReading.timestamp >= start
        )
        .order_by(WeatherReading.timestamp.desc())
        .first()
    )

    ard = WeatherReading.query.filter_by(source="Arduino").order_by(WeatherReading.timestamp.desc()).first()

    return jsonify(
        {
            "display_date": now.strftime("%Y-%m-%d %H:%M"),
            "kma": {
                "temperature": kma.temperature if kma else None,
                "humidity": kma.humidity if kma else None,
                "pressure": kma.pressure if kma else None,
            },
            "arduino": {
                "temperature": ard.temperature if ard else None,
                "humidity": ard.humidity if ard else None,
                "pressure": ard.pressure if ard else None,
            },
        }
    )

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    with app.app_context():
        db.create_all()

    SERVER_START_KST = ensure_kst(datetime.now())

    threading.Thread(target=arduino_thread, daemon=True).start()
    threading.Thread(target=kma_ultra_thread, daemon=True).start()
    threading.Thread(target=asos_thread, daemon=True).start()

    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
