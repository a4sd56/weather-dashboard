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
# KMA 초단기 T,H 10분
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
# KMA ASOS 1시간 T,H,P
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
                        rec = WeatherReading(
                            source="Arduino",
                            temperature=t,
                            humidity=h,
                            pressure=p,
                            timestamp=datetime.now(KST)
                        )
                        db.session.add(rec)
                        db.session.commit()
                        print(f"[Arduino] 저장 완료: T={t}, H={h}, P={p}")
        except Exception as e:
            print(f"[WARN] 시리얼 읽기 오류: {e}")
        time.sleep(5)

# --- 메인 실행 ---
if __name__ == "__main__":
    with app.app_context():
        db.create_all()

    SERVER_START_KST = ensure_kst(datetime.now())

    threading.Thread(target=arduino_thread, daemon=True).start()
    threading.Thread(target=kma_ultra_thread, daemon=True).start()
    threading.Thread(target=asos_thread, daemon=True).start()

    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
