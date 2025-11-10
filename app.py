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

# .env 로드  (.env 없으면 OS 환경변수 사용)
load_dotenv(dotenv_path=os.path.join(basedir, ".env"), override=False)

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(basedir, "weather.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

KST = timezone(timedelta(hours=9))
SERVER_START_KST = None  # 서버 실행 세션 시작 시각 KST aware

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
# 유틸리티
# ---------------------------
def env(key: str, default: str = "") -> str:
    v = os.environ.get(key)
    return v if v not in [None, ""] else default

def ensure_kst_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KST)
    return dt.astimezone(KST)

def to_minute(dt: datetime) -> datetime:
    return ensure_kst_aware(dt).replace(second=0, microsecond=0)

def to_10min_floor(dt: datetime) -> datetime:
    dt = ensure_kst_aware(dt)
    m = dt.minute - (dt.minute % 10)
    return dt.replace(minute=m, second=0, microsecond=0)

def to_10min_ceil(dt: datetime) -> datetime:
    dt = ensure_kst_aware(dt)
    m = ((dt.minute + 9) // 10) * 10
    if m == 60:
        dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        dt = dt.replace(minute=m, second=0, microsecond=0)
    return dt

def ten_min_range(start_dt: datetime, end_dt: datetime):
    start_dt = ensure_kst_aware(start_dt)
    end_dt = ensure_kst_aware(end_dt)
    cur = to_10min_floor(start_dt)
    end_dt = to_10min_floor(end_dt)
    out = []
    while cur <= end_dt:
        out.append(cur)
        cur += timedelta(minutes=10)
    return out

def to_kst_naive(dt: datetime) -> datetime:
    return ensure_kst_aware(dt).replace(tzinfo=None)

def kst_naive_10min_floor(dt: datetime) -> datetime:
    dt = ensure_kst_aware(dt)
    m = dt.minute - (dt.minute % 10)
    return dt.replace(minute=m, second=0, microsecond=0).replace(tzinfo=None)

def kst_naive_10min_ceil(dt: datetime) -> datetime:
    dt = ensure_kst_aware(dt)
    m = ((dt.minute + 9) // 10) * 10
    if m == 60:
        dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        dt = dt.replace(minute=m, second=0, microsecond=0)
    return dt.replace(tzinfo=None)

def linear_fill(ts_to_val: dict, timeline: list[datetime]):
    if not ts_to_val:
        return [None] * len(timeline)

    norm_map = {to_kst_naive(k): v for k, v in ts_to_val.items()}
    tl = [to_kst_naive(t) for t in timeline]
    keys = sorted(norm_map.keys())
    if not keys:
        return [None] * len(tl)

    out = []
    i = 0
    for t in tl:
        while i + 1 < len(keys) and keys[i + 1] <= t:
            i += 1
        if t <= keys[0]:
            out.append(norm_map[keys[0]]); continue
        if t >= keys[-1]:
            out.append(norm_map[keys[-1]]); continue

        left = keys[i]; right = keys[i + 1]
        v1 = norm_map.get(left); v2 = norm_map.get(right)
        if v1 is None or v2 is None:
            out.append(None); continue
        span = (right - left).total_seconds()
        prog = (t - left).total_seconds() / span if span > 0 else 0
        out.append(round(v1 + prog * (v2 - v1), 2))
    return out

# ---------------------------
# KMA 초단기 실황 10분 T/H  fallback 포함 안정화 버전
# ---------------------------
KMA_ULTRA_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

def _requests_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

def fetch_kma_ultra_once():
    # 따옴표/공백/URL 인코딩까지 정리
    raw_key = env("KMA_SERVICE_KEY")
    service_key = unquote(raw_key).strip().strip('"').strip("'")
    nx = int(env("KMA_NX", "98"))
    ny = int(env("KMA_NY", "76"))
    if not service_key:
        print("[KMA10] KMA_SERVICE_KEY 미설정  수집 건너뜀")
        return 0

    now_kst = ensure_kst_aware(datetime.now())
    base = to_10min_floor(now_kst - timedelta(minutes=1))

    # 최근 1시간 범위에서 10분씩 뒤로가며 가장 가까운 유효 슬롯을 사용
    candidate_bases = [base - timedelta(minutes=10 * i) for i in range(0, 6)]

    session = _requests_session()
    updated_total = 0

    for base_dt in candidate_bases:
        params = {
            "serviceKey": service_key,
            "numOfRows": 60,
            "pageNo": 1,
            "dataType": "JSON",
            "base_date": base_dt.strftime("%Y%m%d"),
            "base_time": base_dt.strftime("%H%M"),
            "nx": nx,
            "ny": ny,
        }
        try:
            r = session.get(KMA_ULTRA_URL, params=params, timeout=(5, 30))
            r.raise_for_status()
            j = r.json()
            header = j.get("response", {}).get("header", {})
            code = header.get("resultCode")
            msg = header.get("resultMsg")

            # 03 NO_DATA 이면 바로 이전 슬롯으로 폴백
            if code not in [None, "00"]:
                if code == "03":
                    print(f"[KMA10] NO_DATA at {base_dt.strftime('%H:%M')}, fallback")
                    continue
                print(f"[KMA10] API error code={code} msg={msg} at {base_dt.strftime('%H:%M')}")
                continue

            items = j.get("response", {}).get("body", {}).get("items", {}).get("item", [])
            if not items:
                print(f"[KMA10] empty items at {base_dt.strftime('%H:%M')}, fallback")
                continue

            bucket = {}
            for it in items:
                bdate, btime = it.get("baseDate"), it.get("baseTime")
                if not bdate or not btime:
                    continue
                ts = datetime.strptime(bdate + btime, "%Y%m%d%H%M").replace(tzinfo=KST)
                bucket.setdefault(ts, {"T1H": None, "REH": None})
                cat, val = it.get("category"), it.get("obsrValue")
                if cat in ("T1H", "REH"):
                    try:
                        bucket[ts][cat] = float(val)
                    except:
                        pass

            with app.app_context():
                updated = 0
                for ts, vals in bucket.items():
                    t, h = vals["T1H"], vals["REH"]
                    if t is None and h is None:
                        continue
                    row = WeatherReading.query.filter_by(source="KMA10", timestamp=ts).first()
                    if row:
                        if t is not None: row.temperature = t
                        if h is not None: row.humidity = h
                        db.session.commit()
                    else:
                        db.session.add(WeatherReading(
                            source="KMA10", temperature=t, humidity=h, pressure=None, timestamp=ts
                        ))
                        db.session.commit()
                    updated += 1
            print(f"[KMA10] +{updated} at {base_dt.strftime('%H:%M')}  picked")
            updated_total += updated
            break  # 가장 가까운 유효 슬롯을 넣었으면 종료

        except Exception as e:
            print(f"[KMA10] fail {e} base={base_dt.strftime('%H:%M')}")
            continue

    return updated_total

def kma_ultra_scheduler():
    print("[KMA10] scheduler start")
    fetch_kma_ultra_once()
    time.sleep(3)
    fetch_kma_ultra_once()
    while True:
        now = ensure_kst_aware(datetime.now())
        wait = 600 - ((now.minute % 10) * 60 + now.second)
        print(f"[KMA10] next fetch in {wait} sec")
        time.sleep(max(5, wait))
        fetch_kma_ultra_once()

# ---------------------------
# KMA ASOS 1시간 T/H/P  키 없으면 패스
# ---------------------------
KMA_ASOS_URL = "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"

def fetch_kma_asos_today():
    key = env("KMA_AUTH_KEY")
    if not key:
        print("[KMA] KMA_AUTH_KEY 미설정  pass")
        return 0

    now_kst = ensure_kst_aware(datetime.now())
    params = {
        "authKey": key,
        "tm1": now_kst.strftime("%Y%m%d0000"),
        "tm2": now_kst.strftime("%Y%m%d2300"),
        "stn": "159",  # 부산
        "help": "0",
    }
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = requests.get(KMA_ASOS_URL, params=params, timeout=25, verify=False)
        resp.raise_for_status()
        lines = resp.text.strip().splitlines()

        header_line, data_lines = None, []
        for line in lines:
            s = line.strip()
            if s.startswith("# YYMMDDHHMI"):
                header_line = s.replace("#", "").strip()
            elif s and not s.startswith("#"):
                data_lines.append(s)
        if not header_line or not data_lines:
            return 0

        headers = header_line.split()
        idx = {h: i for i, h in enumerate(headers)}
        stn_i, ta_i, hm_i, pa_i = idx["STN"], idx["TA"], idx["HM"], idx["PA"]

        def f(v):
            return None if v in ["", "-999.0", "-999"] else float(v)

        updated = 0
        with app.app_context():
            for ln in data_lines:
                p = ln.split()
                if p[stn_i] != "159":
                    continue
                ts = datetime.strptime(p[0], "%Y%m%d%H%M").replace(tzinfo=KST)
                ta, hm, pa = f(p[ta_i]), f(p[hm_i]), f(p[pa_i])

                row = WeatherReading.query.filter_by(source="KMA", timestamp=ts).first()
                if row:
                    if ta is not None: row.temperature = ta
                    if hm is not None: row.humidity = hm
                    if pa is not None: row.pressure = pa
                    db.session.commit()
                else:
                    db.session.add(WeatherReading(
                        source="KMA", temperature=ta, humidity=hm, pressure=pa, timestamp=ts
                    ))
                    db.session.commit()
                updated += 1
        print(f"[KMA] +{updated} hourly")
        return updated
    except Exception as e:
        print(f"[KMA] fail {e}")
        return 0

def kma_asos_hourly_scheduler():
    fetch_kma_asos_today()
    while True:
        now = ensure_kst_aware(datetime.now())
        wait = 3600 - (now.minute * 60 + now.second)
        time.sleep(max(5, wait))
        fetch_kma_asos_today()

# ---------------------------
# Arduino 수집 스레드
# ---------------------------
def read_from_arduino():
    port = env("ARDUINO_PORT", "COM3")
    baud = int(env("ARDUINO_BAUD", "9600"))
    last_min = None
    ser = None

    print(f"[Arduino] trying port={port} baud={baud}")

    while True:
        try:
            ser = serial.Serial(port, baud, timeout=2)
            print(f"[Arduino] connected {port}")
            while True:
                line = ser.readline().decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                vals = line.split(",")
                if len(vals) < 2:
                    continue
                try:
                    t = float(vals[0]); h = float(vals[1])
                    p = float(vals[2]) if len(vals) >= 3 else None
                except:
                    continue

                minute = to_minute(datetime.now())
                if minute != last_min:
                    with app.app_context():
                        try:
                            db.session.add(WeatherReading(
                                source="Arduino", temperature=t, humidity=h, pressure=p, timestamp=minute
                            ))
                            db.session.commit()
                            print(f"[Arduino] {minute.astimezone(KST).strftime('%H:%M')}  T={t} H={h} P={p}")
                            last_min = minute
                        except IntegrityError:
                            db.session.rollback()
        except Exception as e:
            print(f"[Arduino] retry {e}")
            time.sleep(5)
        finally:
            try:
                if ser and ser.is_open:
                    ser.close()
            except:
                pass

# ---------------------------
# Web 라우트
# ---------------------------
@app.route("/")
def dashboard():
    now = ensure_kst_aware(datetime.now())
    return render_template("index.html", display_date=now.strftime("%Y-%m-%d %H:%M"))

def pick_kma(start_aw: datetime, end_aw: datetime, field: str):
    # pressure는 ASOS만
    if field == "pressure":
        return WeatherReading.query.filter(
            WeatherReading.source == "KMA",
            WeatherReading.timestamp.between(start_aw, end_aw)
        ).order_by(WeatherReading.timestamp).all()

    # T/H는 초단기(KMA10) 우선, 없으면 ASOS
    k10 = WeatherReading.query.filter(
        WeatherReading.source == "KMA10",
        WeatherReading.timestamp.between(start_aw, end_aw)
    ).order_by(WeatherReading.timestamp).all()

    k = WeatherReading.query.filter(
        WeatherReading.source == "KMA",
        WeatherReading.timestamp.between(start_aw, end_aw)
    ).order_by(WeatherReading.timestamp).all()

    by_ts = {}
    for r in k: by_ts.setdefault(r.timestamp, r)
    for r in k10: by_ts[r.timestamp] = r  # 초단기 우선
    return [by_ts[t] for t in sorted(by_ts.keys())]

@app.route("/api/chart_data/<cat>")
def get_chart_data(cat):
    # 그래프에서는 초단기 실황만 사용  기압 그래프 비활성화
    if cat == "pressure":
        return jsonify({"error": "pressure graph disabled"}), 404

    field = {"temperature": "temperature", "humidity": "humidity"}.get(cat)
    if not field:
        return jsonify({"error": "bad field"}), 404

    now = ensure_kst_aware(datetime.now())
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # 세션 시작 이후의 첫 아두이노 레코드
    session_start = SERVER_START_KST or day_start
    first_arduino = WeatherReading.query.filter(
        WeatherReading.source == "Arduino",
        WeatherReading.timestamp >= session_start,
        WeatherReading.timestamp <= now
    ).order_by(WeatherReading.timestamp.asc()).first()

    if first_arduino:
        start_aw = to_10min_floor(first_arduino.timestamp)      # 내림
        start_naive = kst_naive_10min_floor(first_arduino.timestamp)
        # 라벨 최소 2개 보장
        if now - start_aw < timedelta(minutes=10):
            start_aw = start_aw - timedelta(minutes=10)
    else:
        base = max(session_start, now - timedelta(hours=1))
        start_aw = to_10min_floor(base)
        start_naive = kst_naive_10min_floor(base)

    # 라벨 생성
    labels_aw = ten_min_range(start_aw, now)
    labels_dt = [to_kst_naive(t) for t in labels_aw]
    labels = [t.strftime("%H:%M") for t in labels_aw]
    label_set = set(labels_dt)

    # KMA 값  초단기 실황만 사용
    kma_query_start = start_aw - timedelta(hours=2)
    kma_recs = WeatherReading.query.filter(
        WeatherReading.source == "KMA10",
        WeatherReading.timestamp.between(kma_query_start, now)
    ).order_by(WeatherReading.timestamp).all()

    kma_map = {}
    for r in kma_recs:
        v = getattr(r, field)
        if v is not None:
            kma_map[kst_naive_10min_floor(r.timestamp)] = v
    kma_vals = linear_fill(kma_map, labels_dt)

    # Arduino 값  10분 슬롯으로 버킷팅
    ard_recs = WeatherReading.query.filter(
        WeatherReading.source == "Arduino",
        WeatherReading.timestamp.between(start_aw, now)
    ).order_by(WeatherReading.timestamp).all()

    bucket = {}
    for r in ard_recs:
        slot = kst_naive_10min_floor(r.timestamp)
        if slot in label_set:
            bucket[slot] = getattr(r, field)
    ard_vals = [bucket.get(t) for t in labels_dt]

    return jsonify({"labels": labels, "kma_values": kma_vals, "arduino_values": ard_vals})

@app.route("/api/latest-data")
def get_latest_data():
    now = ensure_kst_aware(datetime.now())
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    kma = WeatherReading.query.filter(
        WeatherReading.source.in_(["KMA10", "KMA"]),
        WeatherReading.timestamp >= start
    ).order_by(WeatherReading.timestamp.desc()).first()

    ard = WeatherReading.query.filter_by(source="Arduino").order_by(
        WeatherReading.timestamp.desc()
    ).first()

    return jsonify({
        "display_date": now.strftime("%Y-%m-%d %H:%M"),
        "kma": {
            "temperature": kma.temperature if kma else None,
            "humidity": kma.humidity if kma else None,
            "pressure": kma.pressure if kma else None
        },
        "arduino": {
            "temperature": ard.temperature if ard else None,
            "humidity": ard.humidity if ard else None,
            "pressure": ard.pressure if ard else None
        }
    })

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    with app.app_context():
        db.create_all()

    # 세션 시작 시각 기록
    SERVER_START_KST = ensure_kst_aware(datetime.now())

    # 스레드 시작  reloader 중복 방지 위해 use_reloader=False로 실행
    threading.Thread(target=read_from_arduino, daemon=True).start()
    threading.Thread(target=kma_ultra_scheduler, daemon=True).start()
    threading.Thread(target=kma_asos_hourly_scheduler, daemon=True).start()
    print("[MAIN] Threads started")

    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
