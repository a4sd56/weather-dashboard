import os
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import requests
import serial
import urllib3
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


# ---------------------------
# DB 모델
# ---------------------------
class WeatherReading(db.Model):
    __table_args__ = (db.UniqueConstraint("source", "timestamp", name="uniq_source_ts"),)
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(10), nullable=False)  # Arduino, KMA
    temperature = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    timestamp = db.Column(db.DateTime(timezone=True), nullable=False)


# ---------------------------
# 유틸 함수
# ---------------------------
def ensure_kst(dt):
    return dt.astimezone(KST) if dt.tzinfo else dt.replace(tzinfo=KST)

def to_minute(dt):
    dt = ensure_kst(dt)
    return dt.replace(second=0, microsecond=0)

def minute_range(start, end):
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(minutes=1)
    return out


# ---------------------------
# AWS KMA (1분 단위 데이터 활용)
# ---------------------------
KMA_AWS_URL = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min"

def conv(v):
    """AWS 결측값 전부 None 처리"""
    if v is None:
        return None
    v = v.strip()
    if v in ["", "-99", "-99.0", "-99.9", "-999", "-999.0", "-999.9"]:
        return None
    try:
        return float(v)
    except:
        return None


def fetch_kma_aws():
    key = os.getenv("KMA_AUTH_KEY")
    if not key:
        return

    now = ensure_kst(datetime.now())
    params = {
        "authKey": key,
        "tm1": now.strftime("%Y%m%d0000"),
        "tm2": now.strftime("%Y%m%d2300"),
        "stn": "159",
        "disp": "0",
        "help": "0",
    }

    try:
        urllib3.disable_warnings()
        r = requests.get(KMA_AWS_URL, params=params, timeout=20, verify=False)
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

        bucket = {}

        for row in data:
            p = row.split()
            ts = datetime.strptime(p[ti], "%Y%m%d%H%M").replace(tzinfo=KST)

            t = conv(p[ta])
            hmd = conv(p[hm])
            pres = conv(p[pa])

            bucket[to_minute(ts)] = {"temperature": t, "humidity": hmd, "pressure": pres}

        with app.app_context():
            for ts, vals in bucket.items():
                row = WeatherReading.query.filter_by(source="KMA", timestamp=ts).first()
                if row:
                    if vals["temperature"] is not None:
                        row.temperature = vals["temperature"]
                    if vals["humidity"] is not None:
                        row.humidity = vals["humidity"]
                    if vals["pressure"] is not None:
                        row.pressure = vals["pressure"]
                else:
                    db.session.add(
                        WeatherReading(
                            source="KMA",
                            temperature=vals["temperature"],
                            humidity=vals["humidity"],
                            pressure=vals["pressure"],
                            timestamp=ts,
                        )
                    )
            db.session.commit()

    except Exception as e:
        print("[AWS ERROR]", e)


def aws_thread():
    fetch_kma_aws()
    while True:
        time.sleep(60)  # 1분마다 시도
        fetch_kma_aws()


# ---------------------------
# Arduino Thread
# ---------------------------
def arduino_thread():
    port = os.getenv("ARDUINO_PORT", "COM3")
    baud = int(os.getenv("ARDUINO_BAUD", "9600"))
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

    threading.Thread(target=arduino_thread, daemon=True).start()
    threading.Thread(target=aws_thread, daemon=True).start()

    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
