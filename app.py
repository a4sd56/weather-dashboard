import os
import threading
import serial
import requests
import time
from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta, timezone
from sqlalchemy.exc import IntegrityError
import urllib3

# --- 기본 설정 ---
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(basedir, "weather.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
KST = timezone(timedelta(hours=9))

# --- 데이터베이스 모델 ---
class WeatherReading(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(10), nullable=False)
    temperature = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    timestamp = db.Column(db.DateTime(timezone=True), nullable=False, unique=True)

# --- 기상청 데이터 업데이트 ---
def update_kma_data_in_db():
    with app.app_context():
        now_kst = datetime.now(KST)
        print(f"\n[INFO] {now_kst.strftime('%Y-%m-%d')} 데이터 업데이트 시도")

        auth_key = "AOns516sTNCp7OderLzQ7Q"
        url = "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"
        start_time_str = now_kst.strftime("%Y%m%d0000")
        end_time_str = now_kst.strftime("%Y%m%d2300")
        params = {
            "authKey": auth_key,
            "tm1": start_time_str,
            "tm2": end_time_str,
            "stn": "159",
            "help": "0",
        }

        try:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            resp = requests.get(url, params=params, timeout=30, verify=False)
            resp.raise_for_status()

            lines = resp.text.strip().splitlines()
            header_line = None
            data_lines = []
            for line in lines:
                s = line.strip()
                if s.startswith("# YYMMDDHHMI"):
                    header_line = s.replace("#", "").strip()
                elif s and not s.startswith("#"):
                    data_lines.append(s)

            if not header_line or not data_lines:
                print("[INFO] 수신 데이터 없음")
                return

            headers = header_line.split()
            stn_idx = headers.index("STN")
            ta_idx = headers.index("TA")
            hm_idx = headers.index("HM")
            pa_idx = headers.index("PA")

            busan_lines = [ln for ln in data_lines if ln.split()[stn_idx] == "159"]
            updated = 0

            for ln in busan_lines:
                vals = ln.split()
                record_time = datetime.strptime(vals[0], "%Y%m%d%H%M").replace(tzinfo=KST)
                ta, hm, pa = vals[ta_idx], vals[hm_idx], vals[pa_idx]

                if all(v and v != "-999.0" for v in [ta, hm, pa]):
                    try:
                        exists = WeatherReading.query.filter_by(timestamp=record_time, source="KMA").first()
                        if not exists:
                            rec = WeatherReading(
                                source="KMA",
                                temperature=float(ta),
                                humidity=float(hm),
                                pressure=float(pa),
                                timestamp=record_time,
                            )
                            db.session.add(rec)
                            db.session.commit()
                            updated += 1
                    except IntegrityError:
                        db.session.rollback()

            if updated:
                print(f"[SUCCESS] {updated}건 DB 업데이트")
            else:
                print("[INFO] 신규 데이터 없음")
        except Exception as e:
            print(f"[FATAL] 데이터 업데이트 실패: {e}")

# --- 웹 대시보드 ---
@app.route("/")
def dashboard():
    update_kma_data_in_db()
    now_kst = datetime.now(KST)
    return render_template("index.html", display_date=now_kst.strftime("%Y-%m-%d %H:%M"))

# --- 차트 데이터 API ---
@app.route("/api/chart_data/<category>")
def get_chart_data(category):
    now_kst = datetime.now(KST)
    start = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now_kst.replace(hour=23, minute=59, second=59, microsecond=59)

    kma_records = WeatherReading.query.filter(
        WeatherReading.source == "KMA",
        WeatherReading.timestamp.between(start, end)
    ).order_by(WeatherReading.timestamp).all()

    arduino_records = WeatherReading.query.filter(
        WeatherReading.source == "Arduino",
        WeatherReading.timestamp.between(start, end)
    ).order_by(WeatherReading.timestamp).all()

    labels = [r.timestamp.strftime("%H:%M") for r in kma_records]
    def values(source_list, field):
        return [getattr(r, field) for r in source_list]

    if category == "temperature":
        return jsonify({
            "labels": labels,
            "kma_values": values(kma_records, "temperature"),
            "arduino_values": {r.timestamp.strftime("%H:%M"): r.temperature for r in arduino_records}
        })
    elif category == "humidity":
        return jsonify({
            "labels": labels,
            "kma_values": values(kma_records, "humidity"),
            "arduino_values": {r.timestamp.strftime("%H:%M"): r.humidity for r in arduino_records}
        })
    elif category == "pressure":
        return jsonify({
            "labels": labels,
            "kma_values": values(kma_records, "pressure"),
            "arduino_values": {}
        })
    return jsonify({"error": "Invalid category"}), 404

# --- 최신 데이터 API ---
@app.route("/api/latest-data")
def get_latest_data():
    now_kst = datetime.now(KST)
    start = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    latest_kma = WeatherReading.query.filter(
        WeatherReading.source == "KMA",
        WeatherReading.timestamp >= start
    ).order_by(WeatherReading.timestamp.desc()).first()

    latest_arduino = WeatherReading.query.filter_by(source="Arduino").order_by(
        WeatherReading.timestamp.desc()).first()

    return jsonify({
        "display_date": now_kst.strftime("%Y-%m-%d %H:%M"),
        "kma": {
            "temperature": latest_kma.temperature if latest_kma else None,
            "humidity": latest_kma.humidity if latest_kma else None,
            "pressure": latest_kma.pressure if latest_kma else None,
        },
        "arduino": {
            "temperature": latest_arduino.temperature if latest_arduino else None,
            "humidity": latest_arduino.humidity if latest_arduino else None,
        },
    })

# --- 아두이노 시리얼 읽기 스레드 ---
def read_from_arduino():
    port = "COM3"  # 윈도우 포트 이름 (확인 후 변경 가능)
    baud = 9600
    try:
        ser = serial.Serial(port, baud)
        print(f"[INFO] 아두이노 연결 성공: {port}")
    except Exception as e:
        print(f"[ERROR] 아두이노 포트 연결 실패: {e}")
        return

    while True:
        try:
            line = ser.readline().decode().strip()
            if line:
                vals = line.split(",")
                if len(vals) == 3:
                    t, h, p = map(float, vals)
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

    # 시리얼 읽기용 스레드 실행
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        print("[INFO] 아두이노 시리얼 읽기 스레드 시작 (메인 프로세스)")
        threading.Thread(target=read_from_arduino, daemon=True).start()
    else:
        print("[INFO] 아두이노 시리얼 읽기 스레드 시작 건너뜀 (리스타터 프로세스)")

    # Flask 서버 실행
    app.run(host="0.0.0.0", port=5000, debug=True)