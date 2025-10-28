import os
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta, timezone
import requests
import json
from sqlalchemy.exc import IntegrityError
import urllib3
import time

# --- 기본 설정 ---
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(basedir, "weather.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
KST = timezone(timedelta(hours=9))
# --- 1. 데이터베이스 모델 정의 ---
class WeatherReading(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(10), nullable=False)
    temperature = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    timestamp = db.Column(db.DateTime(timezone=True), nullable=False, unique=True)

# --- 2. 데이터 업데이트 함수 --
def update_kma_data_in_db():
    """API에서 최신 데이터를 가져와 DB를 업데이트"""
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
                ta = vals[ta_idx]
                hm = vals[hm_idx]
                pa = vals[pa_idx]

                if all(v and v != "-999.0" for v in [ta, hm, pa]):
                    try:
                        exists = WeatherReading.query.filter_by(timestamp=record_time).first()
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

# --- 3. 웹 페이지 렌더링 ---
@app.route("/")
def dashboard():
    update_kma_data_in_db()
    now_kst = datetime.now(KST)
    return render_template("index.html", display_date=now_kst.strftime("%Y-%m-%d %H:%M"))

# --- 4. 동적 차트 데이터 API ---
@app.route("/api/chart_data/<category>")
def get_chart_data(category):
    now_kst = datetime.now(KST)
    start_of_day = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now_kst.replace(hour=23, minute=59, second=59, microsecond=59)

    kma_q = WeatherReading.query.filter(
        WeatherReading.source == "KMA",
        WeatherReading.timestamp.between(start_of_day, end_of_day),
        ).order_by(WeatherReading.timestamp)
    kma_records = kma_q.all()
    chart_labels = [r.timestamp.strftime("%H:%M") for r in kma_records]

    arduino_q = WeatherReading.query.filter(
        WeatherReading.source == "Arduino",
        WeatherReading.timestamp.between(start_of_day, end_of_day),
        ).order_by(WeatherReading.timestamp)
    arduino_records = arduino_q.all()

    if category == "temperature":
        kma_values = [r.temperature for r in kma_records]
        adict = {r.timestamp.strftime("%H:%M"): r.temperature for r in arduino_records}
        arduino_values = [adict.get(lbl) for lbl in chart_labels]
    elif category == "humidity":
        kma_values = [r.humidity for r in kma_records]
        adict = {r.timestamp.strftime("%H:%M"): r.humidity for r in arduino_records}
        arduino_values = [adict.get(lbl) for lbl in chart_labels]
    elif category == "pressure":
        kma_values = [r.pressure for r in kma_records]
        arduino_values = [None] * len(chart_labels)
    else:
        return jsonify({"error": "Invalid category"}), 404

    return jsonify({"labels": chart_labels, "kma_values": kma_values, "arduino_values": arduino_values})

# --- 5. 최신 데이터 API ---
@app.route("/api/latest-data")
def get_latest_data():
    now_kst = datetime.now(KST)
    start_of_day = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)

    latest_kma = (
        WeatherReading.query.filter(
            WeatherReading.source == "KMA",
            WeatherReading.timestamp >= start_of_day,
            )
        .order_by(WeatherReading.timestamp.desc())
        .first()
    )
    latest_arduino = (
        WeatherReading.query.filter_by(source="Arduino")
        .order_by(WeatherReading.timestamp.desc())
        .first()
    )

    data = {
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
    }
    return jsonify(data)

# --- 6. 아두이노 데이터 수신 ---
@app.route("/api/arduino", methods=["POST"])
def receive_arduino_data():
    data = request.get_json()
    if not data or "temperature" not in data or "humidity" not in data:
        return jsonify({"status": "error"}), 400
    try:
        rec = WeatherReading(
            source="Arduino",
            temperature=float(data["temperature"]),
            humidity=float(data["humidity"]),
            timestamp=datetime.now(KST),
        )
        db.session.add(rec)
        db.session.commit()
        return jsonify({"status": "success"}), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({"status": "error", "message": "Duplicate timestamp"}), 409
    except Exception as e:
        print(f"아두이노 데이터 저장 에러: {e}")
        return jsonify({"status": "error"}), 500

# --- 7. 메인 실행 ---
if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(host="0.0.0.0", port=5000, debug=True)
