import os
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
from datetime import datetime, timedelta, timezone
import requests
import json
from sqlalchemy.exc import IntegrityError
import urllib3
import time

# --- 기본 설정 ---
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'weather.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
scheduler = APScheduler()
KST = timezone(timedelta(hours=9))

# --- 1. 데이터베이스 모델 정의 ---
class WeatherReading(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(10), nullable=False)
    temperature = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    timestamp = db.Column(db.DateTime(timezone=True), nullable=False, unique=True)

# --- 2. 백그라운드 자동 업데이트 함수 ---
def update_today_kma_data():
    with app.app_context():
        now_kst = datetime.now(KST)
        print(f"\n[INFO] 스케줄러 실행: '{now_kst.strftime('%Y-%m-%d')}' 데이터 자동 업데이트 시작...")
        auth_key = "AOns516sTNCp7OderLzQ7Q"
        url = 'https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php'
        start_time_str, end_time_str = now_kst.strftime('%Y%m%d0000'), now_kst.strftime('%Y%m%d2300')
        params = {'authKey': auth_key, 'tm1': start_time_str, 'tm2': end_time_str, 'stn': '159', 'help': '0'}
        try:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            response = requests.get(url, params=params, timeout=30, verify=False)
            response.raise_for_status()
            raw_data = response.text
            lines = raw_data.strip().splitlines()
            header_line, data_lines = None, []
            for line in lines:
                if line.strip().startswith('# YYMMDDHHMI'): header_line = line.replace('#', '').strip()
                elif not line.strip().startswith('#') and line.strip(): data_lines.append(line.strip())
            if not header_line or not data_lines: return
            headers = header_line.split()
            stn_index, temp_index, hum_index, pressure_index = headers.index('STN'), headers.index('TA'), headers.index('HM'), headers.index('PA')
            busan_data_lines = [line for line in data_lines if line.split()[stn_index] == '159']
            updated_count = 0
            for line in busan_data_lines:
                values = line.split()
                record_time = datetime.strptime(values[0], '%Y%m%d%H%M').replace(tzinfo=KST)
                temp_str, hum_str, pressure_str = values[temp_index], values[hum_index], values[pressure_index]
                if all(val and val != '-999.0' for val in [temp_str, hum_str, pressure_str]):
                    try:
                        existing_record = WeatherReading.query.filter_by(timestamp=record_time).first()
                        if not existing_record:
                            new_reading = WeatherReading(source='KMA', temperature=float(temp_str), humidity=float(hum_str), pressure=float(pressure_str), timestamp=record_time)
                            db.session.add(new_reading)
                            db.session.commit()
                            updated_count += 1
                    except IntegrityError: db.session.rollback()
            if updated_count > 0: print(f"[SUCCESS] {updated_count}개의 새로운 데이터로 DB를 업데이트했습니다.")
            else: print("[INFO] 새로운 데이터가 없어 DB 업데이트를 건너뜁니다.")
        except Exception as e:
            print(f"[FATAL] 스케줄러 작업 중 에러: {e}")

# --- 3. 웹 페이지 렌더링 (최초 페이지 로딩) ---
@app.route('/')
def dashboard():
    now_kst = datetime.now(KST)
    return render_template('index.html', display_date=now_kst.strftime('%Y-%m-%d %H:%M'))

# --- 4. 동적 차트 데이터를 위한 API ---
@app.route('/api/chart_data/<category>')
def get_chart_data(category):
    now_kst = datetime.now(KST)
    start_of_day = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now_kst.replace(hour=23, minute=59, second=59, microsecond=59)
    kma_records = WeatherReading.query.filter(WeatherReading.source == 'KMA', WeatherReading.timestamp.between(start_of_day, end_of_day)).order_by(WeatherReading.timestamp).all()
    chart_labels = [r.timestamp.strftime('%H:%M') for r in kma_records]
    arduino_records = WeatherReading.query.filter(WeatherReading.source == 'Arduino', WeatherReading.timestamp.between(start_of_day, end_of_day)).order_by(WeatherReading.timestamp).all()
    
    if category == 'temperature':
        kma_values = [r.temperature for r in kma_records]
        arduino_dict = {r.timestamp.strftime('%H:%M'): r.temperature for r in arduino_records}
        arduino_values = [arduino_dict.get(label) for label in chart_labels]
    elif category == 'humidity':
        kma_values = [r.humidity for r in kma_records]
        arduino_dict = {r.timestamp.strftime('%H:%M'): r.humidity for r in arduino_records}
        arduino_values = [arduino_dict.get(label) for label in chart_labels]
    elif category == 'pressure':
        kma_values = [r.pressure for r in kma_records]
        arduino_values = [None] * len(chart_labels)
    else: return jsonify({"error": "Invalid category"}), 404
    return jsonify({"labels": chart_labels, "kma_values": kma_values, "arduino_values": arduino_values})

# --- 5. 자동 업데이트를 위한 최신 데이터 API ---
@app.route('/api/latest-data')
def get_latest_data():
    now_kst = datetime.now(KST)
    start_of_day = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    latest_kma = WeatherReading.query.filter(WeatherReading.source == 'KMA', WeatherReading.timestamp >= start_of_day).order_by(WeatherReading.timestamp.desc()).first()
    latest_arduino = WeatherReading.query.filter_by(source='Arduino').order_by(WeatherReading.timestamp.desc()).first()
    data = {
        "display_date": now_kst.strftime('%Y-%m-%d %H:%M'),
        "kma": {"temperature": latest_kma.temperature if latest_kma else None, "humidity": latest_kma.humidity if latest_kma else None, "pressure": latest_kma.pressure if latest_kma else None},
        "arduino": {"temperature": latest_arduino.temperature if latest_arduino else None, "humidity": latest_arduino.humidity if latest_arduino else None}
    }
    return jsonify(data)

# --- 6. 아두이노 API ---
@app.route('/api/arduino', methods=['POST'])
def receive_arduino_data():
    data = request.get_json()
    if not data or 'temperature' not in data or 'humidity' not in data: return jsonify({'status': 'error'}), 400
    try:
        new_reading = WeatherReading(source='Arduino', temperature=float(data['temperature']), humidity=float(data['humidity']), timestamp=datetime.now(KST))
        db.session.add(new_reading)
        db.session.commit()
        return jsonify({'status': 'success'}), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': 'Duplicate timestamp'}), 409
    except Exception as e:
        print(f"아두이노 데이터 저장 에러: {e}")
        return jsonify({'status': 'error'}), 500

# --- 7. 메인 실행 부분 ---
if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        if not WeatherReading.query.filter_by(source='KMA').first():
            print("--- KMA 초기 데이터 수집 시작 ---")
            update_today_kma_data()
            print("--- KMA 초기 데이터 수집 완료 ---")
    scheduler.add_job(id='KMA Data Updater', func=update_today_kma_data, trigger='cron', minute=1)
    scheduler.init_app(app)
    scheduler.start()
    print("스케줄러가 시작되었습니다. 매시 1분에 자동으로 데이터를 업데이트합니다.")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)