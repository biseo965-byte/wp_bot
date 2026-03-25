import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import firestore
from datetime import datetime, timedelta
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_SESSION_DATE_COUNT = 21

package_infos = [
    {
        "name": "초급",
        "packagecode": "WI3015DB9887B2A7",
        "idx": 13,
        "cate1": "9",
        "cate2": "30",
        "cate3": "0",
        "sectype": "30",
        "price": "80000",
        "thumb": "",
        "subject": "리프자유서핑(초급)",
        "possaleid": "N492525291106G7S4546"
    },
    {
        "name": "중급",
        "packagecode": "WI3015DB98879C47",
        "idx": 14,
        "cate1": "9",
        "cate2": "30",
        "cate3": "0",
        "sectype": "30",
        "price": "80000",
        "thumb": "",
        "subject": "리프자유서핑(중급)",
        "possaleid": "N492525291106G7S4547"
    },
    {
        "name": "상급",
        "packagecode": "WI3015DB988796C8",
        "idx": 15,
        "cate1": "9",
        "cate2": "30",
        "cate3": "0",
        "sectype": "30",
        "price": "80000",
        "thumb": "",
        "subject": "리프자유서핑(상급)",
        "possaleid": "N492525291106G7S4548"
    },
    {
        "name": "Lv4 라인업 레슨",
        "packagecode": "WI7016C431CF3046",
        "idx": 16,
        "cate1": "9",
        "cate2": "30",
        "cate3": "0",
        "sectype": "30",
        "price": "90000",
        "thumb": "",
        "subject": "라인업 레슨 Lv4",
        "possaleid": "N492525291106G7S4549"
    },
    {
        "name": "Lv5 턴기초 레슨",
        "packagecode": "WI7016C431CEDEBD",
        "idx": 17,
        "cate1": "9",
        "cate2": "30",
        "cate3": "0",
        "sectype": "30",
        "price": "90000",
        "thumb": "",
        "subject": "턴기초 레슨 Lv5",
        "possaleid": "N492525291106G7S4550"
    }
]

night_package_infos = [
    {
        "idx": 27926,
        "isFunding": True,
        "available_date": [
            "2025-07-03", "2025-07-06",
            "2025-07-09", "2025-07-12", "2025-07-22", "2025-07-24"
        ],
        "session_name": "펀딩 초급 2시간",
        "minimum_funding_rate": 40,
        "maximun_count": 60
    },
    {
        "idx": 27925,
        "isFunding": True,
        "available_date": [
            "2025-07-02", "2025-07-07",
            "2025-07-11", "2025-07-13", "2025-07-21", "2025-07-23"
        ],
        "session_name": "펀딩 중급 2시간",
        "minimum_funding_rate": 40,
        "maximun_count": 60
    },

    {
        "idx": 27924,
        "isFunding": True,
        "available_date": [
            "2025-07-05", "2025-07-08",
            "2025-07-10", "2025-07-25"
        ],
        "session_name": "펀딩 상급 2시간",
        "minimum_funding_rate": 40,
        "maximun_count": 40
    },

    
    # 25 / 7  중순 ~ 25 / 8월 말
    {
        "idx": 28856,
        "isFunding": False,
        "available_date": [
            "2025-07-13", "2025-08-03",
            "2025-08-17"
        ],
        "session_name": "나이트 초급 2시간",
        "minimum_funding_rate": 0,
        "maximun_count": 60
    },
    {
        "idx": 28855,
        "isFunding": False,
        "available_date": [
            "2025-07-12", "2025-07-27",
            "2025-08-02", "2025-08-09", "2025-08-15", "2025-08-16"
        ],
        "session_name": "나이트 중급 2시간",
        "minimum_funding_rate": 0,
        "maximun_count": 60
    },
    {
        "idx": 28854,
        "isFunding": False,
        "available_date": [
            "2025-07-26"
        ],
        "session_name": "나이트 상급 2시간",
        "minimum_funding_rate": 0,
        "maximun_count": 60
    },


      # 25 / 7  중순 ~ 25 / 8월 말  Funding
    {
        "idx": 27926,
        "isFunding": True,
        "available_date": [
            "2025-07-26", "2025-07-29",
            "2025-07-31", "2025-08-02",
            "2025-08-06", "2025-08-08",
            "2025-08-11", "2025-08-14"
        ],
        "session_name": "펀딩 초급 2시간",
        "minimum_funding_rate": 0,
        "maximun_count": 60
    },
    {
        "idx": 27925,
        "isFunding": True,
        "available_date": [
            "2025-07-28", "2025-07-29",
            "2025-07-30", "2025-07-31",
            "2025-08-03", "2025-08-05",
            "2025-08-06", "2025-08-07",
            "2025-08-10", "2025-08-12",
            "2025-08-14", "2025-08-17"
        ],
        "session_name": "펀딩 중급 2시간",
        "minimum_funding_rate": 0,
        "maximun_count": 60
    },
    {   
        "idx": 27924,
        "isFunding": True,
        "available_date": [
            "2025-07-27", "2025-08-01",
            "2025-08-04", "2025-08-05",
            "2025-08-07", "2025-08-09",
            "2025-08-13", "2025-08-15",
            "2025-08-16"
        ],
        "session_name": "펀딩 상급 2시간",
        "minimum_funding_rate": 0,
        "maximun_count": 60
    },


    #    {
    #     "idx": 27926,
    #     "isFunding": True,
    #     "available_date": [
    #         "2025-07-26", "2025-07-29",
    #         "2025-07-31", "2025-08-02",
    #         "2025-08-06", "2025-08-08",
    #         "2025-08-11", "2025-08-14"
    #     ],
    #     "session_name": "After Wsl 초초급",
    #     "minimum_funding_rate": 0,
    #     "maximun_count": 60
    # },
    # {
    #     "idx": 27925,
    #     "isFunding": True,
    #     "available_date": [
    #         "2025-07-28", "2025-07-29",
    #         "2025-07-30", "2025-07-31",
    #         "2025-08-03", "2025-08-05",
    #         "2025-08-06", "2025-08-07",
    #         "2025-08-10", "2025-08-12",
    #         "2025-08-14", "2025-08-17"
    #     ],
    #     "session_name": "After Wsl 중급",
    #     "minimum_funding_rate": 0,
    #     "maximun_count": 60
    # },
    # {   
    #     "idx": 30060,
    #     "isFunding": True,
    #     "available_date": [
    #         "2025-07-27", "2025-08-01",
    #         "2025-08-04", "2025-08-05",
    #         "2025-08-07", "2025-08-09",
    #         "2025-08-13", "2025-08-15",
    #         "2025-08-16"
    #     ],
    #     "session_name": "After Wsl 상급",
    #     "minimum_funding_rate": 0,
    #     "maximun_count": 60
    # },
]

code = "241511957"
dateActive = "1"

# 2. Waves timetable (valid 기간별로 추가)
waves_timetable = [
    {
        "valid_from": "2025-06-01",
        "valid_to": "2025-07-04",
        "mapping": {
            "10:00:00": "M4 , T1",
            "11:00:00": "M1(E) , M2(E)",
            "12:00:00": "M3 , M4",
            "13:00:00": "M1 , M2",
            "14:00:00": "M4",
            "15:00:00": "M2 , M3",
            "16:00:00": "T1 , T2",
            "17:00:00": "M2 , M3 , M4"
        }
    },
    {
        "valid_from": "2025-07-05",
        "valid_to": "2025-07-25",
        "mapping": {
            "09:00:00": "M4,M4(L)",
            "10:00:00": "T1,T2",
            "11:00:00": "M1(easy),M2(easy)",
            "12:00:00": "M4",
            "13:00:00": "M1,M2",
            "14:00:00": "M2,M3,M4",
            "15:00:00": "M1,M2,M3",
            "16:00:00": "M3,M4",
            "17:00:00": "M2,M3",
            "18:00:00": "M4,T1",
            "19:00:00": "T1,T2"
        }
    },
    {
        "valid_from": "2025-07-26",
        "valid_to": "2025-08-17",
        "mapping": {
             "09:00:00": "M4,M4(L)",
            "10:00:00": "T1,T2",
            "11:00:00": "M1(easy),M2(easy)",
            "12:00:00": "M4",
            "13:00:00": "M1,M2",
            "14:00:00": "M2,M3,M4",
            "15:00:00": "M1,M2,M3(easy)",
            "16:00:00": "M3,M4",
            "17:00:00": "M2,M3",
            "18:00:00": "M4,T1",
            "19:00:00": "T1,T2"
        }
    },
    {
        "valid_from": "2025-08-18",
        "valid_to": "2025-08-30",
        "mapping": {
            "09:00:00": "M4(L)",
            "10:00:00": "M4,T1",
            "11:00:00": "M1(easy),M2(easy)",
            "12:00:00": "M3,M4",
            "13:00:00": "M1,M2",
            "14:00:00": "M2,M3,M4",
            "15:00:00": "M1,M2,M3",
            "16:00:00": "M4",
            "17:00:00": "M2,M3",
            "18:00:00": "T1,T2",
            "19:00:00": "M2,M3,M4"
        }
    },
    
    
    # 이후 기간 timetable도 추가 가능
]

def get_valid_waves_mapping(target_date):
    for item in waves_timetable:
        from_dt = datetime.strptime(item["valid_from"], "%Y-%m-%d")
        to_dt = datetime.strptime(item["valid_to"], "%Y-%m-%d")
        if from_dt <= target_date <= to_dt:
            return item["mapping"]
    return {}

# 나이트 펀딩 세션 크롤링 함수
def get_night_funding_sessions(night_pkg, pickdate):
    """나이트 펀딩 세션 정보 가져오기"""
    url = "https://www.wavepark.co.kr/packagebooking/reserv_pannel"
    data = {
        "idx": night_pkg["idx"],
        "pickdate": pickdate
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.wavepark.co.kr/packagebooking/"
    }
    logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} 요청 payload: {json.dumps(data, ensure_ascii=False)}")
    try:
        response = requests.post(url, data=data, headers=headers)
        logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} 응답코드: {response.status_code}")
        night_sessions = []
        if response.status_code == 200:
            try:
                res_json = response.json()
                out_html = res_json.get('outHtml', '')
                logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} outHtml 길이: {len(out_html)}")
                
                soup = BeautifulSoup(out_html, 'html.parser')
                
                # 세션 시간 정보 찾기
                time_spans = soup.find_all('span', class_='time')
                remain_spans = soup.find_all('span', class_='remain')
                
                logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} time_spans 개수: {len(time_spans)}")
                logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} remain_spans 개수: {len(remain_spans)}")
                
                # HTML 내용 일부 로깅
                if len(out_html) > 0:
                    logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} HTML 일부: {out_html[:500]}...")
                
                logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} 파싱된 세션 개수: {len(time_spans)}")
                
                for i, time_span in enumerate(time_spans):
                    if i < len(remain_spans):
                        time_text = time_span.get_text(strip=True)
                        remain_text = remain_spans[i].get_text(strip=True)
                        
                        logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} 세션 {i+1}: time='{time_text}', remain='{remain_text}'")
                        
                        # 시간 파싱 (예: "22:00 ~ 00:00" -> "22:00:00")
                        time_parts = time_text.split('~')[0].strip()
                        if len(time_parts) == 5:  # "22:00" 형식
                            session_time = time_parts + ":00"
                        else:
                            session_time = time_parts
                        
                        # 잔여 수량 파싱 (예: "21/40" -> 21)
                        remain_parts = remain_text.split('/')
                        if len(remain_parts) >= 1:
                            remain_count = remain_parts[0].strip()
                            try:
                                remain_count = int(remain_count)
                            except ValueError:
                                remain_count = 0
                        else:
                            remain_count = 0
                        
                        # left 값 계산 (minimum_funding_rate|maximun_count)
                        left_value = f"{night_pkg['minimum_funding_rate']}|{night_pkg['maximun_count']}"
                        
                        night_session = {
                            "time": session_time,
                            "name": night_pkg["session_name"],
                            "left": left_value,
                            "right": remain_count,
                            "isfunding": night_pkg["isFunding"],
                            "isNight": not night_pkg["isFunding"],
                            "islesson": False,
                            "waves": ""
                        }
                        night_sessions.append(night_session)
                        logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} 세션 {i+1} 생성: {night_session}")
                        
            except Exception as e:
                logger.error(f"[나이트] {pickdate} {night_pkg['session_name']} JSON/outHtml 파싱 오류: {e}", exc_info=True)
                logger.error(f"[나이트] {pickdate} {night_pkg['session_name']} 응답 본문: {response.text[:1000]}")
        else:
            logger.error(f"[나이트] {pickdate} {night_pkg['session_name']} API 응답코드 비정상: {response.status_code}")
            logger.error(f"[나이트] {pickdate} {night_pkg['session_name']} 응답 본문: {response.text[:500]}")
        
        logger.info(f"[나이트] {pickdate} {night_pkg['session_name']} 최종 세션 수: {len(night_sessions)}")
        return night_sessions
    except Exception as e:
        logger.error(f"[나이트] {pickdate} {night_pkg['session_name']} 요청 예외: {e}", exc_info=True)
        return []

# 3. 1차 API 크롤링
def get_session_info(pkg, pickdate):
    url = "https://www.wavepark.co.kr/generalbooking/ajaxDateCheck"
    data = {
        "usercnt": 0,
        "pickdate": pickdate,
        "cate3": pkg["cate3"],
        "cate2": pkg["cate2"],
        "cate1": pkg["cate1"],
        "sectype": pkg["sectype"],
        "code": code,
        "price": pkg["price"],
        "thumb": pkg["thumb"],
        "subject": pkg["subject"],
        "packagecode": pkg["packagecode"],
        "possaleid": pkg["possaleid"],
        "idx": pkg["idx"],
        "dateActive": dateActive,
        "pannelCnt": 1
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.wavepark.co.kr/generalbooking/"
    }
    logger.info(f"[1차] {pickdate} {pkg['name']} 요청 payload: {json.dumps(data, ensure_ascii=False)}")
    try:
        response = requests.post(url, data=data, headers=headers)
        logger.info(f"[1차] {pickdate} {pkg['name']} 응답코드: {response.status_code}")
        session_list = []
        if response.status_code == 200:
            try:
                res_json = response.json()
                out_html = res_json.get('outHtml', '')
                soup = BeautifulSoup(out_html, 'html.parser')
                lis = soup.find_all('li', class_='reg_items')
                logger.info(f"[1차] {pickdate} {pkg['name']} 파싱된 세션 개수: {len(lis)}")
                for li in lis:
                    session = {
                        "itemidx": li.get('data-itemidx'),
                        "pickdatetime": li.get('data-pickdatetime'),
                        "time": li.get('data-picktime'),
                        "schidx": li.get('data-schidx'),
                        "limit_cnt": li.get('data-limit_cnt')
                    }
                    # 레슨은 remain만 파싱
                    if "레슨" in pkg["name"]:
                        remain_span = li.find('span', class_='remain')
                        if remain_span:
                            remain_text = remain_span.get_text(strip=True)
                            remain = remain_text.split('/')[0].strip()
                            session["remain"] = remain
                        else:
                            session["remain"] = None
                    session_list.append(session)
            except Exception as e:
                logger.error(f"[1차] {pickdate} {pkg['name']} JSON/outHtml 파싱 오류: {e}", exc_info=True)
                logger.error(f"[1차] {pickdate} {pkg['name']} 응답 본문: {response.text[:1000]}")
        else:
            logger.error(f"[1차] {pickdate} {pkg['name']} API 응답코드 비정상: {response.status_code}")
            logger.error(f"[1차] {pickdate} {pkg['name']} 응답 본문: {response.text[:500]}")
        return session_list
    except Exception as e:
        logger.error(f"[1차] {pickdate} {pkg['name']} 요청 예외: {e}", exc_info=True)
        return []

# 4. 2차 API (레슨 아닌 경우만)
def get_section_limitsqty(s, pannelCnt=1):
    url = "https://www.wavepark.co.kr/generalbooking/ajaxSectionCheck"
    data = {
        "limit_cnt": s["limit_cnt"],
        "schidx": s["schidx"],
        "picktime": s["time"],
        "pickdatetime": s["pickdatetime"],
        "itemidx": s["itemidx"],
        "pannelCnt": pannelCnt
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.wavepark.co.kr/generalbooking/"
    }
    logger.info(f"[2차] schidx={s['schidx']} 요청 payload: {json.dumps(data, ensure_ascii=False)}")
    try:
        response = requests.post(url, data=data, headers=headers)
        logger.info(f"[2차] schidx={s['schidx']} 응답코드: {response.status_code}")
        if response.status_code != 200:
            logger.warning(f"[2차] schidx={s['schidx']} 응답코드 비정상: {response.status_code}")
            logger.warning(f"[2차] schidx={s['schidx']} 응답 본문: {response.text[:500]}")
            return {"left": None, "right": None}
        try:
            res_json = response.json()
            out_html = res_json.get('outHtml', '')
            soup = BeautifulSoup(out_html, 'html.parser')
            left_input = soup.find('input', {'id': 'area101'})
            right_input = soup.find('input', {'id': 'area201'})
            result = {
                "left": left_input.get('data-limitsqty') if left_input else None,
                "right": right_input.get('data-limitsqty') if right_input else None
            }
            logger.info(f"[2차] schidx={s['schidx']} limitsqty: {result}")
            return result
        except Exception as e:
            logger.error(f"[2차] schidx={s['schidx']} JSON/outHtml 파싱 오류: {e}", exc_info=True)
            logger.error(f"[2차] schidx={s['schidx']} 응답 본문: {response.text[:500]}")
            return {"left": None, "right": None}
    except Exception as e:
        logger.error(f"[2차] schidx={s['schidx']} 요청 예외: {e}", exc_info=True)
        return {"left": None, "right": None}

# 5. 세션 가공 및 waves 매핑
def process_sessions(raw_sessions, date_str):
    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    waves_map = get_valid_waves_mapping(target_date)
    sessions = []
    for s in raw_sessions:
        name = s["name"]
        # Lv4/Lv5 레슨 시간 고정
        if "Lv4" in name:
            time = "11:00:00"
            islesson = True
        elif "Lv5" in name:
            time = "13:00:00"
            islesson = True
        else:
            time = s.get("time")
            islesson = False
        
        # time이 None이면 건너뛰기
        if time is None:
            logger.warning(f"[필터] time이 None인 세션 제외: {s}")
            continue
            
        # waves 자동 적용 (time만)
        waves = ""
        if time in waves_map:
            waves = waves_map[time]
        session_obj = {
            "time": time,
            "name": name,
            "left": s.get("left"),
            "right": s.get("right"),
            "isfunding": s.get("isfunding", False),
            "islesson": islesson,
            "isNight": s.get("isNight", False),
            "waves": waves
        }
        sessions.append(session_obj)
    # 시간 오름차순 정렬 (None 값 제외)
    sessions.sort(key=lambda x: x["time"] if x["time"] is not None else "99:99:99")
    return sessions

# 6. Firestore 병합 저장 (지난 세션/None 세션 제외)
def save_to_firestore(db, date_str, new_sessions):
    doc_ref = db.collection('daily_sessions').document(date_str)
    doc = doc_ref.get()
    if doc.exists:
        existing_sessions = doc.to_dict().get('sessions', [])
    else:
        existing_sessions = []
    session_map = {(s['time'], s['name']): s for s in existing_sessions if s.get('time') is not None}

    now = datetime.now()
    target_date = datetime.strptime(date_str, "%Y-%m-%d")

    for ns in new_sessions:
        ns_time = ns.get('time')
        # 1. time이 None이면 제외
        if ns_time is None:
            logger.info(f"[필터] time이 None인 세션 제외: {ns}")
            continue
        # 2. 이미 지난 시간 세션 제외
        session_dt = datetime.strptime(f"{date_str} {ns_time}", "%Y-%m-%d %H:%M:%S")
        if session_dt < now:
            logger.info(f"[필터] 이미 지난 시간 세션 제외: {ns}")
            continue

        key = (ns['time'], ns['name'])
        if key in session_map:
            session_map[key]['left'] = ns.get('left')
            session_map[key]['right'] = ns.get('right')
            session_map[key]['name'] = ns.get('name')
            session_map[key]['waves'] = ns.get('waves')
            session_map[key]['isNight'] = ns.get('isNight')
            session_map[key]['isfunding'] = ns.get('isfunding')
        else:
            session_map[key] = ns

    merged_sessions = sorted(
        session_map.values(),
        key=lambda x: x['time']
    )
    doc_ref.set({'sessions': merged_sessions})
    logger.info(f"[저장] {date_str} 세션 {len(merged_sessions)}개(병합) 저장 완료")

# 7. 메인 루프
def main(request):
    if not firebase_admin._apps:
        firebase_admin.initialize_app(options={
            'projectId': os.environ.get('GOOGLE_CLOUD_PROJECT', 'wavepark-d71a3')
        })
    db = firestore.client()
    start_date = datetime.today()
    for day in range(0, MAX_SESSION_DATE_COUNT):
        pickdate = (start_date + timedelta(days=day)).strftime('%Y-%m-%d')
        logger.info(f"=== {pickdate} 전체 패키지 크롤링 시작 ===")
        raw_sessions = []
        
        # 일반 세션 크롤링
        for pkg in package_infos:
            logger.info(f"[START] {pickdate} {pkg['name']} ({pkg['packagecode']}) 크롤링 시작")
            sessions = get_session_info(pkg, pickdate)
            for s in sessions:
                # 레슨: left만, 시간 고정
                if "레슨" in pkg["name"]:
                    s["name"] = pkg["name"]
                    s["left"] = int(s.get("remain") or 0)
                    s["right"] = None
                else:
                    limitsqty = get_section_limitsqty(s)
                    s["name"] = pkg["name"]
                    s["left"] = int(limitsqty.get("left") or 0)
                    s["right"] = int(limitsqty.get("right") or 0)
                raw_sessions.append({
                    "time": s.get("time"),  # 원본 picktime 유지
                    "name": s["name"],
                    "left": s.get("left"),
                    "right": s.get("right"),
                    "isfunding": False,  # 추후 확장
                    "islesson": "레슨" in pkg["name"],
                    "isNight": False,
                    "waves": ""         # process_sessions에서 자동 매핑
                })
        
        # 나이트 펀딩 세션 크롤링
        for night_pkg in night_package_infos:
            if pickdate in night_pkg["available_date"]:
                logger.info(f"[START] {pickdate} {night_pkg['session_name']} 나이트 펀딩 크롤링 시작")
                night_sessions = get_night_funding_sessions(night_pkg, pickdate)
                raw_sessions.extend(night_sessions)
        
        processed_sessions = process_sessions(raw_sessions, pickdate)
        save_to_firestore(db, pickdate, processed_sessions)
        logger.info(f"=== {pickdate} 전체 패키지 크롤링 및 저장 완료 ===")
    logger.info("=== 전체 기간 크롤링 및 저장 완료 ===")
    return "OK"

if __name__ == '__main__':
    main(None)