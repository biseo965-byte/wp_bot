import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import firestore
from datetime import datetime, timedelta
import json
import os
import logging
import re
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WaveParkEventCrawler:
    def __init__(self):
        self.base_url = "https://www.wavepark.co.kr/board/event"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.events = []
    
    def get_page_url(self, page):
        """페이지 URL 생성"""
        return f"{self.base_url}?scolumn=ext2&sorder=+DESC%2C+regdate+DESC&page={page}"
    
    def extract_image_url(self, img_style):
        """CSS background-image에서 이미지 URL 추출"""
        if img_style:
            match = re.search(r"url\('([^']+)'\)", img_style)
            if match:
                img_url = match.group(1)
                if img_url.startswith('/'):
                    return f"https://www.wavepark.co.kr{img_url}"
                return img_url
        return None
    
    def extract_d_day(self, d_day_text):
        """D-day 텍스트에서 숫자 추출"""
        if d_day_text:
            match = re.search(r'D-(\d+)', d_day_text)
            if match:
                return int(match.group(1))
        return None
    
    def parse_event_item(self, li_element):
        """개별 이벤트 항목 파싱"""
        try:
            # 링크 추출
            link_element = li_element.find('a')
            if not link_element:
                return None
            
            event_url = link_element.get('href')
            if event_url.startswith('/'):
                event_url = f"https://www.wavepark.co.kr{event_url}"
            
            # 이미지 추출
            img_div = li_element.find('div', class_='img')
            image_url = None
            if img_div:
                img_style = img_div.get('style', '')
                image_url = self.extract_image_url(img_style)
            
            # 제목 추출
            title_element = li_element.find('h2')
            if not title_element:
                return None
            
            # 제목에서 태그 제거하고 텍스트만 추출
            title_text = title_element.get_text(strip=True)
            
            # 이벤트 타입 추출 ([패키지], [이벤트] 등)
            event_type = ""
            type_span = title_element.find('span', class_=['pkg-c', 'event-c'])
            if type_span:
                event_type = type_span.get_text(strip=True)
                # 제목에서 이벤트 타입 제거
                title_text = title_text.replace(event_type, '').strip()
            
            # D-day 추출
            d_day_element = title_element.find('span', class_='d-day')
            d_day = None
            if d_day_element:
                d_day = self.extract_d_day(d_day_element.get_text(strip=True))
                # 제목에서 D-day 제거
                title_text = title_text.replace(d_day_element.get_text(strip=True), '').strip()
            
            # 날짜 추출
            date_element = li_element.find('p', class_='date')
            date_text = date_element.get_text(strip=True) if date_element else ""
            
            # 이벤트 ID 추출 (URL에서)
            event_id = None
            if event_url:
                match = re.search(r'/detail/(\d+)', event_url)
                if match:
                    event_id = match.group(1)
            
            return {
                'event_id': event_id,
                'title': title_text,
                'event_type': event_type,
                'date': date_text,
                'd_day': d_day,
                'image_url': image_url,
                'event_url': event_url,
                'crawled_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"이벤트 파싱 중 오류 발생: {e}")
            return None
    
    def crawl_page(self, page):
        """특정 페이지 크롤링"""
        try:
            url = self.get_page_url(page)
            logger.info(f"페이지 {page} 크롤링 중: {url}")
            
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 이벤트 목록 찾기
            event_list = soup.find('ul', class_='event-wrap')
            if not event_list:
                logger.info(f"페이지 {page}에서 이벤트 목록을 찾을 수 없습니다.")
                return []
            
            page_events = []
            event_items = event_list.find_all('li')
            
            for item in event_items:
                event_data = self.parse_event_item(item)
                if event_data:
                    page_events.append(event_data)
            
            logger.info(f"페이지 {page}에서 {len(page_events)}개의 이벤트를 찾았습니다.")
            return page_events
            
        except requests.RequestException as e:
            logger.error(f"페이지 {page} 요청 중 오류 발생: {e}")
            return []
        except Exception as e:
            logger.error(f"페이지 {page} 처리 중 오류 발생: {e}")
            return []
    
    def crawl_all_pages(self, max_pages=10):
        """모든 페이지 크롤링 (최대 10페이지)"""
        all_events = []
        
        for page in range(1, max_pages + 1):
            page_events = self.crawl_page(page)
            
            if not page_events:
                logger.info(f"페이지 {page}에서 이벤트를 찾을 수 없습니다. 크롤링을 종료합니다.")
                break
            
            all_events.extend(page_events)
            
            # 서버 부하 방지를 위한 딜레이
            time.sleep(1)
        
        self.events = all_events
        return all_events

def save_to_firestore(db, events):
    """이벤트 데이터를 Firestore에 저장"""
    try:
        # 이벤트 컬렉션에 저장
        events_ref = db.collection('events')
        
        # 기존 이벤트 데이터 삭제 (전체 교체 방식)
        existing_docs = events_ref.stream()
        for doc in existing_docs:
            doc.reference.delete()
        
        # 새 이벤트 데이터 저장
        batch = db.batch()
        for event in events:
            if event.get('event_id'):
                doc_ref = events_ref.document(event['event_id'])
                batch.set(doc_ref, event)
            else:
                # event_id가 없는 경우 자동 생성
                doc_ref = events_ref.document()
                batch.set(doc_ref, event)
        
        batch.commit()
        logger.info(f"Firestore에 {len(events)}개의 이벤트를 저장했습니다.")
        
        # 크롤링 메타데이터 저장
        meta_ref = db.collection('crawling_meta').document('events')
        meta_data = {
            'last_crawled': datetime.now().isoformat(),
            'total_events': len(events),
            'status': 'success'
        }
        meta_ref.set(meta_data)
        logger.info("크롤링 메타데이터를 저장했습니다.")
        
    except Exception as e:
        logger.error(f"Firestore 저장 중 오류 발생: {e}")
        # 메타데이터에 오류 기록
        try:
            meta_ref = db.collection('crawling_meta').document('events')
            meta_data = {
                'last_crawled': datetime.now().isoformat(),
                'total_events': 0,
                'status': 'error',
                'error_message': str(e)
            }
            meta_ref.set(meta_data)
        except Exception as meta_error:
            logger.error(f"메타데이터 저장 중 오류: {meta_error}")

def main(request):
    """Cloud Run 메인 함수"""
    try:
        # Firebase 초기화
        if not firebase_admin._apps:
            firebase_admin.initialize_app(options={
                'projectId': os.environ.get('GOOGLE_CLOUD_PROJECT', 'wavepark-d71a3')
            })
        
        db = firestore.client()
        
        # 크롤러 초기화 및 실행
        crawler = WaveParkEventCrawler()
        logger.info("웨이브파크 이벤트 크롤링을 시작합니다...")
        
        # 모든 페이지 크롤링
        events = crawler.crawl_all_pages(max_pages=10)
        
        if events:
            # Firestore에 저장
            save_to_firestore(db, events)
            
            logger.info(f"크롤링 완료: 총 {len(events)}개의 이벤트를 수집했습니다.")
            
            # 결과 요약 로그
            event_types = {}
            for event in events:
                event_type = event.get('event_type', '기타')
                event_types[event_type] = event_types.get(event_type, 0) + 1
            
            logger.info("이벤트 타입별 통계:")
            for event_type, count in event_types.items():
                logger.info(f"  {event_type}: {count}개")
            
            return {
                'status': 'success',
                'message': f'총 {len(events)}개의 이벤트를 성공적으로 크롤링했습니다.',
                'total_events': len(events),
                'event_types': event_types
            }
        else:
            logger.warning("크롤링된 이벤트가 없습니다.")
            return {
                'status': 'warning',
                'message': '크롤링된 이벤트가 없습니다.',
                'total_events': 0
            }
            
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {e}", exc_info=True)
        return {
            'status': 'error',
            'message': f'크롤링 중 오류가 발생했습니다: {str(e)}',
            'total_events': 0
        }

if __name__ == '__main__':
    # 로컬 테스트용
    result = main(None)
    print(json.dumps(result, ensure_ascii=False, indent=2))
