"""
REXA 공통 함수 모듈
- 뉴스 검색/필터링
- 크롤링
- 저장 (CSV, Google Sheets)
"""

import logging
import os
import time
import re
import csv
import json
from datetime import datetime
from typing import Optional
import asyncio

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

# Google Sheets용
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    logging.warning("gspread not installed. Google Sheets logging disabled.")

# ================================================================================
# 환경변수
# ================================================================================

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")

CSV_FILE_PATH = "news_data.csv"

# ================================================================================
# 글로벌 변수
# ================================================================================

gsheet_client = None
gsheet_worksheet = None

logger = logging.getLogger(__name__)

# ================================================================================
# 뉴스 필터링 시스템
# ================================================================================

def is_headline_news(title: str, description: str) -> bool:
    """
    헤드라인/종합 뉴스인지 판단
    
    Returns:
        True: 헤드라인 뉴스 (제외 대상)
        False: 일반 뉴스
    """
    text = (title + " " + description).lower()
    
    # 헤드라인 패턴 키워드
    headline_keywords = [
        "오늘의 부동산 뉴스",
        "오늘의 뉴스",
        "부동산 뉴스 총정리",
        "헤드라인",
        "뉴스 브리핑",
        "뉴스 모음",
        "주요 뉴스",
        "뉴스 정리",
    ]
    
    for keyword in headline_keywords:
        if keyword in text:
            return True
    
    # 패턴 매칭: "뉴스 (총 N건)", "총 N건의 뉴스" 등
    patterns = [
        r'뉴스\s*\(총\s*\d+건\)',  # 뉴스 (총 5건)
        r'총\s*\d+건',             # 총 5건
        r'\d+건의?\s*뉴스',        # 5건의 뉴스
    ]
    
    for pattern in patterns:
        if re.search(pattern, text):
            return True
    
    return False


def check_celebrity_scandal(title: str, description: str) -> dict:
    """
    연예인 관련 뉴스의 부동산 관련성 판단
    
    Returns:
        {
            'is_celebrity_news': bool,
            'should_exclude': bool,  # True면 제외
            'reason': str
        }
    """
    text = (title + " " + description).lower()
    
    # 연예인 키워드
    celebrity_keywords = [
        "배우", "가수", "연예인", "아이돌", "탤런트",
        "스타", "셀럽", "방송인", "코미디언", "개그맨"
    ]
    
    # 부동산 거래 키워드 (포함 OK)
    transaction_keywords = [
        "매매", "매입", "구입", "구매", "취득", "샀다", "사들",
        "매도", "판매", "처분", "팔았다", "팔아",
        "억원에", "억대", "억원대",
        "투자", "분양", "입주",
        "새집", "이사"
    ]
    
    # 분쟁/스캔들 키워드 (제외)
    scandal_keywords = [
        "분쟁", "갈등", "소송", "고소", "고발",
        "혐의", "의혹", "논란", "폭로", "고발",
        "사기", "횡령", "배임",
        "전 남편", "전 부인", "이혼", "위자료"
    ]
    
    is_celebrity = any(kw in text for kw in celebrity_keywords)
    has_transaction = any(kw in text for kw in transaction_keywords)
    has_scandal = any(kw in text for kw in scandal_keywords)
    
    # 연예인 뉴스가 아니면 패스
    if not is_celebrity:
        return {
            'is_celebrity_news': False,
            'should_exclude': False,
            'reason': '연예인 뉴스 아님'
        }
    
    # 연예인 + 분쟁/스캔들 = 제외
    if has_scandal:
        return {
            'is_celebrity_news': True,
            'should_exclude': True,
            'reason': '연예인 분쟁/스캔들 (부동산 거래 무관)'
        }
    
    # 연예인 + 거래 키워드 = 포함
    if has_transaction:
        return {
            'is_celebrity_news': True,
            'should_exclude': False,
            'reason': '연예인 부동산 매수/매도 (포함)'
        }
    
    # 애매한 경우 - GPT가 판단하도록 넘김
    return {
        'is_celebrity_news': True,
        'should_exclude': False,
        'reason': '연예인 관련이지만 추가 판단 필요'
    }


def filter_real_estate_news(title: str, description: str) -> dict:
    """
    기사가 부동산과 관련이 있는지 GPT로 판단하고 핵심 지표 추출
    
    Returns:
        {
            'is_relevant': bool,
            'relevance_score': int,
            'keywords': list,
            'region': str or None,
            'has_price': bool,
            'has_policy': bool,
            'reason': str
        }
    """
    
    # ============================================================
    # 1단계: 헤드라인 뉴스 사전 필터링
    # ============================================================
    if is_headline_news(title, description):
        logging.info(f"❌ [헤드라인 제외] {title[:50]}...")
        return {
            'is_relevant': False,
            'relevance_score': 0,
            'keywords': [],
            'region': None,
            'has_price': False,
            'has_policy': False,
            'reason': '헤드라인/종합 뉴스'
        }
    
    # ============================================================
    # 2단계: 연예인 분쟁 뉴스 필터링
    # ============================================================
    celebrity_check = check_celebrity_scandal(title, description)
    if celebrity_check['should_exclude']:
        logging.info(f"❌ [연예인 분쟁 제외] {title[:50]}... ({celebrity_check['reason']})")
        return {
            'is_relevant': False,
            'relevance_score': 0,
            'keywords': [],
            'region': None,
            'has_price': False,
            'has_policy': False,
            'reason': celebrity_check['reason']
        }
    
    # ============================================================
    # 3단계: GPT 필터링 (기존 로직)
    # ============================================================
    if not OPENAI_API_KEY:
        logging.warning("⚠️ OPENAI_API_KEY not set - using keyword filtering")
        return filter_by_keywords(title, description)
    
    system_prompt = """당신은 부동산 뉴스 필터링 전문가입니다.

기사 제목과 설명을 보고 이것이 "부동산과 관련이 있는지" 판단하세요.

✅ 부동산 관련 기사:
- 아파트, 오피스텔, 상가, 토지 등 부동산 매매/임대
- 부동산 가격, 시세, 거래량
- 부동산 정책, 세금, 대출, 금리
- 재건축, 재개발, 분양, 청약
- 부동산 투자, 수익형 부동산
- **연예인의 부동산 매수/매도/투자 (OK)**

❌ 부동산 무관 기사:
- **헤드라인 뉴스, 종합 뉴스 (여러 기사를 모은 것)**
- **연예인 분쟁/스캔들 (부동산 거래와 무관한 소송, 갈등)**
- 주식, 채권, 코인 등 금융상품
- 일반 경제 뉴스 (부동산 언급 없음)
- 정치, 사회, 문화 이슈
- 건설사 실적이지만 부동산과 직접 연관 없음

JSON 형식으로 응답:
{
  "is_relevant": true/false,
  "relevance_score": 0-100,
  "keywords": ["키워드1", "키워드2", "키워드3"],
  "region": "지역명" or null,
  "has_price": true/false,
  "has_policy": true/false,
  "reason": "판단 근거 1-2줄"
}"""

    user_prompt = f"""제목: {title}
설명: {description}

이 기사가 부동산과 관련이 있습니까?"""

    try:
        openai_client_filter = OpenAI(api_key=OPENAI_API_KEY)
        response = openai_client_filter.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
            timeout=10
        )
        
        result = json.loads(response.choices[0].message.content)
        
        status = "✅ 관련" if result['is_relevant'] else "❌ 무관"
        logging.info(f"{status} (점수: {result['relevance_score']}) - {title[:40]}...")
        
        return result
        
    except Exception as e:
        logging.error(f"❌ GPT 필터링 실패: {e}")
        return filter_by_keywords(title, description)

def filter_by_keywords(title: str, description: str) -> dict:
    """키워드 기반 간단 필터링 (GPT 실패 시 폴백)"""
    text = (title + " " + description).lower()
    
    real_estate_keywords = [
        "아파트", "오피스텔", "빌딩", "상가", "토지", "주택",
        "매매", "전세", "월세", "분양", "청약", "입주",
        "재건축", "재개발", "정비구역", "부동산", "집값",
        "주택가격", "전세가", "시세", "주담대", "종부세",
        "양도세", "취득세", "국토부", "미분양"
    ]
    
    exclude_keywords = ["주식", "코인", "비트코인", "펀드", "채권"]
    
    matched = sum(1 for kw in real_estate_keywords if kw in text)
    excluded = sum(1 for kw in exclude_keywords if kw in text)
    
    score = max(0, min(100, matched * 30 - excluded * 20))
    is_relevant = score >= 30
    
    keywords = [kw for kw in real_estate_keywords if kw in text][:5]
    region = extract_region(text)
    
    logging.info(f"🔑 키워드 필터 (점수: {score}) - {title[:40]}...")
    
    return {
        'is_relevant': is_relevant,
        'relevance_score': score,
        'keywords': keywords,
        'region': region,
        'has_price': any(kw in text for kw in ['가격', '시세', '억', '만원', '상승', '하락']),
        'has_policy': any(kw in text for kw in ['정책', '규제', '세금', '대출', '금리']),
        'reason': f'키워드 매칭 기반 ({matched}개 매칭)'
    }

def extract_region(text: str) -> str:
    """텍스트에서 지역 정보 추출"""
    seoul_gu = [
        "강남구", "강동구", "강북구", "강서구", "관악구",
        "광진구", "구로구", "금천구", "노원구", "도봉구",
        "동대문구", "동작구", "마포구", "서대문구", "서초구",
        "성동구", "성북구", "송파구", "양천구", "영등포구",
        "용산구", "은평구", "종로구", "중구", "중랑구"
    ]
    
    gyeonggi_cities = [
        "성남시", "용인시", "수원시", "고양시", "화성시",
        "평택시", "부천시", "안양시", "남양주시"
    ]
    
    metropolitan = ["인천", "부산", "대구", "대전", "광주", "울산", "세종"]
    
    for gu in seoul_gu:
        if gu in text:
            return f"서울 {gu}"
    
    for city in gyeonggi_cities:
        if city in text:
            return f"경기 {city}"
    
    for metro in metropolitan:
        if metro in text:
            return metro
    
    return None

def filter_news_batch(news_items: list) -> list:
    """여러 뉴스 기사를 배치로 필터링 (75점 이상만)"""
    filtered = []
    
    # 필터링 통계
    filter_stats = {
        'headline': 0,
        'celebrity_scandal': 0,
        'low_score': 0,
        'not_relevant': 0
    }
    
    for item in news_items:
        result = filter_real_estate_news(item['title'], item['description'])
        item.update(result)
        
        # 부동산 관련 + 75점 이상만 통과
        if result['is_relevant'] and result.get('relevance_score', 0) >= 75:
            filtered.append(item)
        else:
            # 제외 이유 카운트
            reason = result.get('reason', '').lower()
            if '헤드라인' in reason or '종합' in reason:
                filter_stats['headline'] += 1
            elif '연예인' in reason and '분쟁' in reason:
                filter_stats['celebrity_scandal'] += 1
            elif result.get('relevance_score', 0) < 75:
                filter_stats['low_score'] += 1
            else:
                filter_stats['not_relevant'] += 1
    
    # 통계 로깅
    total_filtered = sum(filter_stats.values())
    if total_filtered > 0:
        logger.info("")
        logger.info("📊 필터링 제외 통계:")
        if filter_stats['headline'] > 0:
            logger.info(f"   - 헤드라인 뉴스: {filter_stats['headline']}개")
        if filter_stats['celebrity_scandal'] > 0:
            logger.info(f"   - 연예인 분쟁: {filter_stats['celebrity_scandal']}개")
        if filter_stats['low_score'] > 0:
            logger.info(f"   - 낮은 점수 (75점 미만): {filter_stats['low_score']}개")
        if filter_stats['not_relevant'] > 0:
            logger.info(f"   - 부동산 무관: {filter_stats['not_relevant']}개")
    
    return filtered

# ================================================================================
# 뉴스 검색
# ================================================================================

def search_naver_news(query: str = "부동산", display: int = 10) -> Optional[list]:
    """네이버 뉴스 API로 최신 뉴스 검색 + 부동산 관련성 필터링"""
    url = "https://openapi.naver.com/v1/search/news.json"
    
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET
    }
    
    params = {
        "query": query,
        "display": display,
        "sort": "date"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        items = data.get('items', [])
        if not items:
            return None
        
        # 네이버 뉴스 도메인만 필터링
        naver_items = [item for item in items if 'news.naver.com' in item['link']]
        
        if not naver_items:
            logger.warning("⚠️ 네이버 뉴스가 없습니다. 일반 뉴스를 사용합니다.")
            naver_items = items
        
        logger.info(f"✅ 네이버 뉴스 {len(naver_items)}개 발견")
        
        # 모든 뉴스 아이템 처리
        processed_items = []
        for item in naver_items:
            # HTML 태그 제거
            title = re.sub('<[^<]+?>', '', item['title'])
            description = re.sub('<[^<]+?>', '', item['description'])
            
            # HTML 엔티티 디코딩
            import html
            title = html.unescape(title)
            description = html.unescape(description)
            
            # 요약 길이 제한 (200자)
            if len(description) > 200:
                cut_pos = 200
                for i in range(200, max(0, len(description) - 100), -1):
                    if description[i] in '.!?':
                        cut_pos = i + 1
                        break
                description = description[:cut_pos].strip()
            
            processed_items.append({
                "title": title,
                "description": description,
                "link": item['link'],
                "pubDate": item['pubDate'],
                "timestamp": datetime.now().isoformat()
            })
        
        # 부동산 관련성 필터링 (75점 이상만)
        logger.info(f"🔍 필터링 시작: {len(processed_items)}개 기사")
        filtered_items = filter_news_batch(processed_items)
        logger.info(
            f"✅ 필터링 완료: {len(processed_items)}개 중 {len(filtered_items)}개 선정 (75점 이상) "
            f"({len(filtered_items)/len(processed_items)*100:.1f}%)"
        )
        return filtered_items
        
    except Exception as e:
        logger.error(f"❌ 뉴스 검색 오류: {e}")
        return None

def crawl_news_content(url: str) -> str:
    """뉴스 URL에서 본문 추출 (재시도 포함)"""
    max_retries = 2
    
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
                'Referer': 'https://news.naver.com/'
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 네이버 뉴스 본문 추출
            if 'news.naver.com' in url:
                article = soup.select_one('#dic_area') or soup.select_one('#articeBody') or soup.select_one('.news_end')
                if article:
                    for tag in article.find_all(['script', 'style', 'aside']):
                        tag.decompose()
                    content = article.get_text(strip=True, separator='\n')
                    logger.info(f"📄 크롤링 성공: {len(content)}자")
                    return content
            
            # 일반 뉴스 사이트
            paragraphs = soup.find_all('p')
            content = '\n'.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50])
            
            if content:
                logger.info(f"📄 크롤링 성공: {len(content)}자")
                return content
            else:
                return "본문을 추출할 수 없습니다."
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ 타임아웃 발생 - 재시도 {attempt + 1}/{max_retries}")
                time.sleep(2)
                continue
            else:
                logger.error(f"❌ 크롤링 타임아웃: {url[:50]}...")
                return "본문을 가져올 수 없습니다. (타임아웃)"
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = 3
                    logger.warning(f"⚠️ Rate Limit (429) - {wait_time}초 대기 후 재시도")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ Rate Limit 초과: {url[:50]}...")
                    return "본문을 가져올 수 없습니다. (Rate Limit)"
            else:
                logger.error(f"❌ HTTP 오류 {e.response.status_code}: {url[:50]}...")
                return f"본문을 가져올 수 없습니다. (HTTP {e.response.status_code})"
                
        except Exception as e:
            logger.error(f"❌ 크롤링 오류: {e}")
            return "본문을 가져올 수 없습니다."
    
    return "본문을 가져올 수 없습니다."

# ================================================================================
# Google Sheets & CSV 저장
# ================================================================================

def init_google_sheets():
    """Initialize Google Sheets client"""
    global gsheet_client, gsheet_worksheet
    
    if not GSPREAD_AVAILABLE:
        logger.error("❌ gspread not installed")
        return False
    
    if not GOOGLE_SHEETS_CREDENTIALS or not GOOGLE_SHEETS_SPREADSHEET_ID:
        logger.error("❌ Google Sheets 환경변수 미설정")
        return False
    
    try:
        logger.info("🔄 Initializing Google Sheets...")
        
        creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gsheet_client = gspread.authorize(credentials)
        
        spreadsheet = gsheet_client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        gsheet_worksheet = spreadsheet.sheet1
        
        # 헤더 확인 및 생성
        try:
            headers = gsheet_worksheet.row_values(1)
            if not headers or headers[0] != 'timestamp':
                gsheet_worksheet.insert_row([
                    'timestamp', 'title', 'description', 'url',
                    'is_relevant', 'relevance_score', 'keywords', 'region',
                    'has_price', 'has_policy', 'reason', 'user_id'
                ], 1)
                logger.info("✅ Google Sheets headers created")
        except:
            gsheet_worksheet.insert_row([
                'timestamp', 'title', 'description', 'url',
                'is_relevant', 'relevance_score', 'keywords', 'region',
                'has_price', 'has_policy', 'reason', 'user_id'
            ], 1)
        
        logger.info(f"✅ Google Sheets initialized")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize Google Sheets: {e}")
        return False

def get_recent_urls_from_gsheet(hours: int = 3) -> set:
    """
    구글 시트에서 최근 N시간 내 저장된 URL 목록 가져오기
    
    Args:
        hours: 몇 시간 이내 데이터를 확인할지 (기본 3시간)
    
    Returns:
        최근 N시간 내 URL 집합
    """
    global gsheet_worksheet
    
    if not gsheet_worksheet:
        logger.warning("⚠️ Google Sheets not initialized - 중복 체크 불가")
        return set()
    
    try:
        from datetime import datetime, timedelta
        
        # 현재 시간 - N시간
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # 전체 레코드 가져오기
        all_records = gsheet_worksheet.get_all_records()
        
        recent_urls = set()
        
        for record in all_records:
            try:
                # timestamp 파싱 (ISO format)
                timestamp_str = record.get('timestamp', '')
                if not timestamp_str:
                    continue
                
                # ISO format 파싱
                record_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                
                # 최근 N시간 이내면 URL 추가
                if record_time >= cutoff_time:
                    url = record.get('url', '')
                    if url:
                        recent_urls.add(url)
            except Exception as e:
                # 개별 레코드 파싱 실패는 무시
                continue
        
        logger.info(f"📋 최근 {hours}시간 URL 확인: {len(recent_urls)}개")
        return recent_urls
        
    except Exception as e:
        logger.error(f"❌ 최근 URL 조회 실패: {e}")
        return set()

def init_csv_file():
    """Initialize CSV file with headers"""
    try:
        if not os.path.exists(CSV_FILE_PATH):
            with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'title', 'description', 'url',
                    'is_relevant', 'relevance_score', 'keywords', 'region',
                    'has_price', 'has_policy', 'reason', 'user_id'
                ])
            logger.info(f"✅ CSV file created: {CSV_FILE_PATH}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize CSV: {e}")
        return False

def save_news_to_csv(news_data: dict):
    """Save news to CSV file"""
    try:
        with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                news_data['timestamp'],
                news_data['title'],
                news_data['description'],
                news_data['url'],
                news_data.get('is_relevant', True),
                news_data.get('relevance_score', 0),
                ', '.join(news_data.get('keywords', [])),
                news_data.get('region', ''),
                news_data.get('has_price', False),
                news_data.get('has_policy', False),
                news_data.get('reason', ''),
                news_data['user_id']
            ])
        return True
    except Exception as e:
        logger.error(f"❌ Failed to save to CSV: {e}")
        return False

def save_news_to_gsheet(news_data: dict):
    """Save news to Google Sheets"""
    if not gsheet_worksheet:
        logger.warning("⚠️ Google Sheets not initialized - skipping")
        return False
    
    try:
        gsheet_worksheet.append_row([
            news_data['timestamp'],
            news_data['title'],
            news_data['description'],
            news_data['url'],
            news_data.get('is_relevant', True),
            news_data.get('relevance_score', 0),
            ', '.join(news_data.get('keywords', [])),
            news_data.get('region', ''),
            news_data.get('has_price', False),
            news_data.get('has_policy', False),
            news_data.get('reason', ''),
            news_data['user_id']
        ])
        return True
    except Exception as e:
        logger.error(f"❌ Failed to save to Google Sheets: {e}")
        return False

async def save_all_news_background(news_items: list, user_id: str):
    """백그라운드에서 모든 뉴스 저장 (크롤링 없이 메타데이터만)"""
    logger.info(f"🔄 백그라운드 저장 시작: {len(news_items)}개 (크롤링 제외)")
    saved_count = 0
    
    for idx, news_item in enumerate(news_items):
        try:
            if idx > 0:
                await asyncio.sleep(0.5)
            
            # 키 이름 통일 (link → url)
            if 'link' in news_item and 'url' not in news_item:
                news_item['url'] = news_item['link']
            
            news_item['user_id'] = user_id
            
            # 필터링 메타데이터 기본값
            if 'is_relevant' not in news_item:
                news_item['is_relevant'] = True
                news_item['relevance_score'] = 50
                news_item['keywords'] = []
                news_item['region'] = ''
                news_item['has_price'] = False
                news_item['has_policy'] = False
                news_item['reason'] = 'Filtering module not available'
            
            # 저장
            save_news_to_csv(news_item)
            save_news_to_gsheet(news_item)
            
            saved_count += 1
            logger.info(
                f"✅ [{saved_count}/{len(news_items)}] 저장 완료 "
                f"[{news_item.get('relevance_score', 0)}점] "
                f"{news_item['title'][:30]}..."
            )
            
        except Exception as e:
            logger.error(f"❌ 뉴스 {idx+1} 저장 실패: {e}")
            continue
    
    logger.info(f"🎉 백그라운드 저장 완료: {saved_count}개")
