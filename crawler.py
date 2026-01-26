"""
REXA 자동 뉴스 크롤러
- 5분마다 실행 (Cron Job)
- 부동산 뉴스 20개 수집 → 필터링 → 저장
"""

import asyncio
import logging
import sys
import os
import json
from datetime import datetime
from openai import OpenAI
from difflib import SequenceMatcher

# 공통 함수 임포트
from common import (
    search_naver_news,
    save_all_news_background,
    init_google_sheets,
    init_csv_file,
    get_recent_urls_from_gsheet,
    get_recent_titles_from_gsheet
)

# ================================================================================
# 로깅 설정
# ================================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================================================================================
# 크롤링 통계
# ================================================================================

class CrawlStats:
    """크롤링 통계 추적"""
    def __init__(self):
        self.total_fetched = 0
        self.total_filtered = 0
        self.total_saved = 0
        self.start_time = None
        self.end_time = None
    
    def print_summary(self):
        """통계 요약 출력"""
        if self.start_time and self.end_time:
            elapsed = (self.end_time - self.start_time).total_seconds()
            logger.info("=" * 70)
            logger.info("📊 크롤링 통계 요약")
            logger.info(f"   ⏱️  소요시간: {elapsed:.1f}초")
            logger.info(f"   🔍 수집: {self.total_fetched}개 (네이버 API)")
            logger.info(f"   ✅ 필터링 후: {self.total_filtered}개 (부동산 관련)")
            logger.info(f"   💾 저장: {self.total_saved}개 (구글시트/CSV)")
            if self.total_fetched > 0:
                filter_rate = (self.total_filtered / self.total_fetched) * 100
                logger.info(f"   📈 필터링율: {filter_rate:.1f}%")
            logger.info("=" * 70)

# ================================================================================
# 뉴스 요약 함수 (크롤러 전용)
# ================================================================================

def generate_news_summary(title: str, description: str) -> str:
    """
    GPT를 사용해서 뉴스를 3-4문장으로 요약 (크롤러 전용)
    
    Args:
        title: 뉴스 제목
        description: 네이버 API에서 받은 description
    
    Returns:
        3-4문장의 충실한 요약 (200-250자)
    """
    
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    if not OPENAI_API_KEY:
        # GPT 사용 불가 시 문장 단위로 자르기
        if len(description) > 250:
            sentences = description.split('.')
            if len(sentences) >= 3:
                return sentences[0] + '.' + sentences[1] + '.' + sentences[2] + '.'
            else:
                return description[:250].strip() + '...'
        return description
    
    system_prompt = """당신은 뉴스 요약 전문가입니다.
주어진 뉴스 제목과 설명을 읽고, 핵심 내용을 3-4문장으로 충실하게 요약하세요.

요약 규칙:
- 3-4문장으로 작성 (200-250자)
- 구체적인 정보를 반드시 포함 (수치, 날짜, 지역, 주체 등)
- 단순히 "~했다"가 아니라 "누가, 무엇을, 왜, 어떻게"를 포함
- 뉴스의 맥락과 배경까지 간략히 설명
- 자연스러운 한국어 문장

예시:
입력: "서울 강남구 재건축 아파트 가격 급등...규제 완화 영향"
출력: "서울 강남구 재건축 아파트 가격이 전월 대비 5% 상승했다. 정부의 재건축 규제 완화와 금리 인하 기대감이 주요 원인으로 작용했다. 특히 대치동과 압구정동 일대 단지들이 강세를 보였다. 전문가들은 이러한 상승세가 당분간 지속될 것으로 전망했다."

JSON 형식으로 응답:
{"summary": "요약 내용"}"""

    user_prompt = f"""제목: {title}
설명: {description}

위 뉴스를 3-4문장으로 충실하게 요약하세요. 구체적인 정보(수치, 지역, 날짜 등)를 반드시 포함하세요."""

    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"},
            timeout=15
        )
        
        result = json.loads(response.choices[0].message.content)
        summary = result.get('summary', description[:250])
        
        # 요약이 너무 길면 자르기
        if len(summary) > 280:
            summary = summary[:277] + '...'
        
        return summary
        
    except Exception as e:
        logger.warning(f"⚠️ GPT 요약 실패: {e} - 원본 사용")
        # 실패 시 문장 단위로 자르기
        if len(description) > 250:
            sentences = description.split('.')
            if len(sentences) >= 3:
                return sentences[0] + '.' + sentences[1] + '.' + sentences[2] + '.'
            else:
                return description[:250].strip() + '...'
        return description

def calculate_title_similarity(title1: str, title2: str) -> float:
    """
    두 제목 간의 유사도 계산 (0.0 ~ 1.0)
    
    Args:
        title1: 첫 번째 제목
        title2: 두 번째 제목
    
    Returns:
        유사도 (0.0 = 완전 다름, 1.0 = 완전 같음)
    """
    # 소문자 변환 및 공백 정리
    t1 = title1.lower().strip()
    t2 = title2.lower().strip()
    
    # SequenceMatcher로 유사도 계산
    similarity = SequenceMatcher(None, t1, t2).ratio()
    
    return similarity

def remove_duplicate_news(news_items: list, similarity_threshold: float = 0.75) -> list:
    """
    제목 유사도 기반으로 중복 뉴스 제거
    
    Args:
        news_items: 뉴스 아이템 리스트
        similarity_threshold: 중복 판단 임계값 (0.75 = 75% 이상 유사하면 중복)
    
    Returns:
        중복이 제거된 뉴스 리스트
    """
    if not news_items:
        return []
    
    # 점수 높은 순으로 정렬
    sorted_items = sorted(
        news_items, 
        key=lambda x: x.get('relevance_score', 0), 
        reverse=True
    )
    
    unique_news = []
    removed_count = 0
    
    for item in sorted_items:
        is_duplicate = False
        
        # 이미 선택된 뉴스들과 비교
        for selected in unique_news:
            similarity = calculate_title_similarity(
                item['title'], 
                selected['title']
            )
            
            if similarity >= similarity_threshold:
                is_duplicate = True
                removed_count += 1
                logger.info(
                    f"   ⚠️ 중복 제거: '{item['title'][:40]}...' "
                    f"(유사도: {similarity:.0%} with '{selected['title'][:30]}...')"
                )
                break
        
        if not is_duplicate:
            unique_news.append(item)
    
    logger.info(f"✅ 중복 제거 완료: {len(sorted_items)}개 → {len(unique_news)}개 (중복 {removed_count}개 제거)")
    
    return unique_news

# ================================================================================
# 메인 크롤링 함수
# ================================================================================

async def auto_crawl():
    """자동 크롤링 메인 로직"""
    stats = CrawlStats()
    stats.start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info(f"⏰ 자동 크롤링 시작: {stats.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    try:
        # 1. 초기화
        logger.info("🔧 초기화 중...")
        csv_success = init_csv_file()
        gsheet_success = init_google_sheets()
        
        if csv_success:
            logger.info("   ✅ CSV 초기화 완료")
        if gsheet_success:
            logger.info("   ✅ Google Sheets 초기화 완료")
        
        # 2. 뉴스 검색 (20개)
        logger.info("")
        logger.info("🔍 네이버 뉴스 검색 중...")
        logger.info("   검색어: 부동산")
        logger.info("   요청 개수: 20개")
        
        news_items = search_naver_news("부동산", display=20)
        
        if not news_items or len(news_items) == 0:
            logger.warning("")
            logger.warning("⚠️ 수집된 뉴스 없음")
            logger.warning("   원인: 네이버 API 오류 또는 필터링 결과 0개")
            stats.end_time = datetime.now()
            stats.print_summary()
            return
        
        stats.total_fetched = 20  # 네이버 API 요청 개수
        stats.total_filtered = len(news_items)  # 필터링 후 개수
        
        logger.info("")
        logger.info(f"✅ {len(news_items)}개 부동산 관련 뉴스 발견")
        
        # 상위 3개 뉴스 미리보기
        logger.info("")
        logger.info("📰 상위 3개 뉴스:")
        for idx, item in enumerate(news_items[:3]):
            logger.info(f"   [{idx+1}] {item['title'][:50]}...")
            logger.info(f"       점수: {item.get('relevance_score', 0)}점 | "
                       f"지역: {item.get('region', 'N/A')} | "
                       f"키워드: {', '.join(item.get('keywords', [])[:3])}")
        
        # 3. 뉴스 요약 생성 (크롤러 전용)
        logger.info("")
        logger.info("📝 뉴스 요약 생성 중...")
        for idx, item in enumerate(news_items):
            original_desc = item['description']
            summary = generate_news_summary(item['title'], original_desc)
            item['description'] = summary
            logger.info(f"   [{idx+1}/{len(news_items)}] 요약 완료: {item['title'][:40]}...")
        
        # 4. 중복 뉴스 제거 (제목 유사도 기반)
        logger.info("")
        logger.info("🔍 중복 뉴스 확인 중 (제목 유사도)...")
        original_count = len(news_items)
        news_items = remove_duplicate_news(news_items, similarity_threshold=0.75)
        
        # 중복 제거 후 통계 업데이트
        if len(news_items) < original_count:
            logger.info(f"   📊 중복 제거: {original_count}개 → {len(news_items)}개")
        else:
            logger.info(f"   ✅ 중복 없음: {len(news_items)}개 유지")
        
        # 5. DB 중복 체크 (최근 3시간 URL + 최근 24시간 제목 확인)
        logger.info("")
        logger.info("🔍 DB 중복 확인 중 (URL + 제목)...")
        
        # URL 중복 체크 (최근 3시간)
        recent_urls = get_recent_urls_from_gsheet(hours=3)
        # 제목 중복 체크 (최근 24시간)
        recent_titles = get_recent_titles_from_gsheet(hours=24)
        
        before_db_check = len(news_items)
        new_news_items = []
        duplicate_count = 0
        
        for item in news_items:
            # URL 체크
            url = item.get('link') or item.get('url', '')
            if url and url in recent_urls:
                duplicate_count += 1
                logger.info(f"   ⚠️ URL 중복: '{item['title'][:40]}...' (이미 저장된 URL)")
                continue
            
            # 제목 체크 (정규화)
            title = item.get('title', '')
            normalized_title = title.lower().strip().replace(' ', '')
            if normalized_title and normalized_title in recent_titles:
                duplicate_count += 1
                logger.info(f"   ⚠️ 제목 중복: '{item['title'][:40]}...' (이미 저장된 제목)")
                continue
            
            # 중복이 아니면 추가
            new_news_items.append(item)
        
        news_items = new_news_items
        
        if duplicate_count > 0:
            logger.info(f"   📊 DB 중복 제거: {before_db_check}개 → {len(news_items)}개 (중복 {duplicate_count}개)")
        else:
            logger.info(f"   ✅ DB 중복 없음: {len(news_items)}개 모두 신규")
        
        # 저장할 뉴스가 없으면 종료
        if len(news_items) == 0:
            logger.warning("")
            logger.warning("⚠️ 저장할 신규 뉴스 없음")
            logger.warning("   원인: 모두 최근 3시간 내 저장된 뉴스")
            stats.end_time = datetime.now()
            stats.print_summary()
            return
        
        # 6. 백그라운드 저장
        logger.info("")
        logger.info("💾 구글 시트/CSV 저장 중...")
        await save_all_news_background(news_items, user_id="auto_crawler")
        
        stats.total_saved = len(news_items)
        
        # 7. 완료
        stats.end_time = datetime.now()
        logger.info("")
        logger.info("🎉 크롤링 완료!")
        stats.print_summary()
        
    except KeyboardInterrupt:
        logger.info("")
        logger.info("⚠️ 사용자에 의해 중단됨")
        sys.exit(0)
        
    except Exception as e:
        logger.error("")
        logger.error(f"❌ 크롤링 실패: {type(e).__name__}")
        logger.error(f"   에러 메시지: {e}")
        
        import traceback
        logger.error("")
        logger.error("📋 상세 에러 로그:")
        for line in traceback.format_exc().split('\n'):
            if line.strip():
                logger.error(f"   {line}")
        
        stats.end_time = datetime.now()
        stats.print_summary()
        sys.exit(1)  # 에러 발생 시 종료 코드 1

# ================================================================================
# 스크립트 실행
# ================================================================================

if __name__ == "__main__":
    """
    이 스크립트는 Render Cron Job으로 1시간마다 실행됩니다.
    
    로컬 테스트:
        python crawler.py
    
    Render 설정 (render.yaml):
        schedule: "0 * * * *"  # 매시 0분에 실행
    """
    
    try:
        asyncio.run(auto_crawl())
    except Exception as e:
        logger.error(f"❌ 크롤러 실행 실패: {e}")
        sys.exit(1)
