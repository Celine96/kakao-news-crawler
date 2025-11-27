"""
REXA 자동 뉴스 크롤러
- 1시간마다 실행 (Cron Job)
- 부동산 뉴스 20개 수집 → 필터링 → 저장
"""

import asyncio
import logging
import sys
from datetime import datetime

# 공통 함수 임포트
from common import (
    search_naver_news,
    save_all_news_background,
    init_google_sheets,
    init_csv_file
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
        
        # 3. 백그라운드 저장
        logger.info("")
        logger.info("💾 구글 시트/CSV 저장 중...")
        await save_all_news_background(news_items, user_id="auto_crawler")
        
        stats.total_saved = len(news_items)
        
        # 4. 완료
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
