"""
main.py — 수집 전용 CLI 진입점

모드:
  daily              : 오늘 기준 시장 스냅샷 + 이번 달 일별 주가 수집
  bootstrap          : 특정 연도 과거 데이터 일괄 수집 (체크포인트 기반)
  ohlc-backfill      : US/Crypto OHLC 초기 적재 (연도 범위 지정)
  ohlc-update        : US/Crypto OHLC 증분 업데이트
  financials-update  : US 재무제표 + Crypto 시장 데이터 수집

사용 예:
  # 오늘 데이터 수집 후 Drive 업로드
  python main.py --mode daily --upload-drive

  # 2025년(1년 전) 데이터 수집
  python main.py --mode bootstrap --years-ago 1 --upload-drive

  # 특정 연도 직접 지정
  python main.py --mode bootstrap --year 2022 --skip-prices

  # 실제 저장 없이 테스트
  python main.py --mode bootstrap --years-ago 1 --dry-run

  # US/Crypto OHLC 2020~2025년 백필
  python main.py --mode ohlc-backfill --market all --start-year 2020 --upload-drive

  # US OHLC 증분 업데이트
  python main.py --mode ohlc-update --market us --upload-drive

  # US + Crypto 재무 데이터 수집
  python main.py --mode financials-update --market all --upload-drive

  # Crypto 재무 데이터만 dry-run 테스트
  python main.py --mode financials-update --market crypto --dry-run

  # KR 오늘 데이터 수집 (FDR StockListing)
  python main.py --mode kr-daily --upload-drive

  # KR 과거 누락 구간 backfill (yfinance)
  python main.py --mode kr-backfill --start-date 2026-02-21 --end-date 2026-04-03 --upload-drive

  # US/Crypto 종목 메타데이터 수집 (Sector/Industry/Market 태그, 주 1회)
  python main.py --mode sector-meta --market all --upload-drive
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── 로깅 설정 ─────────────────────────────────────────────────────────────────
_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.stream.reconfigure(encoding="utf-8", errors="replace")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        _stdout_handler,
        logging.FileHandler("collection.log", encoding="utf-8"),
    ],
)
# pykrx 내부 노이즈 억제
logging.getLogger("pykrx").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# daily 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_daily(args):
    """
    오늘 기준 일별 수집:
    - 오늘 날짜 시장 스냅샷 수집
    - 이번 달 일별 주가 수집
    - Drive 업로드 (--upload-drive 시)
    """
    from data import collector, storage, progress

    today    = datetime.today()
    yyyymm   = today.strftime("%Y%m")
    date_str = collector.get_last_business_day()

    logger.info(f"[Daily] 기준일: {date_str} / 대상 월: {yyyymm}")

    # ── 시장 스냅샷 ────────────────────────────────────────────────────────
    if args.dry_run:
        logger.info(f"[DryRun] market/{yyyymm} 수집 시뮬레이션")
    else:
        logger.info(f"[Daily] 시장 스냅샷 수집 중...")
        df = collector.get_market_snapshot(date_str)
        if not df.empty:
            storage.save_market(df, yyyymm)
            progress.mark_done("market", yyyymm)
            logger.info(f"[Daily] 시장 스냅샷 저장 완료: {len(df)}종목")
        else:
            logger.warning("[Daily] 시장 스냅샷 수집 실패")

    # ── 이번 달 일별 주가 ──────────────────────────────────────────────────
    if not args.skip_prices:
        if args.dry_run:
            logger.info(f"[DryRun] prices/{yyyymm} 수집 시뮬레이션")
        else:
            logger.info(f"[Daily] 일별 주가 수집 중 ({yyyymm})...")
            df = collector.get_daily_prices_month(yyyymm)
            if not df.empty:
                storage.save_prices(df, yyyymm)
                progress.mark_done("prices", yyyymm)
                logger.info(f"[Daily] 일별 주가 저장 완료: {len(df):,}건")
            else:
                logger.warning("[Daily] 일별 주가 수집 실패")

    # ── Drive 업로드 ───────────────────────────────────────────────────────
    if args.upload_drive and not args.dry_run:
        _upload_all()

    progress.print_summary()
    storage.print_local_summary()
    logger.info("[Daily] 완료")


# ══════════════════════════════════════════════════════════════════════════════
# bootstrap 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_bootstrap(args):
    """
    과거 데이터 일괄 수집.

    단일 연도:
      --years-ago N  → 현재년도 - N 연도 1개
      --year YYYY    → 직접 연도 지정

    범위 수집:
      --years-range N   → 최근 N년치 (current-N ~ current-1)
      --year-start YYYY → YYYY ~ current-1 전체 (max 모드)
    """
    from data import historical, progress

    current_year = datetime.today().year
    skip = not args.force

    # ── 범위 수집 (years-range / year-start) ──────────────────────────────
    if args.year_start or args.years_range:
        if args.year_start:
            start_year = args.year_start
            label = f"{start_year}년~{current_year - 1}년 (최대치)"
        else:
            start_year = current_year - args.years_range
            label = f"{start_year}년~{current_year - 1}년 ({args.years_range}년치)"

        end_year = current_year - 1

        logger.info(
            f"[Bootstrap] 범위 수집: {label} | "
            f"skip_prices={args.skip_prices}, skip_financials={args.skip_financials}, "
            f"dry_run={args.dry_run}"
        )

        historical.collect_range(
            start_year=start_year,
            end_year=end_year,
            skip_if_done=skip,
            skip_prices=args.skip_prices,
            skip_market=args.skip_market,
            skip_financials=args.skip_financials,
            dry_run=args.dry_run,
            upload_drive=args.upload_drive,
        )

    # ── 단일 연도 수집 (years-ago / year) ─────────────────────────────────
    elif args.year or args.years_ago:
        target_year = args.year if args.year else current_year - args.years_ago

        logger.info(
            f"[Bootstrap] 단일 연도: {target_year}년 | "
            f"skip_prices={args.skip_prices}, skip_financials={args.skip_financials}, "
            f"dry_run={args.dry_run}"
        )

        historical.collect_year(
            year=target_year,
            skip_if_done=skip,
            skip_prices=args.skip_prices,
            dry_run=args.dry_run,
            upload_drive=args.upload_drive,
        )
        if not args.skip_market:
            historical.collect_market_range(
                start_year=target_year,
                end_year=target_year,
                skip_if_done=skip,
                dry_run=args.dry_run,
                upload_drive=args.upload_drive,
            )
        if not args.skip_financials:
            historical.collect_financials_year(
                year=target_year,
                skip_if_done=skip,
                dry_run=args.dry_run,
                upload_drive=args.upload_drive,
            )

    else:
        logger.error("bootstrap 모드에는 --years-ago / --year / --years-range / --year-start 중 하나가 필요합니다.")
        sys.exit(1)

    # 최종 Drive 전체 업로드
    if args.upload_drive and not args.dry_run:
        _upload_all()

    progress.print_summary()
    storage.print_local_summary()
    logger.info("[Bootstrap] 완료")


# ══════════════════════════════════════════════════════════════════════════════
# ohlc-backfill 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_ohlc_backfill(args):
    """
    US/Crypto OHLC 초기 적재.
    --market all|us|crypto, --start-year, --end-year 옵션 사용.
    """
    from data import ohlc_collector
    end_year = args.end_year or (datetime.today().year - 1)
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        logger.info(f"[OhlcBackfill] {market.upper()} {args.start_year}~{end_year}년 백필 시작")
        ohlc_collector.backfill_market(
            market=market,
            start_year=args.start_year,
            end_year=end_year,
            upload=args.upload_drive and not args.dry_run,
        )


# ══════════════════════════════════════════════════════════════════════════════
# ohlc-update 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_ohlc_update(args):
    """
    US/Crypto OHLC 증분 업데이트.
    마지막 업데이트 이후 누락된 데이터를 수집.
    신규 종목이 유니버스에 추가된 경우, 증분 업데이트 전에 과거 이력을 먼저 백필한다.
    """
    from data import ohlc_collector
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        logger.info(f"[OhlcUpdate] {market.upper()} 증분 업데이트 시작")
        if not args.dry_run:
            new_tickers = ohlc_collector.backfill_new_tickers(
                market=market,
                upload=args.upload_drive,
            )
            if new_tickers:
                logger.info(f"[OhlcUpdate] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
            ohlc_collector.update_market(
                market=market,
                upload=args.upload_drive,
            )
        else:
            logger.info(f"[DryRun] {market.upper()} ohlc update 시뮬레이션")


def run_ohlc_new_backfill(args):
    """
    US/Crypto 유니버스에 새로 추가된 종목만 골라 과거 이력을 백필한다.
    daily(ohlc-update)에서도 자동으로 실행되지만, 즉시 반영하고 싶을 때 수동으로 실행 가능.
    """
    from data import ohlc_collector
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        if args.dry_run:
            logger.info(f"[DryRun] {market.upper()} 신규 종목 백필 시뮬레이션")
            continue
        new_tickers = ohlc_collector.backfill_new_tickers(
            market=market,
            start_year=args.start_year,
            upload=args.upload_drive,
        )
        if new_tickers:
            logger.info(f"[OhlcNewBackfill] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
        else:
            logger.info(f"[OhlcNewBackfill] {market.upper()} 신규 종목 없음")


# ══════════════════════════════════════════════════════════════════════════════
# financials-update 모드
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# kr-daily 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_kr_daily(args):
    """
    KR daily 수집 플로우:
    1. Drive에서 현재 연도 parquet + status 다운로드
    2. last_date 확인 → 어제까지 갭이 있으면 yfinance backfill 자동 수행
    3. 오늘 FDR StockListing 스냅샷 수집
    4. 저장 + Drive 업로드
    """
    from datetime import date, timedelta
    from data import kr_collector, kr_db
    import pandas as pd

    if args.dry_run:
        logger.info("[KrDaily] dry-run: 수집 시뮬레이션 (저장 없음)")
        return

    today = date.today()
    current_year = today.year

    # 1. Drive에서 현재 연도 parquet 다운로드 (로컬에 없을 때)
    if not kr_db.local_path(current_year).exists():
        logger.info(f"[KrDaily] marcap-{current_year}.parquet 로컬 없음 → Drive 다운로드 시도")
        kr_db.download_year(current_year)

    # 2. 갭 감지 → 자동 backfill
    last_date = kr_db.get_last_date(current_year)
    if last_date is None:
        # 파일 자체가 없는 경우 — 연초부터 어제까지 백필
        gap_start = date(current_year, 1, 1)
    else:
        gap_start = last_date + timedelta(days=1)

    yesterday = today - timedelta(days=1)

    if gap_start <= yesterday:
        # 주말만 있는 구간인지 확인 (평일이 없으면 스킵)
        bdays = pd.bdate_range(str(gap_start), str(yesterday))
        if len(bdays) > 0:
            logger.info(
                f"[KrDaily] 갭 감지: {gap_start} ~ {yesterday} "
                f"({len(bdays)} 영업일) → yfinance backfill 시작"
            )
            gap_df = kr_collector.collect_backfill(str(gap_start), str(yesterday))
            if not gap_df.empty:
                gap_updated = kr_db.append_rows(gap_df)
                logger.info(f"[KrDaily] 갭 보완 완료: {gap_updated}년 파일 업데이트")
            else:
                logger.warning("[KrDaily] 갭 backfill 수집 결과 없음")
        else:
            logger.info(f"[KrDaily] 갭 없음 (주말만 존재: {gap_start} ~ {yesterday})")
    else:
        logger.info(f"[KrDaily] 갭 없음 — last_date: {last_date}")

    # 3. 오늘 FDR 스냅샷 수집
    logger.info("[KrDaily] 오늘 스냅샷 수집 (FDR StockListing)")
    df = kr_collector.collect_daily()
    if df.empty:
        logger.error("[KrDaily] 오늘 수집 실패 → 종료")
        return

    # 3b. 누락 종목 yfinance 보완
    #   FDR StockListing은 매매정지·관리종목 등 일부 활성 종목을 누락하는 경우가 있어,
    #   직전 영업일 parquet에 있었으나 오늘 결과에 없는 종목을 yfinance로 재시도.
    #   마지막 거래일이 너무 오래된 종목(>7일)은 상장폐지로 간주하고 스킵.
    try:
        prior = kr_db.load_year(current_year)
        if not prior.empty:
            today_ts = pd.Timestamp(today)
            prior_dates = prior["Date"].dropna().unique()
            recent_cutoff = today_ts - pd.Timedelta(days=7)
            recent_codes = set(
                prior[prior["Date"] >= recent_cutoff]["Code"].astype(str).str.zfill(6)
            )
            today_codes = set(df["Code"].astype(str).str.zfill(6))
            missing_codes = sorted(recent_codes - today_codes)

            if missing_codes:
                logger.info(
                    f"[KrDaily] FDR 누락 감지: {len(missing_codes)}종목 → yfinance 보완 시도"
                )
                # 메타(Name/Market) 추출 — 종목별 최근 행 기준
                meta_src = prior.assign(
                    _Code=prior["Code"].astype(str).str.zfill(6)
                ).sort_values("Date")
                latest_meta = meta_src.drop_duplicates(subset=["_Code"], keep="last")
                meta_lookup = latest_meta.set_index("_Code")[["Name", "Market"]].to_dict("index")
                code_meta = {
                    code: {
                        "Name": (meta_lookup.get(code) or {}).get("Name", "") or "",
                        "Market": (meta_lookup.get(code) or {}).get("Market", "") or "KOSPI",
                    }
                    for code in missing_codes
                }
                supp = kr_collector.collect_missing_today(missing_codes, code_meta, today)
                if not supp.empty:
                    df = pd.concat([df, supp], ignore_index=True)
                    df = df.drop_duplicates(subset=["Code", "Date"], keep="first")
                    logger.info(
                        f"[KrDaily] 보완 merge 완료: {len(supp)}종목 추가 (총 {len(df)}종목)"
                    )
    except Exception as e:
        logger.warning(f"[KrDaily] 누락 종목 보완 단계 실패 (무시하고 계속): {e}")

    # 4. 저장
    updated = kr_db.append_rows(df)

    last_saved = df["Date"].max()
    last_saved_date = last_saved.date() if hasattr(last_saved, "date") else last_saved
    existing = kr_db.load_status()
    total_days = existing.get("trading_days_total", 0) + 1
    kr_db.save_status(last_saved_date, total_days)

    # 5. Drive 업로드
    if args.upload_drive and updated:
        kr_db.upload_years(updated)
        logger.info(f"[KrDaily] Drive 업로드 완료: {updated}")


# ══════════════════════════════════════════════════════════════════════════════
# kr-backfill 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_kr_backfill(args):
    """
    yfinance로 과거 누락 구간 KR OHLCV 백필.
    --start-date / --end-date 로 기간 지정.
    """
    from data import kr_collector, kr_db

    if not args.start_date or not args.end_date:
        logger.error("[KrBackfill] --start-date, --end-date 필수 (예: --start-date 2026-02-21)")
        return

    # 날짜 형식 사전 검증 (잘못된 값으로 실행 방지)
    try:
        from datetime import datetime as _dt
        _dt.strptime(args.start_date, "%Y-%m-%d")
        _dt.strptime(args.end_date,   "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"[KrBackfill] 날짜 형식 오류: {e}  (YYYY-MM-DD 필요, 예: 2026-02-21)")
        return

    logger.info(f"[KrBackfill] 기간: {args.start_date} ~ {args.end_date}")

    if args.dry_run:
        logger.info("[KrBackfill] dry-run: 수집 시뮬레이션 (저장 없음)")
        return

    df = kr_collector.collect_backfill(args.start_date, args.end_date)
    if df.empty:
        logger.error("[KrBackfill] 수집 실패 → 종료")
        return

    updated = kr_db.append_rows(df)

    last_date = df["Date"].max()
    status = kr_db.load_status()
    total_days = status.get("trading_days_total", 0)
    kr_db.save_status(last_date.date() if hasattr(last_date, "date") else last_date, total_days)

    if args.upload_drive and updated:
        kr_db.upload_years(updated)
        logger.info(f"[KrBackfill] Drive 업로드 완료: {updated}")


# ══════════════════════════════════════════════════════════════════════════════
# sector-meta 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_sector_meta(args):
    """
    US/Crypto 종목 메타데이터 수집 (주 1회 실행).
    - US: 유니버스 전체 → Market 태그 + Yahoo .info Sector/Industry
    - Crypto: 유니버스 전체 → Market="Crypto", Sector/Industry=""
    결과: {market}_sector_meta.parquet → Drive ohlc_{market} 폴더에 업로드
    """
    from data import ohlc_collector, ohlc_db
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        logger.info(f"[SectorMeta] {market.upper()} 메타데이터 수집 시작")
        if args.dry_run:
            logger.info(f"[DryRun] {market} sector-meta 시뮬레이션")
            continue
        df = ohlc_collector.collect_sector_meta(market)
        if not df.empty:
            ohlc_db.save_sector_meta(df, market)
            if args.upload_drive:
                ohlc_db.upload_sector_meta(market)
            logger.info(f"[SectorMeta] {market.upper()} 완료: {len(df)}종목")
        else:
            logger.warning(f"[SectorMeta] {market.upper()} 결과 없음")


def run_financials_update(args):
    """
    US financials + ratios, Crypto ratios, KR financials(DART) 수집 및 Drive 업로드.
    --market us|crypto|kr|all 옵션 지원.
    """
    from data import financials_collector
    markets = ["us", "crypto", "kr"] if args.market == "all" else [args.market]
    for market in markets:
        if market == "us":
            logger.info("[FinancialsUpdate] US 재무 데이터 수집 시작")
            if not args.dry_run:
                financials_collector.collect_us_financials(upload=args.upload_drive)
            else:
                logger.info("[DryRun] US financials update 시뮬레이션")
        elif market == "crypto":
            logger.info("[FinancialsUpdate] Crypto 시장 데이터 수집 시작")
            if not args.dry_run:
                financials_collector.collect_crypto_ratios(upload=args.upload_drive)
            else:
                logger.info("[DryRun] crypto financials update 시뮬레이션")
        elif market == "kr":
            logger.info("[FinancialsUpdate] KR 재무 데이터 수집 시작")
            if not args.dry_run:
                from data import kr_financials_collector
                kr_financials_collector.collect_kr_financials(upload=args.upload_drive)
            else:
                logger.info("[DryRun] KR financials update 시뮬레이션")


# ══════════════════════════════════════════════════════════════════════════════
# Drive 업로드 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _upload_all():
    """로컬 data/ 전체를 Drive에 동기화."""
    import config
    if not config.GDRIVE_FOLDER_ID:
        logger.warning("[Upload] GDRIVE_FOLDER_ID 미설정 → 업로드 건너뜀")
        return
    has_token = config.GDRIVE_TOKEN_PATH and Path(config.GDRIVE_TOKEN_PATH).exists()
    has_sa    = Path(config.GDRIVE_CREDS_PATH).exists()
    if not has_token and not has_sa:
        logger.warning("[Upload] Drive 자격증명 없음 → 건너뜀")
        return

    try:
        from data.drive_uploader import DriveUploader
        DriveUploader().sync_all_local()
    except Exception as e:
        logger.error(f"[Upload] Drive 동기화 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="한국 주식 퀀트 데이터 수집기 (수집 전용)",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--mode",
        choices=["daily", "bootstrap", "ohlc-backfill", "ohlc-update", "ohlc-new-backfill",
                 "financials-update", "kr-daily", "kr-backfill", "sector-meta"],
        required=True,
        help=(
            "daily: 오늘 수집 / bootstrap: 과거 연도 일괄 수집 / "
            "ohlc-backfill: US/Crypto OHLC 초기 적재 / "
            "ohlc-update: US/Crypto OHLC 증분 업데이트 / "
            "ohlc-new-backfill: US/Crypto 신규 종목만 골라 과거 이력 백필 / "
            "financials-update: US 재무제표 + Crypto 시장 데이터 수집 / "
            "kr-daily: KR 오늘 스냅샷 수집 (FDR) / "
            "kr-backfill: KR 과거 누락 구간 수집 (yfinance) / "
            "sector-meta: US/Crypto 종목 메타데이터 수집 (Sector/Industry, 주 1회)"
        ),
    )

    # bootstrap 전용
    parser.add_argument(
        "--years-ago", type=int, metavar="N",
        help="bootstrap: 현재 기준 N년 전 단일 연도 수집 (예: 1 → 2025년)",
    )
    parser.add_argument(
        "--year", type=int, metavar="YYYY",
        help="bootstrap: 직접 단일 연도 지정",
    )
    parser.add_argument(
        "--years-range", type=int, metavar="N",
        help="bootstrap: 최근 N년치 범위 수집 (예: 3 → 2023~2025년)",
    )
    parser.add_argument(
        "--year-start", type=int, metavar="YYYY",
        help="bootstrap: YYYY년부터 현재까지 전체 수집 (최대치 모드, 예: 2010)",
    )

    # 수집 제어
    parser.add_argument(
        "--skip-prices", action="store_true",
        help="일별 주가 수집 생략",
    )
    parser.add_argument(
        "--skip-market", action="store_true",
        help="시장 스냅샷(PER/PBR) 수집 생략",
    )
    parser.add_argument(
        "--skip-financials", action="store_true",
        help="재무제표 수집 생략 (bootstrap 시)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="체크포인트 무시 → 이미 완료된 월도 재수집",
    )

    # ohlc 모드 전용
    parser.add_argument(
        "--market", choices=["us", "crypto", "kr", "all"], default="all",
        help="ohlc 모드: 대상 시장 (기본: all)",
    )
    parser.add_argument(
        "--start-year", type=int, default=2020,
        help="ohlc-backfill: 수집 시작 연도 (기본: 2020)",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="ohlc-backfill: 수집 종료 연도 (기본: 작년)",
    )

    # kr-backfill 전용
    parser.add_argument(
        "--start-date", type=str, metavar="YYYY-MM-DD",
        help="kr-backfill: 수집 시작일",
    )
    parser.add_argument(
        "--end-date", type=str, metavar="YYYY-MM-DD",
        help="kr-backfill: 수집 종료일",
    )

    # Drive & 기타
    parser.add_argument(
        "--upload-drive", action="store_true",
        help="수집 완료 후 Google Drive에 업로드",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="실제 저장 없이 수집 플로우만 테스트",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="현재 수집 현황만 출력 후 종료",
    )

    args = parser.parse_args()

    # 현황 출력 모드
    if args.status:
        from data import progress, storage
        progress.print_summary()
        storage.print_local_summary()
        return

    logger.info("=" * 60)
    logger.info(f"  Quant-Korea-Data 수집기 시작")
    logger.info(f"  모드: {args.mode} | DryRun: {args.dry_run}")
    logger.info("=" * 60)

    if args.mode == "daily":
        run_daily(args)
    elif args.mode == "bootstrap":
        run_bootstrap(args)
    elif args.mode == "ohlc-backfill":
        run_ohlc_backfill(args)
    elif args.mode == "ohlc-update":
        run_ohlc_update(args)
    elif args.mode == "ohlc-new-backfill":
        run_ohlc_new_backfill(args)
    elif args.mode == "financials-update":
        run_financials_update(args)
    elif args.mode == "kr-daily":
        run_kr_daily(args)
    elif args.mode == "kr-backfill":
        run_kr_backfill(args)
    elif args.mode == "sector-meta":
        run_sector_meta(args)


if __name__ == "__main__":
    # 로컬 임포트 경로 보장
    import os
    sys.path.insert(0, os.path.dirname(__file__))

    # storage를 main에서 직접 참조하기 위한 임포트
    from data import storage

    main()
