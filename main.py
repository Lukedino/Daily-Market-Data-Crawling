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

from data.execution_safety import writer_lock, configure_logging, cli_entry

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
    if args.dry_run:
        logger.info("dry_run")
        return

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
    if args.dry_run:
        logger.info("dry_run")
        return

    from data import historical, progress, storage

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
    if args.dry_run:
        logger.info("dry_run")
        return

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


def run_ohlc_rebase(args):
    """
    US/Crypto 전 종목 이력을 현재 조정 기준으로 한 번에 교체한다(수동, 2026-10-01).

    ohlc-backfill 은 저장본과 다른 가격을 손상으로 보는 가격 기준 검증에 막힌다 —
    옛 기준 행을 덮는 것이 목적이라 1,058종목 중 5% 넘게 다른 것이 당연하다
    (2026-10-01 run 36845205510: 첫 연도에서 price_basis_mismatch). 교체가 목적인
    rebase_action_tickers 를 유니버스 전체에 쓴다: 종목별로 전 연도를 받은 뒤에만
    교체하고(비거나 90% 미만이면 보류), 커서는 건드리지 않는다.
    """
    if args.dry_run:
        logger.info("dry_run")
        return

    from data import ohlc_collector, ohlc_db
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    if args.upload_drive:
        ohlc_db.download_pending()
    pending = ohlc_db.load_pending()
    failures = []
    for market in markets:
        try:
            # 보류 목록이 남아 있으면 그것만 이어서 처리한다 — 이미 교체한 종목을 다시 받지
            # 않고, 대량 요청이 줄어 야후의 빈 응답도 줄어든다(2026-10-01 첫 실행: US 1,058 중
            # 830 보류, 소량 재조회는 7개 연도 모두 저장본과 행 수 일치). 목록이 비면 전 종목.
            queued = pending.get(ohlc_collector.rebase_pending_key(market)) or []
            tickers = [] if queued else ohlc_collector.load_tickers(market)
            logger.info(f"[OhlcRebase] {market.upper()} {args.start_year}년~ "
                        f"{'보류분 ' + str(len(queued)) if queued else '전 종목 ' + str(len(tickers))}개 교체 시작")
            result = ohlc_collector.rebase_action_tickers(
                market, tickers, start_year=args.start_year, upload=args.upload_drive)
            logger.info(f"[OhlcRebase] {market.upper()} 교체 {len(result['replaced'])} / 보류 {len(result['deferred'])}")
        except Exception as error:
            code = getattr(error, "code", None)
            if code is None and len(error.args) == 1:
                code = error.args[0]
            logger.error("market_failed",
                         extra={"failure_code": code if isinstance(code, str) else None})
            failures.append(error)
    if failures:
        raise failures[0]

def run_ohlc_update(args):
    """
    US/Crypto OHLC 증분 업데이트.
    마지막 업데이트 이후 누락된 데이터를 수집.
    신규 종목이 유니버스에 추가된 경우, 증분 업데이트 전에 과거 이력을 먼저 백필한다.
    """
    if args.dry_run:
        logger.info("dry_run")
        return

    from data import ohlc_collector
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    failures = []
    for market in markets:
        logger.info(f"[OhlcUpdate] {market.upper()} 증분 업데이트 시작")
        if not args.dry_run:
            # 한 시장의 실패가 다른 시장의 수집까지 막지 않는다 — 2026-09-22 의
            # market=all 실행은 US 에서 죽어 크립토가 시작조차 못 했다. 실패는
            # 모아 두었다가 루프가 끝난 뒤 첫 건을 그대로 올려 잡을 실패시킨다.
            try:
                new_tickers = ohlc_collector.backfill_new_tickers(
                    market=market,
                    upload=args.upload_drive,
                )
                if new_tickers:
                    logger.info(f"[OhlcUpdate] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
                action_tickers = ohlc_collector.update_market(
                    market=market,
                    upload=args.upload_drive,
                )
                # 증분이 게시된 뒤에만 — 배당·분할 종목의 전체 이력 교체(DM-04). 이미 최신이라
                # 수집이 없었으면(None) 그날은 재수집 대상도 새로 생기지 않는다.
                if action_tickers is not None:
                    try:
                        ohlc_collector.rebase_action_tickers(
                            market, action_tickers, upload=args.upload_drive)
                    except ohlc_collector.CollectionIncompleteError as error:
                        # 그날 증분은 이미 게시됐다. 과거 이력 교체의 하드 실패(레이트리밋 등)로
                        # 잡 전체를 실패시키면 매일 거짓 실패 알림이 간다 — 대상은 재시도 목록에
                        # 남아 다음 실행이 이어 받는다(2026-10-01: 크립토 초저가·심볼 겹침 종목이
                        # 매번 하드 실패). 수동 ohlc-rebase 모드는 지금처럼 실패로 끝난다.
                        if error.code != "rebase_incomplete":
                            raise
                        logger.warning("rebase_deferred")
            except Exception as error:
                # raise failures[0] 는 첫 시장의 예외만 올린다 — 두 번째 시장의
                # 원인이 artifact 에서 통째로 사라지므로 시장마다 한 줄 남긴다.
                code = getattr(error, "code", None)
                if code is None and len(error.args) == 1:
                    code = error.args[0]
                logger.error("market_failed",
                             extra={"failure_code": code if isinstance(code, str) else None})
                failures.append(error)
        else:
            logger.info(f"[DryRun] {market.upper()} ohlc update 시뮬레이션")
    if failures:
        raise failures[0]


def run_ohlc_new_backfill(args):
    """
    US/Crypto 유니버스에 새로 추가된 종목만 골라 과거 이력을 백필한다.
    daily(ohlc-update)에서도 자동으로 실행되지만, 즉시 반영하고 싶을 때 수동으로 실행 가능.
    """
    if args.dry_run:
        logger.info("dry_run")
        return

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

def _recent_code_meta(prior, today, days: int = 7) -> dict:
    """
    직전 parquet에서 최근 `days`일 안에 등장한 종목의 {코드: {Name, Market}}.

    FDR StockListing이 죽어도(2026-09-08 상류 캐시 404) 이 정보만 있으면
    yfinance로 당일 시세를 받아올 수 있다 — 종목 목록·이름·시장 구분은 이미
    받아둔 parquet에 전부 들어있기 때문이다. 갭 backfill과 당일 폴백이 함께 쓴다.

    Args:
        prior: 해당 연도 marcap DataFrame (비어 있으면 빈 dict)
        today: 기준일 (이 날로부터 days일 이내에 거래된 종목만)
        days:  최근성 판단 창. 이보다 오래 안 보인 종목은 상장폐지로 본다.

    Returns:
        {"005930": {"Name": "삼성전자", "Market": "KOSPI"}, ...}
    """
    import pandas as pd

    if prior is None or prior.empty:
        return {}

    recent = prior[prior["Date"] >= pd.Timestamp(today) - pd.Timedelta(days=days)]
    if recent.empty:
        return {}

    recent = recent.assign(_Code=recent["Code"].astype(str).str.zfill(6))
    latest = recent.sort_values("Date").drop_duplicates(subset=["_Code"], keep="last")
    lookup = latest.set_index("_Code")[["Name", "Market"]].to_dict("index")
    return {
        code: {
            "Name": (meta or {}).get("Name", "") or "",
            "Market": (meta or {}).get("Market", "") or "KOSPI",
        }
        for code, meta in lookup.items()
    }


def _official_apply_mode() -> bool:
    import config
    return str(config.KR_OFFICIAL_SETTLE_MODE).strip().lower() == "apply"


def _settle_official_years(years, *, settle_from=None, apply=None, drop_dates=(), drop_keys=(),
                           reports=None):
    """게시 직전 공식값 정정. 반환: (정정으로 다시 쓴 연도, 공식 원천 실패 오류 또는 None).

    공식 원천을 못 받거나 형식이 틀리면 그 연도는 건드리지 않고 오류를 돌려준다 —
    호출자는 잠정 데이터를 게시한 뒤 그 오류로 실패해 알림을 보낸다.
    apply 를 주지 않으면 config.KR_OFFICIAL_SETTLE_MODE 를 따른다. 계획 모드는 쓰지 않는다.
    자동 경로는 계획 모드의 오류를 로그로만 남긴다(_automatic_official_error).
    """
    import config
    from data import kr_collector, kr_official
    floor = settle_from or config.KR_OFFICIAL_SETTLE_FROM
    apply = _official_apply_mode() if apply is None else apply
    settled, failure = [], None
    for year in sorted(set(years)):
        year_drops = [d for d in drop_dates if str(d).startswith(f"{int(year)}-")]
        year_keys = [(c, d) for c, d in drop_keys if str(d).startswith(f"{int(year)}-")]
        try:
            report = kr_official.settle_year(int(year), settle_from=floor, apply=apply,
                                             drop_dates=year_drops, drop_keys=year_keys)
        except (kr_collector.KrCollectionError, kr_db_errors()) as error:
            code = getattr(error, "code", None) or (error.args[0] if len(error.args) == 1 else None)
            logger.error("kr_official_failed", extra={"failure_code": code if isinstance(code, str) else None})
            failure = failure or error
            continue
        if reports is not None:
            reports.append(report)
        if report.get("written"):
            settled.append(int(year))
    return settled, failure


def _automatic_official_error(error):
    """자동 경로: 계획 모드(일괄 정정 승인 전)의 공식 원천 오류는 알림 없이 로그로만 둔다."""
    return error if _official_apply_mode() else None


def kr_db_errors():
    from data import kr_db
    return kr_db.KrCoverageError


def _official_staleness(year):
    """공식 확정이 지나치게 늦으면(잠정 거래일이 한도 초과) 실패 오류를 돌려준다."""
    import config
    from data import kr_official
    pending = kr_official.provisional_sessions(year, settle_from=config.KR_OFFICIAL_SETTLE_FROM)
    if pending > kr_official.STALE_AFTER_SESSIONS:
        logger.error("kr_official_stale", extra={"public_metrics": {"kr_provisional_sessions": pending}})
        return kr_official.OfficialStale()
    return None


def run_kr_settle(args):
    """공식값 정정만 실행한다(수동). --start-date 로 하한을 지정한다.

    과거 이력의 일괄 정정은 별도 승인 뒤, 예약 writer 와 겹치지 않는 시간에 실행한다.
    --plan 은 운영 쓰기와 같은 경로로 계획만 하고(Drive 기준본은 읽기만) 쓰지 않는다.
    --drop-closed-dates 는 휴장일 복사 행 삭제 — 날짜마다 kr_official.verify_closed_sessions 통과 필수.
    --report 는 연도별 계획·대조 결과를 JSON 으로 남긴다(로컬 파일, 업로드 안 함).
    """
    if args.dry_run:
        logger.info("dry_run")
        return
    plan = bool(getattr(args, "plan", False))
    if plan and args.upload_drive:
        logger.error("[KrSettle] --plan 과 --upload-drive 는 함께 쓸 수 없다")
        sys.exit(1)
    drops = [d.strip() for d in (getattr(args, "drop_closed_dates", None) or "").split(",") if d.strip()]
    key_text = [k.strip() for k in (getattr(args, "drop_prelisting_keys", None) or "").split(",") if k.strip()]
    import config
    import pandas as pd
    from datetime import date as _date
    from data import kr_db
    settle_from = args.start_date or config.KR_OFFICIAL_SETTLE_FROM
    try:
        floor = pd.Timestamp(_date.fromisoformat(settle_from))
    except ValueError:
        logger.error("[KrSettle] 날짜 형식 오류 (YYYY-MM-DD)")
        sys.exit(1)
    years = list(range(max(floor.year, config.KR_OFFICIAL_MIN_YEAR), _date.today().year + 1))
    try:
        drops = [_date.fromisoformat(d).isoformat() for d in drops]
        keys = []
        for item in key_text:
            code, day = item.split(":")
            if not (len(code) == 6 and code.isalnum()):
                raise ValueError
            keys.append((code, _date.fromisoformat(day).isoformat()))
    except ValueError:
        logger.error("[KrSettle] --drop-closed-dates / --drop-prelisting-keys 형식 오류")
        sys.exit(1)
    states = kr_db.ensure_year_baselines(years, download=args.upload_drive or plan)
    if "failed" in states.values():
        logger.error("[KrSettle] 기준 연도 파일 확인 실패 — 정정 없이 중단")
        sys.exit(1)
    targets = [year for year in years if states.get(year) == "ok"]
    reports = []
    settled, official_error = _settle_official_years(targets, settle_from=floor.date().isoformat(),
                                                     apply=not plan, drop_dates=drops, drop_keys=keys,
                                                     reports=reports)
    report_path = getattr(args, "report", None)
    if report_path:
        import json
        from pathlib import Path
        Path(report_path).write_text(json.dumps(reports, ensure_ascii=False, indent=2, default=str),
                                     encoding="utf-8")
    if plan:
        if official_error is not None:
            raise official_error
        return
    if args.upload_drive and settled:
        failed_files = kr_db.upload_years(settled)
        if failed_files:
            logger.error(f"[KrSettle] Drive 업로드 실패: {failed_files}")
            sys.exit(1)
    if official_error is not None:
        raise official_error


def _require_daily_coverage(df, reference, *, source):
    """오늘 수집분의 종목 수를 최근 정상 유니버스(인접 거래일 5개의 중앙값)와 비교한다."""
    import pandas as pd
    from data import kr_collector, kr_db
    dates = pd.to_datetime(df["Date"])
    day = dates.max()
    count = int(df.loc[dates == day, "Code"].astype(str).nunique())
    baseline = kr_db.coverage_baseline(kr_db.session_counts(reference), day, exclude={day})
    metrics = kr_db.coverage_metrics(day, count, baseline, source)
    if baseline is None:
        logger.warning("kr_coverage_unverified", extra={"public_metrics": metrics})
        return
    if baseline < kr_db.COVERAGE_MIN_UNIVERSE:
        return                                 # 합성·초기 데이터 — US 게이트와 같은 하한
    if kr_db.is_coverage_short(count, baseline):
        logger.error("kr_coverage_insufficient", extra={"public_metrics": metrics})
        raise kr_collector.KrCollectionError("kr_coverage_insufficient")
    logger.info("kr_coverage_checked", extra={"public_metrics": metrics})


def run_kr_daily(args, today=None):
    """
    KR daily 수집 플로우:
    0. KRX 휴장일이면 아무것도 하지 않는다(exit 0) — 2026-09-25(추석) 실패의 원인
    1. Drive에서 현재 연도 parquet + status 다운로드
    2. last_date 확인 → 어제까지 **거래일** 갭이 있으면 yfinance backfill 자동 수행
    3. 같은 원천 일자의 FDR 스냅샷 수집 (실제 원천 오류는 즉시 보류)
    4. 저장 + Drive 업로드

    today 는 테스트 주입용(기본 date.today()) — collect_kr_financials(today=) 와 같은 관례.

    ⚠️ 3번이 끝내 0건이면 sys.exit(1)로 끝낸다. 2026-09-08에는 조용히 return해서
       GHA가 success로 끝났고, 그래서 데이터 구멍이 하류(KIS EOD 분석)의 알림으로만
       드러났다. 워크플로우에 실패 알림을 붙여도 실패로 끝나지 않으면 뜨지 않는다.
    """
    if args.dry_run:
        logger.info("dry_run")
        return

    from datetime import date, timedelta
    from data import kr_collector, kr_db, krx_calendar
    import pandas as pd

    if args.dry_run:
        logger.info("[KrDaily] dry-run: 수집 시뮬레이션 (저장 없음)")
        return

    today = today or date.today()
    current_year = today.year

    # 0. 휴장일에는 수집할 세션이 없다. 2026-09-25(추석): 어제(09-24, 평일)가 저장본
    #    마지막 날짜(09-23)보다 뒤라 갭으로 판정 → yfinance 백필 → 야후는 휴장 구간에
    #    직전 거래일 봉을 돌려주므로 결과가 비어 있지 않음 → 가격 기준 검증이 yfinance
    #    출처를 거부 → exit 1(실패 알림). 휴장일은 갭도 수집 대상도 아니다.
    session, reason = krx_calendar.session_status(today)
    if session == "closed":
        logger.info("kr_market_closed")
        logger.info(f"[KrDaily] KRX 휴장({reason}) — 수집 없음")
        return
    if session == "unknown":
        logger.warning("kr_calendar_unavailable")

    # 1. Existing local files do not prove that the remote baseline is current.
    baseline_states = kr_db.ensure_year_baselines([current_year], download=args.upload_drive)
    if "failed" in baseline_states.values():
        logger.error("[KrDaily] 기준 파일 확인 실패 — 수집/저장/업로드 없이 중단한다")
        sys.exit(1)

    # 1b. 폴백용 종목 목록 — FDR이 죽어도 쓸 수 있도록 미리 뽑아둔다.
    #     갭 backfill(_build_universe)과 당일 폴백이 둘 다 FDR에 의존하고 있어서
    #     2026-09-08에는 폴백 경로 자체가 같은 404로 막혀 있었다.
    # 저장본은 폴백 유니버스이자 yfinance 후보의 가격 기준 대조 기준(reference)이다.
    reference = kr_db.load_year(current_year, strict=True)
    # 종목 범위 게이트의 기준. 연초에는 같은 해 거래일이 모자라므로 전년도 기준을 붙인다
    # (전년도 기준을 못 받으면 같은 해 기준만 — 오늘 수집을 그 때문에 막지는 않는다).
    coverage_reference = reference
    if len(kr_db.session_counts(reference)) < kr_db.COVERAGE_BASELINE_SESSIONS:
        prev_year = current_year - 1
        if kr_db.ensure_year_baselines([prev_year], download=args.upload_drive).get(prev_year) == "ok":
            coverage_reference = pd.concat([kr_db.load_year(prev_year), reference], ignore_index=True)
    fallback_meta = _recent_code_meta(reference, today)
    if fallback_meta:
        logger.info(f"[KrDaily] 폴백 유니버스 확보: {len(fallback_meta)}종목 (기존 parquet)")

    # 2. 갭 감지 → 자동 backfill
    last_date = kr_db.get_last_date(current_year)
    if last_date is None:
        # 파일 자체가 없는 경우 — 연초부터 어제까지 백필
        gap_start = date(current_year, 1, 1)
    else:
        gap_start = last_date + timedelta(days=1)

    yesterday = today - timedelta(days=1)

    if gap_start <= yesterday:
        # 주말·KRX 휴장일만 있는 구간이면 갭이 아니다. 예전엔 pd.bdate_range(평일)로
        # 판정해 추석 다음 날 09-24 를 갭으로 세었다(위 0 단계 주석). 달력을 모르는
        # 해는 평일을 거래일로 본다 — 기존 동작.
        bdays = krx_calendar.trading_days(gap_start, yesterday)
        if len(bdays) > 0:
            logger.info(
                f"[KrDaily] 갭 감지: {gap_start} ~ {yesterday} "
                f"({len(bdays)} 거래일) → yfinance backfill 시작"
            )
            gap_df = kr_collector.collect_backfill(str(gap_start), str(yesterday),
                                                   fallback_meta=fallback_meta,
                                                   reference=reference)
            if not gap_df.empty:
                kr_collector.validate_price_basis(gap_df)
                gap_updated = kr_db.append_rows(gap_df, ohlc_only=True)
                logger.info(f"[KrDaily] 갭 보완 완료: {gap_updated}년 파일 업데이트")
            else:
                logger.warning("[KrDaily] 갭 backfill 수집 결과 없음")
        else:
            logger.info(f"[KrDaily] 갭 없음 (주말만 존재: {gap_start} ~ {yesterday})")
    else:
        logger.info(f"[KrDaily] 갭 없음 — last_date: {last_date}")

    # 3. 오늘 FDR 스냅샷 수집
    logger.info("[KrDaily] 오늘 스냅샷 수집 (FDR StockListing)")
    source_error = None
    try:
        df = kr_collector.collect_daily()
    except kr_collector.KrCollectionError as error:
        # FDR StockListing은 KRX가 아니라 제3자 GitHub 캐시 저장소의 날짜별 CSV를
        # 읽는다. 그쪽이 그날치를 안 올리면 세 시장 전부 404이고(2026-09-08),
        # collect_daily 는 빈 프레임이 아니라 typed 오류(kr_source_failed 등)를 던진다.
        # 2026-09-25 결정 B(검증된 폴백)가 겨냥한 상황이 바로 이것인데, 아래 폴백이
        # 빈 프레임 분기에만 걸려 있어 실제 장애에서는 한 번도 실행되지 않았다
        # (2026-09-26 확인). 원천 오류 코드는 공개 로그에 남기고 폴백을 시도한다 —
        # 폴백은 저장본과 종목별로 대조된 행만 받고, 비면 같은 typed 오류로 끝난다.
        source_error = error
        code = error.args[0] if len(error.args) == 1 else None
        logger.warning("kr_source_fallback_attempted",
                       extra={"failure_code": code if isinstance(code, str) else None})
        logger.warning(f"[KrDaily] FDR 스냅샷 실패({code}) → yfinance 검증 폴백 시도")
        df = pd.DataFrame()

    used_fallback = False
    if df.empty:
        # 빈 결과 호환경로와 typed 실패 둘 다 여기로 온다. 아래 가격 기준 검증 전에는
        # 저장하지 않는다.
        logger.warning("[KrDaily] FDR 스냅샷 0건 → yfinance 전량 폴백 시도")
        if fallback_meta:
            df = kr_collector.collect_daily_fallback(fallback_meta, today, reference=reference)
            used_fallback = not df.empty
        else:
            logger.error("[KrDaily] 폴백 유니버스도 비어 있음 (기존 parquet 없음)")

    if df.empty:
        if source_error is not None:
            raise source_error            # 원천 실패 코드 그대로 — 실패 알림이 원인을 싣는다
        logger.error("[KrDaily] 오늘 수집 실패 (FDR·yfinance 모두 0건) → 실패로 종료")
        sys.exit(1)

    if used_fallback:
        logger.warning(
            f"[KrDaily] yfinance 폴백으로 수집: {len(df):,}종목 "
            f"(Marcap/Rank 없음 — FDR 복구 후 재수집 대상)"
        )

    # 3b. 누락 종목 yfinance 보완
    #   FDR StockListing은 매매정지·관리종목 등 일부 활성 종목을 누락하는 경우가 있어,
    #   직전 영업일 parquet에 있었으나 오늘 결과에 없는 종목을 yfinance로 재시도.
    #   마지막 거래일이 너무 오래된 종목(>7일)은 상장폐지로 간주하고 스킵.
    try:
        prior = pd.DataFrame() if used_fallback else kr_db.load_year(current_year)
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
                supp = kr_collector.collect_missing_today(missing_codes, code_meta, today,
                                                         reference=reference)
                if not supp.empty:
                    kr_collector.validate_price_basis(supp)
                    df = pd.concat([df, supp], ignore_index=True)
                    df = df.drop_duplicates(subset=["Code", "Date"], keep="first")
                    logger.info(
                        f"[KrDaily] 보완 merge 완료: {len(supp)}종목 추가 (총 {len(df)}종목)"
                    )
    except kr_collector.KrCollectionError:
        raise
    except Exception as e:
        logger.warning(f"[KrDaily] 누락 종목 보완 단계 실패 (무시하고 계속): {e}")

    # 4. 저장
    kr_collector.validate_price_basis(df)
    # 4a. 종목 범위 게이트(2026-10-10) — 폴백·누락 보완까지 거친 뒤에도 정상 유니버스의
    #     90% 미만이면 저장·게시하지 않고 실패한다. 10-01 에는 234/2,873종목이 저장되고
    #     success 로 끝나 하류 ML 이 7일 막혔다. 실패는 kr-daily.yml 텔레그램 알림으로 간다.
    _require_daily_coverage(df, coverage_reference,
                            source="yfinance_fallback" if used_fallback else "fdr_snapshot")
    updated = kr_db.append_rows(df, ohlc_only=used_fallback)

    # 4b. 공식값 정정(2026-10-10) — 게시 직전에 marcap 이 실제로 제공한 날짜를 공식 OHLCV 로
    #     바꾸고 빠진 종목·날짜를 채운다. 오늘 잠정 수집이 공식 날짜를 건드렸더라도 여기서
    #     되돌아가므로 게시본에는 공식값이 남는다. 1월엔 전년도 12월 잠정분도 정정한다.
    settle_years = [current_year]
    if today.month == 1:
        prev_year = current_year - 1
        if kr_db.ensure_year_baselines([prev_year], download=args.upload_drive).get(prev_year) == "ok":
            settle_years.insert(0, prev_year)
    settled, official_error = _settle_official_years(settle_years)
    official_error = _automatic_official_error(official_error)
    if official_error is None and _official_apply_mode():
        official_error = _official_staleness(current_year)
    updated = sorted(set(updated) | set(settled))

    # 5. Drive 업로드
    if args.upload_drive and updated:
        failed_files = kr_db.upload_years(updated)
        if failed_files:
            # 조용히 넘어가면 오늘의 Marcap·Rank 스냅샷이 Drive 에 없는 채로 초록색이 된다(D-02).
            logger.error(f"[KrDaily] Drive 업로드 실패: {failed_files}")
            sys.exit(1)
        logger.info(f"[KrDaily] Drive 업로드 완료: {updated}")

    last_saved = df["Date"].max()
    last_saved_date = last_saved.date() if hasattr(last_saved, "date") else last_saved
    existing = kr_db.load_status()
    total_days = existing.get("trading_days_total", 0) + 1
    kr_db.save_status(last_saved_date, total_days)
    if official_error is not None:
        # 잠정 데이터는 이미 게시됐다 — 정정 실패·지연만 실패로 알린다(→ 텔레그램).
        raise official_error


# ══════════════════════════════════════════════════════════════════════════════
# kr-backfill 모드
# ══════════════════════════════════════════════════════════════════════════════

def run_kr_backfill(args):
    """
    yfinance로 과거 누락 구간 KR OHLCV 백필.
    --start-date / --end-date 로 기간 지정.
    """
    if args.dry_run:
        logger.info("dry_run")
        return

    from data import kr_collector, kr_db
    import pandas as pd

    if not args.start_date or not args.end_date:
        logger.error("[KrBackfill] --start-date, --end-date 필수 (예: --start-date 2026-02-21)")
        sys.exit(1)

    # 날짜 형식 사전 검증 (잘못된 값으로 실행 방지)
    try:
        from datetime import datetime as _dt
        start_date = _dt.strptime(args.start_date, "%Y-%m-%d")
        end_date = _dt.strptime(args.end_date, "%Y-%m-%d")
        if start_date > end_date:
            raise ValueError("start_date must not exceed end_date")
    except ValueError as e:
        logger.error(f"[KrBackfill] 날짜 형식 오류: {e}  (YYYY-MM-DD 필요, 예: 2026-02-21)")
        sys.exit(1)

    logger.info(f"[KrBackfill] 기간: {args.start_date} ~ {args.end_date}")

    if args.dry_run:
        logger.info("[KrBackfill] dry-run: 수집 시뮬레이션 (저장 없음)")
        return

    # The complete requested range must be safe before contacting a collector.
    # Download even when a local file exists: its presence says nothing about
    # whether the current remote baseline was obtained successfully.
    baseline_states = kr_db.ensure_year_baselines(
        range(start_date.year, end_date.year + 1), download=args.upload_drive,
    )
    if "failed" in baseline_states.values():
        logger.error("[KrBackfill] 기준 연도 파일 확인 실패 — 수집/저장/업로드 없이 중단")
        sys.exit(1)

    # 수동 백필도 대조 기준이 필요하다 — 요청 구간이 걸친 연도의 저장본을 합친다.
    reference_frames = []
    for year in range(start_date.year, end_date.year + 1):
        try:
            frame = kr_db.load_year(year, strict=True)
        except Exception:
            frame = pd.DataFrame()
        if frame is not None and not frame.empty:
            reference_frames.append(frame)
    reference = pd.concat(reference_frames, ignore_index=True) if reference_frames else None
    df = kr_collector.collect_backfill(args.start_date, args.end_date, reference=reference)
    if df.empty:
        logger.error("[KrBackfill] 수집 실패 → 종료")
        sys.exit(1)

    try:
        collected_dates = pd.to_datetime(df["Date"], errors="raise")
        if collected_dates.isna().any() or not collected_dates.between(start_date, end_date).all():
            raise ValueError("collector returned dates outside the verified range")
    except (KeyError, TypeError, ValueError) as error:
        logger.error(f"[KrBackfill] 수집 날짜 검증 실패: {type(error).__name__}")
        sys.exit(1)

    kr_collector.validate_price_basis(df)
    updated = kr_db.append_rows(df, ohlc_only=True)
    # 수동 백필(yfinance)이 공식 날짜를 덮었으면 게시 전에 공식값으로 되돌린다.
    settled, official_error = _settle_official_years(updated)
    official_error = _automatic_official_error(official_error)
    updated = sorted(set(updated) | set(settled))

    if args.upload_drive and updated:
        failed_files = kr_db.upload_years(updated)
        if failed_files:
            logger.error(f"[KrBackfill] Drive 업로드 실패: {failed_files}")
            sys.exit(1)
        logger.info(f"[KrBackfill] Drive 업로드 완료: {updated}")
    if official_error is not None:
        raise official_error

    # Failed publication must not be recorded as a successful backfill.
    last_date = collected_dates.max()
    status = kr_db.load_status()
    total_days = status.get("trading_days_total", 0)
    kr_db.save_status(last_date.date(), total_days)


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
    if args.dry_run:
        logger.info("dry_run")
        return

    from data import ohlc_collector, ohlc_db
    markets = ["us", "crypto"] if args.market == "all" else [args.market]
    for market in markets:
        ohlc_db.load_sector_meta(market)
        if args.upload_drive:
            ohlc_db.download_sector_meta(market)
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
            raise ohlc_db.DriveSyncError("sector_collection_empty")


def run_financials_update(args):
    """
    US financials + ratios, Crypto ratios, KR financials(DART) 수집 및 Drive 업로드.
    --market us|crypto|kr|all 옵션 지원.
    """
    if args.dry_run:
        logger.info("dry_run")
        return

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
    from data.drive_uploader import DriveUploader
    DriveUploader().sync_all_local()


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
        choices=["daily", "bootstrap", "ohlc-backfill", "ohlc-rebase", "ohlc-update", "ohlc-new-backfill",
                 "financials-update", "kr-daily", "kr-backfill", "kr-settle", "sector-meta"],
        required=True,
        help=(
            "daily: 오늘 수집 / bootstrap: 과거 연도 일괄 수집 / "
            "ohlc-backfill: US/Crypto OHLC 초기 적재 / "
            "ohlc-rebase: US/Crypto 전 종목 이력을 현재 조정 기준으로 교체 (수동 1회) / "
            "ohlc-update: US/Crypto OHLC 증분 업데이트 / "
            "ohlc-new-backfill: US/Crypto 신규 종목만 골라 과거 이력 백필 / "
            "financials-update: US 재무제표 + Crypto 시장 데이터 수집 / "
            "kr-daily: KR 오늘 스냅샷 수집 (FDR) / "
            "kr-backfill: KR 과거 누락 구간 수집 (yfinance) / "
            "kr-settle: KR 잠정값을 공식 일봉(FinanceData/marcap)으로 정정 (--start-date 하한) / "
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
        "--plan", action="store_true",
        help="kr-settle: 운영과 같은 경로로 계획·대조만 하고 쓰지 않음",
    )
    parser.add_argument(
        "--drop-closed-dates", type=str, metavar="YYYY-MM-DD,...",
        help="kr-settle: 검증된 휴장일 복사 행 삭제 대상 날짜(KRX 달력·직전일 복사·공식 부재 확인)",
    )
    parser.add_argument(
        "--drop-prelisting-keys", type=str, metavar="CODE:YYYY-MM-DD,...",
        help="kr-settle: 검증된 상장 전 무거래 행 삭제 대상 키(공식 첫 등장일 이후·거래량 0 확인)",
    )
    parser.add_argument(
        "--report", type=str, metavar="PATH",
        help="kr-settle: 연도별 계획·대조 결과 JSON 저장 경로",
    )
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

    if args.dry_run:
        logger.info("dry_run")
        return 0
    with writer_lock():
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
        elif args.mode == "ohlc-rebase":
            run_ohlc_rebase(args)
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
        elif args.mode == "kr-settle":
            run_kr_settle(args)
        elif args.mode == "sector-meta":
            run_sector_meta(args)


if __name__ == "__main__":
    configure_logging("collection.log")
    raise SystemExit(cli_entry(main))
