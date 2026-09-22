"""
scripts/verify_ohlc.py — US/Crypto 백필 완결성 검증

사용법:
  python scripts/verify_ohlc.py --market us              # 로컬 파일만 검증
  python scripts/verify_ohlc.py --market all --drive     # Drive에서 다운로드 후 검증
  python scripts/verify_ohlc.py --market us --after 2024-06-01

출력 예시:
  [US]
    ⚠️  DNLI         최초 수집일 2026-01-02  (조치 필요)
    ⏳ TSM          최초 수집일 2026-04-21  (이미 pending)
"""

import argparse
import json
import re
import tempfile
import sys
from datetime import date, datetime
from pathlib import Path

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import ohlc_db
from data.execution_safety import writer_lock, configure_logging, cli_entry


# ══════════════════════════════════════════════════════════════════════════════
# 시장별 분석
# ══════════════════════════════════════════════════════════════════════════════

def analyze_market(market: str, after: date) -> dict:
    """
    market의 티커별 최초 수집일을 조사해 after 이후 시작하는 티커를
    "신규 후보"(pending에 없음)와 "이미 pending"으로 분류한다.
    """
    # A corrupt older partition must not make a ticker look newly collected.
    first_dates = {}
    files = []
    for path in sorted(ohlc_db.local_dir(market).glob(f"{market}_*.parquet")):
        match = re.fullmatch(rf"{re.escape(market)}_([0-9]{{4}})\.parquet", path.name)
        if match:
            files.append((path, int(match.group(1))))
    if not files:
        raise ohlc_db.DriveSyncError("ohlc_baseline_absent")
    for path, year in files:
        frame = ohlc_db.load_year(market, year, strict=True)
        if frame.empty:
            continue
        for ticker, day in frame.groupby("Ticker")["Date"].min().items():
            first_dates[ticker] = min(first_dates.get(ticker, day), day)
    pending = set(ohlc_db.load_pending().get(market, []))

    suspects = {t: d for t, d in first_dates.items() if d > after}

    return {
        "market": market,
        "new": sorted(set(suspects) - pending),
        "already_pending": sorted(set(suspects) & pending),
        "first_dates": suspects,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 출력
# ══════════════════════════════════════════════════════════════════════════════

def print_report(results: list[dict], after: date, market_arg: str, after_arg: str):
    print()
    print("=" * 60)
    print("  US/Crypto 백필 완결성 검증")
    print(f"  기준일(--after): {after} 이후 시작하는 티커를 의심 후보로 분류")
    print("=" * 60)

    total_new = 0

    for r in results:
        market = r["market"]
        new = r["new"]
        already = r["already_pending"]
        first_dates = r["first_dates"]
        total_new += len(new)

        print(f"\n  [{market.upper()}]")
        if not new and not already:
            print("    ✅ 의심 후보 없음")
            continue

        for t in new:
            print(f"    ⚠️  {t:<12} 최초 수집일 {first_dates[t]}  (조치 필요)")
        for t in already:
            print(f"    ⏳ {t:<12} 최초 수집일 {first_dates[t]}  (이미 pending)")

    print()
    print("=" * 60)

    if total_new:
        print(f"\n  [보완 안내] 신규 후보 {total_new}개 발견")
        print(f"  python scripts/verify_ohlc.py --market {market_arg} --after {after_arg} --drive --fix  # pending에 반영")
    else:
        print("\n  ✅ 신규 후보 없음")

    print()


# ══════════════════════════════════════════════════════════════════════════════
# pending 반영 (--fix)
# ══════════════════════════════════════════════════════════════════════════════

def pending_baseline():
    """Read remote pending into staging; never overwrite unpublished local intent."""
    import config
    local = ohlc_db.load_pending()
    uploader = ohlc_db._get_uploader()
    remote = config.DRIVE_PATHS.get("ohlc_meta")
    if uploader is None or not remote:
        raise ohlc_db.DriveSyncError("pending_baseline_unavailable")
    with tempfile.TemporaryDirectory(prefix="pending-baseline-") as folder:
        target = Path(folder) / "backfill_pending.json"
        try:
            result = uploader.download(remote, target.name, str(target))
        except FileNotFoundError:
            return local
        if result is False or not target.is_file():
            raise ohlc_db.DriveSyncError("pending_baseline_failed")
        try:
            incoming = json.loads(target.read_text(encoding="utf-8"))
            if not isinstance(incoming, dict) or any(
                    not isinstance(items, list) or any(not isinstance(item, str) for item in items)
                    for items in incoming.values()):
                raise ValueError()
        except (ValueError, TypeError):
            raise ohlc_db.DriveSyncError("pending_baseline_invalid") from None
    return {market: sorted(set(local.get(market, [])) | set(incoming.get(market, [])))
            for market in local.keys() | incoming.keys()}


def apply_fix(results: list[dict]):
    # One verified union and one confirmed publication for all markets.
    pending = pending_baseline()
    for result in results:
        market = result["market"]
        pending[market] = sorted(set(pending.get(market, [])) | set(result["new"]))
    ohlc_db.publish_pending(pending, upload=True)
    print("pending 후보 게시 확인 완료; 역사 누락 확정은 별도 원천 확인이 필요합니다.")


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="US/Crypto 백필 완결성 검증")
    parser.add_argument(
        "--market", choices=["us", "crypto", "all"], default="all",
        help="검증할 시장 (기본: all)"
    )
    parser.add_argument(
        "--after", type=str, default="2024-01-01", metavar="YYYY-MM-DD",
        help="이 날짜 이후에 최초 데이터가 시작되는 티커를 의심 후보로 분류 (기본: 2024-01-01)"
    )
    parser.add_argument(
        "--drive", action="store_true",
        help="Drive에서 최신 parquet + backfill_pending.json 다운로드 후 검증"
    )
    parser.add_argument(
        "--fix", action="store_true",
        help="신규 후보를 backfill_pending.json에 반영 (--drive 와 함께 사용 권장, Drive 업로드 포함)"
    )
    args = parser.parse_args()

    try:
        after = datetime.strptime(args.after, "%Y-%m-%d").date()
    except ValueError:
        print(f"--after 형식 오류: {args.after} (YYYY-MM-DD 형식 필요)")
        return 1

    with writer_lock():
        return verify(args, after)


def verify(args, after):
    markets = ["us", "crypto"] if args.market == "all" else [args.market]

    if args.drive:
        print("Drive에서 최신 데이터 다운로드 중...")
        for market in markets:
            ohlc_db.download_all_years(market)
        ohlc_db.save_pending(pending_baseline())

    results = []
    for market in markets:
        mdir = ohlc_db.local_dir(market)
        if not mdir.exists():
            print(f"로컬 {market} 폴더 없음")
            print("--drive 옵션으로 Drive에서 다운로드하세요.")
            continue
        results.append(analyze_market(market, after))

    if not results:
        return 1

    print_report(results, after, args.market, args.after)

    if args.fix:
        apply_fix(results)


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(cli_entry(main))
