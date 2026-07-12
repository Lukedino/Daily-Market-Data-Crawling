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
import io
import sys
from datetime import date, datetime
from pathlib import Path

# Windows 콘솔 UTF-8 출력
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import ohlc_db


# ══════════════════════════════════════════════════════════════════════════════
# 시장별 분석
# ══════════════════════════════════════════════════════════════════════════════

def analyze_market(market: str, after: date) -> dict:
    """
    market의 티커별 최초 수집일을 조사해 after 이후 시작하는 티커를
    "신규 후보"(pending에 없음)와 "이미 pending"으로 분류한다.
    """
    first_dates = ohlc_db.first_seen_dates(market)
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

def print_report(results: list[dict], after: date):
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
        print("  python scripts/verify_ohlc.py --fix  # pending에 반영")
    else:
        print("\n  ✅ 신규 후보 없음")

    print()


# ══════════════════════════════════════════════════════════════════════════════
# pending 반영 (--fix)
# ══════════════════════════════════════════════════════════════════════════════

def apply_fix(results: list[dict]):
    """
    각 시장의 신규 후보("new")를 backfill_pending.json에 병합하고 Drive에 업로드한다.
    기존 pending 항목은 지우지 않고 합집합으로 합친다. 실제 fetch는 하지 않는다.
    """
    any_added = False

    for r in results:
        market = r["market"]
        new = r["new"]
        if not new:
            continue

        all_pending = ohlc_db.load_pending()
        merged = sorted(set(all_pending.get(market, [])) | set(new))
        all_pending[market] = merged
        ohlc_db.save_pending(all_pending)
        ohlc_db.upload_pending()

        print(f"  [{market.upper()}] pending에 {len(new)}개 추가 → 총 {len(merged)}개")
        any_added = True

    if any_added:
        print(
            "\npending 반영 완료 — ohlc-new-ticker-backfill 워크플로우를 "
            "실행하면 다음 회차에 자동 재시도됩니다."
        )
    else:
        print("\n반영할 신규 후보 없음")


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
        help="신규 후보를 backfill_pending.json에 반영 (Drive 업로드 포함, 실제 fetch는 안 함)"
    )
    args = parser.parse_args()

    try:
        after = datetime.strptime(args.after, "%Y-%m-%d").date()
    except ValueError:
        print(f"--after 형식 오류: {args.after} (YYYY-MM-DD 형식 필요)")
        return

    markets = ["us", "crypto"] if args.market == "all" else [args.market]

    if args.drive:
        print("Drive에서 최신 데이터 다운로드 중...")
        for market in markets:
            ohlc_db.download_all_years(market)
        ohlc_db.download_pending()

    results = []
    for market in markets:
        mdir = ohlc_db.local_dir(market)
        if not mdir.exists():
            print(f"로컬 {market} 폴더 없음: {mdir}")
            print("--drive 옵션으로 Drive에서 다운로드하세요.")
            continue
        results.append(analyze_market(market, after))

    if not results:
        return

    print_report(results, after)

    if args.fix:
        apply_fix(results)


if __name__ == "__main__":
    main()
