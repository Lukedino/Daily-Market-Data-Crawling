"""
scripts/verify_kr.py — KR DB 현황 검증 + 누락 구간 출력

사용법:
  python scripts/verify_kr.py            # 로컬 파일만 검증
  python scripts/verify_kr.py --drive    # Drive에서 다운로드 후 검증

출력 예시:
  === KR DB 현황 ===
  marcap-2025.parquet : 696,524행 / 242거래일 / 2025-01-02 ~ 2025-12-30 ✅
  marcap-2026.parquet :  95,412행 /  33거래일 / 2026-01-02 ~ 2026-02-20
    ⚠️  누락 구간: 2026-02-21 ~ 2026-04-03 (30 영업일)
"""

import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from data import kr_db
from data.execution_safety import writer_lock, configure_logging, cli_entry


# ══════════════════════════════════════════════════════════════════════════════
# 영업일 계산 (한국 공휴일 미반영 — 주말만 제외, 근사치)
# ══════════════════════════════════════════════════════════════════════════════

def _bdays(start: date, end: date) -> list[date]:
    """start ~ end 사이 평일(월~금) 목록 반환."""
    days = pd.bdate_range(str(start), str(end))
    return [d.date() for d in days]


# ══════════════════════════════════════════════════════════════════════════════
# 단일 연도 파일 분석
# ══════════════════════════════════════════════════════════════════════════════

def analyze_year(year: int) -> dict:
    """연도별 parquet 분석 → 결과 dict 반환."""
    path = kr_db.local_path(year)
    result = {"year": year, "path": path, "exists": path.exists()}

    if not path.exists():
        return result

    try:
        df = kr_db.load_year(year, strict=True)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        trading_dates = sorted(df["Date"].unique())
        result["rows"] = len(df)
        result["trading_days"] = len(trading_dates)
        result["first_date"] = trading_dates[0] if trading_dates else None
        result["last_date"] = trading_dates[-1] if trading_dates else None
        result["ticker_count"] = df["Code"].nunique()
        result["dates_set"] = set(trading_dates)

    except Exception as e:
        result["error"] = "kr_baseline_invalid"

    return result


# ══════════════════════════════════════════════════════════════════════════════
# 누락 구간 감지
# ══════════════════════════════════════════════════════════════════════════════

def find_gaps(analysis: dict) -> list[tuple[date, date]]:
    """
    수집된 날짜 집합과 예상 영업일을 비교해 누락 구간을 반환.
    반환: [(gap_start, gap_end), ...]  — 연속 구간으로 묶음
    """
    if not analysis.get("first_date") or not analysis.get("last_date"):
        return []

    today = date.today()
    # 분석 기준 종료일: 해당 연도 마지막 날 vs 어제 중 작은 값
    year = analysis["year"]
    year_end = date(year, 12, 31)
    check_end = min(year_end, today - timedelta(days=1))

    expected = set(_bdays(analysis["first_date"], check_end))
    actual = analysis.get("dates_set", set())
    missing = sorted(expected - actual)

    if not missing:
        return []

    # 연속 구간으로 묶기
    gaps = []
    gap_start = missing[0]
    prev = missing[0]
    for d in missing[1:]:
        expected_next = _bdays(prev, d)
        # 연속이 아닌 경우 (사이에 평일이 없으면 연속으로 간주)
        if len(expected_next) > 2:
            gaps.append((gap_start, prev))
            gap_start = d
        prev = d
    gaps.append((gap_start, prev))
    return gaps


# ══════════════════════════════════════════════════════════════════════════════
# 출력
# ══════════════════════════════════════════════════════════════════════════════

def print_report(analyses: list[dict]):
    today = date.today()
    print()
    print("=" * 60)
    print("  KR DB 현황 (marcap 스키마)")
    print(f"  기준일: {today}")
    print("=" * 60)

    all_gaps = []

    for a in analyses:
        year = a["year"]
        fname = f"marcap-{year}.parquet"

        if not a.get("exists"):
            print(f"  {fname} : ❌ 파일 없음")
            continue

        if "error" in a:
            print(f"  {fname} : ❌ 읽기 오류 — {a['error']}")
            continue

        first = a["first_date"]
        last = a["last_date"]
        rows = a["rows"]
        days = a["trading_days"]
        tickers = a["ticker_count"]

        gaps = find_gaps(a)

        status = "✅" if not gaps else "⚠️ "
        print(
            f"  {fname} : {rows:>8,}행 / {days:>3}거래일 / "
            f"{first} ~ {last}  {status}  ({tickers}종목)"
        )

        for gs, ge in gaps:
            bdays_in_gap = len(_bdays(gs, ge))
            print(f"    └─ 원천 확인 필요 후보(공휴일 미반영): {gs} ~ {ge}  ({bdays_in_gap} 영업일)")
            all_gaps.append((year, gs, ge, bdays_in_gap))

    print("=" * 60)

    if all_gaps:
        print("\n  [보완 명령어]")
        for year, gs, ge, cnt in all_gaps:
            print(
                f"  python main.py --mode kr-backfill "
                f"--start-date {gs} --end-date {ge} --upload-drive"
                f"  # {cnt} 영업일"
            )
    else:
        print("\n  ✅ 누락 없음")

    print()
    return all_gaps


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="KR DB 현황 검증")
    parser.add_argument(
        "--drive", action="store_true",
        help="Drive에서 최신 파일 다운로드 후 검증"
    )
    parser.add_argument(
        "--fix", action="store_true",
        help="누락 구간을 자동으로 backfill (--drive 와 함께 사용 권장)"
    )
    args = parser.parse_args()

    with writer_lock():
        return verify(args)


def verify(args):
    # Drive에서 다운로드
    if args.drive:
        print("Drive에서 KR parquet 다운로드 중...")
        kr_db.download_all()

    # 파일 목록 수집 (로컬)
    local_root = kr_db._LOCAL_ROOT
    if not local_root.exists():
        print("로컬 KR 폴더 없음")
        if not args.drive:
            print("--drive 옵션으로 Drive에서 다운로드하세요.")
        return 1

    parquet_files = sorted(local_root.glob("marcap-*.parquet"))
    if not parquet_files:
        print("로컬에 marcap-*.parquet 파일 없음")
        return 1

    years = []
    for f in parquet_files:
        match = re.fullmatch(r"marcap-([0-9]{4})\.parquet", f.name)
        if not match:
            raise RuntimeError("kr_local_filename_invalid")
        years.append(int(match.group(1)))

    analyses = [analyze_year(y) for y in sorted(years)]
    if any("error" in item for item in analyses):
        raise RuntimeError("kr_baseline_invalid")
    all_gaps = print_report(analyses)

    # 누락 자동 보완
    if args.fix and all_gaps:
        print("누락 구간 자동 backfill 시작...")
        from data import kr_collector

        states = kr_db.ensure_year_baselines([item[0] for item in all_gaps], download=args.drive)
        if "failed" in states.values():
            raise RuntimeError("kr_baseline_failed")
        for year, gs, ge, cnt in all_gaps:
            print(f"\n  → {gs} ~ {ge} ({cnt} 영업일) 수집 중...")
            df = kr_collector.collect_backfill(str(gs), str(ge))
            if not df.empty:
                kr_collector.validate_price_basis(df)
                updated = kr_db.append_rows(df, ohlc_only=True)
                print(f"     저장 완료: {updated}년 파일 업데이트")
            else:
                raise RuntimeError("kr_collection_empty_unverified")

        print("\n✅ backfill 완료 — 재검증:")
        analyses2 = [analyze_year(y) for y in sorted(years)]
        print_report(analyses2)


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(cli_entry(main))
