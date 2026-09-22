"""
scripts/resave_ohlc.py — US/Crypto 연도 파일을 현재 save_year() 규칙으로 재저장.

야후 재조회 없이 Drive 원본을 받아 병합·중복 제거·휴장일 행 제거·축소 가드·
연속성 게이트를 다시 통과시킨 뒤 기록한다. save_year() 의 규칙이 바뀌어 기존
파일을 소급 정리해야 할 때 쓴다. 첫 용도: [BUG-FOREIGN-CALENDAR] (2026-09-08) —
U-UN.TO(TSX) 백필이 2020~2026 US 파일에 남긴 미국 휴장일 행 34개 제거.

사용:
  python scripts/resave_ohlc.py --market us                      # Drive 다운로드 → 재저장 → 검증 (업로드 없음)
  python scripts/resave_ohlc.py --market us --upload             # 검증 통과 시 Drive 갱신
  python scripts/resave_ohlc.py --market us --years 2025 2026    # 연도 한정

환경: GOOGLE_SERVICE_ACCOUNT_JSON(또는 GOOGLE_APPLICATION_CREDENTIALS) + GDRIVE_OHLC_FOLDER_ID.
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.execution_safety import writer_lock, configure_logging, cli_entry
logger = logging.getLogger("resave_ohlc")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--market", choices=["us", "crypto"], required=True)
    ap.add_argument("--years", nargs="*", type=int, default=None, help="기본: Drive 에 있는 모든 연도 파일")
    ap.add_argument("--upload", action="store_true", help="검증 통과 시 Drive 업로드")
    args = ap.parse_args()

    with writer_lock():
        return resave(args)


def resave(args):
    from data import ohlc_db

    ohlc_db.download_all_years(args.market)
    years = args.years or sorted(
        int(p.stem.split("_")[-1]) for p in ohlc_db.local_dir(args.market).glob(f"{args.market}_*.parquet")
        if p.stem.split("_")[-1].isdigit()
    )

    if not years:
        raise ohlc_db.DriveSyncError("resave_files_absent")
    baselines = {}
    for year in years:
        if not ohlc_db.local_path(args.market, year).is_file():
            raise ohlc_db.DriveSyncError("resave_requested_file_absent")
        baselines[year] = ohlc_db.load_year(args.market, year, strict=True)
    rows = []
    for y in years:
        before = baselines[y]
        if before.empty:
            logger.info(f"{args.market}_{y}: 비어 있음 → 건너뜀")
            continue
        b_bad = ohlc_db.check_coverage_continuity(before)["n_bad"]
        # 자기 자신과 병합 → 중복 제거 → (US) 휴장일 행 제거 → 축소 가드 → 연속성 게이트 → 기록
        ohlc_db.save_year(before, args.market, y)
        after = ohlc_db.load_year(args.market, y, strict=True)
        a_bad = ohlc_db.check_coverage_continuity(after)["n_bad"]
        rows.append((y, len(before), b_bad, len(after), a_bad,
                     after["Ticker"].nunique(), str(after["Date"].max())))

    print("\nyear  rows_before  bad_before  rows_after  bad_after  tickers  last_date")
    for r in rows:
        print("  ".join(str(x) for x in r))

    remaining = [r[0] for r in rows if r[4]]
    if remaining:
        logger.error(f"재저장 후에도 급감일이 남음: {remaining} → 업로드하지 않음")
        return 1
    if args.upload:
        failed = ohlc_db.upload_years(args.market, [r[0] for r in rows])
        if failed:
            raise ohlc_db.DriveSyncError("resave_publication_failed")
        logger.info(f"Drive 업로드 완료: {args.market} {[r[0] for r in rows]}")
    return 0


if __name__ == "__main__":
    configure_logging()
    sys.exit(cli_entry(main))
