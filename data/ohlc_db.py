"""
data/ohlc_db.py — US/Crypto OHLC 로컬 Parquet DB 관리

저장 구조:
  data/local/ohlc_db/{market}/{market}_{year}.parquet
  data/local/ohlc_db/db_status.json

스키마: Ticker | Date | Open | High | Low | Close | Volume
압축: snappy (pyarrow.parquet 기본)
"""

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config

logger = logging.getLogger(__name__)

_LOCAL_ROOT = Path(config.LOCAL_DATA_DIR) / "ohlc_db"
_STATUS_PATH = _LOCAL_ROOT / "db_status.json"
_PENDING_PATH = _LOCAL_ROOT / "backfill_pending.json"

# Parquet 컬럼 순서 (스키마 고정)
_SCHEMA_COLS = [
    "Ticker", "Date", "Open", "High", "Low", "Close", "Volume",
    "Amount", "ChangesRatio", "MarketCap", "Dividends", "Splits",
]


# ══════════════════════════════════════════════════════════════════════════════
# 경로 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def local_dir(market: str) -> Path:
    """시장별 로컬 디렉터리 경로."""
    return _LOCAL_ROOT / market


def local_path(market: str, year: int) -> Path:
    """연도별 Parquet 파일 경로."""
    return local_dir(market) / f"{market}_{year}.parquet"


# ══════════════════════════════════════════════════════════════════════════════
# 읽기 / 저장
# ══════════════════════════════════════════════════════════════════════════════

def load_year(market: str, year: int) -> pd.DataFrame:
    """연도별 Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_path(market, year)
    if not path.exists():
        return pd.DataFrame(columns=_SCHEMA_COLS)
    try:
        df = pq.read_table(str(path)).to_pandas()
        # Date 컬럼을 date 타입으로 통일
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
        return df
    except Exception as e:
        logger.error(f"[OhlcDB] {path.name} 로드 실패: {e}")
        return pd.DataFrame(columns=_SCHEMA_COLS)


def list_known_tickers(market: str) -> set[str]:
    """
    로컬 연도별 parquet 파일들({market}_YYYY.parquet)에서
    한 번이라도 수집된 Ticker 집합을 반환.
    {market}_sector_meta.parquet 등 연도 파일이 아닌 것은 제외한다.
    """
    mdir = local_dir(market)
    if not mdir.exists():
        return set()

    year_pattern = re.compile(rf"^{re.escape(market)}_\d{{4}}\.parquet$")
    known: set[str] = set()

    for pfile in sorted(mdir.glob(f"{market}_*.parquet")):
        if not year_pattern.match(pfile.name):
            continue
        try:
            table = pq.read_table(str(pfile), columns=["Ticker"])
            known.update(table.column("Ticker").to_pylist())
        except Exception as e:
            logger.warning(f"[OhlcDB] {pfile.name} Ticker 컬럼 읽기 실패: {e}")

    return known


def first_seen_dates(market: str) -> dict[str, date]:
    """
    로컬 연도별 parquet 파일들({market}_YYYY.parquet)에서
    티커별 최초 수집일(Date의 최솟값)을 반환.
    """
    mdir = local_dir(market)
    if not mdir.exists():
        return {}

    year_pattern = re.compile(rf"^{re.escape(market)}_\d{{4}}\.parquet$")
    first_dates: dict[str, date] = {}

    for pfile in sorted(mdir.glob(f"{market}_*.parquet")):
        if not year_pattern.match(pfile.name):
            continue
        try:
            table = pq.read_table(str(pfile), columns=["Ticker", "Date"])
            df = table.to_pandas()
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            for ticker, d in df.groupby("Ticker")["Date"].min().items():
                if ticker not in first_dates or d < first_dates[ticker]:
                    first_dates[ticker] = d
        except Exception as e:
            logger.warning(f"[OhlcDB] {pfile.name} Ticker/Date 읽기 실패: {e}")

    return first_dates


# ══════════════════════════════════════════════════════════════════════════════
# 커버리지 축소 가드
# ══════════════════════════════════════════════════════════════════════════════
#
# 같은 사고가 두 번 났다.
#   2026-08-12  crypto 2026 재백필 — 197종목 → 172종목
#   2026-08-20  us 2024 백필      — 899종목 → 727종목
#
# 1번 대응으로 backfill_market 에 download_year() 선행 호출을 넣었는데도
# 2번이 났다. save_year() 에 구멍이 두 개 남아 있었기 때문이다.
#
#   구멍 A — 로컬 파일이 없으면 병합 블록을 건너뛰고 그냥 쓴다.
#            download_year() 는 실패해도 False 만 돌려주고 호출부가 이를
#            보지 않으므로, 다운로드가 실패하면 이번에 받은 것만 저장된다.
#   구멍 B — 병합 중 예외가 나면 logger.warning 만 남기고 덮어쓴다.
#
# 둘 다 조용히 통과한다. 그래서 저장 **직전**에 종목 수를 비교하고, 줄어들면
# 예외를 던져 중단한다. 데이터를 잃는 것보다 빌드가 멈추는 편이 항상 낫다.

TICKER_SHRINK_TOLERANCE_PCT = 5.0
"""허용 축소폭. 상장폐지 등 정상 사유로 소폭 줄 수 있어 여유를 둔다.
US 2024 사고는 −19.1% 였고 crypto 2026 사고는 −12.7% 였으므로 5% 로 둘 다 잡힌다."""


class CoverageShrinkError(RuntimeError):
    """저장하면 기존보다 종목 수가 크게 줄어드는 경우. 저장·업로드를 중단한다."""


def _norm_tickers(values) -> set:
    return {str(t).strip().upper() for t in values}


def _ticker_count_on_disk(market: str, year: int,
                          exclude: Optional[set] = None) -> "int | None":
    """
    디스크의 연도 파일에 담긴 고유 종목 수. 파일이 없으면 None.

    load_year() 와 별개 경로로 읽는다 — 병합이 깨진 상황에서도 비교 기준은
    살아 있어야 하기 때문이다. Ticker 컬럼만 읽으므로 비용이 작다.

    exclude: 축소 판정에서 뺄 종목. `replace_tickers` 로 의도적으로 비우는
        종목이 여기 들어온다. 이들을 빼야 "의도적 purge"와 "사고로 인한
        데이터 소실"이 구분된다 — 예: ARB 2022 는 Arbitrum 이 2023년 출시라
        올바른 심볼에 2022년 데이터가 없어 종목 수가 정당하게 준다.
    """
    path = local_path(market, year)
    if not path.exists():
        return None
    try:
        tbl = pq.read_table(str(path), columns=["Ticker"])
        s = pd.Series(tbl.column("Ticker").to_pylist()).astype(str).str.strip().str.upper()
        if exclude:
            s = s[~s.isin(exclude)]
        return int(s.nunique())
    except Exception as e:      # 읽을 수 없으면 비교 불가 — 호출부가 판단한다
        logger.error(f"[OhlcDB] 기존 파일 종목 수 확인 실패: {path.name}: {e}")
        raise


def download_year_state(market: str, year: int, uploader=None) -> str:
    """
    Drive 다운로드 결과를 세 상태로 구분한다.

      "ok"      내려받았다
      "absent"  Drive 에 아직 없다 (최초 백필 — 새로 쓰는 것이 정상)
      "failed"  네트워크·권한 등으로 실패했다 (**덮어쓰면 안 된다**)

    기존 download_year() 는 뒤 둘을 모두 False 로 돌려줘 구분할 수 없었고,
    그것이 us 2024 사고의 구멍 A 다.
    """
    u = _get_uploader(uploader)
    if u is None:
        logger.error("[OhlcDB] 업로더 없음 — Drive 상태를 확인할 수 없다")
        return "failed"

    remote_path = config.DRIVE_PATHS.get(f"ohlc_{market}")
    if not remote_path:
        logger.error(f"[OhlcDB] DRIVE_PATHS에 'ohlc_{market}' 없음")
        return "failed"

    filename = f"{market}_{year}.parquet"
    dest = local_path(market, year)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        u.download(remote_path, filename, str(dest))
        return "ok"
    except FileNotFoundError:
        logger.info(f"[OhlcDB] Drive에 없음(최초 적재로 간주): {filename}")
        return "absent"
    except Exception as e:
        logger.error(f"[OhlcDB] {filename} 다운로드 실패: {e}")
        return "failed"


def save_year(df: pd.DataFrame, market: str, year: int,
              replace_tickers: Optional[list[str]] = None,
              *,
              allow_shrink: bool = False,
              _existing_override: Optional[pd.DataFrame] = None):
    """
    연도별 Parquet 저장.
    기존 파일이 있으면 병합 후 (Ticker, Date) 기준 중복 제거 (최신 우선).
    snappy 압축으로 저장.

    Args:
        replace_tickers: 지정하면 이 종목들의 **기존 행을 전부 버리고** 이번 df로
            대체한다. 행 단위 병합만으로는 지울 수 없는 오염을 걷어내기 위한 것이다.

            ⚠️ 왜 필요한가: 잘못된 토큰 데이터가 이미 저장돼 있는데 재수집에서 그
            날짜에 데이터가 안 나오면(예: Arbitrum은 2023년 출시라 ARB11841-USD에
            2022년 데이터가 없음) 행 단위 병합으로는 옛 오염 행이 그대로 살아남는다.
            실제로 crypto 재백필 후에도 61/228 종목의 접합이 그대로였다.

            지정하지 않으면 기존 동작(행 단위 병합)이 그대로 유지되므로, 이번
            유니버스에 없는 종목의 과거 데이터는 보존된다
            ([BUG-BACKFILL-REPLACE] 참조).

        allow_shrink: 종목 수가 줄어드는 저장을 명시적으로 허용한다. 기본은
            False 이며, 줄어들면 CoverageShrinkError 를 던져 중단한다.
            의도적으로 유니버스를 줄이는 경우에만 True 로 넘긴다.

        _existing_override: 테스트 전용. 병합에 쓸 기존 프레임을 강제로
            지정해 "병합이 깨진 상황"을 재현한다. 비교 기준(디스크의 종목
            수)은 이 값과 무관하게 실제 파일에서 읽는다.

    Raises:
        CoverageShrinkError: 저장 결과가 기존보다 TICKER_SHRINK_TOLERANCE_PCT
            이상 줄어들 때. 저장도 업로드도 하지 않는다.
    """
    if df.empty and not replace_tickers:
        logger.warning(f"[OhlcDB] 빈 DataFrame → 저장 건너뜀: {market}_{year}")
        return

    path = local_path(market, year)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Date 타입 통일
    df = df.copy()
    if "Date" in df.columns and not df.empty:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # 저장 전 비교 기준 — 병합과 별개 경로로 읽는다.
    # replace_tickers 는 의도적 교체이므로 양쪽에서 똑같이 빼고 센다.
    _exempt = _norm_tickers(replace_tickers) if replace_tickers else set()
    prior_n = _ticker_count_on_disk(market, year, exclude=_exempt)

    # 기존 파일 병합
    if path.exists():
        try:
            existing = (load_year(market, year) if _existing_override is None
                        else _existing_override)
            if not existing.empty:
                if replace_tickers:
                    purge = {str(t).strip().upper() for t in replace_tickers}
                    before = len(existing)
                    existing = existing[
                        ~existing["Ticker"].astype(str).str.strip().str.upper().isin(purge)
                    ]
                    if before != len(existing):
                        logger.info(
                            f"[OhlcDB] {market}_{year}: 재수집 대상 종목의 기존 행 "
                            f"{before - len(existing):,}개 제거 후 재적재"
                        )
                # 빈 프레임을 concat에 넣으면 dtype 추론이 흔들린다는 경고가 뜨므로
                # 실제로 붙일 내용이 있을 때만 합친다.
                frames = [f for f in (existing, df) if not f.empty]
                df = pd.concat(frames, ignore_index=True) if frames else df
        except Exception as e:
            # 구멍 B — 예전에는 경고만 남기고 덮어썼다. 병합에 실패했다는 것은
            # 기존 데이터를 보존할 수 없다는 뜻이므로 덮어쓰면 안 된다.
            raise CoverageShrinkError(
                f"{market}_{year} 기존 파일 병합 실패 — 덮어쓰지 않고 중단한다. "
                f"원인: {e}"
            ) from e

    if df.empty:
        logger.warning(f"[OhlcDB] 병합 결과가 비어 있음 → 저장 건너뜀: {market}_{year}")
        return

    # 중복 제거 (최신 우선)
    if "Ticker" in df.columns and "Date" in df.columns:
        df = df.drop_duplicates(subset=["Ticker", "Date"], keep="last")

    # 컬럼 순서 정렬 (존재하는 컬럼만)
    cols = [c for c in _SCHEMA_COLS if c in df.columns]
    df = df[cols].sort_values(["Ticker", "Date"]).reset_index(drop=True)

    # ── 축소 가드 ────────────────────────────────────────────────────────
    if "Ticker" in df.columns:
        _t = df["Ticker"].astype(str).str.strip().str.upper()
        new_n = int(_t[~_t.isin(_exempt)].nunique()) if _exempt else int(_t.nunique())
    else:
        new_n = None
    if prior_n and new_n is not None and not allow_shrink:
        floor = prior_n * (1.0 - TICKER_SHRINK_TOLERANCE_PCT / 100.0)
        if new_n < floor:
            raise CoverageShrinkError(
                f"{market}_{year} 저장 중단 — 종목 수가 {prior_n:,}개에서 "
                f"{new_n:,}개로 {(1 - new_n / prior_n) * 100:.1f}% 줄어든다 "
                f"(허용 {TICKER_SHRINK_TOLERANCE_PCT:.0f}%). "
                f"Drive 다운로드 실패나 유니버스 축소일 가능성이 높다. "
                f"의도한 축소라면 allow_shrink=True 로 호출하라."
            )

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")

    size_kb = path.stat().st_size / 1024
    logger.info(
        f"[OhlcDB] 저장 완료: {path.name} "
        f"({len(df):,}행, {df['Ticker'].nunique() if 'Ticker' in df.columns else '?'}종목, "
        f"{size_kb:.1f}KB)"
    )


def append_rows(new_df: pd.DataFrame, market: str) -> list[int]:
    """
    새 데이터를 연도별로 분할하여 기존 파일에 append.
    Date 컬럼 기준으로 연도 분리.

    Returns: 업데이트된 연도 목록 (정렬)
    """
    if new_df.empty:
        logger.warning(f"[OhlcDB] append_rows: 빈 DataFrame (market={market})")
        return []

    df = new_df.copy()
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # 연도 컬럼 추가
    df["_year"] = df["Date"].apply(lambda d: d.year)
    updated_years: list[int] = []

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"])
        save_year(year_df, market, int(year))
        updated_years.append(int(year))

    logger.info(f"[OhlcDB] append 완료: {market} → {sorted(updated_years)}년")
    return sorted(updated_years)


def get_last_date(market: str) -> Optional[date]:
    """로컬 파일 전체에서 가장 최근 Date 반환. 파일 없으면 None."""
    mdir = local_dir(market)
    if not mdir.exists():
        return None

    parquet_files = sorted(mdir.glob(f"{market}_*.parquet"), reverse=True)
    if not parquet_files:
        return None

    # 최신 연도 파일부터 확인
    for pfile in parquet_files:
        try:
            df = pq.read_table(str(pfile), columns=["Date"]).to_pandas()
            if df.empty:
                continue
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            return df["Date"].max()
        except Exception as e:
            logger.warning(f"[OhlcDB] {pfile.name} Date 조회 실패: {e}")

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 상태 관리 (db_status.json)
# ══════════════════════════════════════════════════════════════════════════════

def load_status() -> dict:
    """data/local/ohlc_db/db_status.json 로드. 없으면 빈 dict 반환."""
    if not _STATUS_PATH.exists():
        return {}
    try:
        with open(_STATUS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"[OhlcDB] status 로드 실패: {e}")
        return {}


def save_status(status: dict):
    """db_status.json 저장."""
    _STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_STATUS_PATH, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2, ensure_ascii=False, default=str)
        logger.debug(f"[OhlcDB] status 저장 완료: {_STATUS_PATH}")
    except Exception as e:
        logger.error(f"[OhlcDB] status 저장 실패: {e}")


def update_status(market: str, last_date: date, ticker_count: int,
                  oldest_date: Optional[date] = None):
    """
    특정 시장의 상태 정보 업데이트 후 저장.

    status 구조:
      {
        "us": {
          "last_updated": "2025-12-31",
          "ticker_count": 60,
          "oldest_date": "2020-01-02",
          "updated_at": "2026-03-22T10:00:00"
        },
        ...
      }
    """
    status = load_status()
    status[market] = {
        "last_updated": str(last_date),
        "ticker_count": ticker_count,
        "oldest_date": str(oldest_date) if oldest_date else None,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    }
    save_status(status)
    logger.info(
        f"[OhlcDB] status 업데이트: {market} "
        f"last={last_date}, tickers={ticker_count}, oldest={oldest_date}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Drive 연동
# ══════════════════════════════════════════════════════════════════════════════

def _get_uploader(uploader=None):
    """DriveUploader 인스턴스 반환 (인자로 받거나 새로 생성).
    루트 폴더를 GDRIVE_OHLC_FOLDER_ID([Database] Market Crawling Data)로 설정.
    """
    if uploader is not None:
        return uploader
    try:
        from data.drive_uploader import DriveUploader
        return DriveUploader(root_folder_id=config.GDRIVE_OHLC_FOLDER_ID or None)
    except Exception as e:
        logger.error(f"[OhlcDB] DriveUploader 초기화 실패: {e}")
        return None


def upload_years(market: str, years: list[int], uploader=None):
    """지정 연도 Parquet 파일을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        logger.warning("[OhlcDB] uploader 없음 → 업로드 건너뜀")
        return

    remote_key = f"ohlc_{market}"
    remote_path = config.DRIVE_PATHS.get(remote_key)
    if not remote_path:
        logger.error(f"[OhlcDB] DRIVE_PATHS에 '{remote_key}' 없음")
        return

    for year in years:
        path = local_path(market, year)
        if not path.exists():
            logger.warning(f"[OhlcDB] 업로드 대상 없음: {path.name}")
            continue
        try:
            u.upload(str(path), remote_path)
        except Exception as e:
            logger.error(f"[OhlcDB] {path.name} 업로드 실패: {e}")


def download_year(market: str, year: int, uploader=None) -> bool:
    """Drive에서 연도별 Parquet 다운로드. 성공 True, 실패 False."""
    u = _get_uploader(uploader)
    if u is None:
        return False

    remote_key = f"ohlc_{market}"
    remote_path = config.DRIVE_PATHS.get(remote_key)
    if not remote_path:
        logger.error(f"[OhlcDB] DRIVE_PATHS에 '{remote_key}' 없음")
        return False

    filename = f"{market}_{year}.parquet"
    dest = local_path(market, year)
    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        u.download(remote_path, filename, str(dest))
        return True
    except FileNotFoundError:
        logger.debug(f"[OhlcDB] Drive에 없음: {remote_path}/{filename}")
        return False
    except Exception as e:
        logger.error(f"[OhlcDB] {filename} 다운로드 실패: {e}")
        return False


def download_status(uploader=None) -> bool:
    """Drive에서 db_status.json 다운로드. 성공 True, 실패 False."""
    u = _get_uploader(uploader)
    if u is None:
        return False

    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if not remote_path:
        logger.error("[OhlcDB] DRIVE_PATHS에 'ohlc_meta' 없음")
        return False

    _STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        u.download(remote_path, "db_status.json", str(_STATUS_PATH))
        return True
    except FileNotFoundError:
        logger.debug("[OhlcDB] Drive에 db_status.json 없음 (최초 실행)")
        return False
    except Exception as e:
        logger.error(f"[OhlcDB] db_status.json 다운로드 실패: {e}")
        return False


def upload_status(uploader=None):
    """로컬 db_status.json을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if not remote_path:
        logger.error("[OhlcDB] DRIVE_PATHS에 'ohlc_meta' 없음")
        return

    if not _STATUS_PATH.exists():
        logger.warning("[OhlcDB] db_status.json 없음 → 업로드 건너뜀")
        return

    try:
        u.upload(str(_STATUS_PATH), remote_path)
    except Exception as e:
        logger.error(f"[OhlcDB] db_status.json 업로드 실패: {e}")


def load_pending() -> dict:
    """data/local/ohlc_db/backfill_pending.json 로드. 없으면 빈 dict 반환."""
    if not _PENDING_PATH.exists():
        return {}
    try:
        with open(_PENDING_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"[OhlcDB] pending 로드 실패: {e}")
        return {}


def save_pending(pending: dict):
    """backfill_pending.json 저장."""
    _PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_PENDING_PATH, "w", encoding="utf-8") as f:
            json.dump(pending, f, indent=2, ensure_ascii=False)
        logger.debug(f"[OhlcDB] pending 저장 완료: {_PENDING_PATH}")
    except Exception as e:
        logger.error(f"[OhlcDB] pending 저장 실패: {e}")


def download_pending(uploader=None) -> bool:
    """Drive에서 backfill_pending.json 다운로드. 성공 True, 실패 False."""
    u = _get_uploader(uploader)
    if u is None:
        return False

    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if not remote_path:
        logger.error("[OhlcDB] DRIVE_PATHS에 'ohlc_meta' 없음")
        return False

    _PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        u.download(remote_path, "backfill_pending.json", str(_PENDING_PATH))
        return True
    except FileNotFoundError:
        logger.debug("[OhlcDB] Drive에 backfill_pending.json 없음 (최초 실행)")
        return False
    except Exception as e:
        logger.error(f"[OhlcDB] backfill_pending.json 다운로드 실패: {e}")
        return False


def upload_pending(uploader=None):
    """로컬 backfill_pending.json을 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if not remote_path:
        logger.error("[OhlcDB] DRIVE_PATHS에 'ohlc_meta' 없음")
        return

    if not _PENDING_PATH.exists():
        logger.warning("[OhlcDB] backfill_pending.json 없음 → 업로드 건너뜀")
        return

    try:
        u.upload(str(_PENDING_PATH), remote_path)
    except Exception as e:
        logger.error(f"[OhlcDB] backfill_pending.json 업로드 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# 종목 메타데이터 (sector_meta) 저장 / Drive 연동
# ══════════════════════════════════════════════════════════════════════════════

def sector_meta_path(market: str) -> Path:
    """sector_meta parquet 로컬 경로."""
    return local_dir(market) / f"{market}_sector_meta.parquet"


def save_sector_meta(df: pd.DataFrame, market: str):
    """sector_meta parquet 저장 (덮어쓰기)."""
    if df.empty:
        logger.warning(f"[OhlcDB] sector_meta 빈 DataFrame → 저장 건너뜀: {market}")
        return
    path = sector_meta_path(market)
    path.parent.mkdir(parents=True, exist_ok=True)
    # ── 축소 가드 ────────────────────────────────────────────────────────
    if "Ticker" in df.columns:
        _t = df["Ticker"].astype(str).str.strip().str.upper()
        new_n = int(_t[~_t.isin(_exempt)].nunique()) if _exempt else int(_t.nunique())
    else:
        new_n = None
    if prior_n and new_n is not None and not allow_shrink:
        floor = prior_n * (1.0 - TICKER_SHRINK_TOLERANCE_PCT / 100.0)
        if new_n < floor:
            raise CoverageShrinkError(
                f"{market}_{year} 저장 중단 — 종목 수가 {prior_n:,}개에서 "
                f"{new_n:,}개로 {(1 - new_n / prior_n) * 100:.1f}% 줄어든다 "
                f"(허용 {TICKER_SHRINK_TOLERANCE_PCT:.0f}%). "
                f"Drive 다운로드 실패나 유니버스 축소일 가능성이 높다. "
                f"의도한 축소라면 allow_shrink=True 로 호출하라."
            )

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")
    size_kb = path.stat().st_size / 1024
    logger.info(f"[OhlcDB] sector_meta 저장: {path.name} ({len(df)}종목, {size_kb:.1f}KB)")


def load_sector_meta(market: str) -> pd.DataFrame:
    """sector_meta parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = sector_meta_path(market)
    if not path.exists():
        return pd.DataFrame(columns=["Ticker", "Market", "Sector", "Industry", "updated_at"])
    try:
        return pq.read_table(str(path)).to_pandas()
    except Exception as e:
        logger.error(f"[OhlcDB] sector_meta 로드 실패: {e}")
        return pd.DataFrame(columns=["Ticker", "Market", "Sector", "Industry", "updated_at"])


def upload_sector_meta(market: str, uploader=None):
    """sector_meta parquet를 Drive에 업로드."""
    u = _get_uploader(uploader)
    if u is None:
        return
    remote_path = config.DRIVE_PATHS.get(f"ohlc_{market}")
    if not remote_path:
        logger.error(f"[OhlcDB] DRIVE_PATHS에 'ohlc_{market}' 없음")
        return
    path = sector_meta_path(market)
    if not path.exists():
        logger.warning(f"[OhlcDB] sector_meta 없음: {path.name}")
        return
    try:
        u.upload(str(path), remote_path)
        logger.info(f"[OhlcDB] sector_meta 업로드 완료: {path.name} → {remote_path}/")
    except Exception as e:
        logger.error(f"[OhlcDB] sector_meta 업로드 실패: {e}")


def download_sector_meta(market: str, uploader=None) -> bool:
    """Drive에서 sector_meta parquet 다운로드. 성공 True, 실패 False."""
    u = _get_uploader(uploader)
    if u is None:
        return False
    remote_path = config.DRIVE_PATHS.get(f"ohlc_{market}")
    if not remote_path:
        logger.error(f"[OhlcDB] DRIVE_PATHS에 'ohlc_{market}' 없음")
        return False
    filename = f"{market}_sector_meta.parquet"
    dest = sector_meta_path(market)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        u.download(remote_path, filename, str(dest))
        logger.info(f"[OhlcDB] sector_meta 다운로드 완료: {filename}")
        return True
    except FileNotFoundError:
        logger.debug(f"[OhlcDB] Drive에 없음: {remote_path}/{filename}")
        return False
    except Exception as e:
        logger.error(f"[OhlcDB] sector_meta 다운로드 실패: {e}")
        return False


def download_all_years(market: str, uploader=None):
    """
    Drive 해당 시장 폴더의 모든 연도 파일을 로컬에 다운로드.
    drive_uploader.download_all() 활용.
    """
    u = _get_uploader(uploader)
    if u is None:
        return

    remote_key = f"ohlc_{market}"
    remote_path = config.DRIVE_PATHS.get(remote_key)
    if not remote_path:
        logger.error(f"[OhlcDB] DRIVE_PATHS에 '{remote_key}' 없음")
        return

    local_d = local_dir(market)
    local_d.mkdir(parents=True, exist_ok=True)

    try:
        u.download_all(remote_path, str(local_d), extensions=(".parquet",))
        logger.info(f"[OhlcDB] {market} 전체 연도 다운로드 완료")
    except Exception as e:
        logger.error(f"[OhlcDB] {market} 전체 다운로드 실패: {e}")
