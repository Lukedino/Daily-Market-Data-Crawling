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
import math
import os
import re
import tempfile
from datetime import date, datetime, timedelta, timezone
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

def _read_year_path(path: Path, year: int | None = None) -> pd.DataFrame:
    df = pq.read_table(str(path)).to_pandas()
    if df.columns.has_duplicates or not {"Ticker", "Date"}.issubset(df.columns):
        raise ValueError("OHLC baseline key schema invalid")
    if df.empty:
        return df if len(df.columns) else pd.DataFrame(columns=_SCHEMA_COLS)
    if df.columns.has_duplicates or not {"Ticker", "Date"}.issubset(df.columns):
        raise ValueError("OHLC key columns missing/duplicated")
    df["Date"] = pd.to_datetime(df["Date"], errors="raise").dt.date
    if df["Date"].isna().any() or df["Ticker"].isna().any() or df["Ticker"].astype(str).str.strip().eq("").any():
        raise ValueError("OHLC keys invalid")
    if df.duplicated(["Ticker", "Date"]).any():
        raise ValueError("OHLC duplicate keys")
    if year is not None and any(day.year != year for day in df["Date"]):
        raise ValueError("OHLC year partition mismatch")
    return df


def _temporary_path(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    return Path(temporary)


def _atomic_json(path: Path, value: dict):
    temporary = _temporary_path(path)
    try:
        temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_year(market: str, year: int, *, strict: bool = False) -> pd.DataFrame:
    """연도별 Parquet 로드. 파일 없으면 빈 DataFrame 반환."""
    path = local_path(market, year)
    if not path.exists():
        return pd.DataFrame(columns=_SCHEMA_COLS)
    try:
        return _read_year_path(path, year)
    except Exception as e:
        logger.error(f"[OhlcDB] {path.name} 로드 실패: {type(e).__name__}")
        if strict:
            raise DriveSyncError(f"{path.name} 기존 파일 검증 실패") from None
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

    # 메시지는 사람이 읽는 한글 f-string 이라 공개 artifact 에서 지워진다.
    # cli_entry 가 읽는 .code 로 원인을 남긴다(없으면 원인 없는 operation_failed).
    code = "coverage_shrink"


class DriveSyncError(RuntimeError):
    """Drive 와의 동기화가 깨진 상태로는 저장·커서 갱신을 진행하지 않는다.

    D-01  기존 연도 파일 다운로드 **실패**("없음" 이 아니다) → 그대로 저장하면 그 연도가 증분 며칠 치로 교체된다.
    D-02  업로드 실패 → 커서(db_status.json)를 전진시키면 그날은 다시 수집되지 않는다.
    예외로 올려 프로세스를 비정상 종료시키면 워크플로의 실패 알림이 울린다.
    """

    code = "drive_sync_failed"


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
    temporary = _temporary_path(dest)
    try:
        existing = _read_year_path(dest, year) if dest.exists() else pd.DataFrame()
        u.download(remote_path, filename, str(temporary))
        downloaded = _read_year_path(temporary, year)
        if not existing.empty:
            if downloaded.empty:
                merged = existing
            else:
                # Retain local-only, not-yet-uploaded history; current remote
                # non-null values take precedence on shared keys.
                merged = downloaded.set_index(["Ticker", "Date"]).combine_first(
                    existing.set_index(["Ticker", "Date"])).reset_index()
            pq.write_table(pa.Table.from_pandas(merged, preserve_index=False), str(temporary), compression="snappy")
            pd.testing.assert_frame_equal(_read_year_path(temporary, year), merged,
                                          check_dtype=False, check_exact=True)
        os.replace(temporary, dest)
        return "ok"
    except FileNotFoundError:
        logger.info(f"[OhlcDB] Drive에 없음(최초 적재로 간주): {filename}")
        return "absent"
    except Exception as e:
        logger.error(f"[OhlcDB] {filename} 다운로드 실패: {type(e).__name__}")
        return "failed"
    finally:
        temporary.unlink(missing_ok=True)


def ensure_year_baselines(market: str, years, *, download: bool = True) -> dict[int, str]:
    """Validate every affected baseline before a caller starts saving any year."""
    states = {}
    for year in sorted(set(years)):
        if local_path(market, year).exists():
            load_year(market, year, strict=True)
        states[year] = (download_year_state(market, year) if download else
                        "ok" if local_path(market, year).exists() else "absent")
        if states[year] not in ("ok", "absent"):
            raise DriveSyncError(f"{market}_{year} 기준 파일 확인 실패 — 저장 중단")
    return states


COVERAGE_DROP_PCT = 10.0
"""일자별 유니버스 급감 임계. 축소 가드(TICKER_SHRINK_TOLERANCE_PCT)가
파일 **전체**의 종목 수를 보는 반면, 이것은 파일 **안**의 날짜별 종목 수를
본다. us_2024 사고는 파일 전체로는 727종목이 멀쩡히 들어 있었지만
2024-01-02 에 899 -> 704 로 꺼지는 형태였다. 두 검사는 서로를 대체하지
않는다."""


class CoverageGapError(RuntimeError):
    """연도 파일 안에서 유니버스가 하루 만에 급감하는 경우."""

    code = "coverage_gap"


class PriceBasisError(RuntimeError):
    """가격 오류를 단정하지 않고, 조정 기준 미확인 후보의 게시를 보류한다."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def validate_price_basis(df: pd.DataFrame, market: str, *, as_of: date | None = None,
                         replace_tickers=()):
    """확정 과거 overlap과 원천 actions를 확인한다. 전체 이력의 기준 증명은 아니다."""
    request = df.attrs.get("ohlc_request")
    if request is not None and request.get("actions_complete") is not True:
        # actions 자체를 못 받으면 기준 변경을 판정할 근거가 없다 — 이건 보류한다.
        # (실측: US 503/503·크립토 157/157 이 True 라 충족 가능한 조건이다.)
        raise PriceBasisError("price_basis_unverified")
    # 배당·분할이 관측된 종목은 auto_adjust 가 과거 봉을 소급 재조정하므로 overlap 이
    # 어긋나는 것이 **정상**이다. 예전에는 그런 종목이 하나라도 있으면 시장 전체를
    # 보류했는데, 80거래일 표본에서 2거래일 이상 창에 배당이 없던 적이 한 번도
    # 없어 US 는 사실상 영구 보류였다. 그 종목만 overlap 비교에서 빼고, 나머지
    # 99% 의 '예상 밖 변경' 감지는 그대로 남긴다.
    action_tickers = set(request.get("action_tickers") or ()) if request else set()
    if df.empty:
        return
    incoming = df.copy()
    incoming["Date"] = pd.to_datetime(incoming["Date"]).dt.date
    as_of = as_of or datetime.now(timezone.utc).date()
    for year in sorted({day.year for day in incoming["Date"]}):
        existing = load_year(market, year, strict=True)
        if existing.empty:
            continue
        old = existing.set_index(["Ticker", "Date"])
        new = incoming[incoming["Date"].map(lambda day: day.year == year)].set_index(["Ticker", "Date"])
        for key in old.index.intersection(new.index):
            if key[0] in replace_tickers:
                continue  # 기존의 명시적인 Crypto 심볼 정정/purge 계약은 유지한다.
            if key[0] in action_tickers:
                continue  # 이 창에 배당·분할이 있었다 — 재조정은 예상된 변화다.
            # Crypto 당일 및 직전 UTC 일봉은 이전 실행에서 미완성이었을 수 있다.
            # 기존 7일 재조회에 의한 봉 완성을 가격 기준 변경으로 오인하지 않는다.
            if market == "crypto" and key[1] >= as_of - timedelta(days=1):
                continue
            for column in ("Open", "High", "Low", "Close"):
                if column not in old or column not in new:
                    raise PriceBasisError("price_basis_unverified")
                try:
                    before, after = float(old.at[key, column]), float(new.at[key, column])
                except (TypeError, ValueError):
                    raise PriceBasisError("price_basis_unverified") from None
                if not math.isfinite(before) or not math.isfinite(after):
                    raise PriceBasisError("price_basis_unverified")
                if not math.isclose(before, after, rel_tol=1e-7, abs_tol=1e-8):
                    raise PriceBasisError("price_basis_mismatch")


def _preserve_missing_marketcap(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    """동일 키의 신규 시총 결측만 보존한다. 가격·거래량·명시적인 0은 그대로 갱신한다."""
    if existing.empty or incoming.empty or "MarketCap" not in existing:
        return incoming
    result = incoming.copy()
    if "MarketCap" not in result:
        result["MarketCap"] = float("nan")
    old = pd.to_numeric(existing.set_index(["Ticker", "Date"])["MarketCap"], errors="coerce")
    old = old.where(old.map(lambda value: pd.notna(value) and math.isfinite(value) and value >= 0))
    keys = pd.MultiIndex.from_frame(result[["Ticker", "Date"]])
    values = old.reindex(keys).to_numpy()
    missing = result["MarketCap"].isna()
    result.loc[missing, "MarketCap"] = values[missing.to_numpy()]
    return result


def check_coverage_continuity(df: pd.DataFrame,
                              drop_pct: float = COVERAGE_DROP_PCT,
                              min_universe: int = 100) -> dict:
    """
    날짜별 고유 종목 수가 전 거래일 대비 drop_pct% 이상 줄어드는 지점을 찾는다.

    소비 측(ML_Market Data Analysis)의 `src/integrity.check_coverage_continuity`
    와 같은 검사다. 소비자에만 두면 이미 오염된 Drive 를 받은 뒤에야 잡히므로
    생산자에도 둔다.

    반환: {"ok", "n_bad", "worst_date", "worst_pct", "rows"}
    """
    if df.empty or "Date" not in df.columns or "Ticker" not in df.columns:
        return {"ok": True, "n_bad": 0, "worst_date": None,
                "worst_pct": 0.0, "rows": pd.DataFrame()}

    n = df.groupby("Date")["Ticker"].nunique().sort_index()
    prev = n.shift(1)
    pct = (n - prev) / prev * 100
    bad = pd.DataFrame({"n": n, "n_prev": prev, "pct": pct})
    bad = bad[(bad.n_prev >= min_universe) & (bad.pct <= -drop_pct)]
    return {
        "ok": len(bad) == 0,
        "n_bad": len(bad),
        "worst_date": bad.pct.idxmin() if len(bad) else None,
        "worst_pct": float(bad.pct.min()) if len(bad) else 0.0,
        "rows": bad,
    }


# 끝의 거래소 접미사 — 2~3자(.TO .HK .AS .PA .DE .SW .TWO …) + 1자 거래소 코드
# (.L 런던 / .V TSX-V / .F 프랑크푸르트 / .T 도쿄). 그 외 1자는 클래스 구분자(BRK.B)다.
# ohlc_collector._normalize_ticker 와 공유한다(정의는 여기 한 곳).
EXCHANGE_SUFFIX_RE = re.compile(r"\.(?:[A-Z]{2,3}|[LVFT])$")


def drop_foreign_calendar_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    거래소 접미사 종목(U-UN.TO 등 해외 상장)의 행 중, 같은 날짜에 접미사 없는(미국
    상장) 종목이 하나도 없는 날짜의 행을 뺀다. 반환: (남긴 df, 뺀 df).

    US 파일은 미국 달력에만 맞춘다. TSX 는 MLK·현충일·준틴스·독립기념일·추수감사절에
    열려 있어, 2026-09-05 U-UN.TO 백필이 2020~2026 파일 7개에 "U-UN.TO 혼자 있는 날"
    34개를 만들었고 곧이어 update_market() 의 전체 저장이 그 날짜에서
    "1,071 → 1 종목" 으로 연속성 게이트에 막혀 US daily 가 매 실행 죽었다.
    달력 라이브러리 없이 같은 파일의 미국 종목 존재 여부로 판정하므로
    2025-01-09(카터 추모 임시 휴장) 같은 비정기 휴장도 함께 잡힌다.
    """
    if df.empty or "Ticker" not in df.columns or "Date" not in df.columns:
        return df, df.iloc[0:0]
    foreign = df["Ticker"].astype(str).str.strip().str.upper().str.contains(EXCHANGE_SUFFIX_RE)
    domestic_dates = set(df.loc[~foreign, "Date"])
    orphan = foreign & ~df["Date"].isin(domestic_dates)
    return df[~orphan], df[orphan]


def save_year(df: pd.DataFrame, market: str, year: int,
              replace_tickers: Optional[list[str]] = None,
              *,
              allow_shrink: bool = False,
              allow_gap: bool = False,
              subset_merge: bool = False,
              _existing_override: Optional[pd.DataFrame] = None):
    """
    연도별 Parquet 저장.
    기존 파일이 있으면 병합 후 (Ticker, Date) 기준 중복 제거 (최신 우선).
    snappy 압축으로 저장.

    Args:
        subset_merge: 이번 df 가 유니버스의 부분집합(신규 종목 백필 등)임을
            뜻한다. 부분집합은 기존 파일에 없던 날짜(마지막 저장일 이후·기존
            구멍)에 유니버스 커버리지를 만들 수 없으므로, 연속성 게이트는
            **기존 파일에 있던 날짜만** 본다. 기존 날짜 사이의 급감은 그대로
            차단한다(allow_gap 과 다르다). 2026-09-03 daily 3연속 실패 참조.
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
    existing_dates: set = set()   # subset_merge 게이트 범위 — 기존 파일이 커버하던 날짜
    if path.exists():
        try:
            existing = (load_year(market, year, strict=True) if _existing_override is None
                        else _existing_override)
            if not existing.empty:
                if subset_merge and "Date" in existing.columns:
                    existing_dates = set(pd.to_datetime(existing["Date"]).dt.date)
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
                df = _preserve_missing_marketcap(existing, df)
                frames = [f for f in (existing, df) if not f.empty]
                df = pd.concat(frames, ignore_index=True) if frames else df
        except Exception as e:
            # 구멍 B — 예전에는 경고만 남기고 덮어썼다. 병합에 실패했다는 것은
            # 기존 데이터를 보존할 수 없다는 뜻이므로 덮어쓰면 안 된다.
            raise CoverageShrinkError(
                f"{market}_{year} 기존 파일 병합 실패 — 덮어쓰지 않고 중단한다. "
                f"원인: {type(e).__name__}"
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

    # ── 해외 상장 종목의 미국 휴장일 행 제거 (US 전용) ──────────────────────
    # 병합 결과 전체에 적용하므로 백필(부분집합)·증분 어느 경로로 들어오든,
    # 그리고 구 코드가 이미 남긴 오염(2026-09-05)도 다음 저장에서 함께 치유된다.
    if market == "us":
        df, dropped = drop_foreign_calendar_rows(df)
        if not dropped.empty:
            per = dropped.groupby("Ticker")["Date"].apply(
                lambda d: ", ".join(str(x) for x in sorted(d)[:8])
                          + (" …" if len(d) > 8 else "")
            )
            detail = "; ".join(f"{t}×{(dropped['Ticker'] == t).sum()} ({dates})"
                               for t, dates in per.items())
            logger.warning(
                f"[OhlcDB] {market}_{year}: 미국 휴장일에 해외 상장 종목만 있는 행 "
                f"{len(dropped):,}개 제거 — US 파일은 미국 달력에만 맞춘다: {detail}"
            )
        if df.empty:
            logger.warning(f"[OhlcDB] 휴장일 행 제거 후 비어 있음 → 저장 건너뜀: {market}_{year}")
            return

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

    # ── 커버리지 연속성 게이트 ────────────────────────────────────────────
    # 축소 가드는 파일 전체 종목 수를 본다. 이건 파일 안의 날짜별 종목 수를
    # 본다. us_2024 사고(2024-01-02 899 -> 704)는 전자를 통과하고 후자에만
    # 걸린다 — 재수집 결과가 727종목이면 "원래 727이었다"와 구분되지 않기
    # 때문이다. 생산자에서 막아야 Drive 원본이 오염되지 않는다.
    if not allow_gap:
        cov_df = df
        if subset_merge and existing_dates and "Date" in df.columns:
            # 부분집합 병합은 기존 파일에 없던 날짜(후행일·구멍)에 유니버스
            # 커버리지를 만들 수 없다 — 그 날짜는 검사에서 빼고, 기존 날짜 사이의
            # 급감만 본다. 구멍 자체는 update_market() 의 재조회가 채운다.
            cov_df = df[pd.to_datetime(df["Date"]).dt.date.isin(existing_dates)]
        cov = check_coverage_continuity(cov_df)
        if not cov["ok"]:
            raise CoverageGapError(
                f"{market}_{year} 저장 중단 — 파일 안에서 유니버스가 하루 만에 "
                f"{cov['worst_pct']:.1f}% 급감한다 ({cov['worst_date']}). "
                f"총 {cov['n_bad']}일. 수집 누락 가능성이 높다. "
                f"의도한 것이라면 allow_gap=True 로 호출하라."
            )

    # 요청 단위 관측 메타는 혼합 연도 파일 전체의 영구 출처 증명이 아니다.
    df.attrs = {}
    table = pa.Table.from_pandas(df, preserve_index=False)
    temporary = _temporary_path(path)
    try:
        pq.write_table(table, str(temporary), compression="snappy")
        pd.testing.assert_frame_equal(_read_year_path(temporary, year), df.reset_index(drop=True),
                                      check_dtype=False, check_exact=True)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)

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
            result = json.load(f)
        if not isinstance(result, dict):
            raise ValueError("status must be an object")
        return result
    except Exception as e:
        raise DriveSyncError(f"status 로드 실패: {type(e).__name__}") from None


def save_status(status: dict):
    """db_status.json 저장."""
    _atomic_json(_STATUS_PATH, status)


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


def upload_years(market: str, years: list[int], uploader=None) -> list[str]:
    """지정 연도 Parquet 파일을 Drive에 업로드하고 **실패한 파일 이름 목록**을 돌려준다.

    예전에는 예외를 로그로 삼키고 아무것도 돌려주지 않아, 호출자가 업로드 실패를 모른 채
    커서를 전진시켰다(D-02). 호출자는 반환값이 비어 있지 않으면 상태를 갱신하면 안 된다.
    """
    u = _get_uploader(uploader)
    if u is None:
        logger.warning("[OhlcDB] uploader 없음 → 업로드 건너뜀")
        return [f"{market}_{year}.parquet" for year in years]

    remote_key = f"ohlc_{market}"
    remote_path = config.DRIVE_PATHS.get(remote_key)
    if not remote_path:
        logger.error(f"[OhlcDB] DRIVE_PATHS에 '{remote_key}' 없음")
        return [f"{market}_{year}.parquet" for year in years]

    failed: list[str] = []
    for year in years:
        path = local_path(market, year)
        if not path.exists():
            logger.warning(f"[OhlcDB] 업로드 대상 없음: {path.name}")
            failed.append(path.name)
            continue
        try:
            load_year(market, year, strict=True)
            receipt = u.upload(str(path), remote_path)
            if not (receipt is True or isinstance(receipt, str) and receipt.strip()):
                raise DriveSyncError("Drive upload rejected")
        except Exception as e:
            # 공개 저장소 로그다. 예외 문자열에는 Drive 파일·폴더 ID 가 실릴 수 있어 종류만 남긴다.
            logger.error(f"[OhlcDB] {path.name} 업로드 실패: {type(e).__name__}")
            failed.append(path.name)
    return failed


def download_year(market: str, year: int, uploader=None) -> bool:
    """Drive에서 연도별 Parquet 다운로드. 성공 True, 실패 False."""
    return download_year_state(market, year, uploader) == "ok"


def _download_metadata(path: Path, uploader=None) -> bool:
    u = _get_uploader(uploader)
    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if u is None or not remote_path:
        raise DriveSyncError("metadata 다운로드 설정 없음")
    temporary = _temporary_path(path)
    try:
        u.download(remote_path, path.name, str(temporary))
        value = json.loads(temporary.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("metadata must be an object")
        if path == _PENDING_PATH and any(
                not isinstance(tickers, list) or any(not isinstance(ticker, str) for ticker in tickers)
                for tickers in value.values()):
            raise ValueError("pending must map markets to ticker lists")
        if path == _STATUS_PATH and any(not isinstance(status, dict) for status in value.values()):
            raise ValueError("status must map markets to status objects")
        os.replace(temporary, path)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise DriveSyncError(f"{path.name} 다운로드 실패: {type(e).__name__}") from None
    finally:
        temporary.unlink(missing_ok=True)


def _upload_metadata(path: Path, uploader=None) -> bool:
    u = _get_uploader(uploader)
    remote_path = config.DRIVE_PATHS.get("ohlc_meta")
    if u is None or not remote_path or not path.exists():
        raise DriveSyncError(f"{path.name} 업로드 준비 실패")
    try:
        if not isinstance(json.loads(path.read_text(encoding="utf-8")), dict):
            raise ValueError("metadata must be an object")
        receipt = u.upload(str(path), remote_path)
        if not (receipt is True or isinstance(receipt, str) and receipt.strip()):
            raise ValueError("metadata upload rejected")
        return True
    except Exception as e:
        raise DriveSyncError(f"{path.name} 업로드 실패: {type(e).__name__}") from None


def _restore_metadata(path: Path, before: bytes | None):
    if before is None:
        path.unlink(missing_ok=True)
    else:
        temporary = _temporary_path(path)
        try:
            temporary.write_bytes(before)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def publish_status(market: str, last_date: date, ticker_count: int,
                   oldest_date: Optional[date] = None, *, upload: bool = True):
    """Do not leave a locally advanced cursor after failed metadata publication."""
    before = _STATUS_PATH.read_bytes() if _STATUS_PATH.exists() else None
    try:
        update_status(market, last_date, ticker_count, oldest_date)
        if upload and upload_status() is False:
            raise DriveSyncError("status 업로드 실패")
    except Exception:
        _restore_metadata(_STATUS_PATH, before)
        raise


def publish_pending(pending: dict, *, upload: bool = True):
    before = _PENDING_PATH.read_bytes() if _PENDING_PATH.exists() else None
    try:
        save_pending(pending)
        if upload and upload_pending() is False:
            raise DriveSyncError("pending 업로드 실패")
    except Exception:
        _restore_metadata(_PENDING_PATH, before)
        raise


def download_status(uploader=None) -> bool:
    """Download status; only confirmed remote absence returns False."""
    return _download_metadata(_STATUS_PATH, uploader)


def upload_status(uploader=None):
    """로컬 db_status.json을 Drive에 업로드."""
    return _upload_metadata(_STATUS_PATH, uploader)


def load_pending() -> dict:
    """data/local/ohlc_db/backfill_pending.json 로드. 없으면 빈 dict 반환."""
    if not _PENDING_PATH.exists():
        return {}
    try:
        with open(_PENDING_PATH, "r", encoding="utf-8") as f:
            result = json.load(f)
        if not isinstance(result, dict) or any(
                not isinstance(tickers, list) or any(not isinstance(ticker, str) for ticker in tickers)
                for tickers in result.values()):
            raise ValueError("pending must map markets to ticker lists")
        return result
    except Exception as e:
        raise DriveSyncError(f"pending 로드 실패: {type(e).__name__}") from None


def save_pending(pending: dict):
    """backfill_pending.json 저장."""
    _atomic_json(_PENDING_PATH, pending)


def download_pending(uploader=None) -> bool:
    """Download pending; only confirmed remote absence returns False."""
    return _download_metadata(_PENDING_PATH, uploader)


def upload_pending(uploader=None):
    """로컬 backfill_pending.json을 Drive에 업로드."""
    return _upload_metadata(_PENDING_PATH, uploader)


# ══════════════════════════════════════════════════════════════════════════════
# 종목 메타데이터 (sector_meta) 저장 / Drive 연동
# ══════════════════════════════════════════════════════════════════════════════

def sector_meta_path(market: str) -> Path:
    """sector_meta parquet 로컬 경로."""
    return local_dir(market) / f"{market}_sector_meta.parquet"


_SECTOR_COLUMNS = ["Ticker", "Market", "Sector", "Industry", "updated_at"]


def _validate_sector_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.has_duplicates or not set(_SECTOR_COLUMNS).issubset(df.columns):
        raise DriveSyncError("sector_schema_invalid")
    result = df[_SECTOR_COLUMNS].copy()
    result.attrs = {}
    if result.empty:
        return result
    if (result["Ticker"].map(lambda value: not isinstance(value, str) or not value.strip()).any()
            or result["Ticker"].str.strip().str.upper().duplicated().any()
            or result["Market"].map(lambda value: not isinstance(value, str) or not value.strip()).any()):
        raise DriveSyncError("sector_keys_invalid")
    for column in ["Sector", "Industry"]:
        if result[column].map(lambda value: not isinstance(value, str) and not pd.isna(value)).any():
            raise DriveSyncError("sector_field_invalid")
        result[column] = result[column].fillna("")
    try:
        stamps = pd.to_datetime(result["updated_at"], utc=True, errors="raise")
        if stamps.isna().any():
            raise ValueError()
    except (ValueError, TypeError):
        raise DriveSyncError("sector_timestamp_invalid") from None
    return result.reset_index(drop=True)


def _write_sector_frame(path: Path, frame: pd.DataFrame):
    temporary = _temporary_path(path)
    try:
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), temporary, compression="snappy")
        with temporary.open("r+b") as stream:
            os.fsync(stream.fileno())
        observed = _validate_sector_frame(pq.read_table(temporary).to_pandas())
        pd.testing.assert_frame_equal(frame.reset_index(drop=True), observed, check_dtype=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def save_sector_meta(df: pd.DataFrame, market: str, allow_shrink: bool = False):
    """Preserve failed observations and old bytes until the candidate is verified."""
    if df.empty:
        raise DriveSyncError("sector_collection_empty")
    prior = load_sector_meta(market)
    failed_fields = df.attrs.get("sector_failed_fields", {})
    candidate = df[_SECTOR_COLUMNS].copy()
    old = prior.set_index("Ticker")
    unresolved = []   # 기준본에 없어 되돌릴 값이 없는 신규 종목
    for index, row in candidate.iterrows():
        failed = failed_fields.get(row["Ticker"], [])
        if failed:
            if row["Ticker"] not in old.index:
                # 되돌릴 이전 값이 없는 신규 종목이다. 예전에는 여기서 중단해
                # 나쁜 심볼 하나가 그 주 산출물 전체(US 1,061행)를 버리게 했다.
                # 그 행의 해당 필드만 빈 값으로 두고 나머지는 게시한다.
                for field in failed:
                    if field not in {"Sector", "Industry"}:
                        raise DriveSyncError("sector_observation_invalid")
                    candidate.at[index, field] = ""
                unresolved.append(row["Ticker"])
                continue
            for field in failed:
                if field not in {"Sector", "Industry"}:
                    raise DriveSyncError("sector_observation_invalid")
                candidate.at[index, field] = old.at[row["Ticker"], field]
            # One row timestamp must not certify fields that were not observed.
            candidate.at[index, "updated_at"] = old.at[row["Ticker"], "updated_at"]
    candidate = _validate_sector_frame(candidate)
    if len(prior) and not allow_shrink:
        floor = len(prior) * (1.0 - TICKER_SHRINK_TOLERANCE_PCT / 100.0)
        if len(candidate) < floor:
            raise CoverageShrinkError("sector_ticker_coverage_shrink")
    _write_sector_frame(sector_meta_path(market), candidate)
    logger.info("[OhlcDB] sector_saved rows=%d preserved=%d unresolved=%d",
                len(candidate), len(failed_fields), len(unresolved))
    return True


def load_sector_meta(market: str) -> pd.DataFrame:
    path = sector_meta_path(market)
    if not path.exists():
        return pd.DataFrame(columns=_SECTOR_COLUMNS)
    try:
        return _validate_sector_frame(pq.read_table(path).to_pandas())
    except Exception:
        raise DriveSyncError("sector_baseline_invalid") from None


def upload_sector_meta(market: str, uploader=None):
    u = _get_uploader(uploader)
    path = sector_meta_path(market)
    remote = config.DRIVE_PATHS.get(f"ohlc_{market}")
    if u is None or not remote or not path.is_file():
        raise DriveSyncError("sector_upload_unavailable")
    load_sector_meta(market)
    try:
        if not u.upload(str(path), remote):
            raise DriveSyncError("sector_upload_unconfirmed")
    except Exception:
        raise DriveSyncError("sector_publication_failed") from None
    return True


def download_sector_meta(market: str, uploader=None) -> bool:
    """Only confirmed remote absence is False; retain valid unpublished local rows."""
    prior = load_sector_meta(market)
    u = _get_uploader(uploader)
    remote = config.DRIVE_PATHS.get(f"ohlc_{market}")
    if u is None or not remote:
        raise DriveSyncError("sector_download_unavailable")
    path = sector_meta_path(market)
    temporary = _temporary_path(path)
    try:
        try:
            result = u.download(remote, path.name, str(temporary))
        except FileNotFoundError:
            return False
        if result is False:
            raise DriveSyncError("sector_download_failed")
        incoming = _validate_sector_frame(pq.read_table(temporary).to_pandas())
        merged = pd.concat([prior, incoming], ignore_index=True)
        if not merged.empty:
            merged = merged.assign(_stamp=pd.to_datetime(merged["updated_at"], utc=True))
            merged = merged.sort_values("_stamp", kind="stable").drop_duplicates("Ticker", keep="last")
            merged = merged.drop(columns="_stamp").reset_index(drop=True)
        _write_sector_frame(path, _validate_sector_frame(merged))
        return True
    except Exception:
        raise DriveSyncError("sector_download_failed") from None
    finally:
        temporary.unlink(missing_ok=True)


def download_all_years(market: str, uploader=None):
    """전체 목록을 staging에서 검증·병합한 후 반영한다. 실패는 빈 기준이 아니다."""
    if market not in {"us", "crypto"}:
        raise DriveSyncError("ohlc_market_invalid")
    u = _get_uploader(uploader)
    if u is None:
        raise DriveSyncError("ohlc_uploader_unavailable")
    remote_path = config.DRIVE_PATHS.get(f"ohlc_{market}")
    if not remote_path:
        raise DriveSyncError("ohlc_remote_unconfigured")
    local_d = local_dir(market)
    local_d.mkdir(parents=True, exist_ok=True)
    candidates = []
    try:
        # 원격이 absent여도 손상된 기존 로컬 파일을 정상 baseline으로 인증하지 않는다.
        for path in local_d.glob(f"{market}_????.parquet"):
            _read_year_path(path, int(path.stem.rsplit("_", 1)[1]))
        with tempfile.TemporaryDirectory(prefix=f".{market}-baseline-", dir=local_d) as temporary_dir:
            stage = Path(temporary_dir)
            outcome = u.download_all_state(remote_path, str(stage), extensions=(".parquet",))
            if outcome not in {"ok", "absent"}:
                raise DriveSyncError("ohlc_baseline_failed")
            paths = sorted(stage.iterdir())
            if outcome == "absent":
                if paths:
                    raise DriveSyncError("ohlc_baseline_inconsistent")
                return "absent"
            if not paths:
                raise DriveSyncError("ohlc_baseline_incomplete")
            for path in paths:
                if path.name == f"{market}_sector_meta.parquet":
                    continue  # 별도 strict sector baseline 경로가 소유한다.
                match = re.fullmatch(re.escape(market) + r"_([0-9]{4})\.parquet", path.name)
                if not match or not path.is_file() or path.is_symlink():
                    raise DriveSyncError("ohlc_baseline_name_invalid")
                year = int(match.group(1))
                downloaded = _read_year_path(path, year)
                existing = load_year(market, year, strict=True)
                if downloaded.empty:
                    merged = existing if not existing.empty else downloaded
                elif existing.empty:
                    merged = downloaded
                else:
                    merged = downloaded.set_index(["Ticker", "Date"]).combine_first(
                        existing.set_index(["Ticker", "Date"])).reset_index()
                merged.attrs = {}
                dest = local_path(market, year)
                candidate = _temporary_path(dest)
                candidates.append((candidate, dest))
                pq.write_table(pa.Table.from_pandas(merged, preserve_index=False), str(candidate), compression="snappy")
                with candidate.open("r+b") as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
                pd.testing.assert_frame_equal(_read_year_path(candidate, year), merged,
                                              check_dtype=False, check_exact=True)
            # 모든 다운로드/읽기/후보 검증이 끝나기 전에는 기존 연도 파일을 바꾸지 않는다.
            for candidate, dest in candidates:
                os.replace(candidate, dest)
            return "ok" if candidates else "absent"
    except DriveSyncError:
        raise
    except Exception:
        raise DriveSyncError("ohlc_baseline_failed") from None
    finally:
        for candidate, _ in candidates:
            candidate.unlink(missing_ok=True)
