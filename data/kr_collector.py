"""
data/kr_collector.py — KR 시장 OHLC+시총 수집

수집 전략:
  [daily]   FDR StockListing × 2 (KOSPI + KOSDAQ + KONEX)
              → 당일 스냅샷: OHLCV + Marcap + Rank + Market 포함
              → marcap 스키마 그대로 사용

  [backfill] yfinance .KS/.KQ 배치 수집
              → 과거 OHLCV (Marcap/Rank = NaN, Market은 종목 목록에서 보완)
              → pykrx 전종목 엔드포인트는 GHA 환경에서 차단됨 → yfinance 우회

출력 스키마 (marcap 표준):
  Code | Name | Close | Dept | ChangeCode | Changes | ChangesRatio |
  Volume | Amount | Open | High | Low | Marcap | Stocks |
  Market | MarketId | Rank | Date
"""

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

import config

logger = logging.getLogger(__name__)

_SUFFIX_MAP = {"KOSPI": ".KS", "KOSDAQ": ".KQ", "KOSDAQ GLOBAL": ".KQ", "KONEX": ".KQ"}
_MARKETID_MAP = {"KOSPI": "STK", "KOSDAQ": "KSQ", "KOSDAQ GLOBAL": "KSQ", "KONEX": "KNX"}
_BATCH_SIZE = 100


# ══════════════════════════════════════════════════════════════════════════════
# [1] Daily — FDR StockListing (당일 스냅샷)
# ══════════════════════════════════════════════════════════════════════════════

def collect_daily() -> pd.DataFrame:
    """
    FDR StockListing으로 오늘 전종목 스냅샷 수집.
    KOSPI + KOSDAQ + KONEX 합산 → marcap 스키마 반환.
    장 마감 후 실행 권장 (당일 종가 반영).
    """
    try:
        import FinanceDataReader as fdr
    except ImportError:
        raise ImportError("finance-datareader를 설치하세요: pip install finance-datareader")

    markets = ["KOSPI", "KOSDAQ", "KONEX"]
    frames = []

    for market in markets:
        try:
            df = fdr.StockListing(market)
            if df is None or df.empty:
                logger.warning(f"[KrCollector] {market} StockListing 빈 응답")
                continue

            df = df.copy()

            # 컬럼 정규화 (FDR 버전에 따라 ChagesRatio / ChangesRatio 혼재)
            if "ChagesRatio" in df.columns and "ChangesRatio" not in df.columns:
                df = df.rename(columns={"ChagesRatio": "ChangesRatio"})
            elif "ChagesRatio" in df.columns and "ChangesRatio" in df.columns:
                df = df.drop(columns=["ChagesRatio"])

            # Unnamed 컬럼 제거
            df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

            # Market / MarketId 보정
            df["Market"] = market
            if "MarketId" not in df.columns:
                df["MarketId"] = _MARKETID_MAP.get(market, "STK")

            # Rank: 시총 기준 내림차순 (시장 내 순위)
            if "Marcap" in df.columns:
                df["Rank"] = df["Marcap"].rank(ascending=False, method="min").astype("Int64")
            else:
                df["Rank"] = 0

            # Date 추가 (오늘)
            df["Date"] = pd.Timestamp(date.today())

            frames.append(df)
            logger.info(f"[KrCollector] {market} StockListing: {len(df)}종목")

        except Exception as e:
            logger.error(f"[KrCollector] {market} StockListing 실패: {e}")

    if not frames:
        logger.error("[KrCollector] daily 수집 결과 없음")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = _normalize_schema(result)

    logger.info(f"[KrCollector] daily 수집 완료: {len(result):,}종목")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# [1b] Daily 보완 — FDR StockListing 누락 종목 yfinance fallback
# ══════════════════════════════════════════════════════════════════════════════

def collect_missing_today(
    missing_codes: list[str],
    code_meta: dict[str, dict],
    target_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    FDR StockListing 누락 종목을 yfinance로 보완 수집 (당일 단일 거래일).

    매매정지·관리종목·일시 누락 등으로 FDR 스냅샷에서 빠진 종목 중
    yfinance에 당일 거래 데이터가 있는 종목을 marcap 스키마로 반환.

    Args:
        missing_codes: 6자리 종목코드 리스트
        code_meta: {code: {"Name": ..., "Market": ...}} (직전 parquet에서 추출)
        target_date: 수집 대상 거래일 (기본 today)

    Returns:
        marcap 스키마 DataFrame (Marcap/Rank=NaN)
    """
    return _collect_yfinance_day(missing_codes, code_meta, target_date, "누락 종목 보완")


def collect_daily_fallback(
    code_meta: dict[str, dict],
    target_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    FDR StockListing이 통째로 죽었을 때 당일 **전종목** 스냅샷을 yfinance로 수집.

    2026-09-08 사고: FDR의 StockListing은 KRX가 아니라 제3자 GitHub 캐시
    저장소의 날짜별 CSV를 읽는다. 그 저장소가 그날치를 안 올리면 세 시장 전부
    404가 나고 수집이 0건이 된다. 종목 목록·이름·시장 구분은 이미 받아둔
    parquet에 다 들어있으므로, 그걸 code_meta로 넘겨 당일 시세만 yfinance로
    받아오면 하루를 통째로 잃지 않는다.

    보완(collect_missing_today)과 기계적으로 같은 경로지만 대상이 전종목이라
    로그 문구를 구분한다 — 사후에 로그만 보고 "그날 FDR이 죽어서 폴백으로
    받은 날"임을 알 수 있어야 한다.

    Args:
        code_meta: {code: {"Name": ..., "Market": ...}} (직전 parquet에서 추출)
        target_date: 수집 대상 거래일 (기본 today)

    Returns:
        marcap 스키마 DataFrame (Marcap/Rank=NaN — yfinance에 시총 정보 없음)
    """
    return _collect_yfinance_day(sorted(code_meta), code_meta, target_date,
                                 "FDR 폴백 전종목")


def _collect_yfinance_day(
    codes: list[str],
    code_meta: dict[str, dict],
    target_date: Optional[date],
    label: str,
) -> pd.DataFrame:
    """
    지정한 종목들의 **단일 거래일** 시세를 yfinance로 수집 (marcap 스키마).

    collect_missing_today(일부 종목)와 collect_daily_fallback(전종목)이 공유한다.
    label은 로그 문구에만 쓰인다 — 어느 경로로 받은 데이터인지 로그로 구분한다.
    """
    missing_codes = list(codes)
    if not missing_codes:
        return pd.DataFrame()

    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")

    tgt = target_date or date.today()
    # yfinance는 end exclusive — 거래일 + 1일 윈도우, 안전 마진 위해 -1 ~ +1
    start_str = (tgt - timedelta(days=1)).strftime("%Y-%m-%d")
    end_str = (tgt + timedelta(days=1)).strftime("%Y-%m-%d")

    # yf_ticker 구성 (메타 없으면 KOSPI(.KS) 기본)
    yf_tickers = []
    code_by_yf = {}
    for code in missing_codes:
        meta = code_meta.get(code, {})
        market = meta.get("Market") or "KOSPI"
        suffix = _SUFFIX_MAP.get(market, ".KS")
        yf_t = f"{code}{suffix}"
        yf_tickers.append(yf_t)
        code_by_yf[yf_t] = code

    logger.info(
        f"[KrCollector] {label} yfinance 수집: {len(yf_tickers)}종목 "
        f"({tgt})"
    )

    all_rows = []
    for i in range(0, len(yf_tickers), _BATCH_SIZE):
        batch = yf_tickers[i: i + _BATCH_SIZE]
        try:
            raw = yf.download(
                batch,
                start=start_str,
                end=end_str,
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        except Exception as e:
            logger.warning(f"[KrCollector] 보완 배치 다운로드 실패: {e}")
            time.sleep(1.0)
            continue

        if raw is None or raw.empty:
            continue

        for yf_t in batch:
            try:
                df_t = _extract_ticker(raw, yf_t, len(batch))
                if df_t is None or df_t.empty:
                    continue

                code = code_by_yf[yf_t]
                meta = code_meta.get(code, {})
                name = meta.get("Name", "")
                market = meta.get("Market") or "KOSPI"

                df_t = df_t.reset_index()
                df_t["Date"] = pd.to_datetime(df_t["Date"])
                if df_t["Date"].dt.tz is not None:
                    df_t["Date"] = df_t["Date"].dt.tz_localize(None)

                # target_date에 해당하는 행만 선택 (보완 목적상 당일 단일행)
                df_t = df_t[df_t["Date"].dt.date == tgt]
                if df_t.empty:
                    continue

                df_t["Code"] = code
                df_t["Name"] = name
                df_t["Market"] = market
                df_t["MarketId"] = _MARKETID_MAP.get(market, "STK")
                df_t["Dept"] = None
                df_t["ChangeCode"] = None
                df_t["Changes"] = float("nan")
                df_t["ChangesRatio"] = float("nan")
                df_t["Amount"] = df_t["Close"] * df_t["Volume"]
                df_t["Marcap"] = float("nan")
                df_t["Stocks"] = 0
                df_t["Rank"] = 0

                df_t = df_t.dropna(subset=["Close"])
                df_t = df_t[df_t["Close"] > 0]
                if df_t.empty:
                    continue
                all_rows.append(_normalize_schema(df_t))

            except Exception as e:
                logger.debug(f"[KrCollector] {yf_t} 보완 스킵: {type(e).__name__}: {e}")

        time.sleep(0.5)

    if not all_rows:
        logger.info(f"[KrCollector] {label}: 유효 결과 없음 (상장폐지·휴장 추정)")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result = result.drop_duplicates(subset=["Code", "Date"], keep="last")
    logger.info(f"[KrCollector] {label} 완료: {len(result)}종목")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# [2] Backfill — yfinance (과거 OHLCV)
# ══════════════════════════════════════════════════════════════════════════════

def collect_backfill(start_date: str, end_date: str,
                     fallback_meta: Optional[dict[str, dict]] = None) -> pd.DataFrame:
    """
    yfinance로 과거 기간 전종목 OHLCV 수집.
    - FDR StockListing으로 현재 종목 목록 확보 (Code + Name + Market)
    - yfinance .KS/.KQ 배치 수집 (100종목씩)
    - Marcap / Rank = NaN (과거 시총 정보 없음)

    ⚠️ 시세는 yfinance에서 오지만 **종목 목록은 FDR에서 온다.** 그래서 FDR이
       죽으면 이 폴백 경로도 함께 죽는다 — 2026-09-08에는 "오늘 못 받아도 내일
       갭 backfill이 메운다"는 자가 치유가 같은 404로 막혀 있었다.
       fallback_meta를 넘기면 기존 parquet의 종목 목록으로 대신 돈다.

    Args:
        start_date: "YYYY-MM-DD"
        end_date:   "YYYY-MM-DD" (포함)
        fallback_meta: {code: {"Name": ..., "Market": ...}} — FDR이 종목 목록을
            한 시장도 못 줄 때 쓸 대체 유니버스. None이면 기존대로 중단한다.
    """
    try:
        import yfinance as yf
        import FinanceDataReader as fdr
    except ImportError as e:
        raise ImportError(f"필요 라이브러리 미설치: {e}")

    # 날짜 유효성 검증
    try:
        start_dt_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt_obj   = datetime.strptime(end_date,   "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"[KrCollector] 날짜 형식 오류: {e}  (YYYY-MM-DD 형식 필요)")
        return pd.DataFrame()

    if start_dt_obj > end_dt_obj:
        logger.error(f"[KrCollector] start_date({start_date}) > end_date({end_date}) → 종료")
        return pd.DataFrame()

    # 종목 목록 확보
    universe = _build_universe(fdr, fallback_meta=fallback_meta)
    if universe.empty:
        logger.error("[KrCollector] 종목 목록 없음 → backfill 중단")
        return pd.DataFrame()

    logger.info(
        f"[KrCollector] backfill 시작: {start_date} ~ {end_date} / "
        f"{len(universe)}종목"
    )

    # yfinance end는 exclusive
    end_dt = end_dt_obj + timedelta(days=1)
    end_str = end_dt.strftime("%Y-%m-%d")

    all_rows = []
    tickers_yf = universe["yf_ticker"].tolist()
    total_batches = (len(tickers_yf) + _BATCH_SIZE - 1) // _BATCH_SIZE

    for i in range(0, len(tickers_yf), _BATCH_SIZE):
        batch_yf = tickers_yf[i: i + _BATCH_SIZE]
        batch_no = i // _BATCH_SIZE + 1
        logger.info(f"[KrCollector] 배치 {batch_no}/{total_batches} ({len(batch_yf)}종목)")

        try:
            raw = yf.download(
                batch_yf,
                start=start_date,
                end=end_str,
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        except Exception as e:
            logger.warning(f"[KrCollector] 배치 {batch_no} 다운로드 실패: {e}")
            time.sleep(2.0)
            continue

        if raw is None or raw.empty:
            logger.debug(f"[KrCollector] 배치 {batch_no} 빈 응답 (전체 상장폐지 또는 해당 기간 데이터 없음)")
            continue

        for yf_t in batch_yf:
            try:
                df_t = _extract_ticker(raw, yf_t, len(batch_yf))
                if df_t is None or df_t.empty:
                    continue

                code = yf_t.split(".")[0]
                meta = universe[universe["Code"] == code]
                name = meta["Name"].iloc[0] if not meta.empty else ""
                market = meta["Market"].iloc[0] if not meta.empty else "KOSPI"

                df_t = df_t.reset_index()
                df_t["Date"] = pd.to_datetime(df_t["Date"])
                if df_t["Date"].dt.tz is not None:
                    df_t["Date"] = df_t["Date"].dt.tz_localize(None)

                df_t = df_t.rename(columns={
                    "Open": "Open", "High": "High", "Low": "Low",
                    "Close": "Close", "Volume": "Volume",
                })

                df_t["Code"] = code
                df_t["Name"] = name
                df_t["Market"] = market
                df_t["MarketId"] = _MARKETID_MAP.get(market, "STK")

                # marcap 스키마에서 yfinance로 채울 수 없는 컬럼 → NaN/None
                df_t["Dept"] = None
                df_t["ChangeCode"] = None
                df_t["Changes"] = df_t["Close"].diff()
                df_t["ChangesRatio"] = df_t["Close"].pct_change(fill_method=None) * 100
                df_t["Amount"] = df_t["Close"] * df_t["Volume"]
                df_t["Marcap"] = float("nan")
                df_t["Stocks"] = 0
                df_t["Rank"] = 0

                df_t = df_t.dropna(subset=["Close"])
                df_t = df_t[df_t["Close"] > 0]
                all_rows.append(_normalize_schema(df_t))

            except Exception as e:
                # yfinance가 Yahoo Finance로부터 유효하지 않은 날짜(예: 2026-00-01)를
                # 받아오는 경우 ValueError 발생 → 상장폐지/데이터 없음으로 간주하고 스킵
                logger.debug(f"[KrCollector] {yf_t} 스킵: {type(e).__name__}: {e}")

        time.sleep(1.0)

    if not all_rows:
        logger.warning(f"[KrCollector] backfill 수집 결과 없음: {start_date}~{end_date}")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result = result.drop_duplicates(subset=["Code", "Date"], keep="last")
    result = result.sort_values(["Date", "Code"]).reset_index(drop=True)

    logger.info(
        f"[KrCollector] backfill 완료: {len(result):,}행 "
        f"/ {result['Code'].nunique()}종목 "
        f"/ {result['Date'].dt.date.nunique()}거래일"
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _build_universe(fdr, fallback_meta: Optional[dict[str, dict]] = None) -> pd.DataFrame:
    """
    KOSPI + KOSDAQ + KONEX 종목 목록 → Code / Name / Market / yf_ticker.

    ⚠️ FDR StockListing이 죽으면(2026-09-08의 상류 캐시 404) 이 함수도 함께
       죽어서, yfinance 백필이라는 폴백 경로 자체가 성립하지 않았다. 종목
       목록은 이미 받아둔 parquet에 들어있으므로 fallback_meta로 넘기면
       FDR 없이도 유니버스를 만든다.

    Args:
        fdr: FinanceDataReader 모듈 (테스트에서 스텁 주입)
        fallback_meta: {code: {"Name": ..., "Market": ...}} — FDR이 한 시장도
            못 주면 이걸로 유니버스를 만든다. None이면 기존대로 빈 결과.
    """
    frames = []
    for market in ["KOSPI", "KOSDAQ", "KONEX"]:
        try:
            df = fdr.StockListing(market)
            if df is None or df.empty:
                continue
            df = df.copy()
            df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
            code_col = "Code" if "Code" in df.columns else df.columns[0]
            name_col = "Name" if "Name" in df.columns else df.columns[1]
            sub = df[[code_col, name_col]].rename(columns={code_col: "Code", name_col: "Name"})
            sub["Code"] = sub["Code"].astype(str).str.zfill(6)
            sub["Market"] = market
            sub["yf_ticker"] = sub["Code"] + _SUFFIX_MAP.get(market, ".KS")
            frames.append(sub)
        except Exception as e:
            logger.warning(f"[KrCollector] {market} 종목 목록 실패: {e}")

    if not frames:
        if not fallback_meta:
            return pd.DataFrame()
        logger.warning(
            f"[KrCollector] FDR 종목 목록 전멸 → 기존 parquet에서 유니버스 구성 "
            f"({len(fallback_meta)}종목)"
        )
        fb = pd.DataFrame(
            [
                {
                    "Code": str(code).zfill(6),
                    "Name": (meta or {}).get("Name", "") or "",
                    "Market": (meta or {}).get("Market") or "KOSPI",
                }
                for code, meta in sorted(fallback_meta.items())
            ]
        )
        fb["yf_ticker"] = fb["Code"] + fb["Market"].map(
            lambda m: _SUFFIX_MAP.get(m, ".KS")
        )
        return fb

    universe = pd.concat(frames, ignore_index=True)
    universe = universe.drop_duplicates(subset=["Code"]).reset_index(drop=True)
    logger.info(f"[KrCollector] 유니버스 구성: {len(universe)}종목")
    return universe


def _extract_ticker(raw: pd.DataFrame, ticker: str, batch_size: int) -> Optional[pd.DataFrame]:
    """yfinance MultiIndex 응답에서 단일 ticker 추출."""
    if batch_size == 1:
        return raw.copy()

    cols = raw.columns
    if isinstance(cols, pd.MultiIndex):
        level_1 = cols.get_level_values(1).unique().tolist()
        level_0 = cols.get_level_values(0).unique().tolist()
        if ticker in level_1:
            try:
                return raw.xs(ticker, axis=1, level=1)
            except Exception:
                pass
        if ticker in level_0:
            try:
                return raw[ticker].copy()
            except Exception:
                pass
        return None

    if ticker in cols:
        return raw[ticker].copy()
    return None


def _normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼을 marcap 표준 스키마로 정렬. 없는 컬럼은 NaN으로 채움."""
    from data.kr_db import SCHEMA_COLS
    for col in SCHEMA_COLS:
        if col not in df.columns:
            df[col] = None
    return df[SCHEMA_COLS].copy()
