"""
data/ohlc_collector.py — US 주식/ETF 및 크립토 OHLC 수집 (yfinance)

유니버스:
  - US    : S&P500 + NASDAQ100 + DOW30 + LukePicks (동적 크롤링)
            → input/us_universe.txt 파일 있으면 해당 파일 우선 사용
  - Crypto: CoinMarketCap Top 200 (동적 크롤링)
            → input/crypto_universe.txt 파일 있으면 해당 파일 우선 사용

출력 스키마 (OHLC):   Ticker | Date | Open | High | Low | Close | Volume |
                     Amount | ChangesRatio | MarketCap | Dividends | Splits
출력 스키마 (메타):   Ticker | Market | Sector | Industry | updated_at
  - Market: S&P500 / NASDAQ100 / DOW30 / ETF / US / Crypto
  - Sector/Industry: Yahoo Finance .info 기반 (주 1회 수집)
"""

import logging
import re
import time
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── 파일 override 경로 ────────────────────────────────────────────────────────
_UNIVERSE_FILES = {
    "us":     "input/us_universe.txt",
    "crypto": "input/crypto_universe.txt",
}

# ── MANUAL 보완 목록 (동적 크롤링 실패 시 최소 보장) ─────────────────────────
_MANUAL_US = [
    "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "ARKK",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLC", "XLRE", "XLU", "XLP", "XLB", "XLY",
    "GLD", "SLV", "TLT", "HYG", "LQD", "EEM", "EFA", "VWO", "IEMG", "VEA",
    "SOXX", "SMH", "KWEB", "MCHI", "FXI",
    "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "AVGO",
    "LLY", "TSM", "JPM", "V", "UNH", "XOM", "MA", "JNJ", "PG",
    "COST", "HD", "NFLX", "CRM", "AMD", "INTC", "QCOM", "AMAT",
    "PLTR", "SMCI", "ARM", "MSTR", "COIN",
]

_MANUAL_CRYPTO = [
    "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD",
    "AVAX-USD", "TRX-USD", "LINK-USD", "DOT-USD", "MATIC-USD", "LTC-USD", "BCH-USD",
    "UNI-USD", "ATOM-USD", "XLM-USD", "FIL-USD", "HBAR-USD", "ICP-USD",
    "NEAR-USD", "APT-USD", "ARB-USD", "OP-USD", "INJ-USD", "SUI-USD",
    "FTM-USD", "ALGO-USD", "VET-USD", "STX-USD",
]

_BATCH_SIZE = 50   # yfinance rate limit 방지
_CMC_TOP_N  = 200  # CoinMarketCap 상위 N개

# ── ETF 판별 세트 (Market 태그용) ─────────────────────────────────────────────
_ETF_SET = {
    "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "ARKK",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLC", "XLRE", "XLU", "XLP", "XLB", "XLY",
    "GLD", "SLV", "TLT", "HYG", "LQD", "EEM", "EFA", "VWO", "IEMG", "VEA",
    "SOXX", "SMH", "KWEB", "MCHI", "FXI", "TQQQ", "SOXL", "GRNY",
}

# ── HTTP 세션 ─────────────────────────────────────────────────────────────────
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})


# ══════════════════════════════════════════════════════════════════════════════
# 유니버스 동적 크롤링 — US
# ══════════════════════════════════════════════════════════════════════════════

def _normalize_ticker(raw: str) -> str:
    """티커 정규화: 공백 제거, 대문자, '.' → '-', 특수문자 제거."""
    t = str(raw).strip().upper().replace("$", "").replace(".", "-")
    return re.sub(r"[^A-Z0-9\-]", "", t)


def _fetch_sp500() -> list[str]:
    try:
        resp = _SESSION.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", timeout=15
        )
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        tickers = tables[0]["Symbol"].dropna().tolist()
        logger.info(f"[Universe] S&P500: {len(tickers)}종목")
        return tickers
    except Exception as e:
        logger.warning(f"[Universe] S&P500 크롤링 실패: {e}")
        return []


def _fetch_nasdaq100() -> list[str]:
    try:
        resp = _SESSION.get("https://en.wikipedia.org/wiki/Nasdaq-100", timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        for t in tables:
            for col in ("Ticker", "Symbol"):
                if col in t.columns:
                    tickers = t[col].dropna().tolist()
                    logger.info(f"[Universe] NASDAQ100: {len(tickers)}종목")
                    return tickers
    except Exception as e:
        logger.warning(f"[Universe] NASDAQ100 크롤링 실패: {e}")
    return []


def _fetch_dow30() -> list[str]:
    try:
        resp = _SESSION.get(
            "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average", timeout=15
        )
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        for t in tables:
            if "Symbol" in t.columns:
                tickers = t["Symbol"].dropna().tolist()
                logger.info(f"[Universe] DOW30: {len(tickers)}종목")
                return tickers
    except Exception as e:
        logger.warning(f"[Universe] DOW30 크롤링 실패: {e}")
    return []


def _load_luke_picks_from_drive() -> list[str]:
    """
    Google Drive에서 개인 관심종목 리스트(단순 Ticker 컬럼)를 다운로드하여 파싱.
    GOOGLE_SERVICE_ACCOUNT_JSON + GDRIVE_LUKE_PICKS_FILE_ID 둘 다 있어야 동작.
    둘 중 하나라도 없거나 실패하면 조용히 빈 리스트 반환 (필수 기능 아님).
    """
    import os

    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    file_id = os.environ.get("GDRIVE_LUKE_PICKS_FILE_ID", "")
    if not sa_json or not file_id:
        return []

    try:
        import io
        import json as _json
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds = service_account.Credentials.from_service_account_info(
            _json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        meta = service.files().get(fileId=file_id, fields="mimeType").execute()
        mime = meta.get("mimeType", "")

        if mime == "application/vnd.google-apps.spreadsheet":
            raw = service.files().export(
                fileId=file_id, mimeType="text/csv"
            ).execute()
            df = pd.read_csv(io.BytesIO(raw))
        elif mime in (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        ):
            raw = service.files().get_media(fileId=file_id).execute()
            df = pd.read_excel(io.BytesIO(raw))
        else:
            raw = service.files().get_media(fileId=file_id).execute()
            df = pd.read_csv(io.StringIO(raw.decode("utf-8")), sep=None, engine="python")

        for col in ("Ticker", "ticker", "Symbol"):
            if col in df.columns:
                tickers = df[col].dropna().astype(str).str.strip().tolist()
                tickers = [t for t in tickers if t]
                logger.info(f"[Universe] Drive LukePicks: {len(tickers)}종목")
                return tickers

        logger.warning("[Universe] Drive LukePicks: Ticker 컬럼 없음")
        return []

    except Exception as e:
        logger.warning(f"[Universe] Drive LukePicks 로드 실패: {e}")
        return []


def _build_us_universe() -> list[str]:
    """S&P500 + NASDAQ100 + DOW30 + LukePicks + MANUAL → 중복 제거."""
    raw: list[str] = []
    raw.extend(_fetch_sp500())
    raw.extend(_fetch_nasdaq100())
    raw.extend(_fetch_dow30())
    raw.extend(_load_luke_picks_from_drive())
    raw.extend(_MANUAL_US)

    seen: set[str] = set()
    result: list[str] = []
    for t in raw:
        t = _normalize_ticker(t)
        if t and t not in seen:
            seen.add(t)
            result.append(t)

    logger.info(f"[Universe] US 최종: {len(result)}종목 (중복 제거 후)")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 유니버스 동적 크롤링 — Crypto (CoinMarketCap Top 200)
# ══════════════════════════════════════════════════════════════════════════════

_CMC_URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"
_CMC_PARAMS = {
    "start": "1", "limit": str(_CMC_TOP_N),
    "sortBy": "market_cap", "sortType": "desc",
    "convert": "USD", "cryptoType": "all", "tagType": "all",
}


def _fetch_cmc_top200() -> list[str]:
    """CoinMarketCap Top 200 → yfinance 형식 티커 목록 (BTC-USD 등)."""
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent": _SESSION.headers["User-Agent"], "Accept": "application/json"})
        resp = sess.get(_CMC_URL, params=_CMC_PARAMS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        crypto_list = data.get("data", {}).get("cryptoCurrencyList", [])
        if not crypto_list:
            crypto_list = data.get("data", [])

        tickers = []
        for item in crypto_list[:_CMC_TOP_N]:
            symbol = item.get("symbol", "").upper().strip()
            if symbol:
                tickers.append(f"{symbol}-USD")

        logger.info(f"[Universe] CoinMarketCap Top {len(tickers)}종목")
        return tickers
    except Exception as e:
        logger.warning(f"[Universe] CoinMarketCap 크롤링 실패: {e}")
        return []


def _build_crypto_universe() -> list[str]:
    """CoinMarketCap Top 200 → 실패 시 MANUAL 폴백."""
    tickers = _fetch_cmc_top200()
    if tickers:
        return tickers
    logger.warning("[Universe] CMC 실패 → MANUAL 폴백 사용")
    return list(_MANUAL_CRYPTO)


# ══════════════════════════════════════════════════════════════════════════════
# 유니버스 로드 (공개 API)
# ══════════════════════════════════════════════════════════════════════════════

def load_tickers(market: str) -> list[str]:
    """
    티커 목록 로드.
    1순위: input/{market}_universe.txt 파일 (있으면 그대로 사용)
    2순위: 동적 크롤링 (US: Wikipedia, Crypto: CoinMarketCap)
    """
    txt_path = Path(_UNIVERSE_FILES.get(market, ""))
    if txt_path.exists():
        tickers = []
        with open(txt_path, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if t and not t.startswith("#"):
                    tickers.append(t)
        if tickers:
            logger.info(f"[OhlcCollector] {market} 파일 로드: {len(tickers)}종목 ({txt_path})")
            return tickers
        logger.warning(f"[OhlcCollector] {txt_path} 비어 있음 → 동적 크롤링으로 전환")

    if market == "us":
        return _build_us_universe()
    else:
        return _build_crypto_universe()


# ══════════════════════════════════════════════════════════════════════════════
# 핵심 수집 함수
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ohlc_range(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """
    yfinance batch download → flat DataFrame (Ticker | Date | Open | High | Low | Close | Volume)

    - 50개씩 배치 처리 (rate limit 방지)
    - MultiIndex → flat 변환 (단일/복수 ticker 모두 처리)
    - tz 제거, 중복 제거
    - 실패 종목 로깅 후 계속

    Args:
        tickers: 수집할 티커 목록
        start:   시작일 "YYYY-MM-DD"
        end:     종료일 "YYYY-MM-DD" (exclusive, yfinance 관례)
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")

    try:
        from tqdm import tqdm
        batch_iter = tqdm(
            range(0, len(tickers), _BATCH_SIZE),
            desc=f"OHLC fetch {start[:7]}",
            unit="batch",
        )
    except ImportError:
        batch_iter = range(0, len(tickers), _BATCH_SIZE)

    all_rows: list[pd.DataFrame] = []
    failed: list[str] = []

    for i in batch_iter:
        batch = tickers[i: i + _BATCH_SIZE]
        batch_no = i // _BATCH_SIZE + 1
        total_batches = (len(tickers) + _BATCH_SIZE - 1) // _BATCH_SIZE

        logger.debug(
            f"[OhlcCollector] 배치 {batch_no}/{total_batches} "
            f"({len(batch)}종목, {start}~{end})"
        )

        try:
            raw = yf.download(
                batch,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        except Exception as e:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 다운로드 실패: {e}")
            failed.extend(batch)
            time.sleep(2.0)
            continue

        if raw is None or raw.empty:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 빈 응답")
            continue

        # ── MultiIndex / 단일 ticker 분기 처리 ──────────────────────────────
        for ticker in batch:
            try:
                df_t = _extract_ticker_df(raw, ticker, len(batch))
                if df_t is None or df_t.empty:
                    continue

                # tz 제거 및 Date 변환
                df_t = df_t.reset_index()
                date_col = df_t.columns[0]  # 'Date' or 'Datetime'
                df_t[date_col] = pd.to_datetime(df_t[date_col])
                if df_t[date_col].dt.tz is not None:
                    df_t[date_col] = df_t[date_col].dt.tz_localize(None)
                df_t[date_col] = df_t[date_col].dt.date
                df_t = df_t.rename(columns={date_col: "Date"})

                # 컬럼 정규화
                df_t = df_t.rename(columns={
                    "open": "Open", "high": "High", "low": "Low",
                    "close": "Close", "volume": "Volume",
                })

                df_t["Ticker"] = ticker
                keep = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
                df_t = df_t[[c for c in keep if c in df_t.columns]]

                # Close 유효한 행만
                df_t = df_t.dropna(subset=["Close"])
                df_t = df_t[df_t["Close"] > 0]

                if not df_t.empty:
                    # ── 파생 컬럼 추가 ────────────────────────────────────
                    df_t = df_t.sort_values("Date").reset_index(drop=True)
                    # Amount = Close × Volume (거래대금)
                    df_t["Amount"] = df_t["Close"] * df_t["Volume"]
                    # ChangesRatio = (Close / PrevClose - 1) × 100, 첫날 NaN
                    df_t["ChangesRatio"] = (
                        df_t["Close"].pct_change() * 100
                    )
                    # MarketCap: daily update 시 채워짐
                    df_t["MarketCap"] = float("nan")
                    # Dividends / Splits 초기값
                    df_t["Dividends"] = 0.0
                    df_t["Splits"]    = 1.0

                    all_rows.append(df_t)

            except Exception as e:
                logger.debug(f"[OhlcCollector] {ticker} 처리 실패: {e}")
                failed.append(ticker)

        # 배치 간 딜레이 (rate limit)
        time.sleep(1.0)

    if failed:
        logger.warning(f"[OhlcCollector] 실패 종목 ({len(failed)}개): {failed[:20]}")

    _EXTENDED_COLS = [
        "Ticker", "Date", "Open", "High", "Low", "Close", "Volume",
        "Amount", "ChangesRatio", "MarketCap", "Dividends", "Splits",
    ]

    if not all_rows:
        logger.warning(f"[OhlcCollector] {start}~{end} 수집 결과 없음")
        return pd.DataFrame(columns=_EXTENDED_COLS)

    result = pd.concat(all_rows, ignore_index=True)

    # 중복 제거
    result = result.drop_duplicates(subset=["Ticker", "Date"], keep="last")
    result = result.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    # 컬럼 순서 통일
    result = result[[c for c in _EXTENDED_COLS if c in result.columns]]

    logger.info(
        f"[OhlcCollector] 수집 완료: {start}~{end} "
        f"→ {len(result):,}행, {result['Ticker'].nunique()}종목"
    )
    return result


def _extract_ticker_df(raw: pd.DataFrame, ticker: str, batch_size: int) -> Optional[pd.DataFrame]:
    """
    yfinance 응답에서 단일 ticker DataFrame 추출.
    단일 ticker / MultiIndex 양쪽 처리.
    """
    if batch_size == 1:
        # 단일 ticker: 일반 DataFrame
        return raw.copy()

    # 복수 ticker: MultiIndex columns
    cols = raw.columns
    if isinstance(cols, pd.MultiIndex):
        # columns: (metric, ticker) 형태
        # level 확인
        level_0_vals = cols.get_level_values(0).unique().tolist()
        level_1_vals = cols.get_level_values(1).unique().tolist()

        if ticker in level_1_vals:
            # (metric, ticker) → xs로 추출
            try:
                df_t = raw.xs(ticker, axis=1, level=1)
                return df_t
            except Exception:
                pass

        if ticker in level_0_vals:
            # (ticker, metric) → df[ticker]
            try:
                return raw[ticker].copy()
            except Exception:
                pass

        return None

    # 단순 컬럼 (ticker가 최상위 레벨)
    if ticker in cols:
        return raw[ticker].copy()

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 백필 (초기 적재)
# ══════════════════════════════════════════════════════════════════════════════

def backfill_market(
    market: str,
    start_year: int,
    end_year: int,
    tickers: Optional[list[str]] = None,
    upload: bool = True,
):
    """
    연도별 루프로 전체 기간 OHLC 백필.

    Args:
        market:     "us" 또는 "crypto"
        start_year: 수집 시작 연도 (포함)
        end_year:   수집 종료 연도 (포함)
        tickers:    None이면 load_tickers()로 자동 로드
        upload:     True면 연도별 저장 후 Drive 업로드
    """
    from data import ohlc_db

    if tickers is None:
        tickers = load_tickers(market)

    logger.info(
        f"[OhlcCollector] {market.upper()} 백필 시작: "
        f"{start_year}~{end_year}년 / {len(tickers)}종목"
    )

    all_dates: list[date] = []

    for year in range(start_year, end_year + 1):
        start_str = f"{year}-01-01"
        end_str   = f"{year + 1}-01-01"  # yfinance end is exclusive

        logger.info(f"[OhlcCollector] {market.upper()} {year}년 수집 중...")
        try:
            df = fetch_ohlc_range(tickers, start_str, end_str)
        except Exception as e:
            logger.error(f"[OhlcCollector] {market} {year}년 수집 실패: {e}")
            continue

        if df.empty:
            logger.warning(f"[OhlcCollector] {market} {year}년 데이터 없음")
            continue

        ohlc_db.save_year(df, market, year)

        if "Date" in df.columns:
            all_dates.extend(df["Date"].tolist())

        if upload:
            ohlc_db.upload_years(market, [year])

    # 상태 업데이트
    if all_dates:
        last_dt  = max(all_dates)
        oldest_dt = min(all_dates)
        ticker_count = len(tickers)
        ohlc_db.update_status(market, last_dt, ticker_count, oldest_dt)
        if upload:
            ohlc_db.upload_status()

    logger.info(f"[OhlcCollector] {market.upper()} 백필 완료: {start_year}~{end_year}년")


def backfill_new_tickers(
    market: str,
    start_year: int = 2020,
    upload: bool = True,
) -> list[str]:
    """
    현재 유니버스에서 지금까지 한 번도 수집되지 않은 신규 종목을 찾아
    start_year ~ 올해까지 과거 이력을 백필한다.

    Args:
        market:     "us" 또는 "crypto"
        start_year: 백필 시작 연도 (기본 2020, ohlc-backfill 기본값과 동일)
        upload:     True면 연도별 저장 후 Drive 업로드

    Returns:
        새로 발견되어 백필된 티커 목록 (없으면 빈 리스트)
    """
    from data import ohlc_db

    ohlc_db.download_all_years(market)
    ohlc_db.download_status()

    known = ohlc_db.list_known_tickers(market)
    universe = load_tickers(market)
    new_tickers = [t for t in universe if t not in known]

    if not new_tickers:
        logger.info(f"[NewTickerBackfill] {market.upper()} 신규 종목 없음")
        return []

    logger.info(
        f"[NewTickerBackfill] {market.upper()} 신규 종목 {len(new_tickers)}개 발견: {new_tickers}"
    )

    current_year = date.today().year
    all_dates: list[date] = []

    for year in range(start_year, current_year + 1):
        start_str = f"{year}-01-01"
        if year == current_year:
            end_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            end_str = f"{year + 1}-01-01"

        logger.info(
            f"[NewTickerBackfill] {market.upper()} {year}년 수집 중... "
            f"({len(new_tickers)}종목)"
        )
        try:
            df = fetch_ohlc_range(new_tickers, start_str, end_str)
        except Exception as e:
            logger.error(f"[NewTickerBackfill] {market} {year}년 수집 실패: {e}")
            continue

        if df.empty:
            logger.warning(f"[NewTickerBackfill] {market} {year}년 데이터 없음")
            continue

        if year == current_year:
            if market == "crypto":
                df = _enrich_crypto_marketcap(df)
            else:
                df = _enrich_us_marketcap(df)

        ohlc_db.save_year(df, market, year)

        if "Date" in df.columns:
            all_dates.extend(df["Date"].tolist())

        if upload:
            ohlc_db.upload_years(market, [year])

    if all_dates:
        status = ohlc_db.load_status()
        market_status = status.get(market, {})

        new_last = max(all_dates)
        new_oldest = min(all_dates)

        # last_updated는 update_market()의 증분 커서이며 "기존 종목 전체가 이 날짜까지
        # 수집 완료됨"을 의미한다. backfill_new_tickers()는 신규 종목이라는 부분집합만
        # 수집하므로 이 커서를 앞당길 권한이 없다. 기존 값이 있으면 절대 건드리지 않고
        # 그대로 보존하며, status.json 자체가 없는 최초 부트스트랩(신규 마켓)의 경우에만
        # 이번에 수집한 날짜로 초기값을 채운다.
        existing_last_str = market_status.get("last_updated")
        if existing_last_str:
            try:
                last_dt = datetime.strptime(existing_last_str, "%Y-%m-%d").date()
            except ValueError:
                last_dt = new_last
        else:
            last_dt = new_last

        oldest_dt = new_oldest
        existing_oldest_str = market_status.get("oldest_date")
        if existing_oldest_str:
            try:
                existing_oldest = datetime.strptime(existing_oldest_str, "%Y-%m-%d").date()
                oldest_dt = min(existing_oldest, new_oldest)
            except ValueError:
                pass

        ohlc_db.update_status(market, last_dt, len(universe), oldest_dt)
        if upload:
            ohlc_db.upload_status()

    logger.info(f"[NewTickerBackfill] {market.upper()} 신규 종목 백필 완료: {new_tickers}")
    return new_tickers


# ══════════════════════════════════════════════════════════════════════════════
# 증분 업데이트
# ══════════════════════════════════════════════════════════════════════════════

def update_market(
    market: str,
    tickers: Optional[list[str]] = None,
    upload: bool = True,
):
    """
    마지막 업데이트 이후 데이터를 증분 수집.

    1. Drive에서 db_status.json 다운로드
    2. last_date 파싱 (없으면 오늘 - 1년)
    3. start = last_date + 1일, end = 오늘
    4. start >= end 이면 "이미 최신" 로그 후 반환
    5. 현재 연도 parquet을 Drive에서 다운로드 (로컬에 없으면)
    6. fetch_ohlc_range 수집
    7. append_rows + Drive 업로드
    8. update_status 갱신
    """
    from data import ohlc_db

    if tickers is None:
        tickers = load_tickers(market)

    # 1. Drive에서 status 다운로드
    ohlc_db.download_status()

    # 2. last_date 파싱
    status = ohlc_db.load_status()
    market_status = status.get(market, {})
    last_date_str = market_status.get("last_updated")

    if last_date_str:
        try:
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(f"[OhlcCollector] last_updated 파싱 실패: {last_date_str} → 1년 전 사용")
            last_date = date.today() - timedelta(days=365)
    else:
        last_date = date.today() - timedelta(days=365)
        logger.info(f"[OhlcCollector] {market} status 없음 → 기준일: {last_date}")

    # 3. 수집 범위
    start_date = last_date + timedelta(days=1)
    end_date   = date.today() + timedelta(days=1)  # yfinance end는 exclusive이므로 +1

    # 4. 이미 최신이면 종료
    if start_date >= end_date:
        logger.info(
            f"[OhlcCollector] {market.upper()} 이미 최신 상태 "
            f"(last={last_date}, today={end_date})"
        )
        return

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    logger.info(
        f"[OhlcCollector] {market.upper()} 증분 업데이트: "
        f"{start_str} ~ {end_str} / {len(tickers)}종목"
    )

    # 5. 현재 연도 parquet이 로컬에 없으면 Drive에서 다운로드
    current_year = end_date.year
    if not ohlc_db.local_path(market, current_year).exists():
        logger.info(f"[OhlcCollector] {market}_{current_year}.parquet 로컬 없음 → Drive 다운로드 시도")
        ohlc_db.download_year(market, current_year)

    # 6. 수집
    try:
        new_df = fetch_ohlc_range(tickers, start_str, end_str)
    except Exception as e:
        logger.error(f"[OhlcCollector] {market} 증분 수집 실패: {e}")
        return

    if new_df.empty:
        logger.warning(f"[OhlcCollector] {market} 증분 수집 결과 없음")
        return

    # 6-b. MarketCap 보강
    if market == "crypto":
        new_df = _enrich_crypto_marketcap(new_df)
    else:
        new_df = _enrich_us_marketcap(new_df)

    # 7. append + 업로드
    updated_years = ohlc_db.append_rows(new_df, market)
    if upload and updated_years:
        ohlc_db.upload_years(market, updated_years)

    # 8. 상태 갱신
    if "Date" in new_df.columns:
        actual_last = new_df["Date"].max()
        actual_oldest_str = market_status.get("oldest_date")
        actual_oldest: Optional[date] = None
        if actual_oldest_str:
            try:
                actual_oldest = datetime.strptime(actual_oldest_str, "%Y-%m-%d").date()
            except ValueError:
                pass
        ohlc_db.update_status(market, actual_last, len(tickers), actual_oldest)
        if upload:
            ohlc_db.upload_status()

    logger.info(f"[OhlcCollector] {market.upper()} 증분 업데이트 완료")


# ══════════════════════════════════════════════════════════════════════════════
# MarketCap 보강 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _enrich_us_marketcap(df: pd.DataFrame) -> pd.DataFrame:
    """
    US 주식: 수집된 new_df의 각 Ticker에 대해
    yf.Ticker(t).fast_info.market_cap으로 오늘 MarketCap 채우기.
    실패 시 NaN, 에러 로깅 후 계속. 종목당 0.1초 sleep.
    """
    try:
        import yfinance as yf
    except ImportError:
        return df

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    df = df.copy()
    if "MarketCap" not in df.columns:
        df["MarketCap"] = float("nan")

    tickers = df["Ticker"].unique().tolist()
    logger.info(f"[OhlcCollector] US MarketCap 보강 시작: {len(tickers)}종목")

    ticker_iter = tqdm(tickers, desc="MarketCap(US)", unit="ticker") if tqdm else tickers
    cap_map: dict[str, float] = {}

    for t in ticker_iter:
        try:
            mc = yf.Ticker(t).fast_info.market_cap
            cap_map[t] = float(mc) if mc is not None else float("nan")
        except Exception as e:
            logger.debug(f"[OhlcCollector] {t} MarketCap 조회 실패: {e}")
            cap_map[t] = float("nan")
        time.sleep(0.1)

    # 오늘 날짜 행에만 MarketCap 적용 (최신 날짜 기준)
    today = date.today()
    mask = df["Date"] == today
    if not mask.any():
        # 오늘 데이터가 없으면 max Date에 적용
        max_date = df["Date"].max()
        mask = df["Date"] == max_date

    df.loc[mask, "MarketCap"] = df.loc[mask, "Ticker"].map(cap_map)

    filled = mask.sum()
    logger.info(f"[OhlcCollector] US MarketCap 보강 완료: {filled}행 채움")
    return df


def _enrich_crypto_marketcap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crypto: CoinMarketCap에서 현재 시가총액 수집하여 채우기.
    실패 시 NaN, 에러 로깅 후 계속.
    """
    df = df.copy()
    if "MarketCap" not in df.columns:
        df["MarketCap"] = float("nan")

    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": _SESSION.headers["User-Agent"],
            "Accept": "application/json",
        })
        resp = sess.get(_CMC_URL, params=_CMC_PARAMS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        crypto_list = data.get("data", {}).get("cryptoCurrencyList", [])
        if not crypto_list:
            crypto_list = data.get("data", [])

        cap_map: dict[str, float] = {}
        for item in crypto_list:
            symbol = item.get("symbol", "").upper().strip()
            if not symbol:
                continue
            ticker_key = f"{symbol}-USD"
            # CMC quotes 구조: {"USD": {"price": ..., "marketCap": ...}}
            quotes = item.get("quotes", [])
            mc = None
            if isinstance(quotes, list) and quotes:
                mc = quotes[0].get("marketCap")
            elif isinstance(quotes, dict):
                mc = quotes.get("USD", {}).get("marketCap")
            if mc is None:
                mc = item.get("market_cap")
            cap_map[ticker_key] = float(mc) if mc is not None else float("nan")

        logger.info(f"[OhlcCollector] CMC MarketCap 수집: {len(cap_map)}종목")

        # 최신 날짜 행에 적용
        today = date.today()
        mask = df["Date"] == today
        if not mask.any():
            max_date = df["Date"].max()
            mask = df["Date"] == max_date

        df.loc[mask, "MarketCap"] = df.loc[mask, "Ticker"].map(cap_map)
        filled = mask.sum()
        logger.info(f"[OhlcCollector] Crypto MarketCap 보강 완료: {filled}행 채움")

    except Exception as e:
        logger.error(f"[OhlcCollector] CMC MarketCap 수집 실패: {e}")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# 종목 메타데이터 수집 (주 1회 — Sector / Industry / Market 태그)
# ══════════════════════════════════════════════════════════════════════════════

def collect_sector_meta(market: str) -> pd.DataFrame:
    """
    US/Crypto 종목 메타데이터 수집 (주 1회 실행 권장).

    US:
      - 유니버스 전체 조회 → Market 태그 부여 (ETF / DOW30 / S&P500 / NASDAQ100 / US)
      - yf.Ticker(t).info 로 Sector / Industry 수집 (종목당 0.15초 sleep)

    Crypto:
      - 유니버스 전체 → Market="Crypto", Sector/Industry 빈 값

    Returns:
      DataFrame: Ticker | Market | Sector | Industry | updated_at
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")

    from datetime import datetime as _dt
    updated_at = _dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    # ── Crypto ────────────────────────────────────────────────────────────────
    if market == "crypto":
        tickers = load_tickers("crypto")
        rows = [
            {"Ticker": t, "Market": "Crypto", "Sector": "", "Industry": "", "updated_at": updated_at}
            for t in tickers
        ]
        df = pd.DataFrame(rows)
        logger.info(f"[SectorMeta] Crypto 메타 완료: {len(df)}종목")
        return df

    # ── US ────────────────────────────────────────────────────────────────────
    # 유니버스별 세트 구성 (Market 태그 우선순위: ETF > DOW30 > S&P500 > NASDAQ100 > US)
    sp500  = set(_fetch_sp500())
    nasdaq = set(_fetch_nasdaq100())
    dow30  = set(_fetch_dow30())
    all_tickers = load_tickers("us")

    def _tag_market(t: str) -> str:
        if t in _ETF_SET:  return "ETF"
        if t in dow30:     return "DOW30"
        if t in sp500:     return "S&P500"
        if t in nasdaq:    return "NASDAQ100"
        return "US"

    try:
        from tqdm import tqdm
        ticker_iter = tqdm(all_tickers, desc="SectorMeta(US)", unit="ticker")
    except ImportError:
        ticker_iter = all_tickers

    rows = []
    logger.info(f"[SectorMeta] US Sector/Industry 수집 시작: {len(all_tickers)}종목")

    for i, t in enumerate(ticker_iter, 1):
        sector, industry = "", ""
        try:
            info = yf.Ticker(t).info
            sector   = info.get("sector",   "") or ""
            industry = info.get("industry", "") or ""
        except Exception as e:
            logger.debug(f"[SectorMeta] {t} .info 실패: {e}")

        rows.append({
            "Ticker":     t,
            "Market":     _tag_market(t),
            "Sector":     sector,
            "Industry":   industry,
            "updated_at": updated_at,
        })

        if i % 100 == 0:
            logger.info(f"[SectorMeta] 진행: {i}/{len(all_tickers)}")
        time.sleep(0.15)

    df = pd.DataFrame(rows)
    logger.info(f"[SectorMeta] US 메타 완료: {len(df)}종목")
    return df
