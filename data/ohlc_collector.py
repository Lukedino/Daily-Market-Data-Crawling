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

try:
    from yfinance.exceptions import YFRateLimitError
except ImportError:
    YFRateLimitError = None

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
_BATCH_MAX_RETRIES = 3
_RATE_LIMIT_BACKOFF_SECONDS = [30, 60, 120]
_GENERIC_RETRY_BACKOFF_SECONDS = [5, 10, 20]
_CMC_TOP_N  = 200  # CoinMarketCap 상위 N개

# Parquet 저장 스키마 (ohlc_db._SCHEMA_COLS와 동일 순서)
_EXTENDED_COLS = [
    "Ticker", "Date", "Open", "High", "Low", "Close", "Volume",
    "Amount", "ChangesRatio", "MarketCap", "Dividends", "Splits",
]

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


_US_CATEGORY_VALUES = {"미국", "ETF"}
_KR_TICKER_PATTERN = re.compile(r"^\d{6}\.(KS|KQ)$", re.IGNORECASE)


def _load_luke_picks_from_drive() -> list[str]:
    """
    Google Drive에서 개인 관심종목 리스트를 다운로드하여 파싱.
    GOOGLE_SERVICE_ACCOUNT_JSON + GDRIVE_LUKE_PICKS_FILE_ID 둘 다 있어야 동작.
    둘 중 하나라도 없거나 실패하면 조용히 빈 리스트 반환 (필수 기능 아님).

    "구분"(시장분류) 컬럼이 있으면 미국/ETF로 분류된 행만 추출한다
    (Portfolio_ATR_Monitor의 Portfolio.xlsx와 동일 스키마 재사용 지원).
    구분 컬럼이 없으면 단순 티커 리스트로 간주하되, "6자리숫자.KS"/".KQ"
    형식의 한국 종목 코드는 티커 값 자체로 판별해 제외한다
    (실제 사용자 파일은 구분 컬럼 없이 이 형식으로 한국 종목을 구분함).
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

        if "구분" in df.columns:
            df = df[df["구분"].astype(str).str.strip().isin(_US_CATEGORY_VALUES)]

        for col in ("Ticker", "ticker", "Symbol"):
            if col in df.columns:
                tickers = df[col].dropna().astype(str).str.strip().tolist()
                tickers = [
                    t for t in tickers
                    if t and not _KR_TICKER_PATTERN.match(t)
                ]
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

def _finalize_ticker_frame(df_t: pd.DataFrame, label: str) -> Optional[pd.DataFrame]:
    """
    yfinance 원본 프레임 → 저장 스키마로 정규화.

    label은 결과 Ticker 컬럼에 기록할 이름이다. 크립토 티커 충돌 보정
    (_retry_crypto_with_search)에서 실제 조회 심볼(HYPE32196-USD)과
    저장할 이름(HYPE-USD)이 달라지므로 분리해서 받는다.
    """
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

    df_t["Ticker"] = label
    keep = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
    df_t = df_t[[c for c in keep if c in df_t.columns]]

    # Close 유효한 행만
    df_t = df_t.dropna(subset=["Close"])
    df_t = df_t[df_t["Close"] > 0]
    if df_t.empty:
        return None

    # ── 파생 컬럼 추가 ────────────────────────────────────
    df_t = df_t.sort_values("Date").reset_index(drop=True)
    # Amount = Close × Volume (거래대금)
    df_t["Amount"] = df_t["Close"] * df_t["Volume"]
    # ChangesRatio = (Close / PrevClose - 1) × 100, 첫날 NaN
    df_t["ChangesRatio"] = df_t["Close"].pct_change() * 100
    # MarketCap: daily update 시 채워짐
    df_t["MarketCap"] = float("nan")
    # Dividends / Splits 초기값
    df_t["Dividends"] = 0.0
    df_t["Splits"]    = 1.0
    return df_t


# ── 크립토 티커 충돌 보정 ─────────────────────────────────────────────────────
# Yahoo는 심볼이 겹치는 코인을 구분하려고 숫자 접미사를 붙인다
# (예: HYPE-USD는 존재하지 않고 HYPE32196-USD로만 조회됨).
# yf.download()는 exact match만 하므로 이런 종목은 예외도 없이 빈 응답이 되어
# failed 목록에도 안 남고 조용히 누락된다.
#
# 이 접미사는 CoinMarketCap id와 일치한다 (2026-08-12 전수 확인:
# APT21794 / GRT6719 / COMP5692 / POL28321 / PI35697 / HYPE32196 — CMC id와 6/6 일치).
# 유니버스를 CMC에서 받아오므로 id를 이미 갖고 있어 검색 없이 티커를 직접 구성할 수 있다.
_crypto_symbol_cache: dict[str, Optional[str]] = {}
_cmc_id_cache: Optional[dict[str, int]] = None


def _cmc_symbol_id_map() -> dict[str, int]:
    """CMC 목록의 심볼 → id 매핑. 프로세스당 1회만 조회하고 캐싱한다."""
    global _cmc_id_cache
    if _cmc_id_cache is not None:
        return _cmc_id_cache

    id_map: dict[str, int] = {}
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent": _SESSION.headers["User-Agent"],
                             "Accept": "application/json"})
        resp = sess.get(_CMC_URL, params=_CMC_PARAMS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        crypto_list = data.get("data", {}).get("cryptoCurrencyList", [])
        if not crypto_list:
            crypto_list = data.get("data", [])
        for item in crypto_list:
            symbol = str(item.get("symbol", "")).upper().strip()
            cmc_id = item.get("id")
            if symbol and cmc_id:
                id_map[symbol] = int(cmc_id)
        logger.info(f"[CryptoResolve] CMC id 매핑 {len(id_map)}종목 로드")
    except Exception as e:
        logger.warning(f"[CryptoResolve] CMC id 매핑 조회 실패: {e}")

    _cmc_id_cache = id_map
    return id_map


def _search_crypto_yahoo_ticker(symbol: str) -> Optional[str]:
    """
    Yahoo Finance Search로 크립토 실제 티커 탐색 (보조 수단).

    ⚠️ 단독으로는 신뢰할 수 없다. 주식 티커와 겹치는 심볼에서는 관련도 순위에
       밀려 암호화폐가 아예 반환되지 않는다 (APT → Alpha Pro Tech/Aptiv만 나오고
       max_results를 30으로 늘려도 CRYPTOCURRENCY 결과가 0건). 오답도 나온다
       (POL 검색 결과에 Polkadot의 DOT-USD가 섞임). 그래서 CMC id 후보를 먼저
       쓰고 이 함수는 CMC 목록에 없는 종목의 폴백으로만 사용한다.

    정규식을 `^{SYMBOL}\\d*-USD$`로 제한해 위 오답이 채택되는 것을 막는다.
    """
    if symbol in _crypto_symbol_cache:
        return _crypto_symbol_cache[symbol]

    found = None
    try:
        import yfinance as yf
        for q in yf.Search(symbol, max_results=20).quotes:
            yt = str(q.get("symbol", ""))
            if re.match(rf"^{re.escape(symbol)}\d*-USD$", yt, re.IGNORECASE):
                found = yt
                break
    except Exception as e:
        logger.debug(f"[CryptoResolve] {symbol} 검색 실패: {e}")

    _crypto_symbol_cache[symbol] = found
    return found


def _retry_crypto_missing(missing: list[str], start: str, end: str) -> list[pd.DataFrame]:
    """
    배치 수집에서 데이터가 하나도 안 나온 크립토 티커를 대체 심볼로 재수집한다.

    후보 우선순위:
      1. {SYMBOL}{CMC_id}-USD  — 결정적 매핑, 네트워크 조회 없음
      2. yf.Search 결과        — CMC 목록에 없는 종목의 폴백

    ⚠️ 저장되는 Ticker 값은 반드시 원래 이름(HYPE-USD)을 유지해야 한다.
       parquet의 Ticker가 유니버스/워치리스트와 달라지면 소비 측
       (Trading-AI-Pipeline의 CustomChartFetcher 등)이 종목을 못 찾는다.
    """
    import yfinance as yf

    id_map = _cmc_symbol_id_map()
    recovered: list[pd.DataFrame] = []

    for ticker in missing:
        original = ticker.strip().upper()
        symbol = re.sub(r"-USD[T]?$", "", original)

        candidates: list[str] = []
        cmc_id = id_map.get(symbol)
        if cmc_id:
            candidates.append(f"{symbol}{cmc_id}-USD")

        for cand in candidates + ["__search__"]:
            if cand == "__search__":
                # CMC id 후보가 없거나 실패했을 때만 검색 (네트워크 절약)
                cand = _search_crypto_yahoo_ticker(symbol)
                if not cand or cand in candidates:
                    break
            if cand.upper() == original:
                continue

            try:
                raw = yf.download(cand, start=start, end=end, auto_adjust=True,
                                  progress=False, threads=False)
                if raw is None or raw.empty:
                    continue
                if isinstance(raw.columns, pd.MultiIndex):
                    raw = raw.xs(cand, axis=1, level=1)
                df_t = _finalize_ticker_frame(raw.copy(), ticker)
                if df_t is not None:
                    logger.info(f"[CryptoResolve] {ticker} → {cand} 로 복구 ({len(df_t)}행)")
                    recovered.append(df_t)
                    break
            except Exception as e:
                logger.debug(f"[CryptoResolve] {ticker}({cand}) 재수집 실패: {e}")
            finally:
                time.sleep(0.3)

    return recovered


def fetch_ohlc_range(tickers: list[str], start: str, end: str) -> tuple[pd.DataFrame, list[str]]:
    """
    yfinance batch download → flat DataFrame (Ticker | Date | Open | High | Low | Close | Volume)

    - 50개씩 배치 처리 (rate limit 방지)
    - MultiIndex → flat 변환 (단일/복수 ticker 모두 처리)
    - tz 제거, 중복 제거
    - 실패 종목 로깅 후 계속
    - 크립토(-USD)는 배치에서 빈 응답이면 yf.Search로 실제 심볼 재탐색 후 재수집

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

        raw = None
        for attempt in range(_BATCH_MAX_RETRIES + 1):
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
                break
            except Exception as e:
                is_rate_limit = (
                    YFRateLimitError is not None and isinstance(e, YFRateLimitError)
                )
                if attempt >= _BATCH_MAX_RETRIES:
                    logger.warning(
                        f"[OhlcCollector] 배치 {batch_no} 다운로드 실패 "
                        f"(재시도 {_BATCH_MAX_RETRIES}회 모두 소진): {e}"
                    )
                    failed.extend(batch)
                    raw = None
                    break
                backoff = (
                    _RATE_LIMIT_BACKOFF_SECONDS if is_rate_limit
                    else _GENERIC_RETRY_BACKOFF_SECONDS
                )[attempt]
                kind = "rate limit" if is_rate_limit else "일시적 오류"
                logger.warning(
                    f"[OhlcCollector] 배치 {batch_no} 다운로드 실패({kind}): {e} "
                    f"— {backoff}초 후 재시도 ({attempt + 1}/{_BATCH_MAX_RETRIES})"
                )
                time.sleep(backoff)

        if raw is None:
            continue

        if raw.empty:
            logger.warning(f"[OhlcCollector] 배치 {batch_no} 빈 응답")
            continue

        # ── MultiIndex / 단일 ticker 분기 처리 ──────────────────────────────
        for ticker in batch:
            try:
                df_t = _extract_ticker_df(raw, ticker, len(batch))
                if df_t is None or df_t.empty:
                    continue
                df_t = _finalize_ticker_frame(df_t, ticker)
                if df_t is not None:
                    all_rows.append(df_t)
            except Exception as e:
                logger.debug(f"[OhlcCollector] {ticker} 처리 실패: {e}")
                failed.append(ticker)

        # 배치 간 딜레이 (rate limit)
        time.sleep(1.0)

    # ── 크립토 티커 충돌 보정 ────────────────────────────────────────────────
    # 배치에서 한 행도 못 받은 -USD 종목은 Yahoo 심볼 불일치(숫자 접미사) 가능성이
    # 있으므로 Search로 실제 심볼을 찾아 재수집한다. 상장 전 구간이라 정말로
    # 데이터가 없는 경우엔 Search도 빈손이라 그대로 넘어간다.
    collected = {t for df_t in all_rows for t in df_t["Ticker"].unique()}
    missing_crypto = [
        t for t in tickers
        if t not in collected and t.strip().upper().endswith(("-USD", "-USDT"))
    ]
    if missing_crypto:
        logger.info(
            f"[OhlcCollector] 크립토 빈 응답 {len(missing_crypto)}종목 → "
            f"대체 심볼 재탐색 시도: {missing_crypto[:20]}"
        )
        all_rows.extend(_retry_crypto_missing(missing_crypto, start, end))

    if failed:
        logger.warning(f"[OhlcCollector] 실패 종목 ({len(failed)}개): {failed[:20]}")

    if not all_rows:
        logger.warning(f"[OhlcCollector] {start}~{end} 수집 결과 없음")
        return pd.DataFrame(columns=_EXTENDED_COLS), failed

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
    return result, failed


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

        # ⚠️ save_year()는 "로컬 파일이 있으면" 병합하고 없으면 새로 쓴다. GHA 러너는
        # 매번 빈 상태로 시작하므로, 기존 연도 파일을 먼저 받아두지 않으면 이번에
        # 수집한 종목만 담긴 파일이 Drive를 덮어써 버린다. 실제로 2026-08-12
        # crypto 2026 복구 백필에서 유니버스(CMC Top 200)에서 빠진 종목들의 데이터가
        # 이렇게 소실됐다(197종목 → 172종목). update_market()/backfill_new_tickers()는
        # 이미 다운로드를 선행하는데 여기만 빠져 있었다.
        if not ohlc_db.local_path(market, year).exists():
            ohlc_db.download_year(market, year)

        logger.info(f"[OhlcCollector] {market.upper()} {year}년 수집 중...")
        try:
            df, _ = fetch_ohlc_range(tickers, start_str, end_str)
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
    현재 유니버스에서 아직 완전히 백필되지 않은 종목(한 번도 수집된 적 없는
    신규 종목 + 이전 실행에서 일부 연도가 실패해 pending으로 남은 종목)을 찾아
    start_year ~ 올해까지 과거 이력을 백필한다.

    "완전히 백필됨"의 기준은 하드 실패(레이트리밋 등으로 재시도까지 소진)가
    한 번도 없었는지이다. 정상적인 빈 응답(상장 전 등, 예외 없이 빈 결과)은
    실패로 치지 않으므로 pending에 남지 않는다.

    Args:
        market:     "us" 또는 "crypto"
        start_year: 백필 시작 연도 (기본 2020, ohlc-backfill 기본값과 동일)
        upload:     True면 연도별 저장 후 Drive 업로드

    Returns:
        이번에 시도한 티커 목록 (신규 + pending 재시도, 없으면 빈 리스트)
    """
    from data import ohlc_db

    ohlc_db.download_all_years(market)
    ohlc_db.download_status()
    ohlc_db.download_pending()

    known = ohlc_db.list_known_tickers(market)
    pending = set(ohlc_db.load_pending().get(market, []))
    universe = load_tickers(market)
    candidates = [
        t for t in universe if t not in known or t in pending
    ]

    if not candidates:
        logger.info(f"[NewTickerBackfill] {market.upper()} 신규/미완료 종목 없음")
        return []

    new_count = len(candidates) - len(pending & set(candidates))
    retry_count = len(pending & set(candidates))
    logger.info(
        f"[NewTickerBackfill] {market.upper()} 백필 대상 {len(candidates)}개 "
        f"(신규 {new_count}개, 미완료 재시도 {retry_count}개): {candidates}"
    )

    current_year = date.today().year
    all_dates: list[date] = []
    ticker_failed: set[str] = set()

    for year in range(start_year, current_year + 1):
        start_str = f"{year}-01-01"
        if year == current_year:
            end_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            end_str = f"{year + 1}-01-01"

        logger.info(
            f"[NewTickerBackfill] {market.upper()} {year}년 수집 중... "
            f"({len(candidates)}종목)"
        )
        try:
            df, failed_tickers = fetch_ohlc_range(candidates, start_str, end_str)
        except Exception as e:
            logger.error(f"[NewTickerBackfill] {market} {year}년 수집 실패: {e}")
            ticker_failed.update(candidates)
            continue

        ticker_failed.update(failed_tickers)

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

    all_pending = ohlc_db.load_pending()
    all_pending[market] = sorted(ticker_failed)
    ohlc_db.save_pending(all_pending)
    if upload:
        ohlc_db.upload_pending()
    if ticker_failed:
        logger.info(
            f"[NewTickerBackfill] {market.upper()} 다음 실행에 재시도할 "
            f"미완료 종목 {len(ticker_failed)}개: {sorted(ticker_failed)}"
        )

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

    logger.info(f"[NewTickerBackfill] {market.upper()} 백필 완료: {candidates}")
    return candidates


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
    if market == "crypto":
        # ⚠️ [BUG-PARTIAL-CANDLE] 크립토는 24/7이라 "일봉 마감"이 UTC 00:00인데,
        # ohlc-daily.yml은 UTC 22:00에 돈다 — 즉 수집 시점에 오늘 캔들은 아직
        # 2시간 남은 미완성 상태다(Close가 종가가 아닌 22:00 시점 가격, High/Low
        # 마지막 2시간 누락, Volume ~92%). 그런데 last_updated를 그 날짜로 올려
        # 버리면 다음 실행은 그 다음날부터 조회하므로 미완성 캔들이 영구히
        # 확정값으로 남는다.
        # → 커서를 하루 물려 직전 수집일을 다시 받아 덮어쓴다. save_year()가
        #   (Ticker, Date) 중복을 keep="last"로 정리하므로 최신 조회분이 이긴다.
        # US는 장마감(UTC 20~21시)이 크롤 시각보다 앞서 캔들이 이미 확정이라
        # 해당 없음 — 불필요한 재조회를 피하려고 크립토에만 적용한다.
        start_date = last_date
    else:
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
        new_df, _ = fetch_ohlc_range(tickers, start_str, end_str)
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
