"""
data/collector.py — 원자 수집 함수 모음

데이터 소스:
  - FDR (FinanceDataReader): 종목 유니버스 (kind.krx.co.kr, 차단 없음)
  - yfinance               : 일별 OHLCV (Yahoo Finance)
  - pykrx                  : 시장 스냅샷 PER/PBR/시가총액 (09:00-15:30 KST 중 작동)
  - DART OpenAPI           : 연간 재무제표 (OpenDartReader)
  - CompanyGuide           : 재무비율 크롤링 (ROE/ROA/EV_EBITDA 등)

⚠️  pykrx 전종목시세 엔드포인트는 자동화 스크립트에서 차단될 수 있음.
    유니버스/일별가격은 FDR+yfinance 사용 (GitHub Actions + 로컬 공통).
"""

import logging
import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)

# ── pykrx 임포트 (선택적) ─────────────────────────────────────────────────────
try:
    from pykrx import stock as krx
    logger.debug("[Collector] pykrx 로드 완료")
except ImportError:
    krx = None
    logger.warning("[Collector] pykrx 미설치 — 시장 데이터 수집 불가")

# ── OpenDartReader 임포트 (선택적) ────────────────────────────────────────────
try:
    import OpenDartReader
    logger.debug("[Collector] OpenDartReader 로드 완료")
except ImportError:
    OpenDartReader = None
    logger.warning("[Collector] OpenDartReader 미설치 — DART 수집 불가")

# ── yfinance 임포트 (선택적) ──────────────────────────────────────────────────
try:
    import yfinance as yf
    logger.debug("[Collector] yfinance 로드 완료")
except ImportError:
    yf = None
    logger.warning("[Collector] yfinance 미설치 — 일별 주가 수집 불가")

# ── FinanceDataReader 임포트 (선택적) ─────────────────────────────────────────
try:
    import FinanceDataReader as fdr
    logger.debug("[Collector] FinanceDataReader 로드 완료")
except ImportError:
    fdr = None
    logger.warning("[Collector] FinanceDataReader 미설치 — 종목 목록 조회 불가")


# ══════════════════════════════════════════════════════════════════════════════
# [0] 유틸리티
# ══════════════════════════════════════════════════════════════════════════════

def _safe_float(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", str(text))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _retry(fn, *args, label: str = "", **kwargs):
    """재시도 3회 + 지수 백오프. 성공 시 결과 반환, 모두 실패 시 None 반환."""
    for attempt in range(config.MAX_RETRY):
        try:
            if attempt > 0:
                wait = config.BACKOFF_BASE ** attempt + random.uniform(1, 3)
                logger.info(f"[Retry] {label} 재시도 {attempt}/{config.MAX_RETRY - 1} ({wait:.1f}초 대기)")
                time.sleep(wait)
            return fn(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[Retry] {label} 시도 {attempt + 1} 실패: {e}")
    logger.error(f"[Retry] {label} 최대 재시도 초과")
    return None


def get_last_business_day(date: Optional[str] = None) -> str:
    """주어진 날짜(YYYYMMDD) 또는 오늘 기준 최근 영업일 반환.

    주말/공휴일을 제외한 가장 최근 평일 반환.
    (pykrx 불필요 — 단순 평일 계산)
    """
    target = datetime.today() if date is None else datetime.strptime(date, "%Y%m%d")
    for i in range(10):
        d = target - timedelta(days=i)
        if d.weekday() < 5:  # 0=월 ~ 4=금
            return d.strftime("%Y%m%d")
    return target.strftime("%Y%m%d")


# ══════════════════════════════════════════════════════════════════════════════
# [1] 유니버스 (FDR) + 펀더멘탈 (pykrx 개별종목 API)
# ══════════════════════════════════════════════════════════════════════════════

def get_universe(date: str = None) -> list[str]:
    """KOSPI + KOSDAQ 전종목 ticker 리스트 반환.

    FinanceDataReader StockListing 사용 (kind.krx.co.kr — 차단 없음).
    """
    if fdr is None:
        raise ImportError("FinanceDataReader를 설치하세요: pip install finance-datareader")

    tickers = []
    for market in config.MARKETS:
        try:
            listing = fdr.StockListing(market)
            code_col = "Symbol" if "Symbol" in listing.columns else "Code"
            t = listing[code_col].dropna().astype(str).tolist()
            tickers.extend(t)
            logger.info(f"[Collector] {market} 유니버스: {len(t)}종목 (FDR, col={code_col})")
        except Exception as e:
            logger.error(f"[Collector] {market} 종목 목록 조회 실패: {e}")

    return list(set(tickers))


def get_fundamentals_range(start_date: str, end_date: str,
                            tickers: Optional[list] = None) -> pd.DataFrame:
    """
    전체 기간 펀더멘탈 수집 — 종목별 1회 호출.

    pykrx get_market_fundamental(start, end, ticker) 사용.
    개별종목시세 엔드포인트 — GitHub Actions(Azure) / 로컬 공통 차단 없음.

    Bootstrap에서 전체 기간을 한 번에 수집 후 월별 재구성에 활용.
      예) get_fundamentals_range("20100101", "20251231")
          → 2,000 종목 × 1회 API 호출 ≈ 10분

    Returns DataFrame: date(YYYYMMDD), ticker, PER, PBR, EPS, BPS, DIV
    """
    if krx is None:
        raise ImportError("pykrx를 설치하세요: pip install pykrx")

    if tickers is None:
        tickers = get_universe()

    logger.info(
        f"[Collector] 펀더멘탈 범위 수집: {len(tickers)}종목 "
        f"({start_date}~{end_date}) — pykrx 개별종목 API"
    )

    try:
        from tqdm import tqdm
        ticker_iter = tqdm(tickers, desc="Fundamentals")
    except ImportError:
        ticker_iter = tickers

    all_rows = []
    for ticker in ticker_iter:
        for attempt in range(config.MAX_RETRY):
            try:
                df = krx.get_market_fundamental(start_date, end_date, ticker)
                if df is None or df.empty:
                    break
                df = df.reset_index()
                # 인덱스 컬럼명 정규화 ('날짜' 또는 datetime index)
                date_col = df.columns[0]
                df = df.rename(columns={date_col: "date"})
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
                df["ticker"] = ticker
                keep = ["date", "ticker", "PER", "PBR", "EPS", "BPS", "DIV"]
                df = df[[c for c in keep if c in df.columns]]
                all_rows.append(df)
                break
            except Exception as e:
                if attempt < config.MAX_RETRY - 1:
                    time.sleep(config.BACKOFF_BASE ** attempt + random.uniform(0, 1))
                else:
                    logger.debug(f"[Collector] {ticker} 펀더멘탈 실패: {e}")
        time.sleep(random.uniform(0.2, 0.4))

    if not all_rows:
        logger.warning("[Collector] 펀더멘탈 수집 결과 없음")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    logger.info(
        f"[Collector] 펀더멘탈 수집 완료: {len(result):,}건 "
        f"({result['ticker'].nunique()}종목)"
    )
    return result


def get_market_snapshot(date: str) -> pd.DataFrame:
    """
    단일 날짜 시장 스냅샷 (일별 Daily 모드용).

    get_fundamentals_range(date, date) 로 당일 PER/PBR 수집.
    Bootstrap에는 historical.collect_market_range() 를 사용하세요.

    Returns DataFrame: date, ticker, PER, PBR, EPS, BPS, DIV
    """
    return get_fundamentals_range(date, date)


# ══════════════════════════════════════════════════════════════════════════════
# [2] pykrx — 일별 OHLCV (GitHub Actions 전용)
# ══════════════════════════════════════════════════════════════════════════════

def get_daily_prices_month(yyyymm: str) -> pd.DataFrame:
    """
    특정 월의 전체 일별 OHLCV 수집 (전종목).

    yfinance (Yahoo Finance) + FDR 종목 목록 사용.
    KRX 전종목시세 엔드포인트 차단 우회 — 로컬/Actions 공통 경로.

    Returns DataFrame: date, ticker, open, high, low, close, volume
    """
    if yf is None:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")
    if fdr is None:
        raise ImportError("FinanceDataReader를 설치하세요: pip install finance-datareader")

    year  = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end_dt = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = datetime(year, month + 1, 1) - timedelta(days=1)
    end   = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")  # yfinance end 는 exclusive

    logger.info(f"[Collector] 일별 주가 수집 (yfinance): {yyyymm} ({start}~{end_dt.strftime('%Y-%m-%d')})")

    # 종목 목록 — FinanceDataReader StockListing (kind.krx.co.kr, 차단 없음)
    ticker_map: dict[str, str] = {}  # "005930.KS" → "005930"
    suffix_map = {"KOSPI": ".KS", "KOSDAQ": ".KQ"}
    for market in config.MARKETS:
        try:
            listing = fdr.StockListing(market)
            code_col = "Symbol" if "Symbol" in listing.columns else "Code"
            sfx = suffix_map.get(market, ".KS")
            for ticker in listing[code_col].dropna().astype(str):
                ticker_map[f"{ticker}{sfx}"] = ticker
            logger.info(f"[Collector] {market} 종목 수: {len(listing)}")
        except Exception as e:
            logger.error(f"[Collector] {market} 종목 목록 조회 실패: {e}")

    if not ticker_map:
        logger.warning(f"[Collector] {yyyymm} 종목 목록 없음")
        return pd.DataFrame()

    yf_tickers = list(ticker_map.keys())
    logger.info(f"[Collector] {yyyymm} 종목 수: {len(yf_tickers)}")

    # yfinance 배치 다운로드 (500종목씩)
    BATCH = 500
    all_rows: list[pd.DataFrame] = []

    for i in range(0, len(yf_tickers), BATCH):
        batch = yf_tickers[i : i + BATCH]
        batch_no = i // BATCH + 1
        total_batches = (len(yf_tickers) + BATCH - 1) // BATCH
        logger.info(f"[Collector] {yyyymm} 배치 {batch_no}/{total_batches} ({len(batch)}종목)")
        try:
            raw = yf.download(
                batch, start=start, end=end,
                auto_adjust=True, progress=False,
                group_by="ticker", threads=True,
            )
            if raw is None or raw.empty:
                logger.warning(f"[Collector] {yyyymm} 배치 {batch_no} 빈 응답")
                continue

            for yf_t in batch:
                krx_t = ticker_map[yf_t]
                try:
                    df_t = raw[yf_t].copy() if len(batch) > 1 else raw.copy()
                    df_t = df_t.dropna(subset=["Close"])
                    if df_t.empty:
                        continue
                    df_t = df_t.reset_index()
                    df_t = df_t.rename(columns={
                        "Date": "date", "Open": "open", "High": "high",
                        "Low": "low", "Close": "close", "Volume": "volume",
                    })
                    df_t["date"] = pd.to_datetime(df_t["date"]).dt.strftime("%Y%m%d")
                    df_t["ticker"] = krx_t
                    keep = ["date", "ticker", "open", "high", "low", "close", "volume"]
                    df_t = df_t[[c for c in keep if c in df_t.columns]]
                    df_t = df_t[df_t["close"] > 0]
                    if not df_t.empty:
                        all_rows.append(df_t)
                except Exception as e:
                    logger.debug(f"[Collector] {yf_t} 처리 실패: {e}")

        except Exception as e:
            logger.warning(f"[Collector] {yyyymm} 배치 {batch_no} 다운로드 실패: {e}")

        time.sleep(1.0)  # Yahoo Finance 레이트 리밋

    if not all_rows:
        logger.warning(f"[Collector] {yyyymm} 일별 주가 수집 결과 없음")
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    logger.info(f"[Collector] {yyyymm} 일별 주가 수집 완료: {len(result):,}건")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# [3] DART OpenAPI — 연간 재무제표
# ══════════════════════════════════════════════════════════════════════════════

def _get_dart():
    """OpenDartReader 인스턴스 반환."""
    if OpenDartReader is None:
        raise ImportError("OpenDartReader를 설치하세요: pip install OpenDartReader")
    if not config.DART_API_KEY:
        raise ValueError("DART_API_KEY가 설정되지 않았습니다. GitHub Secrets에 등록하세요.")
    import inspect
    if inspect.isclass(OpenDartReader):
        return OpenDartReader(config.DART_API_KEY)
    return OpenDartReader.OpenDartReader(config.DART_API_KEY)


def get_dart_financials(ticker: str, year: int) -> dict:
    """
    DART 단일 종목 연간 재무제표 수집.
    연결재무제표(CFS) 우선, 없으면 별도재무제표(OFS) 사용.

    Returns: dict {year, ticker, name, fs_type, 매출액, 영업이익, ...}
    """
    dart = _get_dart()
    time.sleep(random.uniform(*config.DELAY_API))

    try:
        result = {"year": year, "ticker": ticker}

        # OpenDartReader 0.2.2 의 finstate()는 fs_div 인자를 받지 않는다 — 응답에 CFS/OFS 행이
        # 함께 오고 fs_div 컬럼으로 구분된다. 옛 코드는 fs_div=... 를 넘겨 TypeError 로
        # 전 종목이 조용히 빠졌다(kr_financials_collector 와 같은 선택 규칙으로 정리, 2026-09-05).
        df_all = dart.finstate(ticker, year, reprt_code="11011")
        df = None
        if df_all is not None and not df_all.empty and "fs_div" in df_all.columns:
            for fs_div in ["CFS", "OFS"]:
                sub = df_all[df_all["fs_div"] == fs_div]
                if not sub.empty:
                    df = sub.copy()
                    result["fs_type"] = fs_div
                    break
        if df is None:
            logger.debug(f"[DART] {ticker} {year} 재무제표 없음")
            return {}

        # 종목명
        try:
            result["name"] = dart.corp_name(ticker) or ""
        except Exception:
            result["name"] = ""

        # 계정 과목 매핑
        account_map = {
            "매출액":       ["매출액", "영업수익", "수익(매출액)"],
            "매출원가":     ["매출원가", "영업비용"],
            "영업이익":     ["영업이익", "영업이익(손실)"],
            "당기순이익":   ["당기순이익", "당기순이익(손실)", "분기순이익"],
            "자산총계":     ["자산총계"],
            "부채총계":     ["부채총계"],
            "자본총계":     ["자본총계"],
            "영업현금흐름": ["영업활동현금흐름", "영업활동으로인한현금흐름"],
            "매출총이익":   ["매출총이익"],
        }

        df["account_nm"] = df["account_nm"].str.strip()

        for col, keywords in account_map.items():
            for kw in keywords:
                row = df[df["account_nm"] == kw]
                if not row.empty:
                    val_str = str(row.iloc[0].get("thstrm_amount", "0") or "0").replace(",", "")
                    try:
                        result[col] = float(val_str)
                    except ValueError:
                        result[col] = None
                    break

        # 매출총이익 = 매출액 - 매출원가 (직접 수록이 없을 경우 계산)
        if "매출총이익" not in result and "매출액" in result and "매출원가" in result:
            r, c = result.get("매출액"), result.get("매출원가")
            if r is not None and c is not None:
                result["매출총이익"] = r - c

        return result

    except Exception as e:
        logger.error(f"[DART] {ticker} {year} 수집 실패: {e}")
        return {}


def get_all_dart_financials(tickers: list[str], year: int,
                             dry_run: bool = False) -> pd.DataFrame:
    """전종목 재무제표 배치 수집."""
    try:
        from tqdm import tqdm
        ticker_iter = tqdm(tickers, desc=f"DART {year}년 재무제표")
    except ImportError:
        ticker_iter = tickers

    rows = []
    for i, ticker in enumerate(ticker_iter):
        if dry_run:
            logger.info(f"[DryRun] DART {ticker} {year}")
            continue
        row = get_dart_financials(ticker, year)
        if row:
            rows.append(row)
        if (i + 1) % 100 == 0:
            logger.info(f"[DART] {i + 1}/{len(tickers)} 완료")

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# [4] CompanyGuide (FnGuide) — 재무비율 크롤링
# ══════════════════════════════════════════════════════════════════════════════

_CG_BASE = "http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
_CG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "http://comp.fnguide.com/",
}


def get_company_guide(ticker: str) -> dict:
    """
    CompanyGuide에서 재무비율 수집.
    Returns: {ticker, ROE, ROA, 영업이익률, 순이익률, 부채비율, EV_EBITDA}
    """
    time.sleep(random.uniform(*config.DELAY_TICKER))
    pno = f"A{ticker}"
    params = {"pGubun": "1", "pNm": "SummaryIR", "pSelSCode": pno,
              "pYear": "", "pRpt_tp": "A"}
    result = {"ticker": ticker}

    for attempt in range(config.MAX_RETRY):
        try:
            resp = requests.get(_CG_BASE, params=params, headers=_CG_HEADERS, timeout=10)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
            result.update(_parse_cg_profitability(soup))
            result.update(_parse_cg_stability(soup))
            result["EV_EBITDA"] = _get_cg_ev_ebitda(ticker)
            return result
        except requests.RequestException as e:
            logger.debug(f"[CG] {ticker} 시도 {attempt + 1} 실패: {e}")
            time.sleep(config.BACKOFF_BASE ** attempt)

    return result


def _parse_cg_profitability(soup: BeautifulSoup) -> dict:
    result = {}
    try:
        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if "ROE" not in headers and "영업이익률" not in headers:
                continue
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True)
                val   = _safe_float(cells[-1].get_text(strip=True))
                if   "ROE"    in label: result["ROE"]    = val
                elif "ROA"    in label: result["ROA"]    = val
                elif "영업이익률" in label: result["영업이익률"] = val
                elif "순이익률" in label or "당기순이익률" in label: result["순이익률"] = val
    except Exception as e:
        logger.debug(f"[CG] 수익성 파싱 오류: {e}")
    return result


def _parse_cg_stability(soup: BeautifulSoup) -> dict:
    result = {}
    try:
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                if "부채비율" in cells[0].get_text(strip=True):
                    result["부채비율"] = _safe_float(cells[-1].get_text(strip=True))
                    return result
    except Exception as e:
        logger.debug(f"[CG] 안정성 파싱 오류: {e}")
    return result


def _get_cg_ev_ebitda(ticker: str) -> Optional[float]:
    try:
        time.sleep(0.3)
        url = "http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp"
        resp = requests.get(url, params={"pGubun": "1", "pNm": "Invest",
                                          "pSelSCode": f"A{ticker}"},
                            headers=_CG_HEADERS, timeout=10)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "lxml")
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2 and "EV/EBITDA" in cells[0].get_text(strip=True):
                    return _safe_float(cells[-1].get_text(strip=True))
    except Exception as e:
        logger.debug(f"[CG] EV/EBITDA 수집 실패 {ticker}: {e}")
    return None


def get_all_company_guide(tickers: list[str], year: int,
                           dry_run: bool = False) -> pd.DataFrame:
    """전종목 CompanyGuide 배치 수집."""
    try:
        from tqdm import tqdm
        ticker_iter = tqdm(tickers, desc=f"CompanyGuide {year}년")
    except ImportError:
        ticker_iter = tickers

    rows = []
    for i, ticker in enumerate(ticker_iter):
        if dry_run:
            logger.info(f"[DryRun] CG {ticker} {year}")
            continue
        row = get_company_guide(ticker)
        row["year"] = year
        rows.append(row)
        if (i + 1) % 50 == 0:
            logger.info(f"[CG] {i + 1}/{len(tickers)} 완료")

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
