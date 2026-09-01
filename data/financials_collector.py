"""
data/financials_collector.py — US 재무제표 & Crypto 시장 데이터 수집

US:
  - yfinance quarterly_income_stmt / quarterly_balance_sheet / quarterly_cashflow
    → financials_db.save_financials()
  - yfinance info (재무비율 스냅샷)
    → financials_db.save_ratios()

Crypto:
  - CoinMarketCap listing API → 시장 데이터
    → financials_db.save_ratios() (Sector='Crypto')
"""

import logging
import time
from datetime import date
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# US 분기 재무제표 수집
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_quarterly_financials(ticker: str) -> pd.DataFrame:
    """
    yfinance quarterly_income_stmt + quarterly_balance_sheet + quarterly_cashflow 수집.

    yfinance DataFrame 구조:
      - index: 항목명 (Total Revenue, Gross Profit, ...)
      - columns: 분기 날짜 (pd.Timestamp)
    → transpose 후 각 분기가 row가 됨.

    반환: PeriodDate | Year | Quarter | Revenue | GrossProfit | OperatingIncome |
          NetIncome | EBITDA | TotalAssets | TotalLiabilities | Equity |
          OperatingCashFlow | FreeCashFlow | CapEx | SnapDate | Ticker
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")

    try:
        yticker = yf.Ticker(ticker)

        # ── Income Statement ──────────────────────────────────────────────
        income_raw = yticker.quarterly_income_stmt
        # ── Balance Sheet ─────────────────────────────────────────────────
        balance_raw = yticker.quarterly_balance_sheet
        # ── Cash Flow ─────────────────────────────────────────────────────
        cashflow_raw = yticker.quarterly_cashflow

    except Exception as e:
        logger.warning(f"[FinancialsCollector] {ticker} yfinance 데이터 조회 실패: {e}")
        return pd.DataFrame()

    # 공통 함수: raw DataFrame → {date: {field: value}} 매핑
    def _df_to_dict(raw_df, field_map: dict) -> dict:
        """raw DataFrame(index=항목, columns=날짜) → {date: {field: value}}."""
        if raw_df is None or raw_df.empty:
            return {}
        result: dict = {}
        for col in raw_df.columns:
            try:
                period_date = pd.Timestamp(col).date()
            except Exception:
                continue
            row_data: dict = {}
            for yf_field, our_field in field_map.items():
                try:
                    val = raw_df.loc[yf_field, col] if yf_field in raw_df.index else None
                    if val is not None and pd.notna(val):
                        row_data[our_field] = float(val)
                    else:
                        row_data[our_field] = None
                except Exception:
                    row_data[our_field] = None
            result[period_date] = row_data
        return result

    income_map = {
        "Total Revenue":      "Revenue",
        "Gross Profit":       "GrossProfit",
        "Operating Income":   "OperatingIncome",
        "Net Income":         "NetIncome",
        "EBITDA":             "EBITDA",
        "Diluted EPS":        "DilutedEPS",   # 섹터리더 2단계 — 분기 연속 EPS 성장 판정 재료 (2026-09-01)
    }
    balance_map = {
        "Total Assets":                           "TotalAssets",
        "Total Liabilities Net Minority Interest": "TotalLiabilities",
        "Stockholders Equity":                    "Equity",
    }
    cashflow_map = {
        "Operating Cash Flow": "OperatingCashFlow",
        "Free Cash Flow":      "FreeCashFlow",
        "Capital Expenditure": "CapEx",
    }

    income_data   = _df_to_dict(income_raw, income_map)
    balance_data  = _df_to_dict(balance_raw, balance_map)
    cashflow_data = _df_to_dict(cashflow_raw, cashflow_map)

    # 모든 분기 날짜 합집합
    all_dates = set(income_data) | set(balance_data) | set(cashflow_data)
    if not all_dates:
        logger.debug(f"[FinancialsCollector] {ticker} 분기 데이터 없음")
        return pd.DataFrame()

    snap_date = date.today()
    rows = []
    for period_date in sorted(all_dates):
        row = {
            "Ticker":     ticker,
            "PeriodDate": period_date,
            "Year":       period_date.year,
            "Quarter":    (period_date.month - 1) // 3 + 1,
            "SnapDate":   snap_date,
        }
        row.update(income_data.get(period_date, {}))
        row.update(balance_data.get(period_date, {}))
        row.update(cashflow_data.get(period_date, {}))
        rows.append(row)

    df = pd.DataFrame(rows)
    logger.debug(f"[FinancialsCollector] {ticker} 분기 데이터: {len(df)}건")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# US 재무비율 스냅샷 수집
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_ratios_snapshot(ticker: str, snap_date: date) -> dict:
    """
    yfinance info에서 현재 시점 재무 비율 수집.
    키 없으면 None → 저장 시 NaN.

    퍼센트 값(ROE, ROA, ProfitMargin 등): yfinance가 소수(0.15=15%) → ×100 변환.
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance를 설치하세요: pip install yfinance")

    def _pct(val) -> Optional[float]:
        """소수 → 퍼센트(×100), None/NaN이면 None."""
        if val is None:
            return None
        try:
            f = float(val)
            if pd.isna(f):
                return None
            return f * 100.0
        except Exception:
            return None

    def _val(val) -> Optional[float]:
        """숫자 변환, 실패 시 None."""
        if val is None:
            return None
        try:
            f = float(val)
            return None if pd.isna(f) else f
        except Exception:
            return None

    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as e:
        logger.debug(f"[FinancialsCollector] {ticker} info 조회 실패: {e}")
        info = {}

    return {
        "Ticker":            ticker,
        "SnapDate":          snap_date,
        "Name":              info.get("longName"),
        "Sector":            info.get("sector"),
        "Industry":          info.get("industry"),
        "MarketCap":         _val(info.get("marketCap")),
        "SharesOutstanding": _val(info.get("sharesOutstanding")),
        "PE":                _val(info.get("trailingPE")),
        "ForwardPE":         _val(info.get("forwardPE")),
        "PB":                _val(info.get("priceToBook")),
        "PS":                _val(info.get("priceToSalesTrailing12Months")),
        "ROE":               _pct(info.get("returnOnEquity")),
        "ROA":               _pct(info.get("returnOnAssets")),
        "DebtToEquity":      _val(info.get("debtToEquity")),
        "Beta":              _val(info.get("beta")),
        # 섹터리더 2단계 — 기관 보유비율(%): 스냅샷 누적으로 증가 추세 판정 (2026-09-01)
        "HeldPctInstitutions": _pct(info.get("heldPercentInstitutions")),
        "DividendYield":     _pct(info.get("dividendYield")),
        "EPS":               _val(info.get("trailingEps")),
        "ProfitMargin":      _pct(info.get("profitMargins")),
        "OperatingMargin":   _pct(info.get("operatingMargins")),
        "RevenueGrowth":     _pct(info.get("revenueGrowth")),
        "EarningsGrowth":    _pct(info.get("earningsGrowth")),
        "CurrentRatio":      _val(info.get("currentRatio")),
    }


# ══════════════════════════════════════════════════════════════════════════════
# US 전체 수집 파이프라인
# ══════════════════════════════════════════════════════════════════════════════

def collect_us_financials(
    tickers: Optional[list[str]] = None,
    upload: bool = True,
):
    """
    US 재무제표 + 재무비율 스냅샷 수집.

    1. tickers None이면 load_tickers('us') 사용
    2. 종목별 _fetch_quarterly_financials() → financials_db.save_financials()
    3. 종목별 _fetch_ratios_snapshot() → financials_db.save_ratios()
    4. 10개 종목마다 Drive 업로드 (중간 백업)
    5. tqdm 진행 표시
    6. 에러 시 해당 종목 스킵, 계속
    7. 종목당 0.5초 sleep
    """
    from data import financials_db
    from data.ohlc_collector import load_tickers

    try:
        from tqdm import tqdm
        _tqdm = tqdm
    except ImportError:
        _tqdm = None

    if tickers is None:
        tickers = load_tickers("us")

    # Drive 기존 파일 선다운로드 — absent(최초 실행)면 정상 진행, failed 면 덮어쓰기 방지를 위해 중단
    # (이 선다운로드가 없으면 save→upload 가 이번 실행분만으로 Drive 를 덮어쓴다 — 2026-09-01 결함 수정)
    if upload:
        financials_db.ensure_drive_baseline("us")

    logger.info(f"[FinancialsCollector] US 재무 데이터 수집 시작: {len(tickers)}종목")

    snap_date = date.today()
    fin_rows:   list[pd.DataFrame] = []
    ratio_rows: list[dict] = []
    failed: list[str] = []

    ticker_iter = _tqdm(tickers, desc="US Financials", unit="ticker") if _tqdm else tickers

    for idx, ticker in enumerate(ticker_iter, start=1):
        # ── 분기 재무제표 ──────────────────────────────────────────────────
        try:
            fin_df = _fetch_quarterly_financials(ticker)
            if not fin_df.empty:
                fin_rows.append(fin_df)
        except Exception as e:
            logger.warning(f"[FinancialsCollector] {ticker} financials 실패: {e}")
            failed.append(ticker)

        # ── 재무비율 스냅샷 ───────────────────────────────────────────────
        try:
            ratio_dict = _fetch_ratios_snapshot(ticker, snap_date)
            ratio_rows.append(ratio_dict)
        except Exception as e:
            logger.warning(f"[FinancialsCollector] {ticker} ratios 실패: {e}")

        time.sleep(0.5)

        # ── 10개마다 중간 저장 & 업로드 ───────────────────────────────────
        if idx % 10 == 0:
            if fin_rows:
                combined_fin = pd.concat(fin_rows, ignore_index=True)
                financials_db.save_financials(combined_fin, "us")
                if upload:
                    years = sorted(combined_fin["PeriodDate"].apply(lambda d: d.year).unique())
                    financials_db.upload_financials("us", list(years))
                fin_rows = []

            if ratio_rows:
                ratio_df = pd.DataFrame(ratio_rows)
                financials_db.save_ratios(ratio_df, "us")
                if upload:
                    years = sorted(ratio_df["SnapDate"].apply(lambda d: d.year).unique())
                    financials_db.upload_ratios("us", list(years))
                ratio_rows = []

            logger.info(
                f"[FinancialsCollector] 진행: {idx}/{len(tickers)} "
                f"({idx / len(tickers) * 100:.1f}%)"
            )

    # ── 잔여 저장 ──────────────────────────────────────────────────────────
    if fin_rows:
        combined_fin = pd.concat(fin_rows, ignore_index=True)
        financials_db.save_financials(combined_fin, "us")
        if upload:
            years = sorted(combined_fin["PeriodDate"].apply(lambda d: d.year).unique())
            financials_db.upload_financials("us", list(years))

    if ratio_rows:
        ratio_df = pd.DataFrame(ratio_rows)
        financials_db.save_ratios(ratio_df, "us")
        if upload:
            years = sorted(ratio_df["SnapDate"].apply(lambda d: d.year).unique())
            financials_db.upload_ratios("us", list(years))

    if failed:
        logger.warning(f"[FinancialsCollector] 실패 종목 ({len(failed)}개): {failed[:20]}")

    logger.info(
        f"[FinancialsCollector] US 재무 데이터 수집 완료: "
        f"{len(tickers) - len(failed)}/{len(tickers)}종목 성공"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Crypto 시장 데이터 수집
# ══════════════════════════════════════════════════════════════════════════════

_CMC_URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"
_CMC_TOP_N = 200


def collect_crypto_ratios(
    tickers: Optional[list[str]] = None,
    upload: bool = True,
):
    """
    CoinMarketCap에서 현재 시점 crypto 시장 데이터 수집.
    → ratios 테이블에 저장 (Sector='Crypto', Industry=category 등).

    CMC listing API에서:
      symbol → Ticker (BTC-USD 형식)
      name → Name
      market_cap → MarketCap
      circulating_supply → SharesOutstanding (공급량 근사)
      quotes[0].percentChange24h → 24h 변화율 → RevenueGrowth로 저장
      quotes[0].percentChange7d → 7d 변화율 → EarningsGrowth로 저장

    yfinance fallback으로 Beta 등 일부 보완 (실패해도 ok).
    """
    import requests
    from data import financials_db

    try:
        from tqdm import tqdm
        _tqdm = tqdm
    except ImportError:
        _tqdm = None

    # Drive 기존 파일 선다운로드 — absent(최초 실행)면 정상 진행, failed 면 덮어쓰기 방지를 위해 중단
    if upload:
        financials_db.ensure_drive_baseline("crypto", kinds=("ratios",))

    snap_date = date.today()
    logger.info(f"[FinancialsCollector] Crypto ratios 수집 시작 (CMC Top {_CMC_TOP_N})")

    # ── CMC API 수집 ──────────────────────────────────────────────────────
    cmc_data: list[dict] = []
    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
        })
        resp = sess.get(
            _CMC_URL,
            params={
                "start": "1", "limit": str(_CMC_TOP_N),
                "sortBy": "market_cap", "sortType": "desc",
                "convert": "USD", "cryptoType": "all", "tagType": "all",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        cmc_data = data.get("data", {}).get("cryptoCurrencyList", [])
        if not cmc_data:
            cmc_data = data.get("data", [])
        logger.info(f"[FinancialsCollector] CMC 수집: {len(cmc_data)}종목")
    except Exception as e:
        logger.error(f"[FinancialsCollector] CMC API 호출 실패: {e}")

    if not cmc_data:
        logger.warning("[FinancialsCollector] CMC 데이터 없음 → 수집 종료")
        return

    # ── tickers 필터링 ────────────────────────────────────────────────────
    target_set: Optional[set[str]] = None
    if tickers:
        target_set = {t.upper() for t in tickers}

    # ── 행 구성 ────────────────────────────────────────────────────────────
    def _safe_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            f = float(val)
            return None if pd.isna(f) else f
        except Exception:
            return None

    rows = []
    item_iter = _tqdm(cmc_data, desc="Crypto Ratios", unit="coin") if _tqdm else cmc_data

    for item in item_iter:
        symbol = item.get("symbol", "").upper().strip()
        if not symbol:
            continue
        ticker_key = f"{symbol}-USD"
        if target_set and ticker_key not in target_set:
            continue

        # CMC quotes 파싱
        quotes = item.get("quotes", [])
        q: dict = {}
        if isinstance(quotes, list) and quotes:
            q = quotes[0]
        elif isinstance(quotes, dict):
            q = quotes.get("USD", {})

        market_cap    = _safe_float(q.get("marketCap") or item.get("market_cap"))
        change_24h    = _safe_float(q.get("percentChange24h"))
        change_7d     = _safe_float(q.get("percentChange7d"))
        circ_supply   = _safe_float(item.get("circulatingSupply") or item.get("circulating_supply"))
        price         = _safe_float(q.get("price"))

        # category → Industry
        categories = item.get("tags", []) or []
        if isinstance(categories, list) and categories:
            industry = categories[0] if isinstance(categories[0], str) else str(categories[0])
        else:
            industry = None

        row = {
            "Ticker":            ticker_key,
            "SnapDate":          snap_date,
            "Name":              item.get("name"),
            "Sector":            "Crypto",
            "Industry":          industry,
            "MarketCap":         market_cap,
            "SharesOutstanding": circ_supply,
            "PE":                None,
            "ForwardPE":         None,
            "PB":                None,
            "PS":                None,
            "ROE":               None,
            "ROA":               None,
            "DebtToEquity":      None,
            "Beta":              None,
            "DividendYield":     None,
            "EPS":               None,
            "ProfitMargin":      None,
            "OperatingMargin":   None,
            "RevenueGrowth":     change_24h,   # 24h 변화율
            "EarningsGrowth":    change_7d,    # 7d 변화율
            "CurrentRatio":      None,
        }
        rows.append(row)

    if not rows:
        logger.warning("[FinancialsCollector] Crypto ratios 행 없음 → 저장 건너뜀")
        return

    # ── yfinance fallback (Beta 등) ────────────────────────────────────────
    logger.info(f"[FinancialsCollector] yfinance fallback 시작: {len(rows)}종목")
    try:
        import yfinance as yf
        row_iter = _tqdm(rows, desc="Crypto yf fallback", unit="ticker") if _tqdm else rows
        for row in row_iter:
            try:
                info = yf.Ticker(row["Ticker"]).info or {}
                if row["Beta"] is None and info.get("beta") is not None:
                    try:
                        row["Beta"] = float(info["beta"])
                    except Exception:
                        pass
                if row["MarketCap"] is None and info.get("marketCap") is not None:
                    try:
                        row["MarketCap"] = float(info["marketCap"])
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(0.1)
    except ImportError:
        logger.debug("[FinancialsCollector] yfinance 없음 → fallback 건너뜀")
    except Exception as e:
        logger.warning(f"[FinancialsCollector] yfinance fallback 실패: {e}")

    # ── 저장 & 업로드 ─────────────────────────────────────────────────────
    ratio_df = pd.DataFrame(rows)
    financials_db.save_ratios(ratio_df, "crypto")
    if upload:
        years = sorted(ratio_df["SnapDate"].apply(lambda d: d.year).unique())
        financials_db.upload_ratios("crypto", list(years))

    logger.info(
        f"[FinancialsCollector] Crypto ratios 수집 완료: "
        f"{len(rows)}종목 저장됨"
    )
