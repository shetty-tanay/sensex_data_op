"""
dte_utils.py
------------
Core DTE calculation utilities.

All functions are kept exactly as in the original implementation.
Import config constants rather than re-defining them here.
"""

from datetime import datetime, timedelta

import pandas as pd

from config import MARKET_OPEN, MARKET_CLOSE, TRADING_SECONDS, trading_holidays, special_days


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def is_trading_day(d) -> bool:
    """Return True if `d` (a date or datetime.date) is a valid NSE trading day."""
    d_str = d.strftime("%Y-%m-%d")
    return (
        d_str in special_days
        or (d.weekday() < 5 and d_str not in trading_holidays)
    )


# ─────────────────────────────────────────
# DTE FUNCTIONS
# ─────────────────────────────────────────

def get_intraday_fraction(ts) -> float:
    """
    Fraction of the trading day remaining at timestamp `ts`.

    Returns:
        1.0  — before market open (full day still ahead)
        0.0  — after market close (no time remaining today)
        0..1 — intraday proportion remaining
    """
    t = ts.time()

    if t < MARKET_OPEN:
        return 1.0
    elif t > MARKET_CLOSE:
        return 0.0
    else:
        start_dt = datetime.combine(ts.date(), t)
        close_dt = datetime.combine(ts.date(), MARKET_CLOSE)
        return (close_dt - start_dt).total_seconds() / TRADING_SECONDS


def get_dte(ts, expiry) -> float | None:
    """
    Trading-day DTE from `ts` to `expiry`.

    Returns None if `ts` falls on a non-trading day (safe for bulk use).
    """
    start_date = ts.date()
    end_date   = expiry.date()

    if not is_trading_day(start_date):
        return None  # safer than raising in bulk processing

    intraday = get_intraday_fraction(ts)

    current      = start_date
    trading_days = 0

    while current <= end_date:
        if is_trading_day(current) or current == end_date:
            trading_days += 1
        current += timedelta(days=1)

    return round(trading_days - 1 + intraday, 4)


def get_cal_dte(ts, expiry) -> float:
    """Calendar-day DTE from `ts` to `expiry`."""
    cal_days = (expiry.date() - ts.date()).days
    intraday = get_intraday_fraction(ts)
    return round(cal_days + intraday, 4)


# ─────────────────────────────────────────
# FILL MISSING TIMESTAMPS (GROUP SAFE)
# ─────────────────────────────────────────

def fill_missing_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (expiry) group, expand the timestamp series to the full
    1-minute trading grid and left-join the original data.

    Only valid trading timestamps (trading day + within market hours)
    are included in the expansion.
    """
    df = df.sort_values(["expiry", "timestamp"])

    filled = []

    for expiry, group in df.groupby("expiry"):
        group = group.sort_values("timestamp")

        full_range = pd.date_range(
            start=group["timestamp"].min(),
            end=group["timestamp"].max(),
            freq="1min",
        )

        # Keep only valid trading timestamps
        full_range = [
            ts for ts in full_range
            if (
                is_trading_day(ts.date())
                and MARKET_OPEN <= ts.time() <= MARKET_CLOSE
            )
        ]

        temp = pd.DataFrame({"timestamp": full_range})
        temp = temp.merge(group, on="timestamp", how="left")

        # Enforce expiry consistently
        temp["expiry"] = expiry

        filled.append(temp)

    return pd.concat(filled, ignore_index=True)
