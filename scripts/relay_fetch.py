#!/usr/bin/env python3
"""ISO data relay — runs on a GitHub Actions US-hosted runner where direct
ISO endpoints (ERCOT misapp servlets, portal.spp.org) are reachable.

Pulls recent ERCOT DA/RT prices, ORDC adders + reserves, ancillary-service
prices, and SPP/PJM DA prices via gridstatus; appends to per-file CSVs under
data/relay/ which the workflow commits back. Consumers read them via
raw.githubusercontent.com (see USP_RELAY_BASE in the main project).

Usage:  python scripts/relay_fetch.py [--hours 48] [--out data/relay]
"""
from __future__ import annotations

import argparse
import logging
import os

import pandas as pd

log = logging.getLogger("relay")

ERCOT_HUBS = {"HB_NORTH", "HB_SOUTH", "HB_WEST", "HB_HOUSTON"}


def _hourly_hub_mean(df: pd.DataFrame, ts_col: str, val_col: str,
                     loc_col: str | None = None,
                     hubs: set | None = None) -> pd.Series:
    if loc_col and hubs and loc_col in df.columns:
        sub = df[df[loc_col].isin(hubs)]
        if not sub.empty:
            df = sub
    s = (df.assign(ts_utc=pd.to_datetime(df[ts_col], utc=True))
           .groupby(pd.Grouper(key="ts_utc", freq="1h"))[val_col]
           .mean().dropna())
    return s


def fetch_ercot(hours: int) -> dict[str, pd.DataFrame]:
    import gridstatus

    iso = gridstatus.Ercot()
    start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    today = pd.Timestamp.now(tz="UTC").date()
    start_d = (start - pd.Timedelta(days=1)).date()  # DA posts a day ahead
    out: dict[str, pd.DataFrame] = {}

    try:
        da = iso.get_dam_spp(date=str(start_d), end=str(today))
        # Settlement Point col carries hub names; SPP is the price
        loc = next((c for c in ("Settlement Point", "Location")
                    if c in da.columns), None)
        s = _hourly_hub_mean(da, "Time", "SPP", loc, ERCOT_HUBS)
        out["ercot_da"] = s.rename("day_ahead_usd_mwh").reset_index()
    except Exception as e:  # noqa: BLE001
        log.warning("ercot dam_spp: %s", e)

    try:
        rt = iso.get_rtm_spp(date=str(start.date()), end=str(today))
        loc = next((c for c in ("Settlement Point", "Location")
                    if c in rt.columns), None)
        # keep native 15-min granularity for the spike module
        sub = rt[rt[loc].isin(ERCOT_HUBS)] if loc else rt
        sub = (sub.assign(ts_utc=pd.to_datetime(sub["Time"], utc=True))
                  .groupby(pd.Grouper(key="ts_utc", freq="15min"))
                  [["SPP"]].mean().dropna().reset_index()
                  .rename(columns={"SPP": "realtime_usd_mwh"}))
        out["ercot_rt"] = sub
    except Exception as e:  # noqa: BLE001
        log.warning("ercot rtm_spp: %s", e)

    try:
        ordc = iso.get_real_time_adders_and_reserves(
            date=str(start.date()), end=str(today))
        ts_col = next((c for c in ("Time", "SCED Timestamp",
                                   "Interval Start") if c in ordc.columns),
                      None)
        if ts_col:
            ordc["ts_utc"] = pd.to_datetime(ordc[ts_col], utc=True)
            out["ercot_ordc"] = ordc
    except Exception as e:  # noqa: BLE001
        log.warning("ercot ordc: %s", e)

    try:
        asp = iso.get_as_prices(date=str(start.date()), end=str(today))
        ts_col = next((c for c in ("Time", "Interval Start")
                       if c in asp.columns), None)
        if ts_col:
            asp["ts_utc"] = pd.to_datetime(asp[ts_col], utc=True)
            out["ercot_as"] = asp
    except Exception as e:  # noqa: BLE001
        log.warning("ercot as_prices: %s", e)

    return out


def fetch_spp_da(hours: int) -> pd.DataFrame:
    import gridstatus

    iso = gridstatus.SPP()
    start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    df = iso.get_lmp_day_ahead_hourly(date=str(start.date()),
                                    end=str(pd.Timestamp.now(tz="UTC").date()),
                                    location_type="Hub")
    s = _hourly_hub_mean(df, "Interval Start", "LMP")
    return s.rename("day_ahead_usd_mwh").reset_index()


def fetch_pjm_da(hours: int) -> pd.DataFrame:
    import gridstatus

    if not os.environ.get("PJM_API_KEY"):
        log.warning("PJM_API_KEY not set — skipping PJM")
        return pd.DataFrame()
    iso = gridstatus.PJM()
    start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    df = iso.get_lmp(date=str(start.date()),
                     end=str(pd.Timestamp.now(tz="UTC").date()),
                     market="DAY_AHEAD_HOURLY")
    lt = next((c for c in ("Location Type",) if c in df.columns), None)
    pref = {"HUB", "ZONE", "AGGREGATE", "Hub", "Zone"}
    if lt:
        sub = df[df[lt].isin(pref)]
        if not sub.empty:
            df = sub
    ts_col = next((c for c in ("Interval Start", "Time") if c in df.columns),
                  None)
    s = _hourly_hub_mean(df, ts_col, "LMP")
    return s.rename("day_ahead_usd_mwh").reset_index()


def _append_csv(path: str, new: pd.DataFrame, key: str = "ts_utc",
                keep_days: int = 90) -> None:
    if os.path.exists(path):
        old = pd.read_csv(path, parse_dates=[key])
        new = pd.concat([old, new], ignore_index=True)
    new = (new.drop_duplicates(key, keep="last").sort_values(key))
    new = new[pd.to_datetime(new[key], utc=True) >=
              pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=keep_days)]
    new.to_csv(path, index=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--out", default="data/relay")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    os.makedirs(args.out, exist_ok=True)

    for name, df in fetch_ercot(args.hours).items():
        if df.empty:
            continue
        _append_csv(f"{args.out}/{name}.csv", df)
        log.info("%s: %d rows", name, len(df))

    for name, fn in (("spp_da", fetch_spp_da), ("pjm_da", fetch_pjm_da)):
        try:
            df = fn(args.hours)
        except Exception as e:  # noqa: BLE001
            log.warning("%s: %s", name, e)
            continue
        if not df.empty:
            _append_csv(f"{args.out}/{name}.csv", df)
            log.info("%s: %d rows", name, len(df))


if __name__ == "__main__":
    main()
