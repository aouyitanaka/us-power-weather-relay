#!/usr/bin/env python3
"""ISO data relay — runs on a GitHub Actions US-hosted runner where direct
ISO endpoints (ERCOT misapp servlets, portal.spp.org) are reachable.

Pulls recent ERCOT DA/RT prices, ORDC adders + reserves, ancillary-service
prices, and SPP/PJM data; appends to per-file CSVs under data/relay/ which
the workflow commits back. Consumers read them via raw.githubusercontent.com
(see USP_RELAY_BASE in the main project).

Usage:  python scripts/relay_fetch.py [--hours 48] [--out data/relay]
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import zipfile

import pandas as pd
import requests

log = logging.getLogger("relay")

ERCOT_DOCLIST = ("https://www.ercot.com/misapp/servlets/IceDocListJsonWS"
                 "?reportTypeId={rtid}")
ERCOT_DOC_DL = ("https://www.ercot.com/misdownload/servlets/mirDownload"
                "?doclookupId={did}")

# ERCOT public report type ids (misapp IceDocListJsonWS)
RTID_ORDC = 13221        # NP6-323-CD  Real-Time Price Adders by SCED
RTID_DAM_SPP = 12331     # NP4-190-CD  DAM Settlement Point Prices
RTID_RTM_SPP = 13001     # NP6-905-CD  RTM Settlement Point Prices

ERCOT_HUBS = {"HB_NORTH", "HB_SOUTH", "HB_WEST", "HB_HOUSTON"}
SPP_HUBS = {"SPPNORTH_HUB", "SPPSOUTH_HUB"}


def _ercot_docs(rtid: int, max_docs: int = 6) -> list[dict]:
    """Newest-first N docs for an ERCOT misapp report type (sorted by
    PublishDate desc — the raw list order is not chronological)."""
    r = requests.get(ERCOT_DOCLIST.format(rtid=rtid), timeout=30)
    r.raise_for_status()
    docs = (r.json().get("ListDocsByRptTypeRes", {})
            .get("DocumentList", []))

    def _pub(d):
        try:
            return pd.Timestamp(d.get("Document", {}).get("PublishDate"))
        except Exception:
            return pd.Timestamp.min
    docs = sorted(docs, key=_pub, reverse=True)
    out = []
    for d in docs[:max_docs]:
        doc = d.get("Document", {})
        did = doc.get("DocID") or doc.get("DocLookupId")
        if did:
            out.append({"id": did, "name": doc.get("DocName", "")})
    log.info("rtid %s: %d docs, using newest %d", rtid, len(docs), len(out))
    return out


def _ercot_read_doc(did) -> pd.DataFrame:
    """Download one ERCOT doc (zip-wrapped csv) -> DataFrame."""
    r = requests.get(ERCOT_DOC_DL.format(did=did), timeout=60)
    r.raise_for_status()
    blob = io.BytesIO(r.content)
    if r.content[:2] == b"PK":
        with zipfile.ZipFile(blob) as z:
            name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
            return pd.read_csv(z.open(name))
    return pd.read_csv(blob)


def fetch_ercot_ordc(hours: int) -> pd.DataFrame:
    """Direct misapp fetch of NP6-323-CD (gridstatus's handler has a
    list.remove bug on the post-RTC+B doc set)."""
    frames = []
    # each doc is one SCED tick (~1 row) — fetch a slice covering ~8h
    for d in _ercot_docs(RTID_ORDC, max_docs=150):
        try:
            df = _ercot_read_doc(d["id"])
            df.columns = df.columns.str.strip()
            frames.append(df)
        except Exception as e:  # noqa: BLE001
            log.warning("ordc doc %s: %r", d["name"], e)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("ordc raw: %d rows, cols=%s", len(out), list(out.columns)[:12])
    ts_col = next((c for c in out.columns if "SCED" in c or "Time" in c), None)
    if ts_col:
        # ERCOT stamps are US/Central local time
        out["ts_utc"] = pd.to_datetime(
            out[ts_col]).dt.tz_localize("US/Central",
                                        ambiguous=True,
                                        nonexistent="shift_forward") \
            .dt.tz_convert("UTC")
    return out


def _ercot_spp(rtid: int, hubs_only: bool) -> pd.DataFrame:
    frames = []
    for d in _ercot_docs(rtid, max_docs=30):  # ~30 days of daily files
        try:
            df = _ercot_read_doc(d["id"])
            df.columns = df.columns.str.strip()
            frames.append(df)
        except Exception as e:  # noqa: BLE001
            log.warning("spp doc %s: %r", d["name"], e)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    loc = next((c for c in df.columns if "Settlement" in c or
                c in ("Location", "SettlementPoint")), None)
    px = next((c for c in df.columns if c.upper() in
               ("SPP", "LMP", "SETTLEMENTPOINTPRICE")), None)
    if loc and px and hubs_only:
        sub = df[df[loc].isin(ERCOT_HUBS)]
        if not sub.empty:
            df = sub
    return df


def fetch_ercot(hours: int) -> dict[str, pd.DataFrame]:
    import gridstatus

    iso = gridstatus.Ercot()
    start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    today = pd.Timestamp.now(tz="UTC").date()
    start_d = (start - pd.Timedelta(days=1)).date()
    out: dict[str, pd.DataFrame] = {}

    # DAM settlement-point prices (hub mean, hourly). location_type="Hub"
    # silently empties post-RTC+B (Location Type values changed) — fetch ALL
    # and filter by hub NAME instead.
    try:
        da = iso.get_spp(date=str(start_d), end=str(today),
                         market="DAY_AHEAD_HOURLY")
        ts_col = next((c for c in ("Interval Start", "Time")
                       if c in da.columns), None)
        px_col = next((c for c in ("SPP", "LMP") if c in da.columns), None)
        loc_col = next((c for c in ("Location", "Settlement Point")
                        if c in da.columns), None)
        if da.empty or ts_col is None or px_col is None:
            raise ValueError(f"empty/missing cols: {list(da.columns)[:10]}")
        if loc_col:
            sub = da[da[loc_col].isin(ERCOT_HUBS)]
            if not sub.empty:
                da = sub
        s = (da.assign(ts_utc=pd.to_datetime(da[ts_col], utc=True))
               .groupby(pd.Grouper(key="ts_utc", freq="1h"))[px_col]
               .mean().dropna())
        if s.empty:
            raise ValueError("hub-mean produced 0 rows")
        out["ercot_da"] = s.rename("day_ahead_usd_mwh").reset_index()
    except Exception as e:  # noqa: BLE001
        log.warning("ercot dam via get_spp: %s", e)
        try:  # direct misapp fallback
            df = _ercot_spp(RTID_DAM_SPP, hubs_only=True)
            if not df.empty:
                loc = next(c for c in df.columns if "Settlement" in c)
                px = next(c for c in df.columns
                          if c.upper() in ("SPP", "LMP"))
                tc = next((c for c in df.columns
                           if "Delivery" in c or "Time" in c), None)
                if tc:
                    df["ts_utc"] = pd.to_datetime(df[tc]).dt.tz_localize(
                        "US/Central", ambiguous=True,
                        nonexistent="shift_forward").dt.tz_convert("UTC")
                    out["ercot_da"] = (df.groupby(
                        pd.Grouper(key="ts_utc", freq="1h"))[px]
                        .mean().dropna()
                        .rename("day_ahead_usd_mwh").reset_index())
                    log.info("ercot_da direct: %d rows, cols=%s",
                             len(out["ercot_da"]), list(df.columns)[:10])
        except Exception as e2:  # noqa: BLE001
            log.warning("ercot dam direct: %r", e2)

    # RTM settlement-point prices (hub mean, native 15-min)
    try:
        rt = iso.get_spp(date=str(start.date()), end=str(today),
                         market="REAL_TIME_15_MIN")
        ts_col = next((c for c in ("Interval Start", "Time")
                       if c in rt.columns), None)
        px_col = next((c for c in ("SPP", "LMP") if c in rt.columns), None)
        loc_col = next((c for c in ("Location", "Settlement Point")
                        if c in rt.columns), None)
        if rt.empty or ts_col is None or px_col is None:
            raise ValueError(f"empty/missing cols: {list(rt.columns)[:10]}")
        if loc_col:
            sub = rt[rt[loc_col].isin(ERCOT_HUBS)]
            if not sub.empty:
                rt = sub
        out["ercot_rt"] = (rt.assign(
            ts_utc=pd.to_datetime(rt[ts_col], utc=True))
            .groupby(pd.Grouper(key="ts_utc", freq="15min"))[px_col]
            .mean().dropna().rename("realtime_usd_mwh").reset_index())
    except Exception as e:  # noqa: BLE001
        log.warning("ercot rtm via get_spp: %s", e)
        try:
            df = _ercot_spp(RTID_RTM_SPP, hubs_only=True)
            if not df.empty:
                px = next(c for c in df.columns
                          if c.upper() in ("SPP", "LMP"))
                tc = next((c for c in df.columns
                           if "SCED" in c or "Time" in c), None)
                if tc:
                    df["ts_utc"] = pd.to_datetime(df[tc]).dt.tz_localize(
                        "US/Central", ambiguous=True,
                        nonexistent="shift_forward").dt.tz_convert("UTC")
                    out["ercot_rt"] = (df.groupby(
                        pd.Grouper(key="ts_utc", freq="15min"))[px]
                        .mean().dropna()
                        .rename("realtime_usd_mwh").reset_index())
        except Exception as e2:  # noqa: BLE001
            log.warning("ercot rtm direct: %r", e2)

    # ORDC adders + reserves — direct misapp (gridstatus handler broken
    # post-RTC+B)
    try:
        ordc = fetch_ercot_ordc(hours)
        if not ordc.empty:
            out["ercot_ordc"] = ordc
    except Exception as e:  # noqa: BLE001
        log.warning("ercot ordc: %s", e)

    # Ancillary-service prices (gridstatus path works)
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


def fetch_spp(hours: int) -> dict[str, pd.DataFrame]:
    """SPP DA + RT-5min via gridstatus (portal.spp.org reachable from
    US runners). Post-RTC+B filename changes may 404 some reports — each
    path is tried independently."""
    import gridstatus

    iso = gridstatus.SPP()
    start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    today = pd.Timestamp.now(tz="UTC").date()
    out = {}

    # post-RTC+B SPP dropped dated By_Day/By_Interval archives — only the
    # *-latestInterval.csv snapshots + dated MCP files remain. Relay polls
    # latestInterval every 15min (cron) and accumulates ticks.

    def _dl(fs: str, path: str) -> pd.DataFrame:
        r = requests.get(
            f"https://portal.spp.org/file-browser-api/download/{fs}",
            params={"path": path}, timeout=30)
        r.raise_for_status()
        return pd.read_csv(io.BytesIO(r.content))

    # RT 5-min settlement-location LMPs → hub rows
    try:
        df = _dl("rtbm-lmp-by-location", "/RTBM-LMP-SL-latestInterval.csv")
        loc_col = next(c for c in df.columns if "Settlement" in c or
                       c == "Location")
        hubs = df[df[loc_col].astype(str).str.contains("HUB", na=False)]
        if hubs.empty:
            hubs = df[df[loc_col].isin(SPP_HUBS)]
        if hubs.empty:
            raise ValueError(f"no hub rows; sample locs: "
                             f"{df[loc_col].unique()[:15]}")
        out["spp_rt"] = (hubs.assign(
            ts_utc=pd.to_datetime(hubs["GMTIntervalEnd"], utc=True))
            .groupby("ts_utc")["LMP"].mean()
            .rename("realtime_usd_mwh").reset_index())
        log.info("spp_rt latest interval: %d hub rows", len(hubs))
    except Exception as e:  # noqa: BLE001
        log.warning("spp rt latest: %r", e)

    # RT 5-min market clearing prices (per reserve zone)
    try:
        df = _dl("rtbm-mcp", "/RTBM-MCP-latestInterval.csv")
        df["ts_utc"] = pd.to_datetime(df["GMTIntervalEnd"], utc=True)
        out["spp_mcp"] = df
    except Exception as e:  # noqa: BLE001
        log.warning("spp rtbm mcp: %r", e)

    # DA market clearing prices (posted ~01:00 CT daily)
    try:
        d = pd.Timestamp.now(tz="UTC")
        df = _dl("da-mcp", f"/{d:%Y}/{d:%m}/DA-MCP-{d:%Y%m%d}0100.csv")
        df["ts_utc"] = pd.to_datetime(df["GMTIntervalEnd"], utc=True)
        out["spp_damcp"] = df
    except Exception as e:  # noqa: BLE001
        log.warning("spp da mcp: %r", e)

    return out


def _pjm_lmp(hours: int, market: str, col: str) -> pd.DataFrame:
    import gridstatus

    if not os.environ.get("PJM_API_KEY"):
        log.warning("PJM_API_KEY not set — skipping PJM")
        return pd.DataFrame()
    iso = gridstatus.PJM()
    start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    df = iso.get_lmp(date=str(start.date()),
                     end=str(pd.Timestamp.now(tz="UTC").date()),
                     market=market)
    lt = next((c for c in ("Location Type",) if c in df.columns), None)
    if lt:
        sub = df[df[lt].isin({"HUB", "ZONE", "AGGREGATE", "Hub", "Zone"})]
        if not sub.empty:
            df = sub
    ts_col = next((c for c in ("Interval Start", "Time") if c in df.columns),
                  None)
    s = (df.assign(ts_utc=pd.to_datetime(df[ts_col], utc=True))
           .groupby(pd.Grouper(key="ts_utc", freq="1h"))["LMP"]
           .mean().dropna())
    return s.rename(col).reset_index()


def _append_csv(path: str, new: pd.DataFrame, key: str = "ts_utc",
                keep_days: int = 90) -> None:
    if os.path.exists(path):
        old = pd.read_csv(path, parse_dates=[key])
        new = pd.concat([old, new], ignore_index=True)
    new = (new.drop_duplicates(key, keep="last").sort_values(key))
    new = new[pd.to_datetime(new[key], utc=True) >=
              pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=keep_days)]
    new.to_csv(path, index=False)


def _merge_zone(out_dir: str, da_name: str | None,
                rt_name: str | None) -> pd.DataFrame | None:
    """Join accumulated <zone>_da.csv + <zone>_rt.csv into the single
    <zone>.csv consumed by usp.sources.fetch_relay (hourly, UTC)."""
    da = rt = None
    if da_name and os.path.exists(f"{out_dir}/{da_name}.csv"):
        da = pd.read_csv(f"{out_dir}/{da_name}.csv", parse_dates=["ts_utc"])
    if rt_name and os.path.exists(f"{out_dir}/{rt_name}.csv"):
        rt = pd.read_csv(f"{out_dir}/{rt_name}.csv", parse_dates=["ts_utc"])
        rt = (rt.set_index("ts_utc")
              .resample("1h")["realtime_usd_mwh"].mean().reset_index())
    if da is None and rt is None:
        return None
    if da is None:
        m = rt
    elif rt is None:
        m = da
    else:
        m = da.merge(rt, on="ts_utc", how="outer").sort_values("ts_utc")
    m["ts_utc"] = pd.to_datetime(m["ts_utc"], utc=True)
    return m


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

    for name, df in fetch_spp(args.hours).items():
        if not df.empty:
            _append_csv(f"{args.out}/{name}.csv", df)
            log.info("%s: %d rows", name, len(df))

    for name, market, col in (("pjm_da", "DAY_AHEAD_HOURLY",
                               "day_ahead_usd_mwh"),
                              ("pjm_rt", "REAL_TIME_HOURLY",
                               "realtime_usd_mwh")):
        try:
            pjm = _pjm_lmp(args.hours, market, col)
            if not pjm.empty:
                _append_csv(f"{args.out}/{name}.csv", pjm)
                log.info("%s: %d rows", name, len(pjm))
        except Exception as e:  # noqa: BLE001
            log.warning("%s: %s", name, e)

    # merged <zone>.csv = the contract fetch_relay() reads (hourly DA+RT)
    for zone, da_f, rt_f in (("ercot", "ercot_da", "ercot_rt"),
                             ("spp", None, "spp_rt"),
                             ("pjm", "pjm_da", "pjm_rt")):
        try:
            merged = _merge_zone(args.out, da_f, rt_f)
            if merged is not None and not merged.empty:
                merged.to_csv(f"{args.out}/{zone}.csv", index=False)
                log.info("%s.csv: %d rows", zone, len(merged))
        except Exception as e:  # noqa: BLE001
            log.warning("merge %s: %r", zone, e)


if __name__ == "__main__":
    main()
