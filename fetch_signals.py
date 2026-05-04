#!/usr/bin/env python3
"""
Supertails Offline Campaign — Four-Signal Live Dashboard  v3
=============================================================
Fetches DAILY data from GSC, AppsFlyer, GA4, and Meltwater.
Stores it in data_store.json (incremental — only fetches the delta each run).
Generates supertails_dashboard.html with:
  • Date-range pickers (not week dropdowns)
  • Daily / Weekly / Monthly aggregation toggle
  • t-1 freshness (yesterday's data on every run)
  • Full-year history
  • Browser auto-refresh (reloads every 5 min)

Usage:
  python3 fetch_signals.py --demo          # 365-day sample data, no API keys needed
  python3 fetch_signals.py                 # Fetch delta to t-1, regenerate dashboard
  python3 fetch_signals.py --watch         # Keep running, refresh every 60 min
  python3 fetch_signals.py --watch --interval 30   # Refresh every 30 min
  python3 fetch_signals.py --full-refresh  # Re-fetch entire history (slow, use once)
"""

import argparse, json, os, sys, io, time, random, subprocess, shutil
from datetime import datetime, timedelta, date

import requests
import pandas as pd

STORE_PATH = "data_store.json"
SCHEMA_VERSION = 3

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def yesterday():
    return (date.today() - timedelta(days=1)).isoformat()

def date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end inclusive."""
    cur = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    while cur <= end_d:
        yield cur.isoformat()
        cur += timedelta(days=1)

def load_config(path="config.json"):
    if not os.path.exists(path):
        print(f"ERROR: {path} not found. See SETUP.md.")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)

# ─────────────────────────────────────────────────────────────────────────────
# DATA STORE  (data_store.json)
# ─────────────────────────────────────────────────────────────────────────────

SIGNAL_KEYS = [
    "branded_search", "direct_installs", "total_installs", "paid_installs",
    "direct_installs_blr", "paid_installs_blr",
    # Revenue + Orders
    "revenue_india", "revenue_blr", "orders_blr", "orders_india",
    # GA4 traffic — all channels
    "total_nonpaid_sessions", "total_paid_sessions",
    # GA4 traffic — sub-breakdowns
    "direct_sessions", "direct_new_users",
    "brand_paid_sessions", "blr_paid_sessions",
    # Spend (from Unified Dashboard Google Sheet, d-1)
    "brand_spend", "perf_spend",
    # Social / Meltwater
    "brand_mentions", "hashtag_mentions", "sov_percent", "negative_rate",
    "negative_mentions",
    "competitor_huft", "competitor_wiggles", "competitor_petsutra",
]

# Keys that are dicts (not parallel arrays) — preserved separately in merge
DICT_KEYS = ["city_sessions", "city_list", "gsc_queries", "campaign_daily", "installs_city_daily"]

def empty_store():
    store = {"schema_version": SCHEMA_VERSION, "last_fetched_to": None, "dates": []}
    for k in SIGNAL_KEYS:
        store[k] = []
    return store

def load_store(path=STORE_PATH):
    if not os.path.exists(path):
        return empty_store()
    with open(path) as f:
        s = json.load(f)
    if s.get("schema_version", 0) < SCHEMA_VERSION:
        print(f"  [Store] Schema upgrade — rebuilding store.")
        return empty_store()
    return s

def save_store(store, path=STORE_PATH):
    with open(path, "w") as f:
        json.dump(store, f)

def get_fetch_range(store, config, full_refresh=False):
    """Return (start_date, end_date) to fetch. start = day after last stored; end = yesterday.
    Uses the earliest 'last real data' across key signals to catch gaps where dates exist
    in the store (e.g. from AppsFlyer) but API signals (GSC, GA4) are stale."""
    end = yesterday()
    if full_refresh or not store.get("last_fetched_to"):
        start = (date.today() - timedelta(days=365)).isoformat()
        return start, end

    # Find last non-null date for key API-fetched signals (GSC + GA4)
    api_signals = ["branded_search", "direct_sessions", "total_paid_sessions", "brand_paid_sessions"]
    dates = store.get("dates", [])
    last_api_date = None
    for sig in api_signals:
        arr = store.get(sig, [])
        for i in range(len(arr)-1, -1, -1):
            if arr[i] is not None:
                d = dates[i] if i < len(dates) else None
                if d and (last_api_date is None or d > last_api_date):
                    last_api_date = d
                break

    # Use the earlier of last_fetched_to and last real API data (+1 day)
    base = store["last_fetched_to"]
    if last_api_date and last_api_date < base:
        base = last_api_date  # refetch from where API signals actually stopped

    day_after = (date.fromisoformat(base) + timedelta(days=1)).isoformat()
    if day_after > end:
        return None, None  # Already current
    return day_after, end

def merge_into_store(store, day_data: dict):
    """
    day_data: { 'YYYY-MM-DD': { signal_key: value, ... }, ... }
    Merges new daily rows into the store, maintaining date order, no duplicates.
    Also back-fills null values on existing dates when new data arrives for them
    (e.g. AppsFlyer rows exist but GSC/GA4 data wasn't fetched yet for those dates).
    """
    existing = set(store["dates"])
    new_dates = sorted(d for d in day_data if d not in existing)
    # dates that already exist but may have null signals we can now fill
    update_dates = [d for d in day_data if d in existing]

    if not new_dates and not update_dates:
        return store

    # Keys whose values get reconciled by their source over 1-3 days.
    # Ad platforms revise spend after the fact; Meltwater backfills mentions.
    # Always overwrite these to keep the store fresh.
    _refresh_keys = {
        "brand_spend", "perf_spend",
        "brand_mentions", "hashtag_mentions", "negative_mentions",
        "sov_percent", "negative_rate",
        "competitor_huft", "competitor_wiggles", "competitor_petsutra",
    }

    if not new_dates:
        # Only updating existing dates — patch in-place without rebuilding arrays
        date_to_idx = {d: i for i, d in enumerate(store["dates"])}
        new_store = dict(store)
        for k in SIGNAL_KEYS:
            new_store[k] = list(store.get(k, [None]*len(store["dates"])))
        for d in update_dates:
            j = date_to_idx[d]
            for k, v in day_data[d].items():
                if k not in SIGNAL_KEYS or v is None:
                    continue
                cur = new_store[k][j] if j < len(new_store[k]) else None
                if cur is None or k in _refresh_keys:
                    new_store[k][j] = v
        for k in DICT_KEYS:
            if k in store:
                new_store[k] = store[k]
        return new_store

    # Build combined sorted index
    all_dates = sorted(existing | set(new_dates))
    date_to_idx = {d: i for i, d in enumerate(all_dates)}

    # Re-allocate arrays
    n = len(all_dates)
    new_store = {"schema_version": SCHEMA_VERSION, "last_fetched_to": all_dates[-1], "dates": all_dates}
    for k in SIGNAL_KEYS:
        new_store[k] = [None] * n

    # Fill existing data
    for i, d in enumerate(store["dates"]):
        j = date_to_idx[d]
        for k in SIGNAL_KEYS:
            if i < len(store.get(k, [])):
                new_store[k][j] = store[k][i]

    # Fill new data (new dates + refresh existing dates for spend/Meltwater)
    for d, signals in day_data.items():
        j = date_to_idx[d]
        for k in SIGNAL_KEYS:
            v = signals.get(k)
            if v is None:
                continue
            cur = new_store[k][j]
            # Overwrite if null OR if it's a late-reconciling key (spend, social mentions)
            if cur is None or k in _refresh_keys:
                new_store[k][j] = v

    # Preserve dict-type keys (city_sessions, city_list, gsc_queries, campaign_daily)
    for k in DICT_KEYS:
        if k in store:
            new_store[k] = store[k]

    return new_store

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 1 — BRANDED SEARCH (Google Search Console, daily)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_branded_search_daily(config, start_date, end_date):
    """Returns { 'YYYY-MM-DD': impressions, ... }

    Counts branded-search impressions per day using GSC's API-level filter
    on the query dimension. No query dimension is used in the response, so
    each call returns ONE aggregated row per day — immune to the 25k row-limit
    truncation that affects per-query response shapes.

    Coverage of the 172-term branded list is achieved with two `contains`
    filters that subsume virtually every branded term:
      filter 1: query contains 'supertails'   (covers all "supertails*" variants)
      filter 2: query contains 'super tails'  (covers space-separated variants)
    The two sets are disjoint (verified empirically: 'supertails' substring and
    'super tails' substring never co-occur in the same query string), so we sum.
    The single excluded branded term is 'danish sait pet app' (~5–50 imp/day,
    rounding error). If precision matters for that term in the future, add a
    third `contains 'danish sait'` filter.
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        gsc = config["google_search_console"]
        creds = service_account.Credentials.from_service_account_file(
            gsc["service_account_key_path"],
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        svc = build("searchconsole", "v1", credentials=creds)
        country = gsc.get("geo_country", "ind")

        def _aggregate_for(d, contains_expr):
            try:
                resp = svc.searchanalytics().query(
                    siteUrl=gsc["site_url"],
                    body={
                        "startDate": d, "endDate": d,
                        "dimensions": [],
                        "dimensionFilterGroups": [{"filters": [
                            {"dimension": "country", "operator": "equals",
                             "expression": country},
                            {"dimension": "query", "operator": "contains",
                             "expression": contains_expr},
                        ]}],
                        "rowLimit": 1,
                    }
                ).execute()
            except Exception:
                return None
            rows = resp.get("rows", [])
            return int(rows[0]["impressions"]) if rows else 0

        daily = {}
        for d in date_range(start_date, end_date):
            a = _aggregate_for(d, "supertails")
            b = _aggregate_for(d, "super tails")
            if a is None or b is None:
                continue  # day too recent / API error → leave as None
            daily[d] = a + b

        # Fill zeros only for days GSC has had time to process.
        # GSC's lag is ~2 days for high-traffic sites.
        from datetime import date as _date
        lag_cutoff = (_date.today() - timedelta(days=2)).isoformat()
        for d in date_range(start_date, end_date):
            if d <= lag_cutoff:
                daily.setdefault(d, 0)

        print(f"    ✓ GSC: {len(daily)} days fetched (aggregate filter, no truncation)")
        return daily
    except Exception as e:
        print(f"    ✗ GSC failed: {e}")
        return {}

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 2 — DIRECT APP INSTALLS (AppsFlyer, daily)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_direct_installs_daily(config, start_date, end_date):
    """Returns {
        'india':     { 'YYYY-MM-DD': count, ... },  # All-India organic installs
        'bangalore': { 'YYYY-MM-DD': count, ... },  # Bangalore-only organic installs (for delta)
    }
    'Direct' = media_source in direct_media_sources (organic / (none) / direct / "").
    City filter is NOT applied to the India signal — Bangalore delta is the offline lift measure.
    """
    try:
        af = config.get("appsflyer", {})
        api_token = af.get("api_token", "")
        if not api_token or api_token.startswith("YOUR_"):
            print("    ℹ  AppsFlyer: no API token — using manually loaded data from store")
            return {"india": {}, "bangalore": {}}

        # Support both single app_id and separate android/ios ids
        app_ids = []
        if af.get("app_id"):
            app_ids.append(af["app_id"])
        if af.get("android_app_id"):
            app_ids.append(af["android_app_id"])
        if af.get("ios_app_id"):
            app_ids.append(af["ios_app_id"])
        if not app_ids:
            print("    ✗ AppsFlyer: no app_id configured")
            return {"india": {}, "bangalore": {}}

        # Fetch from all app IDs and concatenate
        frames = []
        for app_id in app_ids:
            url = f"https://hq.appsflyer.com/export/{app_id}/installs_report/v5"
            params = {
                "api_token": api_token, "from": start_date, "to": end_date,
                "timezone": "Asia/Kolkata", "currency": "INR",
            }
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                frames.append(pd.read_csv(io.StringIO(resp.text)))
                print(f"    ✓ AppsFlyer {app_id}: fetched")
            else:
                print(f"    ✗ AppsFlyer {app_id}: HTTP {resp.status_code}")

        if not frames:
            return {"india": {}, "bangalore": {}}
        df = pd.concat(frames, ignore_index=True)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        city_col   = next((c for c in df.columns if "city" in c), None)
        source_col = next((c for c in df.columns if "media_source" in c), None)
        date_col   = next((c for c in df.columns if c in ["install_time","event_time","date"]), None)
        if not source_col or not date_col:
            print(f"    ✗ AppsFlyer: unexpected columns {list(df.columns)}")
            return {"india": {}, "bangalore": {}}

        src = [s.lower() for s in af.get("direct_media_sources", ["organic","(none)","direct",""])]

        # Total installs — ALL media sources (no filter)
        df_total = df.copy()
        df_total["_date"] = pd.to_datetime(df_total[date_col]).dt.date.astype(str)
        total_daily = df_total.groupby("_date").size().to_dict()

        # Organic installs — filter only on media_source (no city filter)
        india_mask = df[source_col].str.lower().isin(src)
        df_india   = df[india_mask].copy()
        df_india["_date"] = pd.to_datetime(df_india[date_col]).dt.date.astype(str)
        india_daily = df_india.groupby("_date").size().to_dict()

        # Bangalore subset (for city-level delta tracking)
        blr = [c.lower() for c in af.get("bangalore_city_labels", ["bangalore","bengaluru"])]
        if city_col:
            blr_mask = india_mask & df[city_col].str.lower().isin(blr)
            df_blr   = df[blr_mask].copy()
            df_blr["_date"] = pd.to_datetime(df_blr[date_col]).dt.date.astype(str)
            blr_daily = df_blr.groupby("_date").size().to_dict()
        else:
            blr_daily = {}

        for d in date_range(start_date, end_date):
            total_daily.setdefault(d, 0)
            india_daily.setdefault(d, 0)
            blr_daily.setdefault(d, 0)

        print(f"    ✓ AppsFlyer: {len(india_daily)} days — total + organic India + Bangalore subset")
        return {"total": total_daily, "india": india_daily, "bangalore": blr_daily}
    except Exception as e:
        print(f"    ✗ AppsFlyer failed: {e}")
        return {"india": {}, "bangalore": {}}

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 3 — WEB SESSIONS (GA4, daily)
# Three streams: Direct | Brand paid | BLR paid
# ─────────────────────────────────────────────────────────────────────────────

def _ga4_run_report(client, property_id, start_date, end_date, dim_filter):
    """Generic GA4 report: date × city × channel × campaign → sessions, newUsers."""
    from google.analytics.data_v1beta.types import (
        RunReportRequest, DateRange, Dimension, Metric
    )
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="date"), Dimension(name="city"),
                    Dimension(name="sessionDefaultChannelGroup"),
                    Dimension(name="sessionCampaignName")],
        metrics=[Metric(name="sessions"), Metric(name="newUsers")],
        dimension_filter=dim_filter,
        limit=100000,
    )
    response = client.run_report(req)
    daily = {}
    for row in response.rows:
        d = row.dimension_values[0].value
        d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
        s = int(row.metric_values[0].value)
        u = int(row.metric_values[1].value)
        daily.setdefault(d, {"sessions": 0, "new_users": 0})
        daily[d]["sessions"]  += s
        daily[d]["new_users"] += u
    return daily

def _city_filter(cities):
    from google.analytics.data_v1beta.types import FilterExpression, FilterExpressionList, Filter
    return FilterExpression(or_group=FilterExpressionList(expressions=[
        FilterExpression(filter=Filter(field_name="city",
            string_filter=Filter.StringFilter(value=v))) for v in cities
    ]))

def _channel_filter(channels):
    from google.analytics.data_v1beta.types import FilterExpression, FilterExpressionList, Filter
    return FilterExpression(or_group=FilterExpressionList(expressions=[
        FilterExpression(filter=Filter(field_name="sessionDefaultChannelGroup",
            string_filter=Filter.StringFilter(value=ch))) for ch in channels
    ]))

def _campaign_contains_filter(keywords):
    from google.analytics.data_v1beta.types import FilterExpression, FilterExpressionList, Filter
    return FilterExpression(or_group=FilterExpressionList(expressions=[
        FilterExpression(filter=Filter(field_name="sessionCampaignName",
            string_filter=Filter.StringFilter(
                value=kw,
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                case_sensitive=False
            ))) for kw in keywords
    ]))

def fetch_direct_web_daily(config, start_date, end_date):
    """
    Returns {
      'YYYY-MM-DD': {
        'sessions': n, 'new_users': n,
        'total_paid_sessions': n,
        'total_nonpaid_sessions': n,
        'brand_paid_sessions': n,
        'blr_paid_sessions': n
      }
    }
    """
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            FilterExpression, FilterExpressionList,
            RunReportRequest, DateRange, Dimension, Metric
        )
        import google.oauth2.service_account as sa

        ga4    = config["ga4"]
        creds  = sa.Credentials.from_service_account_file(
            ga4["service_account_key_path"],
            scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        client = BetaAnalyticsDataClient(credentials=creds)
        prop   = ga4["property_id"]
        cities = ga4.get("bangalore_city_values", ["Bangalore", "Bengaluru"])
        paid_ch= ga4.get("paid_channels", ["Paid Search", "Paid Social", "Display"])
        paid_groups = set(ga4.get("paid_channel_groups", [
            "Paid Search", "Paid Social", "Display",
            "Performance Max", "Paid Shopping", "Paid Other"
        ]))
        brand_kw = ga4.get("brand_campaign_contains", ["Brand"])
        blr_kw   = ga4.get("blr_campaign_contains", ["BLR"])

        india_mode = ga4.get("geo_mode", "india") == "india"
        city_f     = _city_filter(cities) if not india_mode else None

        def with_geo(filters):
            if india_mode or city_f is None:
                return FilterExpression(and_group=FilterExpressionList(expressions=filters))
            return FilterExpression(and_group=FilterExpressionList(expressions=filters + [city_f]))

        # ── 1. All sessions by channel group (for paid/nonpaid split) ──
        req_all = RunReportRequest(
            property=f"properties/{prop}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date"), Dimension(name="sessionDefaultChannelGroup")],
            metrics=[Metric(name="sessions"), Metric(name="newUsers")],
            limit=100000,
        )
        resp_all = client.run_report(req_all)
        all_ch = {}
        for row in resp_all.rows:
            raw_d   = row.dimension_values[0].value
            d       = f"{raw_d[:4]}-{raw_d[4:6]}-{raw_d[6:]}"
            channel = row.dimension_values[1].value
            sess    = int(row.metric_values[0].value)
            users   = int(row.metric_values[1].value)
            all_ch.setdefault(d, {})
            all_ch[d].setdefault(channel, {"sessions": 0, "new_users": 0})
            all_ch[d][channel]["sessions"]  += sess
            all_ch[d][channel]["new_users"] += users
        print(f"    ✓ GA4 All channels: {len(all_ch)} days")

        # ── 2. Brand paid sessions ──
        brand_filter = with_geo([_channel_filter(paid_ch), _campaign_contains_filter(brand_kw)])
        brand_paid   = _ga4_run_report(client, prop, start_date, end_date, brand_filter)

        # ── 3. BLR paid sessions ──
        blr_filter = with_geo([_channel_filter(paid_ch), _campaign_contains_filter(blr_kw)])
        blr_paid   = _ga4_run_report(client, prop, start_date, end_date, blr_filter)

        # ── Merge into unified daily dict ──
        all_dates = set(list(all_ch.keys()) + list(brand_paid.keys()) + list(blr_paid.keys()))
        daily = {}
        for d in all_dates:
            channels = all_ch.get(d, {})
            paid_s = nonpaid_s = direct_s = direct_u = 0
            for ch, vals in channels.items():
                s = vals["sessions"]
                if ch in paid_groups:
                    paid_s += s
                else:
                    nonpaid_s += s
                if ch == "Direct":
                    direct_s += s
                    direct_u += vals["new_users"]
            daily[d] = {
                "sessions":              direct_s,
                "new_users":             direct_u,
                "total_paid_sessions":   paid_s,
                "total_nonpaid_sessions": nonpaid_s,
                "brand_paid_sessions":   brand_paid.get(d, {}).get("sessions", 0),
                "blr_paid_sessions":     blr_paid.get(d, {}).get("sessions", 0),
            }

        print(f"    ✓ GA4 Brand Paid:   {len(brand_paid)} days")
        print(f"    ✓ GA4 BLR Paid:     {len(blr_paid)} days")
        return daily

    except Exception as e:
        print(f"    ✗ GA4 failed: {e}")
        return {}

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 4 — SOCIAL MENTIONS & SOV (Meltwater, daily)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_social_daily(config, start_date, end_date, verbose=False):
    """
    Fetches Meltwater saved searches via POST /v3/analytics/{search_id}/custom.

    Correct API (v3 docs):
      POST https://api.meltwater.com/v3/analytics/{search_id}/custom
      Headers: apikey: {key}, Content-Type: application/json
      Body:    { "start": "YYYY-MM-DDT00:00:00", "end": "YYYY-MM-DDT23:59:59",
                 "tz": "Asia/Kolkata",
                 "analysis": {"type": "date_histogram", "granularity": "day"} }
      Response: { "analysis": [ {"key": "YYYY-MM-DD...", "document_count": N}, ... ] }

    Standard accounts support up to 30 days per request; longer ranges are chunked.
    Meltwater enforces a 12-month lookback cap; start_date is clamped automatically.
    """
    # Meltwater hard limit: start must be within the last 12 months (use 364 days for safety)
    mw_earliest = (date.today() - timedelta(days=364)).isoformat()
    if start_date < mw_earliest:
        start_date = mw_earliest

    try:
        mw = config["meltwater"]
        api_key = mw.get("api_key", "")
        if not api_key or api_key.startswith("YOUR_"):
            print("    ✗ Meltwater: API key not configured")
            return {}

        base = "https://api.meltwater.com/v3"
        ids  = mw.get("search_ids", {})
        tz   = mw.get("timezone", "Asia/Kolkata")

        headers = {
            "apikey":       api_key,
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }

        def _date_chunks(s, e, chunk=28):
            """Yield (chunk_start, chunk_end) pairs ≤ chunk days wide."""
            cur = date.fromisoformat(s)
            end_d = date.fromisoformat(e)
            while cur <= end_d:
                stop = min(cur + timedelta(days=chunk - 1), end_d)
                yield cur.isoformat(), stop.isoformat()
                cur = stop + timedelta(days=1)

        def fetch_volume(search_id):
            """Return {date_str: count} for search_id over start_date→end_date."""
            if not search_id or str(search_id).startswith("SEARCH_ID"):
                return {}
            url    = f"{base}/analytics/{search_id}/custom"
            result = {}
            for chunk_start, chunk_end in _date_chunks(start_date, end_date):
                body = {
                    "start":    f"{chunk_start}T00:00:00",
                    "end":      f"{chunk_end}T23:59:59",
                    "tz":       tz,
                    "analysis": {"type": "date_histogram", "granularity": "day"},
                }
                try:
                    r = requests.post(url, headers=headers, json=body, timeout=30)
                except Exception as e:
                    print(f"    ✗ Meltwater network error ({search_id}): {e}")
                    return {}

                if r.status_code == 401:
                    print("    ✗ Meltwater 401 — API key rejected. Regenerate at Meltwater → Settings → API.")
                    return {}
                if r.status_code == 403:
                    print(f"    ✗ Meltwater 403 — Access denied for search_id {search_id}. Check account permissions.")
                    return {}
                if r.status_code == 404:
                    print(f"    ✗ Meltwater 404 — search_id {search_id} not found. Check config.json → meltwater.search_ids")
                    return {}
                if r.status_code == 422:
                    print(f"    ✗ Meltwater 422 — Invalid request for {search_id}: {r.text[:300]}")
                    return {}
                if r.status_code != 200:
                    print(f"    ✗ Meltwater {search_id}: HTTP {r.status_code} → {r.text[:400]}")
                    return {}

                try:
                    data = r.json()
                except Exception:
                    print(f"    ✗ Meltwater {search_id}: invalid JSON → {r.text[:200]}")
                    return {}

                if verbose:
                    print(f"    [raw {search_id} {chunk_start}..{chunk_end}] keys: {list(data.keys())[:8]}")

                # Response: {"result": {"document_count": N, "analysis": [{"key": "...", "document_count": N}, ...]}}
                # Also handles bare {"analysis": [...]} shape
                items = (data.get("result", {}).get("analysis")
                         or data.get("analysis")
                         or [])
                if not items and isinstance(data, list):
                    items = data
                if not items:
                    print(f"    ⚠ Meltwater {search_id} ({chunk_start}..{chunk_end}): 200 OK but 0 items. "
                          f"Top-level keys: {list(data.keys())}")

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    # "key" is an ISO timestamp; take first 10 chars for YYYY-MM-DD
                    raw_key = (item.get("key") or item.get("date") or item.get("day") or "")
                    d = str(raw_key)[:10]
                    v = (item.get("document_count") or item.get("count")
                         or item.get("volume") or item.get("total") or 0)
                    if d:
                        result[d] = result.get(d, 0) + int(v)
            return result

        brand = fetch_volume(ids.get("brand_campaign_master"))
        huft  = fetch_volume(ids.get("competitor_huft"))
        wig   = fetch_volume(ids.get("competitor_wiggles"))
        pet   = fetch_volume(ids.get("competitor_petsutra"))
        neg   = fetch_volume(ids.get("negative_sentiment"))

        daily = {}
        for d in date_range(start_date, end_date):
            b = brand.get(d, 0)
            h = huft.get(d, 0)
            w = wig.get(d, 0)
            p = pet.get(d, 0)
            n = neg.get(d, 0)
            total_sov = b + h + w + p
            daily[d] = {
                "brand_mentions":      b,
                "hashtag_mentions":    b,
                "negative_mentions":   n,
                "competitor_huft":     h,
                "competitor_wiggles":  w,
                "competitor_petsutra": p,
                "sov_percent":         round(b / total_sov * 100, 2) if total_sov > 0 else None,
                "negative_rate":       round(n / b * 100, 2)         if b > 0        else None,
            }

        fetched = sum(1 for v in brand.values() if v > 0)
        print(f"    ✓ Meltwater: {len(daily)} days fetched ({fetched} days with brand mentions)")
        return daily
    except Exception as e:
        print(f"    ✗ Meltwater failed: {e}")
        return {}


def test_meltwater(config):
    """
    Standalone Meltwater diagnostic — run with:  python3 fetch_signals.py --test-meltwater
    Tests the correct v3 endpoint: POST /v3/analytics/{search_id}/custom
    Prints full request/response so you can confirm data shape and debug issues.
    """
    import json as _json
    mw        = config.get("meltwater", {})
    api_key   = mw.get("api_key", "")
    ids       = mw.get("search_ids", {})
    master_id = ids.get("brand_campaign_master", "")
    tz        = mw.get("timezone", "Asia/Kolkata")

    print("\n══════════════════════════════════════════════")
    print("  MELTWATER API DIAGNOSTIC  (v3 correct endpoint)")
    print("══════════════════════════════════════════════")
    print(f"  API key    : {'✓ (' + api_key[:6] + '…)' if api_key and not api_key.startswith('YOUR_') else '✗ NOT SET'}")
    print(f"  search_id  : {master_id or '✗ NOT SET'}")
    print(f"  timezone   : {tz}")
    print()

    if not api_key or api_key.startswith("YOUR_"):
        print("  → Set api_key in config.json → meltwater"); return
    if not master_id or str(master_id).startswith("SEARCH_ID"):
        print("  → Set search_ids.brand_campaign_master in config.json → meltwater"); return

    base     = "https://api.meltwater.com/v3"
    today    = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    # ── Correct v3 endpoint ───────────────────────────────────────────────────
    url  = f"{base}/analytics/{master_id}/custom"
    body = {
        "start":    f"{week_ago}T00:00:00",
        "end":      f"{today}T23:59:59",
        "tz":       tz,
        "analysis": {"type": "date_histogram", "granularity": "day"},
    }
    hdrs = {"apikey": api_key, "Content-Type": "application/json", "Accept": "application/json"}

    print(f"  POST {url}")
    print(f"  Body: {_json.dumps(body, indent=4)}")
    print()

    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=20)
        print(f"  HTTP {r.status_code}")
        if r.status_code == 200:
            try:
                data = r.json()
                result_block = data.get("result", {})
                items = (result_block.get("analysis")
                         or data.get("analysis")
                         or [])
                total_docs = result_block.get("document_count") or data.get("document_count") or "?"
                print(f"  ✅ SUCCESS — top-level keys: {list(data.keys())}")
                print(f"  Total mentions in period: {total_docs}")
                print(f"  Daily buckets returned  : {len(items)}")
                if items:
                    print(f"  Sample (first 3 days):")
                    for row in items[:3]:
                        print(f"    {_json.dumps(row)}")
                print()
                print("  ✅ Meltwater is working. Run  python3 fetch_signals.py  to backfill brand_mentions.")
                # Also list other search IDs
                unconfigured = [k for k,v in ids.items() if str(v).startswith("SEARCH_ID")]
                if unconfigured:
                    print(f"\n  ⚠ These search_ids are still placeholders (SOV won't work until set):")
                    for k in unconfigured:
                        print(f"    config.json → meltwater.search_ids.{k}")
            except Exception:
                print(f"  Response (not JSON): {r.text[:400]}")
        elif r.status_code == 401:
            print("  ✗ 401 Unauthorized — API key rejected.")
            print("    → Regenerate at: Meltwater app → Settings → API Access")
            print(f"    Response: {r.text[:300]}")
        elif r.status_code == 403:
            print("  ✗ 403 Forbidden — account may not have API access enabled.")
            print(f"    Response: {r.text[:300]}")
        elif r.status_code == 404:
            print(f"  ✗ 404 Not Found — search_id {master_id} doesn't exist.")
            print("    → In Meltwater, open your saved search. The ID is in the URL.")
            print(f"    Response: {r.text[:300]}")
        elif r.status_code == 422:
            print(f"  ✗ 422 Unprocessable — request body rejected.")
            print(f"    Response: {r.text[:400]}")
        else:
            print(f"  Response: {r.text[:400]}")
    except Exception as e:
        print(f"  ✗ Connection error: {e}")
        print("    Check your internet connection and try again.")

    print("══════════════════════════════════════════════\n")

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 1b — TOP BRANDED QUERIES (GSC, snapshot for multiple windows)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_top_queries_gsc(config, windows=None):
    """
    Fetches top branded queries for multiple date windows and stores them
    in data_store.json under store['gsc_queries'][window_label].
    windows: list of (label, start_date, end_date)
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        gsc = config["google_search_console"]
        creds = service_account.Credentials.from_service_account_file(
            gsc["service_account_key_path"],
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        svc = build("searchconsole", "v1", credentials=creds)
        branded = [q.lower() for q in gsc["branded_queries"]]
        geo = gsc.get("geo_country", "ind")

        today = date.today()
        yest = (today - timedelta(days=1)).isoformat()

        if windows is None:
            windows = [
                ("last_7_days",  (today - timedelta(days=7)).isoformat(),  yest),
                ("last_30_days", (today - timedelta(days=30)).isoformat(), yest),
                ("last_90_days", (today - timedelta(days=90)).isoformat(), yest),
            ]

        result = {"fetched_at": yest}
        for label, start, end in windows:
            resp = svc.searchanalytics().query(
                siteUrl=gsc["site_url"],
                body={
                    "startDate": start, "endDate": end,
                    "dimensions": ["query"],
                    "dimensionFilterGroups": [{"filters": [{
                        "dimension": "country", "operator": "equals",
                        "expression": geo
                    }]}],
                    "rowLimit": 1000,
                }
            ).execute()

            rows = []
            for row in resp.get("rows", []):
                q = row["keys"][0]
                if any(b in q.lower() for b in branded):
                    rows.append({
                        "query":        q,
                        "impressions":  int(row.get("impressions", 0)),
                        "clicks":       int(row.get("clicks", 0)),
                        "ctr_pct":      round(row.get("ctr", 0) * 100, 2),
                        "avg_position": round(row.get("position", 0), 1),
                    })
            rows.sort(key=lambda x: x["impressions"], reverse=True)
            result[label] = {
                "period": f"{start} to {end}",
                "total_impressions": sum(r["impressions"] for r in rows),
                "unique_queries": len(rows),
                "queries": rows,
            }
            print(f"    ✓ GSC queries ({label}): {len(rows)} branded queries")

        return result
    except Exception as e:
        print(f"    ✗ GSC query fetch failed: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 2b — INSTALLS FROM GOOGLE SHEET (replaces manual CSV when configured)
# Long format: date × pincode × city × media_source × platform × installs.
# Rolls up pincodes → cities, classifies media_source → organic/paid.
# ─────────────────────────────────────────────────────────────────────────────

def fetch_installs_from_sheet(config):
    """
    Returns {
        'india':     { 'YYYY-MM-DD': {'organic': n, 'paid': n, 'total': n}, ... },
        'bangalore': { 'YYYY-MM-DD': {'organic': n, 'paid': n, 'total': n}, ... },
        'cities':    { 'YYYY-MM-DD': { 'Bangalore': {'organic': n, 'paid': n, 'total': n}, ... }, ... }
    }
    """
    import re as _re

    af  = config.get("appsflyer", {})
    cfg = af.get("sheet", {})
    if not cfg.get("enabled"):
        return None
    sheet_id = cfg.get("sheet_id", "")
    if not sheet_id or sheet_id.startswith("PASTE_") or sheet_id.startswith("YOUR_"):
        print("    ℹ  Installs sheet: no sheet_id configured — skipping (CSV fallback active)")
        return None

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        key_path = (config.get("google_sheets", {}) or {}).get("service_account_key_path") or \
                   config.get("google_search_console", {}).get("service_account_key_path")
        if not key_path:
            print("    ✗ Installs sheet: no service_account_key_path")
            return None

        creds = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        svc = build("sheets", "v4", credentials=creds)

        tab_name  = cfg.get("tab_name", "Installs_Raw")
        cols      = cfg.get("columns", {})
        date_c    = int(cols.get("date", 0))
        pin_c     = int(cols.get("pincode", 1))
        city_c    = int(cols.get("city", 2))
        src_c     = int(cols.get("media_source", 4))
        installs_c = int(cols.get("installs", 6))
        max_c = max(date_c, pin_c, city_c, src_c, installs_c)

        organic_set = set(s.lower() for s in cfg.get("organic_sources", ["organic","(none)","direct",""]))
        tracked     = set(cfg.get("tracked_cities", []))
        aliases     = {k.lower(): v for k, v in (cfg.get("city_aliases") or {}).items()}
        pin_map     = cfg.get("pincode_prefix_to_city", {})

        end_col_letter = ""
        n = max_c
        while True:
            n, r = divmod(n, 26)
            end_col_letter = chr(65 + r) + end_col_letter
            if n == 0: break
            n -= 1
        range_name = f"{tab_name}!A:{end_col_letter}"

        print(f"    → Installs sheet: {sheet_id[:20]}... | Tab: '{tab_name}' | Range: {range_name}")
        result = svc.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=range_name,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        rows = result.get("values", [])
        if not rows:
            print("    ✗ Installs sheet: empty")
            return None

        def parse_date(v):
            if v is None or v == "": return None
            sv = str(v).strip()
            if _re.match(r'^\d{4}-\d{2}-\d{2}$', sv):
                return sv
            m = _re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', sv)
            if m:
                return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
            try:
                serial = float(sv)
                if 40000 < serial < 60000:
                    return (date(1899, 12, 30) + timedelta(days=int(serial))).isoformat()
            except (ValueError, TypeError):
                pass
            return None

        def parse_int(v):
            if v is None or v == "": return 0
            try:
                return int(float(str(v).replace(",", "").strip()))
            except (ValueError, TypeError):
                return 0

        def resolve_city(pincode_raw, city_raw):
            # 1. Try city column first — exact match or alias to a tracked city
            city_resolved = None
            if city_raw:
                c = str(city_raw).strip()
                if c:
                    canonical = aliases.get(c.lower(), c)
                    if canonical in tracked:
                        return canonical
                    city_resolved = canonical  # remember it; may downgrade to Other later
            # 2. Pincode prefix takes precedence when city is unrecognized — this
            #    handles raw event data with hyper-granular city names like
            #    "Connaught Place" / "Darya Ganj" that all roll up to Delhi.
            if pincode_raw not in (None, ""):
                pin = str(pincode_raw).strip().split(".")[0]
                if len(pin) >= 3:
                    mapped = pin_map.get(pin[:3])
                    if mapped and mapped in tracked:
                        return mapped
            # 3. Fall back to whatever the city column gave us (or Unknown)
            return "Other" if city_resolved else "Unknown"

        india  = {}   # date → {organic, paid, total}
        cities = {}   # date → {city → {organic, paid, total}}
        skipped = 0
        bad_dates = 0

        for row in rows:
            row = list(row) + [''] * (max_c + 1 - len(row))
            d = parse_date(row[date_c])
            if not d:
                if row[date_c] not in (None, ''):
                    bad_dates += 1
                else:
                    skipped += 1
                continue
            installs = parse_int(row[installs_c])
            if installs <= 0:
                continue
            src = str(row[src_c] or "").strip().lower()
            bucket = "organic" if src in organic_set else "paid"
            city = resolve_city(row[pin_c], row[city_c])

            d_india = india.setdefault(d, {"organic": 0, "paid": 0, "total": 0})
            d_india[bucket] += installs
            d_india["total"] += installs

            d_cities = cities.setdefault(d, {})
            c_buck = d_cities.setdefault(city, {"organic": 0, "paid": 0, "total": 0})
            c_buck[bucket] += installs
            c_buck["total"] += installs

        # Bangalore convenience subset
        blr = {d: cities[d].get("Bangalore", {"organic": 0, "paid": 0, "total": 0})
               for d in cities}

        days = len(india)
        org_total  = sum(v["organic"] for v in india.values())
        paid_total = sum(v["paid"]    for v in india.values())
        if bad_dates:
            print(f"    ⚠  Installs sheet: {bad_dates} rows with unparseable dates skipped")
        print(f"    ✓ Installs sheet: {days} days — organic={org_total:,}, paid={paid_total:,}, "
              f"cities tracked={len(tracked)}")
        return {"india": india, "bangalore": blr, "cities": cities}
    except Exception as e:
        print(f"    ✗ Installs sheet failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 5 — SPEND (Google Sheet: Unified Dashboard - Supertails, d-1)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_spend_daily(config):
    """
    Fetches brand spend and performance spend from the spend Google Sheet.

    Column layout (0-indexed):
      col A (0) = Date
      col C (2) = Campaign name — rows containing 'BrandMar' → brand_spend; all others → perf_spend
      col E (4) = Spend amount

    Groups by date, sums spend per bucket. Returns:
      { 'YYYY-MM-DD': {'brand_spend': int, 'perf_spend': int}, ... }
    """
    import re

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        gs_cfg   = config.get("google_sheets", {})
        key_path = gs_cfg.get("service_account_key_path") or \
                   config.get("google_search_console", {}).get("service_account_key_path")
        sheet_id = gs_cfg.get("unified_dashboard_sheet_id")
        tab_name = gs_cfg.get("spend_tab_name", "")

        date_col     = int(gs_cfg.get("date_col",     0))   # A
        campaign_col = int(gs_cfg.get("campaign_col", 2))   # C
        spend_col    = int(gs_cfg.get("spend_col",    4))   # E
        brand_kw     = gs_cfg.get("brand_campaign_contains", "BrandMar")

        if not key_path or str(key_path).startswith("YOUR_"):
            print("    ℹ  Spend: no service account key — skipping")
            return {}
        if not sheet_id:
            print("    ℹ  Spend: no sheet ID configured — skipping")
            return {}

        creds = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        svc = build("sheets", "v4", credentials=creds)

        max_col = max(date_col, campaign_col, spend_col)
        def idx_to_col(n):
            s = ""
            n += 1
            while n:
                n, r = divmod(n - 1, 26)
                s = chr(65 + r) + s
            return s

        # Always fetch A:G only — columns beyond G are not needed
        range_name = f"{tab_name}!A:G" if tab_name else "A:G"

        print(f"    → Sheet: {sheet_id[:20]}... | Tab: '{tab_name}' | Range: {range_name}")
        result = svc.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()

        rows = result.get("values", [])
        if not rows:
            print(f"    ✗ Spend: sheet returned no data")
            return {}

        def parse_num(v):
            if v is None or v == "": return None
            try:
                return float(str(v).replace(",", "").strip())
            except (ValueError, TypeError):
                return None

        def parse_date(v):
            if v is None or v == "": return None
            sv = str(v).strip()
            # YYYY-MM-DD
            if re.match(r'^\d{4}-\d{2}-\d{2}$', sv):
                return sv
            # DD/MM/YYYY
            m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', sv)
            if m:
                return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
            # Excel serial
            try:
                serial = float(sv)
                if 40000 < serial < 60000:
                    from datetime import date as _d, timedelta as _td
                    return (_d(1899, 12, 30) + _td(days=int(serial))).isoformat()
            except ValueError:
                pass
            return None

        daily = {}
        skipped = 0
        date_parse_failures = 0

        for row in rows:
            # Extend short rows with empty strings so index access is safe
            row = list(row) + [''] * (max(campaign_col, spend_col) + 1 - len(row))

            raw_date = row[date_col]

            # Skip rows where column A is blank — no forward-fill
            if raw_date == '' or raw_date is None:
                skipped += 1
                continue

            iso_date = parse_date(raw_date)
            if not iso_date:
                # Non-date in col A (header, label, total row) — skip silently
                date_parse_failures += 1
                continue

            campaign = str(row[campaign_col]).strip()
            spend    = parse_num(row[spend_col])

            # Skip zero/null spend or header-like campaign names
            if spend is None or spend == 0:
                skipped += 1
                continue
            if campaign.lower() in ('campaign', 'campaignname', 'campaign name', 'bucket', ''):
                skipped += 1
                continue

            if iso_date not in daily:
                daily[iso_date] = {"brand_spend": 0.0, "perf_spend": 0.0, "campaigns": {}}

            camp_key = campaign[:80]  # truncate very long names
            if brand_kw.lower() in campaign.lower():
                daily[iso_date]["brand_spend"] += spend
            else:
                daily[iso_date]["perf_spend"]  += spend
            # Track per-campaign totals
            daily[iso_date]["campaigns"][camp_key] = daily[iso_date]["campaigns"].get(camp_key, 0.0) + spend

        # Convert floats to ints
        for d in daily:
            daily[d]["brand_spend"] = int(daily[d]["brand_spend"])
            daily[d]["perf_spend"]  = int(daily[d]["perf_spend"])
            daily[d]["campaigns"]   = {k: int(v) for k, v in daily[d]["campaigns"].items()}

        print(f"    ✓ Spend: {len(daily)} days fetched (skipped {skipped} rows, {date_parse_failures} unparseable dates)")
        if daily:
            ds = sorted(daily)
            print(f"    ✓ Spend date range: {ds[0]} → {ds[-1]}")
            sample = sorted(daily.items())[-1]
            print(f"    ✓ Latest day: {sample[0]} → brand ₹{sample[1]['brand_spend']:,}  perf ₹{sample[1]['perf_spend']:,}")
        else:
            print(f"    ✗ Spend: 0 days fetched — check tab name, column positions, and date format")
        return daily

    except Exception as e:
        print(f"    ✗ Spend: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# FETCH ALL SIGNALS → merge into store
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_merge(config, store, start_date, end_date):
    print(f"\n  Fetching {start_date} → {end_date}...\n")

    print(f"  [1/4] Branded Search (GSC)")
    gsc_daily = fetch_branded_search_daily(config, start_date, end_date)
    print(f"  [1b]  Top Branded Queries (GSC)")
    gsc_queries = fetch_top_queries_gsc(config)

    print(f"  [2/4] Direct App Installs (AppsFlyer)")
    sheet_installs = fetch_installs_from_sheet(config)
    if sheet_installs is not None:
        # Sheet is the source of truth when configured
        india_breakdown    = sheet_installs["india"]
        blr_breakdown      = sheet_installs["bangalore"]
        installs_city_daily = sheet_installs["cities"]
        af_total_daily   = {d: v["total"]   for d, v in india_breakdown.items()}
        af_india_daily   = {d: v["organic"] for d, v in india_breakdown.items()}
        af_paid_daily    = {d: v["paid"]    for d, v in india_breakdown.items()}
        af_blr_daily     = {d: v["organic"] for d, v in blr_breakdown.items()}
        af_blr_paid_daily = {d: v["paid"]   for d, v in blr_breakdown.items()}
    else:
        af_result      = fetch_direct_installs_daily(config, start_date, end_date)
        af_total_daily = af_result.get("total", {})
        af_india_daily = af_result.get("india", {})
        af_blr_daily   = af_result.get("bangalore", {})
        af_paid_daily  = {}
        af_blr_paid_daily = {}
        installs_city_daily = {}

    print(f"  [3/4] Direct Web Traffic (GA4)")
    ga_daily  = fetch_direct_web_daily(config, start_date, end_date)

    print(f"  [4/4] Social Mentions (Meltwater)")
    mw_daily  = fetch_social_daily(config, start_date, end_date)

    print(f"  [5/5] Spend (Unified Dashboard Google Sheet)")
    spend_daily = fetch_spend_daily(config)

    # Merge all into one day_data dict
    all_dates = sorted(set(list(gsc_daily) + list(af_india_daily) + list(ga_daily) + list(mw_daily) + list(spend_daily)))
    day_data = {}
    for d in all_dates:
        web   = ga_daily.get(d, {})
        soc   = mw_daily.get(d, {})
        spend = spend_daily.get(d, {})
        day_data[d] = {
            "branded_search":        gsc_daily.get(d),
            "total_installs":        af_total_daily.get(d),
            "direct_installs":       af_india_daily.get(d),
            "paid_installs":         af_paid_daily.get(d),
            "direct_installs_blr":   af_blr_daily.get(d),
            "paid_installs_blr":     af_blr_paid_daily.get(d),
            "direct_sessions":         web.get("sessions"),
            "direct_new_users":        web.get("new_users"),
            "total_paid_sessions":     web.get("total_paid_sessions"),
            "total_nonpaid_sessions":  web.get("total_nonpaid_sessions"),
            "brand_paid_sessions":     web.get("brand_paid_sessions"),
            "blr_paid_sessions":       web.get("blr_paid_sessions"),
            "brand_spend":           spend.get("brand_spend"),
            "perf_spend":            spend.get("perf_spend"),
            "brand_mentions":        soc.get("brand_mentions"),
            "hashtag_mentions":      soc.get("hashtag_mentions"),
            "negative_mentions":     soc.get("negative_mentions"),
            "sov_percent":           soc.get("sov_percent"),
            "negative_rate":         soc.get("negative_rate"),
            "competitor_huft":       soc.get("competitor_huft"),
            "competitor_wiggles":    soc.get("competitor_wiggles"),
            "competitor_petsutra":   soc.get("competitor_petsutra"),
        }

    merged = merge_into_store(store, day_data)
    if gsc_queries:
        merged["gsc_queries"] = gsc_queries
    # Merge per-campaign spend (dict-key: date → {campaign: spend})
    existing_campaign_daily = merged.get("campaign_daily", {})
    for d, v in spend_daily.items():
        camps = v.get("campaigns", {})
        if camps:
            if d not in existing_campaign_daily:
                existing_campaign_daily[d] = camps
            else:
                for camp, spend_val in camps.items():
                    existing_campaign_daily[d][camp] = spend_val  # overwrite with latest fetch
    merged["campaign_daily"] = existing_campaign_daily

    # Merge per-city installs breakdown (dict-key: date → {city: {organic, paid, total}})
    if installs_city_daily:
        existing_city_installs = merged.get("installs_city_daily", {})
        for d, by_city in installs_city_daily.items():
            existing_city_installs[d] = by_city  # sheet is source of truth — overwrite
        merged["installs_city_daily"] = existing_city_installs

    return merged

# ─────────────────────────────────────────────────────────────────────────────
# DEMO DATA — 365 days, daily granularity
# ─────────────────────────────────────────────────────────────────────────────

def generate_demo_store():
    """Generate a realistic 365-day daily data store. Campaign starts 60 days ago."""
    random.seed(42)
    store = empty_store()
    today = date.today()
    start = today - timedelta(days=364)
    campaign_start = today - timedelta(days=60)

    # Baseline daily averages
    B = dict(branded_search=1292, direct_installs=707, total_installs=2100,
             direct_sessions=1303, direct_new_users=700,
             total_nonpaid_sessions=20230, total_paid_sessions=4374,
             brand_paid_sessions=1800,
             revenue_india=5687130,
             brand_mentions=0, hashtag_mentions=0,
             competitor_huft=58, competitor_wiggles=26, competitor_petsutra=17)

    def noise(v, pct=0.12):
        return max(0, int(v * (1 + random.uniform(-pct, pct))))

    def lift(base, days_since_campaign, max_pct):
        if days_since_campaign < 0: return base
        ramp = min(1.0, days_since_campaign / 21)  # ramps over 3 weeks
        return base * (1 + max_pct * ramp)

    day_data = {}
    cur = start
    while cur <= today - timedelta(days=1):  # t-1
        dsc = (cur - campaign_start).days  # days since campaign start
        d = cur.isoformat()

        # Weekend effect — search/install slightly lower on weekends
        wd = cur.weekday()
        wknd = 0.85 if wd >= 5 else 1.0

        bm = noise(lift(B["brand_mentions"],   dsc, 0.55), 0.15)
        hu = noise(B["competitor_huft"],        0.12)
        wi = noise(B["competitor_wiggles"],     0.12)
        pe = noise(B["competitor_petsutra"],    0.12)
        total = bm + hu + wi + pe

        day_data[d] = {
            "branded_search":           noise(int(lift(B["branded_search"],          dsc, 0.40) * wknd), 0.12),
            "direct_installs":          noise(int(lift(B["direct_installs"],         dsc, 0.32) * wknd), 0.12),
            "total_installs":           noise(int(lift(B["total_installs"],          dsc, 0.20) * wknd), 0.10),
            "total_nonpaid_sessions":   noise(int(lift(B["total_nonpaid_sessions"],  dsc, 0.35) * wknd), 0.10),
            "total_paid_sessions":      noise(int(lift(B["total_paid_sessions"],     dsc, 0.15) * wknd), 0.12),
            "brand_paid_sessions":      noise(int(lift(B["brand_paid_sessions"],     dsc, 0.20) * wknd), 0.12),
            "direct_sessions":          noise(int(lift(B["direct_sessions"],         dsc, 0.35) * wknd), 0.12),
            "direct_new_users":         noise(int(lift(B["direct_new_users"],        dsc, 0.42) * wknd), 0.12),
            "revenue_india":            noise(int(lift(B["revenue_india"],           dsc, 0.25) * wknd), 0.08),
            "brand_mentions":           None,
            "hashtag_mentions":         None,
            "negative_mentions":        None,
            "sov_percent":              None,
            "negative_rate":            None,
            "competitor_huft":          hu,
            "competitor_wiggles":       wi,
            "competitor_petsutra":      pe,
        }
        cur += timedelta(days=1)

    store = merge_into_store(store, day_data)
    store["_demo_campaign_start"] = campaign_start.isoformat()
    return store

# ─────────────────────────────────────────────────────────────────────────────
# HTML DASHBOARD  v3
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Supertails Brand Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Nunito+Sans:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js" onerror="window._chartMissing=true;window.Chart=function(ctx,cfg){this.destroy=()=>{};this.data=cfg.data||{};ctx.canvas&&(ctx.canvas.parentElement.innerHTML='<div style=\\'padding:18px;color:#9CA3AF;font-size:12px;text-align:center;\\'>Charts require an internet connection</div>');};"></script>
<style>
:root {
  /* Brand palette — Supertails Brand Guidelines 2026 */
  --brand-green:#19be05;        /* CTA Fill / primary positive */
  --brand-green-stroke:#75b52f; /* CTA Stroke / secondary line */
  --brand-orange:#ff6914;       /* Accent / paid / attention */
  --brand-orange-stroke:#ca5310;/* Alert / negative delta */
  --ink:#0a0a0a;                /* Black for headers */
  --paper:#f5f8fa;              /* Off-white page bg */
  --white:#ffffff;
  /* Legacy aliases — kept so existing class references still resolve */
  --orange:var(--brand-orange);
  --navy:var(--ink);
  --navy2:#1f2937;
  --navy-light:#4b5563;
  --bg:var(--paper);
  --card:var(--white);
  --text:var(--ink);
  --muted:#6b7280;
  --green:var(--brand-green);
  --green-bg:#e7f8e3;
  --yellow:#d97706;
  --yellow-bg:#fef3c7;
  --red:var(--brand-orange-stroke);
  --red-bg:#fee2e2;
  --grey:#9ca3af;
  --grey-bg:#f3f4f6;
  --border:#e5e7eb;
  --r:14px;
}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'Nunito Sans',-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
     background:var(--bg);color:var(--text);font-size:14px;font-weight:500;
     -webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;}

/* HEADER — light, brand-aligned */
.hdr{background:var(--white);color:var(--ink);padding:18px 28px;
     display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;
     border-bottom:1px solid var(--border);}
.hdr-left{display:flex;align-items:center;gap:16px;}
.logo{font-size:18px;font-weight:900;letter-spacing:.5px;color:var(--brand-green);}
.hdr-title{font-size:15px;font-weight:700;color:var(--ink);}
.hdr-sub{font-size:11px;color:var(--muted);margin-top:2px;font-weight:500;}
.hdr-right{display:flex;align-items:center;gap:12px;flex-wrap:wrap;}
.signal-status{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.sig-pill{display:flex;align-items:center;gap:5px;padding:5px 11px;border-radius:20px;
          font-size:10px;font-weight:700;letter-spacing:.3px;white-space:nowrap;}
.sig-pill.live{background:rgba(25,190,5,.12);color:var(--brand-green-stroke);}
.sig-pill.wait{background:rgba(217,119,6,.12);color:#b45309;}
.sig-pill.off{background:var(--grey-bg);color:var(--muted);}
.sig-pill .dot{width:6px;height:6px;border-radius:50%;flex-shrink:0;}
.sig-pill.live .dot{background:var(--brand-green);}
.sig-pill.wait .dot{background:#d97706;}
.sig-pill.off .dot{background:var(--grey);}
.freshness{font-size:11px;color:var(--muted);font-weight:600;}
.freshness b{color:var(--ink);font-weight:800;}
.refresh-ctrl{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--muted);font-weight:600;}
.refresh-toggle{width:32px;height:16px;background:var(--grey-bg);border-radius:8px;
                position:relative;cursor:pointer;border:1px solid var(--border);transition:background .2s;}
.refresh-toggle.on{background:var(--brand-green);border-color:var(--brand-green);}
.refresh-toggle::after{content:'';width:12px;height:12px;background:#fff;border-radius:50%;
                       position:absolute;top:1px;left:2px;transition:left .2s;
                       box-shadow:0 1px 2px rgba(0,0,0,.2);}
.refresh-toggle.on::after{left:17px;}

/* CONTROLS BAR — clean light strip */
.controls{background:var(--white);padding:14px 28px;
          display:flex;align-items:center;gap:16px;flex-wrap:wrap;border-bottom:1px solid var(--border);}
.ctrl-label{font-size:11px;font-weight:800;letter-spacing:.8px;text-transform:uppercase;
            color:var(--muted);white-space:nowrap;}
.date-input{background:var(--paper);border:1px solid var(--border);
            color:var(--ink);border-radius:8px;padding:6px 10px;font-size:12px;
            font-family:inherit;font-weight:600;outline:none;cursor:pointer;
            transition:border-color .15s;}
.date-input:hover{border-color:var(--brand-green);}
.date-input:focus{border-color:var(--brand-green);box-shadow:0 0 0 3px rgba(25,190,5,.12);}
.date-input::-webkit-calendar-picker-indicator{opacity:.6;cursor:pointer;}
.ctrl-arrow{color:var(--muted);font-size:13px;}
.ctrl-divider{width:1px;height:24px;background:var(--border);}
.gran-btns{display:flex;gap:2px;background:var(--grey-bg);padding:3px;border-radius:8px;}
.gran-btn{background:transparent;border:none;
          color:var(--muted);border-radius:5px;padding:5px 12px;
          font-size:11px;font-weight:700;cursor:pointer;transition:all .15s;font-family:inherit;}
.gran-btn:hover{color:var(--ink);}
.gran-btn.active{background:var(--white);color:var(--brand-green);
                 box-shadow:0 1px 3px rgba(0,0,0,.08);}
.apply-btn{background:var(--brand-green);color:#fff;border:none;border-radius:8px;
           padding:7px 16px;font-size:12px;font-weight:800;cursor:pointer;
           transition:all .15s;white-space:nowrap;font-family:inherit;
           box-shadow:0 1px 3px rgba(25,190,5,.3);}
.apply-btn:hover{background:var(--brand-green-stroke);transform:translateY(-1px);
                 box-shadow:0 2px 6px rgba(25,190,5,.4);}

/* COMPARE PANEL — soft cream */
.cmp-panel{background:var(--paper);padding:11px 28px;
           display:flex;align-items:center;gap:14px;flex-wrap:wrap;
           border-bottom:1px solid var(--border);}
.period-tag{font-size:10px;font-weight:800;padding:3px 9px;border-radius:5px;white-space:nowrap;
            letter-spacing:.4px;}
.tag-a{background:var(--brand-green);color:#fff;}
.tag-b{background:var(--white);color:var(--ink);border:1px solid var(--border);}
.cmp-reset{background:var(--white);color:var(--muted);border:1px solid var(--border);
           border-radius:8px;padding:6px 12px;font-size:11px;font-weight:700;
           cursor:pointer;transition:all .15s;font-family:inherit;}
.cmp-reset:hover{color:var(--ink);border-color:var(--ink);}
.cmp-active{background:rgba(25,190,5,.12);border:1px solid var(--brand-green);
            color:var(--brand-green-stroke);
            font-size:10px;font-weight:800;padding:3px 10px;border-radius:20px;display:none;white-space:nowrap;}

/* CITY FILTER — pill row on light */
.city-bar{background:var(--white);padding:11px 28px;display:flex;align-items:center;
          gap:8px;flex-wrap:wrap;border-bottom:1px solid var(--border);}
.city-btn{background:var(--paper);border:1px solid var(--border);
          color:var(--muted);border-radius:20px;padding:5px 14px;
          font-size:11px;font-weight:700;cursor:pointer;transition:all .15s;
          white-space:nowrap;font-family:inherit;}
.city-btn:hover{background:var(--white);color:var(--ink);border-color:var(--ink);}
.city-btn.active{background:var(--brand-green);border-color:var(--brand-green);color:#fff;
                 box-shadow:0 1px 3px rgba(25,190,5,.3);}
.city-note{font-size:10px;color:var(--muted);margin-left:6px;font-style:italic;}

/* SIGNAL TOGGLE BUTTONS */
.toggle-row{display:flex;flex-wrap:wrap;gap:6px;padding:0 28px 10px;}
.tog-btn{background:var(--white);border:2px solid var(--border);
         color:var(--muted);border-radius:20px;padding:6px 15px;
         font-size:11px;font-weight:800;cursor:pointer;transition:all .15s;
         white-space:nowrap;font-family:inherit;}
.tog-btn:hover{background:var(--paper);color:var(--ink);}
.tog-btn.active{background:color-mix(in srgb,var(--tc,#19be05) 14%,white);
                border-color:var(--tc,#19be05);color:var(--ink);}

/* COMPARE SUMMARY */
.cmp-summary{display:none;margin:14px 28px 0;background:rgba(25,190,5,.05);
             border:1px solid rgba(25,190,5,.18);border-radius:var(--r);padding:16px 20px;}
.cmp-sum-title{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
               color:var(--brand-green-stroke);margin-bottom:10px;}
.cmp-sum-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;}
@media(max-width:700px){.cmp-sum-grid{grid-template-columns:repeat(2,1fr);}}
.cmp-sum-item .lbl{font-size:11px;color:var(--muted);margin-bottom:4px;font-weight:600;}
.cmp-sum-item .vals{display:flex;align-items:center;gap:6px;flex-wrap:wrap;}
.av{font-size:14px;font-weight:800;color:var(--brand-green-stroke);}
.bv{font-size:14px;font-weight:800;color:var(--ink);}
.chg{font-size:11px;font-weight:800;padding:3px 8px;border-radius:10px;}

/* ALERT */
.alert-bar{background:#FEF2F2;border:1px solid #FECACA;border-radius:var(--r);
           padding:12px 18px;display:flex;align-items:center;gap:8px;
           font-size:13px;font-weight:700;color:var(--brand-orange-stroke);margin:14px 28px 0;}
.alert-bar.hidden{display:none;}

/* SECTION */
.section{padding:24px 28px 0;}
.sec-title{font-size:11px;font-weight:800;letter-spacing:.8px;color:var(--muted);
           text-transform:uppercase;margin-bottom:14px;}
.sec-hdr{display:flex;align-items:center;gap:12px;margin-bottom:12px;}
.sec-hdr-num{width:26px;height:26px;border-radius:50%;background:var(--brand-green);
             color:#fff;font-size:11px;font-weight:900;display:flex;align-items:center;
             justify-content:center;flex-shrink:0;line-height:1;
             box-shadow:0 1px 3px rgba(25,190,5,.3);}
.sec-hdr-label{font-size:12px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;
               color:var(--ink);}

/* SIGNAL CARDS */
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;padding:16px 28px 0;}
@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr);}}
.card{background:var(--card);border-radius:var(--r);padding:18px;
      border:1px solid var(--border);position:relative;overflow:hidden;
      box-shadow:0 1px 4px rgba(0,0,0,.06),0 4px 12px rgba(0,0,0,.04);}
.spark-wrap{height:36px;margin-top:8px;position:relative;}
.spark-wrap canvas{display:block;}
.s-bar{position:absolute;top:0;left:0;right:0;height:4px;}
.s-bar.green{background:var(--green)}.s-bar.yellow{background:var(--yellow)}
.s-bar.red{background:var(--red)}.s-bar.grey{background:var(--grey)}
.snum{font-size:10px;font-weight:800;color:var(--brand-green-stroke);letter-spacing:.6px;
      text-transform:uppercase;margin-bottom:4px;}
.stitle{font-size:13px;font-weight:800;margin-bottom:14px;color:var(--ink);}
.sval{font-size:30px;font-weight:900;line-height:1;color:var(--ink);}
.sunit{font-size:12px;color:var(--muted);margin-left:3px;}
.smeta{margin-top:8px;display:flex;align-items:center;gap:6px;flex-wrap:wrap;}
.sbase{font-size:11px;color:var(--muted);}
.sdelta{font-size:11px;font-weight:700;padding:2px 7px;border-radius:20px;}
.sdelta.green{color:var(--green);background:var(--green-bg);}
.sdelta.yellow{color:var(--yellow);background:var(--yellow-bg);}
.sdelta.red{color:var(--red);background:var(--red-bg);}
.sdelta.grey{color:var(--grey);background:var(--grey-bg);}
.sthresh{font-size:10px;color:var(--muted);margin-top:6px;}
.stool{font-size:10px;color:var(--muted);margin-top:5px;font-style:italic;}
.sfresh{font-size:9px;font-weight:600;letter-spacing:.3px;margin-top:6px;padding:2px 7px;
        border-radius:10px;display:inline-block;background:rgba(0,0,0,.06);color:var(--muted);}
.sfresh.fresh{background:rgba(22,163,74,.1);color:#16a34a;}
.sfresh.stale{background:rgba(234,179,8,.12);color:#b45309;}
.sfresh.old{background:rgba(220,38,38,.1);color:#dc2626;}
.scmp{display:none;margin-top:8px;padding-top:8px;border-top:1px solid var(--border);}
.scmp-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
.dot{width:7px;height:7px;border-radius:50%;flex-shrink:0;}
.dot-a{background:var(--orange)}.dot-b{background:var(--navy-light)}
.cmp-v{display:flex;align-items:center;gap:4px;}
.cmp-lbl{font-size:10px;color:var(--muted);}
.cmp-num{font-size:13px;font-weight:700;}
.cmp-d{font-size:11px;font-weight:700;padding:2px 7px;border-radius:20px;}

/* CHARTS */
.charts{display:grid;grid-template-columns:repeat(2,1fr);gap:16px;padding:12px 28px 0;}
@media(max-width:900px){.charts{grid-template-columns:1fr;}}
.ccrd{background:var(--card);border-radius:var(--r);padding:22px;border:1px solid var(--border);
      box-shadow:0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03);
      transition:box-shadow .2s, transform .2s;}
.ccrd:hover{box-shadow:0 2px 6px rgba(0,0,0,.06),0 8px 20px rgba(0,0,0,.05);}
.ctop{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px;}
.ctitle{font-size:14px;font-weight:800;color:var(--ink);}
.csub{font-size:11px;color:var(--muted);margin-top:3px;font-weight:500;}
.cwrap{position:relative;height:240px;}

/* SOV + SENTIMENT */
.sov-row{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:12px 28px 0;}
@media(max-width:700px){.sov-row{grid-template-columns:1fr;}}
.sov-crd{background:var(--card);border-radius:var(--r);padding:18px;border:1px solid var(--border);}
.sov-wrap{display:flex;align-items:center;gap:20px;}
.sov-donut{position:relative;width:150px;height:150px;flex-shrink:0;}
.leg-item{display:flex;align-items:center;gap:7px;margin-bottom:7px;}
.leg-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;}
.leg-lbl{font-size:12px;}
.leg-pct{font-size:12px;font-weight:700;margin-left:auto;}
.sent-item{margin-bottom:9px;}
.sent-lbl{display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px;}
.sent-bg{height:7px;background:var(--border);border-radius:4px;overflow:hidden;}
.sent-fill{height:100%;border-radius:4px;}
.neg-rate-val{font-size:22px;font-weight:800;margin-top:10px;}

/* BOTTOM */
.bot{padding:12px 28px 32px;}
.icrd{background:var(--card);border-radius:var(--r);padding:18px;
      border:1px solid var(--border);margin-top:6px;}
table.log-t{width:100%;border-collapse:collapse;font-size:12px;}
table.log-t th{text-align:left;padding:7px 10px;background:var(--bg);
               font-size:10px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;
               color:var(--muted);border-bottom:1px solid var(--border);}
table.log-t td{padding:9px 10px;border-bottom:1px solid var(--border);}
table.log-t tr:last-child td{border-bottom:none;}
.tag{display:inline-block;padding:2px 7px;border-radius:10px;font-size:10px;font-weight:600;}
.t-ooh{background:#FEF3C7;color:#92400E}.t-auto{background:#E0F2FE;color:#075985}
.t-act{background:#FCE7F3;color:#9D174D}.t-pr{background:#EDE9FE;color:#5B21B6}
.t-fest{background:#D1FAE5;color:#065F46}
table.int-t{width:100%;border-collapse:collapse;font-size:12px;}
table.int-t th{text-align:left;padding:7px 10px;background:var(--bg);
               font-size:10px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;
               color:var(--muted);border-bottom:1px solid var(--border);}
table.int-t td{padding:9px 10px;border-bottom:1px solid var(--border);vertical-align:top;line-height:1.5;}
table.int-t tr:last-child td{border-bottom:none;}
.int-sig{font-weight:600;}.int-act{color:var(--orange);font-weight:600;}
footer{text-align:center;padding:20px;font-size:10px;color:var(--muted);}

/* ── TAB BAR ── */
.tab-bar{background:var(--white);display:flex;gap:0;padding:0 20px;border-bottom:1px solid var(--border);}
.tab-btn{background:transparent;border:none;border-bottom:3px solid transparent;
         color:var(--muted);padding:13px 24px;font-size:12px;font-weight:800;
         cursor:pointer;margin-bottom:-1px;transition:all .15s;letter-spacing:.4px;
         white-space:nowrap;font-family:inherit;}
.tab-btn:hover{color:var(--ink);}
.tab-btn.active{color:var(--brand-green);border-bottom-color:var(--brand-green);}

/* ── CAMPAIGN BREAKDOWN ── */
.camp-table{width:100%;border-collapse:collapse;font-size:12px;}
.camp-table th{text-align:left;padding:7px 10px;background:var(--bg);font-size:10px;font-weight:700;
               letter-spacing:.8px;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border);}
.camp-table td{padding:8px 10px;border-bottom:1px solid var(--border);vertical-align:middle;}
.camp-table tr:last-child td{border-bottom:none;}
.camp-tag{display:inline-block;padding:2px 7px;border-radius:10px;font-size:10px;font-weight:600;margin-right:5px;}
.camp-bar{height:5px;border-radius:3px;min-width:2px;}

/* ── INTELLIGENCE TAB ── */
.guide-card{background:#fff;border-radius:var(--r);padding:20px 24px;border:1px solid var(--border);margin:0 28px 12px;}
.guide-title{font-size:13px;font-weight:800;color:var(--navy);margin-bottom:14px;display:flex;align-items:center;gap:8px;}
.guide-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;}
@media(max-width:800px){.guide-grid{grid-template-columns:1fr;}}
.guide-item{background:var(--bg);border-radius:8px;padding:14px;}
.guide-item-title{font-size:11px;font-weight:700;color:var(--navy);margin-bottom:6px;}
.guide-item-body{font-size:11px;color:var(--muted);line-height:1.6;}
.corr-dl-btn{background:var(--white);border:1px solid var(--border);color:var(--muted);
             border-radius:8px;padding:6px 14px;font-size:10px;font-weight:800;cursor:pointer;
             letter-spacing:.3px;transition:all .15s;font-family:inherit;}
.corr-dl-btn:hover{background:var(--paper);color:var(--brand-green);border-color:var(--brand-green);}
</style>
</head>
<body>

<!-- HEADER -->
<header class="hdr">
  <div class="hdr-left">
    <div class="logo">SUPERTAILS</div>
    <div>
      <div class="hdr-title">Brand Dashboard</div>
      <div class="hdr-sub">All India · Daily Tracking</div>
    </div>
  </div>
  <div class="hdr-right">
    <div class="signal-status" id="signalStatus"></div>
    <div class="freshness">Data current as of <b id="freshDate">—</b> (t-1)</div>
    <div class="refresh-ctrl">
      <button class="refresh-toggle on" id="refreshToggle" onclick="toggleAutoRefresh()"></button>
      <span id="refreshLabel">Auto-refresh in <b id="countdown">5:00</b></span>
    </div>
  </div>
</header>

<!-- TAB BAR -->
<div class="tab-bar">
  <button class="tab-btn active" id="tab-btn-dashboard" onclick="switchTab('dashboard')">📊 Dashboard</button>
  <button class="tab-btn" id="tab-btn-intelligence" onclick="switchTab('intelligence')">🎯 Intelligence &amp; Signals</button>
</div>

<!-- ═══════════════════════ TAB 1 — DASHBOARD ═══════════════════════ -->
<div id="tabContent-dashboard">

<!-- VIEW CONTROLS (date range + granularity) -->
<div class="controls">
  <span class="ctrl-label">View</span>
  <input type="date" class="date-input" id="viewStart">
  <span class="ctrl-arrow">→</span>
  <input type="date" class="date-input" id="viewEnd">
  <div class="ctrl-divider"></div>
  <span class="ctrl-label">Granularity</span>
  <div class="gran-btns">
    <button class="gran-btn active" id="gD" onclick="setGran('D')">Daily</button>
    <button class="gran-btn" id="gW" onclick="setGran('W')">Weekly</button>
    <button class="gran-btn" id="gM" onclick="setGran('M')">Monthly</button>
  </div>
  <button class="apply-btn" onclick="applyView()">Apply</button>
</div>

<!-- COMPARE PANEL -->
<div class="cmp-panel">
  <span class="ctrl-label">Compare</span>
  <span class="period-tag tag-a">A</span>
  <input type="date" class="date-input" id="aStart">
  <span class="ctrl-arrow">→</span>
  <input type="date" class="date-input" id="aEnd">
  <div class="ctrl-divider"></div>
  <span class="period-tag tag-b">B</span>
  <input type="date" class="date-input" id="bStart">
  <span class="ctrl-arrow">→</span>
  <input type="date" class="date-input" id="bEnd">
  <button class="apply-btn" onclick="applyComparison()">Compare</button>
  <button class="cmp-reset" onclick="resetComparison()">Reset</button>
  <span class="cmp-active" id="cmpBadge">● Comparing A vs B</span>
</div>

<!-- COMPARISON SUMMARY BAR -->
<div class="cmp-summary" id="cmpSummary">
  <div class="cmp-sum-title">Period Comparison — Averages per day</div>
  <div class="cmp-sum-grid" id="cmpGrid"></div>
</div>

<!-- CITY FILTER -->
<div class="city-bar" id="cityBar">
  <span class="ctrl-label">City</span>
  <button class="city-btn active" id="city-all" onclick="setCity('all')">All India</button>
  <span id="cityBtns"></span>
  <span class="city-note" id="cityNote"></span>
</div>

<!-- SIGNAL CARDS -->
<div class="section"><div class="sec-title">Signal Health — Selected Period vs Baseline</div></div>
<div class="cards">
  <div class="card" id="c_blr_orders" style="border-left:3px solid #f97316;">
    <div class="s-bar" id="b_blr_orders"></div>
    <div class="stitle">🏙 Bangalore Orders</div>
    <div><span class="sval" id="v_blr_orders_card">—</span><span class="sunit" id="su_blr_orders">orders/day</span></div>
    <div class="smeta"><span class="sbase" id="bl_blr_orders">Baseline: —</span><span class="sdelta" id="d_blr_orders_card">—</span></div>
    <div class="scmp" id="cmp_blr_orders"></div>
    <div class="spark-wrap"><canvas id="spark_blr_orders" height="36"></canvas></div>
    <div class="stool">Supertails MCP · City = Bangalore · Offline campaign signal</div>
    <div class="sfresh" id="fr_blr_orders">—</div>
  </div>
  <div class="card" id="c1">
    <div class="s-bar" id="b1"></div>
    <div class="stitle">Branded Search</div>
    <div><span class="sval" id="v1">—</span><span class="sunit" id="su1">impressions/wk</span></div>
    <div class="smeta"><span class="sbase" id="bl1">Baseline: —</span><span class="sdelta" id="d1">—</span></div>
    <div class="scmp" id="cmp1"></div>
    <div class="spark-wrap"><canvas id="spark1" height="36"></canvas></div>
    <div class="stool">Google Search Console · All India</div>
    <div class="sfresh" id="fr1">—</div>
  </div>
  <div class="card" id="c2">
    <div class="s-bar" id="b2"></div>
    <div class="stitle">Organic Installs</div>
    <div><span class="sval" id="v2">—</span><span class="sunit" id="su2">installs/wk</span></div>
    <div class="smeta"><span class="sbase" id="bl2">Baseline: —</span><span class="sdelta" id="d2">—</span></div>
    <div class="scmp" id="cmp2"></div>
    <div class="spark-wrap"><canvas id="spark2" height="36"></canvas></div>
    <div class="stool">AppsFlyer · All India · Organic only · Brand signal</div>
    <div class="sfresh" id="fr2">—</div>
  </div>
  <div class="card" id="c3">
    <div class="s-bar" id="b3"></div>
    <div class="stitle">Non-Paid Sessions</div>
    <div><span class="sval" id="v3">—</span><span class="sunit" id="su3">sessions/wk</span></div>
    <div class="smeta"><span class="sbase" id="bl3">Baseline: —</span><span class="sdelta" id="d3">—</span></div>
    <div class="scmp" id="cmp3"></div>
    <div class="spark-wrap"><canvas id="spark3" height="36"></canvas></div>
    <div class="stool">GA4 · All non-paid channels · All India</div>
    <div class="sfresh" id="fr3">—</div>
  </div>
  <div class="card" id="c4" style="position:relative;">
    <div class="s-bar" id="b4"></div>
    <div class="stitle">Brand Mentions</div>
    <div><span class="sval" id="v4">—</span><span class="sunit" id="su4">mentions/wk</span></div>
    <div class="smeta"><span class="sbase" id="bl4">Baseline: —</span><span class="sdelta" id="d4">—</span></div>
    <div class="scmp" id="cmp4"></div>
    <div class="spark-wrap"><canvas id="spark4" height="36"></canvas></div>
    <div class="stool">Meltwater · Instagram, X, Reddit, LinkedIn</div>
    <div class="sfresh" id="fr4">—</div>
    <!-- Negative mentions sub-line -->
    <div id="neg_sub" style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;">
      <span style="font-size:11px;color:var(--muted);">⚠ Negative mentions</span>
      <span style="display:flex;align-items:center;gap:6px;">
        <span id="v_neg_mentions" style="font-size:12px;font-weight:700;">—</span>
        <span id="d_neg_mentions" class="sdelta grey" style="font-size:10px;">—</span>
      </span>
    </div>
    <div id="c4_nc" style="display:none;position:absolute;inset:0;background:rgba(241,245,249,0.92);border-radius:12px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;">
      <div style="font-size:18px;">🔌</div>
      <div style="font-size:12px;font-weight:600;color:#475569;">Not Connected</div>
      <div style="font-size:10px;color:#94a3b8;">Meltwater plugin required</div>
    </div>
  </div>
  <div class="card" id="c_brand_spend">
    <div class="s-bar" id="b_brand_spend"></div>
    <div class="stitle">Brand Spend (BM)</div>
    <div><span class="sval" id="v_brand_spend">—</span><span class="sunit" id="su_brand_spend">₹/wk</span></div>
    <div class="smeta"><span class="sbase" id="bl_brand_spend">Baseline: —</span><span class="sdelta" id="d_brand_spend">—</span></div>
    <div class="stool">Unified Dashboard · Brand Marketing · d-1</div>
    <div class="sfresh" id="fr_brand_spend">—</div>
  </div>
  <div class="card" id="c_perf_spend">
    <div class="s-bar" id="b_perf_spend"></div>
    <div class="stitle">Performance Spend</div>
    <div><span class="sval" id="v_perf_spend">—</span><span class="sunit" id="su_perf_spend">₹/wk</span></div>
    <div class="smeta"><span class="sbase" id="bl_perf_spend">Baseline: —</span><span class="sdelta" id="d_perf_spend">—</span></div>
    <div class="stool">Unified Dashboard · Performance · d-1</div>
    <div class="sfresh" id="fr_perf_spend">—</div>
  </div>
  <div class="card" id="c_spend_ratio">
    <div class="s-bar" id="b_spend_ratio" style="background:var(--navy-light);"></div>
    <div class="stitle">Brand : Perf Ratio</div>
    <div><span class="sval" id="v_spend_ratio" style="font-size:20px;">—</span></div>
    <div class="smeta"><span class="sbase" id="bl_spend_ratio" style="color:var(--muted);">Latest period split</span></div>
    <div class="stool">Brand vs Performance spend mix</div>
    <div class="sfresh" id="fr_spend_ratio">—</div>
  </div>
</div>

<!-- 1 ─ BRAND CAMPAIGN SPEND -->
<div class="section" style="margin-top:10px;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
    <div class="sec-hdr" style="margin-bottom:0;">
      <div class="sec-hdr-num">1</div>
      <div class="sec-hdr-label">Brand Campaign Spend — <span id="campBreakLabel">Selected Period</span></div>
    </div>
    <span style="font-size:10px;color:var(--muted);">Meta &amp; Google · Brand campaigns only</span>
  </div>
  <div style="background:#fff;border-radius:var(--r);border:1px solid var(--border);overflow:hidden;">
    <div id="campaignBreakTable" style="padding:0;"></div>
  </div>
</div>

<!-- 2 ─ SPEND TRENDS: Brand & Perf individual + Stacked comparison -->
<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">2</div><div class="sec-hdr-label">Spend Trends — Brand &amp; Performance</div></div>
</div>
<div class="charts" style="grid-template-columns:1fr 1fr;">
  <div class="ccrd">
    <div class="ctop"><div><div class="ctitle">Brand Spend (BM) — Daily ₹</div><div class="csub">Google Sheet · d-1 · Brand Marketing</div></div></div>
    <div class="cwrap"><canvas id="ch_brand_spend"></canvas></div>
  </div>
  <div class="ccrd">
    <div class="ctop"><div><div class="ctitle">Performance Spend — Daily ₹</div><div class="csub">Google Sheet · d-1 · Performance campaigns</div></div></div>
    <div class="cwrap"><canvas id="ch_perf_spend"></canvas></div>
  </div>
</div>
<div class="charts" style="grid-template-columns:1fr;">
  <div class="ccrd">
    <div class="ctop">
      <div><div class="ctitle">Brand vs Performance Spend — Stacked</div><div class="csub" id="spendStackRangeLabel">Google Sheet · Daily total outlay</div></div>
      <div id="spendSplitDisplay" style="font-size:11px;color:var(--muted);font-weight:600;"></div>
    </div>
    <div class="cwrap" style="height:240px;"><canvas id="ch_spend_stack"></canvas></div>
  </div>
</div>

<!-- 3 ─ BRANDED SEARCH VOLUME -->
<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">3</div><div class="sec-hdr-label">Branded Search Volume — <span id="chartRangeLabel"></span></div></div>
</div>
<div class="charts" style="grid-template-columns:1fr;">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Branded Search Impressions</div><div class="csub">GSC · All India · Branded queries · 3-day lag</div></div></div><div class="cwrap" style="height:240px;"><canvas id="ch1"></canvas></div></div>
</div>

<!-- 4 ─ INSTALLS: Organic vs Paid -->
<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">4</div><div class="sec-hdr-label">App Installs — <span id="instSplitRangeLabel"></span></div></div>
</div>
<div class="charts" style="grid-template-columns:1fr 1fr;">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Organic Installs</div><div class="csub">AppsFlyer · All India · Organic only · Brand-driven signal</div></div></div><div class="cwrap"><canvas id="ch2"></canvas></div></div>
  <div class="ccrd">
    <div class="ctop">
      <div><div class="ctitle">Organic vs Paid Installs</div><div class="csub">AppsFlyer · All India · Android + iOS</div></div>
      <div id="instSplitRatioDisplay" style="font-size:11px;color:var(--muted);text-align:right;"></div>
    </div>
    <div class="cwrap"><canvas id="ch_inst_split"></canvas></div>
  </div>
</div>
<div class="charts" style="grid-template-columns:1fr;margin-top:10px;">
  <div class="ccrd">
    <div class="ctop">
      <div>
        <div class="ctitle">Installs by City — Organic vs Paid</div>
        <div class="csub">AppsFlyer · Pincode rolled up to city · Period total · <span id="instCityRangeLabel"></span></div>
      </div>
      <div id="instCityNoteDisplay" style="font-size:11px;color:var(--muted);text-align:right;"></div>
    </div>
    <div class="cwrap" style="height:300px;"><canvas id="ch_inst_city"></canvas></div>
  </div>
</div>

<!-- 5 ─ SESSIONS: Non-Paid + Paid individual, then combined toggle -->
<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">5</div><div class="sec-hdr-label">Sessions</div></div>
</div>
<div class="charts">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Non-Paid Sessions</div><div class="csub" id="sub_nonpaid">GA4 · All non-paid channels · <span id="sub_nonpaid_city">All India</span></div></div></div><div class="cwrap"><canvas id="ch_nonpaid"></canvas></div></div>
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Paid Sessions</div><div class="csub" id="sub_paid">GA4 · All paid channels · <span id="sub_paid_city">All India</span></div></div></div><div class="cwrap"><canvas id="ch_paid"></canvas></div></div>
</div>
<div class="section" style="margin-top:10px;">
  <div class="sec-title">All Sessions — Combined View</div>
  <div class="toggle-row" id="sessToggles">
    <button class="tog-btn active" data-key="total_nonpaid_sessions" onclick="toggleSess(this)" style="--tc:#6366f1">Non-Paid</button>
    <button class="tog-btn active" data-key="total_paid_sessions"    onclick="toggleSess(this)" style="--tc:#ef4444">Paid</button>
    <button class="tog-btn"        data-key="direct_sessions"         onclick="toggleSess(this)" style="--tc:#3b82f6">Direct</button>
    <button class="tog-btn"        data-key="brand_paid_sessions"     onclick="toggleSess(this)" style="--tc:#8b5cf6">Brand Paid</button>
    <button class="tog-btn"        data-key="total_installs"          onclick="toggleSess(this)" style="--tc:#f59e0b">Total Installs</button>
    <button class="tog-btn"        data-key="direct_installs"         onclick="toggleSess(this)" style="--tc:#fb923c">Organic Installs</button>
  </div>
</div>
<div class="charts" style="grid-template-columns:1fr;">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">All Sessions — Combined</div><div class="csub" id="sub_all_sess">GA4 · All India · Toggle signals above</div></div></div><div class="cwrap" style="height:260px;"><canvas id="ch_all_sess"></canvas></div></div>
</div>

<!-- 6 ─ SIGNAL CORRELATION -->
<div class="section" style="margin-top:14px;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;flex-wrap:wrap;gap:6px;">
    <div class="sec-hdr" style="margin-bottom:0;"><div class="sec-hdr-num">6</div><div class="sec-hdr-label">Signal Correlation — Normalised Overlay</div></div>
    <button class="corr-dl-btn" onclick="downloadCorrCSV()">↓ Download Weekly CSV</button>
  </div>
  <div style="font-size:11px;color:var(--muted);margin-bottom:6px;">Each signal indexed to 100 = baseline average. Compare trends and lead/lag.</div>
  <div class="toggle-row" id="corrToggles">
    <button class="tog-btn active" data-key="branded_search"          onclick="toggleCorr(this)" style="--tc:#e8450a">Branded Search</button>
    <button class="tog-btn active" data-key="direct_sessions"         onclick="toggleCorr(this)" style="--tc:#3b82f6">Non-Paid Brand</button>
    <button class="tog-btn active" data-key="brand_paid_sessions"     onclick="toggleCorr(this)" style="--tc:#8b5cf6">Brand Paid</button>
    <button class="tog-btn active" data-key="perf_sessions"           onclick="toggleCorr(this)" style="--tc:#ef4444">Performance</button>
    <button class="tog-btn active" data-key="direct_installs"         onclick="toggleCorr(this)" style="--tc:#f59e0b">Organic Installs</button>
    <button class="tog-btn active" data-key="revenue_india"           onclick="toggleCorr(this)" style="--tc:#14b8a6">India NMV</button>
    <button class="tog-btn active" data-key="brand_spend"             onclick="toggleCorr(this)" style="--tc:#7c3aed">Brand Spend</button>
    <button class="tog-btn active" data-key="perf_spend"              onclick="toggleCorr(this)" style="--tc:#dc2626">Perf Spend</button>
  </div>
</div>
<div class="charts" style="grid-template-columns:1fr;">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Signal Correlation View</div><div class="csub">Indexed to 100 = baseline · Toggle signals above</div></div></div><div class="cwrap" style="height:300px;"><canvas id="ch_corr"></canvas></div></div>
</div>

<!-- 7 ─ INDIA: ORDERS + REVENUE -->
<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">7</div><div class="sec-hdr-label">India Orders &amp; NMV — <span id="revRangeLabel"></span></div></div>
</div>
<div class="charts">
  <div class="ccrd">
    <div class="ctop">
      <div><div class="ctitle">India Daily Orders</div><div class="csub">Supertails MCP · All India · Order count</div></div>
      <div style="text-align:right;">
        <div style="font-size:18px;font-weight:800;color:var(--ink);" id="v_ind_orders">—</div>
        <div style="font-size:10px;color:var(--muted);">latest day</div>
        <div id="d_ind_orders" class="sdelta grey" style="margin-top:2px;">—</div>
      </div>
    </div>
    <div class="cwrap"><canvas id="ch_ind_orders"></canvas></div>
  </div>
  <div class="ccrd">
    <div class="ctop">
      <div><div class="ctitle">India NMV (₹)</div><div class="csub">Supertails MCP · All India · Net Merchandise Value</div></div>
      <div style="text-align:right;">
        <div style="font-size:18px;font-weight:800;color:var(--ink);" id="v_ind_rev">—</div>
        <div style="font-size:10px;color:var(--muted);">latest day</div>
        <div id="d_ind_rev" class="sdelta grey" style="margin-top:2px;">—</div>
      </div>
    </div>
    <div class="cwrap"><canvas id="ch_rev"></canvas></div>
  </div>
</div>

<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">8</div><div class="sec-hdr-label">Bangalore Orders &amp; Revenue — <span id="blrOrdRangeLabel"></span></div></div>
</div>
<div class="charts">
  <div class="ccrd">
    <div class="ctop">
      <div><div class="ctitle">Bangalore Daily Orders</div><div class="csub">Supertails MCP · City = Bangalore · Order count</div></div>
      <div style="text-align:right;">
        <div style="font-size:18px;font-weight:700;color:var(--text);" id="v_blr_orders">—</div>
        <div style="font-size:10px;color:var(--muted);">latest day</div>
        <div id="d_blr_orders" class="sdelta grey" style="margin-top:2px;">—</div>
      </div>
    </div>
    <div class="cwrap"><canvas id="ch_blr_orders"></canvas></div>
  </div>
  <div class="ccrd">
    <div class="ctop">
      <div><div class="ctitle">Bangalore NMV (₹)</div><div class="csub">Supertails MCP · City = Bangalore · Net Merchandise Value</div></div>
      <div style="text-align:right;">
        <div style="font-size:18px;font-weight:700;color:var(--text);" id="v_blr_rev">—</div>
        <div style="font-size:10px;color:var(--muted);">latest day</div>
        <div id="d_blr_rev" class="sdelta grey" style="margin-top:2px;">—</div>
      </div>
    </div>
    <div class="cwrap"><canvas id="ch_blr_rev"></canvas></div>
  </div>
</div>

<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">9</div><div class="sec-hdr-label">Paid Breakdown — <span id="paidBreakLabel"></span></div></div>
</div>
<div class="charts">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Brand Campaign Sessions</div><div class="csub">GA4 · Campaigns with "Brand" · All India</div></div></div><div class="cwrap"><canvas id="ch3"></canvas></div></div>
  <div class="ccrd">
    <div class="ctop">
      <div><div class="ctitle">Brand vs Performance Sessions</div><div class="csub">GA4 · Brand = "Brand" campaigns · Perf = all other paid · All India</div></div>
      <div id="bvpRatioDisplay" style="font-size:11px;color:var(--muted);text-align:right;"></div>
    </div>
    <div class="cwrap"><canvas id="ch_bvp"></canvas></div>
  </div>
</div>
<div style="display:none;"><span id="bvpRangeLabel"></span></div>

<!-- SOV + SENTIMENT -->
<div class="section" style="margin-top:14px;">
  <div class="sec-hdr"><div class="sec-hdr-num">10</div><div class="sec-hdr-label">Share of Voice &amp; Sentiment — Latest data point</div></div>
</div>
<div class="sov-row">
  <div class="sov-crd">
    <div class="ctitle" style="margin-bottom:12px;">Share of Voice vs Competitors</div>
    <div class="sov-wrap">
      <div class="sov-donut"><canvas id="sovChart"></canvas></div>
      <div id="sovLegend" style="flex:1;"></div>
    </div>
  </div>
  <div class="sov-crd">
    <div class="ctitle">Sentiment Split</div>
    <div class="csub" style="margin-bottom:12px;">Meltwater automated classification · latest period</div>
    <div id="sentBars"></div>
  </div>
</div>

</div><!-- /tabContent-dashboard -->

<!-- ═══════════════════════ TAB 2 — INTELLIGENCE & SIGNALS ═══════════════════════ -->
<div id="tabContent-intelligence" style="display:none;">

<!-- SIGNAL DICTIONARY -->
<div class="guide-card" style="margin-top:16px;">
  <div class="guide-title">📡 Signal Dictionary — What Each Number Means for the Brand</div>

  <!-- Signal timing flow -->
  <div style="display:flex;align-items:center;justify-content:center;gap:0;margin-bottom:16px;flex-wrap:wrap;">
    <div style="text-align:center;padding:8px 14px;background:#EFF6FF;border-radius:8px;border:1px solid #BFDBFE;">
      <div style="font-size:9px;font-weight:700;color:#1E40AF;letter-spacing:.5px;">LEADING (+2d)</div>
      <div style="font-size:11px;font-weight:700;color:#1E40AF;margin-top:2px;">🔍 Branded Search</div>
      <div style="font-size:9px;color:#3B82F6;margin-top:1px;">Brand Mentions</div>
    </div>
    <div style="font-size:16px;color:var(--muted);padding:0 6px;">→</div>
    <div style="text-align:center;padding:8px 14px;background:#F0FDF4;border-radius:8px;border:1px solid #BBF7D0;">
      <div style="font-size:9px;font-weight:700;color:#15803D;letter-spacing:.5px;">COINCIDENT</div>
      <div style="font-size:11px;font-weight:700;color:#15803D;margin-top:2px;">🌐 Non-Paid Sessions</div>
      <div style="font-size:9px;color:#22C55E;margin-top:1px;">Brand Paid Sessions</div>
    </div>
    <div style="font-size:16px;color:var(--muted);padding:0 6px;">→</div>
    <div style="text-align:center;padding:8px 14px;background:#FFF7ED;border-radius:8px;border:1px solid #FED7AA;">
      <div style="font-size:9px;font-weight:700;color:#C2410C;letter-spacing:.5px;">LAGGING (2–7d)</div>
      <div style="font-size:11px;font-weight:700;color:#C2410C;margin-top:2px;">📱 Organic Installs</div>
      <div style="font-size:9px;color:#F97316;margin-top:1px;">India NMV</div>
    </div>
  </div>

  <!-- Signal rows -->
  <div style="display:flex;flex-direction:column;gap:8px;">

    <!-- Branded Search -->
    <div style="display:grid;grid-template-columns:180px 1fr 1fr 100px;gap:10px;align-items:start;padding:10px 12px;background:#F8F9FA;border-radius:8px;border-left:3px solid #3B82F6;">
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--text);">🔍 Branded Search</div>
        <div style="font-size:9px;color:var(--muted);margin-top:2px;">Source: Google Search Console</div>
        <div style="font-size:9px;color:#3B82F6;margin-top:2px;font-weight:600;">LEADING · +2 days</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">What it measures</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">Weekly impressions for brand search queries (supertails, super tails, etc.) on Google India. This is the earliest signal that offline advertising is creating digital brand recall — people who see a billboard or auto will Google the brand before downloading the app.</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">How to read it (brand POV)</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">+15–20% above baseline = strong offline recall translating to digital intent. If search is up but installs lag, check App Store listing. Sustained lift for 2+ weeks = campaign is building durable brand awareness, not just burst. GSC has a 3-day processing lag.</div>
      </div>
      <div style="text-align:center;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);margin-bottom:4px;">THRESHOLDS</div>
        <div style="font-size:9px;padding:2px 6px;background:#DCFCE7;color:#15803D;border-radius:4px;margin-bottom:2px;">🟢 &gt;+15%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEF3C7;color:#92400E;border-radius:4px;margin-bottom:2px;">🟡 0–15%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEE2E2;color:#991B1B;border-radius:4px;">🔴 Below baseline</div>
      </div>
    </div>

    <!-- Organic Installs -->
    <div style="display:grid;grid-template-columns:180px 1fr 1fr 100px;gap:10px;align-items:start;padding:10px 12px;background:#F8F9FA;border-radius:8px;border-left:3px solid #F97316;">
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--text);">📱 Organic Installs</div>
        <div style="font-size:9px;color:var(--muted);margin-top:2px;">Source: AppsFlyer (CSV)</div>
        <div style="font-size:9px;color:#F97316;margin-top:2px;font-weight:600;">LAGGING · +2 days</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">What it measures</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">App installs attributed to organic sources (no paid media) across Android and iOS, All-India. Organic installs are the clearest evidence that brand awareness is converting into product adoption — someone chose to install without being retargeted.</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">How to read it (brand POV)</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">Lags branded search by ~2 days (r=0.55 in historical data). A +10% lift 2–3 days after a search spike confirms the funnel is converting. Flat installs despite high search = friction in App Store listing or onboarding. This is the single best proof point for campaign ROI.</div>
      </div>
      <div style="text-align:center;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);margin-bottom:4px;">THRESHOLDS</div>
        <div style="font-size:9px;padding:2px 6px;background:#DCFCE7;color:#15803D;border-radius:4px;margin-bottom:2px;">🟢 &gt;+10%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEF3C7;color:#92400E;border-radius:4px;margin-bottom:2px;">🟡 0–10%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEE2E2;color:#991B1B;border-radius:4px;">🔴 Below baseline</div>
      </div>
    </div>

    <!-- Non-Paid Sessions -->
    <div style="display:grid;grid-template-columns:180px 1fr 1fr 100px;gap:10px;align-items:start;padding:10px 12px;background:#F8F9FA;border-radius:8px;border-left:3px solid #22C55E;">
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--text);">🌐 Non-Paid Sessions</div>
        <div style="font-size:9px;color:var(--muted);margin-top:2px;">Source: GA4 (All-India)</div>
        <div style="font-size:9px;color:#22C55E;margin-top:2px;font-weight:600;">COINCIDENT</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">What it measures</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">All website sessions NOT from paid channels (Organic Search, Direct, Referral, Organic Social, Email). This shows how many people are coming to Supertails.com without being pushed by ad spend — a clean read of genuine brand pull.</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">How to read it (brand POV)</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">A spike here without a corresponding increase in paid spend = real organic brand interest. If non-paid sessions rise while paid sessions are flat or down, the brand is becoming self-sustaining. Watch for a 5–7 day sustained lift to separate campaign noise from real pull.</div>
      </div>
      <div style="text-align:center;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);margin-bottom:4px;">THRESHOLDS</div>
        <div style="font-size:9px;padding:2px 6px;background:#DCFCE7;color:#15803D;border-radius:4px;margin-bottom:2px;">🟢 &gt;+10%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEF3C7;color:#92400E;border-radius:4px;margin-bottom:2px;">🟡 0–10%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEE2E2;color:#991B1B;border-radius:4px;">🔴 Below baseline</div>
      </div>
    </div>

    <!-- Brand Paid Sessions -->
    <div style="display:grid;grid-template-columns:180px 1fr 1fr 100px;gap:10px;align-items:start;padding:10px 12px;background:#F8F9FA;border-radius:8px;border-left:3px solid #8B5CF6;">
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--text);">💳 Brand Paid Sessions</div>
        <div style="font-size:9px;color:var(--muted);margin-top:2px;">Source: GA4 (Brand campaigns)</div>
        <div style="font-size:9px;color:#8B5CF6;margin-top:2px;font-weight:600;">COINCIDENT</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">What it measures</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">Sessions from Google Ads campaigns tagged with "Brand" in the campaign name — these are paid brand defence campaigns capturing high-intent searchers who already know Supertails. This number is partially controlled by your spend, not just organic brand strength.</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">How to read it (brand POV)</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">If brand paid sessions rise in line with branded search, you're successfully capturing the intent you're generating. If branded search rises but brand paid sessions don't — either budget is capped or competitor bidding is stealing share. Compare with GSC impression share for full picture.</div>
      </div>
      <div style="text-align:center;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);margin-bottom:4px;">THRESHOLDS</div>
        <div style="font-size:9px;padding:2px 6px;background:#DCFCE7;color:#15803D;border-radius:4px;margin-bottom:2px;">🟢 &gt;+15%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEF3C7;color:#92400E;border-radius:4px;margin-bottom:2px;">🟡 0–15%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEE2E2;color:#991B1B;border-radius:4px;">🔴 Below baseline</div>
      </div>
    </div>

    <!-- Brand Mentions -->
    <div style="display:grid;grid-template-columns:180px 1fr 1fr 100px;gap:10px;align-items:start;padding:10px 12px;background:#F8F9FA;border-radius:8px;border-left:3px solid #22C55E;">
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--text);">💬 Brand Mentions</div>
        <div style="font-size:9px;color:var(--muted);margin-top:2px;">Source: Meltwater (Social)</div>
        <div style="font-size:9px;color:#22C55E;margin-top:2px;font-weight:600;">COINCIDENT</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">What it measures</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">Daily mentions of Supertails across Instagram, X (Twitter), Reddit, LinkedIn — including brand name, hashtags (#Supertails, #DanishSait), and campaign-related queries. Covers earned and owned social, not paid. Negative sub-line shows the % that are negative in sentiment.</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">How to read it (brand POV)</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">A spike in mentions post-OOH launch = the campaign is generating social conversation. Watch for a negative rate above 8% (caution) or 15% (escalate). Brand mentions often move with campaign launches and influencer content. Compare with HUFT mentions to track relative SOV in the pet care category.</div>
      </div>
      <div style="text-align:center;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);margin-bottom:4px;">NEG. RATE</div>
        <div style="font-size:9px;padding:2px 6px;background:#DCFCE7;color:#15803D;border-radius:4px;margin-bottom:2px;">🟢 &lt;8%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEF3C7;color:#92400E;border-radius:4px;margin-bottom:2px;">🟡 8–15%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEE2E2;color:#991B1B;border-radius:4px;">🔴 &gt;15%</div>
      </div>
    </div>

    <!-- Spend -->
    <div style="display:grid;grid-template-columns:180px 1fr 1fr 100px;gap:10px;align-items:start;padding:10px 12px;background:#F8F9FA;border-radius:8px;border-left:3px solid #E8450A;">
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--text);">₹ Brand &amp; Perf Spend</div>
        <div style="font-size:9px;color:var(--muted);margin-top:2px;">Source: Google Sheet (Daily)</div>
        <div style="font-size:9px;color:#E8450A;margin-top:2px;font-weight:600;">INPUT SIGNAL</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">What it measures</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">Daily ₹ split between Brand campaigns (those tagged "BrandMar" — designed to defend and grow branded search share) and Performance campaigns (conversion-focused, retargeting, category). The mix tells you how much of the budget is building the brand vs extracting from existing demand.</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">How to read it (brand POV)</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">Binet &amp; Field benchmark: 40–60% brand investment for sustained growth. With OOH live in Bangalore, increasing brand digital spend amplifies the offline signal by capturing the awareness you're generating. A perf-heavy mix (below 20% brand) means you're mining past equity, not building new equity.</div>
      </div>
      <div style="text-align:center;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);margin-bottom:4px;">BRAND MIX</div>
        <div style="font-size:9px;padding:2px 6px;background:#DCFCE7;color:#15803D;border-radius:4px;margin-bottom:2px;">🟢 &gt;35%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEF3C7;color:#92400E;border-radius:4px;margin-bottom:2px;">🟡 20–35%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEE2E2;color:#991B1B;border-radius:4px;">🔴 &lt;20%</div>
      </div>
    </div>

    <!-- NMV -->
    <div style="display:grid;grid-template-columns:180px 1fr 1fr 100px;gap:10px;align-items:start;padding:10px 12px;background:#F8F9FA;border-radius:8px;border-left:3px solid #14B8A6;">
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--text);">📦 India NMV</div>
        <div style="font-size:9px;color:var(--muted);margin-top:2px;">Source: Supertails data MCP</div>
        <div style="font-size:9px;color:#14B8A6;margin-top:2px;font-weight:600;">LAGGING · 3–7 days</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">What it measures</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">Net Merchandise Value — total revenue net of returns, across All-India orders on the Supertails platform. This is the ultimate downstream outcome of the brand campaign. Because purchase decisions take days to weeks from first brand exposure, NMV is the last signal to move.</div>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:var(--text);margin-bottom:3px;">How to read it (brand POV)</div>
        <div style="font-size:10px;color:var(--muted);line-height:1.5;">A sustained +10% vs baseline, occurring 3–7 days after a branded search spike, is the strongest possible proof of campaign effectiveness. The Bangalore offline lift = NMV from Bangalore orders above the All-India trend. Don't judge NMV in week 1 — wait for the signal lag to resolve.</div>
      </div>
      <div style="text-align:center;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);margin-bottom:4px;">THRESHOLDS</div>
        <div style="font-size:9px;padding:2px 6px;background:#DCFCE7;color:#15803D;border-radius:4px;margin-bottom:2px;">🟢 &gt;+10%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEF3C7;color:#92400E;border-radius:4px;margin-bottom:2px;">🟡 0–10%</div>
        <div style="font-size:9px;padding:2px 6px;background:#FEE2E2;color:#991B1B;border-radius:4px;">🔴 Below baseline</div>
      </div>
    </div>

  </div><!-- /signal rows -->
</div>

<!-- HOW TO NAVIGATE -->
<div class="guide-card" style="margin-top:10px;">
  <div class="guide-title">📖 How to Navigate</div>
  <div class="guide-grid">
    <div class="guide-item">
      <div class="guide-item-title">📊 Dashboard Tab</div>
      <div class="guide-item-body">Your primary daily view. Signal cards show key brand-lift indicators vs pre-campaign baseline. Use the date range and granularity controls to zoom in or out. Toggle signals on the correlation overlay to see what's moving together.</div>
    </div>
    <div class="guide-item">
      <div class="guide-item-title">🎯 Intelligence Tab (here)</div>
      <div class="guide-item-body">Automated narrative, alerts, and action cards generated fresh on each data pull. "Ask Your Data" lets you query any signal in plain English — click a chip or type your own question. Use this tab to decide what to act on each week.</div>
    </div>
    <div class="guide-item">
      <div class="guide-item-title">📏 Baseline &amp; Thresholds</div>
      <div class="guide-item-body">Baseline = Jan 5 – Mar 22 2026 (W01–W11), WTF Sale excluded. Signal cards show % above/below baseline. Green = above threshold. Yellow = watch. Red = action needed. Don't panic at single-day dips — look at 7-day rolling trends.</div>
    </div>
    <div class="guide-item">
      <div class="guide-item-title">🏙️ Bangalore Delta</div>
      <div class="guide-item-body">Digital signals are All-India. Bangalore offline lift = Bangalore NMV above the India-wide trend. Use the city filter on the Dashboard tab to view Bangalore-specific session data and isolate the offline campaign effect.</div>
    </div>
    <div class="guide-item">
      <div class="guide-item-title">🔄 Data Refresh</div>
      <div class="guide-item-body">Run <code style="background:#f3f4f6;padding:1px 5px;border-radius:3px;font-size:10px;">python3 fetch_signals.py</code> daily (t-1 data). GSC has a 3-day processing lag. AppsFlyer loaded via CSV export. Spend auto-updates from Google Sheet each run.</div>
    </div>
    <div class="guide-item">
      <div class="guide-item-title">📊 Correlation Table</div>
      <div class="guide-item-body">Shows weekly values for all signals side-by-side. The r=0.92 figure means 92% of weekly Bangalore NMV variance is explained by the composite signal index — a very strong predictive relationship. Use it to spot weeks where signals diverge from revenue unexpectedly.</div>
    </div>
  </div>
</div>

<!-- DATA FRESHNESS -->
<div style="padding:10px 28px 4px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
    <span style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.5px;">DATA FRESHNESS</span>
    <div id="freshnessSyncBar" style="display:flex;gap:6px;flex-wrap:wrap;"></div>
  </div>
  <span id="commonDateBadge" style="font-size:10px;font-weight:600;background:rgba(232,69,10,.12);color:#E8450A;padding:3px 10px;border-radius:10px;white-space:nowrap;">Analysis to: —</span>
</div>

<!-- SUMMARY + ALERTS + ACTIONS + CHAT -->
<div style="display:grid;grid-template-columns:1fr 300px;gap:10px;padding:0 28px 10px;">
  <!-- Left: summary + action cards -->
  <div style="display:flex;flex-direction:column;gap:10px;">
    <div style="background:#fff;border-radius:var(--r);padding:14px 16px;border:1px solid var(--border);">
      <div style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.5px;margin-bottom:6px;">SUMMARY</div>
      <div id="narrativeText" style="font-size:12px;color:var(--text);line-height:1.65;"></div>
    </div>
    <div id="actionCards" style="display:grid;grid-template-columns:1fr 1fr;gap:10px;"></div>
  </div>
  <!-- Right: alerts + chat -->
  <div style="display:flex;flex-direction:column;gap:10px;">
    <div style="background:#fff;border-radius:var(--r);padding:14px 16px;border:1px solid var(--border);">
      <div style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.5px;margin-bottom:8px;">ALERTS</div>
      <div id="alertsList"></div>
    </div>
    <!-- Chat window -->
    <div style="background:#fff;border-radius:var(--r);border:1px solid var(--border);display:flex;flex-direction:column;flex:1;min-height:300px;">
      <div style="padding:10px 14px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:6px;">
        <span style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.5px;">ASK YOUR DATA</span>
      </div>
      <div id="chatSuggestions" style="padding:8px 14px;display:flex;flex-wrap:wrap;gap:5px;border-bottom:1px solid var(--border);"></div>
      <div id="chatMessages" style="flex:1;overflow-y:auto;padding:10px 14px;display:flex;flex-direction:column;gap:8px;max-height:260px;"></div>
      <div style="padding:8px 14px;border-top:1px solid var(--border);display:flex;gap:6px;">
        <input id="chatInput" type="text" placeholder="e.g. Is the campaign working? · How is brand sentiment? · Which signal first?"
          style="flex:1;border:1px solid var(--border);border-radius:6px;padding:6px 9px;font-size:11px;outline:none;"
          onkeydown="if(event.key==='Enter')sendChatMsg()">
        <button onclick="sendChatMsg()" style="background:var(--orange);color:#fff;border:none;border-radius:6px;padding:6px 12px;font-size:11px;font-weight:600;cursor:pointer;">Ask</button>
      </div>
    </div>
  </div>
</div>

<!-- WEEKLY CORRELATION REPORT TABLE -->
<div class="section" style="margin-top:4px;padding-bottom:16px;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;flex-wrap:wrap;gap:6px;">
    <div class="sec-title" style="margin-bottom:0;">Weekly Correlation Report</div>
    <button class="corr-dl-btn" onclick="downloadCorrCSV()" style="background:var(--orange);border-color:var(--orange);color:#fff;">↓ Download CSV</button>
  </div>
  <div style="font-size:11px;color:var(--muted);margin-bottom:10px;line-height:1.6;">
    Side-by-side weekly values for all signals. <b>How to read:</b> when Branded Search rises in week N, expect Organic Installs to follow in week N+1 and NMV to follow in week N+1 to N+2 (due to signal lags). A week where signals diverge from revenue is worth investigating. The composite r=0.92 means 92% of NMV variance is explained by the four-signal index.
  </div>
  <div style="background:#fff;border-radius:var(--r);border:1px solid var(--border);overflow:auto;">
    <div id="corrReportTable" style="padding:0;min-width:700px;"></div>
  </div>
</div>

</div><!-- /tabContent-intelligence -->

<div class="bot"></div>
<footer>Supertails Brand Dashboard · All India · Confidential</footer>

<script>
// ── ALL declarations at top — eliminates every temporal dead zone risk ────────
const S = __STORE_DATA__;
const CFG = __CONFIG_DATA__;
const ANALYSIS = __ANALYSIS_DATA__;
const baselines     = CFG.baselines || {};
const CITY_SESSIONS = S.city_sessions || {};
const CITY_LIST     = S.city_list || [];
const allDates      = S.dates||[];
const minDate       = allDates[0]||'';
const maxDate       = allDates[allDates.length-1]||'';
const today         = new Date().toISOString().split('T')[0];
const view90        = allDates.length>=90 ? allDates[allDates.length-90] : minDate;
const view7         = allDates.length>=7  ? allDates[allDates.length-7]  : minDate;
// Brand palette — Supertails Brand Guidelines 2026
const BRAND_GREEN='#19be05', BRAND_GREEN_STROKE='#75b52f',
      BRAND_ORANGE='#ff6914', BRAND_ORANGE_STROKE='#ca5310',
      INK='#0a0a0a';
// Legacy aliases — primary signal kept on green; secondary on dark ink
const ORANGE=BRAND_GREEN, NAVY=INK, NAVY_L='#4b5563',
      OBG='rgba(25,190,5,.12)', NBG='rgba(10,10,10,.06)', GREY_C='rgba(107,114,128,.45)';
const NOISE_CITIES  = ['(not set)', 'Ashburn'];
const CITY_KEY_TO_STORE = {
  'direct':        'direct_sessions',  'total_paid':    'total_paid_sessions',
  'total_nonpaid': 'total_nonpaid_sessions', 'brand_paid': 'brand_paid_sessions',
  'blr_paid':      'blr_paid_sessions',
};
const MO=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const CI={};
function addDays(d,n){const dt=new Date(d);dt.setDate(dt.getDate()+n);return dt.toISOString().split('T')[0];}
const cs       = S.campaign_start || allDates[Math.floor(allDates.length*0.8)] || allDates[0] || '';
const aStart0  = cs ? addDays(cs,-30) : minDate;
const aEnd0    = cs ? addDays(cs,-1)  : minDate;
const bStart0  = maxDate ? addDays(maxDate,-29) : minDate;
const bEnd0    = maxDate;
let refreshOn=true, countdown=300;
let activeCity='all';
let gran='D';
let currentSlice=null;
let sessTog=new Set(['total_nonpaid_sessions','total_paid_sessions']);
let corrTog=new Set(['branded_search','direct_sessions','brand_paid_sessions','perf_sessions','direct_installs','revenue_india','brand_spend','perf_spend']);

// ── Helpers
function fmt(n){return n==null?'—':Math.round(n).toLocaleString('en-IN');}
function fmtD(n){return n==null?'—':n.toFixed(1);}
function avg(arr){const v=(arr||[]).filter(x=>x!=null&&!isNaN(x));return v.length?v.reduce((a,b)=>a+b,0)/v.length:null;}
function pct(curr,base){return(curr==null||!base)?null:((curr-base)/base*100);}
function dcls(p,th){if(p==null)return'grey';return p>=th?'green':p>=0?'yellow':'red';}

// ── Header — signal status pills
document.getElementById('freshDate').textContent=S.last_fetched_to||'—';
(function(){
  const cfg = CFG.signals_configured || {};
  const hasMeltwater = (S.brand_mentions||[]).some(v=>v!=null&&v>0);
  const hasGSC       = (S.branded_search||[]).some(v=>v!=null&&v>0);
  const hasGA4       = (S.total_nonpaid_sessions||[]).some(v=>v!=null&&v>0);
  const hasAF        = (S.direct_installs||[]).some(v=>v!=null&&v>0);
  const hasSpend     = (S.brand_spend||[]).some(v=>v!=null&&v>0) || (S.perf_spend||[]).some(v=>v!=null&&v>0);
  const hasRevenue   = (S.revenue_india||[]).some(v=>v!=null&&v>0);
  const signals = [
    {label:'GSC',        live: hasGSC,       wait: !hasGSC && !!cfg.gsc},
    {label:'GA4',        live: hasGA4,        wait: !hasGA4 && !!cfg.ga4},
    {label:'AppsFlyer',  live: hasAF,         wait: !hasAF  && !!cfg.appsflyer},
    {label:'Spend',      live: hasSpend,      wait: !hasSpend},
    {label:'Revenue',    live: hasRevenue,    wait: !hasRevenue},
    {label:'Meltwater',  live: hasMeltwater,  wait: !hasMeltwater && !!cfg.meltwater},
  ];
  const el = document.getElementById('signalStatus');
  if(!el) return;
  el.innerHTML = signals.map(s=>{
    const cls = s.live ? 'live' : s.wait ? 'wait' : 'off';
    const label = s.live ? s.label : s.wait ? s.label+' ⏳' : s.label;
    return `<div class="sig-pill ${cls}"><span class="dot"></span>${label}</div>`;
  }).join('');
})();

// ── Signal card freshness tags
(function(){
  const today = new Date();
  today.setHours(0,0,0,0);

  function lastDate(arr){
    if(!arr) return null;
    for(let i=arr.length-1;i>=0;i--){
      if(arr[i]!=null && arr[i]>0) return (S.dates||[])[i];
    }
    return null;
  }

  function setFresh(id, dateStr){
    const el = document.getElementById(id);
    if(!el) return;
    if(!dateStr){ el.textContent='No data'; el.className='sfresh old'; return; }
    const d = new Date(dateStr+'T00:00:00');
    const days = Math.round((today-d)/(1000*60*60*24));
    el.textContent = 'Data to: '+dateStr+' ('+days+'d ago)';
    el.className = 'sfresh ' + (days<=2?'fresh':days<=5?'stale':'old');
  }

  setFresh('fr1', lastDate(S.branded_search));
  setFresh('fr2', lastDate(S.direct_installs));
  setFresh('fr3', lastDate(S.total_nonpaid_sessions));
  setFresh('fr4', lastDate(S.brand_mentions));
})();

// ── Signal 04 not-connected overlay
(function(){
  const hasData        = (S.brand_mentions||[]).some(v=>v!=null&&v>0);
  const isConfigured   = !!(CFG.signals_configured && CFG.signals_configured.meltwater);
  const nc = document.getElementById('c4_nc');
  if(!nc) return;
  if(hasData){
    nc.style.display = 'none';
  } else if(isConfigured){
    // Credentials set but no data yet — awaiting first fetch
    nc.style.display = 'flex';
    nc.innerHTML = '<div style="font-size:18px;">⏳</div><div style="font-size:12px;font-weight:600;color:#475569;">Awaiting Data</div><div style="font-size:10px;color:#94a3b8;">Run fetch_signals.py to pull</div>';
  } else {
    // Not configured
    nc.style.display = 'flex';
    nc.innerHTML = '<div style="font-size:18px;">🔌</div><div style="font-size:12px;font-weight:600;color:#475569;">Not Connected</div><div style="font-size:10px;color:#94a3b8;">Meltwater plugin required</div>';
  }
})();

// ── Tab switching
function switchTab(name){
  ['dashboard','intelligence'].forEach(t=>{
    const c=document.getElementById('tabContent-'+t);
    const b=document.getElementById('tab-btn-'+t);
    if(c) c.style.display = t===name?'block':'none';
    if(b) b.classList.toggle('active', t===name);
  });
  // Render correlation table when intelligence tab opens
  if(name==='intelligence' && currentSlice) renderCorrReportTable(currentSlice, gran);
}

// ── Auto-refresh countdown
function toggleAutoRefresh(){
  refreshOn=!refreshOn;
  document.getElementById('refreshToggle').classList.toggle('on',refreshOn);
  document.getElementById('refreshLabel').style.opacity=refreshOn?1:.4;
}
function tick(){
  if(!refreshOn){setTimeout(tick,1000);return;}
  countdown--;
  if(countdown<=0){window.location.reload();return;}
  const m=Math.floor(countdown/60),s=countdown%60;
  document.getElementById('countdown').textContent=m+':'+(s<10?'0':'')+s;
  setTimeout(tick,1000);
}
tick();

// ── City filter
(function buildCityBtns(){
  const wrap = document.getElementById('cityBtns');
  const note = document.getElementById('cityNote');
  if(!CITY_LIST.length){ note.textContent='Run backfill_ga4_cities.py to enable city filter'; return; }
  CITY_LIST.forEach(city=>{
    if(!city) return;  // skip null/undefined entries
    const b = document.createElement('button');
    b.className = 'city-btn';
    b.id = 'city-'+String(city).replace(/\s+/g,'-');
    b.textContent = city;
    b.onclick = () => setCity(city);
    wrap.appendChild(b);
  });
  note.textContent = 'Sessions only. Branded search & installs are India-level.';
})();

function setCity(city){
  activeCity = city;
  // Update button active states
  document.querySelectorAll('.city-btn').forEach(b=>b.classList.remove('active'));
  const btn = document.getElementById('city-'+(city==='all'?'all':city.replace(/\s+/g,'-')));
  if(btn) btn.classList.add('active');
  // Update city labels in chart subtitles
  const cityLabel = city==='all' ? 'All India' : city;
  ['sub_nonpaid_city','sub_paid_city','sub_brand_city'].forEach(id=>{
    const el=document.getElementById(id); if(el) el.textContent=cityLabel;
  });
  // Re-render with current slice
  applyView();
}

function getCitySessionArr(key){
  // key: 'direct' | 'total_paid' | 'total_nonpaid' | 'brand_paid' | 'blr_paid'
  const storeKey = CITY_KEY_TO_STORE[key] || key;
  if(activeCity==='all'){
    // Prefer top-level India array if it has real data
    const topArr = S[storeKey]||[];
    const hasData = topArr.some(v=>v!=null && v>0);
    if(hasData) return topArr;
    // Fallback: sum city_sessions across real cities (top cities only — partial coverage)
    return (S.dates||[]).map(d=>{
      const cd = CITY_SESSIONS[d];
      if(!cd) return null;
      let total=0, any=false;
      Object.entries(cd).forEach(([city,vals])=>{
        if(!NOISE_CITIES.includes(city) && vals[key]!=null){ total+=vals[key]; any=true; }
      });
      return any ? total : null;
    });
  }
  // Per-city view — read from city_sessions
  return (S.dates||[]).map(d=>{
    const cd = CITY_SESSIONS[d];
    return cd && cd[activeCity] ? (cd[activeCity][key]??null) : null;
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// INTELLIGENCE PANEL — renders narrative, alerts, action cards, digest, chat
// ══════════════════════════════════════════════════════════════════════════════
(function renderIntelligencePanel(){
  if(!ANALYSIS || !ANALYSIS.common_date) return;

  const A = ANALYSIS;

  // ── Common date badge ──────────────────────────────────────────────────────
  const badge = document.getElementById('commonDateBadge');
  if(badge) badge.textContent = 'Analysis to: ' + A.common_date;

  // ── Freshness sync bar ─────────────────────────────────────────────────────
  const fsBar = document.getElementById('freshnessSyncBar');
  if(fsBar && A.signal_freshness){
    const today2 = new Date(); today2.setHours(0,0,0,0);
    const cd = new Date(A.common_date+'T00:00:00');
    fsBar.innerHTML = Object.entries(A.signal_freshness).map(([key, info])=>{
      if(!info.date) return `<span style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;border-radius:10px;font-size:10px;background:#F3F4F6;color:#6B7280;"><span style="width:6px;height:6px;border-radius:50%;background:#9CA3AF;flex-shrink:0;display:inline-block;"></span>${info.label}: no data</span>`;
      const d = new Date(info.date+'T00:00:00');
      const daysStale = Math.round((today2-d)/(1000*60*60*24));
      const vsCommon  = Math.round((cd-d)/(1000*60*60*24));
      const color = daysStale<=2?'#16A34A':daysStale<=5?'#D97706':'#DC2626';
      const bg    = daysStale<=2?'#DCFCE7':daysStale<=5?'#FEF3C7':'#FEE2E2';
      const lag   = vsCommon>0?` (${vsCommon}d behind common)`:'';
      return `<span style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;border-radius:10px;font-size:10px;background:${bg};color:${color};font-weight:600;"><span style="width:6px;height:6px;border-radius:50%;background:${color};flex-shrink:0;display:inline-block;"></span>${info.label}: ${info.date}${lag}</span>`;
    }).join('');
  }

  // ── Narrative ──────────────────────────────────────────────────────────────
  const narEl = document.getElementById('narrativeText');
  if(narEl) narEl.textContent = A.narrative||'';
  const perEl = document.getElementById('narrativePeriod');
  if(perEl && A.curr_start) perEl.textContent = 'Analysis window: '+A.curr_start+' \u2192 '+A.common_date+' (4-week rolling, all-core-signal common date)';

  // ── Alerts ─────────────────────────────────────────────────────────────────
  const alertsEl = document.getElementById('alertsList');
  if(alertsEl){
    if(!A.alerts||!A.alerts.length){
      alertsEl.innerHTML = '<div style="font-size:12px;color:#16A34A;font-weight:600;">\u2713 No anomalies detected in the current window.</div>';
    } else {
      alertsEl.innerHTML = A.alerts.map(al=>{
        const bg  = al.level==='warn'?'#FEF3C7':'#EFF6FF';
        const col = al.level==='warn'?'#92400E':'#1E40AF';
        return `<div style="background:${bg};border-radius:8px;padding:10px 12px;margin-bottom:8px;">
          <div style="display:flex;align-items:center;gap:6px;font-size:12px;font-weight:700;color:${col};">
            <span>${al.icon}</span><span>${al.signal}</span>
            <span style="margin-left:auto;font-size:11px;">${al.msg}</span>
          </div>
          <div style="font-size:11px;color:${col};opacity:.8;margin-top:4px;">${al.sub}</div>
        </div>`;
      }).join('');
    }
  }

  // ── Action Cards ───────────────────────────────────────────────────────────
  const acEl = document.getElementById('actionCards');
  if(acEl && A.action_cards){
    const impactColor = {'high':'#DC2626','medium':'#D97706','low':'#16A34A'};
    acEl.innerHTML = A.action_cards.map((ac,i)=>{
      const steps = (ac.actions||[]).map(s=>`<li style="margin-bottom:4px;">${s}</li>`).join('');
      const ic = impactColor[ac.impact]||'#6B7280';
      return `<div style="background:#fff;border:1px solid var(--border);border-top:3px solid ${i===0?'#E8450A':'#1B2A3B'};border-radius:var(--r);padding:16px;">
        <div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:8px;">
          <span style="font-size:18px;">${ac.icon}</span>
          <div style="flex:1;">
            <div style="font-size:12px;font-weight:700;color:var(--text);">${ac.title}</div>
            <div style="display:flex;gap:6px;margin-top:4px;">
              <span style="font-size:9px;font-weight:700;padding:1px 6px;border-radius:6px;background:${ic}22;color:${ic};">Impact: ${ac.impact}</span>
              <span style="font-size:9px;font-weight:700;padding:1px 6px;border-radius:6px;background:#6B728022;color:#6B7280;">Effort: ${ac.effort}</span>
            </div>
          </div>
        </div>
        <div style="font-size:11px;color:var(--muted);margin-bottom:8px;">${ac.why}</div>
        <ul style="margin:0;padding-left:16px;font-size:11px;color:var(--text);line-height:1.6;">${steps}</ul>
      </div>`;
    }).join('');
  }

  // ── Chat window ────────────────────────────────────────────────────────────
  const qa = A.chat_qa || [];
  const chatSug = document.getElementById('chatSuggestions');
  const chatMsg = document.getElementById('chatMessages');

  function addMsg(text, isUser){
    if(!chatMsg) return;
    const div = document.createElement('div');
    div.style.cssText = isUser
      ? 'align-self:flex-end;background:#1B2A3B;color:#fff;border-radius:12px 12px 2px 12px;padding:8px 12px;font-size:12px;max-width:85%;line-height:1.5;'
      : 'align-self:flex-start;background:#F8F9FA;color:#1B2A3B;border-radius:12px 12px 12px 2px;padding:8px 12px;font-size:12px;max-width:95%;line-height:1.6;border:1px solid var(--border);';
    div.textContent = text;
    chatMsg.appendChild(div);
    chatMsg.scrollTop = chatMsg.scrollHeight;
  }

  function askQuestion(q, a){
    addMsg(q, true);
    setTimeout(()=>addMsg(a, false), 200);
  }

  // Chip groups — first 6 shown by default, toggle to show all
  const chipGroups = [
    { label: '📊 Campaign', indices: [0, 1] },
    { label: '📡 Signals',  indices: [2, 3, 4, 5] },
    { label: '₹ Spend',     indices: [6, 7] },
    { label: '📈 Revenue',  indices: [8, 9] },
    { label: '⏱ Timing',   indices: [10, 11] },
  ];

  if(chatSug){
    let html = '';
    chipGroups.forEach(grp=>{
      html += `<div style="width:100%;display:flex;align-items:center;gap:5px;flex-wrap:wrap;margin-bottom:2px;">
        <span style="font-size:9px;font-weight:700;color:var(--muted);white-space:nowrap;min-width:60px;">${grp.label}</span>`;
      grp.indices.forEach(i=>{
        if(!qa[i]) return;
        html += `<button onclick="chatSugClick(${i})" style="background:#F0F2F5;border:1px solid var(--border);border-radius:14px;padding:3px 9px;font-size:10px;cursor:pointer;color:var(--text);white-space:nowrap;">${qa[i].q}</button>`;
      });
      html += '</div>';
    });
    chatSug.innerHTML = html;
  }

  window.chatSugClick = function(i){
    if(!qa[i]) return;
    askQuestion(qa[i].q, qa[i].a);
  };

  // Initial greeting
  addMsg("Hi Aditya! Signal analysis loaded to "+A.common_date+". Click any question above — or type your own.", false);

  // Keyword matching — score each Q by keyword overlap, pick best match
  function findBestMatch(input){
    const ql = input.toLowerCase().replace(/[^a-z0-9\s]/g,'');
    // Keyword aliases for common brand terms
    const aliases = {
      'campaign':'bangalore working campaign',
      'working':'bangalore campaign working',
      'brand awareness':'branded search brand strength',
      'awareness':'branded search brand strength',
      'mentions':'brand mentions trending',
      'social':'brand mentions trending meltwater',
      'sentiment':'brand mentions trending negative',
      'installs':'installs lagging install recover',
      'organic installs':'installs lagging recover',
      'sessions':'non-paid sessions tell',
      'non paid':'non-paid sessions',
      'revenue':'revenue trend nmv',
      'nmv':'revenue trend nmv',
      'spend':'spend mix efficient',
      'budget':'spend mix brand efficient',
      'priority':'prioritise week',
      'prioritize':'prioritise week',
      'correlation':'correlation numbers read',
      'r value':'correlation numbers read',
      'when':'installs recover timing',
      'morning':'signal check morning',
      'first':'signal check morning',
      'today':'signal check morning prioritise',
    };
    // Expand query with aliases
    let expanded = ql;
    Object.entries(aliases).forEach(([k,v])=>{ if(ql.includes(k)) expanded += ' '+v; });

    let bestScore = 0, bestMatch = null;
    qa.forEach(item=>{
      const qwords = item.q.toLowerCase().replace(/[^a-z0-9\s]/g,'').split(/\s+/).filter(w=>w.length>3);
      const score = qwords.filter(w=>expanded.includes(w)).length;
      if(score > bestScore){ bestScore=score; bestMatch=item; }
    });
    return bestScore >= 2 ? bestMatch : null;
  }

  // Text input handler
  window.sendChatMsg = function(){
    const inp = document.getElementById('chatInput');
    if(!inp || !inp.value.trim()) return;
    const q = inp.value.trim();
    inp.value = '';
    addMsg(q, true);

    const match = findBestMatch(q);
    if(match){
      setTimeout(()=>addMsg(match.a, false), 300);
    } else {
      // Friendly fallback — show what topics are available
      setTimeout(()=>{
        addMsg("I don\u2019t have a specific answer for that. Here are the topics I can answer \u2014 click any:", false);
        setTimeout(()=>{
          const wrap = document.createElement('div');
          wrap.style.cssText = 'align-self:flex-start;display:flex;flex-wrap:wrap;gap:5px;max-width:95%;margin-top:2px;';
          qa.forEach((item,i)=>{
            const btn = document.createElement('button');
            btn.textContent = item.q;
            btn.style.cssText = 'background:#F0F2F5;border:1px solid var(--border);border-radius:14px;padding:3px 9px;font-size:10px;cursor:pointer;color:var(--text);';
            btn.onclick = ()=>{ addMsg(item.q,true); setTimeout(()=>addMsg(item.a,false),200); };
            wrap.appendChild(btn);
          });
          if(chatMsg){ chatMsg.appendChild(wrap); chatMsg.scrollTop=chatMsg.scrollHeight; }
        }, 100);
      }, 300);
    }
  };
})();

// ── Clip correlation chart to common date ─────────────────────────────────────
// (handled by renderCorrelation which uses the view slice — no extra clip needed;
//  the freshness bar and analysis panel make the date clearly visible)

// ── Initialise date pickers
// (allDates, minDate, maxDate, today, view90 declared at top of script)
document.getElementById('viewStart').value = view7;
document.getElementById('viewEnd').value   = maxDate;
document.getElementById('viewStart').min   = minDate;
document.getElementById('viewEnd').min     = minDate;
document.getElementById('viewStart').max   = maxDate;
document.getElementById('viewEnd').max     = maxDate;

['aStart','aEnd','bStart','bEnd'].forEach(id=>{
  document.getElementById(id).min=minDate;
  document.getElementById(id).max=maxDate;
});
document.getElementById('aStart').value=aStart0<minDate?minDate:aStart0;
document.getElementById('aEnd').value  =aEnd0<minDate?minDate:aEnd0;
document.getElementById('bStart').value=bStart0<minDate?minDate:bStart0;
document.getElementById('bEnd').value  =bEnd0;

// ── Granularity
function setGran(g){
  gran=g;
  ['D','W','M'].forEach(x=>document.getElementById('g'+x).classList.toggle('active',x===g));
  applyView(); // auto-rerender on toggle
}

// ── Slice store by date range
function sliceByDate(start,end){
  const si=allDates.findIndex(d=>d>=start);
  const ei=allDates.findLastIndex(d=>d<=end);
  if(si<0||ei<0||si>ei)return null;
  const sl=arr=>(arr||[]).slice(si,ei+1);
  const sc=key=>{
    const full=getCitySessionArr(key);
    return (full||[]).slice(si,ei+1);
  };
  const _total_paid = sc('total_paid');
  const _brand_paid = sc('brand_paid');
  // Derived: performance sessions = total paid minus brand paid
  const _perf_sessions = _total_paid.map((v,i)=>{
    const b=_brand_paid[i];
    return (v!=null && b!=null) ? Math.max(0, v-b) : (v!=null ? v : null);
  });
  return{
    dates:               allDates.slice(si,ei+1),
    branded_search:      sl(S.branded_search),
    total_installs:      sl(S.total_installs),
    direct_installs:     sl(S.direct_installs),   // organic installs (brand-driven)
    paid_installs:       sl(S.paid_installs),      // paid installs
    revenue_india:       sl(S.revenue_india),
    revenue_blr:         sl(S.revenue_blr),
    orders_blr:          sl(S.orders_blr),
    orders_india:        sl(S.orders_india),
    // All-traffic split
    total_nonpaid_sessions: sc('total_nonpaid'),
    total_paid_sessions:    _total_paid,
    // Paid sub-breakdowns
    brand_paid_sessions:    _brand_paid,
    perf_sessions:          _perf_sessions,
    blr_paid_sessions:      sc('blr_paid'),
    // Non-paid brand proxy
    direct_sessions:        sc('direct'),
    direct_new_users:    sl(S.direct_new_users),
    // Spend (from Unified Dashboard Google Sheet)
    brand_spend:         sl(S.brand_spend),
    perf_spend:          sl(S.perf_spend),
    brand_mentions:      sl(S.brand_mentions),
    negative_mentions:   sl(S.negative_mentions),
    sov_percent:         sl(S.sov_percent),
    negative_rate:       sl(S.negative_rate),
    competitor_huft:     sl(S.competitor_huft),
    competitor_wiggles:  sl(S.competitor_wiggles),
    competitor_petsutra: sl(S.competitor_petsutra),
  };
}

// ── Aggregate dates+values by granularity
// W/M use SUM; labels are human-readable absolute dates / ranges / month names
function fmtDay(d){ const dt=new Date(d+'T00:00:00'); return dt.getDate()+' '+MO[dt.getMonth()]; }

function aggregate(dates,values,g){
  if(g==='D') return{labels:dates.map(d=>fmtDay(d)),values,counts:values.map(v=>v!=null?1:0),sortKeys:dates};

  const grp={}; // sortKey → {label, vals[], count}
  dates.forEach((d,i)=>{
    let sortKey,label;
    if(g==='W'){
      const dt=new Date(d+'T00:00:00'),wd=dt.getDay();
      const mon=new Date(dt); mon.setDate(dt.getDate()-(wd===0?6:wd-1));
      const sun=new Date(mon); sun.setDate(mon.getDate()+6);
      sortKey=mon.toISOString().split('T')[0];
      const ms=mon.getDate(),me=sun.getDate();
      const mm=MO[mon.getMonth()],sm=MO[sun.getMonth()];
      label = mm===sm ? `${ms}–${me} ${mm}` : `${ms} ${mm}–${me} ${sm}`;
    } else {
      sortKey=d.substring(0,7);
      const dt=new Date(d+'T00:00:00');
      label=MO[dt.getMonth()]+' \''+String(dt.getFullYear()).slice(2);
    }
    if(!grp[sortKey])grp[sortKey]={label,vals:[],count:0};
    if(values[i]!=null){grp[sortKey].vals.push(values[i]); grp[sortKey].count++;}
  });
  const keys=Object.keys(grp).sort();
  const labels=keys.map(k=>grp[k].label);
  const counts=keys.map(k=>grp[k].count);
  const agg=keys.map(k=>{
    const v=grp[k].vals.filter(x=>x!=null);
    return v.length ? v.reduce((a,b)=>a+b,0) : null;
  });
  return{labels,values:agg,counts,sortKeys:keys};
}

// ── Campaign-start annotation plugin (registered once)
(function(){
  if(Chart.registry && Chart.registry.plugins && Chart.registry.plugins.get('csLine')) return;
  Chart.register({
    id:'csLine',
    afterDatasetsDraw(chart){
      const idx=chart.options._csIdx;
      if(idx==null||idx<0) return;
      const meta=chart.getDatasetMeta(0);
      if(!meta||!meta.data||!meta.data[idx]) return;
      const x=meta.data[idx].x;
      const {top,bottom}=chart.chartArea;
      const ctx2=chart.ctx;
      ctx2.save();
      ctx2.beginPath();
      ctx2.setLineDash([5,4]);
      ctx2.strokeStyle='#E8450A';
      ctx2.lineWidth=1.5;
      ctx2.globalAlpha=0.7;
      ctx2.moveTo(x,top);
      ctx2.lineTo(x,bottom);
      ctx2.stroke();
      ctx2.globalAlpha=1;
      ctx2.setLineDash([]);
      // Small label
      ctx2.fillStyle='#E8450A';
      ctx2.font='bold 9px sans-serif';
      ctx2.textAlign='center';
      ctx2.fillText('▲ Campaign', x, bottom+12);
      ctx2.restore();
    }
  });
})();

// ── Compute campaign-start bucket index from aggregate sortKeys
function csIdx(sortKeys, g){
  if(!cs || !sortKeys || !sortKeys.length) return -1;
  if(g==='D') return sortKeys.indexOf(cs);
  // For W/M: find which bucket cs falls into
  const csDate=new Date(cs+'T00:00:00');
  for(let i=0;i<sortKeys.length;i++){
    const bStart=new Date(sortKeys[i]+'T00:00:00');
    let bEnd;
    if(g==='W'){
      bEnd=new Date(bStart); bEnd.setDate(bStart.getDate()+6);
    } else {
      bEnd=new Date(bStart.getFullYear(), bStart.getMonth()+1, 0);
    }
    if(csDate>=bStart && csDate<=bEnd) return i;
  }
  return -1;
}

// ── Chart engine
function mkChart(id,labels,datasets,opts){
  if(CI[id]){CI[id].destroy();delete CI[id];}
  const el=document.getElementById(id); if(!el) return;
  const ctx=el.getContext('2d');
  const isCurrency=opts&&opts.currency;
  const annotation=opts&&opts.csIdx!=null?opts.csIdx:-1;
  CI[id]=new Chart(ctx,{
    type:'line',
    data:{labels,datasets},
    options:{
      responsive:true,maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      _csIdx: annotation,
      plugins:{
        legend:{display:datasets.length>1,position:'top',
                labels:{font:{size:10},boxWidth:10,padding:6}},
        tooltip:{callbacks:{
          label:c=>{
            const v=c.parsed.y;
            if(v==null) return c.dataset.label+': —';
            if(isCurrency) return c.dataset.label+': \u20B9'+Math.round(v).toLocaleString('en-IN');
            return c.dataset.label+': '+Math.round(v).toLocaleString('en-IN');
          }
        }}
      },
      scales:{
        x:{grid:{display:false},ticks:{font:{size:11,weight:'600'},maxRotation:45,maxTicksLimit:14}},
        y:{grid:{color:'#F3F4F6'},ticks:{
          font:{size:11,weight:'600'},beginAtZero:false,
          callback: isCurrency ? v=>(v>=100000?'\u20B9'+(v/100000).toFixed(1)+'L':v>=1000?'\u20B9'+(v/1000).toFixed(0)+'k':'\u20B9'+v) : v=>(v>=100000?(v/100000).toFixed(1)+'L':v>=1000?(v/1000).toFixed(0)+'k':v)
        }}
      }
    }
  });
}
function ds(data,label,color,bg,dash){
  return{label,data,borderColor:color,backgroundColor:bg,borderWidth:2,
         pointRadius:allDates.length>90?0:2.5,pointHoverRadius:5,pointBackgroundColor:color,
         fill:true,tension:0.35,borderDash:dash||[]};
}
function baseds(val,len){
  return{label:'Baseline',data:Array(len).fill(val),borderColor:GREY_C,borderWidth:1.5,
         borderDash:[5,4],pointRadius:0,fill:false,tension:0};
}

// ── Signal card update
function updateCard(vi,bli,di,bi,cmpi,avgVal,baseline,threshold){
  document.getElementById(vi).textContent=fmt(avgVal);
  document.getElementById(bli).textContent='Baseline: '+fmt(baseline);
  const p=pct(avgVal,baseline);
  const cls=dcls(p,threshold);
  const del=document.getElementById(di);
  del.textContent=p==null?'No data':(p>=0?'+':'')+p.toFixed(1)+'% vs baseline';
  del.className='sdelta '+cls;
  document.getElementById(bi).className='s-bar '+cls;
}

// ── Mini-sparklines for signal cards
const SPARK_CI={};
function renderSparkline(canvasId, arr, color){
  const el=document.getElementById(canvasId); if(!el) return;
  // Take last 28 daily values
  const vals=(arr||[]).slice(-28);
  const labels=vals.map((_,i)=>'');
  if(SPARK_CI[canvasId]){SPARK_CI[canvasId].destroy();delete SPARK_CI[canvasId];}
  SPARK_CI[canvasId]=new Chart(el.getContext('2d'),{
    type:'line',
    data:{labels,datasets:[{
      data:vals, borderColor:color, backgroundColor:color+'22',
      borderWidth:1.5, pointRadius:0, fill:true, tension:0.4
    }]},
    options:{
      responsive:true, maintainAspectRatio:false,
      animation:false,
      plugins:{legend:{display:false},tooltip:{enabled:false}},
      scales:{
        x:{display:false},
        y:{display:false,beginAtZero:false}
      }
    }
  });
}

// ── Main render
function renderDashboard(slice,g){
  if(!slice){console.warn('No data for selected range');return;}
  currentSlice=slice;

  // Signal cards — granularity-aware (latest period total vs scaled baseline)
  // granVal: get last non-null aggregated period total for given array + granularity
  function granVal(arr){
    const {values,counts}=aggregate(slice.dates,arr,g);
    // Require minimum days to consider a period complete
    const minDays = g==='W' ? 5 : g==='M' ? 20 : 1;
    for(let i=values.length-1;i>=0;i--){
      if(values[i]!=null&&values[i]>0&&counts[i]>=minDays) return values[i];
    }
    return null;
  }
  // granBase: scale a weekly baseline to match current granularity
  function granBase(wkBase){
    if(!wkBase) return 0;
    if(g==='D') return wkBase/7;
    if(g==='M') return wkBase*30/7;
    return wkBase; // W
  }
  // Unit label per granularity
  const unitSuffix = g==='D' ? 'day' : g==='W' ? 'week' : 'month';
  [['su1','impressions'],['su2','installs'],['su3','sessions'],['su4','mentions'],
   ['su_brand_spend','₹'],['su_perf_spend','₹']].forEach(([id,lbl])=>{
    const el=document.getElementById(id); if(el) el.textContent=lbl+'/'+unitSuffix;
  });

  const a1=granVal(slice.branded_search);
  const a2=granVal(slice.direct_installs);
  const a3=granVal(slice.total_nonpaid_sessions);
  const a4=granVal(slice.brand_mentions);
  updateCard('v1','bl1','d1','b1','cmp1',a1,granBase(baselines.branded_search_impressions),20);
  updateCard('v2','bl2','d2','b2','cmp2',a2,granBase(baselines.direct_installs_india||baselines.direct_installs_bangalore),15);
  updateCard('v3','bl3','d3','b3','cmp3',a3,granBase(baselines.total_nonpaid_sessions_india||0),20);
  updateCard('v4','bl4','d4','b4','cmp4',a4,granBase(baselines.brand_mentions||0),20);

  // ── BLR Orders top card ──────────────────────────────────────────────────
  {
    const BLR_ORD_BASE_WK = 691*7;  // March avg: 691/day → 4837/wk (granBase expects weekly)
    const ablr = granVal(slice.orders_blr);
    const blrBase = granBase(BLR_ORD_BASE_WK);
    updateCard('v_blr_orders_card','bl_blr_orders','d_blr_orders_card','b_blr_orders','cmp_blr_orders',ablr,blrBase,10);
    // Override unit label per granularity
    const suBlr = document.getElementById('su_blr_orders');
    if(suBlr) suBlr.textContent = 'orders/'+unitSuffix;
    // Freshness — last non-null date
    const blrDates = slice.dates.filter((_,i)=>slice.orders_blr[i]!=null);
    const frBlr = document.getElementById('fr_blr_orders');
    if(frBlr && blrDates.length) frBlr.textContent = 'Updated: '+blrDates[blrDates.length-1];
    renderSparkline('spark_blr_orders', slice.orders_blr, '#f97316');
  }

  // Spend cards
  const aBS = granVal(slice.brand_spend);
  const aPS = granVal(slice.perf_spend);
  const bsBase = granBase(baselines.gads_brand_spend_per_week||0);
  const psBase = granBase(baselines.perf_spend_per_week||0);
  if(aBS!=null){
    document.getElementById('v_brand_spend').textContent = '\u20B9'+Math.round(aBS).toLocaleString('en-IN');
    document.getElementById('bl_brand_spend').textContent = bsBase ? 'Baseline: \u20B9'+Math.round(bsBase).toLocaleString('en-IN') : 'No baseline set';
    const bsPct = bsBase ? pct(aBS, bsBase) : null;
    const bsEl = document.getElementById('d_brand_spend');
    if(bsPct!=null){ bsEl.textContent=(bsPct>=0?'+':'')+bsPct.toFixed(1)+'% vs baseline'; bsEl.className='sdelta '+dcls(bsPct,10); }
    else { bsEl.textContent='Tracking'; bsEl.className='sdelta grey'; }
    document.getElementById('b_brand_spend').className='s-bar '+(bsPct!=null?dcls(bsPct,10):'grey');
  }
  if(aPS!=null){
    document.getElementById('v_perf_spend').textContent = '\u20B9'+Math.round(aPS).toLocaleString('en-IN');
    document.getElementById('bl_perf_spend').textContent = psBase ? 'Baseline: \u20B9'+Math.round(psBase).toLocaleString('en-IN') : 'No baseline set';
    const psPct = psBase ? pct(aPS, psBase) : null;
    const psEl = document.getElementById('d_perf_spend');
    if(psPct!=null){ psEl.textContent=(psPct>=0?'+':'')+psPct.toFixed(1)+'% vs baseline'; psEl.className='sdelta '+dcls(psPct,10); }
    else { psEl.textContent='Tracking'; psEl.className='sdelta grey'; }
    document.getElementById('b_perf_spend').className='s-bar '+(psPct!=null?dcls(psPct,10):'grey');
  }
  if(aBS!=null && aPS!=null && (aBS+aPS)>0){
    const total = aBS + aPS;
    const bp = Math.round(aBS/total*100);
    const pp = 100-bp;
    document.getElementById('v_spend_ratio').innerHTML =
      '<span style="color:#7c3aed">'+bp+'%</span> : <span style="color:#dc2626">'+pp+'%</span>';
    document.getElementById('bl_spend_ratio').innerHTML =
      '<span style="color:#7c3aed">Brand \u20B9'+Math.round(aBS/1e5)/10+'L</span> \u00B7 <span style="color:#dc2626">Perf \u20B9'+Math.round(aPS/1e5)/10+'L</span>';
    document.getElementById('fr_spend_ratio').textContent = 'Total: \u20B9'+Math.round(total).toLocaleString('en-IN');
  }
  (function(){
    function lastSpendDate(arr){
      for(let i=(arr||[]).length-1;i>=0;i--){if(arr[i]!=null&&arr[i]>0)return (S.dates||[])[i];}
      return null;
    }
    function setSpendFresh(id, dateStr){
      const el=document.getElementById(id); if(!el) return;
      if(!dateStr){el.textContent='No data';el.className='sfresh old';return;}
      const today2=new Date(); today2.setHours(0,0,0,0);
      const d=new Date(dateStr+'T00:00:00');
      const days=Math.round((today2-d)/(1000*60*60*24));
      el.textContent='Data to: '+dateStr+' ('+days+'d ago)';
      el.className='sfresh '+(days<=2?'fresh':days<=5?'stale':'old');
    }
    setSpendFresh('fr_brand_spend', lastSpendDate(S.brand_spend));
    setSpendFresh('fr_perf_spend',  lastSpendDate(S.perf_spend));
  })();

  // Chart range labels
  const rng = fmtDay(slice.dates[0])+' → '+fmtDay(slice.dates[slice.dates.length-1]);
  document.getElementById('chartRangeLabel').textContent = rng;
  const pbl = document.getElementById('paidBreakLabel');
  if(pbl) pbl.textContent = rng;
  const rrl = document.getElementById('revRangeLabel');
  if(rrl) rrl.textContent = rng;

  // Charts — primary traffic split
  const charts=[
    {id:'ch1',        arr:slice.branded_search,           base:baselines.branded_search_impressions,
                      label:'Branded Search (impressions)',color:ORANGE},
    {id:'ch_nonpaid', arr:slice.total_nonpaid_sessions,   base:baselines.total_nonpaid_sessions_india||0,
                      label:'Non-Paid Sessions',           color:'#6366f1'},
    {id:'ch_paid',    arr:slice.total_paid_sessions,      base:baselines.total_paid_sessions_india||0,
                      label:'Paid Sessions',               color:'#ef4444'},
  ];
  charts.forEach(c=>{
    const {labels,values,sortKeys}=aggregate(slice.dates,c.arr,g);
    const color=c.color||ORANGE;
    const bg=color+'26';
    const scaledBase=granBase(c.base);
    mkChart(c.id,labels,[ds(values,c.label,color,bg),baseds(scaledBase,labels.length)],{csIdx:csIdx(sortKeys,g)});
  });

  // Installs chart — Total + Organic as two series
  {
    const {labels:iL, values:totV, sortKeys:iSK} = aggregate(slice.dates, slice.total_installs, g);
    const {values:orgV}                           = aggregate(slice.dates, slice.direct_installs, g);
    const orgBase = granBase(baselines.direct_installs_india||baselines.direct_installs_bangalore||0);
    mkChart('ch2', iL, [
      ds(totV, 'Total Installs',   '#f59e0b', '#f59e0b26'),
      ds(orgV, 'Organic Installs', '#fb923c', '#fb923c26'),
      baseds(orgBase, iL.length),
    ],{csIdx:csIdx(iSK,g)});
  }

  // Paid breakdown sub-charts (Brand)
  const paidBreak=[
    {id:'ch3', arr:slice.brand_paid_sessions, base:baselines.brand_paid_sessions_india||0,
               label:'Brand Campaign Sessions', color:'#8b5cf6'},
  ];
  paidBreak.forEach(c=>{
    const {labels,values,sortKeys}=aggregate(slice.dates,c.arr,g);
    const bg=c.color+'26';
    const scaledBase=granBase(c.base);
    mkChart(c.id,labels,[ds(values,c.label,c.color,bg),baseds(scaledBase,labels.length)],{csIdx:csIdx(sortKeys,g)});
  });

  // Revenue chart
  const {labels:revL, values:revV, sortKeys:revSK} = aggregate(slice.dates, slice.revenue_india, g);
  const revBase = g==='W' ? baselines.revenue_india_weekly||0
                : g==='M' ? (baselines.revenue_india_weekly||0)*30/7
                : baselines.revenue_india_daily||0;
  mkChart('ch_rev', revL, [ds(revV,'India NMV (\u20B9)','#14b8a6','#14b8a622'), baseds(revBase, revL.length)],{currency:true,csIdx:csIdx(revSK,g)});

  // India Orders chart + KPI cards (paired with India NMV)
  {
    const IND_ORD_BASE = 4097; // Mar 1-22 only (strict pre-WTF, gross)
    const {labels:ioL, values:ioV, sortKeys:ioSK} = aggregate(slice.dates, slice.orders_india, g);
    const ioBase = g==='W' ? IND_ORD_BASE*7 : g==='M' ? IND_ORD_BASE*30 : IND_ORD_BASE;
    mkChart('ch_ind_orders', ioL, [ds(ioV,'India Orders','#19be05','#19be0526'), baseds(ioBase, ioL.length)],{csIdx:csIdx(ioSK,g)});

    // KPI tile: shows AVERAGE of selected range (single day = absolute value)
    const indOrdVals = (slice.orders_india||[]).filter(v=>v!=null);
    const indRevVals = (slice.revenue_india||[]).filter(v=>v!=null);
    const indOrdAvg = indOrdVals.length ? indOrdVals.reduce((a,b)=>a+b,0)/indOrdVals.length : null;
    const indRevAvg = indRevVals.length ? indRevVals.reduce((a,b)=>a+b,0)/indRevVals.length : null;
    const indSingle = indOrdVals.length === 1;
    if(indOrdAvg!=null){
      const el = document.getElementById('v_ind_orders');
      if(el) el.textContent = Math.round(indOrdAvg).toLocaleString('en-IN');
      const dl = document.getElementById('d_ind_orders');
      if(dl){
        const pct = ((indOrdAvg/IND_ORD_BASE)-1)*100;
        dl.textContent = (pct>=0?'+':'')+pct.toFixed(1)+'% '+(indSingle?'vs baseline':'avg/day vs baseline');
        dl.className = 'sdelta '+(pct>=5?'green':pct<=-5?'red':'grey');
      }
    }
    if(indRevAvg!=null){
      const el = document.getElementById('v_ind_rev');
      if(el) el.textContent = '\u20B9'+(indRevAvg/100000).toFixed(1)+'L';
      const dl = document.getElementById('d_ind_rev');
      if(dl){
        const base = baselines.revenue_india_daily || 5687130;
        const pct = ((indRevAvg/base)-1)*100;
        dl.textContent = (pct>=0?'+':'')+pct.toFixed(1)+'% '+(indSingle?'vs baseline':'avg/day vs baseline');
        dl.className = 'sdelta '+(pct>=5?'green':pct<=-5?'red':'grey');
      }
    }
  }

  // ── BLR Orders + Revenue charts ──────────────────────────────────────────
  {
    // BLR baselines in GROSS units, computed from clean ex-WTF window
    // (Mar 1-22 + Apr 6-14, n=31 days). Recomputed 2026-05-04.
    const BLR_ORD_BASE = 753;     // Mar 1-22 only (gross)
    const BLR_REV_BASE = 1209000; // ₹12.09L/day, Mar 1-22 (gross)
    const blrOrl = document.getElementById('blrOrdRangeLabel');
    if(blrOrl) blrOrl.textContent = rng;

    // Orders chart
    const {labels:boL, values:boV, sortKeys:boSK} = aggregate(slice.dates, slice.orders_blr, g);
    const boBase = g==='W' ? BLR_ORD_BASE*7 : g==='M' ? BLR_ORD_BASE*30 : BLR_ORD_BASE;
    mkChart('ch_blr_orders', boL, [ds(boV,'BLR Orders','#f97316','#f9731626'), baseds(boBase, boL.length)],{csIdx:csIdx(boSK,g)});

    // Revenue chart
    const {labels:brL, values:brV, sortKeys:brSK} = aggregate(slice.dates, slice.revenue_blr, g);
    const brBase = g==='W' ? BLR_REV_BASE*7 : g==='M' ? BLR_REV_BASE*30 : BLR_REV_BASE;
    mkChart('ch_blr_rev', brL, [ds(brV,'BLR NMV (\u20B9)','#f59e0b','#f59e0b26'), baseds(brBase, brL.length)],{currency:true,csIdx:csIdx(brSK,g)});

    // KPI tile: shows AVERAGE of selected range (single day = absolute value)
    const blrOrdVals = slice.orders_blr.filter(v=>v!=null);
    const blrRevVals = slice.revenue_blr.filter(v=>v!=null);
    const blrOrdAvg = blrOrdVals.length ? blrOrdVals.reduce((a,b)=>a+b,0)/blrOrdVals.length : null;
    const blrRevAvg = blrRevVals.length ? blrRevVals.reduce((a,b)=>a+b,0)/blrRevVals.length : null;
    const blrSingle = blrOrdVals.length === 1;
    if(blrOrdAvg!=null){
      const el = document.getElementById('v_blr_orders');
      if(el) el.textContent = Math.round(blrOrdAvg).toLocaleString();
      const dl = document.getElementById('d_blr_orders');
      if(dl){
        const pct = ((blrOrdAvg/BLR_ORD_BASE)-1)*100;
        dl.textContent = (pct>=0?'+':'')+pct.toFixed(1)+'% '+(blrSingle?'vs baseline':'avg/day vs baseline');
        dl.className = 'sdelta '+(pct>=5?'green':pct<=-5?'red':'grey');
      }
    }
    if(blrRevAvg!=null){
      const el = document.getElementById('v_blr_rev');
      if(el) el.textContent = '\u20B9'+(blrRevAvg/1e5).toFixed(1)+'L';
      const dl = document.getElementById('d_blr_rev');
      if(dl){
        const pct = ((blrRevAvg/BLR_REV_BASE)-1)*100;
        dl.textContent = (pct>=0?'+':'')+pct.toFixed(1)+'% '+(blrSingle?'vs baseline':'avg/day vs baseline');
        dl.className = 'sdelta '+(pct>=5?'green':pct<=-5?'red':'grey');
      }
    }
  }

  // Sparklines — 28-day daily trend on signal cards
  renderSparkline('spark1',    slice.branded_search,        ORANGE);
  renderSparkline('spark2',    slice.direct_installs,       '#fb923c');
  renderSparkline('spark3',    slice.total_nonpaid_sessions,'#6366f1');
  renderSparkline('spark4',    slice.brand_mentions,        '#22c55e');
  // ── Negative mentions sub-line (inside brand mentions card) ────────────
  {
    const negMentions = granVal(slice.negative_mentions);
    const negRate     = slice.negative_rate.filter(x=>x!=null).slice(-1)[0];
    const vnm = document.getElementById('v_neg_mentions');
    if(vnm) vnm.textContent = negMentions!=null ? fmt(negMentions) : '—';
    const dnm = document.getElementById('d_neg_mentions');
    if(dnm){
      if(negRate!=null){
        const cls = negRate>=15?'red':negRate>=8?'yellow':'green';
        dnm.textContent = negRate.toFixed(1)+'% neg. rate';
        dnm.className = 'sdelta '+cls;
      } else {
        dnm.textContent = 'No data'; dnm.className = 'sdelta grey';
      }
    }
  }

  // Re-render the combo charts using current toggle states
  renderAllSessions(slice, g);
  renderInstSplit(slice, g);
  renderInstByCity(slice);
  renderBvP(slice, g);
  renderSpend(slice, g);
  renderCorrelation(slice, g);

  // Campaign breakdown (first fold, dashboard tab)
  renderCampaignBreakdown(slice);

  // If intelligence tab is open, re-render its correlation table too
  const intTab = document.getElementById('tabContent-intelligence');
  if(intTab && intTab.style.display !== 'none') renderCorrReportTable(slice, g);

  // SOV donut (latest data point in slice)
  renderSOV(slice);

}

// ── All Sessions combo chart ──────────────────────────────────────────────────
const SESS_DEFS = [
  {key:'total_nonpaid_sessions', label:'Non-Paid',    color:'#6366f1'},
  {key:'total_paid_sessions',    label:'Paid',        color:'#ef4444'},
  {key:'direct_sessions',        label:'Direct',      color:'#3b82f6'},
  {key:'brand_paid_sessions',    label:'Brand Paid',       color:'#8b5cf6'},
  {key:'total_installs',         label:'Total Installs',   color:'#f59e0b'},
  {key:'direct_installs',        label:'Organic Installs', color:'#fb923c'},
];

function toggleSess(btn){
  const key=btn.dataset.key;
  if(sessTog.has(key)){sessTog.delete(key);}else{sessTog.add(key);}
  btn.classList.toggle('active', sessTog.has(key));
  if(currentSlice) renderAllSessions(currentSlice, gran);
}

function renderAllSessions(slice, g){
  const datasets=[];
  let lastSK=null;
  SESS_DEFS.forEach(d=>{
    if(!sessTog.has(d.key)) return;
    const arr=slice[d.key]||[];
    const {labels,values,sortKeys}=aggregate(slice.dates, arr, g);
    datasets.push(ds(values, d.label, d.color, d.color+'22'));
    renderAllSessions._labels = labels;
    lastSK = sortKeys;
  });
  const labels = renderAllSessions._labels || slice.dates.map(d=>d.slice(5));
  mkChart('ch_all_sess', labels, datasets.length ? datasets : [{label:'No signals selected',data:[],borderColor:'transparent'}],
    {csIdx:csIdx(lastSK,g)});
}

// ── Brand vs Performance Sessions stacked bar ────────────────────────────────
function renderBvP(slice, g){
  const {labels, values: brandVals} = aggregate(slice.dates, slice.brand_paid_sessions, g);
  const {values: perfVals}          = aggregate(slice.dates, slice.perf_sessions, g);

  // Ratio display — latest complete period
  const lastBrand = [...brandVals].reverse().find(v=>v!=null&&v>0) || 0;
  const lastPerf  = [...perfVals].reverse().find(v=>v!=null&&v>0)  || 0;
  const total     = lastBrand + lastPerf;
  const el = document.getElementById('bvpRatioDisplay');
  if(el && total>0){
    const bp = Math.round(lastBrand/total*100);
    const pp = 100-bp;
    el.innerHTML = `Latest period split: <b style="color:#8b5cf6">${bp}% Brand</b> · <b style="color:#ef4444">${pp}% Performance</b>`;
  }

  const rangeEl = document.getElementById('bvpRangeLabel');
  if(rangeEl && labels.length) rangeEl.textContent = labels[0]+' – '+labels[labels.length-1];

  if(CI['ch_bvp']){CI['ch_bvp'].destroy(); delete CI['ch_bvp'];}
  const ctx = document.getElementById('ch_bvp').getContext('2d');
  CI['ch_bvp'] = new Chart(ctx,{
    type:'bar',
    data:{
      labels,
      datasets:[
        {label:'Brand Paid',   data:brandVals, backgroundColor:'#8b5cf6cc', borderColor:'#8b5cf6', borderWidth:1, stack:'paid'},
        {label:'Performance',  data:perfVals,  backgroundColor:'#ef4444cc', borderColor:'#ef4444', borderWidth:1, stack:'paid'},
      ]
    },
    options:{
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:true,position:'top',labels:{color:'rgba(10,10,10,.75)',font:{size:10},boxWidth:10,padding:8}},
        tooltip:{callbacks:{
          label:c=>`${c.dataset.label}: ${Math.round(c.raw||0).toLocaleString('en-IN')}`,
          footer:items=>{
            const t=items.reduce((s,i)=>s+(i.raw||0),0);
            if(!t) return '';
            const b=items.find(i=>i.dataset.label==='Brand Paid');
            const bp=b?Math.round((b.raw||0)/t*100):0;
            return `Brand ${bp}% · Perf ${100-bp}%`;
          }
        }}
      },
      scales:{
        x:{stacked:true,grid:{color:'rgba(10,10,10,.05)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},maxTicksLimit:14}},
        y:{stacked:true,grid:{color:'rgba(10,10,10,.07)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},callback:v=>v>=1000?(v/1000).toFixed(0)+'k':v}}
      }
    }
  });
}

// ── Organic vs Paid Installs stacked bar ─────────────────────────────────────
function renderInstSplit(slice, g){
  const {labels, values: orgVals} = aggregate(slice.dates, slice.direct_installs, g);
  const {values: paidVals}        = aggregate(slice.dates, slice.paid_installs, g);

  const lastOrg  = [...orgVals].reverse().find(v=>v!=null&&v>0)  || 0;
  const lastPaid = [...paidVals].reverse().find(v=>v!=null&&v>0) || 0;
  const total    = lastOrg + lastPaid;
  const el = document.getElementById('instSplitRatioDisplay');
  if(el && total>0){
    const op = Math.round(lastOrg/total*100);
    el.innerHTML = `Latest split: <b style="color:#22c55e">${op}% Organic</b> · <b style="color:#f59e0b">${100-op}% Paid</b>`;
  }
  const rangeEl = document.getElementById('instSplitRangeLabel');
  if(rangeEl && labels.length) rangeEl.textContent = labels[0]+' – '+labels[labels.length-1];

  if(CI['ch_inst_split']){CI['ch_inst_split'].destroy(); delete CI['ch_inst_split'];}
  const ctx = document.getElementById('ch_inst_split').getContext('2d');
  CI['ch_inst_split'] = new Chart(ctx,{
    type:'bar',
    data:{
      labels,
      datasets:[
        {label:'Organic',  data:orgVals,  backgroundColor:'#22c55ecc', borderColor:'#22c55e', borderWidth:1, stack:'inst'},
        {label:'Paid',     data:paidVals, backgroundColor:'#f59e0bcc', borderColor:'#f59e0b', borderWidth:1, stack:'inst'},
      ]
    },
    options:{
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:true,position:'top',labels:{color:'rgba(10,10,10,.75)',font:{size:10},boxWidth:10,padding:8}},
        tooltip:{callbacks:{
          label:c=>`${c.dataset.label}: ${Math.round(c.raw||0).toLocaleString('en-IN')}`,
          footer:items=>{
            const t=items.reduce((s,i)=>s+(i.raw||0),0);
            if(!t) return '';
            const o=items.find(i=>i.dataset.label==='Organic');
            const op=o?Math.round((o.raw||0)/t*100):0;
            return `Organic ${op}% · Paid ${100-op}%`;
          }
        }}
      },
      scales:{
        x:{stacked:true,grid:{color:'rgba(10,10,10,.05)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},maxTicksLimit:14}},
        y:{stacked:true,grid:{color:'rgba(10,10,10,.07)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},callback:v=>v>=1000?(v/1000).toFixed(0)+'k':v}}
      }
    }
  });
}

// ── Installs by City (horizontal stacked bar — period totals) ────────────────
function renderInstByCity(slice){
  const cityDaily = S.installs_city_daily || {};
  const dates = slice.dates || [];
  const totals = {};   // city → {organic, paid}
  for(const d of dates){
    const row = cityDaily[d];
    if(!row) continue;
    for(const [city, v] of Object.entries(row)){
      if(!totals[city]) totals[city] = {organic:0, paid:0};
      totals[city].organic += (v && v.organic) || 0;
      totals[city].paid    += (v && v.paid)    || 0;
    }
  }

  const TRACKED = ['Bangalore','Mumbai','Delhi','Chennai','Hyderabad','Pune','Kolkata','Ahmedabad'];
  const ordered = [];
  for(const c of TRACKED){
    if(totals[c] && (totals[c].organic||totals[c].paid)) ordered.push(c);
  }
  // Append any other resolved cities (Other / Unknown) at the end
  for(const c of Object.keys(totals)){
    if(!TRACKED.includes(c) && (totals[c].organic||totals[c].paid)) ordered.push(c);
  }

  const labels   = ordered;
  const orgVals  = ordered.map(c => totals[c].organic);
  const paidVals = ordered.map(c => totals[c].paid);

  const rangeEl = document.getElementById('instCityRangeLabel');
  if(rangeEl && dates.length) rangeEl.textContent = dates[0] + ' \u2013 ' + dates[dates.length-1];

  const noteEl = document.getElementById('instCityNoteDisplay');
  if(noteEl){
    if(!labels.length){
      noteEl.innerHTML = '<span style="color:#f59e0b">No city data — fill the Installs_Raw sheet</span>';
    } else {
      const totOrg = orgVals.reduce((a,b)=>a+b,0);
      const totPaid = paidVals.reduce((a,b)=>a+b,0);
      noteEl.innerHTML = 'Period totals: <b style="color:#22c55e">'+totOrg.toLocaleString('en-IN')+' Organic</b> \u00B7 <b style="color:#f59e0b">'+totPaid.toLocaleString('en-IN')+' Paid</b>';
    }
  }

  if(CI['ch_inst_city']){CI['ch_inst_city'].destroy(); delete CI['ch_inst_city'];}
  if(!labels.length) return;
  const ctx = document.getElementById('ch_inst_city').getContext('2d');
  CI['ch_inst_city'] = new Chart(ctx,{
    type:'bar',
    data:{
      labels,
      datasets:[
        {label:'Organic', data:orgVals,  backgroundColor:'#22c55ecc', borderColor:'#22c55e', borderWidth:1, stack:'inst'},
        {label:'Paid',    data:paidVals, backgroundColor:'#f59e0bcc', borderColor:'#f59e0b', borderWidth:1, stack:'inst'},
      ]
    },
    options:{
      indexAxis:'y',
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:true,position:'top',labels:{color:'rgba(10,10,10,.75)',font:{size:10},boxWidth:10,padding:8}},
        tooltip:{callbacks:{
          label:c=>`${c.dataset.label}: ${Math.round(c.raw||0).toLocaleString('en-IN')}`,
          footer:items=>{
            const t=items.reduce((s,i)=>s+(i.raw||0),0);
            if(!t) return '';
            const o=items.find(i=>i.dataset.label==='Organic');
            const op=o?Math.round((o.raw||0)/t*100):0;
            return `Organic ${op}% \u00B7 Paid ${100-op}% \u00B7 Total ${t.toLocaleString('en-IN')}`;
          }
        }}
      },
      scales:{
        x:{stacked:true,grid:{color:'rgba(10,10,10,.07)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},callback:v=>v>=1000?(v/1000).toFixed(1)+'k':v}},
        y:{stacked:true,grid:{color:'rgba(10,10,10,.05)'},ticks:{color:'rgba(10,10,10,.75)',font:{size:11}}}
      }
    }
  });
}

// ── Signal Correlation overlay (normalised to index 100 = baseline) ───────────
// ── Spend trend charts ────────────────────────────────────────────────────────
function renderSpend(slice, g){
  const hasSpend = (slice.brand_spend||[]).some(v=>v!=null) || (slice.perf_spend||[]).some(v=>v!=null);

  ['brand_spend','perf_spend'].forEach(key=>{
    const chartId = 'ch_'+key;
    const color = key==='brand_spend' ? '#7c3aed' : '#dc2626';
    const label = key==='brand_spend' ? 'Brand Spend (\u20B9)' : 'Perf Spend (\u20B9)';
    const arr = slice[key]||[];
    const {labels, values, sortKeys} = aggregate(slice.dates, arr, g);
    if(!hasSpend){ if(CI[chartId]){CI[chartId].destroy();delete CI[chartId];} return; }
    mkChart(chartId, labels, [ds(values, label, color, color+'22')], {currency:true, csIdx:csIdx(sortKeys,g)});
  });

  const {labels, values: brandVals} = aggregate(slice.dates, slice.brand_spend||[], g);
  const {values: perfVals}          = aggregate(slice.dates, slice.perf_spend||[], g);

  const lastBrand = [...brandVals].reverse().find(v=>v!=null&&v>0) || 0;
  const lastPerf  = [...perfVals].reverse().find(v=>v!=null&&v>0)  || 0;
  const totalSpend = lastBrand + lastPerf;
  const splitEl = document.getElementById('spendSplitDisplay');
  if(splitEl && totalSpend>0){
    const bp = Math.round(lastBrand/totalSpend*100);
    splitEl.innerHTML = 'Latest: <b style="color:#7c3aed">'+bp+'% Brand</b> \u00B7 <b style="color:#dc2626">'+(100-bp)+'% Perf</b>';
  }
  const rangeEl = document.getElementById('spendStackRangeLabel');
  if(rangeEl && labels.length) rangeEl.textContent = labels[0]+' \u2013 '+labels[labels.length-1];

  if(CI['ch_spend_stack']){CI['ch_spend_stack'].destroy(); delete CI['ch_spend_stack'];}
  const ctx = document.getElementById('ch_spend_stack').getContext('2d');
  CI['ch_spend_stack'] = new Chart(ctx,{
    type:'bar',
    data:{
      labels,
      datasets:[
        {label:'Brand Spend', data:brandVals, backgroundColor:'#7c3aedcc', borderColor:'#7c3aed', borderWidth:1, stack:'spend'},
        {label:'Perf Spend',  data:perfVals,  backgroundColor:'#dc2626cc', borderColor:'#dc2626', borderWidth:1, stack:'spend'},
      ]
    },
    options:{
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:true,position:'top',labels:{color:'rgba(10,10,10,.75)',font:{size:10},boxWidth:10,padding:8}},
        tooltip:{callbacks:{
          label:c=>c.dataset.label+': \u20B9'+Math.round(c.raw||0).toLocaleString('en-IN'),
          footer:items=>{
            const t=items.reduce((s,i)=>s+(i.raw||0),0);
            if(!t) return '';
            const b=items.find(i=>i.dataset.label==='Brand Spend');
            const bp=b?Math.round((b.raw||0)/t*100):0;
            return 'Total: \u20B9'+Math.round(t).toLocaleString('en-IN')+' \u00B7 Brand '+bp+'%';
          }
        }}
      },
      scales:{
        x:{stacked:true,grid:{color:'rgba(10,10,10,.05)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},maxTicksLimit:14}},
        y:{stacked:true,grid:{color:'rgba(10,10,10,.07)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},
           callback:v=>v>=1e6?'\u20B9'+(v/1e6).toFixed(1)+'M':v>=1e3?'\u20B9'+(v/1e3).toFixed(0)+'k':'\u20B9'+v}}
      }
    }
  });
}

const CORR_DEFS = [
  {key:'branded_search',      label:'Branded Search',   color:'#e8450a', base:'branded_search_impressions'},
  {key:'direct_sessions',     label:'Non-Paid Brand',   color:'#3b82f6', base:'direct_sessions_india'},
  {key:'brand_paid_sessions', label:'Brand Paid',       color:'#8b5cf6', base:'brand_paid_sessions_india'},
  {key:'perf_sessions',       label:'Performance',      color:'#ef4444', base:'total_paid_sessions_india'},
  {key:'direct_installs',     label:'Organic Installs', color:'#f59e0b', base:'direct_installs_india'},
  {key:'revenue_india',       label:'India NMV',        color:'#14b8a6', base:'revenue_india_daily'},
  {key:'brand_spend',         label:'Brand Spend',      color:'#7c3aed', base:'gads_brand_spend_per_week'},
  {key:'perf_spend',          label:'Perf Spend',       color:'#dc2626', base:'perf_spend_per_week'},
];

function toggleCorr(btn){
  const key=btn.dataset.key;
  if(corrTog.has(key)){corrTog.delete(key);}else{corrTog.add(key);}
  btn.classList.toggle('active', corrTog.has(key));
  if(currentSlice) renderCorrelation(currentSlice, gran);
}

function indexArr(values, base){
  // Normalise: index 100 = base. If no base, use mean of first 14 non-null values.
  let ref = base && baselines[base] ? baselines[base] : null;
  if(!ref){
    const nonnull = values.filter(v=>v!=null && v>0);
    ref = nonnull.slice(0, Math.min(14, nonnull.length)).reduce((a,b)=>a+b,0) / (Math.min(14, nonnull.length)||1);
  }
  if(!ref) return values.map(()=>null);
  return values.map(v=> v!=null ? Math.round(v/ref*100*10)/10 : null);
}

function renderCorrelation(slice, g){
  const datasets=[];
  let sharedLabels=null;
  let sharedSK=null;
  CORR_DEFS.forEach(d=>{
    if(!corrTog.has(d.key)) return;
    const arr=slice[d.key]||[];
    const {labels,values,sortKeys}=aggregate(slice.dates, arr, g);
    if(!sharedLabels){sharedLabels=labels; sharedSK=sortKeys;}
    const indexed = indexArr(values, d.base);
    const d2=ds(indexed, d.label, d.color, 'transparent');
    d2.fill=false; // no fill for overlay — just lines
    d2.borderWidth=2;
    datasets.push(d2);
  });
  const labels = sharedLabels || slice.dates.map(d=>d.slice(5));
  const annotIdx = csIdx(sharedSK, g);

  if(CI['ch_corr']){CI['ch_corr'].destroy();delete CI['ch_corr'];}
  const ctx=document.getElementById('ch_corr').getContext('2d');
  CI['ch_corr']=new Chart(ctx,{
    type:'line',
    data:{labels, datasets: datasets.length ? datasets : []},
    options:{
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      _csIdx: annotIdx,
      plugins:{
        legend:{display:true,position:'top',labels:{font:{size:10},boxWidth:10,padding:6}},
        tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.raw!=null?Math.round(c.raw):'—'}`}},
        annotation:{annotations:{
          baseline:{type:'line',yMin:100,yMax:100,borderColor:'rgba(10,10,10,.25)',
                    borderWidth:1.5,borderDash:[4,3],
                    label:{content:'Baseline = 100',enabled:true,position:'start',
                           color:'rgba(10,10,10,.45)',font:{size:11,weight:'600'}}}
        }}
      },
      scales:{
        x:{grid:{color:'rgba(10,10,10,.05)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},maxTicksLimit:12}},
        y:{grid:{color:'rgba(10,10,10,.07)'},ticks:{color:'rgba(10,10,10,.55)',font:{size:11,weight:'600'},
           callback:v=>v+''},
           title:{display:true,text:'Index (100 = baseline)',color:'rgba(10,10,10,.45)',font:{size:11,weight:'600'}}}
      }
    }
  });
}

function renderSOV(slice){
  const lastBM  = avg(slice.brand_mentions)||0;
  const lastHUFT= avg((S.competitor_huft||[]).slice(
    allDates.findIndex(d=>d>=slice.dates[0]),
    allDates.findLastIndex(d=>d<=slice.dates[slice.dates.length-1])+1))||0;
  const lastWig = avg((S.competitor_wiggles||[]).slice(
    allDates.findIndex(d=>d>=slice.dates[0]),
    allDates.findLastIndex(d=>d<=slice.dates[slice.dates.length-1])+1))||0;
  const lastPet = avg((S.competitor_petsutra||[]).slice(
    allDates.findIndex(d=>d>=slice.dates[0]),
    allDates.findLastIndex(d=>d<=slice.dates[slice.dates.length-1])+1))||0;
  const total=lastBM+lastHUFT+lastWig+lastPet;
  const pctOf=v=>total>0?(v/total*100).toFixed(1):'0.0';

  const sovColors=['#E8450A','#1B2A3B','#6B7280','#D1D5DB'];
  const sovLabels=['Supertails','HUFT','Wiggles','PetSutra'];
  const sovVals=[lastBM,lastHUFT,lastWig,lastPet];

  if(CI['sov']){CI['sov'].destroy();delete CI['sov'];}
  const ctx=document.getElementById('sovChart').getContext('2d');
  CI['sov']=new Chart(ctx,{type:'doughnut',
    data:{labels:sovLabels,datasets:[{data:sovVals,backgroundColor:sovColors,borderWidth:2,borderColor:'#fff'}]},
    options:{responsive:true,maintainAspectRatio:true,plugins:{
      legend:{display:false},
      tooltip:{callbacks:{label:c=>`${c.label}: ${pctOf(c.parsed)}%`}}
    }}
  });

  const leg=document.getElementById('sovLegend');
  leg.innerHTML=sovLabels.map((l,i)=>`
    <div class="leg-item">
      <div class="leg-dot" style="background:${sovColors[i]}"></div>
      <span class="leg-lbl">${l}</span><span class="leg-pct">${pctOf(sovVals[i])}%</span>
    </div>`).join('')+
    `<div style="margin-top:8px;font-size:10px;color:var(--muted)">Baseline SOV: ${baselines.sov_percent||'—'}%</div>`;

  // Sentiment bars (approximated from negative rate)
  const lastNeg=slice.negative_rate.filter(x=>x!=null).slice(-1)[0]||0;
  const posEst=Math.max(0,100-lastNeg-20), neutEst=Math.min(20,100-lastNeg-posEst);
  const bars=document.getElementById('sentBars');
  bars.innerHTML=[['Positive',posEst,'#16A34A'],['Neutral',neutEst,'#D97706'],['Negative',lastNeg,'#DC2626']]
    .map(([l,p,c])=>`<div class="sent-item">
      <div class="sent-lbl"><span>${l}</span><span>${p.toFixed(1)}%</span></div>
      <div class="sent-bg"><div class="sent-fill" style="width:${p}%;background:${c}"></div></div>
    </div>`).join('');
}

// ── Apply view range
function applyView(){
  const start=document.getElementById('viewStart').value;
  const end=document.getElementById('viewEnd').value;
  if(!start||!end||start>end){alert('Invalid date range.');return;}
  const slice=sliceByDate(start,end);
  renderDashboard(slice,gran);
  // If comparison active, re-apply
  if(document.getElementById('cmpBadge').style.display==='inline-block') applyComparison();
}

// ── Comparison
function applyComparison(){
  const aS=document.getElementById('aStart').value, aE=document.getElementById('aEnd').value;
  const bS=document.getElementById('bStart').value, bE=document.getElementById('bEnd').value;
  if(aS>aE||bS>bE){alert('End date must be after start date.');return;}

  const slA=sliceByDate(aS,aE), slB=sliceByDate(bS,bE);
  if(!slA||!slB){alert('No data for one or both periods.');return;}

  const maxLen=Math.max(slA.dates.length,slB.dates.length);
  const xLabels=Array.from({length:maxLen},(_,i)=>`Day ${i+1}`);

  // Comparison charts — primary
  const cmpCharts=[
    {id:'ch1',        kA:slA.branded_search,           kB:slB.branded_search},
    {id:'ch_nonpaid', kA:slA.total_nonpaid_sessions,   kB:slB.total_nonpaid_sessions},
    {id:'ch_paid',    kA:slA.total_paid_sessions,      kB:slB.total_paid_sessions},
    {id:'ch2',        kA:slA.direct_installs,           kB:slB.direct_installs},
    {id:'ch3',        kA:slA.brand_paid_sessions,       kB:slB.brand_paid_sessions},
  ];
  cmpCharts.forEach(c=>{
    const agA=aggregate(slA.dates,c.kA,gran), agB=aggregate(slB.dates,c.kB,gran);
    mkChart(c.id,xLabels.slice(0,Math.max(agA.labels.length,agB.labels.length)),[
      ds(agA.values,`A: ${aS} → ${aE}`,ORANGE,OBG),
      ds(agB.values,`B: ${bS} → ${bE}`,NAVY_L,NBG),
    ]);
  });

  // Card comparison strips
  const sigs=[
    {cid:'cmp1',kA:slA.branded_search,          kB:slB.branded_search,          th:20},
    {cid:'cmp2',kA:slA.direct_installs,          kB:slB.direct_installs,         th:15},
    {cid:'cmp3',kA:slA.total_nonpaid_sessions,   kB:slB.total_nonpaid_sessions,  th:20},
    {cid:'cmp4',kA:slA.brand_mentions,           kB:slB.brand_mentions,          th:20},
  ];
  sigs.forEach(s=>{
    const aV=avg(s.kA), bV=avg(s.kB), p=pct(bV,aV);
    const cls=dcls(p,s.th);
    const colors={green:'#16A34A',yellow:'#D97706',red:'#DC2626',grey:'#9CA3AF'};
    const bgs={green:'#DCFCE7',yellow:'#FEF3C7',red:'#FEE2E2',grey:'#F3F4F6'};
    const el=document.getElementById(s.cid);
    el.innerHTML=`<div class="scmp-row">
      <div class="cmp-v"><div class="dot dot-a"></div><span class="cmp-lbl">A</span>&nbsp;<span class="cmp-num">${fmt(aV)}</span></div>
      <div class="cmp-v"><div class="dot dot-b"></div><span class="cmp-lbl">B</span>&nbsp;<span class="cmp-num">${fmt(bV)}</span></div>
      ${p!=null?`<span class="cmp-d" style="color:${colors[cls]};background:${bgs[cls]}">${p>=0?'+':''}${p.toFixed(1)}%</span>`:''}
    </div>`;
    el.style.display='block';
  });

  // Summary bar
  document.getElementById('cmpSummary').style.display='block';
  document.getElementById('cmpGrid').innerHTML=[
    {l:'Branded Search',    kA:slA.branded_search,         kB:slB.branded_search,         th:20},
    {l:'Organic Installs',  kA:slA.direct_installs,        kB:slB.direct_installs,        th:15},
    {l:'Non-Paid Sessions', kA:slA.total_nonpaid_sessions, kB:slB.total_nonpaid_sessions, th:20},
    {l:'Paid Sessions',     kA:slA.total_paid_sessions,    kB:slB.total_paid_sessions,    th:20},
    {l:'Brand Paid',        kA:slA.brand_paid_sessions,    kB:slB.brand_paid_sessions,    th:20},
    {l:'India NMV (₹)',     kA:slA.revenue_india,          kB:slB.revenue_india,          th:10},
    {l:'Brand Mentions',    kA:slA.brand_mentions,         kB:slB.brand_mentions,         th:20},
  ].map(s=>{
    const aV=avg(s.kA),bV=avg(s.kB),p=pct(bV,aV);
    const cls=dcls(p,s.th);
    const colors={green:'#16A34A',yellow:'#D97706',red:'#DC2626',grey:'#9CA3AF'};
    const bgs={green:'#DCFCE7',yellow:'#FEF3C7',red:'#FEE2E2',grey:'#F3F4F6'};
    return `<div class="cmp-sum-item">
      <div class="lbl">${s.l}</div>
      <div class="vals">
        <span class="av">${fmt(aV)}</span><span style="color:var(--muted);font-size:11px">→</span>
        <span class="bv">${fmt(bV)}</span>
        ${p!=null?`<span class="chg" style="color:${colors[cls]};background:${bgs[cls]}">${p>=0?'+':''}${p.toFixed(1)}%</span>`:''}
      </div>
    </div>`;
  }).join('');

  document.getElementById('cmpBadge').style.display='inline-block';
}

function resetComparison(){
  ['cmp1','cmp2','cmp3','cmp4'].forEach(id=>{ document.getElementById(id).style.display='none'; });
  document.getElementById('cmpSummary').style.display='none';
  document.getElementById('cmpBadge').style.display='none';
  const start=document.getElementById('viewStart').value;
  const end=document.getElementById('viewEnd').value;
  renderDashboard(sliceByDate(start,end),gran);
}

// ── Activation log
const logBody=document.getElementById('logBody');
if(logBody){
  const tagMap={ooh:'t-ooh',auto:'t-auto',activation:'t-act',pr:'t-pr',festival:'t-fest'};
  (S.activation_log||[]).forEach(e=>{
    logBody.innerHTML+=`<tr><td>${e.date}</td><td>${e.event}</td>
      <td><span class="tag ${tagMap[e.type]||'t-ooh'}">${e.type.toUpperCase()}</span></td></tr>`;
  });
  if(!(S.activation_log||[]).length)
    logBody.innerHTML='<tr><td colspan="3" style="color:var(--muted);text-align:center;padding:18px">No activation events logged yet — add them in config.json</td></tr>';
}

// ── Campaign spend breakdown table — Brand only, Meta / Google classified
function renderCampaignBreakdown(slice){
  const el=document.getElementById('campaignBreakTable');
  if(!el) return;
  const lbl=document.getElementById('campBreakLabel');
  const campDaily=S.campaign_daily||{};
  const totals={};
  slice.dates.forEach(function(d){
    var dc=campDaily[d]; if(!dc) return;
    Object.entries(dc).forEach(function(pair){ totals[pair[0]]=(totals[pair[0]]||0)+pair[1]; });
  });
  if(lbl && slice.dates.length) lbl.textContent=slice.dates[0]+' \u2013 '+slice.dates[slice.dates.length-1];

  // Keep ONLY brand campaigns (contain 'brandmar'), sort by spend desc
  var brandCamps=Object.entries(totals).filter(function(p){
    return p[0].toLowerCase().indexOf('brandmar')>=0;
  }).sort(function(a,b){return b[1]-a[1];});

  if(!brandCamps.length){
    el.innerHTML='<div style="padding:14px 16px;font-size:12px;color:var(--muted);">No brand campaign spend data for this period. Data starts Feb 2026.</div>';
    return;
  }
  const total=brandCamps.reduce(function(s,p){return s+p[1];},0);
  // Totals by platform
  var metaTotal=0, googleTotal=0, otherTotal=0;
  var rows='';
  brandCamps.forEach(function(pair){
    var camp=pair[0], spend=pair[1];
    var p=total>0?spend/total*100:0;
    var cLow=camp.toLowerCase();
    var isMeta=cLow.indexOf('meta')>=0;
    var isGoogle=cLow.indexOf('google')>=0;
    var platform=isMeta?'Meta':isGoogle?'Google':'Other';
    var platBg=isMeta?'#EFF6FF':isGoogle?'#FFF7ED':'#F3F4F6';
    var platColor=isMeta?'#1d4ed8':isGoogle?'#c2410c':'#374151';
    if(isMeta) metaTotal+=spend; else if(isGoogle) googleTotal+=spend; else otherTotal+=spend;
    rows+='<tr>'+
      '<td style="padding:8px 10px;border-bottom:1px solid var(--border);">'+
        '<span class="camp-tag" style="background:'+platBg+';color:'+platColor+'">'+platform+'</span>'+
        '<span style="font-size:12px;">'+camp+'</span></td>'+
      '<td style="padding:8px 10px;border-bottom:1px solid var(--border);text-align:right;font-weight:600;">\u20B9'+Math.round(spend).toLocaleString('en-IN')+'</td>'+
      '<td style="padding:8px 10px;border-bottom:1px solid var(--border);text-align:right;color:var(--muted);font-size:11px;">'+p.toFixed(1)+'%</td>'+
      '<td style="padding:8px 10px;border-bottom:1px solid var(--border);width:140px;">'+
        '<div class="camp-bar" style="width:'+Math.max(2,Math.min(100,p))+'%;background:#7c3aed;"></div></td>'+
    '</tr>';
  });
  // Platform summary footer
  var footerParts=[];
  if(metaTotal>0) footerParts.push('<span style="display:inline-flex;align-items:center;gap:5px;"><span style="width:8px;height:8px;border-radius:2px;background:#1d4ed8;display:inline-block;"></span><b>Meta</b> \u20B9'+Math.round(metaTotal).toLocaleString('en-IN')+'</span>');
  if(googleTotal>0) footerParts.push('<span style="display:inline-flex;align-items:center;gap:5px;"><span style="width:8px;height:8px;border-radius:2px;background:#c2410c;display:inline-block;"></span><b>Google</b> \u20B9'+Math.round(googleTotal).toLocaleString('en-IN')+'</span>');
  if(otherTotal>0) footerParts.push('<span style="display:inline-flex;align-items:center;gap:5px;"><span style="width:8px;height:8px;border-radius:2px;background:#6b7280;display:inline-block;"></span><b>Other</b> \u20B9'+Math.round(otherTotal).toLocaleString('en-IN')+'</span>');
  rows+='<tr style="background:#f8fafc;">'+
    '<td style="padding:9px 10px;font-weight:700;font-size:12px;color:var(--navy);">'+
      'Total Brand Spend&nbsp;&nbsp;<span style="font-weight:400;font-size:11px;color:var(--muted);display:inline-flex;gap:14px;">'+footerParts.join('')+'</span></td>'+
    '<td style="padding:9px 10px;text-align:right;font-weight:700;">\u20B9'+Math.round(total).toLocaleString('en-IN')+'</td>'+
    '<td style="padding:9px 10px;text-align:right;color:var(--muted);">100%</td>'+
    '<td></td></tr>';
  el.innerHTML='<table class="camp-table"><thead><tr>'+
    '<th>Campaign</th>'+
    '<th style="text-align:right;">Brand Spend (\u20B9)</th>'+
    '<th style="text-align:right;">% of Total</th>'+
    '<th style="width:140px;">Share</th>'+
  '</tr></thead><tbody>'+rows+'</tbody></table>';
}

// ── Weekly correlation report table (Intelligence tab)
function renderCorrReportTable(slice, g){
  const el=document.getElementById('corrReportTable');
  if(!el) return;
  const wg='W'; // always weekly
  const weekData={};
  CORR_DEFS.forEach(d=>{
    const arr=slice[d.key]||[];
    const {labels,values}=aggregate(slice.dates, arr, wg);
    labels.forEach((l,i)=>{
      if(!weekData[l]) weekData[l]={};
      weekData[l][d.key]={raw:values[i], idx:indexArr([values[i]], d.base)[0]};
    });
  });
  const weeks=Object.keys(weekData).sort();
  if(!weeks.length){el.innerHTML='<div style="padding:14px;font-size:12px;color:var(--muted);">No data for selected period.</div>';return;}
  const hdr=`<table style="width:100%;border-collapse:collapse;font-size:11px;">
    <thead><tr>
      <th style="text-align:left;padding:8px 10px;background:#f8fafc;font-size:10px;font-weight:700;letter-spacing:.6px;color:var(--muted);border-bottom:1px solid var(--border);white-space:nowrap;position:sticky;left:0;">Week</th>
      ${CORR_DEFS.map(d=>`<th style="text-align:right;padding:8px 10px;background:#f8fafc;font-size:10px;font-weight:700;letter-spacing:.6px;color:${d.color};border-bottom:1px solid var(--border);white-space:nowrap;">${d.label}</th>`).join('')}
    </tr></thead>
    <tbody>${weeks.map((w,wi)=>{
      const isAlt=wi%2===0;
      const rowBg=isAlt?'#ffffff':'#fafafa';
      return '<tr style="background:'+rowBg+'">'+
        '<td style="padding:7px 10px;border-bottom:1px solid var(--border);font-weight:600;white-space:nowrap;position:sticky;left:0;background:'+rowBg+';">'+w+'</td>'+
        CORR_DEFS.map(d=>{
          const v=weekData[w]&&weekData[w][d.key];
          const raw=v?v.raw:null;
          const idx=v?v.idx:null;
          const idxColor=idx==null?'var(--muted)':idx>=115?'#16a34a':idx>=95?'var(--text)':'#dc2626';
          return '<td style="text-align:right;padding:7px 10px;border-bottom:1px solid var(--border);">'+
            '<div style="font-weight:600;">'+(raw!=null?Math.round(raw).toLocaleString('en-IN'):'—')+'</div>'+
            '<div style="font-size:9px;color:'+idxColor+';font-weight:700;">'+(idx!=null?'idx:'+idx:'—')+'</div>'+
          '</td>';
        }).join('')+
      '</tr>';
    }).join('')}
    </tbody>
  </table>`;
  el.innerHTML=hdr;
}

// ── Download correlation data as CSV
function downloadCorrCSV(){
  const slice=currentSlice;
  if(!slice){alert('Select a date range and click Apply first.');return;}
  const wg='W';
  const weekData={};
  CORR_DEFS.forEach(d=>{
    const arr=slice[d.key]||[];
    const {labels,values}=aggregate(slice.dates, arr, wg);
    labels.forEach((l,i)=>{
      if(!weekData[l]) weekData[l]={};
      weekData[l][d.key]={raw:values[i], idx:indexArr([values[i]], d.base)[0]};
    });
  });
  const weeks=Object.keys(weekData).sort();
  const rawHeader='Week,'+CORR_DEFS.map(d=>d.label+' (raw)').join(',');
  const idxHeader=','+CORR_DEFS.map(d=>d.label+' (idx)').join(',');
  const rows=[rawHeader+idxHeader];
  weeks.forEach(w=>{
    const raw=CORR_DEFS.map(d=>{const v=weekData[w]&&weekData[w][d.key];return v&&v.raw!=null?Math.round(v.raw):'';}).join(',');
    const idx=CORR_DEFS.map(d=>{const v=weekData[w]&&weekData[w][d.key];return v&&v.idx!=null?v.idx:'';}).join(',');
    rows.push(w+','+raw+','+idx);
  });
  const blob=new Blob([rows.join('\n')],{type:'text/csv'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;
  a.download='supertails_weekly_correlation_'+new Date().toISOString().slice(0,10)+'.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── Initial render (last 90 days)
renderDashboard(sliceByDate(view7,maxDate),gran);
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# GENERATE DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

def generate_analysis(store, config):
    """
    Generates the intelligence analysis for the dashboard.
    Returns a dict with: common_date, signal_freshness, narrative, alerts, action_cards, digest, chat_context, chat_qa
    All analysis is based on the 'common date' — last date where all core signals have data.
    """
    from datetime import date as _date, timedelta as _td
    import statistics

    dates = store.get("dates", [])
    if not dates:
        return {"common_date": None, "signal_freshness": {}, "narrative": "No data available yet.", "alerts": [], "action_cards": [], "digest": {}, "chat_context": "", "chat_qa": []}

    baselines = config.get("baselines", {})
    BS_WK  = baselines.get("branded_search_impressions", 9044)
    DI_WK  = baselines.get("direct_installs_india", 6440)
    BP_WK  = baselines.get("brand_paid_sessions_india", 162760)
    REV_WK = baselines.get("revenue_india_weekly", 39809911)
    # daily equivalents
    BS_DAY  = BS_WK / 7
    DI_DAY  = DI_WK / 7
    BP_DAY  = BP_WK / 7
    REV_DAY = REV_WK / 7

    # ── Signal freshness ──────────────────────────────────────────────────────
    signal_meta = [
        ("branded_search",      "GSC",          "Branded Search"),
        ("direct_installs",     "AppsFlyer",    "Organic Installs"),
        ("brand_paid_sessions", "GA4",          "Brand Paid Sessions"),
        ("revenue_india",       "Revenue MCP",  "India NMV"),
        ("brand_spend",         "Spend Sheet",  "Brand Spend"),
        ("perf_spend",          "Spend Sheet",  "Perf Spend"),
    ]
    freshness = {}
    for sig, source, label in signal_meta:
        arr = store.get(sig, [])
        for i in range(len(arr) - 1, -1, -1):
            if arr[i] is not None and arr[i] > 0:
                freshness[sig] = {"date": dates[i] if i < len(dates) else None, "source": source, "label": label}
                break
        if sig not in freshness:
            freshness[sig] = {"date": None, "source": source, "label": label}

    # ── Common date = min of core signal fresh dates ──────────────────────────
    core_keys = ["branded_search", "direct_installs", "brand_paid_sessions", "revenue_india"]
    core_fresh = [freshness[k]["date"] for k in core_keys if freshness[k]["date"]]
    if not core_fresh:
        return {"common_date": None, "signal_freshness": freshness, "narrative": "Insufficient data for analysis.", "alerts": [], "action_cards": [], "digest": {}, "chat_context": "", "chat_qa": []}
    common_date = min(core_fresh)

    # ── Windowed helpers ──────────────────────────────────────────────────────
    cd = _date.fromisoformat(common_date)
    curr_start = (cd - _td(days=27)).isoformat()   # 4 weeks ending common_date
    prev_end   = (cd - _td(days=28)).isoformat()   # 4 weeks before that
    prev_start = (cd - _td(days=55)).isoformat()

    def window_vals(sig, s, e):
        arr = store.get(sig, [])
        return [arr[i] for i, d in enumerate(dates)
                if s <= d <= e and i < len(arr) and arr[i] is not None and arr[i] > 0]

    def wk_avg(vals):
        """Convert daily values to weekly total equivalent."""
        if not vals: return None
        return sum(vals) / len(vals) * 7

    def pct(curr, base):
        if curr is None or not base: return None
        return (curr - base) / base * 100

    def fmt_pct(p, plus=True):
        if p is None: return "—"
        sign = "+" if p >= 0 else ""
        return f"{sign}{p:.1f}%"

    def fmt_num(n, precision=0):
        if n is None: return "—"
        return f"{n:,.{precision}f}"

    # ── Compute metrics ───────────────────────────────────────────────────────
    # Current 4-week window
    bs_c  = wk_avg(window_vals("branded_search",      curr_start, common_date))
    di_c  = wk_avg(window_vals("direct_installs",     curr_start, common_date))
    bp_c  = wk_avg(window_vals("brand_paid_sessions", curr_start, common_date))
    rv_c  = wk_avg(window_vals("revenue_india",       curr_start, common_date))
    ps_c  = wk_avg(window_vals("perf_spend",          curr_start, common_date))
    bms_c = wk_avg(window_vals("brand_spend",         curr_start, common_date))

    # Prev 4-week window
    bs_p  = wk_avg(window_vals("branded_search",      prev_start, prev_end))
    di_p  = wk_avg(window_vals("direct_installs",     prev_start, prev_end))
    bp_p  = wk_avg(window_vals("brand_paid_sessions", prev_start, prev_end))
    rv_p  = wk_avg(window_vals("revenue_india",       prev_start, prev_end))

    # Deltas vs baseline
    bs_vs_bl  = pct(bs_c,  BS_WK)
    di_vs_bl  = pct(di_c,  DI_WK)
    bp_vs_bl  = pct(bp_c,  BP_WK)
    rv_vs_bl  = pct(rv_c,  REV_WK)

    # WoW / period-over-period
    bs_pop = pct(bs_c, bs_p)
    di_pop = pct(di_c, di_p)
    bp_pop = pct(bp_c, bp_p)
    rv_pop = pct(rv_c, rv_p)

    # Spend mix (use available spend data, whatever window it's in)
    all_bs  = window_vals("brand_spend", "2020-01-01", common_date)
    all_ps  = window_vals("perf_spend",  "2020-01-01", common_date)
    brand_spend_day = sum(all_bs[-28:]) / len(all_bs[-28:]) if all_bs else None
    perf_spend_day  = sum(all_ps[-28:]) / len(all_ps[-28:]) if all_ps else None
    total_spend_day = (brand_spend_day or 0) + (perf_spend_day or 0)
    brand_mix_pct   = (brand_spend_day / total_spend_day * 100) if total_spend_day > 0 else None

    # WoW variance for anomaly detection (last 7 days vs 7 days before)
    last7_start  = (cd - _td(days=6)).isoformat()
    prev7_start  = (cd - _td(days=13)).isoformat()
    prev7_end    = (cd - _td(days=7)).isoformat()

    bs_7  = wk_avg(window_vals("branded_search",      last7_start, common_date))
    bs_7p = wk_avg(window_vals("branded_search",      prev7_start, prev7_end))
    di_7  = wk_avg(window_vals("direct_installs",     last7_start, common_date))
    di_7p = wk_avg(window_vals("direct_installs",     prev7_start, prev7_end))
    rv_7  = wk_avg(window_vals("revenue_india",       last7_start, common_date))
    rv_7p = wk_avg(window_vals("revenue_india",       prev7_start, prev7_end))

    bs_wow  = pct(bs_7, bs_7p)
    di_wow  = pct(di_7, di_7p)
    rv_wow  = pct(rv_7, rv_7p)

    # ── Narrative ─────────────────────────────────────────────────────────────
    campaign_start = config.get("campaign", {}).get("start_date", "2026-04-15")
    days_since_launch = (cd - _date.fromisoformat(campaign_start)).days if campaign_start else None
    phase_str = f"Day {days_since_launch} of the Bangalore campaign" if days_since_launch and days_since_launch >= 0 else "Pre-campaign baseline period"

    def signal_sentence(name, curr, baseline, pop):
        delta_bl  = pct(curr, baseline)
        if curr is None: return f"{name} data not yet available for this period."
        if delta_bl is None:
            return f"{name} is tracking at {fmt_num(curr, 0)}/wk."
        dir_bl = "above" if delta_bl >= 0 else "below"
        pop_str = f", {fmt_pct(pop)} vs the prior 4 weeks" if pop is not None else ""
        strength = "significantly" if abs(delta_bl) > 20 else "modestly" if abs(delta_bl) > 8 else "roughly in line"
        return f"{name} is {strength} {dir_bl} baseline at {fmt_pct(delta_bl)}{pop_str}."

    bs_sentence  = signal_sentence("Branded search", bs_c,  BS_WK,  bs_pop)
    di_sentence  = signal_sentence("Organic installs", di_c, DI_WK, di_pop)
    bp_sentence  = signal_sentence("Brand paid sessions", bp_c, BP_WK, bp_pop)
    rv_sentence  = signal_sentence("India NMV", rv_c, REV_WK, rv_pop)

    # Cross-signal divergence commentary
    divergence_notes = []
    if bs_vs_bl is not None and di_vs_bl is not None:
        diff = bs_vs_bl - di_vs_bl
        if diff > 15:
            divergence_notes.append(
                f"There is a notable divergence: branded search is outpacing organic installs by {diff:.0f} percentage points vs baseline. "
                "This may reflect a 2–3 day lag in app store conversion, or friction in the app store funnel."
            )
        elif di_vs_bl > bs_vs_bl + 15:
            divergence_notes.append(
                "Organic installs are outpacing branded search growth — a positive sign that word-of-mouth or direct navigation is amplifying search intent."
            )

    spend_note = ""
    if brand_mix_pct is not None:
        if brand_mix_pct < 15:
            spend_note = (
                f"Spend mix is heavily performance-weighted ({brand_mix_pct:.0f}% brand vs {100-brand_mix_pct:.0f}% performance). "
                "Given the Bangalore OOH campaign is live, there may be headroom to increase brand investment to amplify offline reach."
            )
        elif brand_mix_pct > 40:
            spend_note = (
                f"Brand spend represents {brand_mix_pct:.0f}% of total — a strong brand investment posture that should compound into organic signal growth over the next 2–3 weeks."
            )

    narrative_parts = [
        f"Analysis window: {curr_start} → {common_date} ({phase_str}).",
        bs_sentence, di_sentence, bp_sentence, rv_sentence,
    ]
    if divergence_notes:
        narrative_parts.extend(divergence_notes)
    if spend_note:
        narrative_parts.append(spend_note)

    narrative = " ".join(narrative_parts)

    # ── Alerts ────────────────────────────────────────────────────────────────
    alerts = []
    ALERT_THRESH = 15  # % WoW change to flag

    def make_alert(signal_label, wow_pct, vs_bl_pct, direction="down"):
        if wow_pct is None: return None
        if direction == "down" and wow_pct < -ALERT_THRESH:
            return {
                "level": "warn",
                "signal": signal_label,
                "msg": f"Down {abs(wow_pct):.0f}% vs prior week",
                "sub": f"{fmt_pct(vs_bl_pct)} vs baseline. Check for data freshness issues or real signal drop.",
                "icon": "↓"
            }
        elif direction == "up" and wow_pct > ALERT_THRESH * 2:
            return {
                "level": "info",
                "signal": signal_label,
                "msg": f"Up {wow_pct:.0f}% vs prior week",
                "sub": f"Strong momentum: {fmt_pct(vs_bl_pct)} vs baseline.",
                "icon": "↑"
            }
        return None

    for sig_label, wow, vs_bl in [
        ("Branded Search", bs_wow, bs_vs_bl),
        ("Organic Installs", di_wow, di_vs_bl),
        ("India NMV", rv_wow, rv_vs_bl),
    ]:
        a = make_alert(sig_label, wow, vs_bl, "down")
        if a: alerts.append(a)
        a = make_alert(sig_label, wow, vs_bl, "up")
        if a: alerts.append(a)

    # Divergence alert
    if bs_vs_bl is not None and di_vs_bl is not None and (bs_vs_bl - di_vs_bl) > 15:
        alerts.append({
            "level": "warn",
            "signal": "Search → Install Gap",
            "msg": f"Branded search leading installs by {bs_vs_bl - di_vs_bl:.0f}pp vs baseline",
            "sub": "App store conversion may be lagging. Check Play Store / App Store listing conversion rate.",
            "icon": "⚡"
        })

    if brand_mix_pct is not None and brand_mix_pct < 12:
        alerts.append({
            "level": "info",
            "signal": "Spend Mix",
            "msg": f"Brand spend only {brand_mix_pct:.0f}% of total outlay",
            "sub": "Performance-heavy mix may limit long-term brand compounding during the OOH campaign window.",
            "icon": "₹"
        })

    # ── Action Cards ──────────────────────────────────────────────────────────
    actions = []

    # 1. If branded search up but installs lagging → app store action
    if bs_vs_bl is not None and di_vs_bl is not None and bs_vs_bl > 10 and di_vs_bl < bs_vs_bl - 10:
        actions.append({
            "priority": 1,
            "title": "Fix Search → Install Leakage",
            "why": f"Branded search is {fmt_pct(bs_vs_bl)} vs baseline but organic installs are only {fmt_pct(di_vs_bl)}. Intent is being created but not converting.",
            "actions": [
                "Check Play Store and App Store listing conversion rate (install page CTR).",
                "A/B test app store screenshots and description copy for Bangalore audience.",
                "Ensure brand campaign UTMs are correctly attributing installs.",
            ],
            "impact": "high",
            "effort": "medium",
            "icon": "🔧"
        })

    # 2. Spend efficiency / brand mix
    if brand_mix_pct is not None and brand_mix_pct < 15:
        actions.append({
            "priority": 2,
            "title": "Rebalance Spend Mix for Campaign Amplification",
            "why": f"Brand spend is only {brand_mix_pct:.0f}% of total. With OOH live in Bangalore, increasing brand digital spend now amplifies the offline signal.",
            "actions": [
                f"Consider increasing brand search budget by 20–30% in Bangalore-targeted campaigns.",
                "Allocate a portion of performance budget to branded keywords to capture OOH-primed searches.",
                "Monitor brand impression share — aim to defend 90%+ in Bangalore.",
            ],
            "impact": "high",
            "effort": "low",
            "icon": "📈"
        })

    # 3. Revenue is positive but brand sessions massively up → efficiency check
    if bp_vs_bl is not None and rv_vs_bl is not None and bp_vs_bl > 100 and rv_vs_bl < bp_vs_bl / 3:
        actions.append({
            "priority": 3,
            "title": "Audit Brand Campaign Efficiency",
            "why": f"Brand paid sessions are {fmt_pct(bp_vs_bl)} vs baseline but revenue is only {fmt_pct(rv_vs_bl)}. Session growth is not translating proportionally.",
            "actions": [
                "Review conversion rate on brand campaign landing pages.",
                "Check if brand campaign traffic is landing on correct category pages.",
                "Segment brand campaign performance by city to identify BLR vs rest-of-India split.",
            ],
            "impact": "medium",
            "effort": "low",
            "icon": "🔍"
        })

    # 4. Continue / maintain if things are good
    if bs_vs_bl is not None and bs_vs_bl > 15 and rv_vs_bl is not None and rv_vs_bl > 10:
        actions.append({
            "priority": len(actions) + 1,
            "title": "Maintain Campaign Momentum — Week 2 Check",
            "why": f"Core signals are healthy: branded search {fmt_pct(bs_vs_bl)} vs baseline, revenue {fmt_pct(rv_vs_bl)} vs baseline.",
            "actions": [
                "Hold current OOH and digital placements through week 2.",
                "Begin tracking Bangalore-specific NMV delta vs non-Bangalore cities as the install lag resolves.",
                "Schedule a post-campaign decay check 2 weeks after OOH ends.",
            ],
            "impact": "medium",
            "effort": "low",
            "icon": "✅"
        })

    actions.sort(key=lambda a: a["priority"])

    # ── Weekly Digest ─────────────────────────────────────────────────────────
    def status(vs_bl, threshold=10):
        if vs_bl is None: return "no_data"
        if vs_bl >= threshold: return "on_track"
        if vs_bl >= 0: return "watch"
        return "off_track"

    digest = {
        "period": f"{curr_start} → {common_date}",
        "generated_at": str(_date.today()),
        "signals": [
            {"name": "Branded Search",     "current": f"{fmt_num(bs_c, 0)}/wk",  "vs_baseline": fmt_pct(bs_vs_bl), "vs_prev": fmt_pct(bs_pop), "status": status(bs_vs_bl)},
            {"name": "Organic Installs",   "current": f"{fmt_num(di_c, 0)}/wk",  "vs_baseline": fmt_pct(di_vs_bl), "vs_prev": fmt_pct(di_pop), "status": status(di_vs_bl, 5)},
            {"name": "Brand Paid Sessions","current": f"{fmt_num(bp_c, 0)}/wk",  "vs_baseline": fmt_pct(bp_vs_bl), "vs_prev": fmt_pct(bp_pop), "status": status(bp_vs_bl, 15)},
            {"name": "India NMV",          "current": f"₹{fmt_num(rv_c, 0)}/wk", "vs_baseline": fmt_pct(rv_vs_bl), "vs_prev": fmt_pct(rv_pop), "status": status(rv_vs_bl)},
        ],
        "top_concern": alerts[0]["signal"] if alerts else None,
        "top_opportunity": actions[0]["title"] if actions else None,
    }

    # ── Chat Context (compact data summary for the Q&A engine) ───────────────
    chat_context = (
        f"Supertails brand signal dashboard — Analysis as of {common_date}.\n"
        f"4-week window: {curr_start} → {common_date}.\n"
        f"Baselines (weekly): branded search {BS_WK:,} impressions | organic installs {DI_WK:,} | brand paid sessions {BP_WK:,} | NMV ₹{REV_WK:,}.\n"
        f"Current (weekly): branded search {fmt_num(bs_c,0)} ({fmt_pct(bs_vs_bl)} vs baseline) | "
        f"organic installs {fmt_num(di_c,0)} ({fmt_pct(di_vs_bl)} vs baseline) | "
        f"brand paid sessions {fmt_num(bp_c,0)} ({fmt_pct(bp_vs_bl)} vs baseline) | "
        f"NMV ₹{fmt_num(rv_c,0)} ({fmt_pct(rv_vs_bl)} vs baseline).\n"
        f"Period-over-period: branded search {fmt_pct(bs_pop)} | installs {fmt_pct(di_pop)} | revenue {fmt_pct(rv_pop)}.\n"
        f"Spend mix: brand ₹{fmt_num(brand_spend_day,0)}/day ({f'{brand_mix_pct:.0f}' if brand_mix_pct is not None else 'N/A'}% of total) | perf ₹{fmt_num(perf_spend_day,0)}/day.\n"
        f"Key concern: {alerts[0]['signal'] + ' — ' + alerts[0]['msg'] if alerts else 'No major anomalies'}.\n"
        f"Campaign: Bangalore offline launch ~Apr 15 2026."
    )

    # ── Pre-generated Q&A ─────────────────────────────────────────────────────
    _gap = round((bs_vs_bl or 0) - (di_vs_bl or 0))
    chat_qa = [
        # ── Campaign performance ──────────────────────────────────────────────
        {
            "q": "Is the Bangalore campaign working?",
            "a": (
                f"Early signals look {'encouraging' if (bs_vs_bl or 0) > 5 else 'mixed'}. "
                f"Branded search is {fmt_pct(bs_vs_bl)} above baseline — this is your earliest digital proof that offline advertising (OOH, autos, clinics) is generating brand recall. "
                f"Revenue is {fmt_pct(rv_vs_bl)} vs baseline. "
                "Remember: the digital signals are All-India. The true Bangalore-specific lift will be clearest once you compare Bangalore NMV against the national trend — "
                "look for Bangalore revenue growing faster than the India average starting 5–7 days post-launch. The campaign went live April 15; expect the strongest signal window Apr 17–25."
            )
        },
        {
            "q": "What should I prioritise this week?",
            "a": (
                f"Top priorities this week: "
                f"(1) {actions[0]['title'] if actions else 'Monitor campaign signals daily'} — {actions[0]['why'] if actions else 'watch for lift vs baseline'}. "
                f"(2) {actions[1]['title'] if len(actions)>1 else 'Check spend mix'} — {actions[1]['why'] if len(actions)>1 else 'ensure brand is adequately funded'}. "
                f"(3) Re-export AppsFlyer CSVs with the City dimension enabled — this unlocks Bangalore-specific install data, which is the missing link in the four-signal framework."
            )
        },
        # ── Signal explanations ───────────────────────────────────────────────
        {
            "q": "What does branded search tell us about brand strength?",
            "a": (
                f"Branded search impressions (from Google Search Console) are the first signal to move when offline advertising works. "
                f"Currently {fmt_pct(bs_vs_bl)} vs the Jan–Mar 2026 baseline of {BS_WK:,} impressions/week. "
                "When someone sees a Supertails billboard or auto wrap and doesn't immediately download the app, they often Google the brand later — that's what this captures. "
                "A sustained +15% for 2+ weeks means the campaign is building durable brand memory, not just a burst. "
                f"The historical lag to installs is ~2 days, so branded search today predicts installs by {common_date[:7]}-end."
            )
        },
        {
            "q": "Why are installs lagging behind branded search?",
            "a": (
                f"Branded search is {fmt_pct(bs_vs_bl)} vs baseline while organic installs are {fmt_pct(di_vs_bl)} — "
                f"a gap of {abs(_gap)} percentage points. "
                + ("This gap is within the normal 2-day lag window — installs should catch up shortly. " if _gap < 15 else
                   "This gap is wider than the usual 2-day lag. Three possible causes: "
                   "(1) App store listing friction — someone searches 'supertails' and lands on the Play Store page but doesn't install; check the listing conversion rate. "
                   "(2) Attribution — some installs may be tagged as 'paid' rather than 'organic' due to media source tags. "
                   "(3) Audience mismatch — OOH may be reaching a slightly older demographic that searches but is slower to install. ")
                + "Check again in 3 days; if the gap persists above 15 points, it's worth auditing the App Store listing."
            )
        },
        {
            "q": "What does non-paid sessions tell us?",
            "a": (
                f"Non-paid sessions (from GA4) are currently {fmt_pct(bp_vs_bl)} vs baseline. "
                "This captures website visits from Organic Search, Direct, Referral, and Organic Social — everything that doesn't require you to pay for the click. "
                "A rise here without a corresponding paid spend increase is the clearest signal of genuine brand pull: people are actively seeking out Supertails rather than being pushed by ads. "
                "Watch for sustained lift over 5–7 days. A spike that drops within 48 hours is campaign noise; one that holds is brand equity building."
            )
        },
        {
            "q": "How are brand mentions trending?",
            "a": (
                "Brand mentions (from Meltwater) track daily Supertails mentions across Instagram, X, Reddit, and LinkedIn — including the #DanishSait content and #SupertailsBangalore hashtag. "
                "A spike in mentions post-April 15 means the OOH and auto campaign is generating social conversation. "
                "The negative sub-line shows the % of mentions that are negative in sentiment. Healthy is below 8%. 8–15% is watch territory. Above 15% needs CX escalation. "
                "Compare the mention trend to branded search — they should move together if content is amplifying the offline campaign effectively."
            )
        },
        # ── Spend & efficiency ────────────────────────────────────────────────
        {
            "q": "How is the spend mix?",
            "a": (
                f"Current spend: brand ₹{fmt_num(brand_spend_day,0)}/day ({f'{brand_mix_pct:.0f}' if brand_mix_pct is not None else 'N/A'}% of total) "
                f"| perf ₹{fmt_num(perf_spend_day,0)}/day. "
                "Binet & Field benchmarks suggest 40–60% brand investment for sustained growth in a category like pet care. "
                "With OOH live in Bangalore, the brand digital budget should be amplifying the offline awareness you're generating — "
                "capturing search intent from people who saw the billboard but didn't immediately install. "
                + ("The current mix is heavily performance-weighted. This is mining past equity, not building new equity. "
                   "Consider temporarily shifting 10–15% of perf budget to brand campaigns in BLR." if brand_mix_pct and brand_mix_pct < 25 else
                   "The current mix looks reasonable for the campaign phase.")
            ) if brand_mix_pct else "Connect the Google Sheet to see live spend data. The sheet is linked — run python3 fetch_signals.py to pull the latest."
        },
        {
            "q": "Is our brand spend efficient?",
            "a": (
                f"Brand spend efficiency = how much branded search lift you get per ₹ of brand spend. "
                f"Currently: branded search is {fmt_pct(bs_vs_bl)} vs baseline | brand spend ₹{fmt_num(brand_spend_day,0)}/day. "
                "True brand ROI in a campaign like this isn't purely CPC — it's the downstream NMV lift per ₹ invested in brand awareness. "
                f"NMV is currently {fmt_pct(rv_vs_bl)} vs baseline. "
                "With a 3–7 day revenue lag, the full efficiency picture won't be visible until week 2 of the campaign. "
                "Track the branded search/₹ ratio weekly and compare to the pre-campaign baseline period (Jan 5–Mar 22)."
            ) if brand_mix_pct else "Spend data is needed to calculate efficiency. Run python3 fetch_signals.py to pull the latest from the Google Sheet."
        },
        # ── Revenue & baselines ───────────────────────────────────────────────
        {
            "q": "What is the revenue trend?",
            "a": (
                f"India NMV for the current 4-week window is ₹{fmt_num(rv_c,0)}/week, "
                f"{fmt_pct(rv_vs_bl)} vs the pre-campaign baseline of ₹{REV_WK:,.0f}/week. "
                f"Week-over-week: {fmt_pct(rv_pop)}. "
                "Revenue is the last signal to move — it lags branded search by 3–7 days and installs by 2–4 days. "
                "Don't judge campaign effectiveness on week-1 NMV. The baseline excludes the WTF Sale (Mar 23–Apr 5) to avoid inflated comps. "
                "A sustained +10% over 3 consecutive weeks post-April 15 would be strong proof of campaign contribution."
            )
        },
        {
            "q": "How do I read the correlation numbers?",
            "a": (
                "The weekly correlation report shows the Pearson r between each signal and Bangalore NMV. "
                "r=0.92 (composite signal → NMV) means 92% of weekly NMV variance is explained by the four-signal index — a very strong relationship for a marketing signal. "
                "r=0.55 (branded search → installs, 2-day lag) is a moderate-strong lead relationship. "
                "How to use this: when branded search rises, installs should follow in 2 days. When installs rise, NMV should follow in 3–5 days. "
                "If a signal rises but the downstream signal doesn't follow within the expected lag window, there's a conversion bottleneck worth investigating."
            )
        },
        # ── Timing & forecasting ──────────────────────────────────────────────
        {
            "q": "When will installs recover?",
            "a": (
                f"Branded search leads organic installs by ~2 days (r=0.55 from historical data). "
                f"The campaign went live April 15. If branded search lifted from that date, expect the first install bump by April 17–18. "
                f"The most recent data in this dashboard is to {common_date}. "
                "If you're seeing branded search up but installs flat beyond 4 days, check two things: "
                "(1) App Store and Play Store listing — is the install page converting? "
                "(2) AppsFlyer attribution — are organic installs being credited correctly or tagged as campaign-driven?"
            )
        },
        {
            "q": "Which signal should I check first each morning?",
            "a": (
                "Morning signal priority order: "
                "(1) Branded Search (GSC) — first mover; up = campaign is generating brand recall. "
                "(2) Brand Mentions (Meltwater) — real-time social pulse; check for negative spikes. "
                "(3) Non-Paid Sessions (GA4) — confirms web interest without ad dependency. "
                "(4) Organic Installs (AppsFlyer) — 2-day lag from search; confirms funnel is converting. "
                "(5) India NMV — 3–7 day lag; the final proof point. "
                "If signals 1–2 are green but 4–5 are lagging, it's expected in week 1. If all five are flat by day 7, re-examine campaign placements and creative."
            )
        },
    ]

    return {
        "common_date": common_date,
        "signal_freshness": freshness,
        "curr_start": curr_start,
        "narrative": narrative,
        "alerts": alerts,
        "action_cards": actions,
        "digest": digest,
        "chat_context": chat_context,
        "chat_qa": chat_qa,
    }


def generate_dashboard(store, config, output_path="supertails_dashboard.html"):
    campaign = config.get("campaign", {})
    store_out = dict(store)
    store_out["campaign_phase"]       = campaign.get("phase", "01")
    store_out["campaign_phase_label"] = campaign.get("phase_label", "OOH + Auto Live")
    store_out["campaign_start"]       = campaign.get("start_date", "")
    store_out["activation_log"]       = campaign.get("activation_log", [])
    # City data — pass through (may be empty dict if backfill_ga4_cities.py hasn't run)
    store_out["city_sessions"]        = store.get("city_sessions", {})
    # Filter out noise/unresolved cities from the filter bar
    noise_cities = {"(not set)", "Ashburn", "(not set)"}
    store_out["city_list"]            = [c for c in store.get("city_list", []) if c not in noise_cities]
    # Campaign-level spend breakdown
    store_out["campaign_daily"]       = store.get("campaign_daily", {})

    # Check which signals are configured (credentials present)
    mw     = config.get("meltwater", {})
    gsc    = config.get("google_search_console", {})
    ga4    = config.get("ga4", {})
    af     = config.get("appsflyer", {})
    mw_key = mw.get("api_key", "")
    mw_id  = mw.get("search_ids", {}).get("brand_campaign_master", "")

    config_out = {
        "baselines": config.get("baselines", {}),
        "signals_configured": {
            "meltwater":  bool(mw_key  and not mw_key.startswith("YOUR_")  and mw_id and not mw_id.startswith("SEARCH_ID")),
            "gsc":        bool(gsc.get("service_account_key_path") and gsc.get("site_url")),
            "ga4":        bool(ga4.get("property_id")),
            "appsflyer":  bool(af.get("api_token") and not str(af.get("api_token","")).startswith("YOUR_"))
                          or bool(any(v for v in (store.get("direct_installs") or []) if v)),
        }
    }

    # Generate intelligence analysis
    analysis     = generate_analysis(store, config)
    analysis_json = json.dumps(analysis, default=str)

    store_json  = json.dumps(store_out,  default=str)
    config_json = json.dumps(config_out, default=str)

    html = DASHBOARD_HTML \
        .replace("__STORE_DATA__",    store_json) \
        .replace("__CONFIG_DATA__",   config_json) \
        .replace("__ANALYSIS_DATA__", analysis_json)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(output_path) // 1024
    print(f"\n✅  Dashboard → {output_path}  ({size_kb} KB)")
    print(f"    Data: {store.get('dates', [''])[0]}  →  {store.get('last_fetched_to','—')}")
    print(f"    Days in store: {len(store.get('dates', []))}")


# ─────────────────────────────────────────────────────────────────────────────
# WATCH MODE
# ─────────────────────────────────────────────────────────────────────────────

def merge_revenue_sidecar(store):
    """
    Picks up revenue_update.json if present (written by Claude via MCP fetch),
    merges into store, then renames the file so it isn't re-applied next run.
    This is how Supertails NMV data — which comes from a remote MCP the Python
    script can't call directly — gets into the store automatically.
    """
    sidecar = os.path.join(os.path.dirname(os.path.abspath(__file__)), "revenue_update.json")
    if not os.path.exists(sidecar):
        return store
    try:
        payload = json.load(open(sidecar))
        rev_data = payload.get("revenue_india", {})
        dates = store.get("dates", [])
        rev_arr = store.get("revenue_india", [None] * len(dates))
        updated = 0
        for d, v in rev_data.items():
            if d in dates:
                i = dates.index(d)
                if rev_arr[i] is None or rev_arr[i] == 0:
                    rev_arr[i] = int(v)
                    updated += 1
            # If date not in store yet, it will be added on next full fetch
        store["revenue_india"] = rev_arr
        fetched = payload.get("_fetched", "?")
        print(f"  [Rev] Revenue sidecar applied — {updated} days updated (fetched {fetched})")
        # Rename to .applied so it won't re-run but is preserved for audit
        os.rename(sidecar, sidecar.replace(".json", ".applied.json"))
    except Exception as e:
        print(f"  [Rev] Could not apply revenue sidecar: {e}")
    return store


def run_once(config, full_refresh, output):
    store = load_store()
    # Apply any pending revenue MCP update from sidecar file
    store = merge_revenue_sidecar(store)
    start, end = get_fetch_range(store, config, full_refresh)
    if start is None:
        print("  Already current to t-1. Regenerating dashboard from store...")
        # Spend is a full-history pull — always re-fetch regardless of date range
        print("\n  [5/5] Spend (Google Sheet — always refreshed)")
        spend_daily = fetch_spend_daily(config)
        if spend_daily:
            store = merge_into_store(store, spend_daily)
            existing_cd = store.get("campaign_daily", {})
            for d, v in spend_daily.items():
                camps = v.get("campaigns", {})
                if camps:
                    existing_cd[d] = camps
            store["campaign_daily"] = existing_cd

        # Meltwater — always re-fetch last 30 days to overwrite any stale zeros
        mw_cfg = config.get("meltwater", {})
        mw_key = mw_cfg.get("api_key", "")
        if mw_key and not mw_key.startswith("YOUR_"):
            mw_end   = yesterday()
            mw_start = (date.fromisoformat(mw_end) - timedelta(days=29)).isoformat()
            print(f"\n  [MW] Meltwater — refreshing last 30 days ({mw_start} → {mw_end})")
            soc_daily = fetch_social_daily(config, mw_start, mw_end)
            if soc_daily:
                store = merge_into_store(store, soc_daily)

        save_store(store)
    else:
        store = fetch_and_merge(config, store, start, end)
        save_store(store)
    generate_dashboard(store, config, output)
    netlify_deploy(output)
    return store

def watch_loop(config, interval_minutes, output):
    print(f"\n{'='*56}")
    print(f"  Supertails · Dashboard Watch Mode")
    print(f"{'='*56}")
    print(f"  Refresh interval : {interval_minutes} min")
    print(f"  Output           : {output}")
    print(f"  Keep this terminal open.")
    print(f"  Open {output} in your browser — it auto-refreshes.\n")
    while True:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] Refreshing...")
        try:
            run_once(config, full_refresh=False, output=output)
            print(f"  Next refresh in {interval_minutes} min. (Ctrl+C to stop)")
        except Exception as e:
            print(f"  ❌  Error: {e}")
        time.sleep(interval_minutes * 60)


# ─────────────────────────────────────────────────────────────────────────────
# NETLIFY AUTO-DEPLOY
# ─────────────────────────────────────────────────────────────────────────────

def netlify_deploy(output_path="supertails_dashboard.html"):
    """Auto-push the dashboard to GitHub, which triggers Netlify auto-deploy.

    Requires:
      • The folder is a git repo (git init done once)
      • A remote 'origin' pointing to GitHub (git remote add done once)
      • Netlify connected to that GitHub repo (done once in Netlify UI)

    After setup, every run of fetch_signals.py auto-deploys — no extra steps.
    """
    if not shutil.which("git"):
        return  # git not available — skip silently

    # Check if we're inside a git repo
    check = subprocess.run(["git", "rev-parse", "--git-dir"],
                           capture_output=True, text=True)
    if check.returncode != 0:
        return  # Not a git repo yet — skip silently

    # Check if a remote is configured
    remotes = subprocess.run(["git", "remote"],
                              capture_output=True, text=True).stdout.strip()
    if not remotes:
        return  # No remote configured yet — skip silently

    print("\n📤  Pushing dashboard to GitHub → Netlify will auto-deploy...", flush=True)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    def run_git(cmd):
        r = subprocess.run(cmd, capture_output=True, text=True)
        combined = (r.stdout + r.stderr).strip()
        return r.returncode, combined

    # Stage files
    files_to_stage = [output_path, "data_store.json", "config.json", "fetch_signals.py"]
    files_to_stage = [f for f in files_to_stage if os.path.exists(f)]
    code, out = run_git(["git", "add"] + files_to_stage)
    if code != 0:
        print(f"⚠️  git add failed:\n    {out}"); return

    # Commit
    code, out = run_git(["git", "commit", "--no-verify", "-m", f"update {ts}"])
    if code != 0:
        if "nothing to commit" in out:
            print("  (no changes — dashboard already up to date on GitHub)"); return
        print(f"⚠️  git commit failed:\n    {out}"); return

    # Push
    code, out = run_git(["git", "push"])
    if code != 0:
        print(f"⚠️  git push failed:\n    {out}"); return

    print("✅  Pushed to GitHub. Netlify will deploy in ~30 seconds.")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Supertails Offline Campaign Dashboard v3")
    p.add_argument("--demo",             action="store_true", help="365-day sample data, no API keys")
    p.add_argument("--watch",            action="store_true", help="Keep running, refresh on interval")
    p.add_argument("--interval",         type=int, default=60, help="Watch refresh interval in minutes (default 60)")
    p.add_argument("--full-refresh",     action="store_true", help="Re-fetch entire 12-month history")
    p.add_argument("--test-meltwater",   action="store_true", help="Diagnose Meltwater API connection and print raw response")
    p.add_argument("--config",           default="config.json")
    p.add_argument("--output",           default="supertails_dashboard.html")
    args = p.parse_args()

    print(f"\n{'='*56}")
    print(f"  Supertails · Four-Signal Dashboard  v3")
    print(f"{'='*56}")
    if not args.test_meltwater:
        print(f"  Mode   : {'DEMO' if args.demo else ('WATCH' if args.watch else 'LIVE')}")
        print(f"  Output : {args.output}\n")

    config = load_config(args.config)

    if args.test_meltwater:
        test_meltwater(config)
        return

    if args.demo:
        print("🎭  Generating 365-day demo data store...\n")
        store = generate_demo_store()
        # Patch config with demo baselines & campaign
        config.setdefault("baselines", {})
        config["baselines"].update(dict(
            branded_search_impressions=600, direct_installs_bangalore=44,
            direct_web_sessions_bangalore=265, direct_new_users_bangalore=140,
            brand_mentions=34, sov_percent=34.0, negative_sentiment_rate=4.5
        ))
        config.setdefault("campaign", {})
        config["campaign"].setdefault("phase", "01")
        config["campaign"].setdefault("phase_label", "OOH + Auto Live")
        config["campaign"].setdefault("start_date", store.get("_demo_campaign_start",""))
        config["campaign"].setdefault("activation_log", [
            {"date": store.get("_demo_campaign_start",""), "event": "OOH Wave 1 Live",              "type": "ooh"},
            {"date": (date.today()-timedelta(days=55)).isoformat(), "event": "Auto Branding Live",  "type": "auto"},
            {"date": (date.today()-timedelta(days=45)).isoformat(), "event": "Society Activation — Koramangala", "type": "activation"},
            {"date": (date.today()-timedelta(days=30)).isoformat(), "event": "Petcare Festival Week","type": "festival"},
        ])
        generate_dashboard(store, config, args.output)

    elif args.watch:
        watch_loop(config, args.interval, args.output)

    else:
        run_once(config, full_refresh=args.full_refresh, output=args.output)

if __name__ == "__main__":
    main()
