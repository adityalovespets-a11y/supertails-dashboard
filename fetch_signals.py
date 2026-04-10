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
    "branded_search", "direct_installs", "total_installs", "direct_installs_blr",
    # Revenue
    "revenue_india",
    # GA4 traffic — all channels
    "total_nonpaid_sessions", "total_paid_sessions",
    # GA4 traffic — sub-breakdowns
    "direct_sessions", "direct_new_users",
    "brand_paid_sessions", "blr_paid_sessions",
    # Social / Meltwater
    "brand_mentions", "hashtag_mentions", "sov_percent", "negative_rate",
    "competitor_huft", "competitor_wiggles", "competitor_petsutra",
]

# Keys that are dicts (not parallel arrays) — preserved separately in merge
DICT_KEYS = ["city_sessions", "city_list", "gsc_queries"]

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
    """Return (start_date, end_date) to fetch. start = day after last stored; end = yesterday."""
    end = yesterday()
    if full_refresh or not store.get("last_fetched_to"):
        # Go back 1 year
        start = (date.today() - timedelta(days=365)).isoformat()
    else:
        day_after = (date.fromisoformat(store["last_fetched_to"]) + timedelta(days=1)).isoformat()
        if day_after > end:
            return None, None  # Already current
        start = day_after
    return start, end

def merge_into_store(store, day_data: dict):
    """
    day_data: { 'YYYY-MM-DD': { signal_key: value, ... }, ... }
    Merges new daily rows into the store, maintaining date order, no duplicates.
    """
    existing = set(store["dates"])
    new_dates = sorted(d for d in day_data if d not in existing)
    if not new_dates:
        return store

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

    # Fill new data
    for d, signals in day_data.items():
        j = date_to_idx[d]
        for k in SIGNAL_KEYS:
            if k in signals:
                new_store[k][j] = signals[k]

    # Preserve dict-type keys (city_sessions, city_list, gsc_queries)
    for k in DICT_KEYS:
        if k in store:
            new_store[k] = store[k]

    return new_store

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 1 — BRANDED SEARCH (Google Search Console, daily)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_branded_search_daily(config, start_date, end_date):
    """Returns { 'YYYY-MM-DD': impressions, ... }"""
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

        resp = svc.searchanalytics().query(
            siteUrl=gsc["site_url"],
            body={
                "startDate": start_date, "endDate": end_date,
                "dimensions": ["date", "query"],
                "dimensionFilterGroups": [{"filters": [{
                    "dimension": "country", "operator": "equals",
                    "expression": gsc.get("geo_country", "ind")
                }]}],
                "rowLimit": 25000
            }
        ).execute()

        daily = {}
        for row in resp.get("rows", []):
            d, q = row["keys"][0], row["keys"][1].lower()
            if any(b in q for b in branded):
                daily[d] = daily.get(d, 0) + int(row.get("impressions", 0))

        # Fill zeros only for days GSC has had time to process (3-day lag)
        from datetime import date as _date
        lag_cutoff = (_date.today() - timedelta(days=3)).isoformat()
        for d in date_range(start_date, end_date):
            if d <= lag_cutoff:
                daily.setdefault(d, 0)
            # Leave recent days as None — GSC hasn't processed them yet

        print(f"    ✓ GSC: {len(daily)} days fetched")
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
        af = config["appsflyer"]
        url = f"https://hq.appsflyer.com/export/{af['app_id']}/installs_report/v5"
        params = {
            "api_token": af["api_token"], "from": start_date, "to": end_date,
            "timezone": "Asia/Kolkata", "currency": "INR",
        }
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))
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
        'brand_paid_sessions': n,
        'blr_paid_sessions': n
      }
    }
    """
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import FilterExpression, FilterExpressionList
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
        brand_kw = ga4.get("brand_campaign_contains", ["Brand"])
        blr_kw   = ga4.get("blr_campaign_contains", ["BLR"])

        india_mode = ga4.get("geo_mode", "india") == "india"
        city_f     = _city_filter(cities) if not india_mode else None

        def with_geo(filters):
            """Wrap filters with city filter if in bangalore mode."""
            if india_mode or city_f is None:
                return FilterExpression(and_group=FilterExpressionList(expressions=filters))
            return FilterExpression(and_group=FilterExpressionList(expressions=filters + [city_f]))

        # ── 1. Direct sessions ──
        direct_filter = with_geo([_channel_filter(ga4.get("direct_channel_values", ["Direct"]))])
        direct = _ga4_run_report(client, prop, start_date, end_date, direct_filter)

        # ── 2. Brand paid sessions ──
        brand_filter = with_geo([_channel_filter(paid_ch), _campaign_contains_filter(brand_kw)])
        brand_paid   = _ga4_run_report(client, prop, start_date, end_date, brand_filter)

        # ── 3. BLR paid sessions ──
        blr_filter = with_geo([_channel_filter(paid_ch), _campaign_contains_filter(blr_kw)])
        blr_paid   = _ga4_run_report(client, prop, start_date, end_date, blr_filter)

        # Merge into unified daily dict
        all_dates = set(list(direct.keys()) + list(brand_paid.keys()) + list(blr_paid.keys()))
        daily = {}
        for d in all_dates:
            daily[d] = {
                "sessions":            direct.get(d, {}).get("sessions", 0),
                "new_users":           direct.get(d, {}).get("new_users", 0),
                "brand_paid_sessions": brand_paid.get(d, {}).get("sessions", 0),
                "blr_paid_sessions":   blr_paid.get(d, {}).get("sessions", 0),
            }

        print(f"    ✓ GA4 Direct:      {len(direct)} days")
        print(f"    ✓ GA4 Brand Paid:  {len(brand_paid)} days")
        print(f"    ✓ GA4 BLR Paid:    {len(blr_paid)} days")
        return daily

    except Exception as e:
        print(f"    ✗ GA4 failed: {e}")
        return {}

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL 4 — SOCIAL MENTIONS & SOV (Meltwater, daily)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_social_daily(config, start_date, end_date):
    """
    Fetches 5 Meltwater saved searches:
      1. brand_campaign_master — Supertails brand + Bangalore campaign + Danish Sait + OOH (all-in-one)
      2. competitor_huft       — Heads Up For Tails
      3. competitor_wiggles    — Wiggles
      4. competitor_petsutra   — Petsutra
      5. negative_sentiment    — Supertails complaints / negative keywords
    SOV = brand_campaign_master / (brand + huft + wiggles + petsutra) * 100
    Negative rate = negative_sentiment / brand_campaign_master * 100
    """
    try:
        mw = config["meltwater"]
        api_key = mw.get("api_key", "")
        if not api_key or api_key.startswith("YOUR_"):
            print("    ✗ Meltwater: API key not configured")
            return {}

        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
        base = "https://api.meltwater.com/v3"
        ids  = mw["search_ids"]

        def fetch_volume(search_id):
            if not search_id or search_id.startswith("SEARCH_ID"):
                return {}
            url = f"{base}/searches/{search_id}/analytics/volume"
            params = {"from": f"{start_date}T00:00:00Z", "to": f"{end_date}T23:59:59Z", "groupby": "day"}
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code != 200:
                print(f"    ✗ Meltwater search {search_id}: HTTP {r.status_code}")
                return {}
            return {item["date"][:10]: item.get("count", 0) for item in r.json().get("data", [])}

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
                "hashtag_mentions":    b,   # same search covers hashtags; split if needed later
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
# FETCH ALL SIGNALS → merge into store
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_merge(config, store, start_date, end_date):
    print(f"\n  Fetching {start_date} → {end_date}...\n")

    print(f"  [1/4] Branded Search (GSC)")
    gsc_daily = fetch_branded_search_daily(config, start_date, end_date)
    print(f"  [1b]  Top Branded Queries (GSC)")
    gsc_queries = fetch_top_queries_gsc(config)

    print(f"  [2/4] Direct App Installs (AppsFlyer)")
    af_result      = fetch_direct_installs_daily(config, start_date, end_date)
    af_total_daily = af_result.get("total", {})
    af_india_daily = af_result.get("india", {})
    af_blr_daily   = af_result.get("bangalore", {})

    print(f"  [3/4] Direct Web Traffic (GA4)")
    ga_daily  = fetch_direct_web_daily(config, start_date, end_date)

    print(f"  [4/4] Social Mentions (Meltwater)")
    mw_daily  = fetch_social_daily(config, start_date, end_date)

    # Merge all into one day_data dict
    all_dates = sorted(set(list(gsc_daily) + list(af_india_daily) + list(ga_daily) + list(mw_daily)))
    day_data = {}
    for d in all_dates:
        web = ga_daily.get(d, {})
        soc = mw_daily.get(d, {})
        day_data[d] = {
            "branded_search":        gsc_daily.get(d),
            "total_installs":        af_total_daily.get(d),
            "direct_installs":       af_india_daily.get(d),
            "direct_installs_blr":   af_blr_daily.get(d),
            "direct_sessions":       web.get("sessions"),
            "direct_new_users":      web.get("new_users"),
            "brand_paid_sessions":   web.get("brand_paid_sessions"),
            "blr_paid_sessions":     web.get("blr_paid_sessions"),
            "brand_mentions":        soc.get("brand_mentions"),
            "hashtag_mentions":      soc.get("hashtag_mentions"),
            "sov_percent":           soc.get("sov_percent"),
            "negative_rate":         soc.get("negative_rate"),
            "competitor_huft":       soc.get("competitor_huft"),
            "competitor_wiggles":    soc.get("competitor_wiggles"),
            "competitor_petsutra":   soc.get("competitor_petsutra"),
        }

    merged = merge_into_store(store, day_data)
    if gsc_queries:
        merged["gsc_queries"] = gsc_queries
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
<title>Supertails · Offline Campaign Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
:root {
  --orange:#E8450A; --navy:#1B2A3B; --navy2:#243447; --navy-light:#3B5068;
  --bg:#F0F2F5; --card:#fff; --text:#1B2A3B; --muted:#6B7280;
  --green:#16A34A; --green-bg:#DCFCE7; --yellow:#D97706; --yellow-bg:#FEF3C7;
  --red:#DC2626; --red-bg:#FEE2E2; --grey:#9CA3AF; --grey-bg:#F3F4F6;
  --border:#E5E7EB; --r:12px;
}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
     background:var(--bg);color:var(--text);font-size:14px;}

/* HEADER */
.hdr{background:var(--navy);color:#fff;padding:14px 28px;
     display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;}
.hdr-left{display:flex;align-items:center;gap:16px;}
.logo{font-size:17px;font-weight:800;letter-spacing:1.5px;color:var(--orange);}
.hdr-title{font-size:14px;font-weight:600;color:rgba(255,255,255,.9);}
.hdr-sub{font-size:11px;color:rgba(255,255,255,.45);margin-top:2px;}
.hdr-right{display:flex;align-items:center;gap:12px;flex-wrap:wrap;}
.phase-badge{background:var(--orange);color:#fff;padding:5px 12px;
             border-radius:20px;font-size:11px;font-weight:700;letter-spacing:.5px;white-space:nowrap;}
.freshness{font-size:11px;color:rgba(255,255,255,.55);}
.freshness b{color:rgba(255,255,255,.85);}
.refresh-ctrl{display:flex;align-items:center;gap:6px;font-size:11px;color:rgba(255,255,255,.5);}
.refresh-toggle{width:32px;height:16px;background:rgba(255,255,255,.15);border-radius:8px;
                position:relative;cursor:pointer;border:none;transition:background .2s;}
.refresh-toggle.on{background:var(--orange);}
.refresh-toggle::after{content:'';width:12px;height:12px;background:#fff;border-radius:50%;
                       position:absolute;top:2px;left:2px;transition:left .2s;}
.refresh-toggle.on::after{left:18px;}

/* CONTROLS BAR */
.controls{background:var(--navy2);padding:12px 28px;
          display:flex;align-items:center;gap:16px;flex-wrap:wrap;border-bottom:1px solid rgba(255,255,255,.07);}
.ctrl-label{font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase;
            color:rgba(255,255,255,.45);white-space:nowrap;}
.date-input{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.2);
            color:#fff;border-radius:6px;padding:5px 8px;font-size:12px;outline:none;cursor:pointer;}
.date-input::-webkit-calendar-picker-indicator{filter:invert(1);opacity:.6;cursor:pointer;}
.ctrl-arrow{color:rgba(255,255,255,.4);font-size:13px;}
.ctrl-divider{width:1px;height:24px;background:rgba(255,255,255,.12);}
.gran-btns{display:flex;gap:2px;}
.gran-btn{background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.15);
          color:rgba(255,255,255,.6);border-radius:5px;padding:4px 10px;
          font-size:11px;font-weight:600;cursor:pointer;transition:all .15s;}
.gran-btn.active{background:var(--orange);border-color:var(--orange);color:#fff;}
.apply-btn{background:var(--orange);color:#fff;border:none;border-radius:6px;
           padding:6px 14px;font-size:12px;font-weight:700;cursor:pointer;transition:opacity .15s;white-space:nowrap;}
.apply-btn:hover{opacity:.85;}

/* COMPARE PANEL */
.cmp-panel{background:#111e2b;padding:10px 28px;
           display:flex;align-items:center;gap:14px;flex-wrap:wrap;
           border-bottom:1px solid rgba(255,255,255,.06);}
.period-tag{font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;white-space:nowrap;}
.tag-a{background:var(--orange);color:#fff;}
.tag-b{background:var(--navy-light);color:#fff;border:1px solid rgba(255,255,255,.15);}
.cmp-reset{background:transparent;color:rgba(255,255,255,.4);border:1px solid rgba(255,255,255,.15);
           border-radius:6px;padding:5px 12px;font-size:11px;cursor:pointer;transition:all .15s;}
.cmp-reset:hover{color:#fff;border-color:rgba(255,255,255,.35);}
.cmp-active{background:rgba(232,69,10,.2);border:1px solid var(--orange);color:var(--orange);
            font-size:10px;font-weight:700;padding:2px 9px;border-radius:20px;display:none;white-space:nowrap;}

/* CITY FILTER */
.city-bar{background:#1a2535;padding:9px 28px;display:flex;align-items:center;
          gap:8px;flex-wrap:wrap;border-bottom:1px solid rgba(255,255,255,.06);}
.city-btn{background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.12);
          color:rgba(255,255,255,.6);border-radius:20px;padding:4px 13px;
          font-size:11px;font-weight:600;cursor:pointer;transition:all .15s;white-space:nowrap;}
.city-btn:hover{background:rgba(255,255,255,.12);color:#fff;}
.city-btn.active{background:var(--orange);border-color:var(--orange);color:#fff;}
.city-note{font-size:10px;color:rgba(255,255,255,.3);margin-left:6px;font-style:italic;}

/* SIGNAL TOGGLE BUTTONS */
.toggle-row{display:flex;flex-wrap:wrap;gap:6px;padding:0 28px 10px;}
.tog-btn{background:rgba(255,255,255,.07);border:2px solid rgba(255,255,255,.15);
         color:rgba(255,255,255,.5);border-radius:20px;padding:5px 14px;
         font-size:11px;font-weight:700;cursor:pointer;transition:all .15s;white-space:nowrap;}
.tog-btn:hover{background:rgba(255,255,255,.12);color:#fff;}
.tog-btn.active{background:color-mix(in srgb,var(--tc,#6366f1) 20%,transparent);
                border-color:var(--tc,#6366f1);color:#fff;}

/* COMPARE SUMMARY */
.cmp-summary{display:none;margin:14px 28px 0;background:rgba(232,69,10,.06);
             border:1px solid rgba(232,69,10,.2);border-radius:var(--r);padding:14px 18px;}
.cmp-sum-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;
               color:var(--orange);margin-bottom:10px;}
.cmp-sum-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;}
@media(max-width:700px){.cmp-sum-grid{grid-template-columns:repeat(2,1fr);}}
.cmp-sum-item .lbl{font-size:11px;color:var(--muted);margin-bottom:4px;}
.cmp-sum-item .vals{display:flex;align-items:center;gap:6px;flex-wrap:wrap;}
.av{font-size:14px;font-weight:700;color:var(--orange);}
.bv{font-size:14px;font-weight:700;color:var(--navy);}
.chg{font-size:11px;font-weight:700;padding:2px 7px;border-radius:10px;}

/* ALERT */
.alert-bar{background:#FEF2F2;border:1px solid #FECACA;border-radius:var(--r);
           padding:11px 16px;display:flex;align-items:center;gap:8px;
           font-size:13px;font-weight:600;color:var(--red);margin:14px 28px 0;}
.alert-bar.hidden{display:none;}

/* SECTION */
.section{padding:20px 28px 0;}
.sec-title{font-size:10px;font-weight:700;letter-spacing:1.2px;color:var(--muted);
           text-transform:uppercase;margin-bottom:12px;}

/* SIGNAL CARDS */
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;padding:16px 28px 0;}
@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr);}}
.card{background:var(--card);border-radius:var(--r);padding:18px;
      border:1px solid var(--border);position:relative;overflow:hidden;}
.s-bar{position:absolute;top:0;left:0;right:0;height:4px;}
.s-bar.green{background:var(--green)}.s-bar.yellow{background:var(--yellow)}
.s-bar.red{background:var(--red)}.s-bar.grey{background:var(--grey)}
.snum{font-size:10px;font-weight:700;color:var(--orange);letter-spacing:1px;
      text-transform:uppercase;margin-bottom:3px;}
.stitle{font-size:12px;font-weight:700;margin-bottom:12px;}
.sval{font-size:28px;font-weight:800;line-height:1;}
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
.scmp{display:none;margin-top:8px;padding-top:8px;border-top:1px solid var(--border);}
.scmp-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
.dot{width:7px;height:7px;border-radius:50%;flex-shrink:0;}
.dot-a{background:var(--orange)}.dot-b{background:var(--navy-light)}
.cmp-v{display:flex;align-items:center;gap:4px;}
.cmp-lbl{font-size:10px;color:var(--muted);}
.cmp-num{font-size:13px;font-weight:700;}
.cmp-d{font-size:11px;font-weight:700;padding:2px 7px;border-radius:20px;}

/* CHARTS */
.charts{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;padding:12px 28px 0;}
@media(max-width:900px){.charts{grid-template-columns:1fr;}}
.ccrd{background:var(--card);border-radius:var(--r);padding:18px;border:1px solid var(--border);}
.ctop{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px;}
.ctitle{font-size:12px;font-weight:700;}
.csub{font-size:10px;color:var(--muted);margin-top:2px;}
.cwrap{position:relative;height:175px;}

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
</style>
</head>
<body>

<!-- HEADER -->
<header class="hdr">
  <div class="hdr-left">
    <div class="logo">SUPERTAILS</div>
    <div>
      <div class="hdr-title">Offline Campaign · Four-Signal Dashboard</div>
      <div class="hdr-sub">Unlocking Bangalore · Daily Tracking · v3</div>
    </div>
  </div>
  <div class="hdr-right">
    <div class="freshness">Data current as of <b id="freshDate">—</b> (t-1)</div>
    <div class="phase-badge" id="phaseBadge">Phase 01</div>
    <div class="refresh-ctrl">
      <button class="refresh-toggle on" id="refreshToggle" onclick="toggleAutoRefresh()"></button>
      <span id="refreshLabel">Auto-refresh in <b id="countdown">5:00</b></span>
    </div>
  </div>
</header>

<!-- VIEW CONTROLS (date range + granularity) -->
<div class="controls">
  <span class="ctrl-label">View</span>
  <input type="date" class="date-input" id="viewStart">
  <span class="ctrl-arrow">→</span>
  <input type="date" class="date-input" id="viewEnd">
  <div class="ctrl-divider"></div>
  <span class="ctrl-label">Granularity</span>
  <div class="gran-btns">
    <button class="gran-btn" id="gD" onclick="setGran('D')">Daily</button>
    <button class="gran-btn active" id="gW" onclick="setGran('W')">Weekly</button>
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

<!-- NEGATIVE ALERT -->
<div class="alert-bar hidden" id="negAlert">
  ⚠️ NEGATIVE SENTIMENT ALERT — Rate above 15% threshold. Escalate to Brand & CX immediately.
</div>

<!-- SIGNAL CARDS -->
<div class="section"><div class="sec-title">Signal Health — Selected Period vs Baseline</div></div>
<div class="cards">
  <div class="card" id="c1">
    <div class="s-bar" id="b1"></div>
    <div class="snum">Signal 01</div>
    <div class="stitle">Branded Search Volume</div>
    <div><span class="sval" id="v1">—</span><span class="sunit" id="su1">impressions/day</span></div>
    <div class="smeta"><span class="sbase" id="bl1">Baseline: —</span><span class="sdelta" id="d1">—</span></div>
    <div class="scmp" id="cmp1"></div>
    <div class="sthresh">Threshold: +20% above baseline</div>
    <div class="stool">Google Search Console · All India</div>
  </div>
  <div class="card" id="c2">
    <div class="s-bar" id="b2"></div>
    <div class="snum">Signal 02</div>
    <div class="stitle">Direct App Installs</div>
    <div><span class="sval" id="v2">—</span><span class="sunit" id="su2">installs/day</span></div>
    <div class="smeta"><span class="sbase" id="bl2">Baseline: —</span><span class="sdelta" id="d2">—</span></div>
    <div class="scmp" id="cmp2"></div>
    <div class="sthresh">Threshold: +15–25% above baseline</div>
    <div class="stool">AppsFlyer · All India · Unattributed</div>
  </div>
  <div class="card" id="c3">
    <div class="s-bar" id="b3"></div>
    <div class="snum">Signal 03</div>
    <div class="stitle">Non-Paid Sessions</div>
    <div><span class="sval" id="v3">—</span><span class="sunit" id="su3">sessions/day</span></div>
    <div class="smeta"><span class="sbase" id="bl3">Baseline: —</span><span class="sdelta" id="d3">—</span></div>
    <div class="scmp" id="cmp3"></div>
    <div class="sthresh">Threshold: +20% vs baseline</div>
    <div class="stool">GA4 · All non-paid channels · All India</div>
  </div>
  <div class="card" id="c4" style="position:relative;">
    <div class="s-bar" id="b4"></div>
    <div class="snum">Signal 04</div>
    <div class="stitle">Brand Mentions</div>
    <div><span class="sval" id="v4">—</span><span class="sunit" id="su4">mentions/day</span></div>
    <div class="smeta"><span class="sbase" id="bl4">Baseline: —</span><span class="sdelta" id="d4">—</span></div>
    <div class="scmp" id="cmp4"></div>
    <div class="sthresh">Threshold: +20% & SOV +5pts</div>
    <div class="stool">Meltwater · Instagram, X, Reddit, LinkedIn</div>
    <div id="c4_nc" style="display:none;position:absolute;inset:0;background:rgba(241,245,249,0.92);border-radius:12px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;">
      <div style="font-size:18px;">🔌</div>
      <div style="font-size:12px;font-weight:600;color:#475569;">Not Connected</div>
      <div style="font-size:10px;color:#94a3b8;">Meltwater plugin required</div>
    </div>
  </div>
</div>

<!-- TREND CHARTS -->
<div class="section" style="margin-top:12px;"><div class="sec-title">Trends — <span id="chartRangeLabel"></span></div></div>
<div class="charts">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Branded Search Volume</div><div class="csub">GSC · All India · Branded queries</div></div></div><div class="cwrap"><canvas id="ch1"></canvas></div></div>
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Non-Paid Sessions</div><div class="csub" id="sub_nonpaid">GA4 · All non-paid channels · <span id="sub_nonpaid_city">All India</span></div></div></div><div class="cwrap"><canvas id="ch_nonpaid"></canvas></div></div>
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Paid Sessions</div><div class="csub" id="sub_paid">GA4 · All paid channels · <span id="sub_paid_city">All India</span></div></div></div><div class="cwrap"><canvas id="ch_paid"></canvas></div></div>
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">App Installs</div><div class="csub">AppsFlyer · India · Total &amp; Organic</div></div></div><div class="cwrap"><canvas id="ch2"></canvas></div></div>
</div>
<!-- PAID BREAKDOWN -->
<div class="section" style="margin-top:12px;"><div class="sec-title">Paid Breakdown — <span id="paidBreakLabel"></span></div></div>
<div class="charts">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Brand Campaign Sessions</div><div class="csub">GA4 · Campaigns with "Brand" · All India</div></div></div><div class="cwrap"><canvas id="ch3"></canvas></div></div>
</div>

<!-- REVENUE CHART -->
<div class="section" style="margin-top:12px;"><div class="sec-title">India NMV — <span id="revRangeLabel"></span></div></div>
<div class="charts" style="grid-template-columns:1fr;">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Daily / Weekly / Monthly Revenue (NMV)</div><div class="csub">All India · Supertails MCP · ₹</div></div></div><div class="cwrap" style="height:220px;"><canvas id="ch_rev"></canvas></div></div>
</div>

<!-- ALL SESSIONS — SINGLE TOGGLEABLE CHART -->
<div class="section" style="margin-top:12px;">
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
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">All Sessions</div><div class="csub" id="sub_all_sess">GA4 · All India · Select signals above</div></div></div><div class="cwrap" style="height:260px;"><canvas id="ch_all_sess"></canvas></div></div>
</div>

<!-- SIGNAL CORRELATION OVERLAY -->
<div class="section" style="margin-top:12px;">
  <div class="sec-title">Signal Correlation — Normalised Overlay</div>
  <div style="font-size:11px;color:var(--muted);margin-bottom:6px;">Each signal indexed to 100 = pre-campaign baseline. Compare trends and lead/lag.</div>
  <div class="toggle-row" id="corrToggles">
    <button class="tog-btn active" data-key="branded_search"          onclick="toggleCorr(this)" style="--tc:#e8450a">Branded Search</button>
    <button class="tog-btn active" data-key="total_nonpaid_sessions"  onclick="toggleCorr(this)" style="--tc:#6366f1">Non-Paid Sessions</button>
    <button class="tog-btn active" data-key="total_paid_sessions"     onclick="toggleCorr(this)" style="--tc:#ef4444">Paid Sessions</button>
    <button class="tog-btn active" data-key="direct_installs"         onclick="toggleCorr(this)" style="--tc:#f59e0b">Organic Installs</button>
    <button class="tog-btn active" data-key="revenue_india"           onclick="toggleCorr(this)" style="--tc:#14b8a6">India NMV</button>
    <button class="tog-btn"        data-key="brand_paid_sessions"     onclick="toggleCorr(this)" style="--tc:#8b5cf6">Brand Paid</button>
  </div>
</div>
<div class="charts" style="grid-template-columns:1fr;">
  <div class="ccrd"><div class="ctop"><div><div class="ctitle">Signal Correlation View</div><div class="csub">Indexed to 100 = baseline · Toggle signals above</div></div></div><div class="cwrap" style="height:280px;"><canvas id="ch_corr"></canvas></div></div>
</div>

<!-- SOV + SENTIMENT -->
<div class="section" style="margin-top:12px;"><div class="sec-title">Share of Voice & Sentiment — Latest data point</div></div>
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
    <div style="margin-top:10px;">
      <div style="font-size:11px;color:var(--muted);">Negative rate · Alert threshold: 15%</div>
      <div class="neg-rate-val" id="negRateVal">—</div>
    </div>
  </div>
</div>

<!-- ACTIVATION LOG + GUIDE -->
<div class="bot">
  <div class="sec-title" style="margin-top:16px;">Campaign Activation Log</div>
  <div class="icrd">
    <table class="log-t"><thead><tr><th>Date</th><th>Event</th><th>Type</th></tr></thead>
    <tbody id="logBody"></tbody></table>
  </div>
  <div class="sec-title" style="margin-top:16px;">Signal Interpretation Guide</div>
  <div class="icrd">
    <table class="int-t">
      <thead><tr><th>What you see</th><th>What it means</th><th>Action</th></tr></thead>
      <tbody>
        <tr><td class="int-sig">✅ All 4 signals lift within 2 weeks of OOH</td><td>Strong evidence offline is working.</td><td class="int-act">Scale campaign, proceed to Phase 2.</td></tr>
        <tr><td class="int-sig">🔍 Search + social lift, installs + web flat</td><td>Awareness building but not converting.</td><td class="int-act">Stronger CTA, check install flow.</td></tr>
        <tr><td class="int-sig">📱 Installs + web lift, search + social flat</td><td>Strong direct recall, quiet action.</td><td class="int-act">Maintain media, audit format mix.</td></tr>
        <tr><td class="int-sig">💬 Social lifts, digital signals flat</td><td>Conversation without conversion.</td><td class="int-act">Add QR codes or short URLs to OOH.</td></tr>
        <tr><td class="int-sig">⚪ All 4 flat after 3+ weeks</td><td>Offline not cutting through.</td><td class="int-act">Audit locations, test new creative.</td></tr>
        <tr><td class="int-sig">⚠️ Negative sentiment above 15%</td><td>Service issue reaching new users.</td><td class="int-act">Escalate to Brand + CX. Don't pause.</td></tr>
      </tbody>
    </table>
  </div>
</div>
<footer>Supertails · Offline Campaign Tracking · Four-Signal Framework v3 · 2025–26 · Confidential</footer>

<script>
const S = __STORE_DATA__;
const CFG = __CONFIG_DATA__;
const baselines = CFG.baselines || {};
const CITY_SESSIONS = S.city_sessions || {};
const CITY_LIST     = S.city_list || [];
const ORANGE='#E8450A', NAVY='#1B2A3B', NAVY_L='#3B5068',
      OBG='rgba(232,69,10,.10)', NBG='rgba(59,80,104,.10)', GREY_C='rgba(107,114,128,.4)';

// ── Helpers
function fmt(n){return n==null?'—':Math.round(n).toLocaleString('en-IN');}
function fmtD(n){return n==null?'—':n.toFixed(1);}
function avg(arr){const v=(arr||[]).filter(x=>x!=null&&!isNaN(x));return v.length?v.reduce((a,b)=>a+b,0)/v.length:null;}
function pct(curr,base){return(curr==null||!base)?null:((curr-base)/base*100);}
function dcls(p,th){if(p==null)return'grey';return p>=th?'green':p>=0?'yellow':'red';}

// ── Header
document.getElementById('phaseBadge').textContent='Phase '+S.campaign_phase+' — '+S.campaign_phase_label;
document.getElementById('freshDate').textContent=S.last_fetched_to||'—';

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

// ── Auto-refresh countdown
let refreshOn=true, countdown=300;
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
let activeCity = 'all';

(function buildCityBtns(){
  const wrap = document.getElementById('cityBtns');
  const note = document.getElementById('cityNote');
  if(!CITY_LIST.length){ note.textContent='Run backfill_ga4_cities.py to enable city filter'; return; }
  CITY_LIST.forEach(city=>{
    const b = document.createElement('button');
    b.className = 'city-btn';
    b.id = 'city-'+city.replace(/\s+/g,'-');
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

const NOISE_CITIES = ['(not set)', 'Ashburn'];

// Maps city_sessions city-level key → top-level India store key
const CITY_KEY_TO_STORE = {
  'direct':        'direct_sessions',
  'total_paid':    'total_paid_sessions',
  'total_nonpaid': 'total_nonpaid_sessions',
  'brand_paid':    'brand_paid_sessions',
  'blr_paid':      'blr_paid_sessions',
};

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

// ── Initialise date pickers
const allDates = S.dates||[];
const minDate  = allDates[0]||'';
const maxDate  = allDates[allDates.length-1]||'';
const today    = new Date().toISOString().split('T')[0];
// Default view: last 90 days
const view90   = allDates.length>=90 ? allDates[allDates.length-90] : minDate;
document.getElementById('viewStart').value = view90;
document.getElementById('viewEnd').value   = maxDate;
document.getElementById('viewStart').min   = minDate;
document.getElementById('viewEnd').min     = minDate;
document.getElementById('viewStart').max   = maxDate;
document.getElementById('viewEnd').max     = maxDate;

// Campaign start for defaults
const cs = S.campaign_start || allDates[Math.floor(allDates.length*0.8)] || allDates[0];
// Comparison defaults: A = 30 days before campaign start, B = last 30 days
function addDays(d,n){const dt=new Date(d);dt.setDate(dt.getDate()+n);return dt.toISOString().split('T')[0];}
const aStart0=addDays(cs,-30), aEnd0=addDays(cs,-1);
const bStart0=addDays(maxDate,-29), bEnd0=maxDate;
['aStart','aEnd','bStart','bEnd'].forEach(id=>{
  document.getElementById(id).min=minDate;
  document.getElementById(id).max=maxDate;
});
document.getElementById('aStart').value=aStart0<minDate?minDate:aStart0;
document.getElementById('aEnd').value  =aEnd0<minDate?minDate:aEnd0;
document.getElementById('bStart').value=bStart0<minDate?minDate:bStart0;
document.getElementById('bEnd').value  =bEnd0;

// ── Granularity
let gran='W';
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
  return{
    dates:               allDates.slice(si,ei+1),
    branded_search:      sl(S.branded_search),
    total_installs:      sl(S.total_installs),
    direct_installs:     sl(S.direct_installs),
    revenue_india:       sl(S.revenue_india),
    // All-traffic split
    total_nonpaid_sessions: sc('total_nonpaid'),
    total_paid_sessions:    sc('total_paid'),
    // Sub-breakdowns
    direct_sessions:        sc('direct'),
    brand_paid_sessions:    sc('brand_paid'),
    blr_paid_sessions:      sc('blr_paid'),
    direct_new_users:    sl(S.direct_new_users),
    brand_mentions:      sl(S.brand_mentions),
    sov_percent:         sl(S.sov_percent),
    negative_rate:       sl(S.negative_rate),
    competitor_huft:     sl(S.competitor_huft),
    competitor_wiggles:  sl(S.competitor_wiggles),
    competitor_petsutra: sl(S.competitor_petsutra),
  };
}

// ── Aggregate dates+values by granularity
// W/M use SUM; labels are human-readable absolute dates / ranges / month names
const MO=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function fmtDay(d){ const dt=new Date(d+'T00:00:00'); return dt.getDate()+' '+MO[dt.getMonth()]; }

function aggregate(dates,values,g){
  if(g==='D') return{labels:dates.map(d=>fmtDay(d)),values,counts:values.map(v=>v!=null?1:0)};

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
  return{labels,values:agg,counts};
}

// ── Chart engine
const CI={};
function mkChart(id,labels,datasets){
  if(CI[id]){CI[id].destroy();delete CI[id];}
  const ctx=document.getElementById(id).getContext('2d');
  CI[id]=new Chart(ctx,{
    type:'line',
    data:{labels,datasets},
    options:{
      responsive:true,maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:datasets.length>1,position:'top',
                labels:{font:{size:10},boxWidth:10,padding:6}},
        tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y!=null?Math.round(c.parsed.y).toLocaleString('en-IN'):'—'}`}}
      },
      scales:{
        x:{grid:{display:false},ticks:{font:{size:9},maxRotation:45,maxTicksLimit:12}},
        y:{grid:{color:'#F3F4F6'},ticks:{font:{size:9}},beginAtZero:false}
      }
    }
  });
}
function ds(data,label,color,bg,dash){
  return{label,data,borderColor:color,backgroundColor:bg,borderWidth:2.5,
         pointRadius:allDates.length>90?0:3,pointBackgroundColor:color,
         fill:true,tension:0.3,borderDash:dash||[]};
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

// ── Main render
let currentSlice=null;
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
  [['su1','impressions'],['su2','installs'],['su3','sessions'],['su4','mentions']].forEach(([id,lbl])=>{
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
    const {labels,values}=aggregate(slice.dates,c.arr,g);
    const color=c.color||ORANGE;
    const bg=color+'26';
    mkChart(c.id,labels,[ds(values,c.label,color,bg),baseds(c.base,labels.length)]);
  });

  // Installs chart — Total + Organic as two series
  {
    const {labels:iL, values:totV} = aggregate(slice.dates, slice.total_installs, g);
    const {values:orgV}            = aggregate(slice.dates, slice.direct_installs, g);
    const orgBase = baselines.direct_installs_india||baselines.direct_installs_bangalore||0;
    mkChart('ch2', iL, [
      ds(totV, 'Total Installs',   '#f59e0b', '#f59e0b26'),
      ds(orgV, 'Organic Installs', '#fb923c', '#fb923c26'),
      baseds(orgBase, iL.length),
    ]);
  }

  // Paid breakdown sub-charts (Brand)
  const paidBreak=[
    {id:'ch3', arr:slice.brand_paid_sessions, base:baselines.brand_paid_sessions_india||0,
               label:'Brand Campaign Sessions', color:'#8b5cf6'},
  ];
  paidBreak.forEach(c=>{
    const {labels,values}=aggregate(slice.dates,c.arr,g);
    const bg=c.color+'26';
    mkChart(c.id,labels,[ds(values,c.label,c.color,bg),baseds(c.base,labels.length)]);
  });

  // Revenue chart
  const {labels:revL, values:revV} = aggregate(slice.dates, slice.revenue_india, g);
  const revBase = g==='W' ? baselines.revenue_india_weekly||0
                : g==='M' ? (baselines.revenue_india_weekly||0)*30/7
                : baselines.revenue_india_daily||0;
  mkChart('ch_rev', revL, [ds(revV,'India NMV (₹)','#14b8a6','#14b8a622'), baseds(revBase, revL.length)]);

  // Re-render the two new combo charts using current toggle states
  renderAllSessions(slice, g);
  renderCorrelation(slice, g);

  // SOV donut (latest data point in slice)
  renderSOV(slice);

  // Negative alert
  const lastNeg=slice.negative_rate.filter(x=>x!=null).slice(-1)[0];
  document.getElementById('negAlert').classList.toggle('hidden',!(lastNeg>=15));
  document.getElementById('negRateVal').textContent=lastNeg!=null?fmtD(lastNeg)+'%':'—';
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
let sessTog = new Set(['total_nonpaid_sessions','total_paid_sessions']); // default on

function toggleSess(btn){
  const key=btn.dataset.key;
  if(sessTog.has(key)){sessTog.delete(key);}else{sessTog.add(key);}
  btn.classList.toggle('active', sessTog.has(key));
  if(currentSlice) renderAllSessions(currentSlice, gran);
}

function renderAllSessions(slice, g){
  const datasets=[];
  SESS_DEFS.forEach(d=>{
    if(!sessTog.has(d.key)) return;
    const arr=slice[d.key]||[];
    const {labels,values}=aggregate(slice.dates, arr, g);
    datasets.push(ds(values, d.label, d.color, d.color+'22'));
    // store labels from last valid def
    renderAllSessions._labels = labels;
  });
  const labels = renderAllSessions._labels || slice.dates.map(d=>d.slice(5));
  mkChart('ch_all_sess', labels, datasets.length ? datasets : [{label:'No signals selected',data:[],borderColor:'transparent'}]);
}

// ── Signal Correlation overlay (normalised to index 100 = baseline) ───────────
const CORR_DEFS = [
  {key:'branded_search',         label:'Branded Search',  color:'#e8450a', base:'branded_search_impressions'},
  {key:'total_nonpaid_sessions', label:'Non-Paid Sess',   color:'#6366f1', base:'total_nonpaid_sessions_india'},
  {key:'total_paid_sessions',    label:'Paid Sessions',   color:'#ef4444', base:'total_paid_sessions_india'},
  {key:'direct_installs',        label:'Organic Installs',color:'#f59e0b', base:'direct_installs_india'},
  {key:'revenue_india',          label:'India NMV',       color:'#14b8a6', base:'revenue_india_daily'},
  {key:'brand_paid_sessions',    label:'Brand Paid',      color:'#8b5cf6', base:'brand_paid_sessions_india'},
];
let corrTog = new Set(['branded_search','total_nonpaid_sessions','total_paid_sessions','direct_installs','revenue_india']);

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
  CORR_DEFS.forEach(d=>{
    if(!corrTog.has(d.key)) return;
    const arr=slice[d.key]||[];
    const {labels,values}=aggregate(slice.dates, arr, g);
    if(!sharedLabels) sharedLabels=labels;
    const indexed = indexArr(values, d.base);
    const d2=ds(indexed, d.label, d.color, 'transparent');
    d2.fill=false; // no fill for overlay — just lines
    datasets.push(d2);
  });
  const labels = sharedLabels || slice.dates.map(d=>d.slice(5));

  if(CI['ch_corr']){CI['ch_corr'].destroy();delete CI['ch_corr'];}
  const ctx=document.getElementById('ch_corr').getContext('2d');
  CI['ch_corr']=new Chart(ctx,{
    type:'line',
    data:{labels, datasets: datasets.length ? datasets : []},
    options:{
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{display:true,position:'top',labels:{font:{size:10},boxWidth:10,padding:6}},
        tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.raw!=null?c.raw+'':' —'}`}},
        annotation:{annotations:{
          baseline:{type:'line',yMin:100,yMax:100,borderColor:'rgba(255,255,255,.25)',
                    borderWidth:1,borderDash:[4,3],
                    label:{content:'Baseline = 100',enabled:true,position:'start',
                           color:'rgba(255,255,255,.4)',font:{size:9}}}
        }}
      },
      scales:{
        x:{grid:{color:'rgba(255,255,255,.04)'},ticks:{color:'rgba(255,255,255,.5)',font:{size:9},maxTicksLimit:12}},
        y:{grid:{color:'rgba(255,255,255,.06)'},ticks:{color:'rgba(255,255,255,.5)',font:{size:9},
           callback:v=>v+''},
           title:{display:true,text:'Index (100 = baseline)',color:'rgba(255,255,255,.4)',font:{size:9}}}
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
const tagMap={ooh:'t-ooh',auto:'t-auto',activation:'t-act',pr:'t-pr',festival:'t-fest'};
(S.activation_log||[]).forEach(e=>{
  logBody.innerHTML+=`<tr><td>${e.date}</td><td>${e.event}</td>
    <td><span class="tag ${tagMap[e.type]||'t-ooh'}">${e.type.toUpperCase()}</span></td></tr>`;
});
if(!(S.activation_log||[]).length)
  logBody.innerHTML='<tr><td colspan="3" style="color:var(--muted);text-align:center;padding:18px">No activation events logged yet — add them in config.json</td></tr>';

// ── Initial render (last 90 days)
renderDashboard(sliceByDate(view90,maxDate),gran);
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# GENERATE DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

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
            "appsflyer":  bool(af.get("api_token") and not str(af.get("api_token","")).startswith("YOUR_")),
        }
    }

    store_json  = json.dumps(store_out,  default=str)
    config_json = json.dumps(config_out, default=str)

    html = DASHBOARD_HTML \
        .replace("__STORE_DATA__",  store_json) \
        .replace("__CONFIG_DATA__", config_json)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(output_path) // 1024
    print(f"\n✅  Dashboard → {output_path}  ({size_kb} KB)")
    print(f"    Data: {store.get('dates', [''])[0]}  →  {store.get('last_fetched_to','—')}")
    print(f"    Days in store: {len(store.get('dates', []))}")


# ─────────────────────────────────────────────────────────────────────────────
# WATCH MODE
# ─────────────────────────────────────────────────────────────────────────────

def run_once(config, full_refresh, output):
    store = load_store()
    start, end = get_fetch_range(store, config, full_refresh)
    if start is None:
        print("  Already current to t-1. Regenerating dashboard from store...")
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
    cmds = [
        ["git", "add", output_path, "data_store.json"],
        ["git", "commit", "-m", f"dashboard: auto-update {ts}"],
        ["git", "push"],
    ]
    for cmd in cmds:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            # "nothing to commit" is fine — not a real error
            if "nothing to commit" in r.stdout or "nothing to commit" in r.stderr:
                print("  (no changes to push — dashboard already up to date)")
                return
            print(f"⚠️   git command failed: {' '.join(cmd)}\n    {r.stderr.strip()}")
            return

    print("✅  Pushed to GitHub. Netlify will deploy in ~30 seconds.")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Supertails Offline Campaign Dashboard v3")
    p.add_argument("--demo",         action="store_true", help="365-day sample data, no API keys")
    p.add_argument("--watch",        action="store_true", help="Keep running, refresh on interval")
    p.add_argument("--interval",     type=int, default=60, help="Watch refresh interval in minutes (default 60)")
    p.add_argument("--full-refresh", action="store_true", help="Re-fetch entire 12-month history")
    p.add_argument("--config",       default="config.json")
    p.add_argument("--output",       default="supertails_dashboard.html")
    args = p.parse_args()

    print(f"\n{'='*56}")
    print(f"  Supertails · Four-Signal Dashboard  v3")
    print(f"{'='*56}")
    print(f"  Mode   : {'DEMO' if args.demo else ('WATCH' if args.watch else 'LIVE')}")
    print(f"  Output : {args.output}\n")

    config = load_config(args.config)

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
