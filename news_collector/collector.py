from __future__ import annotations

import datetime as dt
import json
from typing import List, Dict, Optional

from dateutil import tz, parser as dtparse
from tqdm import tqdm

from .api import fetch_top_headlines_category, fetch_everything_by_domains
from .db import connect_db, save_article


def filter_since(items: List[Dict], since_dt: Optional[dt.datetime], keep_no_pub: bool = True) -> List[Dict]:
    if not since_dt:
        return items
    out: List[Dict] = []
    for it in items:
        pub = it.get("published")
        if not pub:
            if keep_no_pub:
                out.append(it)
            continue
        try:
            if dtparse.isoparse(pub) >= since_dt:
                out.append(it)
        except Exception:
            if keep_no_pub:
                out.append(it)
    return out


def collect_categories(categories: List[str], country: str, page_size: int,
                       since_hours: Optional[int], limit_per_cat: Optional[int],
                       max_pages: int, api_key: Optional[str], to_json: Optional[str],
                       debug: bool = False) -> Dict[str, Dict[str, int]]:
    if not api_key:
        raise RuntimeError("NEWSAPI_KEY 필요")
    conn = connect_db()
    since_dt = None
    if since_hours is not None:
        since_dt = dt.datetime.now(tz=tz.UTC) - dt.timedelta(hours=since_hours)
        if debug:
            print(f"[Filter] since={since_dt.isoformat()} UTC")
    results: Dict[str, Dict[str, int]] = {}
    dump: List[Dict] = []
    for cat in categories:
        fetched = fetch_top_headlines_category(api_key, cat, country, page_size, max_pages, debug)
        before = len(fetched)
        filtered = filter_since(fetched, since_dt, True)
        if debug:
            print(f"[Filter] {cat}: {before} -> {len(filtered)}")
        filtered.sort(key=lambda x: x.get("published") or "", reverse=True)
        if limit_per_cat:
            filtered = filtered[:limit_per_cat]
        saved = skipped = 0
        for a in tqdm(filtered, desc=f"Saving [{cat}]"):
            if save_article(conn, a):
                saved += 1
            else:
                skipped += 1
        results[cat] = {"saved": saved, "skipped": skipped, "count": len(filtered)}
        if to_json:
            dump.extend(filtered)
    if to_json:
        with open(to_json, "w", encoding="utf-8") as f:
            json.dump(dump, f, ensure_ascii=False, indent=2)
    return results


def collect_categories_domains_mode(categories: List[str], page_size: int,
                                    since_hours: Optional[int], limit_per_cat: Optional[int],
                                    max_pages: int, api_key: Optional[str], to_json: Optional[str],
                                    languages: List[str], domains_file: str,
                                    debug: bool = False) -> Dict[str, Dict[str, int]]:
    if not api_key:
        raise RuntimeError("NEWSAPI_KEY 필요")
    import json
    with open(domains_file, "r", encoding="utf-8") as f:
        raw_map = json.load(f)
    dom_map = {k: ",".join(sorted(set(v))) for k, v in raw_map.items()}

    conn = connect_db()
    since_dt = None
    if since_hours is not None:
        since_dt = dt.datetime.now(tz=tz.UTC) - dt.timedelta(hours=since_hours)
        if debug:
            print(f"[Filter] since={since_dt.isoformat()} UTC")

    results: Dict[str, Dict[str, int]] = {}
    dump: List[Dict] = []
    for cat in categories:
        dom_csv = dom_map.get(cat)
        if not dom_csv:
            if debug:
                print(f"[Domains] {cat}: none")
            results[cat] = {"saved": 0, "skipped": 0, "count": 0}
            continue
        merged: List[Dict] = []
        for lang in languages:
            merged.extend(fetch_everything_by_domains(
                api_key, domains_csv=dom_csv, language=lang,
                page_size=page_size, max_pages=max_pages, debug=debug
            ))
        before = len(merged)
        filtered = filter_since(merged, since_dt, True)
        if debug:
            print(f"[Domains] {cat}: {before} -> {len(filtered)} (langs={','.join(languages)})")
        for it in filtered:
            it["category"] = cat
        filtered.sort(key=lambda x: x.get("published") or "", reverse=True)
        if limit_per_cat:
            filtered = filtered[:limit_per_cat]
        saved = skipped = 0
        for a in tqdm(filtered, desc=f"Saving [domains:{cat}]"):
            if save_article(conn, a):
                saved += 1
            else:
                skipped += 1
        results[cat] = {"saved": saved, "skipped": skipped, "count": len(filtered)}
        if to_json:
            dump.extend(filtered)
    if to_json:
        with open(to_json, "w", encoding="utf-8") as f:
            json.dump(dump, f, ensure_ascii=False, indent=2)
    return results
