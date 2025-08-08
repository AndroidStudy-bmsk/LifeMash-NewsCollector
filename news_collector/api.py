from __future__ import annotations
import time, requests
from typing import List, Dict, Optional
from .constants import NEWSAPI_CATEGORIES
from .utils import make_id, norm_time


def fetch_top_headlines_category(api_key: str, category: str, country: str = "us",
                                 page_size: int = 100, max_pages: int = 1, debug: bool = False) -> List[Dict]:
    assert category in NEWSAPI_CATEGORIES
    url = "https://newsapi.org/v2/top-headlines"
    sess = requests.Session()
    items: List[Dict] = []
    page = 1
    while page <= max_pages:
        params = {"apiKey": api_key, "category": category, "country": country,
                  "pageSize": page_size, "page": page}
        r = sess.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "ok":
            if debug: print(f"[NewsAPI error] top-headlines {category} p{page}: {data.get('message')}")
            break
        arts = data.get("articles", []) or []
        if debug: print(f"[NewsAPI] top-headlines {category} p{page} -> {len(arts)}")
        for a in arts:
            title, link = a.get("title"), a.get("url")
            items.append({
                "id": make_id(title, link),
                "title": title,
                "url": link,
                "source": (a.get("source") or {}).get("name") or "NewsAPI",
                "published": norm_time(a.get("publishedAt")),
                "summary": (a.get("description") or "")[:2000],
                "category": category,
                "raw": a,
            })
        if len(arts) < page_size: break
        page += 1
        time.sleep(0.2)
    return items


def fetch_everything_by_domains(api_key: str, *, domains_csv: str, language: str,
                                page_size: int = 100, max_pages: int = 1, debug: bool = False,
                                extra_params: Optional[Dict] = None) -> List[Dict]:
    url = "https://newsapi.org/v2/everything"
    sess = requests.Session()
    items: List[Dict] = []
    page = 1
    while page <= max_pages:
        params = {"apiKey": api_key, "language": language, "domains": domains_csv,
                  "pageSize": page_size, "page": page}
        if extra_params: params.update(extra_params)
        try:
            r = sess.get(url, params=params, timeout=20)
            r.raise_for_status()
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if debug: print(f"[HTTPError] everything {language} p{page} code={code} msg={e}")
            if code in (401, 426, 429): break
            raise
        data = r.json()
        if data.get("status") != "ok":
            if debug: print(f"[NewsAPI error] everything {language} p{page}: {data.get('message')}")
            break
        arts = data.get("articles", []) or []
        if debug: print(f"[NewsAPI] everything {language} p{page} -> {len(arts)}")
        for a in arts:
            title, link = a.get("title"), a.get("url")
            items.append({
                "id": make_id(title, link),
                "title": title,
                "url": link,
                "source": (a.get("source") or {}).get("name") or "NewsAPI",
                "published": norm_time(a.get("publishedAt")),
                "summary": (a.get("description") or "")[:2000],
                "raw": a,
            })
        if len(arts) < page_size: break
        page += 1
        time.sleep(0.2)
    return items
