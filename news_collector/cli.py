from __future__ import annotations

import argparse
import json
import os
from typing import List

from news_collector.collector import collect_categories_domains_mode, collect_categories
from news_collector.constants import NEWSAPI_CATEGORIES


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Category-based News Collector (NewsAPI) - ko/en domains or top-headlines")
    p.add_argument("--categories", nargs="+", default=NEWSAPI_CATEGORIES)
    p.add_argument("--country", default="us")
    p.add_argument("--page-size", type=int, default=100)
    p.add_argument("--max-pages", type=int, default=1)
    p.add_argument("--since-hours", type=int, default=None)
    p.add_argument("--limit", type=int)
    p.add_argument("--out")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--domains-file", help="category->domains JSON. If set, use /v2/everything with languages.")
    p.add_argument("--languages", default="ko,en", help="comma-separated (e.g., ko,en)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    invalid = [c for c in args.categories if c not in NEWSAPI_CATEGORIES]
    if invalid:
        raise ValueError(f"invalid category: {', '.join(invalid)}")
    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        raise ValueError("NEWSAPI_KEY environment variable not set")
    if args.domains_file:
        langs: List[str] = [s.strip() for s in args.languages.split(",") if s.strip()]
        result = collect_categories_domains_mode(
            categories=args.categories,
            page_size=args.page_size,
            since_hours=args.since_hours,
            limit_per_cat=args.limit,
            max_pages=args.max_pages,
            api_key=api_key,
            to_json=args.out,
            languages=langs,
            domains_file=args.domains_file,
            debug=args.debug,
        )
    else:
        result = collect_categories(
            categories=args.categories,
            country=args.country,
            page_size=args.page_size,
            since_hours=args.since_hours,
            limit_per_cat=args.limit,
            max_pages=args.max_pages,
            api_key=api_key,
            to_json=args.out,
            debug=args.debug,
        )
    print(json.dumps([{"category": k, **v} for k, v in result.items()], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
