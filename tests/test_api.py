import responses
from news_collector.api import fetch_top_headlines_category, fetch_everything_by_domains


@responses.activate
def test_fetch_top_headlines_category_basic():
    responses.add(
        responses.GET, "https://newsapi.org/v2/top-headlines",
        json={"status": "ok", "articles": [
            {"source": {"name": "X"}, "title": "A", "url": "https://x/a", "publishedAt": "2025-08-01T00:00:00Z",
             "description": "d"}
        ]},
        status=200
    )
    items = fetch_top_headlines_category("KEY", "technology", country="us", page_size=100, max_pages=1)
    assert len(items) == 1
    assert items[0]["category"] == "technology"
    assert items[0]["url"] == "https://x/a"


@responses.activate
def test_fetch_everything_by_domains_pagination_stop_on_426():
    # page=1 OK
    responses.add(
        responses.GET, "https://newsapi.org/v2/everything",
        json={"status": "ok", "articles": [
            {"source": {"name": "X"}, "title": "A", "url": "https://x/a", "publishedAt": "2025-08-01T00:00:00Z"}]},
        status=200
    )
    # page=2 -> 426
    responses.add(
        responses.GET, "https://newsapi.org/v2/everything",
        json={"status": "error", "message": "upgrade"}, status=426
    )
    items = fetch_everything_by_domains("KEY", domains_csv="chosun.com", language="ko", max_pages=2, page_size=100,
                                        debug=True)
    assert len(items) == 1  # 2페이지에서 멈춤
