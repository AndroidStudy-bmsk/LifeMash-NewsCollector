import json, sqlite3, datetime as dt
from dateutil import tz
from news_collector.collector import collect_categories


def test_collect_categories_filters_and_saves(monkeypatch, tmp_path):
    # 1) API 모킹: 둘 다 통과시키려면 '최근' 시간으로
    recent = (dt.datetime.now(tz=tz.UTC) - dt.timedelta(hours=1)).isoformat()

    def fake_fetch(api_key, category, country, page_size, max_pages, debug):
        return [
            {"id": "1", "title": "t1", "url": "u1", "source": "s",
             "published": recent, "summary": "s", "category": category, "raw": {}},
            {"id": "2", "title": "t2", "url": "u2", "source": "s",
             "published": None, "summary": "s", "category": category, "raw": {}},
        ]

    monkeypatch.setattr("news_collector.collector.fetch_top_headlines_category", fake_fetch)

    # 2) DB 격리: 임시 파일 DB로 connect_db를 대체
    from news_collector import db as dbmod
    tmpdb = tmp_path / "test.db"
    monkeypatch.setattr(
        "news_collector.collector.connect_db",
        lambda: dbmod.connect_db(str(tmpdb))
    )

    # 3) since_hours=None 로 시간 필터 끄거나, 위처럼 recent 로 설정
    out_json = tmp_path / "out.json"
    res = collect_categories(
        categories=["technology"],
        country="us",
        page_size=100,
        since_hours=None,  # ← 필터 끔 (대신 위에서 recent 로 보장)
        limit_per_cat=None,
        max_pages=1,
        api_key="KEY",
        to_json=str(out_json),
        debug=False,
    )

    assert res["technology"]["count"] == 2
    assert out_json.exists()
    data = json.loads(out_json.read_text())
    assert len(data) == 2
