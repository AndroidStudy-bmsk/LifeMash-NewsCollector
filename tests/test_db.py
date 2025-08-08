import sqlite3
from news_collector.db import connect_db, save_article


def test_save_and_merge_categories(tmp_path):
    db_path = tmp_path / "db.sqlite"
    conn = connect_db(str(db_path))

    a = {
        "id": "aaa",
        "title": "t",
        "url": "u",
        "source": "s",
        "published": "2025-08-01T00:00:00+00:00",
        "summary": "sum",
        "category": "technology",
        "raw": {"foo": "bar"},
    }
    assert save_article(conn, a) is True

    # 같은 기사, 다른 카테고리 → INSERT 대신 UPDATE 병합
    a2 = dict(a, category="science")
    assert save_article(conn, a2) is False

    row = conn.execute("SELECT categories FROM articles WHERE id=?", ("aaa",)).fetchone()
    assert row is not None
    cats = set((row[0] or "").split(","))
    assert {"science", "technology"} <= cats
