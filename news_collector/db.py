from __future__ import annotations

import json
import sqlite3
from typing import Dict, Optional

from .constants import DB_PATH


def connect_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS articles(
        id TEXT PRIMARY KEY,
        title TEXT,
        url TEXT,
        source TEXT,
        published TEXT,
        summary TEXT,
        categories TEXT,
        raw_json TEXT
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pub ON articles(published)")
    cols = [r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()]
    if "categories" not in cols:
        conn.execute("ALTER TABLE articles ADD COLUMN categories TEXT")
    return conn


def _merge_categories(old_csv: Optional[str], new_cat: Optional[str]) -> str:
    s = set(c.strip() for c in (old_csv or "").split(",") if c.strip())
    if new_cat:
        s.add(new_cat.strip())
    return ",".join(sorted(s)) if s else ""


def save_article(conn: sqlite3.Connection, a: Dict) -> bool:
    try:
        conn.execute("""INSERT INTO articles(id,title,url,source,published,summary,categories,raw_json)
                        VALUES(?,?,?,?,?,?,?,?)""",
                     (a["id"], a.get("title"), a.get("url"), a.get("source"),
                      a.get("published"), a.get("summary"),
                      a.get("category", "") or "", json.dumps(a.get("raw"), ensure_ascii=False)))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        cur = conn.execute("SELECT categories FROM articles WHERE id=?", (a["id"],))
        row = cur.fetchone()
        merged = _merge_categories(row[0] if row else "", a.get("category", ""))
        conn.execute("UPDATE articles SET categories=? WHERE id=?", (merged, a["id"]))
        conn.commit()
        return False
