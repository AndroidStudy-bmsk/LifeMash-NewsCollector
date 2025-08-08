# news_collector/db_firestore.py
from __future__ import annotations

from typing import Dict, List

import firebase_admin
from firebase_admin import credentials, firestore

_app = None
_db = None


def connect_firestore() -> firestore.Client:
    global _app, _db
    if _db:
        return _db
    # GOOGLE_APPLICATION_CREDENTIALS 환경변수를 사용
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        _app = firebase_admin.initialize_app(cred)
    _db = firestore.client()
    return _db


def to_firestore_doc(a: Dict) -> Dict:
    """
    SQLite용 dict -> Firestore용 dict으로 변환.
    권장: categories는 배열로 저장. (없으면 빈 배열)
    published는 문자열(ISO) 그대로 두거나, Timestamp로 추가 필드 병행 가능.
    """
    cats: List[str] = []
    cat = (a.get("category") or "").strip()
    if cat:
        cats = [cat]
    doc = {
        "title": a.get("title"),
        "url": a.get("url"),
        "source": a.get("source"),
        "published": a.get("published"),  # ISO 문자열
        "summary": a.get("summary"),
        "categories": cats,  # 배열로 저장
        "raw_json": a.get("raw"),  # Firestore 문서 최대 1MiB 유의
    }
    return {k: v for k, v in doc.items() if v is not None}


def save_article(db: firestore.Client, a: Dict) -> bool:
    """
    Upsert. 이미 있으면 categories 병합.
    반환: True=신규 생성, False=기존 문서 업데이트(병합)
    """
    doc_ref = db.collection("articles").document(a["id"])
    snap = doc_ref.get()
    data = to_firestore_doc(a)

    if not snap.exists:
        # 신규
        doc_ref.set(data)
        return True
    else:
        # 기존 → categories 병합
        old = snap.to_dict() or {}
        old_cats = set(old.get("categories") or [])
        new_cats = set(data.get("categories") or [])
        merged = sorted(old_cats.union(new_cats))
        update = data.copy()
        update["categories"] = merged
        doc_ref.set(update, merge=True)
        return False
