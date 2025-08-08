# news_collector/db_firestore.py
from __future__ import annotations

from typing import Dict, Any, List, Optional

import firebase_admin
from dateutil import parser as dtparse
from firebase_admin import credentials, firestore

_app = None
_db: Optional[firestore.Client] = None


def connect_firestore() -> firestore.Client:
    """Application Default Credentials(GOOGLE_APPLICATION_CREDENTIALS)로 Firestore 클라이언트 생성."""
    global _app, _db
    if _db:
        return _db
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        _app = firebase_admin.initialize_app(cred)
    _db = firestore.client()

    # 헬스체크: 프로젝트에 (default) DB가 없으면 여기서 친절한 에러
    try:
        _db.collection("healthcheck").document("ping").get()
    except Exception as e:
        raise RuntimeError(
            "Firestore 연결 실패: 프로젝트에 (default) 데이터베이스가 없거나 권한/지역 설정 문제가 있습니다. "
            "콘솔에서 Firestore를 Native 모드로 생성한 뒤 다시 시도하세요."
        ) from e
    return _db


def _parse_timestamp(iso_str: Optional[str]):
    """ISO8601 문자열 -> Python datetime (Firestore가 Timestamp로 저장). 실패 시 None."""
    if not iso_str:
        return None
    try:
        return dtparse.isoparse(iso_str)
    except Exception:
        return None


def _extract_image_url(raw: Any) -> Optional[str]:
    """NewsAPI 원본에서 대표 이미지 URL 추출."""
    if isinstance(raw, dict):
        url = raw.get("urlToImage") or raw.get("imageUrl") or raw.get("image_url")
        if isinstance(url, str) and url.strip():
            return url.strip()
    return None


def _to_doc(a: Dict) -> Dict[str, Any]:
    """
    수집 아이템 -> Firestore 문서 변환.
    - published (string, UTC ISO8601) 그대로 보존
    - published_ts (Timestamp) 추가 저장 (정렬/범위쿼리용)
    - categories: 배열로 저장
    - image_url: raw_json에서 추출해 명시 필드로 저장
    """
    cat = (a.get("category") or "").strip()
    cats: List[str] = [cat] if cat else []

    published_iso = a.get("published")
    ts = _parse_timestamp(published_iso)

    img = _extract_image_url(a.get("raw"))

    doc = {
        "title": a.get("title"),
        "url": a.get("url"),
        "source": a.get("source"),
        "published": published_iso,
        "published_ts": ts,  # ✅ Timestamp
        "summary": a.get("summary"),
        "categories": cats,  # 배열
        "image_url": img,
        "raw_json": a.get("raw"),
    }
    # None 값은 필드에서 제거
    return {k: v for k, v in doc.items() if v is not None}


def save_article(db: firestore.Client, a: Dict) -> bool:
    """
    Upsert 저장.
    - 새 문서는 set()
    - 기존 문서는 categories 병합 후 set(merge=True)
    반환: True=신규, False=기존 업데이트
    """
    ref = db.collection("articles").document(a["id"])
    snap = ref.get()
    data = _to_doc(a)

    if not snap.exists:
        ref.set(data)
        return True

    old = snap.to_dict() or {}
    old_c = set(old.get("categories") or [])
    new_c = set(data.get("categories") or [])
    merged_cats = sorted(old_c | new_c)

    # 기존 필드 유지 + 새로운 필드 병합
    update = data.copy()
    update["categories"] = merged_cats

    ref.set(update, merge=True)
    return False
