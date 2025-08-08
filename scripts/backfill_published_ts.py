#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backfill Firestore 'articles' docs:
- published_ts (Timestamp) from string 'published'
- image_url from raw_json.urlToImage (if missing)
- categories normalize to array
"""

import argparse
import sys
import time
from typing import Optional, Dict, Any

import firebase_admin
from dateutil import parser as dtparse
from firebase_admin import credentials, firestore

BATCH_LIMIT = 450  # Firestore write batch ≤ 500, 여유 있게


def parse_args():
    p = argparse.ArgumentParser(description="Backfill published_ts/image_url/categories in Firestore")
    p.add_argument("--project", help="GCP project (optional; usually inferred from creds)")
    p.add_argument("--collection", default="articles", help="Firestore collection name")
    p.add_argument("--dry-run", action="store_true", help="Do not write; just log what would change")
    p.add_argument("--sleep", type=float, default=0.0, help="Between batches (seconds)")
    p.add_argument("--page-size", type=int, default=200, help="Docs per read page")
    return p.parse_args()


def init_firestore(project: Optional[str] = None) -> firestore.Client:
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, options={"projectId": project} if project else None)
    db = firestore.client()
    # 간단 헬스체크
    _ = db.collection("healthcheck").document("ping").get()
    return db


def is_timestamp_field_missing(doc: Dict[str, Any]) -> bool:
    return doc.get("published_ts") is None


def parse_ts(iso_str: Optional[str]):
    if not iso_str:
        return None
    try:
        return dtparse.isoparse(iso_str)
    except Exception:
        return None


def extract_image_url(raw: Any) -> Optional[str]:
    if isinstance(raw, dict):
        for k in ("urlToImage", "imageUrl", "image_url"):
            v = raw.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def ensure_categories_array(doc: Dict[str, Any]):
    # 기존에 categories가 string이거나 None일 수 있음 → array로 표준화
    cats = doc.get("categories")
    if isinstance(cats, list):
        return cats
    if isinstance(cats, str) and cats.strip():
        # CSV 가능성
        parts = [c.strip() for c in cats.split(",") if c.strip()]
        return sorted(set(parts))
    return []


def backfill(db: firestore.Client, col_name: str, dry_run: bool, page_size: int, sleep_s: float):
    col = db.collection(col_name)

    # 전체 스캔 (페이지네이션)
    last_doc = None
    total_scanned = total_updated = 0

    while True:
        q = col.order_by("published")  # 문자열 정렬로 페이징 (필드 존재 가정)
        if last_doc:
            q = q.start_after({"published": last_doc.get("published")})
        q = q.limit(page_size)

        docs = list(q.stream())
        if not docs:
            break

        batch = db.batch()
        batch_ops = 0

        for d in docs:
            total_scanned += 1
            data = d.to_dict() or {}
            updates = {}

            # 1) published_ts
            if is_timestamp_field_missing(data):
                ts = parse_ts(data.get("published"))
                if ts is not None:
                    updates["published_ts"] = ts

            # 2) image_url
            if not data.get("image_url"):
                img = extract_image_url(data.get("raw_json"))
                if img:
                    updates["image_url"] = img

            # 3) categories → array로 표준화
            normalized = ensure_categories_array(data)
            if normalized != data.get("categories"):
                updates["categories"] = normalized

            if updates:
                total_updated += 1
                if dry_run:
                    print(f"[DRY-RUN] would update {d.id}: {list(updates.keys())}")
                else:
                    batch.update(d.reference, updates)
                    batch_ops += 1

            last_doc = data  # 페이징용 (published 값 사용)

            # 배치 한도 도달 시 커밋
            if not dry_run and batch_ops >= BATCH_LIMIT:
                batch.commit()
                batch = db.batch()
                batch_ops = 0
                if sleep_s:
                    time.sleep(sleep_s)

        # 페이지 끝 → 남은 배치 커밋
        if not dry_run and batch_ops > 0:
            batch.commit()
            if sleep_s:
                time.sleep(sleep_s)

        # 다음 페이지로
        # (마지막 문서의 published 값 기준 start_after)
        # 위에서 last_doc는 data(dict) 저장했음
        # Firestore SDK는 도큐먼트 스냅샷도 가능하지만 여기선 필드 기반으로 진행

    print(f"Scanned: {total_scanned}, Updated: {total_updated}, Dry-run: {dry_run}")


def main():
    args = parse_args()
    try:
        db = init_firestore(project=args.project)
    except Exception as e:
        print("❌ Firestore 초기화 실패:", e, file=sys.stderr)
        sys.exit(1)
    try:
        backfill(db, args.collection, args.dry_run, args.page_size, args.sleep)
    except Exception as e:
        print("❌ 백필 도중 에러:", e, file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
