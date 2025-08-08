from __future__ import annotations
import hashlib
from typing import Optional
from dateutil import tz, parser as dtparse


def make_id(title: Optional[str], url: Optional[str]) -> str:
    h = hashlib.sha256()
    h.update((title or "").encode("utf-8"))
    h.update((url or "").encode("utf-8"))
    return h.hexdigest()[:32]


def norm_time(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    try:
        d = dtparse.parse(dt_str)
        if not d.tzinfo:
            d = d.replace(tzinfo=tz.UTC)
        return d.astimezone(tz.UTC).isoformat()
    except Exception:
        return None
