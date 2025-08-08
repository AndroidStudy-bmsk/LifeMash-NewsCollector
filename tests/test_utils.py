from news_collector.utils import make_id, norm_time


def test_make_id_stable():
    a = make_id("title", "https://example.com")
    b = make_id("title", "https://example.com")
    assert a == b and len(a) == 32


def test_make_id_robust_to_none():
    assert make_id(None, None) != ""
    assert len(make_id(None, None)) == 32


def test_norm_time_ok():
    s = norm_time("2025-08-01T10:00:00+09:00")
    assert s.endswith("+00:00")  # UTC 변환


def test_norm_time_none():
    assert norm_time(None) is None
    assert norm_time("not-a-date") is None
