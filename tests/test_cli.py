import json
from types import SimpleNamespace

from news_collector import cli


def test_cli_runs_domains_mode(monkeypatch, tmp_path):
    dom = tmp_path / "dom.json"
    dom.write_text(json.dumps({"technology": ["zdnet.co.kr"]}), encoding="utf-8")

    # collector 함수 모킹: 결과만 반환
    def fake_collect(**kwargs):
        return {"technology": {"saved": 1, "skipped": 0, "count": 1}}

    monkeypatch.setattr("news_collector.collector.collect_categories_domains_mode", lambda **kw: fake_collect(**kw))

    args = SimpleNamespace(
        categories=["technology"],
        country="us",
        page_size=100,
        max_pages=1,
        since_hours=None,
        limit=None,
        out=None,
        api_key="KEY",
        debug=False,
        domains_file=str(dom),
        languages="ko,en"
    )
    # run_once 직접 호출
    cli.run_once(args)
    # 별도의 assert는 생략(에러 없이 완료되는지 확인)
