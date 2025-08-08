import pytest


@pytest.fixture(autouse=True)
def no_env_leak(monkeypatch):
    # 테스트 중엔 NEWSAPI_KEY가 없어도 터지지 않게 기본값 주거나, 필요시 개별 테스트에서 설정
    monkeypatch.delenv("NEWSAPI_KEY", raising=False)


@pytest.fixture
def mem_db(monkeypatch, tmp_path):
    # news_collector.db.connect_db가 사용하는 경로를 인메모리로 바꾸고 싶다면
    # 현재 코드는 상수 경로를 쓰므로, sqlite3.connect(":memory:")를 직접 호출하는 대신
    # 테스트에서 connect_db 호출 후 커넥션을 그대로 재사용
    # 필요 시 monkeypatch로 함수 대체도 가능
    # 여기선 그냥 테스트마다 새 DB 파일을 씀
    db_path = tmp_path / "test.db"
    yield str(db_path)