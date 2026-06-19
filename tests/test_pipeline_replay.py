"""
Оффлайн replay-тест полного пайплайна Replicator1C.

Поднимает фейковый сервер 1С (tests/fake_1c.py), который проигрывает записанные ответы, и гоняет
против него реальный Replicator1C с sqlite-БД. Без живой 1С и без Postgres — годится для CI.

Параметризуется по подпапкам tests/responses/* : добавление новой конфигурации (версия/конфигурация
1С) автоматически добавляет тест-кейсы.
"""

import sqlite3
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

import fake_1c  # соседний модуль в tests/ (pytest добавляет каталог теста в sys.path)
from cdc_1c import Replicator1C

# sqlite не умеет биндить объект uuid.UUID: при повторном merge в существующую таблицу dbmerge
# рефлектит тип колонки как generic (без SQLAlchemy Uuid()-процессора, который конвертит UUID->str).
# Регистрируем адаптер для теста. В проде целевая БД — Postgres с нативным UUID, там это не нужно.
sqlite3.register_adapter(uuid.UUID, str)

RESPONSES_DIR = Path(__file__).parent / "responses"
CONFIGS = sorted(p for p in RESPONSES_DIR.iterdir() if (p / "manifest.json").exists())


@pytest.fixture
def fake_server(request):
    """Поднимает фейковый сервер для конфигурации request.param, отдаёт (odata_url, fake)."""
    with fake_1c.running_server(request.param) as (odata_url, fake):
        yield odata_url, fake


def _make_replicator(odata_url, queue_guid, tmp_path):
    return Replicator1C(
        odata_url=odata_url,
        odata_auth=None,                  # фейковый сервер не проверяет auth
        exchange_name="ДляODATA",   # сервер матчит ExchangePlan по пути, имя не важно
        queue_guid=queue_guid,
        engine=create_engine(f"sqlite:///{tmp_path / 'cdc.db'}"),
        db_schema=None,
    )


def _row_count(engine) -> int:
    total = 0
    with engine.connect() as conn:
        for table in inspect(engine).get_table_names():
            total += conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
    return total


@pytest.mark.parametrize("fake_server", CONFIGS, ids=[p.name for p in CONFIGS], indirect=True)
def test_run_once_replay(fake_server, tmp_path):
    odata_url, fake = fake_server
    repl = _make_replicator(odata_url, fake.queue_guid, tmp_path)

    repl.run_once()

    # Метаданные прочитаны, первый пакет сохранён в БД.
    assert len(repl.metadata) > 0
    assert inspect(repl.engine).get_table_names()
    assert _row_count(repl.engine) >= 1
    # notify прошёл → состояние очереди продвинулось на первый пакет.
    assert fake.received_no == 1


@pytest.mark.parametrize("fake_server", CONFIGS, ids=[p.name for p in CONFIGS], indirect=True)
def test_run_forever_replay(fake_server, tmp_path):
    odata_url, fake = fake_server
    n_batches = len(fake.batches)
    repl = _make_replicator(odata_url, fake.queue_guid, tmp_path)

    # interval=0 — без пауз; max_iterations=n_batches — обработать все записанные пакеты.
    repl.run_forever(interval=0, max_iterations=n_batches)

    # Все пакеты подтверждены по очереди → счётчик дошёл до последнего MessageNo.
    assert fake.received_no == n_batches
    assert _row_count(repl.engine) >= n_batches
