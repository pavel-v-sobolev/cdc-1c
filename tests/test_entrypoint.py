"""
Оффлайн-тесты entrypoint (python -m cdc_1c): выбор режима CDC1C_MODE и параметров из окружения.
Replicator1C.from_config подменяется — сеть/БД не задействуются.
"""

import pytest

import cdc_1c.__main__ as entry


class _FakeReplicator:
    def __init__(self):
        self.calls = []

    def run_once(self, *args, **kwargs):
        self.calls.append(("once", args, kwargs))

    def run_forever(self, *args, **kwargs):
        self.calls.append(("forever", args, kwargs))


def _set_env(monkeypatch, **extra):
    env = {"CDC1C_ODATA_URL": "http://x", "CDC1C_EXCHANGE_NAME": "E",
           "CDC1C_QUEUE_GUID": "Q", "CDC1C_DB_URL": "sqlite://"}
    env.update(extra)
    for k, v in env.items():
        monkeypatch.setenv(k, v)


def _patch_from_config(monkeypatch, replicator):
    monkeypatch.setattr(entry.Replicator1C, "from_config",
                        classmethod(lambda cls, cfg: replicator))


def test_main_loop_mode(monkeypatch):
    _set_env(monkeypatch, CDC1C_MODE="loop", CDC1C_POLL_INTERVAL="5")
    rep = _FakeReplicator()
    _patch_from_config(monkeypatch, rep)

    entry.main()
    assert rep.calls == [("forever", (), {"interval": 5.0})]


def test_main_loop_is_default(monkeypatch):
    _set_env(monkeypatch)   # без CDC1C_MODE → loop, период по умолчанию 60
    rep = _FakeReplicator()
    _patch_from_config(monkeypatch, rep)

    entry.main()
    assert rep.calls == [("forever", (), {"interval": 60.0})]


def test_main_once_mode(monkeypatch):
    _set_env(monkeypatch, CDC1C_MODE="once")
    rep = _FakeReplicator()
    _patch_from_config(monkeypatch, rep)

    entry.main()
    assert rep.calls == [("once", (), {})]


def test_main_unknown_mode(monkeypatch):
    _set_env(monkeypatch, CDC1C_MODE="bogus")
    _patch_from_config(monkeypatch, _FakeReplicator())

    with pytest.raises(SystemExit):
        entry.main()
