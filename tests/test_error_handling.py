"""
Ошибки HTTP и ретрай цикла: тело ответа 1С попадает в ошибку/лог (raise_for_status),
а упавший run_forever ретраится с экспоненциальной паузой (см. replicator.BACKOFF_FACTOR).
"""

import pytest
import requests
from sqlalchemy import create_engine

from cdc_1c import Replicator1C
from cdc_1c.common_functions import MAX_ERROR_BODY_CHARS, raise_for_status
from cdc_1c.replicator import DEFAULT_MAX_BACKOFF


class _Resp:
    def __init__(self, status_code=500, text='', reason='Internal Server Error'):
        self.ok = 200 <= status_code < 300
        self.status_code = status_code
        self.text = text
        self.reason = reason
        self.url = 'http://1c/odata/SelectChanges'


def test_raise_for_status_keeps_body_and_context():
    # Содержательное у 1С только в теле — оно должно быть и в тексте ошибки, вместе с контекстом.
    resp = _Resp(text='Ошибка блокировки данных при попытке чтения')

    with pytest.raises(requests.HTTPError) as excinfo:
        raise_for_status(resp, 'SelectChanges (message 42)')

    message = str(excinfo.value)
    assert 'Ошибка блокировки данных' in message
    assert 'SelectChanges (message 42)' in message
    assert '500' in message
    assert excinfo.value.response is resp


def test_raise_for_status_truncates_long_body():
    with pytest.raises(requests.HTTPError) as excinfo:
        raise_for_status(_Resp(text='x' * (MAX_ERROR_BODY_CHARS + 500)), 'ctx')

    assert '[+500 chars]' in str(excinfo.value)


def test_raise_for_status_passes_ok_response():
    assert raise_for_status(_Resp(status_code=200, text='ok'), 'ctx') is None


def _replicator():
    return Replicator1C(
        odata_url='http://1c/odata',
        odata_auth=('u', 'p'),
        exchange_name='X',
        queue_guid='guid',
        engine=create_engine('sqlite://'),
    )


def _run_and_collect_delays(monkeypatch, error: Exception, waits: int, interval: float = 60.0):
    """Гоняет run_forever с падающим run_once, возвращая паузы, которые он запросил.
    Последняя итерация обрывается по max_iterations до паузы, поэтому итераций на одну больше."""
    repl = _replicator()
    delays = []

    def fake_run_once(*args, **kwargs):
        raise error

    monkeypatch.setattr(repl, 'run_once', fake_run_once)
    monkeypatch.setattr('cdc_1c.replicator._StopSignal.wait', lambda self, d: delays.append(d))
    repl.run_forever(interval=interval, max_iterations=waits + 1)
    return delays


def test_transient_failures_back_off_exponentially(monkeypatch):
    # Слепой повтор каждые interval секунд накладывает попытки друг на друга — пауза растёт.
    delays = _run_and_collect_delays(monkeypatch, requests.ConnectionError('1C is down'), 3)

    assert delays == [120.0, 240.0, 480.0]


def test_backoff_is_capped(monkeypatch):
    delays = _run_and_collect_delays(monkeypatch, requests.ConnectionError('1C is down'), 12)

    assert max(delays) == DEFAULT_MAX_BACKOFF
    assert delays[-1] == DEFAULT_MAX_BACKOFF


def test_permanent_error_goes_straight_to_max_backoff(monkeypatch):
    # 403 не лечится повтором: сразу потолок, а не 15 попыток по мере роста паузы.
    error = requests.HTTPError('forbidden', response=_Resp(status_code=403, reason='Forbidden'))
    delays = _run_and_collect_delays(monkeypatch, error, 2)

    assert delays == [DEFAULT_MAX_BACKOFF, DEFAULT_MAX_BACKOFF]


def test_backoff_resets_after_success(monkeypatch):
    repl = _replicator()
    delays = []
    calls = {'n': 0}

    def flaky_run_once(*args, **kwargs):
        calls['n'] += 1
        if calls['n'] in (1, 2):
            raise requests.ConnectionError('1C is down')

    monkeypatch.setattr(repl, 'run_once', flaky_run_once)
    monkeypatch.setattr(repl, '_dispatch_full_loads', lambda executor: None)
    monkeypatch.setattr('cdc_1c.replicator._StopSignal.wait', lambda self, d: delays.append(d))
    repl.run_forever(interval=60.0, max_iterations=4)

    assert delays == [120.0, 240.0, 60.0]
