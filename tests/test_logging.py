"""
Пометки режима загрузки в логе (CHANGES / FULL RELOAD), человекочитаемый размер ответа и
отсутствие построчного спама при разборе страницы.
"""

import logging
import threading

import pytest

from cdc_1c.common_functions import format_bytes
from cdc_1c.logging_config import (LOAD_MODE_CHANGES, LOAD_MODE_FULL, get_logger, load_mode,
                                   log_prefix)


@pytest.mark.parametrize("size, expected", [
    (0, "0 B"),
    (512, "512 B"),
    (1024, "1.0 KB"),
    (1536, "1.5 KB"),
    (10 * 1024 * 1024 + 200 * 1024, "10.2 MB"),
    (3 * 1024 ** 3, "3.0 GB"),
])
def test_format_bytes(size, expected):
    assert format_bytes(size) == expected


def test_log_prefix_only_inside_load_mode():
    assert log_prefix() == ''
    with load_mode(LOAD_MODE_CHANGES):
        assert log_prefix() == '[CHANGES] '
        with load_mode(LOAD_MODE_FULL):     # вложенный режим перекрывает
            assert log_prefix() == '[FULL RELOAD] '
        assert log_prefix() == '[CHANGES] '
    assert log_prefix() == ''               # режим снимается на выходе из блока


def test_adapter_prefixes_message(caplog):
    logger = get_logger('cdc_1c.test')
    with caplog.at_level(logging.INFO, logger='cdc_1c.test'):
        logger.info('Saving %s records', 5)
        with load_mode(LOAD_MODE_FULL):
            logger.info('Saving %s records', 5)

    assert caplog.messages == ['Saving 5 records', '[FULL RELOAD] Saving 5 records']


def test_load_mode_does_not_leak_between_threads():
    # Полная выгрузка идёт фоновым потоком параллельно с чтением изменений: режим одного потока
    # не должен просачиваться в другой (ради этого ContextVar, а не глобальная переменная).
    seen = {}

    def worker():
        seen['thread_before'] = log_prefix()
        with load_mode(LOAD_MODE_FULL):
            seen['thread_inside'] = log_prefix()

    with load_mode(LOAD_MODE_CHANGES):
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
        seen['main'] = log_prefix()

    assert seen == {'thread_before': '', 'thread_inside': '[FULL RELOAD] ',
                    'main': '[CHANGES] '}
