"""
Entrypoint для запуска из окружения: `python -m cdc_1c` или команда `cdc-1c`.

Настройки читаются из переменных окружения CDC1C_* (см. Config.from_env), из них строится
Replicator1C. Режим — CDC1C_MODE: `loop` (по умолчанию) запускает run_forever с периодом
CDC1C_POLL_INTERVAL, `once` — один run_once.
"""
import logging

from cdc_1c.config import Config
from cdc_1c.replicator import Replicator1C


def main() -> None:
    config = Config.from_env()
    replicator = Replicator1C.from_config(config)
    # Уровень логирования из конфига — после конструктора: он вешает обработчик на логгер cdc_1c
    # (по умолчанию INFO), а тут переопределяем на заданный (например, DEBUG/WARNING).
    logging.getLogger("cdc_1c").setLevel(config.log_level.upper())

    if config.mode == "once":
        replicator.run_once()
    elif config.mode == "loop":
        replicator.run_forever(interval=config.poll_interval)
    else:
        raise SystemExit(f"Unknown CDC1C_MODE={config.mode!r} (expected 'loop' or 'once')")


if __name__ == "__main__":
    main()
