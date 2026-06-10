import logging

# Пакетный логгер. Логгеры модулей (getLogger(__name__) → cdc_1c.replicator и т.п.) — его потомки,
# поэтому хендлер/уровень, выставленные здесь, действуют на весь пакет.
logger = logging.getLogger("cdc_1c")


def _ensure_handler(level: int = logging.INFO) -> None:
    """
    Настроить вывод логов cdc_1c в stderr — только если логирование ещё не настроено
    (ни на логгере cdc_1c, ни выше по цепочке до root). Если приложение уже настроило
    логирование, ничего не делаем: не перебиваем его выбор уровня и не плодим дубли.

    Вызывается из конструктора Replicator1C (точка начала работы), а не на импорте: к этому
    моменту приложение, если хотело, уже сконфигурировало логирование, и hasHandlers() даёт
    корректный снимок. Идемпотентна — после первого вызова свой хендлер уже висит на cdc_1c,
    и hasHandlers() вернёт True.
    """
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(level)
