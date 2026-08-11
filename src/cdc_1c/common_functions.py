import requests

from cdc_1c.logging_config import get_logger

ODATA_PREFIX = 'StandardODATA.'

logger = get_logger(__name__)

# Сколько символов тела ответа попадает в текст ошибки. 1С отдаёт описание ошибки (в т.ч. текст
# исключения и стек модуля) в теле; полный дамп в лог не нужен, но обрезать до пары строк мало.
MAX_ERROR_BODY_CHARS = 2000

BYTE_UNITS = ('B', 'KB', 'MB', 'GB', 'TB')


def format_bytes(size: float) -> str:
    """
    Размер ответа для лога в удобной единице: байты для мелочи, дальше КБ/МБ/ГБ. Ответы 1С
    различаются на порядки (страница справочника — килобайты, набор движений — мегабайты),
    и в сырых байтах разницу глазом не поймать.
    """
    for unit in BYTE_UNITS:
        if size < 1024 or unit == BYTE_UNITS[-1]:
            return f'{size:.0f} {unit}' if unit == BYTE_UNITS[0] else f'{size:.1f} {unit}'
        size /= 1024


def format_duration(seconds: float) -> str:
    """
    Длительность для лога: секунды с десятой долей, от минуты — «1m 04s», от часа — «1h 05m 03s».
    Обработка пакета занимает от долей секунды до десятков минут, и в сырых секундах такой разброс
    читается плохо.
    """
    if seconds < 60:
        return f'{seconds:.1f}s'
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f'{hours}h {minutes:02d}m {sec:02d}s'
    return f'{minutes}m {sec:02d}s'


def raise_for_status(response: requests.Response, context: str = '') -> None:
    """
    Замена response.raise_for_status(): всё содержательное в ответе 1С лежит в теле, а штатный
    raise_for_status отдаёт наружу только «HTTPError: 500» и причину из логов не видно.
    Тело (обрезанное) попадает и в лог, и в текст HTTPError.
    """
    if response.ok:
        return

    body = (response.text or '').strip()
    if len(body) > MAX_ERROR_BODY_CHARS:
        body = f'{body[:MAX_ERROR_BODY_CHARS]}... [+{len(body) - MAX_ERROR_BODY_CHARS} chars]'

    message = (f'1C request failed: {response.status_code} {response.reason} '
               f'for {context or response.url}: {body or "<empty body>"}')
    logger.error(message)
    raise requests.HTTPError(message, response=response)

def parse_object_full_name(object_full_name):
    """
    Очищаем имя объекта от разных префиксов, постфиксов и скобок.
    Возвращает очищенное имя и тип объекта
    """
    if object_full_name is None:
        logger.error(f'Object full name is None')
        return None, None

    object_name = object_full_name

    if object_name.startswith('Collection'):
        object_name = object_name.removeprefix('Collection(')
        object_name = object_name.removesuffix(')')

    object_name = object_name.removeprefix(ODATA_PREFIX)
    object_name = object_name.removesuffix('_RowType')

    if '_' in object_name:
        object_type = object_name.split('_')[0]
    else:
        logger.error(f'Object type not found in object full name {object_full_name}')
        return None, None
    return object_name, object_type
