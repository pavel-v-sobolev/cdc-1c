import logging

import requests

ODATA_PREFIX = 'StandardODATA.'

logger = logging.getLogger(__name__)

# Сколько символов тела ответа попадает в текст ошибки. 1С отдаёт описание ошибки (в т.ч. текст
# исключения и стек модуля) в теле; полный дамп в лог не нужен, но обрезать до пары строк мало.
MAX_ERROR_BODY_CHARS = 2000


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
