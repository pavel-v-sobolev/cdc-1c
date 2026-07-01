import logging

ODATA_PREFIX = 'StandardODATA.'

logger = logging.getLogger(__name__)

def parse_object_full_name(self, object_full_name):
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
