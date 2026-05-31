_TRANSLIT_TABLE = str.maketrans({
    'а': 'a',  'б': 'b',  'в': 'v',  'г': 'g',  'д': 'd',
    'е': 'e',  'ё': 'yo', 'ж': 'zh', 'з': 'z',  'и': 'i',
    'й': 'j',  'к': 'k',  'л': 'l',  'м': 'm',  'н': 'n',
    'о': 'o',  'п': 'p',  'р': 'r',  'с': 's',  'т': 't',
    'у': 'u',  'ф': 'f',  'х': 'kh', 'ц': 'ts', 'ч': 'ch',
    'ш': 'sh', 'щ': 'sch','ъ': '',   'ы': 'y',  'ь': '',
    'э': 'e',  'ю': 'yu', 'я': 'ya',
    'А': 'A',  'Б': 'B',  'В': 'V',  'Г': 'G',  'Д': 'D',
    'Е': 'E',  'Ё': 'Yo', 'Ж': 'Zh', 'З': 'Z',  'И': 'I',
    'Й': 'J',  'К': 'K',  'Л': 'L',  'М': 'M',  'Н': 'N',
    'О': 'O',  'П': 'P',  'Р': 'R',  'С': 'S',  'Т': 'T',
    'У': 'U',  'Ф': 'F',  'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch',
    'Ш': 'Sh', 'Щ': 'Sch','Ъ': '',   'Ы': 'Y',  'Ь': '',
    'Э': 'E',  'Ю': 'Yu', 'Я': 'Ya',
})


def _translit(s: str) -> str:
    return s.translate(_TRANSLIT_TABLE)


class NameMapper1C:
    """
    Транслитерирует русские имена объектов и полей 1С в латиницу.
    manual_mapping позволяет задать точные переводы, минуя транслитерацию.
    Ключи могут быть как именами объектов ("Document_ЗаказКлиента"),
    так и именами полей ("НомерСтроки").
    """

    def __init__(self, manual_mapping: dict[str, str] | None = None):
        self.manual_mapping: dict[str, str] = manual_mapping or {}

    def map_name(self, name: str) -> str:
        if name in self.manual_mapping:
            return self.manual_mapping[name]
        return _translit(name)

    def map_object_name(self, name: str) -> str:
        """
        Имя объекта вида "Document_ЗаказКлиента":
        тип (Document) оставляем без изменений, русскую часть транслитерируем.
        """
        if name in self.manual_mapping:
            return self.manual_mapping[name]
        if '_' in name:
            prefix, _, rest = name.partition('_')
            return f'{prefix}_{_translit(rest)}'
        return _translit(name)

    def map_field_name(self, name: str) -> str:
        return self.map_name(name)
