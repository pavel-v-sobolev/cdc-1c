"""
Пакет пользовательских обработчиков. Обычный python-пакет — никакого сканирования каталогов и
особых соглашений об именах файлов: что импортировали и передали в HandlerLoop, то и работает.

Отсюда же берётся и общий код: соседний модуль подключается обычным
`from handlers._common import ...`.
"""

from .zakazy_klientov import ZakazyKlientov
from .zakazy_klientov_grouped import ZakazyKlientovGrouped

__all__ = ["ZakazyKlientov", "ZakazyKlientovGrouped"]
