from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cdc_1C")
except PackageNotFoundError:
    __version__ = "dev"

from .MetadataReader1C import MetadataReader1C
from .DataReader1C import DataReader1C, DataObject1C
from .ChangeReader1C import ChangeReader1C
from .NameMapper1C import NameMapper1C

__all__ = ["MetadataReader1C", "DataReader1C", "DataObject1C", "ChangeReader1C", "NameMapper1C", "__version__"]

