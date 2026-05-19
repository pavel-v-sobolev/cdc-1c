from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cdc_1C")
except PackageNotFoundError:
    __version__ = "dev"

from .MetadataReader import MetadataReader
from .DataReader import DataReader
from .ChangeReader import ChangeReader

__all__ = ["MetadataReader","DataReader","ChangeReader", "__version__"]

