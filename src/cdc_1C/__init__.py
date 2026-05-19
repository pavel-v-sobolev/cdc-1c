from importlib.metadata import version, PackageNotFoundError

# try:
#     __version__ = version("CDC_1C")
# except PackageNotFoundError:
#     __version__ = "dev"

from .MetadataReader import MetadataReader
from .DataReader import DataReader

__all__ = ["MetadataReader","DataReader"]

#__all__ = ["MetadataReader", "__version__"]