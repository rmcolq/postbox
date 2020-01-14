from pkg_resources import get_distribution

try:
    __version__ = get_distribution("postbox").version
except:
    __version__ = "local"

from postbox import *
