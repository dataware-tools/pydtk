import sys

if sys.version_info[0] < 3:
    raise Exception("Python >= 3.7 is required")

if sys.version_info[1] < 7:
    raise Exception("Python >= 3.7 is required")

if sys.version_info[1] == 7:
    from .pql_python37 import pql
else:
    from .pql_python38 import pql
