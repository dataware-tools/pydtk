"""DB Handlers."""

# Version 2
from .v2 import BaseDBHandler as V2BaseDBHandler
from .v2 import TimeSeriesDBHandler as V2TimeSeriesDBHandler
from .v2 import MetaDBHandler as V2MetaDBHandler
from .v2 import BaseDBSearchEngine as V2BaseSearchEngine
from .v2 import TimeSeriesDBSearchEngine as V2TimeSeriesDBSearchEngine

# Version 3
from .v3 import DBHandler as V3DBHandler
from .v3 import TimeSeriesDBHandler as V3TimeSeriesDBHandler
from .v3 import MetaDBHandler as V3MetaDBHandler
from .v3 import TimeSeriesCassandraDBHandler as V3TimeSeriesCassandraDBHandler
from .v3 import DBSearchEngine as V3DBSearchEngine
from .v3 import TimeSeriesCassandraDBSearchEngine as V3TimeSeriesCassandraDBSearchEngine

# Version 4
from .v4 import DBHandler as V4DBHandler
from .v4 import MetaDBHandler as V4MetaDBHandler
from .v4 import DatabaseIDDBHandler as V4DatabaseIDDBHandler

# Default DB Handler
DBHandler = V4DBHandler
