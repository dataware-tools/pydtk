"""DB Handlers."""

# Version 2
from .v2 import BaseDBHandler as V2BaseDBHandler  # NOQA
from .v2 import BaseDBSearchEngine as V2BaseSearchEngine  # NOQA
from .v2 import MetaDBHandler as V2MetaDBHandler  # NOQA
from .v2 import TimeSeriesDBHandler as V2TimeSeriesDBHandler  # NOQA
from .v2 import TimeSeriesDBSearchEngine as V2TimeSeriesDBSearchEngine  # NOQA

# Version 3
from .v3 import DBHandler as V3DBHandler  # NOQA
from .v3 import DBSearchEngine as V3DBSearchEngine  # NOQA
from .v3 import MetaDBHandler as V3MetaDBHandler  # NOQA
from .v3 import TimeSeriesCassandraDBHandler as V3TimeSeriesCassandraDBHandler  # NOQA
from .v3 import TimeSeriesCassandraDBSearchEngine as V3TimeSeriesCassandraDBSearchEngine  # NOQA
from .v3 import TimeSeriesDBHandler as V3TimeSeriesDBHandler  # NOQA

# Version 4
from .v4 import AnnotationDBHandler as V4AnnotationDBHandler  # NOQA
from .v4 import DatabaseIDDBHandler as V4DatabaseIDDBHandler  # NOQA
from .v4 import DBHandler as V4DBHandler  # NOQA
from .v4 import MetaDBHandler as V4MetaDBHandler  # NOQA

# Default DB Handler
DBHandler = V4DBHandler
