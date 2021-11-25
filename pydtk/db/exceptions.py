"""Exceptions."""


class DatabaseNotInitializedError(BaseException):
    """Database is not initialized."""

    pass


class InvalidDatabaseConfigError(BaseException):
    """Database-config is not valid."""

    pass
