import httpx
from sqlalchemy.exc import (
    DBAPIError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import (
    TimeoutError as SQLAlchemyTimeoutError,
)


def is_database_error(error: Exception) -> bool:
    return isinstance(
        error, (DBAPIError, SQLAlchemyTimeoutError, ConnectionError, OSError)
    )


def is_retryable_database_error(error: Exception) -> bool:
    if isinstance(error, DBAPIError):
        code = getattr(error.orig, "sqlstate", None) or getattr(
            error.orig, "pgcode", None
        )
        if code:
            return code.startswith("08") or code in {
                "40001",
                "40P01",
                "55P03",
                "57P01",
                "57P02",
                "57P03",
                "53300",
            }
        return error.connection_invalidated or isinstance(error, DATABASE_RETRY_ERRORS)
    return isinstance(error, (SQLAlchemyTimeoutError, ConnectionError, OSError))


DATABASE_RETRY_ERRORS = (
    OperationalError,
    InterfaceError,
    SQLAlchemyTimeoutError,
)


def is_retryable_error(error: Exception) -> bool:
    if isinstance(error, httpx.HTTPStatusError):
        status_code = error.response.status_code
        return status_code in {408, 429} or 500 <= status_code < 600

    if isinstance(
        error,
        (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
            httpx.ProxyError,
            TimeoutError,
        ),
    ):
        return True

    return is_retryable_database_error(error)


def get_retry_counter_key(error: Exception) -> str:
    if is_database_error(error):
        return "db_retry_count"

    return "webhook_retry_count"
