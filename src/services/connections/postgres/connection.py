__all__ = ["get_session", "get_engine"]

import threading
from contextlib import contextmanager

import backoff
from airflow.hooks.base import BaseHook
from psycopg2 import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import scoped_session, sessionmaker

from src.settings.logger import get_airflow_logger

_session_lock = threading.Lock()
_sessions = {}

logger = get_airflow_logger(__name__)


def is_connection_issue(exc: Exception) -> bool:
    """
    Determine if the exception is a connection-related issue.

    :param exc: The exception instance.
    :return: True if it's connection-related, False otherwise.
    """
    connection_errors = [
        "could not connect to server",
        "connection timed out",
        "connection refused",
        "server closed the connection unexpectedly",
    ]
    return any(error in str(exc).lower() for error in connection_errors)


def get_engine(conn_id: str) -> Engine:
    """
    Create and return an SQLAlchemy engine for the given Airflow connection ID.

    :param conn_id: The Airflow connection ID.
    :return: An SQLAlchemy Engine.
    """
    db_hook = BaseHook.get_hook(conn_id=conn_id)
    engine = create_engine(db_hook.get_uri())
    return engine


@backoff.on_exception(
    backoff.expo,
    OperationalError,
    max_time=60,
    max_tries=3,
    giveup=lambda exc: not is_connection_issue(exc),
)
@contextmanager
def get_session(conn_id: str):
    """
    Singleton context manager to provide a SQLAlchemy session for the given Airflow connection ID.

    :param conn_id: The Airflow connection ID.
    :yield: A SQLAlchemy session object.
    """
    global _sessions

    with _session_lock:
        if conn_id not in _sessions:
            engine = get_engine(conn_id)
            session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=True)
            _sessions[conn_id] = scoped_session(session_factory)

    session = _sessions[conn_id]()
    try:
        yield session
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
