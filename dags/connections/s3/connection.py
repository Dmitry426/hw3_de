__all__ = ("get_s3_hook", "get_s3_resource")

from functools import lru_cache

import backoff
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from boto3.resources.base import ServiceResource
from botocore.exceptions import (
    ClientError,
    ConnectionError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ParamValidationError,
)

from dags.settings.logger import get_airflow_logger

logger = get_airflow_logger(__name__)


class InvalidS3CredentialsError(ValueError):
    """Custom exception for invalid S3 credentials."""


@backoff.on_exception(
    backoff.expo,
    (EndpointConnectionError, ConnectionError, ConnectTimeoutError),
    max_time=60,
    max_tries=3,
)
@lru_cache(maxsize=None)
def get_s3_hook(s3_connection) -> S3Hook:
    """Get S3Hook object. Uses cache to make it a singleton."""
    try:
        return S3Hook(
            s3_connection,
            transfer_config_args={
                "use_threads": False,
            },
        )
    except (ParamValidationError, ClientError) as error:
        logger.error(
            "S3 credentials error for connection '%s': %s", s3_connection, error
        )
        raise InvalidS3CredentialsError(
            f"The AWS parameters provided for connection '{s3_connection}' are incorrect: {error}"
        ) from error


@backoff.on_exception(
    backoff.expo,
    (EndpointConnectionError, ConnectionError, ConnectTimeoutError),
    max_time=60,
    max_tries=3,
)
def get_s3_resource(s3_connection) -> ServiceResource:
    """Get a boto3 S3 resource for the specified connection."""
    try:
        s3 = get_s3_hook(s3_connection)
        session = s3.get_session(s3.conn_config.region_name)
        return session.resource("s3", endpoint_url=s3.conn_config.endpoint_url)
    except (ParamValidationError, ClientError) as error:
        logger.error(
            "Failed to get S3 resource for connection '%s': %s", s3_connection, error
        )
        raise InvalidS3CredentialsError(
            f"Error obtaining S3 resource for connection '{s3_connection}': {error}"
        ) from error
