from dags.s3_service.base_data_loader import BaseDataLoader
from dags.s3_service.data_validators import fraud_validators
from dags.settings.fraud_etl_settings import Config
from dags.settings.logger import get_airflow_logger

logger = get_airflow_logger(__name__)


class TransactionDataLoader(BaseDataLoader):
    def __init__(self, s3_connection_config, bucket_name):
        """
        Transaction-specific data loader.

        :param s3_connection_config: Configuration details for S3 connection.
        :param bucket_name: The name of the S3 bucket.
        """
        super().__init__(
            s3_connection_config=s3_connection_config, bucket_name=bucket_name
        )

    def load_file(self, file_stream, file_key):
        """
        Loads the file based on the file key and returns the corresponding DataFrame.

        :param file_stream: The stream of data loaded from S3.
        :param file_key: The key corresponding to the file.
        :return: Loaded DataFrame.
        """
        try:
            logger.info(f"Loading file for key: {file_key}")
            loader = fraud_validators.get(file_key)
            if loader:
                return loader(file_stream)
            else:
                logger.warning(f"No loader defined for {file_key}. Returning None.")
                return None
        except Exception as e:
            logger.error(f"Error loading file for key {file_key}: {e}")
            raise RuntimeError(f"Failed to load file for key {file_key}: {e}")

    def transform_data(self, transformer):
        """
        Transforms the loaded data using the provided transformer classes.
        """
        transformed_data = {}
        for file_key, df in self.dataframes.items():
            try:
                transformer = transformer.get(file_key)
                if transformer and df is not None:
                    logger.info(f"Transforming data for key: {file_key}")
                    transformed_data[file_key] = transformer.transform(df)
                else:
                    logger.warning(
                        f"No transformer found or empty DataFrame for {file_key}. Skipping."
                    )
            except Exception as e:
                logger.error(f"Error transforming data for key {file_key}: {e}")
                raise RuntimeError(f"Failed to transform data for key {file_key}: {e}")
        return transformed_data


def get_transaction_loader(config: Config) -> TransactionDataLoader:
    loader = TransactionDataLoader(
        config.minio.s3_connection,
        config.minio.bucket_name,
    )
    return loader
