from io import BytesIO
from typing import Dict

from dags.connections.s3.connection import get_s3_hook
from dags.settings.logger import get_airflow_logger

logger = get_airflow_logger(__name__)


class BaseDataLoader:
    def __init__(self, s3_connection_config, bucket_name):
        """
        Base class to load data from S3 and manage the loaded data.

        :param s3_connection_config: Configuration details for S3 connection.
        :param bucket_name: The name of the S3 bucket.
        """
        self.s3_hook = get_s3_hook(s3_connection_config)
        self.bucket_name = bucket_name
        self.dataframes = {}

    def load_data_from_s3(self, file_names: Dict[str, str]) -> Dict:
        """
        Loads data from S3 into DataFrames using the specified file names.

        :param file_names: A dictionary where keys are file keys and values are file names in S3.
        :return: A dictionary of loaded DataFrames.
        """
        logger.info(f"Starting to load files: {file_names}")

        try:
            for file_key, file_name in file_names.items():
                try:
                    logger.info(f"Loading {file_name} from S3...")
                    file_obj = self.s3_hook.get_key(
                        key=file_name,
                        bucket_name=self.bucket_name,
                    )
                    file_stream = BytesIO(file_obj.get()["Body"].read())
                    self.dataframes[file_key] = self.load_file(file_stream, file_key)
                    logger.info(f"Successfully loaded {file_name}.")
                except Exception as e:
                    logger.error(f"Failed to load {file_name}: {e}")
                    self.dataframes[file_key] = None

        except Exception as e:
            logger.error(f"Failed to load data from S3: {e}")
            raise RuntimeError(f"Error loading data: {e}")

        return self.dataframes

    def load_file(self, file_stream, file_key):
        """
        Method to be overridden by the child class to define how to load each file.

        :param file_stream: The stream of data loaded from S3.
        :param file_key: The key corresponding to the file.
        :return: Loaded data (DataFrame or other format).
        """
        raise NotImplementedError(
            "Child class must implement this method to load the data."
        )
