__all__ = "fraud_validators"

from io import BytesIO
import polars as pl

from src.settings.logger import get_airflow_logger

LOG = get_airflow_logger(__name__)

def load_transactions(file_stream: BytesIO) -> pl.DataFrame:
    """
    Custom loader for transaction files.
    """
    LOG.info("Loading transaction data...")
    df = pl.read_csv(file_stream, separator=";")
    df = df.with_columns(
        pl.col("amount").str.replace(",", ".").cast(pl.Float64)
    )
    return df


def load_passport_blacklist(file_stream: BytesIO) -> pl.DataFrame:
    """
    Custom loader for passport_blacklist files.
    """
    LOG.info("Loading passport blacklist data...")
    return pl.read_excel(file_stream)


def load_terminals(file_stream: BytesIO) -> pl.DataFrame:
    """
    Custom loader for terminal files.
    """
    LOG.info("Loading terminal data...")
    return pl.read_excel(file_stream)



fraud_validators = {
        "transactions": load_transactions,
        "passport_blacklist": load_passport_blacklist,
        "terminals": load_terminals,
    }
