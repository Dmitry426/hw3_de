""" Dag to   """

from datetime import timedelta
from io import BytesIO

from airflow import DAG

from airflow.utils.dates import days_ago

from src.services.db_manarger import create_databases, insert_into_table, truncate_tables_with_ddl, \
    query_from_schema, handle_scd2_updates, join_stg_and_dim_data
from src.services.validators.fraud_validators import fraud_validators
from src.services.connections.s3.connection import get_s3_hook
from src.settings.bank_etl.fraud_etl_settings import Config
from src.settings.logger import get_airflow_logger
from src.test import PROJECT_ROOT

LOG = get_airflow_logger(__name__)

CONFIG = Config.from_yaml((PROJECT_ROOT.parent / "configs" / "fraud_etl.yaml"))

DEFAULT_ARGS = {
    "owner": "Dmitrii",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "fraud_etl",
    tags=["fraud"],
    catchup=True,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)

def migrate(*args, **kwargs):
    LOG.info("Making tables")
    connection = CONFIG.db.intermediate_database
    ddl_migrations = [CONFIG.stg.ddl, CONFIG.dim.ddl, CONFIG.fact.ddl, CONFIG.report.ddl]

    for ddl in ddl_migrations:
        create_databases(connection, (PROJECT_ROOT.parent / "configs" / ddl))



def prepare_fraud_loaded_data(target_date, **kwargs):
    """
    Dag prepares preloaded transaction , passport blacklist and terminal
        data from minio to stg tables
    """
    connection = CONFIG.db.intermediate_database

    file_names = CONFIG.minio.file_templates.generate_file_names(
        target_date, date_format=CONFIG.minio.files_date_format
    )
    print(f"Generated file names for {target_date}: {file_names}")

    s3_hook = get_s3_hook(CONFIG.minio.s3_connection)
    dataframes = {}
    try:
        for file_key, file_name in file_names.items():
            try:
                print(f"Loading {file_name} directly into Polars from S3...")
                file_obj = s3_hook.get_key(
                    key=file_name,
                    bucket_name=CONFIG.minio.bucket_name,
                )
                file_stream = BytesIO(file_obj.get()["Body"].read())

                loader = fraud_validators.get(file_key)

                if loader:
                    df = loader(file_stream)
                    dataframes[file_key] = df
                    print(f"Loaded {file_name} into Polars DataFrame.")
                else:
                    print(f"No loader defined for {file_key}. Skipping file.")

            except Exception as e:
                print(f"Failed to load {file_name}: {e}")
                raise RuntimeError(f"Error loading {file_name}: {e}")

        passport_blacklist_data = [{"date": row["date"], "passport": row["passport"]} for row in
                                   dataframes["passport_blacklist"].iter_rows(named=True)]

        transactions_data = [{"transaction_id": row["transaction_id"], "transaction_date": row["transaction_date"],
                              "amount": row["amount"], "card_num": row["card_num"], "oper_type": row["oper_type"],
                              "oper_result": row["oper_result"], "terminal": row["terminal"]}
                             for row in dataframes["transactions"].iter_rows(named=True)]

        terminals_data = [{"terminal_id": row["terminal_id"], "terminal_type": row["terminal_type"],
                           "terminal_city": row["terminal_city"], "terminal_address": row["terminal_address"]}
                          for row in dataframes["terminals"].iter_rows(named=True)]

        insert_into_table(connection, CONFIG.stg.stg_blacklist_name, passport_blacklist_data)
        insert_into_table(connection, CONFIG.stg.stg_transactions_name, transactions_data)
        insert_into_table(connection, CONFIG.stg.stg_terminals_name, terminals_data)

        db_data = {
            "accounts": CONFIG.stg.stg_accounts_name,
            "cards": CONFIG.stg.stg_cards_name,
            'clients': CONFIG.stg.stg_clients_name
        }
        for table, stg_table in db_data.items():
            data = query_from_schema(conn_id=connection, table_name=table, schema="info")
            insert_into_table(conn_id=CONFIG.db.intermediate_database, table_name=stg_table, data=data)


    except Exception as main_exc:
        truncate_tables_with_ddl(connection, (PROJECT_ROOT.parent / "configs" / CONFIG.stg.truncate_ddl))
        LOG.critical("Critical error in loading and inserting data. Halting DAG execution.", exc_info=True)
        raise RuntimeError("DAG execution halted due to critical error.") from main_exc


def safe_populate_dim_tables(**kwargs):
    tables_info = {
        "dim_terminals": {
            "unique_columns": ["terminal_id"],
            "compare_columns": ["terminal_type", "terminal_city", "terminal_address"],
            "staging_name": CONFIG.stg.stg_terminals_name,
            "dim_name": CONFIG.dim.dim_terminals_name
        },
        "dim_clients": {
            "unique_columns": ["client_id"],
            "compare_columns": ["last_name", "first_name", "patronymic", "passport_num", "phone"],
            "staging_name": CONFIG.stg.stg_clients_name,
            "dim_name": CONFIG.dim.dim_clients_name
        },
        "dim_accounts": {
            "unique_columns": ["account"],
            "compare_columns": ["valid_to", "client"],
            "staging_name": CONFIG.stg.stg_accounts_name,
            "dim_name": CONFIG.dim.dim_accounts_name
        },

        "dim_cards": {
            "unique_columns": ["card_num"],
            "compare_columns": ["account"],
            "staging_name": CONFIG.stg.stg_cards_name,
            "dim_name": CONFIG.dim.dim_cards_name
        },
    }

    try:
        for table, columns_info in tables_info.items():
            staging_data = query_from_schema(CONFIG.db.intermediate_database, columns_info["staging_name"], 'public')

            handle_scd2_updates(
                conn_id=CONFIG.db.intermediate_database,
                staging_data=staging_data,
                table_name=columns_info["dim_name"],
                unique_columns=columns_info["unique_columns"],
                compare_columns=columns_info["compare_columns"],
                scd2_schema='public'
            )

            LOG.info(f"{table} successfully updated.")

    except Exception as e:
        LOG.error(f"Error during SCD2 update process: {e}")
        raise RuntimeError("DAG execution halted due to critical error.") from e

def populate_fact_tables():
    try:
        joined_data = join_stg_and_dim_data(CONFIG.db.intermediate_database, stg_schema="public", dim_schema="public")

        fact_data = []
        for row in joined_data:
            fact_data.append({
                "transaction_id": row["transaction_id"],
                "transaction_date": row["transaction_date"],
                "amount": row["amount"],
                "card_num": row["card_num"],
                "terminal_id": row["terminal"],
                "oper_type": row["oper_type"],
                "oper_result": row["oper_result"]
            })

        insert_into_table(CONFIG.db.intermediate_database, CONFIG.fact.fact_transactions_name, fact_data)

    except Exception as e:
        LOG.error(f"Error occurred while fetching, joining, and inserting transactions: {e}")
        raise RuntimeError(f"Error occurred: {e}")