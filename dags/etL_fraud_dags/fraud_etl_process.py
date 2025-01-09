from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from dags.db_managers.db_manarger import DbManager
from dags.db_managers.scd2_manager import BaseSCD2Handler
from dags.s3_service.s3_loaders import get_transaction_loader
from dags.services.fraud_handler import generate_fraud_report
from dags.settings.fraud_etl_settings import Config, PROJECT_ROOT
from dags.settings.logger import get_airflow_logger

LOG = get_airflow_logger(__name__)

CONFIG = Config.from_yaml((PROJECT_ROOT.parent / "configs" / "fraud_etl.yaml"))


DEFAULT_ARGS = {
    "owner": "Dmitrii",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "fraud_etl",
    tags=["fraud"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)


def migrate(*args, **kwargs):
    LOG.info("Making tables intermidiate")
    connection = CONFIG.db.intermediate_database
    ddl_migrations = [
        CONFIG.stg.ddl,
    ]

    for ddl in ddl_migrations:
        DbManager.create_databases(connection, (PROJECT_ROOT.parent / "configs" / ddl))
    LOG.info("Creating tables in target db")

    connection_target = CONFIG.db.target_database
    ddl_migrations = [
        CONFIG.dim.ddl,
        CONFIG.fact.ddl,
        CONFIG.report.ddl,
    ]
    for ddl in ddl_migrations:
        DbManager.create_databases(
            connection_target, (PROJECT_ROOT.parent / "configs" / ddl)
        )


def prepare_fraud_loaded_data(target_date, **kwargs):
    """
    Dag prepares preloaded transaction , passport blacklist and terminal
        data from minio to stg tables
    """
    try:
        LOG.info(target_date)
        file_names = CONFIG.minio.file_templates.generate_file_names(
            target_date, date_format=CONFIG.minio.files_date_format
        )
        LOG.info(f"Generated file names for {target_date}: {file_names}")

        s3_loader = get_transaction_loader(CONFIG)
        dataframes = s3_loader.load_data_from_s3(file_names=file_names)

        passport_blacklist_data = [
            {"date": row["date"], "passport": row["passport"]}
            for row in dataframes["passport_blacklist"].iter_rows(named=True)
        ]

        transactions_data = [
            {
                "transaction_id": row["transaction_id"],
                "transaction_date": row["transaction_date"],
                "amount": row["amount"],
                "card_num": row["card_num"],
                "oper_type": row["oper_type"],
                "oper_result": row["oper_result"],
                "terminal_id": row["terminal"],
            }
            for row in dataframes["transactions"].iter_rows(named=True)
        ]

        terminals_data = [
            {
                "terminal_id": row["terminal_id"],
                "terminal_type": row["terminal_type"],
                "terminal_city": row["terminal_city"],
                "terminal_address": row["terminal_address"],
            }
            for row in dataframes["terminals"].iter_rows(named=True)
        ]
        for row in passport_blacklist_data:
            DbManager.upsert_into_table(
                conn_id=CONFIG.db.intermediate_database,
                table_name=CONFIG.stg.stg_blacklist_name,
                data=row,
                conflict_columns=["passport"],
                update_columns=["date"],
            )

        DbManager.insert_into_table(
            CONFIG.db.intermediate_database,
            CONFIG.stg.stg_transactions_name,
            transactions_data,
        )
        DbManager.insert_into_table(
            CONFIG.db.intermediate_database,
            CONFIG.stg.stg_terminals_name,
            terminals_data,
        )

    except Exception as main_exc:
        LOG.critical(
            "Critical error in loading and inserting data. Halting DAG execution.",
            exc_info=False,
        )
        raise RuntimeError("DAG execution halted due to critical error.") from main_exc


def prepare_client_data(*args, **kwargs):
    dataframes = {}
    try:
        db_data = {
            "accounts": CONFIG.stg.stg_accounts_name,
            "cards": CONFIG.stg.stg_cards_name,
            "clients": CONFIG.stg.stg_clients_name,
        }
        for table, stg_table in db_data.items():
            data = DbManager.query_from_schema(
                conn_id='pg_hse', table_name=table, schema="info"
            )
            dataframes[table] = data
        accounts_data = [
            {
                "account_id": row["account"],
                "valid_to": row["valid_to"],
                "client_id": row["client"],
                "create_dt": row["create_dt"],
                "update_dt": row["update_dt"],
            }
            for row in dataframes["accounts"].iter_rows(named=True)
        ]

        clients_data = [
            {
                "client_id": row["client_id"],
                "last_name": row["last_name"],
                "first_name": row["first_name"],
                "patronymic": row["patronymic"],
                "date_of_birth": row["date_of_birth"],
                "passport_num": (
                    row["passport_num"].strip() if row["passport_num"] else None
                ),
                "passport_valid_to": row["passport_valid_to"],
                "phone": row["phone"],
                "create_dt": row["create_dt"],
                "update_dt": row["update_dt"],
            }
            for row in dataframes["clients"].iter_rows(named=True)
        ]

        cards_data = [
            {
                "card_num": row["card_num"].strip() if row["card_num"] else None,
                "account_id": row["account"],
                "create_dt": row["create_dt"],
                "update_dt": row["update_dt"],
            }
            for row in dataframes["cards"].iter_rows(named=True)
        ]
        DbManager.insert_into_table(
            CONFIG.db.intermediate_database, CONFIG.stg.stg_accounts_name, accounts_data
        )

        DbManager.insert_into_table(
            CONFIG.db.intermediate_database, CONFIG.stg.stg_clients_name, clients_data
        )

        DbManager.insert_into_table(
            CONFIG.db.intermediate_database, CONFIG.stg.stg_cards_name, cards_data
        )

    except Exception as main_exc:
        DbManager.truncate_tables_with_ddl(
            CONFIG.db.intermediate_database,
            (PROJECT_ROOT.parent / "configs" / CONFIG.stg.truncate_ddl),
        )
        LOG.critical(
            "Critical error in loading and inserting data. Halting DAG execution.",
            exc_info=False,
        )
        raise RuntimeError("DAG execution halted due to critical error.") from main_exc


def safe_populate_dim_tables(**kwargs):
    tables_info = {
        "dim_terminals": {
            "unique_columns": ["terminal_id"],
            "compare_columns": ["terminal_type", "terminal_city", "terminal_address"],
            "staging_name": CONFIG.stg.stg_terminals_name,
            "dim_name": CONFIG.dim.dim_terminals_name,
        },
        "dim_clients": {
            "unique_columns": ["client_id"],
            "compare_columns": [
                "last_name",
                "first_name",
                "patronymic",
                "passport_num",
                "phone",
                "date_of_birth",
                "passport_valid_to",
            ],
            "staging_name": CONFIG.stg.stg_clients_name,
            "dim_name": CONFIG.dim.dim_clients_name,
        },
        "dim_accounts": {
            "unique_columns": ["account_id"],
            "compare_columns": ["valid_to", "client_id"],
            "staging_name": CONFIG.stg.stg_accounts_name,
            "dim_name": CONFIG.dim.dim_accounts_name,
        },
        "dim_cards": {
            "unique_columns": ["card_num"],
            "compare_columns": ["account_id"],
            "staging_name": CONFIG.stg.stg_cards_name,
            "dim_name": CONFIG.dim.dim_cards_name,
        },
    }
    scd2_handler = BaseSCD2Handler(
        conn_id=CONFIG.db.target_database, scd2_schema=CONFIG.dim.db_schema
    )

    try:
        for table, columns_info in tables_info.items():
            staging_data = DbManager.query_from_schema(
                CONFIG.db.target_database,
                columns_info["staging_name"],
                CONFIG.stg.db_schema,
                as_df=False
            )

            scd2_handler.handle_scd2_updates(
                staging_data=staging_data,
                table_name=columns_info["dim_name"],
                unique_columns=columns_info["unique_columns"],
                compare_columns=columns_info["compare_columns"],
            )

            LOG.info(f"{table} successfully updated.")

    except Exception as e:
        LOG.error(f"Error during SCD2 update process: {e}")
        raise RuntimeError("DAG execution halted due to critical error.") from e


def process_and_insert_transactions(**kwargs):
    """
    Fetch data from separate databases, join stg_transactions with dim_cards in memory,
    and insert the transformed data into the fact table as an Airflow task.
    """
    try:
        stg_data = DbManager.query_from_schema(
            conn_id=CONFIG.db.intermediate_database,
            table_name=CONFIG.stg.stg_transactions_name,
            schema=CONFIG.stg.db_schema,
            as_df=True,
        )

        dim_data = DbManager.query_from_schema(
            conn_id=CONFIG.db.target_database,
            table_name=CONFIG.dim.dim_cards_name,
            schema=CONFIG.dim.db_schema,
            as_df=True,
        )

        if stg_data.is_empty() or dim_data.is_empty():
            raise ValueError("One or both source tables are empty. Task failed.")

        joined_data = stg_data.join(dim_data, on="card_num", how="inner")

        fact_data = [
            {
                "transaction_id": row["transaction_id"],
                "transaction_date": row["transaction_date"],
                "amount": row["amount"],
                "card_num": row["card_num"],
                "terminal_id": row["terminal_id"],
                "oper_type": row["oper_type"],
                "oper_result": row["oper_result"],
            }
            for row in joined_data.to_dicts()
        ]

        DbManager.insert_into_table(
            conn_id=CONFIG.db.target_database,
            table_name=CONFIG.fact.fact_transactions_name,
            data=fact_data,
        )

        LOG.info(f"Successfully processed and inserted {len(fact_data)} records.")

    except ValueError as e:
        LOG.error(f"Validation error: {e}")
        raise
    except Exception as e:
        LOG.error(f"Error occurred while processing transactions: {e}", exc_info=True)
        raise RuntimeError(f"Error occurred: {e}")


def process_and_insert_blacklist(**kwargs):
    """
    Generate and insert data into the fact_passport_blacklist table.
    """
    joined_data = DbManager.query_from_schema(
        CONFIG.db.target_database,
        CONFIG.stg.stg_blacklist_name,
        "public",
        as_df=True,
    )
    try:
        blacklist_data = [
            {
                "blacklist_date": row["date"],
                "passport": row["passport"],
            }
            for row in joined_data.to_dicts()
        ]

        if blacklist_data:
            DbManager.insert_into_table(
                conn_id=CONFIG.db.target_database,
                table_name=CONFIG.fact.fact_passport_blacklist,
                data=blacklist_data,
            )

            LOG.info(
                f"Successfully inserted {len(blacklist_data)} records into "
                f"fact_passport_blacklist."
            )

    except Exception as e:
        LOG.error(
            f"Error occurred while processing passport blacklist: {e}", exc_info=True
        )
        raise RuntimeError(f"Error occurred while processing passport blacklist: {e}")



def fraud_report():
    transactions = DbManager.query_from_schema(
        CONFIG.db.target_database,
        CONFIG.fact.fact_transactions_name,
        "public",
        as_df=True,
    )
    dim_clients = DbManager.query_from_schema(
        CONFIG.db.target_database,
        CONFIG.dim.dim_clients_name,
        "public",
        as_df=True,
    )
    fact_passport_blacklist = DbManager.query_from_schema(
        CONFIG.db.target_database,
        CONFIG.fact.fact_passport_blacklist,
        "public",
        as_df=True,
    )
    dim_accounts = DbManager.query_from_schema(
        CONFIG.db.target_database,
        CONFIG.dim.dim_accounts_name,
        "public",
        as_df=True,
    )
    dim_cards = DbManager.query_from_schema(
        CONFIG.db.target_database,
        CONFIG.dim.dim_cards_name,
        "public",
        as_df=True,
    )
    dim_terminals = DbManager.query_from_schema(
        CONFIG.db.target_database,
        CONFIG.dim.dim_terminals_name_name,
        "public",
        as_df=True,
    )

    generate_fraud_report(transactions,
        dim_clients,
        fact_passport_blacklist,
        dim_accounts,
        dim_cards,
        dim_terminals)

def clear_stg(**kwargs):
    DbManager.truncate_tables_with_ddl(
        CONFIG.db.intermediate_database,
        (PROJECT_ROOT.parent / "configs" / CONFIG.stg.truncate_ddl),
    )

with dag:
    task_init = PythonOperator(
        task_id="migrate", provide_context=True, python_callable=migrate, dag=dag
    )
    tasks=[]
    for time in  CONFIG.minio.target_dates:
        task = PythonOperator(
            task_id=f"load_{time}",
            op_args=[time],
            provide_context=True,
            python_callable=prepare_fraud_loaded_data,
            dag=dag,
        )
        tasks.append(task)

    prepare_client_data = PythonOperator(
        task_id="prepare_client_data",
        provide_context=True,
        python_callable=prepare_client_data,
        dag=dag,
    )

    task_populate_dim_tables = PythonOperator(
        task_id="safe_populate_dim_tables",
        provide_context=True,
        python_callable=safe_populate_dim_tables,
        dag=dag,
    )
    task_populate_fact = PythonOperator(
        task_id="process_and_insert_transactions",
        provide_context=True,
        python_callable=process_and_insert_transactions,
        dag=dag,
    )

    task_populate_blacklist = PythonOperator(
        task_id="process_and_insert_blacklist",
        provide_context=True,
        python_callable=process_and_insert_blacklist,
        dag=dag,
    )
    fraud_report = PythonOperator(
        task_id="fraud_report",
        provide_context=True,
        python_callable=fraud_report,
        trigger_rule=TriggerRule.ALL_FAILED,
        dag=dag,
    )
    clear =PythonOperator(
        task_id="clear_stg_on_fails",
        provide_context=True,
        python_callable=clear_stg,
        trigger_rule=TriggerRule.ALL_FAILED,
        dag=dag,
    )
    clean_up =PythonOperator(
        task_id="clear_stg",
        provide_context=True,
        python_callable=clear_stg,
        trigger_rule=TriggerRule.ALL_FAILED,
        dag=dag,
    )
task_init >> tasks >> prepare_client_data >> task_populate_dim_tables >>task_populate_fact >> task_populate_blacklist >>fraud_report >> clean_up