from datetime import datetime
from typing import Dict, Any, List

from sqlalchemy import MetaData, Table, and_

from src.services.connections.postgres.connection import get_session
from src.services.save_file_loader import load_sql_script
from src.settings.logger import get_airflow_logger

LOG = get_airflow_logger(__name__)


def query_from_schema(conn_id: str, table_name: str, schema: str):
    """
    Query data from a specified table in the info schema of the source database.

    :param conn_id: The Airflow connection ID for the database.
    :param table_name: The name of the table to query from.
    :param schema: Database schema.
    :return: List of dictionaries containing the queried data.
    """

    query = f"SELECT * FROM {schema}.{table_name};"

    try:
        with get_session(conn_id) as session:
            result = session.execute(query).fetchall()

            if result:
                columns = result[0].keys()

                data = [dict(zip(columns, row)) for row in result]
                return data
            else:
                return []
    except Exception as e:
        LOG.error(f"Error executing query on table {table_name}: {e}")
        raise


def create_databases(conn_id: str, ddl_file_path: str):
    """
    Create databases from a provided DDL file.

    :param conn_id: The Airflow connection ID.
    :param ddl_file_path: Path to the SQL DDL file.
    """
    ddl_script = load_sql_script(ddl_file_path)

    LOG.info(f"Executing ddl query {ddl_script} ")

    with get_session(conn_id) as session:
        for statement in ddl_script.split(';'):
            statement = statement.strip()
            if statement:
                session.execute(statement)
        session.commit()


def insert_into_table(conn_id: str, table_name: str, data: List[Dict[str, Any]]):
    """
    Inserts data into a specified table.

    :param conn_id: The Airflow connection ID.
    :param table_name: The name of the table to insert into.
    :param data: The data to insert as a dictionary where the key is the column name and the value is the column value.
    """
    with get_session(conn_id) as session:
        metadata = MetaData(bind=session.bind)
        table = Table(table_name, metadata, autoload_with=session.bind)

        try:
            session.execute(table.insert().values(data))
            session.commit()
        except Exception as e:
            session.rollback()
            raise e


def truncate_tables_with_ddl(conn_id: str, ddl_file_path: str):
    """
    Truncate tables using a provided DDL file.

    :param conn_id: The Airflow connection ID.
    :param ddl_file_path: Path to the SQL DDL file containing TRUNCATE statements.
    """
    ddl_script = load_sql_script(ddl_file_path)

    LOG.info(f"Executing truncate DDL script from {ddl_file_path}")

    with get_session(conn_id) as session:
        try:
            for statement in ddl_script.split(';'):
                statement = statement.strip()
                if statement:
                    LOG.info(f"Executing statement: {statement}")
                    session.execute(statement)
            session.commit()
            LOG.info("Truncate operation completed successfully.")
        except Exception as e:
            session.rollback()
            LOG.critical(f"Failed to execute truncate DDL: {e}", exc_info=True)
            raise

def handle_scd2_updates(conn_id, staging_data, table_name, unique_columns, compare_columns, scd2_schema):
    """
    Handle SCD2 updates for a dimension table.

    :param conn_id: Airflow conn id.
    :param staging_data: List of dictionaries with staging data.
    :param table_name: SQLAlchemy model for the dimension table.
    :param unique_columns: List of columns that uniquely identify a record.
    :param scd2_schema: Schema name where the table resides.
    :param compare_columns: List of columns to check for changes.
    """
    with get_session(conn_id) as session:
        try:
            metadata = MetaData(bind=session.bind)
            table = Table(table_name, metadata, schema=scd2_schema, autoload_with=session.bind)

            for record in staging_data:
                missing_columns = [col for col in unique_columns + compare_columns if col not in record]
                if missing_columns:
                    LOG.warning(f"Missing columns in the record: {missing_columns}")
                    continue

                filter_condition = and_(
                    *[getattr(table.c, col) == record[col] for col in unique_columns],
                    table.c.effective_to == datetime(2300, 12, 31),
                    table.c.deleted_flg == False
                )

                existing_record = session.execute(table.select().where(filter_condition)).fetchone()

                if existing_record:
                    changes = any(
                        existing_record[col] != record[col]
                        for col in compare_columns
                    )

                    if changes:
                        session.execute(
                            table.update()
                            .where(and_(*[getattr(table.c, col) == existing_record[col] for col in unique_columns]))
                            .values(effective_to=datetime.now(), deleted_flg=True)
                        )

                        session.execute(
                            table.insert().values(
                                **{col: record[col] for col in unique_columns + compare_columns},
                                effective_from=datetime.now(),
                                effective_to=datetime(2300, 12, 31),
                                deleted_flg=False
                            )
                        )
                else:
                    session.execute(
                        table.insert().values(
                            **{col: record[col] for col in unique_columns + compare_columns},
                            effective_from=datetime.now(),
                            effective_to=datetime(2300, 12, 31),
                            deleted_flg=False
                        )
                    )
            session.commit()
        except Exception as e:
            session.rollback()
            LOG.critical(f"Failed to update SCD2 due to: {e}", exc_info=True)
            raise RuntimeError(f"Error handling SCD2 updates: {e}")


def join_stg_and_dim_data(conn_id: str, stg_schema: str, dim_schema: str):
    """Join stg_transactions and dim_cards directly in the database."""

    query = f"""
        SELECT 
            st.transaction_id,
            st.transaction_date,
            st.amount,
            st.card_num,
            st.oper_type,
            st.oper_result,
            st.terminal,
            dc.account
        FROM {stg_schema}.stg_transactions st
        JOIN {dim_schema}.dim_cards dc ON st.card_num = dc.card_num;
    """

    try:
        with get_session(conn_id) as session:
            result = session.execute(query).fetchall()

            if result:
                columns = result[0].keys()
                data = [dict(zip(columns, row)) for row in result]
                return data
            else:
                return []
    except Exception as e:
        LOG.error(f"Error executing the join query: {e}")
        raise RuntimeError(f"Error executing the join query: {e}")