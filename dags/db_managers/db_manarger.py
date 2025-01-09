import os
from typing import Dict, Any, List

from sqlalchemy import MetaData, Table

import polars as pl
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from dags.connections.postgres.connection import get_session
from dags.settings.logger import get_airflow_logger

LOG = get_airflow_logger(__name__)


class DbManager:

    @staticmethod
    def query_from_schema(conn_id: str, table_name: str, schema: str, as_df=True):
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
                if as_df:
                    if result:
                        columns = result[0].keys()
                        return pl.DataFrame([dict(zip(columns, row)) for row in result])
                    else:
                        return pl.DataFrame()
                else:
                    return [dict(zip(result[0].keys(), row)) for row in result]

        except Exception as e:
            LOG.error(f"Error executing query on table {table_name}: {e}")
            raise

    @staticmethod
    def create_databases(conn_id: str, ddl_file_path: str):
        """
        Create databases from a provided DDL file.

        :param conn_id: The Airflow connection ID.
        :param ddl_file_path: Path to the SQL DDL file.
        """
        ddl_script = DbManager.load_sql_script(ddl_file_path)
        LOG.info(f"Executing ddl query {ddl_script}")

        with get_session(conn_id) as session:
            for statement in ddl_script.split(";"):
                statement = statement.strip()
                if statement:
                    session.execute(statement)
            session.commit()

    @staticmethod
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

    @staticmethod
    def upsert_into_table(
            conn_id: str,
            table_name: str,
            data: Dict[str, Any],
            conflict_columns: List[str],
            update_columns: List[str],
    ):
        """
        Inserts or updates data in a specified table. Skips the operation if the record already exists.

        :param conn_id: The Airflow connection ID for the database.
        :param table_name: The name of the table to perform the upsert on.
        :param data: A dictionary containing the data to upsert.
        :param conflict_columns: Columns to identify conflicts (unique constraints or primary keys).
        :param update_columns: Columns to update if a conflict occurs.
        """
        with get_session(conn_id) as session:
            metadata = MetaData(bind=session.bind)
            table = Table(table_name, metadata, autoload_with=session.bind)

            # Check if the record already exists
            existing_record_query = session.query(table).filter(
                *[
                    table.c[col] == data[col]
                    for col in conflict_columns
                    if col in data
                ]
            )
            existing_record = existing_record_query.first()

            if existing_record:
                LOG.info(
                    f"Skipping upsert for table {table_name}: Record already exists with {conflict_columns} = {[data[col] for col in conflict_columns]}."
                )
                return  # Skip the upsert if the record exists

            try:
                # Construct the upsert statement
                stmt = insert(table).values(data)
                update_dict = {col: stmt.excluded[col] for col in update_columns}

                # Add the ON CONFLICT clause
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_columns,
                    set_=update_dict,
                )

                # Execute the statement
                session.execute(stmt)
                session.commit()
                LOG.info(f"Upsert operation completed successfully for table {table_name}.")
            except SQLAlchemyError as e:
                session.rollback()
                LOG.error(
                    f"Failed to upsert data into table {table_name}: {e}",
                    exc_info=True,
                )
                raise
    @staticmethod
    def truncate_tables_with_ddl(conn_id: str, ddl_file_path: str):
        """
        Truncate tables using a provided DDL file.

        :param conn_id: The Airflow connection ID.
        :param ddl_file_path: Path to the SQL DDL file containing TRUNCATE statements.
        """
        ddl_script = DbManager.load_sql_script(ddl_file_path)
        LOG.info(f"Executing truncate DDL script from {ddl_file_path}")

        with get_session(conn_id) as session:
            try:
                for statement in ddl_script.split(";"):
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

    @staticmethod
    def load_sql_script(file_path: str) -> str:
        """
        Load the SQL script from the given file path.

        :param file_path: Path to the SQL file.
        :return: The contents of the SQL file as a string.
        :raises FileNotFoundError: If the file does not exist.
        :raises PermissionError: If there is no permission to read the file.
        :raises IOError: For other IO-related issues.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"No read permission for file: {file_path}")

        try:
            with open(file_path, "r") as file:
                return file.read()
        except UnicodeDecodeError:
            raise ValueError(f"The file is not a valid text file: {file_path}")
        except IOError as e:
            raise IOError(
                f"An error occurred while reading the file: {file_path}. Error: {e}"
            )
