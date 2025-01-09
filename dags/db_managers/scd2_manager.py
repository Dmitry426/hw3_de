from datetime import datetime

from sqlalchemy import MetaData, Table, and_, insert, update
from sqlalchemy.exc import IntegrityError

from dags.connections.postgres.connection import get_session
from dags.settings.logger import get_airflow_logger

LOG = get_airflow_logger(__name__)


class BaseSCD2Handler:
    def __init__(self, scd2_schema: str, conn_id: str):
        self.scd2_schema = scd2_schema
        self.conn_id = conn_id

    def _get_table(self, session, table_name):
        metadata = MetaData(bind=session.bind)
        return Table(
            table_name, metadata, schema=self.scd2_schema, autoload_with=session.bind
        )

    @staticmethod
    def _get_filter_condition(table, unique_columns, record):
        """Create a filter condition to locate an existing record based on unique columns."""
        conditions = []
        for col in unique_columns:
            table_col = getattr(table.c, col)
            record_value = record[col]

            if isinstance(record_value, str) and hasattr(table_col.type, "python_type"):
                try:
                    record_value = table_col.type.python_type(record_value)
                except Exception as e:
                    LOG.warning(
                        f"Type conversion failed for column {col} with value {record_value}: {e}"
                    )
                    continue

            conditions.append(table_col == record_value)

        return and_(*conditions)

    @staticmethod
    def _insert_new_record(session, table, record, unique_columns, compare_columns):
        session.execute(
            insert(table).values(
                **{col: record[col] for col in unique_columns + compare_columns},
                effective_from=datetime.now(),
                effective_to=datetime(2300, 12, 31),
                deleted_flg=0,
            )
        )

    @staticmethod
    def _update_existing_record(session, table, existing_record, unique_columns):
        session.execute(
            update(table)
            .where(
                and_(
                    *[
                        getattr(table.c, col) == existing_record[col]
                        for col in unique_columns
                    ]
                )
            )
            .values(effective_to=datetime.now(), deleted_flg=1)
        )

    def _process_existing_record(
        self, session, table, existing_record, record, unique_columns, compare_columns
    ):
        changes_detected = any(
            existing_record[col] != record[col] for col in compare_columns
        )
        if changes_detected:
            self._update_existing_record(
                session, table, existing_record, unique_columns
            )
            self._insert_new_record(
                session, table, record, unique_columns, compare_columns
            )

    def handle_scd2_updates(
        self, staging_data, table_name, unique_columns, compare_columns
    ):
        try:
            with get_session(self.conn_id) as session:
                table = self._get_table(session, table_name)
                LOG.info(
                    f"Processing {len(staging_data)} records for SCD2 updates in table '{table_name}'."
                )

                session.begin()
                for record in staging_data:
                    if any(
                        col not in record for col in unique_columns + compare_columns
                    ):
                        LOG.warning(f"Skipping record due to missing columns: {record}")
                        continue

                    filter_condition = self._get_filter_condition(
                        table, unique_columns, record
                    )
                    existing_record = session.execute(
                        table.select().where(filter_condition)
                    ).fetchone()

                    if existing_record:
                        self._process_existing_record(
                            session,
                            table,
                            existing_record,
                            record,
                            unique_columns,
                            compare_columns,
                        )
                    else:
                        self._insert_new_record(
                            session, table, record, unique_columns, compare_columns
                        )

                session.commit()
                LOG.info(
                    f"Successfully processed all records for table '{table_name}'."
                )
        except IntegrityError as e:
            LOG.error(f"Database error during SCD2 updates: {e}", exc_info=True)
            session.rollback()
            raise
        except Exception as e:
            LOG.critical(f"Unhandled error during SCD2 updates: {e}", exc_info=True)
            raise
