from pathlib import Path
from typing import List, Dict
from datetime import datetime
from pydantic import BaseModel, model_validator, field_validator
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

class Report(BaseModel):
    ddl: str = "ddl_report.sql"
    db_schema: str = "public"
    rep_fraud_name: str = "rep_fraud"


class FACT(BaseModel):
    ddl: str = "ddl_fact.sql"
    db_schema: str = "public"
    fact_transactions_name: str = "fact_transactions"
    fact_passport_blacklist: str = "fact_passport_blacklist"


class DIM(BaseModel):
    ddl: str = "ddl_dim.sql"
    db_schema: str = "public"
    dim_terminals_name: str = "dim_terminals"
    dim_accounts_name: str = "dim_accounts"
    dim_cards_name: str = "dim_cards"
    dim_clients_name: str = "dim_clients"


class STG(BaseModel):
    ddl: str = "ddl_stg.sql"
    db_schema: str = "public"
    truncate_ddl: str = "ddl_truncate_stg.sql"
    stg_blacklist_name: str = "stg_passport_blacklist"
    stg_terminals_name: str = "stg_terminals"
    stg_accounts_name: str = "stg_accounts"
    stg_cards_name: str = "stg_cards"
    stg_clients_name: str = "stg_clients"
    stg_transactions_name: str = "stg_transactions"


class DataBaseConfig(BaseModel):
    target_database: str = "pg_hse"
    intermediate_database: str = "pg_connection_local"


class FileTemplates(BaseModel):
    transactions: str
    passport_blacklist: str
    terminals: str

    @classmethod
    @model_validator(mode="before")
    def validate_and_fill_filename_templates(cls, values):
        """
        Validate that {date} is included in all templates.
        """
        for field in ["transactions", "passport_blacklist", "terminals"]:
            value = values.get(field)
            if value is None:
                raise ValueError(f"{field} cannot be None.")
            if "{date}" not in value:
                raise ValueError(
                    f"{value} for {field} should contain {{date}} placeholder."
                )
        return values

    def generate_file_names(
        self, date: str, date_format: str = "%d%m%Y"
    ) -> Dict[str, str]:
        """
        Generate file names using the templates and a given date.
        Format the date using the provided `date_format` (default: "%d%m%Y").
        """
        try:
            # Format the date using the provided date_format
            formatted_date = datetime.strptime(date, "%Y-%m-%d").strftime(date_format)
        except ValueError:
            raise ValueError(
                f"Invalid date format: {date}. Could not format with {date_format}."
            )

        return {
            "transactions": self.transactions.format(date=formatted_date),
            "passport_blacklist": self.passport_blacklist.format(date=formatted_date),
            "terminals": self.terminals.format(date=formatted_date),
        }


class MinioConfig(BaseModel):
    s3_connection: str
    bucket_name: str
    file_templates: FileTemplates
    target_dates: List[str] = []
    files_date_format: str = "%d%m%Y"

    @classmethod
    @model_validator(mode="before")
    def default_target_dates(cls, values):
        """
        If no target dates are provided, default to the current date.
        """
        if not values.get("target_dates"):
            values["target_dates"] = [datetime.now().strftime("%Y-%m-%d")]
        return values

    @classmethod
    @field_validator("target_dates", mode="before")
    def validate_target_dates(cls, values):
        """
        Ensure all target dates are in the correct format.
        """
        if not isinstance(values, list):
            raise ValueError("target_dates must be a list of dates.")
        for date in values:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid date format: {date}. Must be YYYY-MM-DD.")
        return values


class Config(BaseModel):
    minio: MinioConfig
    db: DataBaseConfig
    stg: STG
    dim: DIM
    fact: FACT
    report: Report

    @classmethod
    def from_yaml(cls, yaml_file: str):
        """
        Load the configuration from a YAML file and validate it.
        """
        with open(yaml_file, "r") as file:
            config_data = yaml.safe_load(file)
        return cls(**config_data)

