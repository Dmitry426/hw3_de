import polars as pl
from datetime import datetime, timedelta


def detect_blocked_or_expired_passports(
        transactions, dim_cards, dim_accounts, dim_clients, fact_passport_blacklist
):
    """
    Detect transactions where the client's passport is blocked or expired.
    """
    dim_cards = dim_cards.select(["card_num", "account_id"])
    dim_accounts = dim_accounts.select(["account_id", "client_id"])
    dim_clients = dim_clients.select(
        [
            "client_id",
            "last_name",
            "first_name",
            "passport_num",
            "passport_valid_to",
            "phone",
        ]
    )
    fact_passport_blacklist = fact_passport_blacklist.select(["passport"])

    return (
        transactions.join(dim_cards, on="card_num", how="inner")
        .join(dim_accounts, left_on="account_id", right_on="account_id", how="inner")
        .join(dim_clients, left_on="client_id", right_on="client_id", how="inner")
        .join(
            fact_passport_blacklist,
            left_on="passport_num",
            right_on="passport",
            how="left",
        )
        .filter(
            (pl.col("passport_num").is_not_null())  # Passport is in the blacklist
            | (
                    pl.col("passport_valid_to") < datetime.now().date()
            )
        )
        .select(
            [
                pl.col("transaction_date").alias("event_dt"),
                pl.col("passport_num").alias("passport"),
                (pl.col("last_name") + " " + pl.col("first_name")).alias("fio"),
                pl.col("phone"),
                pl.lit("Заблокированный или просроченный паспорт").alias("event_type"),
                pl.lit(datetime.now().date()).alias("report_dt"),
            ]
        )
    )


def detect_invalid_contracts(transactions, dim_clients, dim_accounts, dim_cards):
    """
    Detect transactions conducted on accounts with expired contracts.
    """

    dim_clients = dim_clients.select(
        ["client_id", "passport_num", "last_name", "first_name", "patronymic", "phone"]
    )
    dim_accounts = dim_accounts.select(["account_id", "valid_to", "client_id"])
    dim_cards = dim_cards.select(["card_num", "account_id"])

    transactions_with_accounts = transactions.join(
        dim_cards, on="card_num", how="inner"
    )

    transactions_with_accounts_clients = transactions_with_accounts.join(
        dim_accounts, on="account_id", how="inner"
    )

    full_data = transactions_with_accounts_clients.join(
        dim_clients, on="client_id", how="inner"
    )

    expired = full_data.filter(pl.col("valid_to") < datetime.now().date())

    expired = expired.with_columns(
        pl.concat_str(
            [
                pl.col("last_name").fill_null(""),
                pl.col("first_name").fill_null(""),
                pl.col("patronymic").fill_null(""),
            ],
            separator=" ",
        ).alias("fio")
    )

    result = expired.select(
        [
            pl.col("transaction_date").alias("event_dt"),
            pl.col("passport_num").alias("passport"),
            pl.col("fio"),
            pl.col("phone"),
            pl.lit("Недействующий договор").alias("event_type"),
            pl.lit(datetime.now().date()).alias("report_dt"),
        ]
    )

    return result


def detect_operations_in_different_cities(
        transactions, dim_clients, dim_accounts, dim_cards, dim_terminals
):
    """
    Detect transactions performed in different cities within 1 hour using Polars.
    """

    dim_clients = dim_clients.select(
        ["client_id", "passport_num", "last_name", "first_name", "patronymic", "phone"]
    )
    dim_accounts = dim_accounts.select(["client_id", "account_id"])
    dim_cards = dim_cards.select(["card_num", "account_id"])
    dim_terminals = dim_terminals.select(
        ["terminal_id", "terminal_city", "terminal_address"]
    )

    transactions_with_accounts = transactions.join(
        dim_cards, on="card_num", how="inner"
    )

    transactions_with_clients = transactions_with_accounts.join(
        dim_accounts, on="account_id", how="inner"
    )

    transactions_with_clients = transactions_with_clients.join(
        dim_clients, on="client_id", how="inner"
    )

    full_data = transactions_with_clients.join(
        dim_terminals, on="terminal_id", how="inner"
    )

    full_data = full_data.with_columns(
        pl.concat_str(
            [
                pl.col("last_name").fill_null(""),
                pl.col("first_name").fill_null(""),
                pl.col("patronymic").fill_null(""),
            ],
            separator=" ",
        ).alias("fio")
    )

    full_data = full_data.with_columns(
        [
            pl.col("transaction_date").cast(pl.Datetime),
        ]
    )

    self_join = full_data.join(
        full_data.select(
            [
                pl.col("client_id").alias("client_id_2"),
                pl.col("transaction_date").alias("transaction_date_2"),
                pl.col("terminal_city").alias("terminal_city_2"),
                pl.col("terminal_address").alias("terminal_address_2"),
            ]
        ),
        left_on=["client_id"],
        right_on=["client_id_2"],
        how="inner",
    )
    filtered = self_join.filter(
        (pl.col("terminal_city") != pl.col("terminal_city_2"))
        & (pl.col("transaction_date") < pl.col("transaction_date_2"))
        & (
                (pl.col("transaction_date_2") - pl.col("transaction_date")).dt.nanoseconds()
                <= 3600 * 10 ** 9
        )
    )

    result = filtered.select(
        [
            pl.col("transaction_date").alias("event_dt"),
            pl.col("passport_num").alias("passport"),
            pl.col("fio"),
            pl.col("phone"),
            pl.lit("Операции в разных городах").alias("event_type"),
            pl.lit(datetime.now().date()).alias("report_dt"),
            pl.col("terminal_address").alias("source_terminal_address"),
            pl.col("terminal_address_2").alias("destination_terminal_address"),
        ]
    )

    return result


def detect_attempted_amount_guessing(
        transactions, dim_clients, dim_cards, dim_accounts
):
    """
    Detect attempts to guess the amount through multiple declining transactions.
    """

    transactions = transactions.with_columns(
        pl.col("transaction_date").cast(pl.Datetime)
    )

    transactions = transactions.with_columns(
        [
            pl.col("amount").shift(1).alias("amount_shift_1"),
            pl.col("amount").shift(2).alias("amount_shift_2"),
            pl.col("transaction_date").shift(1).alias("transaction_date_shift_1"),
            pl.col("oper_result").shift(-1).alias("next_oper_result"),
        ]
    )

    filtered = transactions.filter(
        (
                pl.col("transaction_date")
                >= (pl.col("transaction_date_shift_1") - timedelta(minutes=20))
        )
        & (pl.col("amount_shift_1") > pl.col("amount"))
        & (pl.col("amount_shift_2") > pl.col("amount_shift_1"))
        & (pl.col("next_oper_result") == "SUCCESS")
    )

    enriched_transactions = filtered.join(
        dim_cards.select(["card_num", "account_id"]), on="card_num", how="inner"
    )

    enriched_transactions = enriched_transactions.join(
        dim_accounts.select(["account_id", "client_id"]), on="account_id", how="inner"
    )

    result = enriched_transactions.join(
        dim_clients, on="client_id", how="inner"
    ).select(
        [
            pl.col("transaction_date").alias("event_dt"),
            pl.col("passport_num").alias("passport"),
            pl.concat_str(
                [
                    pl.col("last_name").fill_null(""),
                    pl.col("first_name").fill_null(""),
                    pl.col("patronymic").fill_null(""),
                ],
                separator=" ",
            ).alias("fio"),
            pl.col("phone"),
            pl.lit("Попытка подбора суммы").alias("event_type"),
            pl.lit(datetime.now().date()).alias("report_dt"),
        ]
    )

    return result


def generate_fraud_report(
        transactions,
        dim_clients,
        fact_passport_blacklist,
        dim_accounts,
        dim_cards,
        dim_terminals,
):
    """
    Generate a combined fraud report by running all detection functions.
    """
    blocked_or_expired = detect_blocked_or_expired_passports(
        transactions, dim_cards, dim_accounts, dim_clients, fact_passport_blacklist
    )
    invalid_contracts = detect_invalid_contracts(
        transactions, dim_clients, dim_accounts, dim_cards
    )
    # different_cities = detect_operations_in_different_cities(
    #     transactions, dim_clients,dim_accounts,dim_cards,dim_terminals
    # )
    attempted_guessing = detect_attempted_amount_guessing(
        transactions, dim_clients, dim_cards, dim_accounts
    )

    fraud_cases = pl.concat([blocked_or_expired, invalid_contracts, attempted_guessing])

    return fraud_cases
