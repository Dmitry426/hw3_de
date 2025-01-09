class PassportBlacklistTransformer:
    @staticmethod
    def transform(df):
        return [
            {"date": row["date"], "passport": row["passport"]}
            for row in df.iter_rows(named=True)
        ]


class TransactionsTransformer:
    @staticmethod
    def transform(df):
        return [
            {
                "transaction_id": row["transaction_id"],
                "transaction_date": row["transaction_date"],
                "amount": row["amount"],
                "card_num": row["card_num"],
                "oper_type": row["oper_type"],
                "oper_result": row["oper_result"],
                "terminal": row["terminal"],
            }
            for row in df.iter_rows(named=True)
        ]


class TerminalsTransformer:
    @staticmethod
    def transform(df):
        return [
            {
                "terminal_id": row["terminal_id"],
                "terminal_type": row["terminal_type"],
                "terminal_city": row["terminal_city"],
                "terminal_address": row["terminal_address"],
            }
            for row in df.iter_rows(named=True)
        ]
