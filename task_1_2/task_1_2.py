import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

postgres_user = os.environ["POSTGRES_USER"]
postgres_pass = os.environ["POSTGRES_PASSWORD"]
postgres_port = os.environ["POSTGRES_PORT"]

engine = create_engine(
    f"postgresql://{postgres_user}:{postgres_pass}@db:{postgres_port}"
)


def not_present_errors(rows: pd.DataFrame):
    """Write error log for each symbol that is not present in bars_1"""
    not_present = rows[~rows.symbol.isin(symbols["symbol"])]

    if not_present.shape[0] != 0:
        rows = rows[rows.symbol.isin(symbols["symbol"])]

        not_present["launch_timestamp"] = datetime.utcnow()

        def error_msg(row):
            return f"{row['symbol']} not present in tables_bars_1 on {row['date']}"

        not_present["message"] = not_present.apply(lambda row: error_msg(row), axis=1)
        not_present.drop(
            ["id", "adj_close", "high", "low", "open", "volume", "close"], axis=1
        ).to_sql("error_log", engine, if_exists="append", index=False)


def compare_with_last_ten_days(rows: pd.DataFrame):
    """
    If record's close is bigger than minimum of this symbol
    close prices over the last 10 days from bars_1
    append it to bars_1. Else put an error
    """
    tmp = pd.merge(rows, last_ten_days, on="symbol")
    to_write = tmp[tmp["close"] > tmp["min"]]
    to_write.drop(["min", "id"], axis=1).to_sql(
        "bars_1", engine, if_exists="append", index=False
    )

    errors = tmp[~(tmp["close"] > tmp["min"])]
    errors["launch_timestamp"] = datetime.utcnow()

    def error_msg(row):
        return f"{row['symbol']} close price is not bigger than the minimum over the past 10 days on {row['date']}"

    errors["message"] = errors.apply(lambda row: error_msg(row), axis=1)
    errors.drop(
        ["min", "id", "adj_close", "high", "low", "open", "volume", "close"], axis=1
    ).to_sql("error_log", engine, if_exists="append", index=False)


if __name__ == "__main__":
    print("Taking next 20k rows from bars_2")

    symbols = pd.read_sql_query("""select distinct symbol from "bars_1\"""", con=engine)

    last_ten_days = pd.read_sql_query(
        """
        select symbol, min(close)
        from "bars_1"
        where date_part('day', age(CURRENT_DATE, date)) < 10
        group by symbol
    """,
        con=engine,
    )

    rows = pd.read_sql_query(
        """
        select id, date, symbol, adj_close, close, high, low, open, volume
        from "bars_2"
        limit 20000
    """,
        con=engine,
    )

    if rows.shape[0] == 0:
        with engine.connect() as con:
            statement = text(
                """
                    INSERT INTO error_log(launch_timestamp, date, symbol, message)
                    VALUES (:launch_timestamp, :date, :symbol, :message)"""
            )
            con.execute(
                statement,
                launch_timestamp=datetime.utcnow(),
                date=None,
                symbol=None,
                message="No values available in bars_2",
            )

    else:
        # delete rows imidiately, so it won't interfere with other processes
        with engine.connect() as con:
            statement = text("""DELETE FROM bars_2 WHERE id IN :ids_list""")
            con.execute(
                statement,
                ids_list=tuple(rows["id"]),
            )

        rows = rows.dropna()

        not_present_errors(rows)

        compare_with_last_ten_days(rows)
