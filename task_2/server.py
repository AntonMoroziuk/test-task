import asyncio
import json
from collections import OrderedDict

import pandas as pd
import websockets

EXAMPLE_PARQUET_FILE = "trades_sample.parquet"


# Using ordered dict so that we can replay the data in chronological order
to_send = OrderedDict()


async def handler(websocket, path):
    for value in to_send.values():
        msg = [
            {
                "timestamp": item["timestamp"].strftime("%Y-%m-%d %H:%M:%S.%f"),
                "price": item["price"],
                "volume": item["volume"],
                "ticker": item["ticker"],
            }
            for item in value
        ]
        await websocket.send(json.dumps(msg))


async def start_server(parquet_file_path: str):
    df = pd.read_parquet(parquet_file_path, engine="pyarrow")

    # Grouping identical timestamps together to send them in one message
    for index, row in df.iterrows():
        if row["timestamp"] in to_send:
            to_send[row["timestamp"]].append(row)
        else:
            to_send[row["timestamp"]] = [row]

    print("Server running at port 8000")

    async with websockets.serve(handler, "127.0.0.1", 8000):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(start_server(EXAMPLE_PARQUET_FILE))
