import asyncio
from functools import partial

import websockets
from pyarrow.parquet import ParquetFile

EXAMPLE_PARQUET_FILE = "trades_sample.parquet"
BATCH_SIZE = 1024


async def handler(websocket, path):
    parquet_file = ParquetFile(path)
    iterator = parquet_file.iter_batches(batch_size=BATCH_SIZE)
    msg = None

    batch = next(iterator).to_pandas()

    while batch is not None:
        to_send = batch[batch["timestamp"].eq(batch.iloc[:1]["timestamp"].values[0])]
        batch = batch.drop(to_send.index)

        while batch.shape[0] == 0:
            # we might have other records with the same timestamp in the next batch
            try:
                batch = next(iterator).to_pandas()
            except StopIteration:
                # this was the last batch
                batch = None
                break
            
            additional_rows = batch[
                batch["timestamp"].eq(to_send.iloc[:1]["timestamp"])
            ]

            if additional_rows.shape[0]:
                to_send = to_send.append(additional_rows)
                batch = batch.drop(additional_rows.index)

        to_send["timestamp"] = to_send.apply(
            lambda x: x["timestamp"].strftime("%Y-%m-%d %H:%M:%S.%f"), axis=1
        )
        msg = to_send.to_json(orient="records")

        await websocket.send(msg)


async def start_server(parquet_file_path: str):
    async with websockets.serve(
        partial(handler, path=parquet_file_path), "127.0.0.1", 8000
    ):
        print("Server running at port 8000")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(start_server(EXAMPLE_PARQUET_FILE))
