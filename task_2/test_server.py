import asyncio
import json
import multiprocessing
import time

import pandas as pd
import pytest
import websockets

from server import start_server as _start_server

PYTEST_PARQUET_FILE = "pytest.parquet"


@pytest.fixture()
def create_test_file(tmp_path):
    data = [
        [
            pd.to_datetime("2022-07-01 00:05:14.631000"),
            6000000,
            1200000000000,
            "10000NFT-USDT-SWAP@BYBIT",
        ],
        [
            pd.to_datetime("2022-07-01 00:05:14.631000"),
            6000001,
            1200000000001,
            "10000NFT-USDT-SWAP@BYBIT",
        ],
        [
            pd.to_datetime("2022-07-01 00:05:14.631000"),
            6000002,
            1200000000002,
            "10000NFT-USDT-SWAP@BYBIT",
        ],
        [
            pd.to_datetime("2022-07-01 00:07:14.631000"),
            6000003,
            1200000000003,
            "10000NFT-USDT-SWAP@BYBIT",
        ],
        [
            pd.to_datetime("2022-07-01 00:08:14.631000"),
            6000004,
            1200000000004,
            "10000NFT-USDT-SWAP@BYBIT",
        ],
        [
            pd.to_datetime("2022-07-01 00:08:14.631000"),
            6000005,
            1200000000005,
            "10000NFT-USDT-SWAP@BYBIT",
        ],
        [
            pd.to_datetime("2022-07-01 00:09:14.631000"),
            6000006,
            1200000000006,
            "10000NFT-USDT-SWAP@BYBIT",
        ],
    ]

    df = pd.DataFrame(data, columns=["timestamp", "price", "volume", "ticker"])

    df.to_parquet(tmp_path / PYTEST_PARQUET_FILE)


def start_server(parquet_file_path: str):
    asyncio.run(_start_server(parquet_file_path))


@pytest.fixture()
def setup_server(create_test_file, tmp_path):
    p = multiprocessing.Process(
        target=start_server, args=(tmp_path / PYTEST_PARQUET_FILE,)
    )
    p.start()

    # Wait for the server to start
    time.sleep(1)
    yield
    p.terminate()


def test_trade_history(setup_server):
    async def inner():
        async with websockets.connect("ws://127.0.0.1:8000") as websocket:
            response = json.loads(await websocket.recv())
            assert response == [
                {
                    "timestamp": "2022-07-01 00:05:14.631000",
                    "price": 6000000,
                    "volume": 1200000000000,
                    "ticker": "10000NFT-USDT-SWAP@BYBIT",
                },
                {
                    "timestamp": "2022-07-01 00:05:14.631000",
                    "price": 6000001,
                    "volume": 1200000000001,
                    "ticker": "10000NFT-USDT-SWAP@BYBIT",
                },
                {
                    "timestamp": "2022-07-01 00:05:14.631000",
                    "price": 6000002,
                    "volume": 1200000000002,
                    "ticker": "10000NFT-USDT-SWAP@BYBIT",
                },
            ]

            response = json.loads(await websocket.recv())
            assert response == [
                {
                    "timestamp": "2022-07-01 00:07:14.631000",
                    "price": 6000003,
                    "volume": 1200000000003,
                    "ticker": "10000NFT-USDT-SWAP@BYBIT",
                },
            ]

            response = json.loads(await websocket.recv())
            assert response == [
                {
                    "timestamp": "2022-07-01 00:08:14.631000",
                    "price": 6000004,
                    "volume": 1200000000004,
                    "ticker": "10000NFT-USDT-SWAP@BYBIT",
                },
                {
                    "timestamp": "2022-07-01 00:08:14.631000",
                    "price": 6000005,
                    "volume": 1200000000005,
                    "ticker": "10000NFT-USDT-SWAP@BYBIT",
                },
            ]

            response = json.loads(await websocket.recv())
            assert response == [
                {
                    "timestamp": "2022-07-01 00:09:14.631000",
                    "price": 6000006,
                    "volume": 1200000000006,
                    "ticker": "10000NFT-USDT-SWAP@BYBIT",
                },
            ]

    return asyncio.get_event_loop().run_until_complete(inner())
