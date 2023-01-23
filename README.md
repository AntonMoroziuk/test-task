# test-task

This task consists of three parts, each in separate folder.

## Task 1_1

This task is solved in Jupyter notebook in the corresponding directory. But to run it you'll needed Postgres db from the docker-compose(see below).

## Task 1_2

This task is a part of docker-compose, so to run it you just need to run docker-compose(see below). It can be run localy if you change `db` to `localhost` on line 13(you'll need postgres container with data running).

## Task 2

This task is implemented as server script(`server.py`) and tests for it(`test_server.py`).

Start by initializing `pipenv`:
```
pipenv install
```

Server runs on port 8000. To run server(note: it takes some time to load as it reads parquet file):
```
pipenv run python3 server.py
```

To run tests(server should be stoped or moved to another port):
```
pipenv run pytest
```

## docker-compose

docker-compose uses three environmental variables - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT`. You need to set all of them. Example:
```
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_PORT=5433
```

Then just run:
```
docker-compose up
```