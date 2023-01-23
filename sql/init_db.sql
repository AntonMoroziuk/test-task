CREATE TABLE IF NOT EXISTS bars_1 (
  id SERIAL,
  date DATE,
  /* Symbols are up to 5 chars long */
  symbol VARCHAR(5),
  adj_close NUMERIC,
  close NUMERIC,
  high NUMERIC,
  low NUMERIC,
  open NUMERIC,
  volume NUMERIC,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS bars_2 (
  id serial,
  date DATE,
  /* Symbols are up to 5 chars long */
  symbol VARCHAR(5),
  adj_close NUMERIC,
  close NUMERIC,
  high NUMERIC,
  low NUMERIC,
  open NUMERIC,
  volume NUMERIC,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS error_log (
  id serial,
  launch_timestamp TIMESTAMP NOT NULL,
  date DATE,
  symbol VARCHAR(5),
  message VARCHAR(255) NOT NULL
);


COPY bars_1(date, symbol, adj_close, close, high, low, open, volume)
FROM '/data/bars_1.csv'
DELIMITER ','
CSV HEADER;

COPY bars_2(date, symbol, adj_close, close, high, low, open, volume)
FROM '/data/bars_2.csv'
DELIMITER ','
CSV HEADER;
