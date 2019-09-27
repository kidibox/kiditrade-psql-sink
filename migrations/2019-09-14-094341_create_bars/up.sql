/* CREATE EXTENSION IF NOT EXISTS time */
CREATE TABLE bars (
    symbol VARCHAR NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    size INT NOT NULL,
    open DECIMAL(10, 4) NOT NULL,
    high DECIMAL(10, 4) NOT NULL,
    low DECIMAL(10, 4) NOT NULL,
    close DECIMAL(10, 4) NOT NULL,
    wap DECIMAL(10, 4) NOT NULL,
    volume BIGINT NOT NULL,
    trades BIGINT NOT NULL,
    PRIMARY KEY (symbol, time, size)
);
