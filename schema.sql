-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DROP TABLE IF EXISTS market_ticks CASCADE;
DROP TABLE IF EXISTS derivatives_stats CASCADE;
DROP TABLE IF EXISTS news_sentiment CASCADE;

-- 1. Market Ticks (High Frequency)
CREATE TABLE IF NOT EXISTS market_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    source TEXT,
    side VARCHAR(4) -- 'BUY' or 'SELL'
);

-- Convert to Hypertable (partition by time)
SELECT create_hypertable('market_ticks', 'time', if_not_exists => TRUE);

-- Index for fast symbol lookups ordered by time
CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_time ON market_ticks (symbol, time DESC);

-- 2. Derivatives Stats (Funding, OI, Greeks)
CREATE TABLE IF NOT EXISTS derivatives_stats (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    funding_rate DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    turnover DOUBLE PRECISION,
    iv DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    source TEXT,
    expiry TIMESTAMPTZ, -- Option Expiry
    strike DOUBLE PRECISION, -- Option Strike
    option_type VARCHAR(4) -- 'CALL' or 'PUT'
);

SELECT create_hypertable('derivatives_stats', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_derivatives_symbol_time ON derivatives_stats (symbol, time DESC);

-- 3. News & Whales (Lower Frequency, Text Heavy)
CREATE TABLE IF NOT EXISTS news_sentiment (
    time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    title TEXT,
    currency TEXT,
    sentiment TEXT,
    amount DOUBLE PRECISION, -- For whale transactions
    raw_data JSONB -- Store extra metadata like specific whale wallet addresses
);

SELECT create_hypertable('news_sentiment', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_news_time ON news_sentiment (time DESC);
CREATE INDEX IF NOT EXISTS idx_news_currency ON news_sentiment (currency, time DESC);
