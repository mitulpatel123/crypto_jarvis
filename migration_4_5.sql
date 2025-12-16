-- Phase 4.5 Migration: Data Enrichment
-- 1. Add Side to Market Ticks
ALTER TABLE market_ticks ADD COLUMN IF NOT EXISTS side VARCHAR(4); -- 'BUY' or 'SELL' or NULL

-- 2. Add Option Metadata to Derivatives Stats
ALTER TABLE derivatives_stats ADD COLUMN IF NOT EXISTS expiry TIMESTAMPTZ;
ALTER TABLE derivatives_stats ADD COLUMN IF NOT EXISTS strike DOUBLE PRECISION;
ALTER TABLE derivatives_stats ADD COLUMN IF NOT EXISTS option_type VARCHAR(4); -- 'CALL' or 'PUT'

-- 3. Index for new columns (Optional but good for future queries)
CREATE INDEX IF NOT EXISTS idx_derivatives_expiry ON derivatives_stats (expiry);
CREATE INDEX IF NOT EXISTS idx_derivatives_strike ON derivatives_stats (strike);
