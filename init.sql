-- init.sql
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Main token transfers table
CREATE TABLE token_transfers (
  id BIGSERIAL,
  slot BIGINT NOT NULL,
  signature TEXT NOT NULL,
  transfer_index INT NOT NULL,
  mint TEXT NOT NULL,
  from_account TEXT,
  to_account TEXT,
  amount NUMERIC(40, 0),
  decimals INT,
  block_time TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (block_time, id)
);

-- Convert to hypertable (TimescaleDB magic)
SELECT create_hypertable('token_transfers', 'block_time');

-- Indexes for common query patterns
CREATE INDEX idx_mint_time ON token_transfers (mint, block_time DESC);
CREATE INDEX idx_signature ON token_transfers (signature);
CREATE INDEX idx_slot ON token_transfers (slot);
CREATE INDEX idx_from ON token_transfers (from_account, block_time DESC);
CREATE INDEX idx_to ON token_transfers (to_account, block_time DESC);

CREATE UNIQUE INDEX uniq_token_transfer
ON token_transfers (signature, transfer_index);


-- Pipeline state tracking (single row table)
CREATE TABLE pipeline_state (
  id INT PRIMARY KEY DEFAULT 1,
  last_processed_slot BIGINT NOT NULL DEFAULT 0,
  streaming_start_slot BIGINT,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT single_row CHECK (id = 1)
);

-- Insert initial state
INSERT INTO pipeline_state (id, last_processed_slot, last_updated) 
VALUES (1, 0, NOW());

-- Auto-delete data older than 30 days
SELECT add_retention_policy('token_transfers', INTERVAL '30 days');

-- Compression after 7 days (saves ~90% storage)
ALTER TABLE token_transfers SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'mint'
);
SELECT add_compression_policy('token_transfers', INTERVAL '7 days');

-- Create a view for easier querying
CREATE VIEW recent_transfers AS
SELECT 
  signature,
  mint,
  from_account,
  to_account,
  amount::NUMERIC / POW(10, decimals) AS amount_normalized,
  decimals,
  block_time,
  slot
FROM token_transfers
WHERE block_time > NOW() - INTERVAL '24 hours'
ORDER BY block_time DESC;