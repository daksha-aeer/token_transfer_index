// db.ts
import { Pool } from 'pg';
import type { PoolClient } from 'pg';

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME || 'token_indexer',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  max: 20, // Maximum number of connections
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// Test connection on startup
pool.on('connect', () => {
  console.log('Database connection established');
});

pool.on('error', (err) => {
  console.error('Unexpected database error:', err);
});

export interface TokenTransfer {
  slot: bigint;
  signature: string;
  transfer_index: number;
  mint: string;
  from_account: string | null;
  to_account: string | null;
  amount: string;
  decimals: number;
  block_time: Date;
}


/**
 * Insert a single token transfer into the database
 */
export async function insertTokenTransfer(transfer: TokenTransfer): Promise<void> {
  const query = `
    INSERT INTO token_transfers (
      slot, signature, mint, from_account, to_account, 
      amount, decimals, block_time
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (signature) DO NOTHING
  `;

  const values = [
    transfer.slot.toString(),
    transfer.signature,
    transfer.mint,
    transfer.from_account,
    transfer.to_account,
    transfer.amount,
    transfer.decimals,
    transfer.block_time,
  ];

  await pool.query(query, values);
}


export async function insertTokenTransfersBatch(transfers: TokenTransfer[]): Promise<void> {
  if (transfers.length === 0) return;

  const colsPerRow = 9;
  const values: any[] = [];
  const placeholders: string[] = [];

  transfers.forEach((t, i) => {
    const base = i * colsPerRow;
    placeholders.push(`($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9})`);
    values.push(
      t.slot.toString(),
      t.signature,
      t.transfer_index,
      t.mint,
      t.from_account,
      t.to_account,
      t.amount,
      t.decimals,
      t.block_time
    );
  });

  const query = `
    INSERT INTO token_transfers (
      slot,
      signature,
      transfer_index,
      mint,
      from_account,
      to_account,
      amount,
      decimals,
      block_time
    )
    VALUES ${placeholders.join(',')}
    ON CONFLICT (signature, transfer_index) DO NOTHING
  `;

  await pool.query(query, values);
}

/**
 * Get the last processed slot from pipeline state
 */
export async function getLastProcessedSlot(): Promise<bigint> {
  const result = await pool.query(
    'SELECT last_processed_slot FROM pipeline_state WHERE id = 1'
  );
  return BigInt(result.rows[0].last_processed_slot);
}

/**
 * Update the last processed slot
 */
export async function updateLastProcessedSlot(slot: bigint): Promise<void> {
  await pool.query(
    `UPDATE pipeline_state 
     SET last_processed_slot = $1, last_updated = NOW() 
     WHERE id = 1`,
    [slot.toString()]
  );
}

/**
 * Close database connections gracefully
 */
export async function closeDatabase(): Promise<void> {
  await pool.end();
  console.log('Database connections closed');
}

/**
 * Test database connection
 */
export async function testConnection(): Promise<boolean> {
  try {
    const result = await pool.query('SELECT NOW()');
    console.log('Database connection successful:', result.rows[0].now);
    return true;
  } catch (error) {
    console.error('Database connection failed:', error);
    return false;
  }
}

export { pool };