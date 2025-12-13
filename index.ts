// index.ts
import bs58 from 'bs58';
import { subscribe, CommitmentLevel, SubscribeRequest, ChannelOptions, CompressionAlgorithms } from 'helius-laserstream'
import type { LaserstreamConfig } from 'helius-laserstream'

import { Connection } from '@solana/web3.js';

import { getMajorTokenMints } from "./fetch_tokens.js";

import { 
  insertTokenTransfer,
  insertTokenTransfersBatch,
  getLastProcessedSlot,
  updateLastProcessedSlot, 
  closeDatabase,
  testConnection,
  pool,
  type TokenTransfer 
} from "./db.js";

// --- Batching buffer for live ingestion ---
const LIVE_BATCH_SIZE = 200;       // tune as needed
const LIVE_FLUSH_INTERVAL = 500;   // ms
let liveBuffer: TokenTransfer[] = [];
let flushInProgress = false;

async function flushLiveBuffer() {
  if (flushInProgress) return;
  if (liveBuffer.length === 0) return;

  flushInProgress = true;
  const batch = liveBuffer.splice(0, liveBuffer.length); // take all items

  try {
    await insertTokenTransfersBatch(batch);
  } catch (err) {
    console.error("Live batch insert failed, items will be requeued:", err);
    // Requeue into front — do NOT lose transfers
    liveBuffer.unshift(...batch);
  } finally {
    flushInProgress = false;
  }
}

// Flush on interval (in background)
setInterval(() => {
  flushLiveBuffer();
}, LIVE_FLUSH_INTERVAL);


const channelOptions: ChannelOptions = {

  'grpc.default_compression_algorithm': CompressionAlgorithms.zstd,
  'grpc.max_receive_message_length': 1_000_000_000,

  connectTimeoutSecs: 20,
  maxDecodingMessageSize: 2_000_000_000,
  maxEncodingMessageSize: 64_000_000,
  
  http2KeepAliveIntervalSecs: 15,
  keepAliveTimeoutSecs: 10,
  keepAliveWhileIdle: true,
  
  initialStreamWindowSize: 8_388_608,
  initialConnectionWindowSize: 16_777_216,
  
  http2AdaptiveWindow: true,
  tcpNodelay: true,
  bufferSize: 131_072,
};

async function getCurrentSlot(): Promise<number> {
  const connection = new Connection('https://mainnet.helius-rpc.com/?api-key=<api-key>');
  try {
    const slot: number = await connection.getSlot('confirmed');
    console.log('Current slot (default commitment):', slot);
    return slot;
  } catch (error) {
    console.error('Error fetching current slot:', error);
    throw error;
  }
}

async function main() {

  const lastSlot = await getLastProcessedSlot();  
  // console.log("Resuming from slot:", lastSlot.toString());

  const dbConnected = await testConnection();
  if (!dbConnected) {
    console.error('Failed to connect to database. Exiting...');
    process.exit(1);
  }

  let majorMints = new Set(await getMajorTokenMints());

  setInterval(async () => {
    try {
      majorMints = new Set(await getMajorTokenMints());
      console.log(`Refreshed token list: ${majorMints.size} tokens`);
    } catch (err) {
      console.error('Failed to refresh tokens:', err);
      // Keep using old list
    }
  }, 6 * 60 * 60 * 1000);

  const request = {
    transactions: {
      "token-txs": {
        accountInclude: ['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'],
        accountExclude: [],
        accountRequired: [],
        vote: false,
        failed: false
      }
    },
    commitment: CommitmentLevel.CONFIRMED,
    slots: {
      "slots": {}
    },
    accounts: {},
    transactionsStatus: {},
    blocks: {},
    blocksMeta: {},
    entry: {},
    accountsDataSlice: [],
  };

  const config: LaserstreamConfig = {
    apiKey: process.env.HELIUS_API_KEY!,
    endpoint: 'https://laserstream-mainnet-ewr.helius-rpc.com',
    maxReconnectAttempts: 10,
    channelOptions: channelOptions,
    replay: true,
  }

  request.slots.slots = { start: Number(lastSlot) };
  const currentSlot = await getCurrentSlot();

  await pool.query(`
    UPDATE pipeline_state 
    SET streaming_start_slot = $1 
    WHERE id = 1 AND streaming_start_slot IS NULL
  `, [currentSlot.toString()]);
  
  let lastCheckpointSlot = 0n;
  const CHECKPOINT_INTERVAL = 100n;

  try {
    const stream = await subscribe(
      config,
      request,
      async (update) => {
        if (update.transaction?.tokenTransfers?.length > 0) {
          const slot = BigInt(update.slot);
          const blockTime = new Date(update.transaction.transaction.blockTime * 1000);
          const signature = bs58.encode(update.transaction.transaction.signature);
          
          update.transaction.tokenTransfers.forEach(
            (t: any, i: number) => {
              if (!majorMints.has(t.mint)) return;

              const transfer: TokenTransfer = {
                slot,
                signature,
                transfer_index: i,
                mint: t.mint,
                from_account: t.fromUserAccount || null,
                to_account: t.toUserAccount || null,
                amount: t.tokenAmount.toString(),
                decimals: t.tokenAmountDecimals,
                block_time: blockTime,
              };

              liveBuffer.push(transfer);
            }
          );

          if (liveBuffer.length >= LIVE_BATCH_SIZE) {
            flushLiveBuffer(); // fire-and-forget
          }


          if (slot - lastCheckpointSlot >= CHECKPOINT_INTERVAL) {
            try {
              await updateLastProcessedSlot(slot);
              lastCheckpointSlot = slot;
            } catch (error) {
              console.error('Failed to update slot checkpoint:', error);
            }
          }
          
        }

      },
      async (error) => {
        console.error('Stream error:', {
          timestamp: new Date().toISOString(),
          error: error.message,
        });
      }
    );

    console.log(`Stream connected: ${stream.id}`);
    
    process.on('SIGINT', async () => {
      console.log('Shutting down...');
      stream.cancel();
      await closeDatabase();
      process.exit(0);
    });
    
  } catch (error) {
    console.error('Failed to start stream:', error);
    await closeDatabase();
    process.exit(1);
  }
}

main().catch(console.error);