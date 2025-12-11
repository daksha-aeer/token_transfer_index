import bs58 from 'bs58';
import { subscribe, CommitmentLevel, SubscribeRequest, ChannelOptions, CompressionAlgorithms } from 'helius-laserstream'
import type { LaserstreamConfig } from 'helius-laserstream'

// function convertBuffers(obj: any): any {
//   if (!obj) return obj;
//   if (Buffer.isBuffer(obj) || obj instanceof Uint8Array) {
//     return bs58.encode(obj);
//   }
//   if (Array.isArray(obj)) {
//     return obj.map(item => convertBuffers(item));
//   }
//   if (typeof obj === 'object') {
//     return Object.fromEntries(
//       Object.entries(obj).map(([key, value]) => [key, convertBuffers(value)])
//     );
//   }
//   return obj;
// }

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

async function main() {
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
    apiKey: 'YOUR_API_KEY',
    endpoint: 'https://laserstream-mainnet-ewr.helius-rpc.com',
    maxReconnectAttempts: 10,
    channelOptions: channelOptions,
    replay: true,
  }

  try {
    const stream = await subscribe(
      config,
      request,
      async (update) => {
        // if (update.slot) {
        //   console.log(`Slot ${update.slot.slot}: parent=${update.slot.parent}`);
        // }
        if (update.transaction?.tokenTransfers?.length > 0) {
          for (const t of update.transaction.tokenTransfers) {
            console.log("SPL Token Transfer Detected:");
            console.log({
              signature: bs58.encode(update.transaction.transaction.signature),
              mint: t.mint,
              from: t.fromUserAccount,
              to: t.toUserAccount,
              amount: t.tokenAmount,
              decimals: t.tokenAmountDecimals,
              slot: update.slot,
            });
          }
        }

        // if (update.transaction) {
        //   console.log(`Transaction: ${update.transaction.transaction?.signature}`);
        //   const decodedTransaction = convertBuffers(update.transaction);
        //   console.log('💸 Decoded transaction:', JSON.stringify(decodedTransaction, null, 2));

        //   processTransaction(update.transaction);
        // }
      },
      async (error) => {
        console.error('Stream error:', error);
      }
    );

    console.log(`Stream connected: ${stream.id}`);
    
    // Handle graceful shutdown
    process.on('SIGINT', () => {
      console.log('Shutting down...');
      stream.cancel();
      process.exit(0);
    });
    
  } catch (error) {
    console.error('Failed to start stream:', error);
    process.exit(1);
  }
}

// function processTransaction(txUpdate: any) {
//   const tx = txUpdate.transaction;
//   const meta = tx.meta;
  
//   console.log('Transaction Details:');
//   console.log('- Signature:', bs58.encode(tx.signature));
//   console.log('- Slot:', txUpdate.slot);
//   console.log('- Success:', meta.err === null);
//   console.log('- Fee:', meta.fee, 'lamports');
//   console.log('- Compute Units:', meta.computeUnitsConsumed);
  
//   // Account keys are already available in the message
//   const message = tx.transaction.message;
//   if (message.accountKeys) {
//     console.log('- Account Keys:');
//     message.accountKeys.forEach((key: Uint8Array, index: number) => {
//       console.log(`  ${index}: ${bs58.encode(key)}`);
//     });
//   }
  
//   // Log messages are already UTF-8 strings
//   if (meta.logMessages && meta.logMessages.length > 0) {
//     console.log('- Log Messages:');
//     meta.logMessages.forEach((log: string) => {
//       console.log(`  ${log}`);
//     });
//   }
// }

main().catch(console.error);