// backfill.ts
import { Connection, PublicKey } from "@solana/web3.js";
import { insertTokenTransfersBatch, type TokenTransfer } from "./db.js";
import { getMajorTokenMints } from "./fetch_tokens.js";

const HELIUS_API_KEY = process.env.HELIUS_API_KEY!;
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

const connection = new Connection(RPC_URL);

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// Enhanced Transactions endpoint
async function fetchParsedTransactions(signatures: string[]) {
  const url = `https://api-mainnet.helius-rpc.com/v0/transactions?api-key=${HELIUS_API_KEY}`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ transactions: signatures })
  });

  return res.json();
}

async function fetchParsedTransactionsBatch(signatures: string[], maxRetries = 3) {
  const url = `https://api-mainnet.helius-rpc.com/v0/transactions?api-key=${HELIUS_API_KEY}`;

  let attempt = 0;
  while (true) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ transactions: signatures })
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Helius error ${res.status}: ${text}`);
      }

      const json = await res.json();
      // json is expected to be an array (same order as signatures)
      return json;
    } catch (err) {
      attempt++;
      if (attempt > maxRetries) throw err;
      const backoffMs = 200 * Math.pow(2, attempt); // exponential
      console.warn(`Helius request failed (attempt ${attempt}) — retrying in ${backoffMs}ms:`, err);
      await sleep(backoffMs);
    }
  }
}


export async function backfillMint(mint: string, majorMints: Set<string>, opts?: { concurrencySignatures?: number }) {
  console.log(`\n=== Backfilling mint ${mint} ===`);
  let before: string | undefined = undefined;
  const THIRTY_DAYS = Math.floor(Date.now() / 1000) - 30 * 24 * 3600;
  const signaturesPageSize = 1000; // getSignaturesForAddress supports up to 1000
  const heliusChunkSize = 100; // POST up to ~100 signatures / request (adjust per plan/rate limits)
  const dbBatchSize = 500; // number of transfers to insert per DB batch (tune for memory & DB)

  while (true) {
    const options: any = { limit: signaturesPageSize };
    if (before) options.before = before;

    const sigInfos = await connection.getSignaturesForAddress(new PublicKey(mint), options);
    if (!sigInfos || sigInfos.length === 0) break;

    const signatures = sigInfos.map((s) => s.signature);

    // fetch parsed transactions in chunks (100 per POST)
    const signatureChunks = chunk(signatures, heliusChunkSize);
    for (const sigChunk of signatureChunks) {
      const parsedTxs = await fetchParsedTransactionsBatch(sigChunk);

      // Collect transfers to insert as batch
      const transfersToInsert: TokenTransfer[] = [];

      for (const tx of parsedTxs) {
        if (!tx?.tokenTransfers?.length) continue;

        const ts = tx.timestamp ?? tx.blockTime;
        if (ts && ts < THIRTY_DAYS) {
          if (transfersToInsert.length > 0) {
            await insertTokenTransfersBatch(transfersToInsert);
          }
          return;
        }

        const blockTime = ts
          ? new Date(ts * 1000)
          : new Date();

        tx.tokenTransfers.forEach(
          (t: any, i: number) => {
            if (!majorMints.has(t.mint)) return;

            transfersToInsert.push({
              slot: BigInt(tx.slot),
              signature: tx.signature,
              transfer_index: i,
              mint: t.mint,
              from_account: t.fromUserAccount ?? null,
              to_account: t.toUserAccount ?? null,
              amount: t.tokenAmount.toString(),
              decimals: t.tokenAmountDecimals ?? 0,
              block_time: blockTime,
            });
          }
        );

        if (transfersToInsert.length >= dbBatchSize) {
          await insertTokenTransfersBatch(
            transfersToInsert.splice(0, dbBatchSize)
          );
        }
      } // each parsedTx

      // flush any remaining transfers for this chunk
      if (transfersToInsert.length > 0) {
        await insertTokenTransfersBatch(transfersToInsert.splice(0));
      }

      // small sleep to avoid bursting Helius
      await sleep(100);
    } // signatureChunks

    const last = sigInfos[sigInfos.length - 1];
    before = last?.signature;
    // small throttle between pages
    await sleep(50);
  } // while
}


export async function runBackfill(concurrency = 6) {
  const majorMintsArr = Array.from(await getMajorTokenMints());
  const majorMintsSet = new Set(majorMintsArr);

  console.log(`Starting backfill for ${majorMintsArr.length} mints with concurrency ${concurrency}`);

  let index = 0;
  const workers: Promise<void>[] = [];

  const worker = async () => {
    while (true) {
      const i = index++;
      if (i >= majorMintsArr.length) return;
      const mint = majorMintsArr[i]!;
      try {
        await backfillMint(mint, majorMintsSet);
      } catch (err) {
        console.error(`Backfill error for mint ${mint}:`, err);
        // continue to next mint
      }
    }
  };

  for (let i = 0; i < concurrency; i++) workers.push(worker());

  await Promise.all(workers);
  console.log("Backfill complete.");
}
