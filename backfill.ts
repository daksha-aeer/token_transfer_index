// backfill.ts
import fetch from "node-fetch";

// import { Connection, PublicKey } from "@solana/web3.js";
import { insertTokenTransfersBatch, type TokenTransfer } from "./db.js";
import { getMajorTokenMints } from "./fetch_tokens.js";

const HELIUS_API_KEY = process.env.HELIUS_API_KEY!;
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

const BASE_URL = "https://api-mainnet.helius-rpc.com/v0/addresses";


// const connection = new Connection(RPC_URL);

export async function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}


export async function backfillMint(
  mint: string,
  majorMints: Set<string>
) {
  console.log(`\n=== Backfilling mint ${mint} ===`);

  const THIRTY_DAYS_AGO =
    Math.floor(Date.now() / 1000) - 30 * 24 * 60 * 60;

  let before: string | undefined;
  const limit = 100;
  const dbBatchSize = 500;

  while (true) {
    const url =
      `${BASE_URL}/${mint}/transactions` +
      `?api-key=${HELIUS_API_KEY}` +
      `&type=TRANSFER` +
      `&limit=${limit}` +
      (before ? `&before=${before}` : "");

    const res = await fetch(url);
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Helius error ${res.status}: ${text}`);
    }

    const txs = await res.json();
    if (!Array.isArray(txs) || txs.length === 0) break;

    const transfers: TokenTransfer[] = [];

    for (const tx of txs) {
      if (!tx.timestamp || tx.timestamp < THIRTY_DAYS_AGO) {
        if (transfers.length > 0) {
          await insertTokenTransfersBatch(transfers);
        }
        return;
      }

      if (!tx.tokenTransfers?.length) continue;

      const blockTime = new Date(tx.timestamp * 1000);

      tx.tokenTransfers.forEach(
        (t: any, i: number) => {
          if (!majorMints.has(t.mint)) return;

          transfers.push({
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

      if (transfers.length >= dbBatchSize) {
        await insertTokenTransfersBatch(
          transfers.splice(0, dbBatchSize)
        );
      }
    }

    if (transfers.length > 0) {
      await insertTokenTransfersBatch(transfers);
    }

    before = txs[txs.length - 1].signature;
    await sleep(100); // rate-limit protection
  }
}

export async function runBackfill(concurrency = 6) {
  const mints = await getMajorTokenMints();
  const majorMints = new Set(mints);

  let index = 0;

  const worker = async () => {
    while (true) {
      const i = index++;
      if (i >= mints.length) return;
      await backfillMint(mints[i]!, majorMints);
    }
  };

  await Promise.all(
    Array.from({ length: concurrency }, worker)
  );

  console.log("Backfill complete.");
}
