// fetch_tokens.ts
import fetch from "node-fetch";

interface BirdEyeToken {
  address: string;
  symbol: string;
  name: string;
  liquidity: number | string;
}

export async function getTopTokens(): Promise<BirdEyeToken[]> {
  const url = "https://public-api.birdeye.so/defi/v3/token/list?sort_by=liquidity&sort_type=desc&limit=100";

  const res = await fetch(url, {
    headers: {
      "x-chain": "solana",
      "accept": "application/json",
      "X-API-KEY": process.env.BIRDEYE_API_KEY!,
    },
  });

  const data = await res.json();

  if (!data.success) {
    console.error("error:", data);
    return [];
  }

  const tokens = data.data.items;

  return tokens as BirdEyeToken[];
}

export async function getMajorTokenMints(): Promise<string[]> {
  const tokens = await getTopTokens();
  const topTokens = tokens.slice(0, 50);
  const mints = topTokens.map((t: BirdEyeToken) => t.address);

  return mints;
}
