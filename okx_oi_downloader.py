# okx_oi_downloader.py (GitHub Actions 版本)
import asyncio
import httpx
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import time

# --- 配置區 ---
# (SYMBOLS 列表保持不變，此處省略以節省篇幅)
SYMBOLS = [
    '1INCH-USDT-SWAP', 'A-USDT-SWAP', 'AAVE-USDT-SWAP', 'ACE-USDT-SWAP', 'ACH-USDT-SWAP', 'ACT-USDT-SWAP', 
    'ADA-USDT-SWAP', 'AERO-USDT-SWAP', 'AEVO-USDT-SWAP', 'AGLD-USDT-SWAP', 'AI16Z-USDT-SWAP', 'AIXBT-USDT-SWAP', 
    'ALCH-USDT-SWAP', 'ALGO-USDT-SWAP', 'ANIME-USDT-SWAP', 'APE-USDT-SWAP', 'API3-USDT-SWAP', 'APT-USDT-SWAP', 
    'AR-USDT-SWAP', 'ARB-USDT-SWAP', 'ARC-USDT-SWAP', 'ARKM-USDT-SWAP', 'ATH-USDT-SWAP', 'ATOM-USDT-SWAP', 
    'AUCTION-USDT-SWAP', 'AVAAI-USDT-SWAP', 'AVAX-USDT-SWAP', 'AXS-USDT-SWAP', 'BABY-USDT-SWAP', 'BAND-USDT-SWAP', 
    'BAT-USDT-SWAP', 'BCH-USDT-SWAP', 'BERA-USDT-SWAP', 'BICO-USDT-SWAP', 'BIGTIME-USDT-SWAP', 'BIO-USDT-SWAP', 
    'BLUR-USDT-SWAP', 'BNB-USDT-SWAP', 'BNT-USDT-SWAP', 'BOME-USDT-SWAP', 'BONK-USDT-SWAP', 'BRETT-USDT-SWAP', 
    'BTC-USDT-SWAP', 'CAT-USDT-SWAP', 'CATI-USDT-SWAP', 'CELO-USDT-SWAP', 'CETUS-USDT-SWAP', 'CFX-USDT-SWAP', 
    'CHZ-USDT-SWAP', 'COMP-USDT-SWAP', 'COOKIE-USDT-SWAP', 'CORE-USDT-SWAP', 'CRO-USDT-SWAP', 'CRV-USDT-SWAP', 
    'CTC-USDT-SWAP', 'CVC-USDT-SWAP', 'CVX-USDT-SWAP', 'DEGEN-USDT-SWAP', 'DGB-USDT-SWAP', 'DOG-USDT-SWAP', 
    'DOGE-USDT-SWAP', 'DOGS-USDT-SWAP', 'DOOD-USDT-SWAP', 'DOT-USDT-SWAP', 'DUCK-USDT-SWAP', 'DYDX-USDT-SWAP', 
    'EGLD-USDT-SWAP', 'EIGEN-USDT-SWAP', 'ENJ-USDT-SWAP', 'ENS-USDT-SWAP', 'ETC-USDT-SWAP', 'ETH-USDT-SWAP', 
    'ETHFI-USDT-SWAP', 'ETHW-USDT-SWAP', 'FARTCOIN-USDT-SWAP', 'FIL-USDT-SWAP', 'FLM-USDT-SWAP', 'FLOKI-USDT-SWAP', 
    'FLOW-USDT-SWAP', 'FXS-USDT-SWAP', 'GALA-USDT-SWAP', 'GAS-USDT-SWAP', 'GLM-USDT-SWAP', 'GMT-USDT-SWAP', 
    'GMX-USDT-SWAP', 'GOAT-USDT-SWAP', 'GODS-USDT-SWAP', 'GPS-USDT-SWAP', 'GRASS-USDT-SWAP', 'GRIFFAIN-USDT-SWAP', 
    'GRT-USDT-SWAP', 'H-USDT-SWAP', 'HBAR-USDT-SWAP', 'HMSTR-USDT-SWAP', 'HOME-USDT-SWAP', 'HUMA-USDT-SWAP', 
    'HYPE-USDT-SWAP', 'ICP-USDT-SWAP', 'ICX-USDT-SWAP', 'IMX-USDT-SWAP', 'INIT-USDT-SWAP', 'INJ-USDT-SWAP', 
    'IOST-USDT-SWAP', 'IOTA-USDT-SWAP', 'IP-USDT-SWAP', 'JELLYJELLY-USDT-SWAP', 'JOE-USDT-SWAP', 'JST-USDT-SWAP', 
    'JTO-USDT-SWAP', 'JUP-USDT-SWAP', 'KAITO-USDT-SWAP', 'KMNO-USDT-SWAP', 'KSM-USDT-SWAP', 'LA-USDT-SWAP', 
    'LAUNCHCOIN-USDT-SWAP', 'LAYER-USDT-SWAP', 'LDO-USDT-SWAP', 'LINK-USDT-SWAP', 'LPT-USDT-SWAP', 'LQTY-USDT-SWAP', 
    'LRC-USDT-SWAP', 'LTC-USDT-SWAP', 'LUNA-USDT-SWAP', 'LUNC-USDT-SWAP', 'MAGIC-USDT-SWAP', 'MAJOR-USDT-SWAP', 
    'MANA-USDT-SWAP', 'MASK-USDT-SWAP', 'ME-USDT-SWAP', 'MEME-USDT-SWAP', 'MERL-USDT-SWAP', 'METIS-USDT-SWAP', 
    'MEW-USDT-SWAP', 'MINA-USDT-SWAP', 'MKR-USDT-SWAP', 'MOG-USDT-SWAP', 'MOODENG-USDT-SWAP', 'MORPHO-USDT-SWAP', 
    'MOVE-USDT-SWAP', 'MUBARAK-USDT-SWAP', 'NEAR-USDT-SWAP', 'NEIRO-USDT-SWAP', 'NEIROETH-USDT-SWAP', 
    'NEO-USDT-SWAP', 'NEWT-USDT-SWAP', 'NMR-USDT-SWAP', 'NOT-USDT-SWAP', 'NXPC-USDT-SWAP', 'OL-USDT-SWAP', 
    'OM-USDT-SWAP', 'ONDO-USDT-SWAP', 'ONE-USDT-SWAP', 'ONT-USDT-SWAP', 'OP-USDT-SWAP', 'ORBS-USDT-SWAP', 
    'ORDI-USDT-SWAP', 'PARTI-USDT-SWAP', 'PENGU-USDT-SWAP', 'PEOPLE-USDT-SWAP', 'PEPE-USDT-SWAP', 'PERP-USDT-SWAP', 
    'PI-USDT-SWAP', 'PLUME-USDT-SWAP', 'PNUT-USDT-SWAP', 'POL-USDT-SWAP', 'POPCAT-USDT-SWAP', 'PRCL-USDT-SWAP', 
    'PROMPT-USDT-SWAP', 'PUMP-USDT-SWAP', 'PYTH-USDT-SWAP', 'QTUM-USDT-SWAP', 'RAY-USDT-SWAP', 'RENDER-USDT-SWAP', 
    'RESOLV-USDT-SWAP', 'RSR-USDT-SWAP', 'RVN-USDT-SWAP', 'S-USDT-SWAP', 'SAHARA-USDT-SWAP', 'SAND-USDT-SWAP', 
    'SATS-USDT-SWAP', 'SCR-USDT-SWAP', 'SHELL-USDT-SWAP', 'SHIB-USDT-SWAP', 'SIGN-USDT-SWAP', 'SLP-USDT-SWAP', 
    'SNX-USDT-SWAP', 'SOL-USDT-SWAP', 'SOLV-USDT-SWAP', 'SONIC-USDT-SWAP', 'SOON-USDT-SWAP', 'SOPH-USDT-SWAP', 
    'SPK-USDT-SWAP', 'SPX-USDT-SWAP', 'SSV-USDT-SWAP', 'STORJ-USDT-SWAP', 'STRK-USDT-SWAP', 'STX-USDT-SWAP', 
    'SUI-USDT-SWAP', 'SUSHI-USDT-SWAP', 'SWARMS-USDT-SWAP', 'SYRUP-USDT-SWAP', 'T-USDT-SWAP', 'TAO-USDT-SWAP', 
    'THETA-USDT-SWAP', 'TIA-USDT-SWAP', 'TNSR-USDT-SWAP', 'TON-USDT-SWAP', 'TRB-USDT-SWAP', 'TREE-USDT-SWAP', 
    'TRUMP-USDT-SWAP', 'TRX-USDT-SWAP', 'TURBO-USDT-SWAP', 'UMA-USDT-SWAP', 'UNI-USDT-SWAP', 'USDC-USDT-SWAP', 
    'USELESS-USDT-SWAP', 'USTC-USDT-SWAP', 'UXLINK-USDT-SWAP', 'VANA-USDT-SWAP', 'VINE-USDT-SWAP', 
    'VIRTUAL-USDT-SWAP', 'W-USDT-SWAP', 'WAL-USDT-SWAP', 'WAXP-USDT-SWAP', 'WCT-USDT-SWAP', 'WIF-USDT-SWAP', 
    'WLD-USDT-SWAP', 'WLFI-USDT-SWAP', 'WOO-USDT-SWAP', 'XAUT-USDT-SWAP', 'XLM-USDT-SWAP', 'XPL-USDT-SWAP', 
    'XRP-USDT-SWAP', 'XTZ-USDT-SWAP', 'YFI-USDT-SWAP', 'YGG-USDT-SWAP', 'ZENT-USDT-SWAP', 'ZEREBRO-USDT-SWAP', 
    'ZETA-USDT-SWAP', 'ZIL-USDT-SWAP', 'ZK-USDT-SWAP', 'ZRO-USDT-SWAP', 'ZRX-USDT-SWAP'
]
DATA_DIR = Path("okx_oi_data")
OKX_API_URL = "https://www.okx.com/api/v5/public/open-interest"
REQUESTS_PER_BATCH = 10
BATCH_INTERVAL_SECONDS = 0.2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# fetch_oi 和 save_data 函數與之前版本完全相同，此處省略以節省篇幅
# ... (請從上一回答中複製 fetch_oi 和 save_data 函數到這裡) ...
async def fetch_oi(client: httpx.AsyncClient, symbol: str) -> dict | None:
    """異步獲取單個交易對的未平倉合約數據"""
    params = {"instId": symbol}
    try:
        response = await client.get(OKX_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get("code") == "0" and data.get("data"):
            item = data["data"][0]
            return {"symbol": symbol, "timestamp_ms": int(item["ts"]), "oi_value": float(item["oiCcy"])}
        else:
            logging.warning(f"Failed to fetch {symbol}. API response: {data.get('msg', 'Unknown error')}")
            return None
    except httpx.RequestError as e:
        logging.error(f"Request error for {symbol}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred for {symbol}: {e}")
        return None

def save_data(data_points: list[dict]):
    """將採集到的數據點保存到按日期分區的CSV文件中"""
    if not data_points:
        return
    df = pd.DataFrame(data_points)
    df['datetime_utc'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
    df = df[['datetime_utc', 'symbol', 'oi_value', 'timestamp_ms']]
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    date_dir = DATA_DIR / today_str
    date_dir.mkdir(parents=True, exist_ok=True)
    file_path = date_dir / f"{today_str}_okx_oi.csv"
    is_new_file = not file_path.exists()
    df.to_csv(file_path, mode='a', header=is_new_file, index=False)
    logging.info(f"Successfully saved {len(data_points)} data points to {file_path}")

async def main():
    """主程序，只運行一次獲取當前數據"""
    logging.info(f"--- Starting fetch cycle at {datetime.utcnow().isoformat()} ---")
    all_results = []
    async with httpx.AsyncClient() as client:
        for i in range(0, len(SYMBOLS), REQUESTS_PER_BATCH):
            batch = SYMBOLS[i:i + REQUESTS_PER_BATCH]
            tasks = [fetch_oi(client, symbol) for symbol in batch]
            results = await asyncio.gather(*tasks)
            successful_results = [res for res in results if res is not None]
            all_results.extend(successful_results)
            if i + REQUESTS_PER_BATCH < len(SYMBOLS):
                await asyncio.sleep(BATCH_INTERVAL_SECONDS)
    logging.info(f"Fetch cycle completed. Fetched {len(all_results)}/{len(SYMBOLS)} data points.")
    save_data(all_results)
    logging.info("--- Fetch cycle finished ---")

if __name__ == "__main__":
    asyncio.run(main())