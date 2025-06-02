const axios = require('axios');
const { URLSearchParams } = require('url'); // For ServerChan POST data
const os = require('os'); // Not strictly needed here as SERVERCHAN_SENDKEY is main env var

// --- Configuration ---
const SERVERCHAN_SENDKEY = 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e';
const KLINE_INTERVAL_STR = "1h"; // Binance API interval string for 1 hour
const VOLUME_MULTIPLIER = 10;
const MA_PERIOD = 20;

const QUOTE_ASSET_FILTER = 'USDT';
const CONTRACT_TYPE_FILTER = 'PERPETUAL';
const STATUS_FILTER = 'TRADING';

const BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com";

// --- Helper Functions ---

async function sendServerChanNotification(title, content) {
    if (!SERVERCHAN_SENDKEY) {
        console.log("ServerChan SendKey not configured. Skipping notification.");
        return;
    }
    const url = `https://sctapi.ftqq.com/${SERVERCHAN_SENDKEY}.send`;
    const params = new URLSearchParams();
    params.append('title', title);
    params.append('desp', content);

    try {
        const response = await axios.post(url, params, {
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            timeout: 10000 // 10 seconds timeout
        });
        if (response.data && (response.data.code === 0 || response.data.errno === 0 || response.data.data?.errno === 0)) { // Check new and old API response
            console.log(`ServerChan notification sent successfully: ${title}`);
        } else {
            console.error(`ServerChan notification failed: ${response.data.message || response.data.errmsg || 'Unknown error'}`, response.data);
        }
    } catch (error) {
        if (error.response) {
            console.error(`Error sending ServerChan notification (status ${error.response.status}):`, error.response.data);
        } else if (error.request) {
            console.error('Error sending ServerChan notification: No response received.', error.request);
        } else {
            console.error('Error sending ServerChan notification:', error.message);
        }
    }
}

async function getTradableSymbols() {
    console.log(`Fetching Binance ${QUOTE_ASSET_FILTER} perpetual futures symbols (direct API)...`);
    const endpoint = "/fapi/v1/exchangeInfo";
    const url = BINANCE_FUTURES_BASE_URL + endpoint;
    try {
        const response = await axios.get(url, { timeout: 15000 }); // 15s timeout
        const exchangeInfo = response.data;
        let symbols = [];
        if (exchangeInfo && exchangeInfo.symbols) {
            symbols = exchangeInfo.symbols
                .filter(item =>
                    item.quoteAsset === QUOTE_ASSET_FILTER &&
                    item.contractType === CONTRACT_TYPE_FILTER &&
                    item.status === STATUS_FILTER &&
                    !item.symbol.includes('_') // Filter out symbols like BTCUSDT_230929
                )
                .map(item => item.symbol);
        }
        symbols.sort();
        console.log(`Fetched ${symbols.length} qualifying symbols.`);
        if (symbols.length > 0) {
            console.log(`Sample symbols: ${symbols.slice(0, 5).join(', ')}...`);
        }
        return symbols;
    } catch (error) {
        console.error(`Failed to fetch symbols: ${error.message}`);
        return [];
    }
}

async function getKlinesData(symbol, intervalStr, limit = 50) {
    const endpoint = "/fapi/v1/klines";
    const url = BINANCE_FUTURES_BASE_URL + endpoint;
    const params = { symbol, interval: intervalStr, limit };
    try {
        const response = await axios.get(url, { params, timeout: 10000 }); // 10s timeout
        const klinesRaw = response.data;
        // K-line data structure:
        // [ Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, ... ]
        return klinesRaw.map(k => ({
            openTime: new Date(k[0]),
            open: parseFloat(k[1]),
            high: parseFloat(k[2]),
            low: parseFloat(k[3]),
            close: parseFloat(k[4]),
            volume: parseFloat(k[5]),
            closeTime: new Date(k[6]),
            quoteAssetVolume: parseFloat(k[7]),
            numberOfTrades: parseInt(k[8]),
        }));
    } catch (error) {
        console.error(`Failed to fetch klines for ${symbol}: ${error.message}`);
        return null;
    }
}

function calculateMA(data, period, key = 'volume') {
    if (!data || data.length < period) return NaN;
    let sum = 0;
    for (let i = data.length - period; i < data.length; i++) {
        sum += data[i][key];
    }
    return sum / period;
}

async function checkVolumeAlert(symbol) {
    const now = new Date().toISOString();
    console.log(`[${now}] Checking ${symbol}...`);

    // Fetch enough data for MA_PERIOD and previous/current candle
    const klines = await getKlinesData(symbol, KLINE_INTERVAL_STR, MA_PERIOD + 10);

    if (!klines || klines.length < MA_PERIOD + 2) {
        console.log(`${symbol} insufficient data for analysis (need ${MA_PERIOD + 2}, got ${klines ? klines.length : 0}).`);
        return;
    }

    // The last candle in the array is the most recent *complete* one if script runs shortly after hour top
    const currentCandle = klines[klines.length - 1];
    const previousCandle = klines[klines.length - 2];

    if (!currentCandle || !previousCandle) {
        console.log(`${symbol} could not identify current or previous candle.`);
        return;
    }

    const currentVolume = currentCandle.volume;
    const previousVolume = previousCandle.volume;

    // Calculate MA20 based on the 20 candles *ending at the previousCandle*
    const ma20DataSlice = klines.slice(klines.length - 1 - MA_PERIOD, klines.length - 1);
    const ma20Volume = calculateMA(ma20DataSlice, MA_PERIOD, 'volume');

    console.log(`${symbol} @ ${currentCandle.closeTime.toISOString()}:`);
    console.log(`  Current Volume: ${currentVolume.toLocaleString(undefined, {maximumFractionDigits:2})}`);
    console.log(`  Previous Hour Volume: ${previousVolume.toLocaleString(undefined, {maximumFractionDigits:2})}`);
    console.log(`  MA${MA_PERIOD} Volume (ending previous hour): ${isNaN(ma20Volume) ? 'N/A' : ma20Volume.toLocaleString(undefined, {maximumFractionDigits:2})}`);

    let alertTriggered = false;
    const alertReasons = [];

    if (previousVolume > 0) {
        if (currentVolume >= VOLUME_MULTIPLIER * previousVolume) {
            alertTriggered = true;
            const reason = `Current Volume (${currentVolume.toLocaleString(undefined, {maximumFractionDigits:2})}) >= ${VOLUME_MULTIPLIER} * Previous Volume (${previousVolume.toLocaleString(undefined, {maximumFractionDigits:2})})`;
            alertReasons.push(reason);
            console.log(`  ALERT: ${reason}`);
        }
    } else if (currentVolume > 0) {
        console.log(`  INFO: Previous hour volume was 0, current volume is ${currentVolume.toLocaleString(undefined, {maximumFractionDigits:2})}.`);
    }

    if (!isNaN(ma20Volume) && ma20Volume > 0) {
        if (currentVolume >= VOLUME_MULTIPLIER * ma20Volume) {
            alertTriggered = true;
            const reason = `Current Volume (${currentVolume.toLocaleString(undefined, {maximumFractionDigits:2})}) >= ${VOLUME_MULTIPLIER} * MA${MA_PERIOD} Volume (${ma20Volume.toLocaleString(undefined, {maximumFractionDigits:2})})`;
            if (!alertReasons.some(r => r.startsWith(reason.substring(0,20)))) { // Basic check to avoid very similar reasons
                 alertReasons.push(reason);
            }
            console.log(`  ALERT: ${reason}`);
        }
    } else if (!isNaN(ma20Volume) && ma20Volume === 0 && currentVolume > 0) {
        console.log(`  INFO: MA${MA_PERIOD} volume was 0, current volume is ${currentVolume.toLocaleString(undefined, {maximumFractionDigits:2})}.`);
    } else if (isNaN(ma20Volume)) {
        console.log(`  INFO: MA${MA_PERIOD} volume could not be calculated.`);
    }


    if (alertTriggered) {
        const title = `Binance ${symbol} Futures Hourly Volume Alert!`;
        const content = `
Trading Pair: ${symbol}
Time (Candle Close): ${currentCandle.closeTime.toISOString()} UTC
Current Volume: ${currentVolume.toLocaleString(undefined, {maximumFractionDigits:2})}
Previous Hour Volume: ${previousVolume.toLocaleString(undefined, {maximumFractionDigits:2})}
MA${MA_PERIOD} Volume (ending previous hour): ${isNaN(ma20Volume) ? 'N/A' : ma20Volume.toLocaleString(undefined, {maximumFractionDigits:2})}

Trigger Reasons:
${alertReasons.map(r => `- ${r}`).join('\n')}
        `;
        await sendServerChanNotification(title, content.trim());
    } else {
        console.log(`  ${symbol} no alert triggered.`);
    }
    console.log("-".repeat(30));
}

// --- Main Execution ---
async function main() {
    console.log("Starting Binance Futures Volume Alert (Node.js - direct API)...");

    if (!SERVERCHAN_SENDKEY) {
        console.warn("Warning: SERVERCHAN_SENDKEY is not configured. Notifications will be skipped.");
    }

    const symbolsToMonitor = await getTradableSymbols();

    if (!symbolsToMonitor || symbolsToMonitor.length === 0) {
        console.error("No symbols to monitor. Exiting.");
        process.exit(1);
    }

    console.log(`Will monitor ${symbolsToMonitor.length} symbols.`);

    const sleepBetweenSymbolsMs = 300; // 300ms

    for (const symbol of symbolsToMonitor) {
        try {
            await checkVolumeAlert(symbol);
        } catch (error) {
            console.error(`Error processing symbol ${symbol}:`, error);
        }
        await new Promise(resolve => setTimeout(resolve, sleepBetweenSymbolsMs));
    }

    console.log("All symbols checked.");
}

main().catch(error => {
    console.error("Critical error in main execution:", error);
    process.exit(1);
});
