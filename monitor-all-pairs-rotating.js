const axios = require('axios');
const fs = require('fs');
const path = require('path');

// 配置参数
const config = {
  // 爆量阈值
  hourSpikeThreshold: 8,    // 1小时成交量是前一根的8倍
  fourHourSpikeThreshold: 5, // 4小时成交量是前一根的5倍
  hourMaThreshold: 8,       // 1小时成交量是MA20的8倍
  fourHourMaThreshold: 5,   // 4小时成交量是MA20的5倍
  minQuoteVolume: 1000000,  // 最小成交额过滤(1百万美元)
  
  // 交易所API
  binance: {
    exchangeInfo: 'https://fapi.binance.com/fapi/v1/exchangeInfo',
    klines: 'https://fapi.binance.com/fapi/v1/klines',
    dailyKline: 'https://fapi.binance.com/fapi/v1/klines?interval=1d'
  },
  okx: {
    instruments: 'https://www.okx.com/api/v5/public/instruments?instType=SWAP',
    klines: 'https://www.okx.com/api/v5/market/candles',
    dailyKline: 'https://www.okx.com/api/v5/market/candles?bar=1D'
  },
  
  // Server酱通知配置
  serverChan: {
    sckey: 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e'
  },
  
  // 系统配置
  pairsPerRun: 10,          // 每次运行检测的交易对数
  checkInterval: 15 * 60 * 1000, // 15分钟检测一次
  stateFile: 'monitor-state.json' // 状态保存文件
};

// 状态管理
let state = {
  binance: {
    allSymbols: [],
    lastIndex: 0,
    lastRun: 0
  },
  okx: {
    allSymbols: [],
    lastIndex: 0,
    lastRun: 0
  }
};

// 主监控函数
async function monitor() {
  console.log('开始全交易对爆量监控检测(轮询模式)...');
  const startTime = new Date();
  
  try {
    // 加载或初始化状态
    await loadState();
    
    // 获取所有交易对(如果尚未获取或需要刷新)
    if (shouldRefreshSymbols()) {
      await getAllSymbols();
    }
    
    console.log(`Binance交易对: ${state.binance.allSymbols.length}个, OKX交易对: ${state.okx.allSymbols.length}个`);
    
    // 获取本次要检测的交易对(轮询方式)
    const [binanceSymbols, okxSymbols] = getSymbolsForThisRun();
    console.log(`本次检测: Binance ${binanceSymbols.length}个, OKX ${okxSymbols.length}个`);
    
    if (binanceSymbols.length === 0 && okxSymbols.length === 0) {
      console.log('没有可检测的交易对');
      return;
    }
    
    // 检测交易对
    const [binanceSpikes, okxSpikes] = await Promise.all([
      checkExchange('binance', binanceSymbols),
      checkExchange('okx', okxSymbols)
    ]);
    
    const allSpikes = [...binanceSpikes, ...okxSpikes];
    
    if (allSpikes.length > 0) {
      console.log(`检测到 ${allSpikes.length} 个爆量信号`);
      await sendServerChanNotifications(allSpikes);
    } else {
      // 微信通知
      try {
        await axios.post(`https://sctapi.ftqq.com/${serverChan.sckey}.send`, {
          title: 'BTC价格警报',
          desp: message
        });
        console.log('微信通知已发送');
      } catch (e) {
        console.error('微信通知发送失败:', e.message);
      }
      
      console.log('未检测到爆量信号');
    }
    
    // 更新状态并保存
    updateStateIndexes(binanceSymbols.length, okxSymbols.length);
    await saveState();
    
  } catch (error) {
    console.error('监控出错:', error);
  }
  
  console.log(`监控完成，耗时: ${(new Date() - startTime)/1000}秒`);
}

// 状态管理函数
// 在 loadState() 函数中添加初始化逻辑
async function loadState() {
  try {
    if (fs.existsSync(config.stateFile)) {
      const data = fs.readFileSync(config.stateFile, 'utf8');
      const savedState = JSON.parse(data);
      
      state.binance.allSymbols = savedState.binance?.allSymbols || [];
      state.binance.lastIndex = savedState.binance?.lastIndex || 0;
      state.binance.lastRun = savedState.binance?.lastRun || 0;
      
      state.okx.allSymbols = savedState.okx?.allSymbols || [];
      state.okx.lastIndex = savedState.okx?.lastIndex || 0;
      state.okx.lastRun = savedState.okx?.lastRun || 0;
      
      console.log('已加载之前的状态');
      return;
    }
  } catch (error) {
    console.error('加载状态失败:', error);
  }

  // 如果没有状态文件，初始化新状态
  console.log('初始化新状态');
  state = {
    binance: {
      allSymbols: [],
      lastIndex: 0,
      lastRun: 0
    },
    okx: {
      allSymbols: [],
      lastIndex: 0,
      lastRun: 0
    }
  };
}
  
async function saveState() {
  try {
    fs.writeFileSync(config.stateFile, JSON.stringify(state, null, 2));
    console.log('状态已保存');
  } catch (error) {
    console.error('保存状态失败:', error);
  }
}

function shouldRefreshSymbols() {
  // 如果还没有获取过交易对
  if (state.binance.allSymbols.length === 0 || state.okx.allSymbols.length === 0) {
    return true;
  }
  
  // 如果超过24小时没有刷新交易对列表
  const now = Date.now();
  const twentyFourHours = 24 * 60 * 60 * 1000;
  return (now - state.binance.lastRun) > twentyFourHours || 
         (now - state.okx.lastRun) > twentyFourHours;
}

// 获取所有交易对
async function getAllSymbols() {
  const [binanceSymbols, okxSymbols] = await Promise.all([
    getBinanceSymbols(),
    getOKXSymbols()
  ]);
  
  state.binance.allSymbols = binanceSymbols;
  state.okx.allSymbols = okxSymbols;
  state.binance.lastRun = Date.now();
  state.okx.lastRun = Date.now();
}

async function getBinanceSymbols() {
  try {
    const response = await axios.get(config.binance.exchangeInfo);
    return response.data.symbols
      .filter(s => s.contractType === 'PERPETUAL' && s.status === 'TRADING')
      .map(s => s.symbol);
  } catch (error) {
    console.error('获取Binance交易对失败:', error.message);
    return [];
  }
}

async function getOKXSymbols() {
  try {
    const response = await axios.get(config.okx.instruments);
    return response.data.data
      .filter(i => i.instType === 'SWAP' && i.state === 'live')
      .map(i => i.instId);
  } catch (error) {
    console.error('获取OKX交易对失败:', error.message);
    return [];
  }
}

// 获取本次运行的交易对(轮询)
function getSymbolsForThisRun() {
  const binanceSymbols = [];
  const okxSymbols = [];
  
  // 计算每个交易所应该检测的交易对数(尽量平均分配)
  const binanceCount = Math.min(
    Math.ceil(config.pairsPerRun / 2),
    state.binance.allSymbols.length
  );
  const okxCount = Math.min(
    Math.floor(config.pairsPerRun / 2),
    state.okx.allSymbols.length
  );
  
  // 如果某个交易所没有足够的交易对，调整另一个交易所的检测数量
  const remaining = config.pairsPerRun - (binanceCount + okxCount);
  if (remaining > 0) {
    if (state.binance.allSymbols.length > binanceCount) {
      binanceCount += remaining;
    } else if (state.okx.allSymbols.length > okxCount) {
      okxCount += remaining;
    }
  }
  
  // Binance交易对
  for (let i = 0; i < binanceCount; i++) {
    const index = (state.binance.lastIndex + i) % state.binance.allSymbols.length;
    binanceSymbols.push(state.binance.allSymbols[index]);
  }
  
  // OKX交易对
  for (let i = 0; i < okxCount; i++) {
    const index = (state.okx.lastIndex + i) % state.okx.allSymbols.length;
    okxSymbols.push(state.okx.allSymbols[index]);
  }
  
  return [binanceSymbols, okxSymbols];
}

// 更新状态索引
function updateStateIndexes(binanceCount, okxCount) {
  if (state.binance.allSymbols.length > 0) {
    state.binance.lastIndex = (state.binance.lastIndex + binanceCount) % state.binance.allSymbols.length;
  }
  
  if (state.okx.allSymbols.length > 0) {
    state.okx.lastIndex = (state.okx.lastIndex + okxCount) % state.okx.allSymbols.length;
  }
}

// 检测单个交易所的多个交易对
async function checkExchange(exchange, symbols) {
  const spikes = [];
  
  for (const symbol of symbols) {
    try {
      // 获取1小时和4小时数据
      const [hourData, fourHourData] = await Promise.all([
        getKlineData(exchange, symbol, '1h', 21), // 20 MA + 当前
        getKlineData(exchange, symbol, '4h', 21)
      ]);
      
      // 获取日线数据用于成交额过滤
      const dailyData = await getDailyKline(exchange, symbol);
      if (dailyData && dailyData.quoteVolume < config.minQuoteVolume) {
        continue; // 跳过成交额太小的交易对
      }
      
      // 检测1小时爆量
      const hourSpike = detectSpike(hourData, config.hourSpikeThreshold, 'hour');
      if (hourSpike) {
        hourSpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
        spikes.push({...hourSpike, exchange, symbol});
      }
      
      // 检测1小时均线爆量
      const hourMASpike = detectMASpike(hourData, config.hourMaThreshold, 'hour-ma');
      if (hourMASpike) {
        hourMASpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
        spikes.push({...hourMASpike, exchange, symbol});
      }
      
      // 检测4小时爆量
      const fourHourSpike = detectSpike(fourHourData, config.fourHourSpikeThreshold, 'fourhour');
      if (fourHourSpike) {
        fourHourSpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
        spikes.push({...fourHourSpike, exchange, symbol});
      }
      
      // 检测4小时均线爆量
      const fourHourMASpike = detectMASpike(fourHourData, config.fourHourMaThreshold, 'fourhour-ma');
      if (fourHourMASpike) {
        fourHourMASpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
        spikes.push({...fourHourMASpike, exchange, symbol});
      }
      
    } catch (error) {
      console.error(`${exchange} ${symbol} 检测失败:`, error.message);
    }
  }
  
  return spikes;
}

// 获取K线数据
async function getKlineData(exchange, symbol, interval, limit) {
  let url, params;
  
  if (exchange === 'binance') {
    url = `${config.binance.klines}?symbol=${symbol}&interval=${interval}&limit=${limit}`;
    const response = await axios.get(url);
    return response.data.map(k => ({
      time: k[0],
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
      quoteVolume: parseFloat(k[7])
    }));
  } else {
    const bar = interval === '1h' ? '1H' : '4H';
    url = `${config.okx.klines}?instId=${symbol}&bar=${bar}&limit=${limit}`;
    const response = await axios.get(url);
    return response.data.data.map(k => ({
      time: k[0],
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
      quoteVolume: parseFloat(k[7])
    }));
  }
}

// 检测普通爆量
function detectSpike(data, threshold, type) {
  if (!data || data.length < 2) return null;
  
  const current = data[data.length - 1];
  const previous = data[data.length - 2];
  
  // 确保有有效的成交量数据
  if (current.volume <= 0 || previous.volume <= 0) {
    return null;
  }
  
  const ratio = current.volume / previous.volume;
  
  if (ratio >= threshold) {
    return {
      type,
      price: current.close,
      timestamp: current.time,
      currentVolume: current.volume,
      compareValue: previous.volume,
      ratio
    };
  }
  return null;
}

// 检测均线爆量
function detectMASpike(data, threshold, type) {
  if (!data || data.length < 21) return null; // MA20需要至少20个数据点
  
  const volumes = data.map(d => d.volume);
  const current = volumes[volumes.length - 1];
  const maValues = calculateMA(volumes, 20);
  const ma = maValues[maValues.length - 1];
  
  // 确保有有效的成交量数据
  if (current <= 0 || ma <= 0) {
    return null;
  }
  
  const ratio = current / ma;
  
  if (ratio >= threshold) {
    return {
      type,
      price: data[data.length - 1].close,
      timestamp: data[data.length - 1].time,
      currentVolume: current,
      compareValue: ma,
      ratio
    };
  }
  return null;
}

// 计算移动平均
function calculateMA(data, period) {
  return data.slice(period - 1).map((_, i) => {
    const sum = data.slice(i, i + period).reduce((a, b) => a + b, 0);
    return sum / period;
  });
}

// 获取日线数据
async function getDailyKline(exchange, symbol) {
  try {
    if (exchange === 'binance') {
      const response = await axios.get(
        `${config.binance.dailyKline}&symbol=${symbol}&limit=1`
      );
      const kline = response.data[0];
      return {
        time: kline[0],
        open: parseFloat(kline[1]),
        high: parseFloat(kline[2]),
        low: parseFloat(kline[3]),
        close: parseFloat(kline[4]),
        volume: parseFloat(kline[5]),
        quoteVolume: parseFloat(kline[7])
      };
    } else {
      const response = await axios.get(
`        `${config.okx.dailyKline}&instId=${symbol}&limit=1`
      );
      const kline = response.data.data[0];
      return {
        time: kline[0],
        open: parseFloat(kline[1]),
        high: parseFloat(kline[2]),
        low: parseFloat(kline[3]),
        close: parseFloat(kline[4]),
        volume: parseFloat(kline[5]),
        quoteVolume: parseFloat(kline[7])
      };
    }
  } catch (error) {
    console.error(`获取${exchange}日线数据失败:`, error.message);
    return null;
  }
}

// 发送Server酱通知
async function sendServerChanNotifications(spikes) {
  // 按交易所分组
  const grouped = spikes.reduce((acc, spike) => {
    if (!acc[spike.exchange]) acc[spike.exchange] = [];
    acc[spike.exchange].push(spike);
    return acc;
  }, {});
  
  // 为每个交易所发送一条汇总通知
  for (const [exchange, exchangeSpikes] of Object.entries(grouped)) {
    const exchangeName = exchange === 'binance' ? 'Binance' : 'OKX';
    
    // 构造Markdown消息
    let message = `## ${exchangeName} 爆量警报汇总\n\n`;
    message += `**检测时间**: ${new Date().toLocaleString()}\n\n`;
    message += "| 交易对 | 类型 | 价格 | 成交量 | 倍数 | 24h成交额 |\n";
    message += "|--------|------|------|--------|------|----------|\n";
    
    exchangeSpikes.forEach(spike => {
      const typeMap = {
        'hour': '1小时',
        'hour-ma': '1小时MA',
        'fourhour': '4小时',
        'fourhour-ma': '4小时MA'
      };
      
      message += `| ${spike.symbol} | ${typeMap[spike.type]} | ${spike.price.toFixed(4)} `;
      message += `| ${spike.currentVolume.toLocaleString()} | ${spike.ratio.toFixed(1)}x `;
      message += `| ${formatVolume(spike.dailyQuoteVolume)} |\n`;
    });
    
    message += `\n[查看完整报告](${getReportLink(exchangeSpikes)})`;
    
    try {
      await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
        title: `${exchangeName} 爆量警报 (${exchangeSpikes.length}个)`,
        desp: message
      });
      console.log(`${exchangeName} Server酱通知发送成功`);
    } catch (error) {
      console.error(`${exchangeName} Server酱通知发送失败:`, error.message);
    }
  }
}

// 格式化成交额
function formatVolume(volume) {
  if (!volume) return '-';
  if (volume >= 1000000000) {
    return `$${(volume / 1000000000).toFixed(2)}B`;
  } else if (volume >= 1000000) {
    return `$${(volume / 1000000).toFixed(2)}M`;
  }
  return `$${Math.round(volume).toLocaleString()}`;
}

// 生成报告链接(示例)
function getReportLink(spikes) {
  const exchange = spikes[0].exchange;
  const symbol = spikes[0].symbol;
  
  if (exchange === 'binance') {
    return `https://www.tradingview.com/chart/?symbol=BINANCE:${symbol}`;
  } else {
    return `https://www.tradingview.com/chart/?symbol=OKX:${symbol.replace('-', '')}`;
  }
}

// 立即执行监控
monitor();
