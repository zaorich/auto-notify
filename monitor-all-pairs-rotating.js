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
  minQuoteVolume: 10000000,  // 最小成交额过滤(1百万美元)
  
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
  maxRequestsPerBatch: 10,  // 每次最多请求10个交易对的K线数据
  checkInterval: 60 * 60 * 1000, // 每小时检测一次
  heartbeatInterval: 6 * 60 * 60 * 1000 // 心跳间隔6小时
};

// 状态管理
let state = {
  binance: {
    allSymbols: [],
    lastRun: 0
  },
  okx: {
    allSymbols: [],
    lastRun: 0
  },
  stats: {
    totalRuns: 0,
    lastRunTime: null,
    spikesDetected: 0,
    lastHeartbeat: 0
  }
};

// 主监控函数
async function monitor() {
  console.log('开始全交易对爆量监控检测...');
  const startTime = new Date();
  
  try {
    // 加载或初始化状态
    await loadState();
    
    // 更新统计信息
    state.stats.totalRuns++;
    state.stats.lastRunTime = new Date();
    
    // 获取所有交易对(如果尚未获取或需要刷新)
    if (shouldRefreshSymbols()) {
      await getAllSymbols();
    }
    
    // 获取所有USDT合约交易对
    const [binanceSymbols, okxSymbols] = getSymbolsForThisRun();
    console.log(`Binance USDT合约: ${binanceSymbols.length}个, OKX USDT合约: ${okxSymbols.length}个`);
    
    if (binanceSymbols.length === 0 && okxSymbols.length === 0) {
      console.log('没有可检测的交易对');
      await sendNoTradingPairsNotification();
      return;
    }
    
    // 分批检测交易对（每次最多10个）
    const allSpikes = [];
    
    // 分批检测Binance交易对
    for (let i = 0; i < binanceSymbols.length; i += config.maxRequestsPerBatch) {
      const batch = binanceSymbols.slice(i, i + config.maxRequestsPerBatch);
      console.log(`检测Binance交易对批次 ${i/config.maxRequestsPerBatch + 1}: ${batch.join(', ')}`);
      const spikes = await checkExchange('binance', batch);
      allSpikes.push(...spikes);
      
      // 批次间延迟，避免请求过于频繁
      if (i + config.maxRequestsPerBatch < binanceSymbols.length) {
        await sleep(1000);
      }
    }
    
    // 分批检测OKX交易对
    for (let i = 0; i < okxSymbols.length; i += config.maxRequestsPerBatch) {
      const batch = okxSymbols.slice(i, i + config.maxRequestsPerBatch);
      console.log(`检测OKX交易对批次 ${i/config.maxRequestsPerBatch + 1}: ${batch.join(', ')}`);
      const spikes = await checkExchange('okx', batch);
      allSpikes.push(...spikes);
      
      // 批次间延迟
      if (i + config.maxRequestsPerBatch < okxSymbols.length) {
        await sleep(1000);
      }
    }
    
    state.stats.spikesDetected += allSpikes.length;
    
    if (allSpikes.length > 0) {
      console.log(`检测到 ${allSpikes.length} 个爆量信号`);
    } else {
      console.log('未检测到爆量信号');
    }
    
    // 发送通知（无论是否有爆量）
    await sendServerChanNotifications(allSpikes);
    
    // 检查是否需要发送心跳
    await checkHeartbeat();
    
    // 更新状态并保存
    state.binance.lastRun = Date.now();
    state.okx.lastRun = Date.now();
    await saveState();
    
  } catch (error) {
    console.error('监控出错:', error);
    await sendErrorNotification(error);
  }
  
  console.log(`监控完成，耗时: ${((new Date() - startTime)/1000).toFixed(2)}秒`);
}

// 辅助函数：延迟执行
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// 获取本次运行的所有USDT合约交易对
function getSymbolsForThisRun() {
  // 获取Binance所有USDT合约交易对
  const binanceSymbols = state.binance.allSymbols.filter(symbol => 
    symbol.endsWith('USDT')
  );
  
  // 获取OKX所有USDT合约交易对
  const okxSymbols = state.okx.allSymbols.filter(symbol => 
    symbol.includes('-USDT-') || symbol.endsWith('-USDT')
  );
  
  return [binanceSymbols, okxSymbols];
}

// 加载状态
async function loadState() {
  try {
    if (fs.existsSync(config.stateFile)) {
      const data = fs.readFileSync(config.stateFile, 'utf8');
      const savedState = JSON.parse(data);
      
      // 恢复状态
      state.binance = savedState.binance || state.binance;
      state.okx = savedState.okx || state.okx;
      state.stats = savedState.stats || state.stats;
      
      console.log('已加载之前的状态');
    }
  } catch (error) {
    console.error('加载状态失败:', error);
  }
}

// 保存状态
async function saveState() {
  try {
    fs.writeFileSync(config.stateFile, JSON.stringify(state, null, 2));
    console.log('状态已保存');
  } catch (error) {
    console.error('保存状态失败:', error);
  }
}

// 检查是否需要刷新交易对列表
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

// 获取Binance交易对
async function getBinanceSymbols() {
  try {
    const response = await axios.get(config.binance.exchangeInfo);
    return response.data.symbols
      .filter(s => 
        s.contractType === 'PERPETUAL' && 
        s.status === 'TRADING' &&
        s.symbol.endsWith('USDT')
      )
      .map(s => s.symbol);
  } catch (error) {
    console.error('获取Binance交易对失败:', error.message);
    return [];
  }
}

// 获取OKX交易对
async function getOKXSymbols() {
  try {
    const response = await axios.get(config.okx.instruments);
    return response.data.data
      .filter(i => 
        i.instType === 'SWAP' && 
        i.state === 'live' &&
        (i.instId.includes('-USDT-') || i.instId.endsWith('-USDT'))
      )
      .map(i => i.instId);
  } catch (error) {
    console.error('获取OKX交易对失败:', error.message);
    return [];
  }
}

// 检测单个交易所的多个交易对（带请求限制）
async function checkExchange(exchange, symbols) {
  const spikes = [];
  
  // 限制并发请求数量
  const batchSize = 5; // 每次最多同时请求5个交易对的K线数据
  for (let i = 0; i < symbols.length; i += batchSize) {
    const batch = symbols.slice(i, i + batchSize);
    
    // 并行获取K线数据
    const batchResults = await Promise.all(batch.map(async symbol => {
      try {
        // 获取1小时和4小时数据
        const [hourData, fourHourData] = await Promise.all([
          getKlineData(exchange, symbol, '1h', 21),
          getKlineData(exchange, symbol, '4h', 21)
        ]);
        
        // 获取日线数据用于成交额过滤
        const dailyData = await getDailyKline(exchange, symbol);
        if (dailyData && dailyData.quoteVolume < config.minQuoteVolume) {
          return null; // 跳过成交额太小的交易对
        }
        
        const symbolSpikes = [];
        
        // 检测1小时爆量
        const hourSpike = detectSpike(hourData, config.hourSpikeThreshold, 'hour');
        if (hourSpike) {
          hourSpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
          symbolSpikes.push({...hourSpike, exchange, symbol});
        }
        
        // 检测1小时均线爆量
        const hourMASpike = detectMASpike(hourData, config.hourMaThreshold, 'hour-ma');
        if (hourMASpike) {
          hourMASpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
          symbolSpikes.push({...hourMASpike, exchange, symbol});
        }
        
        // 检测4小时爆量
        const fourHourSpike = detectSpike(fourHourData, config.fourHourSpikeThreshold, 'fourhour');
        if (fourHourSpike) {
          fourHourSpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
          symbolSpikes.push({...fourHourSpike, exchange, symbol});
        }
        
        // 检测4小时均线爆量
        const fourHourMASpike = detectMASpike(fourHourData, config.fourHourMaThreshold, 'fourhour-ma');
        if (fourHourMASpike) {
          fourHourMASpike.dailyQuoteVolume = dailyData?.quoteVolume || 0;
          symbolSpikes.push({...fourHourMASpike, exchange, symbol});
        }
        
        return symbolSpikes;
      } catch (error) {
        console.error(`${exchange} ${symbol} 检测失败:`, error.message);
        return null;
      }
    }));
    
    // 将结果加入spikes数组
    batchResults.forEach(result => {
      if (result && result.length > 0) {
        spikes.push(...result);
      }
    });
    
    // 批次间延迟
    if (i + batchSize < symbols.length) {
      await sleep(500);
    }
  }
  
  return spikes;
}

// 获取K线数据
async function getKlineData(exchange, symbol, interval, limit) {
  let url;
  
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
        `${config.okx.dailyKline}&instId=${symbol}&limit=1`
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

// 发送无交易对可检测的通知
async function sendNoTradingPairsNotification() {
  const message = `## 爆量监控系统通知\n\n` +
                 `**检测时间**: ${new Date().toLocaleString()}\n\n` +
                 `⚠️ 没有可检测的交易对\n\n` +
                 `请检查交易所API是否正常`;
  
  try {
    await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
      title: `爆量监控 - 无交易对可检测`,
      desp: message
    });
    console.log('无交易对通知发送成功');
  } catch (error) {
    console.error('无交易对通知发送失败:', error.message);
  }
}

// 发送错误通知
async function sendErrorNotification(error) {
  const message = `## 爆量监控系统错误\n\n` +
                 `**发生时间**: ${new Date().toLocaleString()}\n\n` +
                 `**错误信息**:\n` +
                 `\`\`\`\n${error.message}\n\`\`\`\n\n` +
                 `**堆栈跟踪**:\n` +
                 `\`\`\`\n${error.stack}\n\`\`\``;
  
  try {
    await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
      title: `爆量监控 - 系统错误`,
      desp: message
    });
    console.log('错误通知发送成功');
  } catch (err) {
    console.error('错误通知发送失败:', err.message);
  }
}

// 检查并发送心跳
async function checkHeartbeat() {
  // 每隔6小时发送一次心跳
  if (Date.now() - state.stats.lastHeartbeat > config.heartbeatInterval) {
    try {
      await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
        title: `爆量监控系统运行正常`,
        desp: `## 系统心跳检测\n\n` +
              `**最后运行时间**: ${new Date().toLocaleString()}\n\n` +
              `**统计信息**:\n` +
              `- 总运行次数: ${state.stats.totalRuns}\n` +
              `- 累计检测到爆量: ${state.stats.spikesDetected}次\n` +
              `- Binance交易对: ${state.binance.allSymbols.length}个\n` +
              `- OKX交易对: ${state.okx.allSymbols.length}个`
      });
      state.stats.lastHeartbeat = Date.now();
      console.log('心跳通知发送成功');
    } catch (error) {
      console.error('心跳通知发送失败:', error.message);
    }
  }
}

// 发送Server酱通知
async function sendServerChanNotifications(spikes) {
  // 按交易所分组
  const grouped = spikes.reduce((acc, spike) => {
    if (!acc[spike.exchange]) acc[spike.exchange] = [];
    acc[spike.exchange].push(spike);
    return acc;
  }, {
    binance: [],
    okx: []
  });
  
  // 为每个交易所发送通知
  for (const [exchange, exchangeSpikes] of Object.entries(grouped)) {
    const exchangeName = exchange === 'binance' ? 'Binance' : 'OKX';
    const symbolsChecked = exchange === 'binance' 
      ? getSymbolsForThisRun()[0].length 
      : getSymbolsForThisRun()[1].length;
    
    let message;
    if (exchangeSpikes.length > 0) {
      // 构造爆量警报消息
      message = `## ${exchangeName} 爆量警报汇总\n\n`;
      message += `**检测时间**: ${new Date().toLocaleString()}\n\n`;
      message += `**检测统计**:\n`;
      message += `- 已检测交易对: ${state[exchange].allSymbols.length}个\n`;
      message += `- 本次检测交易对: ${symbolsChecked}个\n\n`;
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
    } else {
      // 构造无爆量通知消息
      message = `## ${exchangeName} 爆量监控报告\n\n`;
      message += `**检测时间**: ${new Date().toLocaleString()}\n\n`;
      message += `**检测统计**:\n`;
      message += `- 已检测交易对: ${state[exchange].allSymbols.length}个\n`;
      message += `- 本次检测交易对: ${symbolsChecked}个\n\n`;
      message += "✅ 本次检测未发现爆量信号\n\n";
      message += `**系统状态**:\n`;
      message += `- 总运行次数: ${state.stats.totalRuns}\n`;
      message += `- 累计检测到爆量: ${state.stats.spikesDetected}次\n`;
    }
    
    try {
      // 只有当有爆量时才发送通知
  if (exchangeSpikes.length > 0) {
      await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
        title: `${exchangeName} 爆量监控 - ${exchangeSpikes.length > 0 ? '发现' + exchangeSpikes.length + '个爆量' : '未发现爆量'}`,
        desp: message
      });
  }
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
    return `$${(volume / 1000000000).toFixed(2)}M`;
  }
  return `$${Math.round(volume).toLocaleString()}`;
}

// 生成报告链接
function getReportLink(spikes) {
  if (!spikes || spikes.length === 0) {
    return 'https://www.tradingview.com/markets/cryptocurrencies/prices-all/';
  }
  
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
