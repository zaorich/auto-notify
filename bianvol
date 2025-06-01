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
  minQuoteVolume: 50000000,  // 最小成交额过滤(5千万美元)
  
  // 交易所API
  binance: {
    exchangeInfo: 'https://fapi.binance.com/fapi/v1/exchangeInfo',
    klines: 'https://fapi.binance.com/fapi/v1/klines',
    dailyKline: 'https://fapi.binance.com/fapi/v1/klines?interval=1d'
  },
  
  // Server酱通知配置
  serverChan: {
    sckey: 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e'
  },
  
  // 系统配置
  checkInterval: 60 * 60 * 1000, // 每小时检测一次
  heartbeatInterval: 6 * 60 * 60 * 1000 // 心跳间隔6小时
};

// 状态管理
let state = {
  binance: {
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
  console.log('开始币安交易对爆量监控检测...');
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
    const binanceSymbols = getSymbolsForThisRun();
    console.log(`币安USDT合约总数: ${binanceSymbols.length}个`);
    
    if (binanceSymbols.length === 0) {
      console.log('没有可检测的交易对');
      await sendNoTradingPairsNotification();
      return;
    }
    
    // 随机选择一个交易对
    const randomSymbol = binanceSymbols[Math.floor(Math.random() * binanceSymbols.length)];
    console.log(`随机选择交易对: ${randomSymbol}`);
    
    // 检测选中的交易对
    const spikes = await checkSingleSymbol('binance', randomSymbol);
    
    if (spikes.length > 0) {
      console.log(`检测到 ${spikes.length} 个爆量信号`);
      state.stats.spikesDetected += spikes.length;
    } else {
      console.log('未检测到爆量信号');
    }
    
    // 发送通知（无论是否有爆量）
    await sendSingleSymbolNotification(randomSymbol, spikes);
    
    // 检查是否需要发送心跳
    await checkHeartbeat();
    
    // 更新状态并保存
    state.binance.lastRun = Date.now();
    await saveState();
    
  } catch (error) {
    console.error('监控出错:', error);
    await sendErrorNotification(error);
  }
  
  console.log(`监控完成，耗时: ${((new Date() - startTime)/1000).toFixed(2)}秒`);
}

// 检测单个交易对
async function checkSingleSymbol(exchange, symbol) {
  try {
    // 获取1小时和4小时数据
    const [hourData, fourHourData] = await Promise.all([
      getKlineData(exchange, symbol, '1h', 21),
      getKlineData(exchange, symbol, '4h', 21)
    ]);
    
    // 获取日线数据用于成交额过滤
    const dailyData = await getDailyKline(exchange, symbol);
    if (dailyData && dailyData.quoteVolume < config.minQuoteVolume) {
      console.log(`${symbol} 成交额不足，跳过检测`);
      return [];
    }
    
    const spikes = [];
    
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
    
    return spikes;
  } catch (error) {
    console.error(`${exchange} ${symbol} 检测失败:`, error.message);
    return [];
  }
}

// 发送单个交易对的通知
async function sendSingleSymbolNotification(symbol, spikes) {
  let message = `## 币安随机检测报告\n\n`;
  message += `**检测时间**: ${new Date().toLocaleString()}\n\n`;
  message += `**检测交易对**: ${symbol}\n\n`;
  
  if (spikes.length > 0) {
    message += `### 发现 ${spikes.length} 个爆量信号\n\n`;
    message += "| 类型 | 价格 | 成交量 | 倍数 | 24h成交额 |\n";
    message += "|------|------|--------|------|----------|\n";
    
    spikes.forEach(spike => {
      const typeMap = {
        'hour': '1小时',
        'hour-ma': '1小时MA',
        'fourhour': '4小时',
        'fourhour-ma': '4小时MA'
      };
      
      message += `| ${typeMap[spike.type]} | ${spike.price.toFixed(4)} `;
      message += `| ${spike.currentVolume.toLocaleString()} | ${spike.ratio.toFixed(1)}x `;
      message += `| ${formatVolume(spike.dailyQuoteVolume)} |\n`;
    });
  } else {
    message += "✅ 未发现爆量信号\n\n";
  }
  
  // 添加K线图表链接
  message += `\n[查看K线图表](https://www.tradingview.com/chart/?symbol=BINANCE:${symbol})`;
  
  try {
    await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
      title: `币安检测: ${symbol} ${spikes.length > 0 ? '发现爆量' : '正常'}`,
      desp: message
    });
    console.log('微信通知发送成功');
  } catch (error) {
    console.error('微信通知发送失败:', error.message);
  }
}

// 辅助函数：延迟执行
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// 获取本次运行的所有USDT合约交易对
function getSymbolsForThisRun() {
  return state.binance.allSymbols.filter(symbol => 
    symbol.endsWith('USDT')
  );
}

// 加载状态
async function loadState() {
  try {
    if (fs.existsSync(config.stateFile)) {
      const data = fs.readFileSync(config.stateFile, 'utf8');
      const savedState = JSON.parse(data);
      
      // 恢复状态
      state.binance = savedState.binance || state.binance;
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
  if (state.binance.allSymbols.length === 0) {
    return true;
  }
  
  const now = Date.now();
  const twentyFourHours = 24 * 60 * 60 * 1000;
  return (now - state.binance.lastRun) > twentyFourHours;
}

// 获取所有交易对
async function getAllSymbols() {
  state.binance.allSymbols = await getBinanceSymbols();
  state.binance.lastRun = Date.now();
}

// 获取Binance交易对
async function getBinanceSymbols() {
  try {
    const response = await axios.get(config.binance.exchangeInfo);
    const symbols = response.data.symbols
      .filter(s => 
        s.contractType === 'PERPETUAL' && 
        s.status === 'TRADING' &&
        s.symbol.endsWith('USDT')
      )
      .map(s => s.symbol);
    
    console.log(`获取到 ${symbols.length} 个币安USDT合约`);
    return symbols;
  } catch (error) {
    console.error('获取币安交易对失败:', {
      message: error.message,
      response: error.response?.data,
      stack: error.stack
    });
    return [];
  }
}

// 获取K线数据
async function getKlineData(exchange, symbol, interval, limit) {
  const url = `${config.binance.klines}?symbol=${symbol}&interval=${interval}&limit=${limit}`;
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
}

// 检测普通爆量
function detectSpike(data, threshold, type) {
  if (!data || data.length < 2) return null;
  
  const current = data[data.length - 1];
  const previous = data[data.length - 2];
  
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
  if (!data || data.length < 21) return null;
  
  const volumes = data.map(d => d.volume);
  const current = volumes[volumes.length - 1];
  const maValues = calculateMA(volumes, 20);
  const ma = maValues[maValues.length - 1];
  
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
  } catch (error) {
    console.error(`获取${exchange}日线数据失败:`, error.message);
    return null;
  }
}

// 发送无交易对可检测的通知
async function sendNoTradingPairsNotification() {
  const message = `## 币安爆量监控系统通知\n\n` +
               `**检测时间**: ${new Date().toLocaleString()}\n\n` +
               `⚠️ 没有可检测的交易对\n\n` +
               `请检查API是否正常`;
  
  try {
    await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
      title: `币安监控 - 无交易对可检测`,
      desp: message
    });
    console.log('无交易对通知发送成功');
  } catch (error) {
    console.error('无交易对通知发送失败:', error.message);
  }
}

// 发送错误通知
async function sendErrorNotification(error) {
  const message = `## 币安爆量监控系统错误\n\n` +
               `**发生时间**: ${new Date().toLocaleString()}\n\n` +
               `**错误信息**:\n` +
               `\`\`\`\n${error.message}\n\`\`\`\n\n` +
               `**堆栈跟踪**:\n` +
               `\`\`\`\n${error.stack}\n\`\`\``;
  
  try {
    await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
      title: `币安监控 - 系统错误`,
      desp: message
    });
    console.log('错误通知发送成功');
  } catch (err) {
    console.error('错误通知发送失败:', err.message);
  }
}

// 检查并发送心跳
async function checkHeartbeat() {
  if (Date.now() - state.stats.lastHeartbeat > config.heartbeatInterval) {
    try {
      await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
        title: `币安监控系统运行正常`,
        desp: `## 系统心跳检测\n\n` +
              `**最后运行时间**: ${new Date().toLocaleString()}\n\n` +
              `**统计信息**:\n` +
              `- 总运行次数: ${state.stats.totalRuns}\n` +
              `- 累计检测到爆量: ${state.stats.spikesDetected}次\n` +
              `- 币安交易对: ${state.binance.allSymbols.length}个`
      });
      state.stats.lastHeartbeat = Date.now();
      console.log('心跳通知发送成功');
    } catch (error) {
      console.error('心跳通知发送失败:', error.message);
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

// 立即执行监控
monitor();
