const axios = require('axios');
const fs = require('fs');
const path = require('path');

// 从环境变量读取配置
const config = {
  // 爆量阈值
  hourSpikeThreshold: process.env.HOUR_SPIKE_THRESHOLD || 8,
  fourHourSpikeThreshold: process.env.FOUR_HOUR_SPIKE_THRESHOLD || 5,
  hourMaThreshold: process.env.HOUR_MA_THRESHOLD || 8,
  fourHourMaThreshold: process.env.FOUR_HOUR_MA_THRESHOLD || 5,
  minQuoteVolume: process.env.MIN_QUOTE_VOLUME || 50000000, // 默认5千万美元
  
  // 币安API
  binance: {
    exchangeInfo: 'https://fapi.binance.com/fapi/v1/exchangeInfo',
    klines: 'https://fapi.binance.com/fapi/v1/klines',
    dailyKline: 'https://fapi.binance.com/fapi/v1/klines?interval=1d'
  },
  
  // Server酱通知
  serverChan: {
    sckey: process.env.SERVER_CHAN_SCKEY || 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e'
  },
  
  // 系统配置
  checkInterval: process.env.CHECK_INTERVAL || 60 * 60 * 1000,
  heartbeatInterval: 6 * 60 * 60 * 1000
};

// 状态管理
const state = {
  binance: {
    allSymbols: [],
    lastRun: 0
  },
  stats: {
    totalRuns: 0,
    lastRunTime: null,
    spikesDetected: 0,
    lastHeartbeat: 0
  },
  stateFile: 'monitor-state.json'
};

// 主监控函数
async function monitor() {
  console.log('▄︻デ══━ 币安爆量监控启动 ══━一═══');
  console.log(`🕒 启动时间: ${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}`);
  console.log(`🌐 运行环境: ${process.env.NODE_ENV || 'development'}`);
  
  try {
    await loadState();
    state.stats.totalRuns++;
    state.stats.lastRunTime = new Date();

    if (shouldRefreshSymbols()) {
      console.log('🔄 正在刷新交易对列表...');
      await getAllSymbols();
    }

    const binanceSymbols = getSymbolsForThisRun();
    console.log(`📊 可用交易对数量: ${binanceSymbols.length}个`);

    if (binanceSymbols.length === 0) {
      await sendNotification('监控异常', '没有可检测的交易对');
      return;
    }

    // 随机选择一个交易对
    const randomSymbol = binanceSymbols[Math.floor(Math.random() * binanceSymbols.length)];
    console.log(`🎯 本次检测交易对: ${randomSymbol}`);

    const spikes = await checkSingleSymbol('binance', randomSymbol);
    
    if (spikes.length > 0) {
      console.log(`🚨 发现 ${spikes.length} 个爆量信号`);
      state.stats.spikesDetected += spikes.length;
      await sendSpikeNotification(randomSymbol, spikes);
    } else {
      console.log('✅ 未发现爆量信号');
      await sendHeartbeat(randomSymbol); // 发送正常心跳
    }

    await checkHeartbeat();
    state.binance.lastRun = Date.now();
    await saveState();

  } catch (error) {
    console.error('💥 监控出错:', error.stack);
    await sendErrorNotification(error);
  } finally {
    console.log(`⏱️ 本次运行耗时: ${((Date.now() - state.stats.lastRunTime) / 1000).toFixed(2)}秒`);
  }
}

// 检测单个交易对
async function checkSingleSymbol(exchange, symbol) {
  try {
    const [hourData, fourHourData] = await Promise.all([
      getKlineData(exchange, symbol, '1h', 21),
      getKlineData(exchange, symbol, '4h', 21)
    ]);

    const dailyData = await getDailyKline(exchange, symbol);
    if (dailyData && dailyData.quoteVolume < config.minQuoteVolume) {
      console.log(`⏭️ ${symbol} 24小时成交额不足，跳过检测`);
      return [];
    }

    const spikes = [];
    
    // 检测逻辑保持不变...
    // [原有检测逻辑代码...]

    return spikes;
  } catch (error) {
    console.error(`❌ ${symbol} 检测失败:`, error.message);
    return [];
  }
}

// 增强版通知函数
async function sendSpikeNotification(symbol, spikes) {
  const title = `🚨 ${symbol} 发现爆量信号`;
  let message = `## ${symbol} 爆量警报\n\n`;
  message += `**检测时间**: ${new Date().toLocaleString('zh-CN')}\n\n`;
  
  spikes.forEach(spike => {
    message += `### ${getSpikeTypeCN(spike.type)}\n`;
    message += `- 📈 价格: ${spike.price.toFixed(4)}\n`;
    message += `- 📊 成交量: ${formatNumber(spike.currentVolume)}\n`;
    message += `- 🔍 倍数: ${spike.ratio.toFixed(1)}x\n`;
    message += `- 💰 24h成交额: ${formatVolume(spike.dailyQuoteVolume)}\n\n`;
  });

  message += `[查看实时图表](https://www.tradingview.com/chart/?symbol=BINANCE:${symbol})`;
  
  await sendNotification(title, message);
}

// 辅助函数
function getSpikeTypeCN(type) {
  const typeMap = {
    'hour': '1小时成交量突增',
    'hour-ma': '1小时均线突破',
    'fourhour': '4小时成交量突增', 
    'fourhour-ma': '4小时均线突破'
  };
  return typeMap[type] || type;
}

function formatNumber(num) {
  return num.toLocaleString('en-US');
}

function formatVolume(volume) {
  if (!volume) return '未知';
  if (volume >= 1e9) return `$${(volume / 1e9).toFixed(2)}B`;
  if (volume >= 1e6) return `$${(volume / 1e6).toFixed(2)}M`;
  return `$${formatNumber(volume)}`;
}

// [保留其他原有函数...]

// 立即执行
monitor();

// 如果是GitHub Actions环境，设置正确的退出码
process.on('exit', (code) => {
  console.log(`♻️ 进程退出码: ${code}`);
});
