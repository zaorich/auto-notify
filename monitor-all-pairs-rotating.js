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
  maxRequestsPerBatch: 10,  // 每次最多请求10个交易对的K线数据
  checkInterval: 60 * 60 * 1000 // 调整为每小时检测一次
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
      console.log(`检测Binance交易对批次: ${batch.join(', ')}`);
      const spikes = await checkExchange('binance', batch);
      allSpikes.push(...spikes);
      
      // 批次间延迟，避免请求过于频繁
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // 分批检测OKX交易对
    for (let i = 0; i < okxSymbols.length; i += config.maxRequestsPerBatch) {
      const batch = okxSymbols.slice(i, i + config.maxRequestsPerBatch);
      console.log(`检测OKX交易对批次: ${batch.join(', ')}`);
      const spikes = await checkExchange('okx', batch);
      allSpikes.push(...spikes);
      
      // 批次间延迟
      await new Promise(resolve => setTimeout(resolve, 1000));
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
  
  console.log(`监控完成，耗时: ${(new Date() - startTime)/1000}秒`);
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
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  return spikes;
}

// [保留原有的 loadState, saveState, shouldRefreshSymbols, getAllSymbols, getBinanceSymbols, 
// getOKXSymbols, getKlineData, detectSpike, detectMASpike, calculateMA, getDailyKline, 
// sendNoTradingPairsNotification, sendErrorNotification, checkHeartbeat, 
// sendServerChanNotifications, formatVolume, getReportLink 函数]

// 立即执行监控
monitor();
