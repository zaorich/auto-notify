const axios = require('axios');

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
    sckey: 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e' // 你的Server酱KEY
  },
  
  // 系统配置
  maxPairsPerRun: 50,      // 每次运行最多检测的交易对数(防止API限制)
  checkInterval: 15 * 60 * 1000 // 15分钟检测一次
};

// 主监控函数
async function monitor() {
  console.log('开始全交易对爆量监控检测...');
  const startTime = new Date();
  
  try {
    // 获取所有交易对
    const [binanceSymbols, okxSymbols] = await Promise.all([
      getAllBinanceSymbols(),
      getAllOKXSymbols()
    ]);
    
    console.log(`获取到 ${binanceSymbols.length} 个Binance交易对和 ${okxSymbols.length} 个OKX交易对`);
    
    // 限制每次检测的交易对数量
    const limitedBinanceSymbols = binanceSymbols.slice(0, config.maxPairsPerRun);
    const limitedOKXSymbols = okxSymbols.slice(0, config.maxPairsPerRun);
    
    // 并行检测两个交易所
    const [binanceSpikes, okxSpikes] = await Promise.all([
      checkExchange('binance', limitedBinanceSymbols),
      checkExchange('okx', limitedOKXSymbols)
    ]);
    
    const allSpikes = [...binanceSpikes, ...okxSpikes];
    
    if (allSpikes.length > 0) {
      console.log(`检测到 ${allSpikes.length} 个爆量信号`);
      await sendServerChanNotifications(allSpikes);
    } else {
      console.log('未检测到爆量信号');
    }
    
  } catch (error) {
    console.error('监控出错:', error);
  }
  
  console.log(`监控完成，耗时: ${(new Date() - startTime)/1000}秒`);
}

// 获取Binance所有交易对
async function getAllBinanceSymbols() {
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

// 获取OKX所有交易对
async function getAllOKXSymbols() {
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

// [保留之前的 getKlineData, detectSpike, detectMASpike, calculateMA, getDailyKline 函数...]

// 发送Server酱通知(增强版)
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
  if (volume >= 1000000000) {
    return `$${(volume / 1000000000).toFixed(2)}B`;
  } else if (volume >= 1000000) {
    return `$${(volume / 1000000).toFixed(2)}M`;
  }
  return `$${volume.toLocaleString()}`;
}

// 生成报告链接(示例)
function getReportLink(spikes) {
  // 这里可以替换为你的报告生成逻辑
  return `https://www.tradingview.com/markets/cryptocurrencies/prices-all/`;
}

// 立即执行监控
monitor();
