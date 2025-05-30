const axios = require('axios');

// 配置参数
const config = {
  // 爆量阈值
  hourSpikeThreshold: 8,    // 1小时成交量是前一根的8倍
  fourHourSpikeThreshold: 5, // 4小时成交量是前一根的5倍
  hourMaThreshold: 8,       // 1小时成交量是MA20的8倍
  fourHourMaThreshold: 5,   // 4小时成交量是MA20的5倍
  
  // 交易所API
  binance: {
    klines: 'https://fapi.binance.com/fapi/v1/klines',
    symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] // 监控的主要交易对
  },
  okx: {
    klines: 'https://www.okx.com/api/v5/market/candles',
    symbols: ['BTC-USD-SWAP', 'ETH-USD-SWAP', 'SOL-USD-SWAP']
  },
  
  // Server酱通知配置
  serverChan: {
    sckey: 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e' // 你的Server酱KEY
  }
};

// 主监控函数
async function monitor() {
  console.log('开始爆量监控检测...');
  const now = new Date();
  
  try {
    // 并行检测两个交易所
    const [binanceSpikes, okxSpikes] = await Promise.all([
      checkExchange('binance'),
      checkExchange('okx')
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
  
  console.log(`监控完成，耗时: ${(new Date() - now)/1000}秒`);
}

// [之前的 checkExchange, getKlineData, detectSpike, detectMASpike, calculateMA 函数保持不变...]

// 发送Server酱通知
async function sendServerChanNotifications(spikes) {
  for (const spike of spikes) {
    const message = formatServerChanMessage(spike);
    
    try {
      await axios.post(`https://sctapi.ftqq.com/${config.serverChan.sckey}.send`, {
        title: `${spike.exchange.toUpperCase()} ${spike.symbol} 爆量警报`,
        desp: message
      });
      console.log('Server酱通知发送成功');
    } catch (error) {
      console.error('Server酱通知发送失败:', error.message);
    }
  }
}

// 格式化Server酱消息
function formatServerChanMessage(spike) {
  const exchangeName = spike.exchange === 'binance' ? 'Binance' : 'OKX';
  const timeStr = new Date(parseInt(spike.timestamp)).toLocaleString();
  const typeMap = {
    'hour': '1小时爆量',
    'hour-ma': '1小时均线爆量',
    'fourhour': '4小时爆量',
    'fourhour-ma': '4小时均线爆量'
  };
  
  return `## ${exchangeName} ${spike.symbol} 爆量警报
**类型**: ${typeMap[spike.type]}  
**价格**: ${spike.price}  
**时间**: ${timeStr}  
**当前成交量**: ${spike.currentVolume.toLocaleString()}  
**对比值**: ${spike.compareValue.toLocaleString()}  
**倍数**: ${spike.ratio.toFixed(1)}x  

[查看K线图](${getChartLink(spike)})`;
}

// 获取K线图链接
function getChartLink(spike) {
  if (spike.exchange === 'binance') {
    return `https://www.tradingview.com/chart/?symbol=BINANCE:${spike.symbol}`;
  } else {
    return `https://www.tradingview.com/chart/?symbol=OKX:${spike.symbol.replace('-', '')}`;
  }
}

// 立即执行监控
monitor();
