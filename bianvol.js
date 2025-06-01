const axios = require('axios');
const fs = require('fs');

// 配置项
const config = {
  thresholds: {
    hourSpike: 8,
    fourHourSpike: 5,
    hourMA: 8,
    fourHourMA: 5,
    minVolume: 50000000 // 50M USD
  },
  binanceAPI: {
    baseURL: 'https://fapi.binance.com',
    endpoints: {
      exchangeInfo: '/fapi/v1/exchangeInfo',
      klines: '/fapi/v1/klines'
    }
  },
  notification: {
    serverChanKey: process.env.SERVER_CHAN_SCKEY || 'YOUR_KEY'
  }
};

// 状态管理
const state = {
  symbols: [],
  stats: {
    totalRuns: 0,
    spikesDetected: 0
  }
};

class BianvolMonitor {
  constructor() {
    this.api = axios.create({
      baseURL: config.binanceAPI.baseURL,
      timeout: 10000
    });
  }

  async init() {
    await this.loadSymbols();
    console.log(`✅ 初始化完成，共加载 ${state.symbols.length} 个交易对`);
  }

  async loadSymbols() {
    try {
      const res = await this.api.get(config.binanceAPI.endpoints.exchangeInfo);
      state.symbols = res.data.symbols
        .filter(s => s.contractType === 'PERPETUAL' && s.status === 'TRADING')
        .map(s => s.symbol);
    } catch (error) {
      console.error('❌ 加载交易对失败:', error.message);
      throw error;
    }
  }

  async run() {
    state.stats.totalRuns++;
    const symbol = this.selectRandomSymbol();
    console.log(`🔍 正在检测 ${symbol}`);

    try {
      const spikes = await this.checkSymbol(symbol);
      if (spikes.length > 0) {
        await this.sendAlert(symbol, spikes);
      }
    } catch (error) {
      console.error(`❌ ${symbol} 检测失败:`, error.message);
      await this.sendError(error);
    }
  }

  async checkSymbol(symbol) {
    const [hourly, fourHourly] = await Promise.all([
      this.getKlines(symbol, '1h', 21),
      this.getKlines(symbol, '4h', 21)
    ]);

    const spikes = [];
    spikes.push(...this.detectSpikes(hourly, 'hour'));
    spikes.push(...this.detectMASpikes(hourly, 'hour-ma'));
    spikes.push(...this.detectSpikes(fourHourly, 'fourhour'));
    spikes.push(...this.detectMASpikes(fourHourly, 'fourhour-ma'));

    return spikes.filter(Boolean);
  }

  async getKlines(symbol, interval, limit) {
    const res = await this.api.get(config.binanceAPI.endpoints.klines, {
      params: { symbol, interval, limit }
    });
    return res.data.map(k => ({
      time: k[0],
      open: parseFloat(k[1]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
      quoteVolume: parseFloat(k[7])
    }));
  }

  detectSpikes(data, type) {
    if (!data || data.length < 2) return [];
    const current = data[data.length - 1];
    const prev = data[data.length - 2];
    const ratio = current.volume / prev.volume;
    
    return ratio >= config.thresholds[type] ? [{
      type,
      price: current.close,
      volume: current.volume,
      ratio: ratio.toFixed(1)
    }] : [];
  }

  detectMASpikes(data, type) {
    if (!data || data.length < 21) return [];
    const volumes = data.map(d => d.volume);
    const current = volumes[volumes.length - 1];
    const ma = volumes.slice(-20).reduce((a, b) => a + b, 0) / 20;
    const ratio = current / ma;
    
    return ratio >= config.thresholds[type] ? [{
      type,
      price: data[data.length - 1].close,
      volume: current,
      ratio: ratio.toFixed(1)
    }] : [];
  }

  async sendAlert(symbol, spikes) {
    const title = `🚨 ${symbol} 爆量警报`;
    let message = `## 检测到 ${spikes.length} 个爆量信号\n\n`;
    
    spikes.forEach(spike => {
      message += `- 类型: ${this.getTypeName(spike.type)}\n`;
      message += `- 价格: ${spike.price.toFixed(4)}\n`;
      message += `- 成交量: ${this.formatNumber(spike.volume)}\n`;
      message += `- 倍数: ${spike.ratio}x\n\n`;
    });

    await this.sendNotification(title, message);
  }

  async sendError(error) {
    await this.sendNotification(
      '⚠️ 监控系统错误',
      `错误信息:\n\`\`\`\n${error.stack}\n\`\`\``
    );
  }

  async sendNotification(title, message) {
    if (!config.notification.serverChanKey) return;
    
    try {
      await axios.post(
        `https://sctapi.ftqq.com/${config.notification.serverChanKey}.send`,
        { title, desp: message }
      );
    } catch (error) {
      console.error('通知发送失败:', error.message);
    }
  }

  // 辅助方法
  selectRandomSymbol() {
    return state.symbols[Math.floor(Math.random() * state.symbols.length)];
  }

  getTypeName(type) {
    const map = {
      'hour': '1小时突增',
      'hour-ma': '1小时均线突破',
      'fourhour': '4小时突增',
      'fourhour-ma': '4小时均线突破'
    };
    return map[type] || type;
  }

  formatNumber(num) {
    return num.toLocaleString('en-US');
  }
}

// 执行主程序
(async () => {
  try {
    const monitor = new BianvolMonitor();
    await monitor.init();
    await monitor.run();
    process.exit(0);
  } catch (error) {
    console.error('监控系统异常终止:', error);
    process.exit(1);
  }
})();
