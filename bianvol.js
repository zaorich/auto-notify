const axios = require('axios');
const fs = require('fs');

// ==================== 配置部分 ====================
const config = {
  // 爆量检测阈值
  thresholds: {
    hourSpike: process.env.HOUR_SPIKE_THRESHOLD || 8,      // 1小时成交量突增阈值
    fourHourSpike: process.env.FOUR_HOUR_SPIKE_THRESHOLD || 5, // 4小时成交量突增阈值
    hourMA: process.env.HOUR_MA_THRESHOLD || 8,            // 1小时均线突破阈值
    fourHourMA: process.env.FOUR_HOUR_MA_THRESHOLD || 5,    // 4小时均线突破阈值
    minVolume: process.env.MIN_QUOTE_VOLUME || 50000000     // 最小成交额(USD)
  },

  // API端点
  binanceAPI: {
    baseURL: 'https://fapi.binance.com',
    endpoints: {
      exchangeInfo: '/fapi/v1/exchangeInfo',
      klines: '/fapi/v1/klines'
    }
  },

  // 通知系统
  notification: {
    serverChan: {
      enabled: true,
      endpoint: 'https://sctapi.ftqq.com',
      key: process.env.SERVER_CHAN_SCKEY || 'YOUR_DEFAULT_KEY'
    }
  },

  // 系统设置
  settings: {
    checkInterval: 3600000,    // 1小时检测一次
    maxRetries: 3,            // API最大重试次数
    stateFile: 'state.json'   // 状态存储文件
  }
};

// ==================== 状态管理 ====================
const state = {
  initialized: false,
  symbols: [],
  stats: {
    totalChecks: 0,
    lastCheck: null,
    spikesDetected: 0
  }
};

// ==================== 核心功能 ====================
class BinanceVolMonitor {
  constructor() {
    this.axios = axios.create({
      baseURL: config.binanceAPI.baseURL,
      timeout: 10000
    });
  }

  async initialize() {
    console.log('🟢 初始化币安爆量监控系统');
    await this.loadState();
    
    if (!state.initialized || this.shouldRefreshSymbols()) {
      await this.fetchSymbols();
      state.initialized = true;
    }

    console.log(`📊 已加载交易对数量: ${state.symbols.length}`);
  }

  async loadState() {
    try {
      if (fs.existsSync(config.settings.stateFile)) {
        const data = JSON.parse(fs.readFileSync(config.settings.stateFile));
        Object.assign(state, data);
        console.log('🔄 已恢复之前的状态');
      }
    } catch (error) {
      console.error('❌ 状态加载失败:', error.message);
    }
  }

  async saveState() {
    try {
      fs.writeFileSync(config.settings.stateFile, JSON.stringify(state, null, 2));
      console.log('💾 状态保存成功');
    } catch (error) {
      console.error('❌ 状态保存失败:', error.message);
    }
  }

  shouldRefreshSymbols() {
    return (
      state.symbols.length === 0 || 
      Date.now() - (state.stats.lastCheck || 0) > 86400000 // 24小时刷新一次
    );
  }

  async fetchSymbols() {
    console.log('🔍 获取币安交易对列表...');
    try {
      const response = await this.axios.get(config.binanceAPI.endpoints.exchangeInfo);
      state.symbols = response.data.symbols
        .filter(s => s.contractType === 'PERPETUAL' && s.status === 'TRADING' && s.symbol.endsWith('USDT'))
        .map(s => s.symbol);
      
      console.log(`✅ 获取到 ${state.symbols.length} 个USDT永续合约`);
    } catch (error) {
      console.error('❌ 获取交易对失败:', this.formatError(error));
      throw error;
    }
  }

  async runCheck() {
    state.stats.totalChecks++;
    state.stats.lastCheck = Date.now();
    
    const symbol = this.selectRandomSymbol();
    console.log(`🎯 本次检测交易对: ${symbol}`);

    try {
      const spikes = await this.checkSymbol(symbol);
      
      if (spikes.length > 0) {
        state.stats.spikesDetected += spikes.length;
        await this.sendAlert(symbol, spikes);
      } else {
        await this.sendHeartbeat(symbol);
      }
    } catch (error) {
      console.error(`❌ ${symbol} 检测失败:`, this.formatError(error));
      await this.sendErrorNotification(error);
    } finally {
      await this.saveState();
    }
  }

  selectRandomSymbol() {
    return state.symbols[Math.floor(Math.random() * state.symbols.length)];
  }

  async checkSymbol(symbol) {
    const [hourly, fourHourly] = await Promise.all([
      this.getKlines(symbol, '1h', 21),
      this.getKlines(symbol, '4h', 21)
    ]);

    const dailyVolume = await this.getDailyVolume(symbol);
    if (dailyVolume < config.thresholds.minVolume) {
      console.log(`⏭️ ${symbol} 24小时成交额不足 (${this.formatMoney(dailyVolume)})`);
      return [];
    }

    const spikes = [];
    
    // 1小时检测
    spikes.push(...this.detectSpikes(hourly, 'hour', config.thresholds.hourSpike));
    spikes.push(...this.detectMASpikes(hourly, 'hour-ma', config.thresholds.hourMA));
    
    // 4小时检测
    spikes.push(...this.detectSpikes(fourHourly, 'fourhour', config.thresholds.fourHourSpike));
    spikes.push(...this.detectMASpikes(fourHourly, 'fourhour-ma', config.thresholds.fourHourMA));

    return spikes.filter(Boolean);
  }

  async getKlines(symbol, interval, limit) {
    const params = {
      symbol,
      interval,
      limit
    };

    try {
      const response = await this.axios.get(config.binanceAPI.endpoints.klines, { params });
      return response.data.map(k => ({
        time: k[0],
        open: parseFloat(k[1]),
        high: parseFloat(k[2]),
        low: parseFloat(k[3]),
        close: parseFloat(k[4]),
        volume: parseFloat(k[5]),
        quoteVolume: parseFloat(k[7])
      }));
    } catch (error) {
      console.error(`❌ 获取 ${symbol} ${interval} K线失败:`, this.formatError(error));
      throw error;
    }
  }

  async getDailyVolume(symbol) {
    try {
      const klines = await this.getKlines(symbol, '1d', 1);
      return klines[0]?.quoteVolume || 0;
    } catch (error) {
      console.error(`❌ 获取 ${symbol} 日成交量失败:`, this.formatError(error));
      return 0;
    }
  }

  detectSpikes(data, type, threshold) {
    if (!data || data.length < 2) return [];
    
    const current = data[data.length - 1];
    const previous = data[data.length - 2];
    
    if (current.volume <= 0 || previous.volume <= 0) return [];
    
    const ratio = current.volume / previous.volume;
    
    return ratio >= threshold ? [{
      type,
      price: current.close,
      time: new Date(current.time),
      volume: current.volume,
      compareValue: previous.volume,
      ratio: ratio.toFixed(2)
    }] : [];
  }

  detectMASpikes(data, type, threshold) {
    if (!data || data.length < 21) return [];
    
    const volumes = data.map(d => d.volume);
    const current = volumes[volumes.length - 1];
    const ma = this.calculateMA(volumes, 20);
    
    if (current <= 0 || ma <= 0) return [];
    
    const ratio = current / ma;
    
    return ratio >= threshold ? [{
      type,
      price: data[data.length - 1].close,
      time: new Date(data[data.length - 1].time),
      volume: current,
      compareValue: ma,
      ratio: ratio.toFixed(2)
    }] : [];
  }

  calculateMA(data, period) {
    const sum = data.slice(-period).reduce((a, b) => a + b, 0);
    return sum / period;
  }

  // ==================== 通知系统 ====================
  async sendAlert(symbol, spikes) {
    if (!config.notification.serverChan.enabled) return;

    const title = `🚨 币安爆量警报 - ${symbol}`;
    let message = `## ${symbol} 检测到 ${spikes.length} 个爆量信号\n\n`;
    
    spikes.forEach(spike => {
      message += `### ${this.getSpikeTypeName(spike.type)}\n`;
      message += `- 📅 时间: ${spike.time.toLocaleString('zh-CN')}\n`;
      message += `- 💵 价格: ${spike.price.toFixed(4)} USDT\n`;
      message += `- 📊 成交量: ${this.formatNumber(spike.volume)}\n`;
      message += `- 🔍 倍数: ${spike.ratio}x (阈值: ${this.getThresholdForType(spike.type)})\n\n`;
    });

    message += `[查看实时图表](https://www.tradingview.com/chart/?symbol=BINANCE:${symbol})`;
    
    await this.sendNotification(title, message);
  }

  async sendHeartbeat(symbol) {
    if (!config.notification.serverChan.enabled) return;
    
    const title = `💓 币安监控心跳 - ${symbol}`;
    const message = `## 系统运行正常\n\n`
      + `**最后检测**: ${new Date().toLocaleString('zh-CN')}\n`
      + `**检测交易对**: ${symbol}\n`
      + `**累计检测次数**: ${state.stats.totalChecks}\n`
      + `**累计爆量信号**: ${state.stats.spikesDetected}\n\n`
      + `[查看交易对](${this.getSymbolLink(symbol)})`;
    
    await this.sendNotification(title, message);
  }

  async sendErrorNotification(error) {
    if (!config.notification.serverChan.enabled) return;
    
    const title = '⚠️ 币安监控系统错误';
    const message = `## 系统发生错误\n\n`
      + `**时间**: ${new Date().toLocaleString('zh-CN')}\n`
      + `**错误信息**:\n\`\`\`\n${error.message}\n\`\`\`\n`
      + `**堆栈追踪**:\n\`\`\`\n${error.stack}\n\`\`\``;
    
    await this.sendNotification(title, message);
  }

  async sendNotification(title, message) {
    try {
      await axios.post(`${config.notification.serverChan.endpoint}/${config.notification.serverChan.key}.send`, {
        title,
        desp: message
      });
      console.log('📢 通知发送成功');
    } catch (error) {
      console.error('❌ 通知发送失败:', this.formatError(error));
    }
  }

  // ==================== 工具函数 ====================
  formatError(error) {
    return {
      message: error.message,
      code: error.code,
      response: error.response?.data
    };
  }

  formatNumber(num) {
    return num.toLocaleString('en-US');
  }

  formatMoney(amount) {
    if (amount >= 1e9) return `$${(amount / 1e9).toFixed(2)}B`;
    if (amount >= 1e6) return `$${(amount / 1e6).toFixed(2)}M`;
    return `$${this.formatNumber(amount)}`;
  }

  getSpikeTypeName(type) {
    const types = {
      'hour': '1小时成交量突增',
      'hour-ma': '1小时均线突破',
      'fourhour': '4小时成交量突增',
      'fourhour-ma': '4小时均线突破'
    };
    return types[type] || type;
  }

  getThresholdForType(type) {
    return {
      'hour': config.thresholds.hourSpike,
      'hour-ma': config.thresholds.hourMA,
      'fourhour': config.thresholds.fourHourSpike,
      'fourhour-ma': config.thresholds.fourHourMA
    }[type];
  }

  getSymbolLink(symbol) {
    return `https://www.binance.com/en/futures/${symbol}`;
  }
}

// ==================== 执行入口 ====================
(async () => {
  try {
    const monitor = new BinanceVolMonitor();
    await monitor.initialize();
    await monitor.runCheck();
    
    console.log('✅ 监控任务完成');
    process.exit(0);
  } catch (error) {
    console.error('❌ 监控系统致命错误:', error);
    process.exit(1);
  }
})();
