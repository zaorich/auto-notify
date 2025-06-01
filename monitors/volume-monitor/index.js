const path = require('path');
const { promisify } = require('util');
const fs = require('fs');
const writeFile = promisify(fs.writeFile);
const readFile = promisify(fs.readFile);
const axios = require('axios');

// 动态加载配置
const config = Object.assign(
  {
    env: process.env.NODE_ENV || 'development',
    cacheDir: process.env.CACHE_DIR || './cache'
  },
  require('./config.json')
);

// 唯一标识本次运行
const runId = `${Date.now()}-${Math.random().toString(36).substr(2, 5)}`;

class BinanceVolumeMonitor {
  constructor() {
    this.api = axios.create({
      baseURL: config.apiSettings.baseUrl,
      timeout: 10000,
      headers: {
        'X-Monitor-ID': runId  // 请求标识
      }
    });
  }

  async execute() {
    try {
      await this.ensureCacheDir();
      const symbols = await this.fetchValidSymbols();
      const topVolumes = await this.analyzeVolumes(symbols);
      await this.sendNotification(topVolumes);
      await this.cleanup();
    } catch (error) {
      await this.handleError(error);
    }
  }

  async ensureCacheDir() {
    if (!fs.existsSync(config.cacheDir)) {
      fs.mkdirSync(config.cacheDir, { recursive: true });
    }
  }

  async fetchValidSymbols() {
    const [allSymbols, existingCache] = await Promise.all([
      this.fetchSymbols(),
      this.loadCache()
    ]);

    return allSymbols.filter(symbol => {
      return !existingCache.some(c => c.symbol === symbol);
    });
  }

  async analyzeVolumes(symbols) {
    // ... (保持原有分析逻辑)
    // 关键改进：添加请求间隔控制
    await this.sleep(config.apiSettings.rateLimit);
  }

  async sendNotification(data) {
    // ... (保持原有通知逻辑)
    // 添加监控标识
    const title = `[${config.env.toUpperCase()}] ${data.length > 0 ? '📊' : '⚠️'} Binance Volume Alert`;
  }

  async cleanup() {
    // 清理7天前的缓存
    const files = await fs.promises.readdir(config.cacheDir);
    const cleanupTasks = files.map(async file => {
      if (file.endsWith('.json') && file !== 'latest.json') {
        const filePath = path.join(config.cacheDir, file);
        const stats = await fs.promises.stat(filePath);
        if (Date.now() - stats.mtimeMs > 7 * 24 * 60 * 60 * 1000) {
          await fs.promises.unlink(filePath);
        }
      }
    });
    await Promise.all(cleanupTasks);
  }
}

// 启动监控
new BinanceVolumeMonitor().execute();
