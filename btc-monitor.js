const axios = require('axios');

// 配置参数
const config = {
  // 价格监控阈值
  priceThreshold: 50000, // 当BTC超过这个价格时通知
  // Server酱微信通知（https://sct.ftqq.com）
  wechatKey: 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e',
  // 邮件通知配置（可选）
  email: {
    service: 'QQ',
    user: 'your_email@qq.com',
    pass: 'your_smtp_password',
    to: 'receiver@example.com'
  }
};

async function checkBTCPrice() {
  try {
    // 从CoinGecko获取BTC价格
    const response = await axios.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd');
    const btcPrice = response.data.bitcoin.usd;
    console.log(`当前BTC价格: $${btcPrice}`);
    
    if (btcPrice > config.priceThreshold) {
      await sendNotification(btcPrice);
    }
  } catch (error) {
    console.error('获取价格失败:', error.message);
  }
}

async function sendNotification(price) {
  const message = `🚨 BTC价格警报: $${price} (超过阈值 $${config.priceThreshold})`;
  
  // 微信通知
  try {
    await axios.post(`https://sctapi.ftqq.com/${config.wechatKey}.send`, {
      title: 'BTC价格警报',
      desp: message
    });
    console.log('微信通知已发送');
  } catch (e) {
    console.error('微信通知发送失败:', e.message);
  }
  
  // 邮件通知（可选）
  // await sendEmail(message);
}

// 立即执行一次
checkBTCPrice();
