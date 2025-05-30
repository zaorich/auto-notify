// 使用 Server酱 微信通知
const axios = require("axios");

async function sendWeChat() {
    const sckey = "你的Server酱KEY";
    await axios.post(`https://sctapi.ftqq.com/${sckey}.send`, {
        title: "定时通知",
        desp: `⏰ 现在是 ${new Date().toLocaleString()}`
    });
}

sendWeChat().catch(console.error);
