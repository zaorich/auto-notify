import urllib.request
import json
import time

def check_market():
    # 币安 U本位合约 24hr 价格变动接口
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    
    try:
        # 添加 User-Agent 伪装浏览器
        headers = {'User-Agent': 'Mozilla/5.0'}
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        # 数据处理：将涨幅转为浮点数
        for item in data:
            item['priceChangePercent'] = float(item['priceChangePercent'])
            
        # 排序：按涨幅降序
        sorted_data = sorted(data, key=lambda x: x['priceChangePercent'], reverse=True)
        
        # 取前10
        top_10 = sorted_data[:10]
        
        # 输出日志
        print(f"\n=== 币安合约 24H 涨幅榜 TOP 10 ===")
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n")
        
        # 格式化输出表头
        print(f"{'交易对':<20} {'最新价格':<15} {'24H涨幅':<10}")
        print("-" * 50)
        
        for item in top_10:
            symbol = item['symbol']
            price = float(item['lastPrice'])
            # 这里的格式说明：price 用 g (通用格式) 去掉多余零，change 用 .2f 保留两位小数
            print(f"{symbol:<20} {price:<15g} {item['priceChangePercent']:+.2f}%")
            
    except Exception as e:
        print(f"获取数据失败: {e}")

if __name__ == "__main__":
    check_market()
