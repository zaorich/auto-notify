import urllib.request
import json
import time
import os

def check_market():
    # 既然有了日本代理，我们直接用回币安的官方接口
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    
    # 设置代理地址 (对应 config.json 里的 inbounds 端口 10808)
    proxy_addr = "127.0.0.1:10808"
    
    try:
        # 配置代理 Handler
        proxy_handler = urllib.request.ProxyHandler({
            'http': f'http://{proxy_addr}',
            'https': f'http://{proxy_addr}'
        })
        opener = urllib.request.build_opener(proxy_handler)
        
        # 伪装 Header
        headers = {'User-Agent': 'Mozilla/5.0'}
        req = urllib.request.Request(url, headers=headers)
        
        # 使用带代理的 opener 发送请求
        with opener.open(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        # --- 下面是原本的数据处理逻辑 ---
        
        for item in data:
            item['priceChangePercent'] = float(item['priceChangePercent'])
            
        sorted_data = sorted(data, key=lambda x: x['priceChangePercent'], reverse=True)
        top_10 = sorted_data[:10]
        
        print(f"\n=== 币安合约 24H 涨幅榜 (代理模式: 日本) ===")
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n")
        print(f"{'交易对':<20} {'最新价格':<15} {'24H涨幅':<10}")
        print("-" * 50)
        
        for item in top_10:
            symbol = item['symbol']
            price = float(item['lastPrice'])
            print(f"{symbol:<20} {price:<15g} {item['priceChangePercent']:+.2f}%")
            
    except Exception as e:
        print(f"获取数据失败: {e}")

if __name__ == "__main__":
    check_market()
