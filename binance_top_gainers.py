import urllib.request
import json
import time

def check_market():
    # 目标接口：币安 U本位合约 24hr 价格变动
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    
    # 本地代理地址 (对应 config.json 中的 inbounds 端口)
    proxy_addr = "127.0.0.1:10808"
    
    try:
        # 1. 配置代理处理器
        proxy_handler = urllib.request.ProxyHandler({
            'http': f'http://{proxy_addr}',
            'https': f'http://{proxy_addr}'
        })
        
        # 2. 构建 Opener
        opener = urllib.request.build_opener(proxy_handler)
        
        # 3. 准备请求 (伪装 User-Agent)
        headers = {'User-Agent': 'Mozilla/5.0'}
        req = urllib.request.Request(url, headers=headers)
        
        # 4. 发送请求
        with opener.open(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        # --- 数据处理部分 ---
        
        # 转换数据类型
        for item in data:
            item['priceChangePercent'] = float(item['priceChangePercent'])
            
        # 排序：按涨幅降序
        sorted_data = sorted(data, key=lambda x: x['priceChangePercent'], reverse=True)
        
        # 取前10名
        top_10 = sorted_data[:10]
        
        # 输出结果
        print(f"\n=== 币安合约 24H 涨幅榜 (代理模式: 日本) ===")
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n")
        print(f"{'交易对':<20} {'最新价格':<15} {'24H涨幅':<10}")
        print("-" * 50)
        
        for item in top_10:
            symbol = item['symbol']
            price = float(item['lastPrice'])
            # 格式化: g去除多余0, .2f保留两位小数
            print(f"{symbol:<20} {price:<15g} {item['priceChangePercent']:+.2f}%")
            
    except Exception as e:
        print(f"获取数据失败: {e}")

if __name__ == "__main__":
    check_market()
