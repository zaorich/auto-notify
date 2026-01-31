import urllib.request
import json
import time

# --- 配置区域 ---
PROXY_ADDR = "127.0.0.1:10808"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

def get_proxy_opener():
    """创建一个带有本地代理配置的请求打开器"""
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_futures_gainers(opener):
    """任务1: 获取合约市场 24H 涨幅榜"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    print(f"\n{'='*20} 任务 1: 合约涨幅榜 {'='*20}")
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with opener.open(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        # 数据处理
        for item in data:
            item['priceChangePercent'] = float(item['priceChangePercent'])
        
        # 排序并取前10
        top_10 = sorted(data, key=lambda x: x['priceChangePercent'], reverse=True)[:10]
        
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        print(f"{'交易对':<20} {'最新价格':<15} {'24H涨幅':<10}")
        print("-" * 50)
        
        for item in top_10:
            symbol = item['symbol']
            price = float(item['lastPrice'])
            print(f"{symbol:<20} {price:<15g} {item['priceChangePercent']:+.2f}%")
            
    except Exception as e:
        print(f"❌ 任务 1 失败: {e}")

def get_wallet_token_list(opener):
    """任务2: 获取 Wallet/DeFi Token 列表"""
    # 你提供的新接口
    url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    print(f"\n{'='*20} 任务 2: Wallet Token 列表 {'='*20}")
    
    try:
        # BAPI 接口通常需要更严格的 Header，加上 Content-Type 和 Origin
        headers = {
            'User-Agent': USER_AGENT,
            'Content-Type': 'application/json',
            'Origin': 'https://www.binance.com'
        }
        req = urllib.request.Request(url, headers=headers)
        
        with opener.open(req) as response:
            raw_data = json.loads(response.read().decode('utf-8'))
        
        # BAPI 的数据通常包裹在 "data" 字段里，且有 "code" 状态码
        if raw_data.get('code') != '000000':
            print(f"❌ API 返回错误代码: {raw_data.get('code')}, 消息: {raw_data.get('message')}")
            return

        token_list = raw_data.get('data', [])
        count = len(token_list)
        
        print(f"✅ 成功获取数据，共发现 {count} 个币种/网络配置。")
        print(f"此处展示前 10 个结果作为示例 (Raw Data Sample):\n")
        
        # 定义表头
        print(f"{'资产代码(Asset)':<15} {'网络(Network)':<15} {'合约地址(部分)':<20}")
        print("-" * 55)
        
        # 遍历前10个输出
        for item in token_list[:10]:
            asset = item.get('asset', 'N/A')
            network = item.get('network', 'N/A')
            contract = item.get('contractAddress', '')
            
            # 合约地址如果太长，截断一下显示
            short_contract = (contract[:8] + '...' + contract[-6:]) if len(contract) > 15 else contract
            if not short_contract:
                short_contract = "Native"
                
            print(f"{asset:<15} {network:<15} {short_contract:<20}")

    except Exception as e:
        print(f"❌ 任务 2 失败: {e}")

if __name__ == "__main__":
    # 1. 准备代理连接器
    opener = get_proxy_opener()
    
    # 2. 执行任务 1 (合约)
    get_futures_gainers(opener)
    
    # 3. 执行任务 2 (新接口)
    get_wallet_token_list(opener)
