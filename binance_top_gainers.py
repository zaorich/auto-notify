import urllib.request
import json
import time

# --- 配置区域 ---
PROXY_ADDR = "127.0.0.1:10808"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 判定过期的时间阈值：10分钟 (毫秒)
# 如果数据滞后超过10分钟，认为该交易对已暂停或下架
TIME_TOLERANCE_MS = 10 * 60 * 1000 

def get_proxy_opener():
    """创建一个带有本地代理配置的请求打开器"""
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_futures_gainers(opener):
    """任务1: 获取合约市场 24H 涨幅榜 (带时间过滤)"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    print(f"\n{'='*20} 任务 1: 合约涨幅榜 (已过滤过期数据) {'='*20}")
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with opener.open(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        current_time_ms = int(time.time() * 1000)
        clean_data = []
        ignored_count = 0

        # --- 核心过滤逻辑 ---
        for item in data:
            close_time = int(item['closeTime'])
            
            # 检查数据时效性
            if current_time_ms - close_time > TIME_TOLERANCE_MS:
                ignored_count += 1
                continue # 跳过过期数据

            # 正常处理数据
            item['priceChangePercent'] = float(item['priceChangePercent'])
            clean_data.append(item)
        
        # 排序并取前10
        top_10 = sorted(clean_data, key=lambda x: x['priceChangePercent'], reverse=True)[:10]
        
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        print(f"数据总条数: {len(data)} | 有效: {len(clean_data)} | 已忽略过期: {ignored_count}")
        print("-" * 65)
        print(f"{'交易对':<20} {'最新价格':<15} {'24H涨幅':<10} {'数据延迟'}")
        print("-" * 65)
        
        for item in top_10:
            symbol = item['symbol']
            price = float(item['lastPrice'])
            
            # 计算延迟秒数，方便观察
            delay_seconds = (current_time_ms - item['closeTime']) / 1000
            
            print(f"{symbol:<20} {price:<15g} {item['priceChangePercent']:+.2f}%     {delay_seconds:.1f}s")
            
    except Exception as e:
        print(f"❌ 任务 1 失败: {e}")

def get_wallet_token_list(opener):
    """任务2: 获取 Wallet/DeFi Token 列表"""
    url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    print(f"\n{'='*20} 任务 2: Wallet Token 列表 {'='*20}")
    
    try:
        headers = {
            'User-Agent': USER_AGENT,
            'Content-Type': 'application/json',
            'Origin': 'https://www.binance.com'
        }
        req = urllib.request.Request(url, headers=headers)
        
        with opener.open(req) as response:
            raw_data = json.loads(response.read().decode('utf-8'))
        
        if raw_data.get('code') != '000000':
            print(f"❌ API 返回错误代码: {raw_data.get('code')}")
            return

        token_list = raw_data.get('data', [])
        count = len(token_list)
        
        print(f"✅ 成功获取数据，共发现 {count} 个币种/网络配置。")
        print(f"此处展示前 10 个结果:\n")
        
        print(f"{'资产代码':<15} {'网络':<15} {'合约地址(Short)'}")
        print("-" * 55)
        
        for item in token_list[:10]:
            asset = item.get('asset', 'N/A')
            network = item.get('network', 'N/A')
            contract = item.get('contractAddress', '')
            
            short_contract = (contract[:8] + '...' + contract[-6:]) if len(contract) > 15 else contract
            if not short_contract:
                short_contract = "Native"
                
            print(f"{asset:<15} {network:<15} {short_contract}")

    except Exception as e:
        print(f"❌ 任务 2 失败: {e}")

if __name__ == "__main__":
    opener = get_proxy_opener()
    get_futures_gainers(opener)
    get_wallet_token_list(opener)
