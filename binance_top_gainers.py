import urllib.request
import json
import time

# --- 配置区域 ---
PROXY_ADDR = "127.0.0.1:10808"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 判定过期的时间阈值：10分钟 (毫秒)
TIME_TOLERANCE_MS = 10 * 60 * 1000 

def get_proxy_opener():
    """创建一个带有本地代理配置的请求打开器"""
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_data(opener, url, is_bapi=False):
    """通用数据获取函数"""
    headers = {'User-Agent': USER_AGENT}
    if is_bapi:
        headers.update({
            'Content-Type': 'application/json',
            'Origin': 'https://www.binance.com'
        })
    
    req = urllib.request.Request(url, headers=headers)
    with opener.open(req) as response:
        return json.loads(response.read().decode('utf-8'))

def get_futures_gainers(opener):
    """任务1: 合约市场涨幅榜 (U本位)"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    print(f"\n{'='*20} 任务 1: 合约涨幅榜 (Futures) {'='*20}")
    
    try:
        data = get_data(opener, url)
        
        current_time_ms = int(time.time() * 1000)
        clean_data = []

        for item in data:
            if current_time_ms - int(item['closeTime']) > TIME_TOLERANCE_MS:
                continue # 过滤过期数据

            item['priceChangePercent'] = float(item['priceChangePercent'])
            clean_data.append(item)
        
        top_10 = sorted(clean_data, key=lambda x: x['priceChangePercent'], reverse=True)[:10]
        
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        print(f"{'交易对':<20} {'最新价格':<15} {'24H涨幅':<10}")
        print("-" * 55)
        
        for item in top_10:
            print(f"{item['symbol']:<20} {float(item['lastPrice']):<15g} {item['priceChangePercent']:+.2f}%")
            
    except Exception as e:
        print(f"❌ 任务 1 失败: {e}")

def get_wallet_gainers(opener):
    """任务2: Wallet Token 列表涨幅榜 (需关联现货数据)"""
    # 1. 目标接口 (只包含币种列表，不含价格)
    token_list_url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    
    # 2. 现货行情接口 (用来查价格)
    spot_ticker_url = "https://api.binance.com/api/v3/ticker/24hr"
    
    print(f"\n{'='*20} 任务 2: Wallet Token 涨幅榜 {'='*20}")
    
    try:
        # --- 步骤 A: 获取 Wallet 币种白名单 ---
        print("正在获取 Wallet 列表...", end="", flush=True)
        raw_wallet_data = get_data(opener, token_list_url, is_bapi=True)
        
        if raw_wallet_data.get('code') != '000000':
            print(f"\n❌ BAPI 错误: {raw_wallet_data.get('code')}")
            return
            
        # 提取所有资产名称 (例如: 'BTC', 'ETH', 'UNI') 并去重
        wallet_assets = set()
        for item in raw_wallet_data.get('data', []):
            wallet_assets.add(item.get('asset'))
            
        print(f" 完成 (共 {len(wallet_assets)} 个币种)")

        # --- 步骤 B: 获取现货市场实时行情 ---
        print("正在获取现货市场价格...", end="", flush=True)
        spot_data = get_data(opener, spot_ticker_url)
        print(" 完成")
        
        # --- 步骤 C: 匹配与筛选 ---
        matched_data = []
        current_time_ms = int(time.time() * 1000)
        
        for item in spot_data:
            symbol = item['symbol']
            
            # 规则：必须是 USDT 交易对 (例如 BTCUSDT)
            if not symbol.endswith('USDT'):
                continue
                
            # 规则：必须在 Wallet 列表中 (移除后缀后匹配，例如 BTCUSDT -> BTC)
            base_asset = symbol[:-4] 
            if base_asset not in wallet_assets:
                continue

            # 规则：检查数据时效
            if current_time_ms - int(item['closeTime']) > TIME_TOLERANCE_MS:
                continue

            matched_data.append({
                'symbol': symbol,
                'price': float(item['lastPrice']),
                'change': float(item['priceChangePercent']),
                'asset': base_asset
            })
            
        # --- 步骤 D: 排序输出 ---
        # 按涨幅降序
        top_10 = sorted(matched_data, key=lambda x: x['change'], reverse=True)[:10]
        
        print(f"\n[筛选结果] 在 {len(spot_data)} 个现货交易对中，匹配到 {len(matched_data)} 个属于该列表的币种。\n")
        print(f"{'币种(Asset)':<15} {'交易对(Pair)':<15} {'价格':<15} {'24H涨幅':<10}")
        print("-" * 65)
        
        for item in top_10:
            print(f"{item['asset']:<15} {item['symbol']:<15} {item['price']:<15g} {item['change']:+.2f}%")

    except Exception as e:
        print(f"\n❌ 任务 2 失败: {e}")

if __name__ == "__main__":
    opener = get_proxy_opener()
    get_futures_gainers(opener)
    get_wallet_gainers(opener)
