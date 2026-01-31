import urllib.request
import json
import time
from datetime import datetime

# --- 配置区域 ---
PROXY_ADDR = "127.0.0.1:10808"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 任务1 (合约) 时间阈值：10分钟
# 只有最近 10 分钟内有成交的合约才会被统计，过滤掉僵尸交易对
FUTURES_TIME_TOLERANCE = 10 * 60 * 1000 

def format_duration(timestamp_ms):
    """将时间戳转换为 '已上市 X天' 的格式"""
    if not timestamp_ms or timestamp_ms <= 0:
        return "未知", "N/A"
    
    listing_time = timestamp_ms / 1000
    now = time.time()
    diff = now - listing_time
    
    # 转换为日期字符串
    date_str = datetime.utcfromtimestamp(listing_time).strftime('%Y-%m-%d')
    
    if diff < 0:
        return f"预发布 ({date_str})", date_str
        
    days = int(diff // (24 * 3600))
    hours = int((diff % (24 * 3600)) // 3600)
    
    if days > 365:
        return f"{days // 365}年{days % 365}天", date_str
    elif days > 0:
        return f"{days}天 {hours}小时", date_str
    else:
        return f"{hours}小时前", date_str

def get_proxy_opener():
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_data(opener, url, is_bapi=False):
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
    """任务1: 合约市场涨幅榜 (严格过滤: 10分钟)"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    print(f"\n{'='*20} 任务 1: 合约涨幅榜 (Futures) {'='*20}")
    
    try:
        data = get_data(opener, url)
        current_time_ms = int(time.time() * 1000)
        clean_data = []
        ignored_count = 0

        for item in data:
            # --- 严格过滤逻辑 ---
            if current_time_ms - int(item['closeTime']) > FUTURES_TIME_TOLERANCE:
                ignored_count += 1
                continue 
            
            item['priceChangePercent'] = float(item['priceChangePercent'])
            clean_data.append(item)
        
        top_10 = sorted(clean_data, key=lambda x: x['priceChangePercent'], reverse=True)[:10]
        
        print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        print(f"有效数据: {len(clean_data)} 条 | 已过滤过期(>10m): {ignored_count} 条")
        print("-" * 55)
        print(f"{'交易对':<15} {'最新价格':<12} {'24H涨幅':<10} {'延迟(秒)'}")
        print("-" * 55)
        
        for item in top_10:
            delay = (current_time_ms - item['closeTime']) / 1000
            print(f"{item['symbol']:<15} {float(item['lastPrice']):<12g} {item['priceChangePercent']:+.2f}%     {delay:.1f}s")
            
    except Exception as e:
        print(f"❌ 任务 1 失败: {e}")

def get_wallet_gainers(opener):
    """任务2: Wallet Token 列表涨幅榜 (无时间过滤)"""
    token_list_url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    spot_ticker_url = "https://api.binance.com/api/v3/ticker/24hr"
    
    print(f"\n{'='*20} 任务 2: Wallet Token 涨幅榜 (含上市时间) {'='*20}")
    
    try:
        # --- 步骤 A: 获取 Wallet 币种元数据 ---
        print("正在获取 Wallet 列表...", end="", flush=True)
        raw_wallet_data = get_data(opener, token_list_url, is_bapi=True)
        
        if raw_wallet_data.get('code') != '000000':
            print(f" 错误: {raw_wallet_data.get('code')}")
            return
            
        wallet_meta = {}
        for item in raw_wallet_data.get('data', []):
            asset = item.get('asset')
            l_time = item.get('listingTime')
            if asset not in wallet_meta or (l_time and l_time > 0):
                wallet_meta[asset] = l_time
            
        print(f" 完成 (共 {len(wallet_meta)} 个币种)")

        # --- 步骤 B: 获取现货行情 ---
        print("正在获取现货市场价格...", end="", flush=True)
        spot_data = get_data(opener, spot_ticker_url)
        print(" 完成")
        
        # --- 步骤 C: 匹配与筛选 ---
        matched_data = []
        
        for item in spot_data:
            symbol = item['symbol']
            
            if not symbol.endswith('USDT'): continue
                
            base_asset = symbol[:-4] 
            if base_asset not in wallet_meta: continue

            # [修改点] 这里不再检查 closeTime，所有数据默认有效
            
            l_time_ms = wallet_meta[base_asset]
            duration_str, date_str = format_duration(l_time_ms)

            matched_data.append({
                'symbol': symbol,
                'price': float(item['lastPrice']),
                'change': float(item['priceChangePercent']),
                'asset': base_asset,
                'duration': duration_str,
                'date': date_str
            })
            
        # --- 步骤 D: 排序输出 ---
        top_10 = sorted(matched_data, key=lambda x: x['change'], reverse=True)[:10]
        
        print(f"\n[结果] 筛选出 {len(matched_data)} 个交易对 (无时间过滤)。\n")
        
        header = f"{'币种':<10} {'价格':<10} {'24H涨幅':<10} {'上市日期':<12} {'已上市时长'}"
        print(header)
        print("-" * len(header) * 2) 
        
        for item in top_10:
            print(f"{item['asset']:<10} {item['price']:<10g} {item['change']:+.2f}%     {item['date']:<12} {item['duration']}")

    except Exception as e:
        print(f"\n❌ 任务 2 失败: {e}")

if __name__ == "__main__":
    opener = get_proxy_opener()
    get_futures_gainers(opener)
    get_wallet_gainers(opener)
