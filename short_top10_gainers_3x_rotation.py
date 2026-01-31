import urllib.request
import json
import time
import os
from datetime import datetime

# --- 策略核心配置 ---
PROXY_ADDR = "127.0.0.1:10808"
DATA_FILE = "strategy_data.json"

# 资金参数
INIT_BALANCE = 1000.0     # 每个时间点策略的初始总本金
TRADE_MARGIN = 100.0      # 每次开仓使用的总保证金 (100U)
LEVERAGE = 3.0            # 杠杆倍数 (3x)
MAX_POSITIONS = 10        # 每次做空前10名

# 爆仓阈值 (做空 3倍杠杆，价格上涨 33.33% 即爆仓)
LIQUIDATION_THRESHOLD = 1 / LEVERAGE 

# 伪装 Header
HEADERS = {'User-Agent': 'Mozilla/5.0'}

def get_proxy_opener():
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_market_data(opener):
    """获取所有合约最新价格和涨幅，并返回 Top 10"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with opener.open(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        market_map = {}
        rank_list = []
        current_ts = int(time.time() * 1000)
        
        for item in data:
            # 过滤掉 10 分钟无成交的僵尸数据
            if current_ts - int(item['closeTime']) > 10 * 60 * 1000:
                continue
                
            symbol = item['symbol']
            price = float(item['lastPrice'])
            change = float(item['priceChangePercent'])
            
            market_map[symbol] = price
            rank_list.append({'symbol': symbol, 'change': change, 'price': price})
            
        # 按涨幅降序排序
        rank_list.sort(key=lambda x: x['change'], reverse=True)
        return market_map, rank_list[:MAX_POSITIONS]
        
    except Exception as e:
        print(f"❌ 获取行情失败: {e}")
        return {}, []

def load_data():
    if not os.path.exists(DATA_FILE):
        print("初始化策略数据文件...")
        data = {}
        # 初始化 0-23 号策略
        for i in range(24):
            data[str(i)] = {
                "balance": INIT_BALANCE,
                "positions": [],   # 当前持仓
                "history": [],     # 历史记录
                "last_trade_date": "" # 上次交易日期 (YYYY-MM-DD)
            }
        return data
    with open(DATA_FILE, 'r') as f:
        return json.load(f)

def save_data(data):
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def check_risk_management(data, market_map):
    """每15分钟运行：检查所有策略的所有持仓是否爆仓"""
    print("🛡️ 开始风控检查 (每15分钟)...")
    
    for s_id in data:
        strategy = data[s_id]
        active_positions = []
        
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry_price = pos['entry_price']
            margin = pos['margin']
            
            # 如果当前市场没这个币价格（可能下架），暂时保留
            if symbol not in market_map:
                active_positions.append(pos)
                continue
                
            curr_price = market_map[symbol]
            
            # 做空亏损计算: (当前价 - 开仓价) / 开仓价
            pnl_pct = (curr_price - entry_price) / entry_price
            
            # 检查是否爆仓
            if pnl_pct >= LIQUIDATION_THRESHOLD:
                print(f"💥 [爆仓] 策略{s_id} {symbol}: 入场{entry_price} -> 当前{curr_price} (涨幅{(pnl_pct*100):.2f}%)")
                # 记录亏损日志
                strategy['history'].append({
                    "time": time.strftime('%Y-%m-%d %H:%M'),
                    "type": "LIQUIDATION",
                    "symbol": symbol,
                    "pnl": -margin, # 亏掉该仓位全额保证金
                    "entry": entry_price,
                    "exit": curr_price,
                    "note": "触发布局爆仓线"
                })
                strategy['balance'] -= margin
                # 爆仓后该仓位移除，不再加入 active_positions
            else:
                active_positions.append(pos)
        
        strategy['positions'] = active_positions

def execute_rotation(data, market_map, top_10):
    """整点轮动逻辑"""
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    
    strategy = data[current_hour]
    
    # 检查今天是否已经开过仓 (避免重复执行)
    if strategy['last_trade_date'] == today_str:
        print(f"⏳ 策略 {current_hour} 今日已执行过，跳过开仓。")
        return

    print(f"\n🔄 执行 策略 {current_hour} 轮动逻辑...")
    
    # 1. 平掉昨日旧仓位 (如果有)
    total_pnl = 0
    closed_count = 0
    
    for pos in strategy['positions']:
        symbol = pos['symbol']
        entry_price = pos['entry_price']
        amount = pos['amount']
        
        # 结算价格
        exit_price = market_map.get(symbol, entry_price) # 如果取不到价格，按保本算(极罕见)
        
        # 做空收益: (开仓价 - 平仓价) * 数量
        pnl = (entry_price - exit_price) * amount
        total_pnl += pnl
        
        strategy['history'].append({
            "time": time.strftime('%Y-%m-%d %H:%M'),
            "type": "CLOSE",
            "symbol": symbol,
            "pnl": pnl,
            "entry": entry_price,
            "exit": exit_price
        })
        closed_count += 1
    
    # 更新余额
    strategy['balance'] += total_pnl
    strategy['positions'] = [] # 清空仓位
    if closed_count > 0:
        print(f"💰 平仓结算: 结束 {closed_count} 个订单，总盈亏: {total_pnl:.2f}U")

    # 2. 开新仓
    # 检查余额是否足够支付 100U 保证金
    if strategy['balance'] < 100:
        print(f"⚠️ 策略 {current_hour} 余额不足 100U ({strategy['balance']:.2f})，跳过开仓。")
        # 即使不开仓，也要更新日期，免得下一次重试
        strategy['last_trade_date'] = today_str
        return

    # 分配保证金: 100U 分给 10个币 -> 每个 10U
    margin_per_coin = TRADE_MARGIN / MAX_POSITIONS
    
    print(f"📉 开设新仓位 (做空 Top {MAX_POSITIONS}):")
    new_positions = []
    
    for item in top_10:
        symbol = item['symbol']
        price = item['price']
        
        # 计算持仓数量 (币) = (保证金 * 杠杆) / 价格
        amount = (margin_per_coin * LEVERAGE) / price
        
        new_positions.append({
            "symbol": symbol,
            "entry_price": price,
            "margin": margin_per_coin,
            "amount": amount,
            "leverage": LEVERAGE,
            "open_time": time.strftime('%Y-%m-%d %H:%M')
        })
        print(f"   SHORT {symbol:<10} price: {price:<10g} amount: {amount:.4f}")
        
    strategy['positions'] = new_positions
    strategy['last_trade_date'] = today_str
    print(f"✅ 策略 {current_hour} 轮动完成，当前余额: {strategy['balance']:.2f}U")

if __name__ == "__main__":
    opener = get_proxy_opener()
    
    # 1. 获取最新市场数据
    market_map, top_10 = get_market_data(opener)
    
    if market_map:
        # 2. 加载数据
        data = load_data()
        
        # 3. 风控检查 (所有策略、每15分钟都查)
        check_risk_management(data, market_map)
        
        # 4. 执行轮动 (只针对当前 UTC 小时的策略)
        execute_rotation(data, market_map, top_10)
        
        # 5. 保存数据 (Git 会自动检测变化并提交)
        save_data(data)
