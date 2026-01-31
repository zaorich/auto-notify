import urllib.request
import json
import time
import os
import csv
from datetime import datetime

# --- 策略核心配置 ---
PROXY_ADDR = "127.0.0.1:10808"
STATE_FILE = "strategy_state.json"      # [状态文件] 存当前持仓和余额 (覆盖模式)
HISTORY_FILE = "strategy_history.csv"   # [历史文件] 存所有操作和监控日志 (追加模式)

# 资金参数
INIT_BALANCE = 1000.0     # 初始总本金
TRADE_MARGIN = 100.0      # 每次开仓总保证金
LEVERAGE = 3.0            # 3倍杠杆
MAX_POSITIONS = 10        # 持仓数量

# 爆仓阈值 (33.33%)
LIQUIDATION_THRESHOLD = 1 / LEVERAGE 

HEADERS = {'User-Agent': 'Mozilla/5.0'}

def get_proxy_opener():
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_market_data(opener):
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with opener.open(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        market_map = {}
        rank_list = []
        current_ts = int(time.time() * 1000)
        
        for item in data:
            if current_ts - int(item['closeTime']) > 10 * 60 * 1000:
                continue
            symbol = item['symbol']
            price = float(item['lastPrice'])
            change = float(item['priceChangePercent'])
            market_map[symbol] = price
            rank_list.append({'symbol': symbol, 'change': change, 'price': price})
            
        rank_list.sort(key=lambda x: x['change'], reverse=True)
        return market_map, rank_list[:MAX_POSITIONS]
    except Exception as e:
        print(f"❌ 获取行情失败: {e}")
        return {}, []

# --- CSV 记录核心函数 ---
def log_to_csv(record_type, strategy_id, symbol, price, amount, pnl, balance, note=""):
    """
    追加写入 CSV 文件
    字段: 时间, 策略ID, 类型, 币种, 价格, 数量, 盈亏(U), 当前余额(U), 备注
    """
    file_exists = os.path.isfile(HISTORY_FILE)
    
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # 如果是新文件，先写入表头
        if not file_exists:
            writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "Amount", "PnL", "Balance", "Note"])
        
        writer.writerow([current_time, strategy_id, record_type, symbol, price, amount, pnl, balance, note])

# --- 状态管理函数 ---
def load_state():
    if not os.path.exists(STATE_FILE):
        print("初始化状态文件...")
        data = {}
        for i in range(24):
            data[str(i)] = {
                "balance": INIT_BALANCE,
                "positions": [],
                "last_trade_date": ""
            }
        return data
    with open(STATE_FILE, 'r') as f:
        return json.load(f)

def save_state(data):
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# --- 核心逻辑 ---

def check_risk_management(data, market_map):
    """每15分钟运行：详细监控并记录CSV"""
    print("\n🛡️ [监控] 开始风控检查 & 状态记录...")
    
    for s_id in data:
        strategy = data[s_id]
        active_positions = []
        positions_changed = False
        
        # 如果该策略没持仓，跳过
        if not strategy['positions']:
            continue
            
        print(f"  > 策略 {s_id} (余额: {strategy['balance']:.2f}U) 持仓监控:")
        
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry_price = pos['entry_price']
            amount = pos['amount']
            margin = pos['margin']
            
            if symbol not in market_map:
                active_positions.append(pos)
                print(f"    - {symbol}: ⚠️ 无法获取最新价格")
                continue
                
            curr_price = market_map[symbol]
            # 做空浮动盈亏: (开仓价 - 当前价) * 数量
            unrealized_pnl = (entry_price - curr_price) * amount
            pnl_pct = unrealized_pnl / margin  # 盈亏比例
            
            # 格式化输出
            status_icon = "🟢" if unrealized_pnl >= 0 else "🔴"
            print(f"    {status_icon} {symbol:<10} 开: {entry_price:<8g} 现: {curr_price:<8g} 盈亏: {unrealized_pnl:+.2f}U ({pnl_pct*100:+.2f}%)")
            
            # --- 写入 CSV 监控快照 ---
            # 为了不让CSV爆炸，你可以选择是否每次都记。这里为了"详细回溯"，我们记录它。
            log_to_csv("MONITOR", s_id, symbol, curr_price, amount, f"{unrealized_pnl:.2f}", strategy['balance'], f"浮盈: {pnl_pct*100:.1f}%")

            # 爆仓检查
            # 这里的爆仓逻辑是：如果亏损达到保证金的 100% (实际上3倍杠杆只要涨33%就亏光了)
            # 亏损百分比 pnl_pct <= -1.0 (即 -100%)
            # 注意：做空时，价格上涨，unrealized_pnl 为负数。
            # 所以判断条件是: unrealized_pnl <= -margin (亏光本金)
            if unrealized_pnl <= -margin:
                print(f"    💥 {symbol} 触发爆仓！本金归零。")
                
                # 记录爆仓日志
                log_to_csv("LIQUIDATION", s_id, symbol, curr_price, amount, -margin, strategy['balance'] - margin, "触发强平")
                
                strategy['balance'] -= margin
                positions_changed = True
                # 爆仓后移除该仓位
            else:
                active_positions.append(pos)
        
        if positions_changed:
            strategy['positions'] = active_positions

def execute_rotation(data, market_map, top_10):
    """整点轮动逻辑"""
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    
    strategy = data[current_hour]
    
    if strategy['last_trade_date'] == today_str:
        print(f"⏳ 策略 {current_hour} 今日已执行过，跳过。")
        return

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动逻辑...")
    
    # 1. 平掉旧仓位
    total_pnl = 0
    
    for pos in strategy['positions']:
        symbol = pos['symbol']
        entry_price = pos['entry_price']
        amount = pos['amount']
        
        exit_price = market_map.get(symbol, entry_price)
        pnl = (entry_price - exit_price) * amount
        total_pnl += pnl
        
        # 记录平仓日志
        log_to_csv("CLOSE", current_hour, symbol, exit_price, amount, f"{pnl:.2f}", strategy['balance'] + pnl, "每日轮动平仓")

    strategy['balance'] += total_pnl
    strategy['positions'] = [] # 清空
    
    # 2. 开新仓
    if strategy['balance'] < 100:
        print(f"⚠️ 余额不足，跳过开仓。")
        log_to_csv("SKIP", current_hour, "ALL", 0, 0, 0, strategy['balance'], "余额不足100U")
    else:
        margin_per_coin = TRADE_MARGIN / MAX_POSITIONS
        new_positions = []
        
        print(f"📉 开设新仓位 (Top 10):")
        # 将本次选中的 Top 10 列表作为字符串记录一下，方便回溯知道当时选了谁
        top10_str = "|".join([x['symbol'] for x in top_10])
        log_to_csv("INFO", current_hour, "TOP10_LIST", 0, 0, 0, strategy['balance'], top10_str)

        for item in top_10:
            symbol = item['symbol']
            price = item['price']
            amount = (margin_per_coin * LEVERAGE) / price
            
            new_positions.append({
                "symbol": symbol,
                "entry_price": price,
                "margin": margin_per_coin,
                "amount": amount,
                "leverage": LEVERAGE
            })
            
            print(f"   SHORT {symbol} @ {price}")
            # 记录开仓日志
            log_to_csv("OPEN", current_hour, symbol, price, amount, 0, strategy['balance'], "开空")

        strategy['positions'] = new_positions

    strategy['last_trade_date'] = today_str
    print(f"✅ 策略 {current_hour} 完成。当前余额: {strategy['balance']:.2f}U")

if __name__ == "__main__":
    opener = get_proxy_opener()
    market_map, top_10 = get_market_data(opener)
    
    if market_map:
        data = load_state()
        check_risk_management(data, market_map)
        execute_rotation(data, market_map, top_10)
        save_state(data)
