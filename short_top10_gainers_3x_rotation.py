import urllib.request
import urllib.parse
import json
import time
import os
import csv
import re
from datetime import datetime

# --- 策略核心配置 ---
PROXY_ADDR = "127.0.0.1:10808"
STATE_FILE = "strategy_state.json"
HISTORY_FILE = "strategy_history.csv"

# 资金参数
INIT_BALANCE = 1000.0     # 策略初始总本金
POSITIONS_COUNT = 10      # 仓位数量
LEVERAGE = 3.0            # 3倍杠杆

HEADERS = {'User-Agent': 'Mozilla/5.0'}
SERVERCHAN_KEY = os.environ.get("SERVERCHAN_KEY")

def get_proxy_opener():
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_data(opener, url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with opener.open(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"❌ 请求失败 [{url}]: {e}")
        return None

def get_market_rank(opener):
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    data = get_data(opener, url)
    if not data: return {}, []
    
    market_map = {}
    rank_list = []
    current_ts = int(time.time() * 1000)
    valid_symbol_pattern = re.compile(r'^[A-Z0-9]+$')
    
    for item in data:
        symbol = item['symbol']
        if not valid_symbol_pattern.match(symbol): continue
        if current_ts - int(item['closeTime']) > 10 * 60 * 1000: continue
            
        price = float(item['lastPrice'])
        change = float(item['priceChangePercent'])
        market_map[symbol] = price
        rank_list.append({'symbol': symbol, 'change': change, 'price': price})
        
    rank_list.sort(key=lambda x: x['change'], reverse=True)
    return market_map, rank_list[:POSITIONS_COUNT]

def get_recent_high_price(opener, symbol):
    safe_symbol = urllib.parse.quote(symbol)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={safe_symbol}&interval=15m&limit=1"
    data = get_data(opener, url)
    if data and len(data) > 0:
        return float(data[0][2])
    return 0.0

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, pos_pnl, equity, note=""):
    """
    CSV 记录函数
    Current_Price: 当前交易对价格
    High_Price: 15分钟最高价(用于回测风控)
    Pos_PnL: 单个仓位的盈亏
    Equity: 整个策略的当前净值(余额+所有盈亏)
    """
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # 控制台日志格式化对齐
    print(f"📝 [CSV] {record_type:<10} 策略{strategy_id:<2} {symbol:<8} 价:{price:<8g} 仓位盈亏:{pos_pnl:+.2f} 策略净值:{equity:.2f}U | {note}")

    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # CSV 表头
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Pos_PnL", "Strategy_Equity", "Note"])
            
            # 写入数据行
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, pos_pnl, equity, note])
    except Exception as e:
        print(f"❌ 写入CSV失败: {e}")

def load_state():
    if not os.path.exists(STATE_FILE):
        data = {}
        for i in range(24):
            data[str(i)] = {
                "balance": INIT_BALANCE, # 这里的 balance 指"钱包余额"(已实现盈亏)
                "positions": [],
                "last_trade_date": ""
            }
        return data
    with open(STATE_FILE, 'r') as f:
        return json.load(f)

def save_state(data):
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def check_risk_management(opener, data, market_map):
    print("\n🛡️ [监控] 开始全仓风控检查 (含插针检测)...")
    
    for s_id in data:
        strategy = data[s_id]
        wallet_balance = strategy['balance']
        positions = strategy['positions']
        
        if not positions: continue

        total_unrealized_pnl = 0.0
        details = []

        # 1. 计算全仓总盈亏 (基于15分钟最高价，模拟最坏情况)
        for pos in positions:
            symbol = pos['symbol']
            entry = pos['entry_price']
            amount = pos['amount']
            
            # 获取当前行情
            curr = market_map.get(symbol, entry)
            # 获取过去15分钟最高价 (插针)
            high_15m = get_recent_high_price(opener, symbol)
            # 风控计算价格：取两者较大值，确保捕捉到插针爆仓
            risk_price = max(curr, high_15m) if high_15m > 0 else curr
            
            # 做空浮动盈亏 = (开仓价 - 风险价格) * 数量
            pnl = (entry - risk_price) * amount
            total_unrealized_pnl += pnl
            
            # 暂存明细，用于后面记录
            details.append({
                'symbol': symbol,
                'curr': curr,
                'high': risk_price,
                'amount': amount,
                'pnl': pnl
            })

        # 2. 计算当前动态净值 (Equity)
        equity = wallet_balance + total_unrealized_pnl
        
        # 3. 记录监控日志 (CSV)
        # 为避免日志过于冗长，这里只记录每单的状况，但 Equity 是整体的
        for d in details:
            log_to_csv("MONITOR", s_id, d['symbol'], d['curr'], d['high'], d['amount'], d['pnl'], equity, "全仓监控")

        # 4. 全仓爆仓判断
        # 如果 净值 <= 0，则该策略下所有仓位全部强平
        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零 ({equity:.2f}U)")
            
            # 记录爆仓日志
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['high'], d['high'], d['amount'], d['pnl'], 0, "全仓强平")
            
            # 重置策略状态
            strategy['balance'] = 0
            strategy['positions'] = [] # 清空所有持仓
            
        else:
            # 安全，无需操作
            pass

def execute_rotation(opener, data, market_map, top_10):
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    strategy = data[current_hour]
    
    if strategy['last_trade_date'] == today_str:
        return False

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动逻辑...")
    
    # 1. 平掉旧仓位 (全仓模式下，按当前价结算，更新钱包余额)
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    
    # 如果此时已经爆仓归零了，就没法平仓了
    if wallet_balance > 0 and strategy['positions']:
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry = pos['entry_price']
            amount = pos['amount']
            
            exit_price = market_map.get(symbol, entry)
            pnl = (entry - exit_price) * amount
            total_close_pnl += pnl
            
            # 平仓时的净值 = 平仓前的钱包余额 + 该单盈亏 (近似)
            # 为了CSV好看，我们算出平仓后的累计净值
            temp_equity = wallet_balance + total_close_pnl
            
            log_to_csv("CLOSE", current_hour, symbol, exit_price, exit_price, amount, pnl, temp_equity, "轮动平仓")

        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    # 2. 开新仓
    # 更新后的钱包余额
    current_equity = strategy['balance']
    
    if current_equity < 100:
        log_to_csv("SKIP", current_hour, "ALL", 0, 0, 0, 0, current_equity, "净值不足100U")
    else:
        # 全仓模式：资金也是均分
        margin_per_coin = current_equity / POSITIONS_COUNT
        
        top10_str = "|".join([x['symbol'] for x in top_10])
        log_to_csv("INFO", current_hour, "TOP10_LIST", 0, 0, 0, 0, current_equity, top10_str)

        new_positions = []
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
            log_to_csv("OPEN", current_hour, symbol, price, price, amount, 0, current_equity, "开空")
            
        strategy['positions'] = new_positions

    strategy['last_trade_date'] = today_str
    return True

def report_to_wechat(opener, data, market_map):
    if not SERVERCHAN_KEY: return
    print("\n📤 正在生成全仓净值报告...")
    
    total_equity = 0
    total_init = 24 * INIT_BALANCE
    max_profit = -999999
    best_strategy = ""
    
    # 表格头: ID | 净值 | 盈亏 | 持仓
    md_table = "| ID | 净值(U) | 盈亏 | 持仓 |\n| :---: | :---: | :---: | :---: |\n"
    detail_text = ""
    
    for i in range(24):
        s_id = str(i)
        info = data[s_id]
        wallet_bal = info['balance']
        positions = info['positions']
        
        # 计算该策略当前的浮动盈亏总和
        strategy_floating_pnl = 0
        pos_details = []
        
        if positions:
            for pos in positions:
                symbol = pos['symbol']
                entry = pos['entry_price']
                amount = pos['amount']
                
                curr = market_map.get(symbol, entry)
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m == 0: high_15m = curr
                
                pnl = (entry - curr) * amount
                strategy_floating_pnl += pnl
                
                warn = "⚠️" if high_15m > entry * 1.05 else ""
                pos_details.append(f"- `{symbol:<6} 开:{entry:<8g} 现:{curr:<8g} 盈亏:{pnl:+.1f}U {warn}`")

        # 全仓净值 = 钱包余额 + 浮动盈亏
        equity = wallet_bal + strategy_floating_pnl
        total_equity += equity
        
        net_pnl = equity - INIT_BALANCE
        
        if net_pnl > max_profit:
            max_profit = net_pnl
            best_strategy = f"策略{s_id}"

        icon = "🔴" if net_pnl < 0 else "🟢"
        md_table += f"| {s_id} | {equity:.0f} | {icon}{net_pnl:+.0f} | {len(positions)} |\n"

        if positions:
            detail_text += f"\n🔷 **策略 {s_id} 全仓详情** (净值:{equity:.1f}U):\n"
            detail_text += "\n".join(pos_details) + "\n"

    total_pnl = total_equity - total_init
    total_pnl_pct = (total_pnl / total_init) * 100

    current_utc = datetime.utcnow().strftime("%H:%M")
    title = f"策略日报: 总净值 {total_equity:.0f}U ({total_pnl_pct:+.2f}%)"
    
    description = f"""
**UTC 时间**: {current_utc}
**总净值**: {total_equity:.1f} U
**总盈亏**: {total_pnl:+.1f} U
**最佳**: {best_strategy} ({max_profit:+.1f} U)

---
{md_table}
---
### 📝 持仓明细
{detail_text}
    """
    
    print(f"\n{'='*20} 📢 微信通知预览 {'='*20}")
    print(f"【标题】: {title}")
    # print(description) # 内容太长，控制台不打印全部正文，只发微信
    print(f"{'='*55}\n")

    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    params = {'title': title, 'desp': description}
    try:
        req = urllib.request.Request(url, data=urllib.parse.urlencode(params).encode('utf-8'), method='POST')
        with urllib.request.urlopen(req) as f:
            print("✅ 微信通知已发送")
    except Exception as e:
        print(f"❌ 微信发送失败: {e}")

if __name__ == "__main__":
    opener = get_proxy_opener()
    market_map, top_10 = get_market_rank(opener)
    
    if market_map:
        data = load_state()
        check_risk_management(opener, data, market_map)
        has_rotated = execute_rotation(opener, data, market_map, top_10)
        save_state(data)
        
        if has_rotated:
            report_to_wechat(opener, data, market_map)
