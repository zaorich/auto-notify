import urllib.request
import urllib.parse
import json
import time
import os
import csv
from datetime import datetime

# --- 策略核心配置 ---
PROXY_ADDR = "127.0.0.1:10808"
STATE_FILE = "strategy_state.json"
HISTORY_FILE = "strategy_history.csv"
EQUITY_FILE = "equity_curve.csv"

# 资金参数
INIT_BALANCE = 1000.0     
POSITIONS_COUNT = 10      
LEVERAGE = 3.0            

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
    
    for item in data:
        symbol = item['symbol']
        if current_ts - int(item['closeTime']) > 10 * 60 * 1000:
            continue
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

def calculate_strategy_equity(strategy, market_map, opener=None, use_high_price=False):
    """核心计算函数"""
    wallet_balance = strategy['balance']
    positions = strategy['positions']
    
    total_unrealized_pnl = 0.0
    details = []
    
    if positions:
        for pos in positions:
            symbol = pos['symbol']
            entry = float(pos['entry_price'])
            amount = float(pos['amount'])
            
            curr = market_map.get(symbol, entry)
            calc_price = curr
            warn_msg = ""
            
            if opener and use_high_price:
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m > 0:
                    calc_price = max(curr, high_15m)
                    if high_15m > entry * 1.05: warn_msg = "⚠️"

            pnl = (entry - calc_price) * amount
            total_unrealized_pnl += pnl
            
            details.append({
                'symbol': symbol,
                'entry': entry,
                'curr': curr,
                'calc_price': calc_price,
                'amount': amount,
                'pnl': pnl,
                'warn': warn_msg
            })
            
    equity = wallet_balance + total_unrealized_pnl
    return equity, details

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, pos_pnl, equity, note=""):
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"📝 [CSV] {record_type:<10} 策略{strategy_id:<2} {symbol:<8} 价:{price:<8g} 仓位盈亏:{pos_pnl:+.2f} 净值:{equity:.2f} | {note}")

    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Pos_PnL", "Strategy_Equity", "Note"])
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, pos_pnl, equity, note])
    except Exception as e:
        print(f"❌ 写入历史CSV失败: {e}")

def record_equity_snapshot(data, market_map):
    file_exists = os.path.isfile(EQUITY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    row_data = [current_time]
    total_equity = 0.0
    
    for i in range(24):
        s_id = str(i)
        eq, _ = calculate_strategy_equity(data[s_id], market_map, opener=None, use_high_price=False)
        if eq < 0: eq = 0
        row_data.append(round(eq, 2))
        total_equity += eq
        
    row_data.append(round(total_equity, 2))
    
    try:
        with open(EQUITY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                headers = ["Time"] + [f"S_{i}" for i in range(24)] + ["Total"]
                writer.writerow(headers)
            writer.writerow(row_data)
    except Exception as e:
        print(f"❌ 写入净值CSV失败: {e}")

def load_state():
    if not os.path.exists(STATE_FILE):
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

def check_risk_management(opener, data, market_map):
    print("\n🛡️ [监控] 开始全仓风控检查 (含插针检测)...")
    liquidated_ids = [] # 记录本次运行爆仓的策略ID
    
    for s_id in data:
        strategy = data[s_id]
        if not strategy['positions']: continue

        equity, details = calculate_strategy_equity(strategy, market_map, opener, use_high_price=True)
        
        for d in details:
            log_to_csv("MONITOR", s_id, d['symbol'], d['curr'], d['calc_price'], d['amount'], d['pnl'], equity, "全仓监控")

        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            liquidated_ids.append(s_id) # 记录下来
            
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, "全仓强平")
            strategy['balance'] = 0
            strategy['positions'] = []
            
    return liquidated_ids

def execute_rotation(opener, data, market_map, top_10):
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    strategy = data[current_hour]
    
    # [逻辑优化] 只要日期不对，无论现在是14:00还是14:59，都会执行补单
    if strategy['last_trade_date'] == today_str:
        return None # 今天已做过，无需操作

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动/补单逻辑 (当前时间不是整点也能触发)...")
    
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    
    if wallet_balance > 0 and strategy['positions']:
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry = float(pos['entry_price'])
            amount = float(pos['amount'])
            
            exit_price = market_map.get(symbol, entry)
            pnl = (entry - exit_price) * amount
            total_close_pnl += pnl
            
            temp_equity = wallet_balance + total_close_pnl
            log_to_csv("CLOSE", current_hour, symbol, exit_price, exit_price, amount, pnl, temp_equity, "轮动平仓")

        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    current_equity = strategy['balance']
    
    if current_equity < 100:
        log_to_csv("SKIP", current_hour, "ALL", 0, 0, 0, 0, current_equity, "净值不足100U")
    else:
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
    
    return current_hour # 返回执行了轮动的策略ID

def report_to_wechat(opener, data, market_map, rotated_id, liquidated_ids):
    if not SERVERCHAN_KEY: 
        print("⚠️ 未配置 SERVERCHAN_KEY，跳过通知")
        return
        
    print("\n📤 正在生成详细报告...")
    
    total_equity = 0
    total_init = 24 * INIT_BALANCE
    max_profit = -999999
    best_strategy = ""
    
    md_table = "| ID | 净值(U) | 盈亏 | 持仓 |\n| :---: | :---: | :---: | :---: |\n"
    detail_text = ""
    
    for i in range(24):
        s_id = str(i)
        
        equity, details = calculate_strategy_equity(data[s_id], market_map, opener, use_high_price=False)
        
        total_equity += equity
        net_pnl = equity - INIT_BALANCE
        
        if net_pnl > max_profit:
            max_profit = net_pnl
            best_strategy = f"策略{s_id}"

        # 状态图标
        icon = "🔴" if net_pnl < 0 else "🟢"
        if equity == 0: icon = "💀" # 爆仓
        elif s_id == rotated_id: icon = "🔄" # 刚换仓
        
        pos_len = len(data[s_id]['positions'])
        md_table += f"| {s_id} | {equity:.0f} | {icon}{net_pnl:+.0f} | {pos_len} |\n"

        # --- 生成简报 ---
        if pos_len > 0:
            # 策略标题增加标记
            prefix = ""
            if s_id == rotated_id: prefix = "🔄"
            
            detail_text += f"\n🔷 **{prefix}策略{s_id}** (净:{equity:.0f}U):\n"
            
            simple_items = []
            for d in details:
                warn_mark = "⚠️" if d.get('warn') else ""
                short_symbol = d['symbol'].replace("USDT", "")
                item_str = f"{short_symbol}({d['pnl']:+.1f}){warn_mark}"
                simple_items.append(item_str)
            
            detail_text += ", ".join(simple_items) + "\n"
        elif equity == 0:
             detail_text += f"\n💀 **策略{s_id}** (已爆仓): 净值归零\n"

    total_pnl = total_equity - total_init
    total_pnl_pct = (total_pnl / total_init) * 100
    current_utc = datetime.utcnow().strftime("%H:%M")

    # --- [动态标题生成] ---
    title_parts = []
    if rotated_id:
        title_parts.append(f"🔄S{rotated_id}")
    if liquidated_ids:
        # 将列表转为 S01,S05 格式
        bust_str = ",".join([f"S{uid}" for uid in liquidated_ids])
        title_parts.append(f"💥{bust_str}")
        
    title_base = f"总净值 {total_equity:.0f}U ({total_pnl_pct:+.2f}%)"
    
    # 组合标题: "🔄S14 💥S02 | 总净值..."
    if title_parts:
        title = f"{' '.join(title_parts)} | {title_base}"
    else:
        title = f"策略日报: {title_base}"
    # ---------------------
    
    description = f"""
**UTC 时间**: {current_utc}
**总净值**: {total_equity:.1f} U
**总盈亏**: {total_pnl:+.1f} U
**最佳**: {best_strategy} ({max_profit:+.1f} U)

---
{md_table}
---
### 📝 持仓概览
{detail_text}
    """
    
    print(f"\n{'='*20} 📢 微信通知预览 {'='*20}")
    print(f"标题: {title}")
    # print(description)
    print("正文已生成，准备发送...")

    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    params = {'title': title, 'desp': description}
    try:
        req = urllib.request.Request(url, data=urllib.parse.urlencode(params).encode('utf-8'), method='POST')
        with urllib.request.urlopen(req) as f:
            print("✅ 微信推送请求已发送")
    except Exception as e:
        print(f"❌ 微信发送失败: {e}")

if __name__ == "__main__":
    opener = get_proxy_opener()
    market_map, top_10 = get_market_rank(opener)
    
    if market_map:
        data = load_state()
        
        # 1. 风控 (返回爆仓名单)
        liquidated_ids = check_risk_management(opener, data, market_map)
        
        # 2. 轮动 (返回轮动ID)
        rotated_id = execute_rotation(opener, data, market_map, top_10)
        
        record_equity_snapshot(data, market_map)
        save_state(data)
        
        # 3. 只要有轮动 或者 有爆仓，就必须通知
        if rotated_id or liquidated_ids:
            report_to_wechat(opener, data, market_map, rotated_id, liquidated_ids)
