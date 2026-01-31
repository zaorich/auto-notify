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
EQUITY_FILE = "equity_curve.csv"  # [新增] 专门用于画图的净值表

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
    """记录详细操作日志 (strategy_history.csv)"""
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

# --- [新增] 记录净值曲线 ---
def record_equity_snapshot(data, market_map):
    """
    将当前时刻所有策略的净值记录到一行，方便画图
    """
    file_exists = os.path.isfile(EQUITY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # 准备一行数据: [Time, S0_Eq, S1_Eq, ... S23_Eq, Total_Eq]
    row_data = [current_time]
    total_equity = 0.0
    
    # 遍历 0 到 23 号策略
    for i in range(24):
        s_id = str(i)
        strategy = data[s_id]
        
        # 计算该策略实时净值
        equity = strategy['balance'] # 基础余额
        
        # 加上持仓浮动盈亏
        if strategy['positions']:
            for pos in strategy['positions']:
                symbol = pos['symbol']
                entry = pos['entry_price']
                amount = pos['amount']
                # 使用当前价计算净值 (画图用当前价即可，不用最高价)
                curr = market_map.get(symbol, entry)
                pnl = (entry - curr) * amount
                equity += pnl
        
        # 如果已经爆仓归零
        if equity < 0: equity = 0
            
        row_data.append(round(equity, 2))
        total_equity += equity
        
    # 最后加上总净值
    row_data.append(round(total_equity, 2))
    
    try:
        with open(EQUITY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 如果是新文件，写入表头: Time, S_0, S_1 ... S_23, Total
            if not file_exists:
                headers = ["Time"] + [f"S_{i}" for i in range(24)] + ["Total"]
                writer.writerow(headers)
            
            writer.writerow(row_data)
        print("📈 [图表数据] 已更新净值曲线数据 (equity_curve.csv)")
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
    
    for s_id in data:
        strategy = data[s_id]
        wallet_balance = strategy['balance']
        positions = strategy['positions']
        
        if not positions: continue

        total_unrealized_pnl = 0.0
        details = []

        for pos in positions:
            symbol = pos['symbol']
            entry = pos['entry_price']
            amount = pos['amount']
            
            curr = market_map.get(symbol, entry)
            high_15m = get_recent_high_price(opener, symbol)
            risk_price = max(curr, high_15m) if high_15m > 0 else curr
            
            pnl = (entry - risk_price) * amount
            total_unrealized_pnl += pnl
            
            details.append({
                'symbol': symbol, 'curr': curr, 'high': risk_price, 'amount': amount, 'pnl': pnl
            })

        equity = wallet_balance + total_unrealized_pnl
        
        # 记录监控日志
        for d in details:
            log_to_csv("MONITOR", s_id, d['symbol'], d['curr'], d['high'], d['amount'], d['pnl'], equity, "全仓监控")

        # 爆仓判断
        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['high'], d['high'], d['amount'], d['pnl'], 0, "全仓强平")
            strategy['balance'] = 0
            strategy['positions'] = []

def execute_rotation(opener, data, market_map, top_10):
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    strategy = data[current_hour]
    
    if strategy['last_trade_date'] == today_str:
        return False

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动逻辑...")
    
    # 平旧仓
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    
    if wallet_balance > 0 and strategy['positions']:
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry = pos['entry_price']
            amount = pos['amount']
            
            exit_price = market_map.get(symbol, entry)
            pnl = (entry - exit_price) * amount
            total_close_pnl += pnl
            
            temp_equity = wallet_balance + total_close_pnl
            log_to_csv("CLOSE", current_hour, symbol, exit_price, exit_price, amount, pnl, temp_equity, "轮动平仓")

        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    # 开新仓
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
    return True

def report_to_wechat(opener, data, market_map):
    if not SERVERCHAN_KEY: return
    print("\n📤 正在生成全仓净值报告...")
    
    total_equity = 0
    total_init = 24 * INIT_BALANCE
    max_profit = -999999
    best_strategy = ""
    
    md_table = "| ID | 净值(U) | 盈亏 | 持仓 |\n| :---: | :---: | :---: | :---: |\n"
    detail_text = ""
    
    for i in range(24):
        s_id = str(i)
        info = data[s_id]
        wallet_bal = info['balance']
        positions = info['positions']
        
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
    
    print(f"\n{'='*20} 📢 微信通知内容 (Log) {'='*20}")
    print(f"【标题】: {title}")
    print(f"{'='*55}\n")

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
        
        # 1. 风控检查
        check_risk_management(opener, data, market_map)
        
        # 2. 轮动开仓
        has_rotated = execute_rotation(opener, data, market_map, top_10)
        
        # 3. [新增] 记录净值曲线数据 (无论是否开仓，每15分钟都记录一次，让曲线更平滑)
        record_equity_snapshot(data, market_map)
        
        save_state(data)
        
        if has_rotated:
            report_to_wechat(opener, data, market_map)
