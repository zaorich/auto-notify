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
POSITIONS_COUNT = 10      # 仓位数量 (10个)
LEVERAGE = 3.0            # 3倍杠杆
# 爆仓阈值 (做空: 涨幅 >= 33.33%)
LIQUIDATION_THRESHOLD = 1 / LEVERAGE 

HEADERS = {'User-Agent': 'Mozilla/5.0'}
SERVERCHAN_KEY = os.environ.get("SERVERCHAN_KEY")

def get_proxy_opener():
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_data(opener, url):
    """通用请求函数"""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with opener.open(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"❌ 请求失败 [{url}]: {e}")
        return None

def get_market_rank(opener):
    """获取涨幅榜 Top 10 (含正则过滤和时间过滤)"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    data = get_data(opener, url)
    
    if not data: return {}, []
    
    market_map = {}
    rank_list = []
    current_ts = int(time.time() * 1000)
    
    # 正则表达式：只允许由大写字母和数字组成的交易对
    valid_symbol_pattern = re.compile(r'^[A-Z0-9]+$')
    
    for item in data:
        symbol = item['symbol']
        
        # 1. 过滤非法字符
        if not valid_symbol_pattern.match(symbol):
            continue

        # 2. 过滤10分钟无成交的僵尸数据
        if current_ts - int(item['closeTime']) > 10 * 60 * 1000:
            continue
            
        price = float(item['lastPrice'])
        change = float(item['priceChangePercent'])
        market_map[symbol] = price
        rank_list.append({'symbol': symbol, 'change': change, 'price': price})
        
    rank_list.sort(key=lambda x: x['change'], reverse=True)
    return market_map, rank_list[:POSITIONS_COUNT]

def get_recent_high_price(opener, symbol):
    """获取过去15分钟K线最高价 (含URL编码修复)"""
    safe_symbol = urllib.parse.quote(symbol)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={safe_symbol}&interval=15m&limit=1"
    
    data = get_data(opener, url)
    
    if data and len(data) > 0:
        return float(data[0][2])
    return 0.0

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, equity, balance, note=""):
    """
    CSV 记录函数 (同时打印到控制台)
    """
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # 1. 打印到日志
    print(f"📝 [CSV] {record_type:<10} 策略{strategy_id:<2} {symbol:<8} 价格:{price:<8g} 净值:{equity} 备注:{note}")

    # 2. 写入文件
    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Equity/PnL", "Balance", "Note"])
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, equity, balance, note])
    except Exception as e:
        print(f"❌ 写入CSV失败: {e}")

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
    print("\n🛡️ [监控] 开始风控检查 (含插针检测)...")
    
    for s_id in data:
        strategy = data[s_id]
        active_positions = []
        positions_changed = False
        
        if not strategy['positions']: continue
            
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry_price = pos['entry_price']
            amount = pos['amount']
            margin = pos['margin']
            
            # 获取价格数据
            curr_price = market_map.get(symbol, entry_price)
            high_15m = get_recent_high_price(opener, symbol)
            check_price = max(curr_price, high_15m) if high_15m > 0 else curr_price
            
            # 计算盈亏
            max_loss_pnl = (entry_price - check_price) * amount
            curr_pnl = (entry_price - curr_price) * amount
            equity = margin + curr_pnl 

            # 记录 CSV
            log_to_csv("MONITOR", s_id, symbol, curr_price, check_price, amount, f"{equity:.2f}", strategy['balance'], "监控")

            # 爆仓判断
            if max_loss_pnl <= -margin:
                print(f"    💥 策略{s_id} {symbol} 触发爆仓! (15m最高: {check_price})")
                log_to_csv("LIQUIDATION", s_id, symbol, check_price, check_price, amount, 0, strategy['balance'] - margin, "15m插针爆仓")
                strategy['balance'] -= margin
                positions_changed = True
            else:
                active_positions.append(pos)
        
        if positions_changed:
            strategy['positions'] = active_positions

def execute_rotation(opener, data, market_map, top_10):
    """整点轮动逻辑"""
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    strategy = data[current_hour]
    
    if strategy['last_trade_date'] == today_str:
        return False

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动逻辑...")
    
    # 1. 平旧仓
    total_pnl = 0
    for pos in strategy['positions']:
        symbol = pos['symbol']
        entry_price = pos['entry_price']
        amount = pos['amount']
        
        exit_price = market_map.get(symbol, entry_price)
        pnl = (entry_price - exit_price) * amount
        total_pnl += pnl
        
        log_to_csv("CLOSE", current_hour, symbol, exit_price, exit_price, amount, f"{pnl:.2f}", strategy['balance'] + pnl, "轮动平仓")

    strategy['balance'] += total_pnl
    strategy['positions'] = [] 
    
    # 2. 开新仓
    current_balance = strategy['balance']
    
    if current_balance < 100: 
        log_to_csv("SKIP", current_hour, "ALL", 0, 0, 0, 0, current_balance, "余额不足")
    else:
        margin_per_coin = current_balance / POSITIONS_COUNT
        top10_str = "|".join([x['symbol'] for x in top_10])
        log_to_csv("INFO", current_hour, "TOP10_LIST", 0, 0, 0, 0, current_balance, top10_str)

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
            log_to_csv("OPEN", current_hour, symbol, price, price, amount, margin_per_coin, current_balance, "开空")
            
        strategy['positions'] = new_positions

    strategy['last_trade_date'] = today_str
    return True

def report_to_wechat(opener, data, market_map):
    if not SERVERCHAN_KEY: return

    print("\n📤 正在生成详细报告...")
    
    total_balance = 0
    total_init = 24 * INIT_BALANCE
    max_profit = -999999
    best_strategy = ""
    
    md_table = "| ID | 余额 | 盈亏 | 持仓 |\n| :---: | :---: | :---: | :---: |\n"
    detail_text = ""
    
    for i in range(24):
        s_id = str(i)
        info = data[s_id]
        bal = info['balance']
        pnl = bal - INIT_BALANCE
        pos_count = len(info['positions'])
        total_balance += bal
        
        if pnl > max_profit:
            max_profit = pnl
            best_strategy = f"策略{s_id}"

        icon = "🔴" if pnl < 0 else "🟢"
        md_table += f"| {s_id} | {bal:.0f} | {icon}{pnl:+.0f} | {pos_count} |\n"

        if pos_count > 0:
            detail_text += f"\n🔷 **策略 {s_id} 详情** (余额:{bal:.1f}U):\n"
            detail_text += f"`{'币种':<6} {'开仓价':<8} {'现价':<8} {'15m高':<8} {'净值':<8}`\n"
            
            for pos in info['positions']:
                symbol = pos['symbol']
                entry = pos['entry_price']
                amount = pos['amount']
                margin = pos['margin']
                
                curr = market_map.get(symbol, entry)
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m == 0: high_15m = curr
                
                unrealized_pnl = (entry - curr) * amount
                equity = margin + unrealized_pnl
                
                warn = "⚠️" if high_15m > entry * 1.05 else ""
                
                detail_text += f"- `{symbol:<6} {entry:<8g} {curr:<8g} {high_15m:<8g} {equity:>6.1f}U {warn}`\n"

    total_pnl = total_balance - total_init
    total_pnl_pct = (total_pnl / total_init) * 100

    current_utc = datetime.utcnow().strftime("%H:%M")
    title = f"策略日报: 总盈亏 {total_pnl:+.1f}U"
    
    description = f"""
**UTC 时间**: {current_utc}
**总资金**: {total_balance:.1f} U ({total_pnl_pct:+.2f}%)
**最佳**: {best_strategy} ({max_profit:+.1f} U)

---
{md_table}
---
### 📝 持仓详细监控
{detail_text}
    """

    # --- 这里增加了打印，方便在 GitHub Actions 日志里直接看 ---
    print(f"\n{'='*20} 📢 微信通知预览 {'='*20}")
    print(f"【标题】: {title}")
    print(f"【正文】:\n{description}")
    print(f"{'='*55}\n")

    # 发送请求
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
