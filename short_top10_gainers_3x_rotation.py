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

# 资金参数
INIT_BALANCE = 1000.0     # 策略总本金
POSITIONS_COUNT = 10      # 仓位数量 (10个)
LEVERAGE = 3.0            # 3倍杠杆
# 单个仓位保证金 = 总资金 / 10 = 100U (动态计算，如果亏损了就是余额/10)

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
    """获取涨幅榜 Top 10"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    data = get_data(opener, url)
    
    if not data: return {}, []
    
    market_map = {}
    rank_list = []
    current_ts = int(time.time() * 1000)
    
    for item in data:
        # 过滤10分钟无成交的僵尸数据
        if current_ts - int(item['closeTime']) > 10 * 60 * 1000:
            continue
        symbol = item['symbol']
        price = float(item['lastPrice'])
        change = float(item['priceChangePercent'])
        market_map[symbol] = price
        rank_list.append({'symbol': symbol, 'change': change, 'price': price})
        
    rank_list.sort(key=lambda x: x['change'], reverse=True)
    return market_map, rank_list[:POSITIONS_COUNT]

def get_recent_high_price(opener, symbol):
    """
    [核心新增] 获取指定币种过去15分钟K线（1根）的最高价
    用于判断是否插针爆仓
    """
    # 获取最近的 15m K线，limit=1
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=15m&limit=1"
    data = get_data(opener, url)
    
    if data and len(data) > 0:
        # K线数据格式: [Open Time, Open, High, Low, Close, ...]
        # 索引 2 是 High Price
        return float(data[0][2])
    return 0.0

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, equity, balance, note=""):
    """
    CSV 字段更新: 增加了 15m_High (15分钟最高价) 和 Equity (当前净值)
    """
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Equity/PnL", "Balance", "Note"])
        writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, equity, balance, note])

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
            
            # 1. 获取当前价
            curr_price = market_map.get(symbol, entry_price)
            
            # 2. [关键] 获取过去15分钟最高价，用于判断插针爆仓
            high_15m = get_recent_high_price(opener, symbol)
            # 如果获取失败或滞后，保底使用当前价
            check_price = max(curr_price, high_15m) if high_15m > 0 else curr_price
            
            # 计算最大亏损 (按最高价算)
            # 做空亏损 = (开仓 - 最高价) * 数量
            max_loss_pnl = (entry_price - check_price) * amount
            
            # 当前实际浮动盈亏 (按当前价算)
            curr_pnl = (entry_price - curr_price) * amount
            equity = margin + curr_pnl # 当前仓位价值

            # 记录 CSV (类型 MONITOR)
            log_to_csv("MONITOR", s_id, symbol, curr_price, check_price, amount, f"{equity:.2f}", strategy['balance'], "监控")

            # 3. 爆仓判断 (使用 check_price 判定是否曾达到爆仓线)
            # 如果亏损超过保证金 (max_loss_pnl <= -margin)
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
        
        # 记录平仓: 15m_high 暂填 exit_price
        log_to_csv("CLOSE", current_hour, symbol, exit_price, exit_price, amount, f"{pnl:.2f}", strategy['balance'] + pnl, "轮动平仓")

    strategy['balance'] += total_pnl
    strategy['positions'] = [] 
    
    # 2. 开新仓
    current_balance = strategy['balance']
    if current_balance < 100: # 余额太少就不开了
        log_to_csv("SKIP", current_hour, "ALL", 0, 0, 0, 0, current_balance, "余额不足")
    else:
        # 按照要求: 将当前余额分成 10 份
        margin_per_coin = current_balance / POSITIONS_COUNT
        
        top10_str = "|".join([x['symbol'] for x in top_10])
        log_to_csv("INFO", current_hour, "TOP10_LIST", 0, 0, 0, 0, current_balance, top10_str)

        new_positions = []
        for item in top_10:
            symbol = item['symbol']
            price = item['price']
            # 数量 = (保证金 * 杠杆) / 价格
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

    print("📤 正在生成详细报告...")
    
    total_balance = 0
    total_init = 24 * INIT_BALANCE
    max_profit = -999999
    best_strategy = ""
    
    # 表格头: ID | 余额 | 持仓数
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
            # 表头
            detail_text += f"`{'币种':<6} {'开仓价':<8} {'现价':<8} {'15m高':<8} {'净值':<8}`\n"
            
            for pos in info['positions']:
                symbol = pos['symbol']
                entry = pos['entry_price']
                amount = pos['amount']
                margin = pos['margin']
                
                curr = market_map.get(symbol, entry)
                # 获取15分钟最高价
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m == 0: high_15m = curr
                
                # 计算净值
                unrealized_pnl = (entry - curr) * amount
                equity = margin + unrealized_pnl
                
                # 预警标记
                warn = "⚠️" if high_15m > entry * 1.05 else "" # 如果最近涨了5%标记一下
                
                # 格式化输出 (为了手机阅读，尽量紧凑)
                # Symbol | Open | Curr | High | Equity
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
    
    # 1. 获取基础行情 (Top10 和所有现价)
    market_map, top_10 = get_market_rank(opener)
    
    if market_map:
        data = load_state()
        
        # 2. 风控检查 (传入 opener 以便获取 15m 高点)
        check_risk_management(opener, data, market_map)
        
        # 3. 轮动
        has_rotated = execute_rotation(opener, data, market_map, top_10)
        
        save_state(data)
        
        # 4. 只有轮动后才发报告
        if has_rotated:
            report_to_wechat(opener, data, market_map)
