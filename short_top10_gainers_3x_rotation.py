import urllib.request
import urllib.parse
import json
import time
import os
import csv
# import re
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
        # 移除正则过滤，接收所有币种
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

# --- [新增] 核心计算函数 (统一口径) ---
def calculate_strategy_equity(strategy, market_map, opener=None, use_high_price=False):
    """
    计算单个策略的当前动态净值
    :param use_high_price: True则使用15m最高价(用于风控), False则使用现价(用于报表)
    :return: (equity, details_list)
    """
    wallet_balance = strategy['balance']
    positions = strategy['positions']
    
    total_unrealized_pnl = 0.0
    details = []
    
    if positions:
        for pos in positions:
            symbol = pos['symbol']
            entry = float(pos['entry_price'])
            amount = float(pos['amount'])
            
            # 获取价格
            curr = market_map.get(symbol, entry)
            
            calc_price = curr
            warn_msg = ""
            
            # 如果需要插针检测 (Opener 不为空且指定了 use_high_price)
            if opener and use_high_price:
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m > 0:
                    calc_price = max(curr, high_15m)
                    if high_15m > entry * 1.05: warn_msg = "⚠️"

            # 这里的 calc_price: 报表时是现价，风控时是插针价
            # 做空盈亏 = (开仓 - 结算) * 数量
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
        # 使用统一函数计算 (False表示使用现价画图)
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
    
    for s_id in data:
        strategy = data[s_id]
        if not strategy['positions']: continue

        # 调用核心计算函数 (use_high_price=True, 开启插针检测)
        equity, details = calculate_strategy_equity(strategy, market_map, opener, use_high_price=True)
        
        # 记录监控日志
        for d in details:
            log_to_csv("MONITOR", s_id, d['symbol'], d['curr'], d['calc_price'], d['amount'], d['pnl'], equity, "全仓监控")

        # 爆仓判断
        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, "全仓强平")
            strategy['balance'] = 0
            strategy['positions'] = []

def execute_rotation(opener, data, market_map, top_10):
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    strategy = data[current_hour]
    
    if strategy['last_trade_date'] == today_str:
        return False

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动逻辑...")
    
    # 1. 平仓逻辑
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
            
            # 临时净值用于记录
            temp_equity = wallet_balance + total_close_pnl
            log_to_csv("CLOSE", current_hour, symbol, exit_price, exit_price, amount, pnl, temp_equity, "轮动平仓")

        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    # 2. 开仓逻辑
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
    if not SERVERCHAN_KEY: 
        print("⚠️ 未配置 SERVERCHAN_KEY，跳过通知")
        return
        
    print("\n📤 正在生成全仓净值报告 (使用统一计算函数)...")
    
    total_equity = 0
    total_init = 24 * INIT_BALANCE
    max_profit = -999999
    best_strategy = ""
    
    md_table = "| ID | 净值(U) | 盈亏 | 持仓 |\n| :---: | :---: | :---: | :---: |\n"
    detail_text = ""
    
    for i in range(24):
        s_id = str(i)
        
        # [关键] 调用同一个计算函数，使用现价 (use_high_price=False)
        # 这样能保证和 CSV 里的逻辑、数据源完全一致
        equity, details = calculate_strategy_equity(data[s_id], market_map, opener, use_high_price=False)
        
        # 累加总净值
        total_equity += equity
        net_pnl = equity - INIT_BALANCE
        
        if net_pnl > max_profit:
            max_profit = net_pnl
            best_strategy = f"策略{s_id}"

        icon = "🔴" if net_pnl < 0 else "🟢"
        pos_len = len(data[s_id]['positions'])
        md_table += f"| {s_id} | {equity:.0f} | {icon}{net_pnl:+.0f} | {pos_len} |\n"

        if pos_len > 0:
            detail_text += f"\n🔷 **策略 {s_id} 全仓详情** (净值:{equity:.1f}U):\n"
            for d in details:
                # 打印详细 debug 信息到控制台，方便你核对
                print(f"   Debug {s_id}: {d['symbol']} 开:{d['entry']} 现:{d['curr']} 量:{d['amount']:.4f} PnL:{d['pnl']:.2f}")
                
                # 微信消息格式
                warn = d.get('warn', '')
                detail_text += f"- `{d['symbol']:<6} 开:{d['entry']:<8g} 现:{d['curr']:<8g} 盈亏:{d['pnl']:+.1f}U {warn}`\n"

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
    
    print(f"\n{'='*20} 📢 微信通知内容 {'='*20}")
    print(f"标题: {title}")
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
        
        # 1. 风控 (使用 High Price 检测)
        check_risk_management(opener, data, market_map)
        
        # 2. 轮动
        has_rotated = execute_rotation(opener, data, market_map, top_10)
        
        # 3. 记录净值曲线 (使用 Current Price)
        record_equity_snapshot(data, market_map)
        
        save_state(data)
        
        if has_rotated:
            report_to_wechat(opener, data, market_map)
