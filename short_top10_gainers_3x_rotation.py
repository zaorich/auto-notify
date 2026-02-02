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

# 基础参数
INITIAL_UNIT = 1000.0     # 标准开仓/复活金额
POSITIONS_COUNT = 10      # 持仓数量
LEVERAGE = 3.0            # 杠杆倍数
MIN_ALIVE_BALANCE = 10.0  # “存活”阈值：低于10U视为无法开单，强制复活

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
    """计算策略当前净值"""
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

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, pos_pnl, equity, total_invested, note=""):
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # 净盈亏 = 当前净值 - 总投入
    equity_val = float(equity)
    invested_val = float(total_invested)
    
    print(f"📝 [CSV] {record_type:<10} 策略{strategy_id:<2} {symbol:<8} 净值:{equity_val:.0f} 投入:{invested_val:.0f} | {note}")

    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", "Note"])
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, pos_pnl, equity_val, invested_val, note])
    except Exception as e:
        print(f"❌ 写入历史CSV失败: {e}")

def record_equity_snapshot(data, market_map):
    file_exists = os.path.isfile(EQUITY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    row_data = [current_time]
    
    total_equity = 0.0
    total_invested_all = 0.0
    
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        eq, _ = calculate_strategy_equity(strat, market_map, opener=None, use_high_price=False)
        if eq < 0: eq = 0
        
        row_data.append(round(eq, 2))
        
        total_equity += eq
        total_invested_all += strat.get('total_invested', INITIAL_UNIT)
        
    row_data.append(round(total_equity, 2))
    row_data.append(round(total_invested_all, 2))
    
    try:
        with open(EQUITY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                headers = ["Time"] + [f"S_{i}" for i in range(24)] + ["Total_Equity", "Total_Invested"]
                writer.writerow(headers)
            writer.writerow(row_data)
    except Exception as e:
        print(f"❌ 写入净值CSV失败: {e}")

def load_state():
    if not os.path.exists(STATE_FILE):
        data = {}
        for i in range(24):
            data[str(i)] = {
                "balance": INITIAL_UNIT,
                "positions": [],
                "last_trade_date": "",
                "total_invested": INITIAL_UNIT,
                "liquidation_count": 0
            }
        return data
        
    with open(STATE_FILE, 'r') as f:
        data = json.load(f)
        
    for k, v in data.items():
        if "total_invested" not in v:
            v["total_invested"] = INITIAL_UNIT
        if "liquidation_count" not in v:
            v["liquidation_count"] = 0
            
    return data

def save_state(data):
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def check_risk_management(opener, data, market_map):
    print("\n🛡️ [监控] 开始全仓风控检查 (含插针检测)...")
    liquidated_ids = [] 
    
    for s_id in data:
        strategy = data[s_id]
        # 如果已经没钱了且没仓位，说明已经死透了等待复活，跳过检查
        if strategy['balance'] <= 0 and not strategy['positions']:
            continue
            
        equity, details = calculate_strategy_equity(strategy, market_map, opener, use_high_price=True)
        invested = strategy.get('total_invested', INITIAL_UNIT)

        for d in details:
            log_to_csv("MONITOR", s_id, d['symbol'], d['curr'], d['calc_price'], d['amount'], d['pnl'], equity, invested, "全仓监控")

        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            liquidated_ids.append(s_id)
            
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, invested, "全仓强平")
            
            strategy['balance'] = 0
            strategy['positions'] = []
            strategy['liquidation_count'] = strategy.get('liquidation_count', 0) + 1
            # 注意：这里不补钱！必须等到 execute_rotation 才会补
            
    return liquidated_ids

def execute_rotation(opener, data, market_map, top_10):
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    strategy = data[current_hour]
    
    if strategy['last_trade_date'] == today_str:
        return None

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动逻辑...")
    
    # 1. 先平掉旧仓位 (如果有)
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    invested = strategy['total_invested']
    
    if wallet_balance > 0 and strategy['positions']:
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry = float(pos['entry_price'])
            amount = float(pos['amount'])
            
            exit_price = market_map.get(symbol, entry)
            pnl = (entry - exit_price) * amount
            total_close_pnl += pnl
            
            temp_equity = wallet_balance + total_close_pnl
            log_to_csv("CLOSE", current_hour, symbol, exit_price, exit_price, amount, pnl, temp_equity, invested, "轮动平仓")

        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    # 2. 判断是否需要“复活” (核心逻辑修改)
    # 只有当余额低于 MIN_ALIVE_BALANCE (10U) 时，才认为是爆仓/死亡，需要补充 1000U
    # 否则，剩下多少钱就用多少钱开仓，绝不追加
    current_equity = strategy['balance']
    
    if current_equity < MIN_ALIVE_BALANCE:
        print(f"💀 策略 {current_hour} 已归零 (余额 {current_equity:.1f}U < {MIN_ALIVE_BALANCE}U)，执行复活程序...")
        
        # 强制重置为0 (消除可能的微小负数或零头)，然后加1000
        strategy['balance'] = INITIAL_UNIT
        # 只有在这里，才增加总投入
        strategy['total_invested'] += INITIAL_UNIT
        
        current_equity = strategy['balance'] # 更新当前可用资金
        log_to_csv("REPLENISH", current_hour, "USDT", 0, 0, 0, 0, current_equity, strategy['total_invested'], "爆仓后重新投入")
    else:
        print(f"🔋 策略 {current_hour} 存活 (余额 {current_equity:.1f}U)，使用剩余资金开仓，不追加投入。")
        # 记录一下，虽然没操作资金，但确认了不追加
        # log_to_csv("ALIVE", current_hour, "USDT", 0, 0, 0, 0, current_equity, strategy['total_invested'], "存活继续")

    # 3. 开新仓
    margin_per_coin = current_equity / POSITIONS_COUNT
    
    # 如果资金太少(比如只有20U，分成10份是2U)，可能开不出来，这里加个简单判断防止报错
    if margin_per_coin < 1.0:
        log_to_csv("SKIP", current_hour, "ALL", 0, 0, 0, 0, current_equity, strategy['total_invested'], "余额过小无法开仓")
    else:
        top10_str = "|".join([x['symbol'] for x in top_10])
        log_to_csv("INFO", current_hour, "TOP10_LIST", 0, 0, 0, 0, current_equity, strategy['total_invested'], top10_str)

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
            log_to_csv("OPEN", current_hour, symbol, price, price, amount, 0, current_equity, strategy['total_invested'], "开空")
            
        strategy['positions'] = new_positions

    strategy['last_trade_date'] = today_str
    return current_hour

def report_to_wechat(opener, data, market_map, rotated_id, liquidated_ids):
    if not SERVERCHAN_KEY: 
        print("⚠️ 未配置 SERVERCHAN_KEY，跳过通知")
        return
        
    print("\n📤 正在生成详细报告...")
    
    total_equity = 0
    total_invested_all = 0
    total_liquidations = 0
    
    max_profit = -999999
    best_strategy = ""
    
    md_table = "| ID | 投入 | 净值 | 盈亏 | 爆 |\n| :--: | :--: | :--: | :--: | :--: |\n"
    detail_text = ""
    
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        
        invested = strat.get('total_invested', INITIAL_UNIT)
        liq_count = strat.get('liquidation_count', 0)
        
        equity, details = calculate_strategy_equity(strat, market_map, opener, use_high_price=False)
        
        total_equity += equity
        total_invested_all += invested
        total_liquidations += liq_count
        
        net_pnl = equity - invested
        
        if net_pnl > max_profit:
            max_profit = net_pnl
            best_strategy = f"S{s_id}"

        icon = "🔴" if net_pnl < 0 else "🟢"
        if equity == 0: icon = "💀" 
        elif s_id == rotated_id: icon = "🔄"
        
        liq_str = str(liq_count) if liq_count > 0 else "-"
        md_table += f"| {s_id} | {invested:.0f} | {equity:.0f} | {icon}{net_pnl:+.0f} | {liq_str} |\n"

        pos_len = len(strat['positions'])
        if pos_len > 0:
            prefix = "🔄" if s_id == rotated_id else ""
            detail_text += f"\n🔷 **{prefix}S{s_id}** (投:{invested:.0f} 净:{equity:.0f} 亏:{net_pnl:+.0f}):\n"
            
            simple_items = []
            for d in details:
                warn_mark = "⚠️" if d.get('warn') else ""
                short_symbol = d['symbol'].replace("USDT", "")
                item_str = f"{short_symbol}({d['pnl']:+.1f}){warn_mark}"
                simple_items.append(item_str)
            detail_text += ", ".join(simple_items) + "\n"
        elif equity == 0:
             detail_text += f"\n💀 **S{s_id}** (待复活): 累计爆仓 {liq_count} 次，总亏损 {net_pnl:.0f}U\n"

    total_pnl = total_equity - total_invested_all
    total_pnl_pct = (total_pnl / total_invested_all) * 100 if total_invested_all > 0 else 0
    current_utc = datetime.utcnow().strftime("%H:%M")

    title_parts = []
    if rotated_id: title_parts.append(f"🔄S{rotated_id}")
    if liquidated_ids: title_parts.append(f"💥{len(liquidated_ids)}个")
    
    title_base = f"投{total_invested_all:.0f} 剩{total_equity:.0f} ({total_pnl_pct:+.1f}%)"
    
    if title_parts:
        title = f"{' '.join(title_parts)} | {title_base}"
    else:
        title = f"策略日报: {title_base}"
    
    description = f"""
**UTC 时间**: {current_utc}
**总投入**: {total_invested_all:.0f} U
**总净值**: {total_equity:.0f} U
**总盈亏**: {total_pnl:+.1f} U
**总爆仓**: {total_liquidations} 次

---
{md_table}
---
### 📝 持仓概览
{detail_text}
    """
    
    print(f"\n{'='*20} 📢 微信通知预览 {'='*20}")
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
        
        liquidated_ids = check_risk_management(opener, data, market_map)
        rotated_id = execute_rotation(opener, data, market_map, top_10)
        
        record_equity_snapshot(data, market_map)
        save_state(data)
        
        if rotated_id or liquidated_ids:
            report_to_wechat(opener, data, market_map, rotated_id, liquidated_ids)
