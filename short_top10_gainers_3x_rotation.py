import urllib.request
import urllib.parse
import json
import time
import os
import csv
from datetime import datetime, timedelta

# ==========================================
#               策略核心配置
# ==========================================
PROXY_ADDR = "127.0.0.1:10808"
STATE_FILE = "strategy_state.json"
HISTORY_FILE = "strategy_history.csv"
SNAPSHOT_FILE = "positions_snapshot.csv"
EQUITY_FILE = "equity_curve.csv"

# --- [新功能开关] ---
ENABLE_COMPOUNDING = True  # 复利开关
ENABLE_ROI_PAYBACK = True  # 回本提取开关

# --- [资金参数: S0-S23 轮动做空] ---
INITIAL_UNIT = 1000.0     # 标准开仓/复活金额
POSITIONS_COUNT = 10      # 持仓数量
LEVERAGE = 3.0            # 杠杆倍数
MIN_ALIVE_BALANCE = 10.0  # “存活”阈值
MAX_DELAY_SECONDS = 3600  # 最大延迟容忍时间(秒)

# --- [新增: S_CHASE 追涨做多] ---
CHASE_STRAT_ID = "S_CHASE"
CHASE_MARGIN = 100.0      # 固定保证金(U)
CHASE_LEVERAGE = 3.0      # 杠杆倍数
CHASE_HOLD_HOURS = 11     # 持仓时间(小时)

HEADERS = {'User-Agent': 'Mozilla/5.0'}
SERVERCHAN_KEY = os.environ.get("SERVERCHAN_KEY")

# ==========================================
#               网络与基础函数
# ==========================================

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
    wallet_balance = strategy['balance']
    positions = strategy['positions']
    
    total_unrealized_pnl = 0.0
    details = []
    
    if positions:
        for pos in positions:
            symbol = pos['symbol']
            entry = float(pos['entry_price'])
            amount = float(pos['amount'])
            
            # 判断方向 (默认为SHORT, 兼容旧数据)
            side = pos.get('side', 'SHORT')
            
            curr = market_map.get(symbol, entry)
            calc_price = curr
            warn_msg = ""
            
            if opener and use_high_price:
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m > 0:
                    calc_price = max(curr, high_15m)
                    if high_15m > entry * 1.05: warn_msg = "⚠️"

            # 计算盈亏
            if side == 'LONG':
                # 做多: (现价 - 开仓) * 数量
                pnl = (curr - entry) * amount
            else:
                # 做空: (开仓 - 现价) * 数量
                pnl = (entry - calc_price) * amount
                
            total_unrealized_pnl += pnl
            
            details.append({
                'symbol': symbol,
                'entry': entry,
                'curr': curr,
                'calc_price': calc_price,
                'amount': amount,
                'pnl': pnl,
                'warn': warn_msg,
                'side': side
            })
            
    equity = wallet_balance + total_unrealized_pnl
    return equity, details

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, pos_pnl, equity, total_invested, used_margin, round_pnl, change_pct=0.0, note=""):
    """
    日志记录函数
    """
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    equity_val = float(equity)
    invested_val = float(total_invested)
    used_margin_val = float(used_margin)
    round_pnl_val = float(round_pnl)
    change_pct_val = float(change_pct)
    
    # === [关键过滤逻辑] ===
    # 增加新的类型 OPEN_LONG, CLOSE_LONG
    CRITICAL_EVENTS = ["OPEN", "CLOSE", "OPEN_LONG", "CLOSE_LONG", "LIQUIDATION", "REPLENISH", "WITHDRAW", "ROUND_RES", "SNAPSHOT"]
    
    if record_type not in CRITICAL_EVENTS:
        return 

    change_str = ""
    if "OPEN" in record_type:
        change_str = f"涨:{change_pct_val:>+5.1f}%"
    
    # 控制台打印
    print(f"📝 [CSV] {record_type:<10} {strategy_id:<3} {symbol:<8} 净:{equity_val:.0f} 投:{invested_val:.0f} 轮:{round_pnl_val:+.0f} {change_str} | {note}")

    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", "Used_Margin", "Round_PnL", "24h_Change", "Note"])
            
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, pos_pnl, equity_val, invested_val, used_margin_val, round_pnl_val, change_pct_val, note])
    except Exception as e:
        print(f"❌ 写入历史CSV失败: {e}")

def record_positions_snapshot(data, market_map):
    """
    记录当前所有持仓的快照 (S0-S23 + S_CHASE)
    """
    file_exists = os.path.isfile(SNAPSHOT_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        with open(SNAPSHOT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Symbol", "Entry_Price", "Current_Price", "Max_Price", "Min_Price", "Amount", "Unrealized_PnL"])
            
            # 遍历所有策略
            for s_id, strat in data.items():
                if not strat.get('positions'): continue
                
                for pos in strat['positions']:
                    symbol = pos['symbol']
                    entry = float(pos['entry_price'])
                    amount = float(pos['amount'])
                    side = pos.get('side', 'SHORT')
                    
                    curr = float(market_map.get(symbol, entry))
                    max_p = float(pos.get('max_price', entry))
                    min_p = float(pos.get('min_price', entry))
                    
                    if side == 'LONG':
                        pnl = (curr - entry) * amount
                    else:
                        pnl = (entry - curr) * amount
                    
                    writer.writerow([current_time, s_id, symbol, entry, curr, max_p, min_p, amount, pnl])
    except Exception as e:
        print(f"❌ 写入快照失败: {e}")

def record_equity_snapshot(data, market_map):
    file_exists = os.path.isfile(EQUITY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    row_data = [current_time]
    total_equity = 0.0
    total_invested_all = 0.0
    
    # 1. 记录 S0-S23 (保持原格式列)
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        eq, _ = calculate_strategy_equity(strat, market_map, opener=None, use_high_price=False)
        if eq < 0: eq = 0
        
        row_data.append(round(eq, 2))
        total_equity += eq
        total_invested_all += strat.get('total_invested', INITIAL_UNIT)
    
    # 2. 将 S_CHASE 的资金加到总计中 (但不新增列，以免破坏分析脚本)
    if CHASE_STRAT_ID in data:
        c_strat = data[CHASE_STRAT_ID]
        c_eq, _ = calculate_strategy_equity(c_strat, market_map)
        total_equity += c_eq
        total_invested_all += c_strat.get('total_invested', 0)

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
        
    # 初始化 S_CHASE 结构 (如果不存在)
    if CHASE_STRAT_ID not in data:
        data[CHASE_STRAT_ID] = {
            "balance": 1000.0, # 初始资金池
            "positions": [],
            "prev_top10": [],  # 记录上一次 Top10
            "total_invested": 1000.0,
            "liquidation_count": 0
        }
    if "prev_top10" not in data[CHASE_STRAT_ID]:
        data[CHASE_STRAT_ID]["prev_top10"] = []
        
    for k, v in data.items():
        if k == CHASE_STRAT_ID: continue
        if "total_invested" not in v: v["total_invested"] = INITIAL_UNIT
        if "liquidation_count" not in v: v["liquidation_count"] = 0
    return data

def save_state(data):
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def update_price_stats(data, market_map):
    for s_id, strategy in data.items():
        if not strategy.get('positions'): continue
        for pos in strategy['positions']:
            symbol = pos['symbol']
            if symbol in market_map:
                curr_price = float(market_map[symbol])
                if 'max_price' not in pos: pos['max_price'] = float(pos['entry_price'])
                if 'min_price' not in pos: pos['min_price'] = float(pos['entry_price'])
                if curr_price > pos['max_price']: pos['max_price'] = curr_price
                if curr_price < pos['min_price']: pos['min_price'] = curr_price

# ==========================================
#               核心逻辑函数
# ==========================================

def check_risk_management(opener, data, market_map):
    print("\n🛡️ [监控] 开始全仓风控检查...")
    liquidated_ids = [] 
    
    for s_id in data:
        strategy = data[s_id]
        if strategy.get('balance', 0) <= 0 and not strategy.get('positions'): continue
            
        equity, details = calculate_strategy_equity(strategy, market_map, opener, use_high_price=True)
        invested = strategy.get('total_invested', INITIAL_UNIT)
        
        used_margin = sum([p.get('margin', 0) for p in strategy['positions']])
        round_pnl = equity - strategy['balance'] 
        
        if details:
            coin_details_list = []
            for d in details:
                side_icon = "🟢" if d['side'] == 'LONG' else "🔴"
                short_symbol = d['symbol'].replace("USDT", "")
                warn = "!" if d.get('warn') else ""
                coin_str = f"{side_icon}{short_symbol}({d['pnl']:+.0f}){warn}"
                coin_details_list.append(coin_str)
            
            pnl = equity - invested
            print(f"   >> {s_id:<5} 净:{equity:>5.0f} ({pnl:>+5.0f}) 押:{used_margin:>4.0f} 轮:{round_pnl:>+5.0f} | {' '.join(coin_details_list)}")
        
        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            liquidated_ids.append(s_id)
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, invested, used_margin, -used_margin, 0.0, "全仓强平")
            
            strategy['balance'] = 0
            strategy['positions'] = []
            strategy['liquidation_count'] = strategy.get('liquidation_count', 0) + 1
            
    return liquidated_ids

def execute_single_strategy(s_id, strategy, opener, market_map, top_10, current_utc, target_date_str, is_late_close_only, delay_str):
    print(f"\n⚡ [操作] 策略 {s_id} (延迟模式: {'是' if is_late_close_only else '否'}, 时长: {delay_str})")
    
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    invested = strategy['total_invested']
    current_ts = int(time.time())
    
    # 1. 平旧仓
    if wallet_balance > 0 and strategy['positions']:
        used_margin = sum([p.get('margin', 0) for p in strategy['positions']])
        duration_hours = 0.0
        if strategy['positions']:
            entry_time = strategy['positions'][0].get('entry_time', 0)
            if entry_time > 0:
                duration_hours = (current_ts - entry_time) / 3600.0

        close_note = f"轮动平仓(延{delay_str})" if delay_str != "0.0h" else "轮动平仓"
            
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry = float(pos['entry_price'])
            amount = float(pos['amount'])
            
            exit_price = market_map.get(symbol, entry)
            # 获取极值记录
            max_p = pos.get('max_price', entry)
            min_p = pos.get('min_price', entry)
            if exit_price > max_p: max_p = exit_price
            if exit_price < min_p: min_p = exit_price

            pnl = (entry - exit_price) * amount
            total_close_pnl += pnl
            temp_equity = wallet_balance + total_close_pnl
            
            note_str = f"{close_note} | Max:{max_p:.4g} Min:{min_p:.4g}"
            log_to_csv("CLOSE", s_id, symbol, exit_price, exit_price, amount, pnl, temp_equity, invested, used_margin, pnl, 0.0, note_str)

        roi_pct = (total_close_pnl / used_margin * 100) if used_margin > 0 else 0
        summary_note = f"本轮结算: 利润{total_close_pnl:+.1f}U, ROI:{roi_pct:+.1f}%, 持仓{duration_hours:.1f}h"
        log_to_csv("ROUND_RES", s_id, "ALL", 0, 0, 0, total_close_pnl, wallet_balance + total_close_pnl, invested, used_margin, total_close_pnl, 0.0, summary_note)

        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    current_equity = strategy['balance']
    
    if is_late_close_only:
        strategy['last_trade_date'] = target_date_str
        print(f"🚫 策略 {s_id} 延迟 {delay_str} (>1h)，仅执行平仓。")
        return "CLOSED_ONLY"

    # 复活与回本
    if current_equity < MIN_ALIVE_BALANCE:
        print(f"💀 策略 {s_id} 已归零，执行复活程序...")
        strategy['balance'] = INITIAL_UNIT
        strategy['total_invested'] += INITIAL_UNIT
        current_equity = strategy['balance']
        log_to_csv("REPLENISH", s_id, "USDT", 0, 0, 0, 0, current_equity, strategy['total_invested'], 0, 0, 0.0, "爆仓复活")
    elif ENABLE_ROI_PAYBACK and current_equity >= (INITIAL_UNIT * 2):
        withdraw_amount = INITIAL_UNIT
        strategy['balance'] -= withdraw_amount
        strategy['total_invested'] -= withdraw_amount
        print(f"💰 策略 {s_id} 触发回本: 提取 {withdraw_amount}U")
        log_to_csv("WITHDRAW", s_id, "USDT", 0, 0, 0, 0, strategy['balance'], strategy['total_invested'], 0, 0, 0.0, "回本提取")
        current_equity = strategy['balance'] 

    # 5. 开新仓
    trading_capital = current_equity
    if not ENABLE_COMPOUNDING:
        if trading_capital > INITIAL_UNIT: trading_capital = INITIAL_UNIT
    
    if trading_capital < 1.0: 
        log_to_csv("SKIP", s_id, "ALL", 0, 0, 0, 0, current_equity, strategy['total_invested'], 0, 0, 0.0, "资金不足")
    else:
        margin_per_coin = trading_capital / POSITIONS_COUNT
        entry_ts = int(time.time())
        total_used_margin = trading_capital
        
        new_positions = []
        for item in top_10:
            symbol = item['symbol']
            price = item['price']
            amount = (margin_per_coin * LEVERAGE) / price
            change_pct = item.get('change', 0.0)
            
            new_positions.append({
                "symbol": symbol, "entry_price": price, "margin": margin_per_coin, "amount": amount,
                "leverage": LEVERAGE, "entry_time": entry_ts,
                "max_price": price, "min_price": price
            })
            log_to_csv("OPEN", s_id, symbol, price, price, amount, 0, current_equity, strategy['total_invested'], total_used_margin, 0, change_pct, "开空")
            
        strategy['positions'] = new_positions

    strategy['last_trade_date'] = target_date_str
    return "ROTATED"

def scan_and_execute_strategies(opener, data, market_map, top_10):
    rotated_ids = []
    closed_only_info = {} 
    current_utc = datetime.utcnow()
    print(f"\n🔍 [扫描] 当前UTC时间: {current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    
    for i in range(24):
        s_id = str(i)
        strategy = data[s_id]
        sched_time_today = datetime(current_utc.year, current_utc.month, current_utc.day, i, 0, 0)
        target_dt = sched_time_today if current_utc >= sched_time_today else sched_time_today - timedelta(days=1)
        target_date_str = target_dt.strftime('%Y-%m-%d')
        
        if strategy['last_trade_date'] == target_date_str: continue 
            
        delay_seconds = (current_utc - target_dt).total_seconds()
        delay_str = f"{delay_seconds/3600:.1f}h"
        print(f"   >> 发现策略 {s_id} 待处理: (延迟 {delay_str})")
        
        is_late = delay_seconds > MAX_DELAY_SECONDS
        result = execute_single_strategy(s_id, strategy, opener, market_map, top_10, current_utc, target_date_str, is_late, delay_str)
        if result == "ROTATED": rotated_ids.append(s_id)
        elif result == "CLOSED_ONLY": closed_only_info[s_id] = delay_str
            
    return rotated_ids, closed_only_info

# --- [新增] 追涨策略逻辑 ---
def run_chase_strategy(data, market_map, top_10):
    """
    逻辑：
    1. 检查现有持仓：满 11 小时平仓。
    2. 检查新币：对比上一次 Top10 和本次 Top10，新增的开多。
    """
    strat = data[CHASE_STRAT_ID]
    prev_top10 = set(strat.get("prev_top10", []))
    curr_top10 = set([x['symbol'] for x in top_10])
    
    current_ts = int(time.time())
    acted = False
    
    print(f"\n🚀 [追涨] 检查 S_CHASE 策略...")
    
    # 1. 检查平仓
    remaining_positions = []
    if strat['positions']:
        for pos in strat['positions']:
            entry_time = pos.get('entry_time', 0)
            hold_time = (current_ts - entry_time) / 3600.0
            
            if hold_time >= CHASE_HOLD_HOURS:
                # 平仓
                symbol = pos['symbol']
                entry = float(pos['entry_price'])
                amount = float(pos['amount'])
                exit_price = market_map.get(symbol, entry)
                
                # 做多盈亏: (平仓 - 开仓) * 数量
                pnl = (exit_price - entry) * amount
                strat['balance'] += pnl
                
                max_p = pos.get('max_price', entry)
                if exit_price > max_p: max_p = exit_price
                
                note = f"追涨平仓({hold_time:.1f}h) | Max:{max_p:.4g}"
                log_to_csv("CLOSE_LONG", CHASE_STRAT_ID, symbol, exit_price, exit_price, amount, pnl, 
                           strat['balance'], strat['total_invested'], 0, pnl, 0, note)
                acted = True
            else:
                remaining_positions.append(pos)
        strat['positions'] = remaining_positions

    # 2. 检查开仓 (新上榜)
    # 首次运行无历史，只更新不交易
    if not prev_top10:
        print("   >> 首次运行或无历史，初始化 Top10 列表。")
    else:
        new_coins = curr_top10 - prev_top10
        for sym in new_coins:
            info = next((x for x in top_10 if x['symbol'] == sym), None)
            if not info: continue
            
            price = info['price']
            chg = info['change']
            
            margin = CHASE_MARGIN
            amount = (margin * CHASE_LEVERAGE) / price
            
            new_pos = {
                "symbol": sym, "entry_price": price, "margin": margin, "amount": amount,
                "leverage": CHASE_LEVERAGE, "entry_time": current_ts,
                "max_price": price, "min_price": price, "side": "LONG"
            }
            strat['positions'].append(new_pos)
            
            log_to_csv("OPEN_LONG", CHASE_STRAT_ID, sym, price, price, amount, 0, 
                       strat['balance'], strat['total_invested'], margin, 0, chg, "新上榜追涨")
            print(f"   >> 发现新币 {sym}，开多！")
            acted = True

    # 更新历史记录
    strat['prev_top10'] = list(curr_top10)
    return acted

# ==========================================
#               通知与主程序
# ==========================================

def report_to_wechat(opener, data, market_map, rotated_ids, closed_only_info, liquidated_ids, chase_acted):
    if not SERVERCHAN_KEY: return
    print("\n📤 正在生成报告...")
    
    total_equity = 0
    total_invested = 0
    
    md_table = "| ID | 投入 | 押金 | 净值 | 总盈 | 轮盈 | 爆 |\n| :--: | :--: | :--: | :--: | :--: | :--: | :--: |\n"
    
    # S0-S23 循环
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        eq, _ = calculate_strategy_equity(strat, market_map)
        inv = strat.get('total_invested', INITIAL_UNIT)
        pnl = eq - inv
        
        total_equity += eq
        total_invested += inv
        
        icon = "🔴" if pnl < 0 else "🟢"
        if eq == 0: icon = "💀"
        
        status = ""
        if s_id in rotated_ids: status = "🔄"
        elif s_id in closed_only_info: status = "🛑"
        elif s_id in liquidated_ids: status = "💥"
        
        # 只显示有变动的或头部/尾部
        if status or i < 3 or i > 20:
            md_table += f"| {s_id} | {inv:.0f} | - | {eq:.0f} | {icon}{pnl:.0f} | - | {status} |\n"
            
    # 加上追涨策略资金
    c_strat = data[CHASE_STRAT_ID]
    c_eq, c_details = calculate_strategy_equity(c_strat, market_map)
    c_inv = c_strat.get('total_invested', 1000)
    c_pnl = c_eq - c_inv
    
    total_equity += c_eq
    total_invested += c_inv
    total_pnl = total_equity - total_invested
    pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    
    current_utc_str = datetime.utcnow().strftime("%H:%M")
    title = f"总净{total_equity:.0f} ({pnl_pct:+.1f}%)"
    if rotated_ids: title = f"🔄S{len(rotated_ids)} | {title}"
    if chase_acted: title = f"🚀追涨 | {title}"
    
    # 构造追涨详情
    chase_info = "无持仓"
    if c_details:
        items = [f"{d['symbol'].replace('USDT','')}({d['pnl']:+.1f})" for d in c_details]
        chase_info = ", ".join(items)
    
    description = f"""
**UTC**: {current_utc_str}
**总投**: {total_invested:.0f} U
**总净**: {total_equity:.0f} U
**盈亏**: {total_pnl:+.1f} U

---
### 📉 轮动策略 (Top 10 Short)
{md_table}

---
### 🚀 追涨策略 (Top 10 Chase)
**净值**: {c_eq:.0f} U  **盈亏**: {c_pnl:+.1f} U
**持仓**: {chase_info}
    """
    try:
        req = urllib.request.Request(f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send", data=urllib.parse.urlencode({'title': title, 'desp': description}).encode('utf-8'), method='POST')
        with urllib.request.urlopen(req) as f: print("✅ 微信推送成功")
    except Exception as e: print(f"❌ 微信推送失败: {e}")

if __name__ == "__main__":
    opener = get_proxy_opener()
    market_map, top_10 = get_market_rank(opener)
    
    if market_map:
        data = load_state()
        
        # 0. 更新价格
        update_price_stats(data, market_map)
        
        # 1. 风控
        liquidated_ids = check_risk_management(opener, data, market_map)
        
        # 2. 轮动 (S0-S23)
        rotated_ids, closed_only_info = scan_and_execute_strategies(opener, data, market_map, top_10)
        
        # 3. 追涨 (S_CHASE)
        chase_acted = run_chase_strategy(data, market_map, top_10)
        
        # 4. 净值
        record_equity_snapshot(data, market_map)
        
        # 5. 快照
        if rotated_ids or closed_only_info or liquidated_ids or chase_acted:
            record_positions_snapshot(data, market_map)
        
        save_state(data)
        
        # 6. 通知
        if rotated_ids or closed_only_info or liquidated_ids or chase_acted:
            report_to_wechat(opener, data, market_map, rotated_ids, closed_only_info, liquidated_ids, chase_acted)
