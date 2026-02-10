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

# --- [现有配置] ---
ENABLE_COMPOUNDING = True
ENABLE_ROI_PAYBACK = True
INITIAL_UNIT = 1000.0     # 轮动策略本金
POSITIONS_COUNT = 10
LEVERAGE = 3.0
MIN_ALIVE_BALANCE = 10.0
MAX_DELAY_SECONDS = 3600

# --- [新增：追涨策略配置] ---
CHASE_STRAT_ID = "S_CHASE" # 追涨策略独立ID
CHASE_MARGIN = 100.0       # 每次开仓保证金(U)
CHASE_LEVERAGE = 3.0       # 追涨杠杆
CHASE_HOLD_HOURS = 11      # 持仓时间(小时)

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
            is_long = pos.get('side', 'SHORT') == 'LONG' # 默认为空，兼容旧数据
            
            curr = market_map.get(symbol, entry)
            calc_price = curr
            warn_msg = ""
            
            if opener and use_high_price:
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m > 0:
                    calc_price = max(curr, high_15m)
                    if high_15m > entry * 1.05: warn_msg = "⚠️"

            if is_long:
                pnl = (curr - entry) * amount # 做多盈亏
            else:
                pnl = (entry - calc_price) * amount # 做空盈亏
                
            total_unrealized_pnl += pnl
            
            details.append({
                'symbol': symbol,
                'entry': entry,
                'curr': curr,
                'calc_price': calc_price,
                'amount': amount,
                'pnl': pnl,
                'warn': warn_msg,
                'side': 'LONG' if is_long else 'SHORT'
            })
            
    equity = wallet_balance + total_unrealized_pnl
    return equity, details

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, pos_pnl, equity, total_invested, used_margin, round_pnl, change_pct=0.0, note=""):
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # 格式化数值
    equity_val = float(equity)
    invested_val = float(total_invested)
    used_margin_val = float(used_margin)
    round_pnl_val = float(round_pnl)
    change_pct_val = float(change_pct)
    
    CRITICAL_EVENTS = ["OPEN", "CLOSE", "OPEN_LONG", "CLOSE_LONG", "LIQUIDATION", "REPLENISH", "WITHDRAW", "ROUND_RES", "SNAPSHOT"]
    
    if record_type not in CRITICAL_EVENTS: return 

    change_str = ""
    if "OPEN" in record_type: change_str = f"涨:{change_pct_val:>+5.1f}%"
    
    if record_type != "SNAPSHOT":
        print(f"📝 [CSV] {record_type:<10} {strategy_id:<3} {symbol:<8} 净:{equity_val:.0f} 投:{invested_val:.0f} 轮:{round_pnl_val:+.0f} {change_str} | {note}")

    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", "Used_Margin", "Round_PnL", "24h_Change", "Note"])
            
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, pos_pnl, equity_val, invested_val, used_margin_val, round_pnl_val, change_pct_val, note])
    except Exception as e:
        print(f"❌ 写入历史CSV失败: {e}")

# ==========================================
#               记录与快照逻辑
# ==========================================
def record_aggregated_snapshot(data, market_map):
    print(f"📸 [快照] 正在聚合记录持仓币种价格...")
    agg_data = {}
    
    for s_id, strat in data.items():
        if not strat.get('positions'): continue
        
        for pos in strat['positions']:
            sym = pos['symbol']
            entry = float(pos['entry_price'])
            side = pos.get('side', 'SHORT')
            
            # Key 区分多空: DOGE_SHORT / DOGE_LONG
            key = f"{sym}_{side}"
            
            if key not in agg_data:
                agg_data[key] = {'sym': sym, 'side': side, 'count': 0, 'total_entry': 0.0, 's_ids': []}
            
            agg_data[key]['count'] += 1
            agg_data[key]['total_entry'] += entry
            agg_data[key]['s_ids'].append(str(s_id))
    
    if not agg_data:
        print("📸 [快照] 当前无持仓，跳过。")
        return

    count = 0
    for key, info in agg_data.items():
        curr_price = float(market_map.get(info['sym'], 0))
        if curr_price == 0: continue
        
        avg_entry = info['total_entry'] / info['count']
        s_list = ",".join(info['s_ids'])
        
        # 备注: 多空方向 | 持仓数 | 均价
        note_str = f"{info['side']} | Hold:{info['count']} | AvgEntry:{avg_entry:.4g} | S:{s_list}"
        
        log_to_csv("SNAPSHOT", "AGG", info['sym'], curr_price, 0, 0, 0, 0, 0, 0, 0, 0, note_str)
        count += 1
            
    print(f"✅ [快照] 完成，共记录 {count} 条聚合持仓信息。")

def record_equity_snapshot(data, market_map):
    file_exists = os.path.isfile(EQUITY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    row_data = [current_time]
    total_equity = 0.0
    total_invested_all = 0.0
    
    # 记录 S0-S23 的净值
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        eq, _ = calculate_strategy_equity(strat, market_map)
        if eq < 0: eq = 0
        row_data.append(round(eq, 2))
        total_equity += eq
        total_invested_all += strat.get('total_invested', INITIAL_UNIT)
    
    # 加上 S_CHASE 的净值到总计
    if CHASE_STRAT_ID in data:
        chase_strat = data[CHASE_STRAT_ID]
        eq, _ = calculate_strategy_equity(chase_strat, market_map)
        total_equity += eq
        total_invested_all += chase_strat.get('total_invested', 0)
        
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
        # 初始化 S0-S23
        for i in range(24):
            data[str(i)] = {"balance": INITIAL_UNIT, "positions": [], "last_trade_date": "", "total_invested": INITIAL_UNIT, "liquidation_count": 0}
        return data
        
    with open(STATE_FILE, 'r') as f:
        data = json.load(f)
        
    # 确保 S0-S23 字段完整
    for k, v in data.items():
        if k == CHASE_STRAT_ID: continue
        if "total_invested" not in v: v["total_invested"] = INITIAL_UNIT
        if "liquidation_count" not in v: v["liquidation_count"] = 0
    
    # 初始化 S_CHASE (如果不存在)
    if CHASE_STRAT_ID not in data:
        data[CHASE_STRAT_ID] = {
            "balance": 1000.0, # 初始资金池
            "positions": [],
            "prev_top10": [],  # 记录上一次的Top10，用于判断新上榜
            "total_invested": 1000.0,
            "liquidation_count": 0
        }
    else:
        # 确保 prev_top10 字段存在
        if "prev_top10" not in data[CHASE_STRAT_ID]:
            data[CHASE_STRAT_ID]["prev_top10"] = []
            
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
#               核心业务逻辑
# ==========================================

# --- 1. 风控检查 ---
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
                side_icon = "🟢" if d['side']=='LONG' else "🔴"
                short_symbol = d['symbol'].replace("USDT", "")
                coin_str = f"{side_icon}{short_symbol}({d['pnl']:+.0f})"
                coin_details_list.append(coin_str)
            pnl = equity - invested
            print(f"   >> {s_id:<3} 净:{equity:>5.0f} ({pnl:>+5.0f}) 押:{used_margin:>4.0f} 轮:{round_pnl:>+5.0f} | {' '.join(coin_details_list)}")
        
        # 爆仓判定 (简化：净值<=0即归零)
        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            liquidated_ids.append(s_id)
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, invested, used_margin, -used_margin, 0.0, "全仓强平")
            strategy['balance'] = 0
            strategy['positions'] = []
            strategy['liquidation_count'] = strategy.get('liquidation_count', 0) + 1
            
    return liquidated_ids

# --- 2. 轮动策略执行 (做空) ---
def execute_single_strategy(s_id, strategy, opener, market_map, top_10, current_utc, target_date_str, is_late_close_only, delay_str):
    print(f"\n⚡ [轮动] 策略 {s_id} (延迟: {delay_str})")
    
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    invested = strategy['total_invested']
    current_ts = int(time.time())
    
    # 平仓逻辑
    if wallet_balance > 0 and strategy['positions']:
        used_margin = sum([p.get('margin', 0) for p in strategy['positions']])
        duration_hours = 0.0
        if strategy['positions']:
            entry_time = strategy['positions'][0].get('entry_time', 0)
            if entry_time > 0: duration_hours = (current_ts - entry_time) / 3600.0

        close_note = f"轮动平仓(延{delay_str})" if delay_str != "0.0h" else "轮动平仓"
            
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry, amount = float(pos['entry_price']), float(pos['amount'])
            exit_price = market_map.get(symbol, entry)
            max_p, min_p = pos.get('max_price', entry), pos.get('min_price', entry)
            # 更新最终极值
            if exit_price > max_p: max_p = exit_price
            if exit_price < min_p: min_p = exit_price
            
            pnl = (entry - exit_price) * amount # 做空盈亏
            total_close_pnl += pnl
            temp_equity = wallet_balance + total_close_pnl
            
            log_to_csv("CLOSE", s_id, symbol, exit_price, exit_price, amount, pnl, temp_equity, invested, used_margin, pnl, 0.0, f"{close_note} | Max:{max_p:.4g} Min:{min_p:.4g}")

        roi_pct = (total_close_pnl / used_margin * 100) if used_margin > 0 else 0
        summary_note = f"本轮结算: 利润{total_close_pnl:+.1f}U, ROI:{roi_pct:+.1f}%, 持仓{duration_hours:.1f}h"
        log_to_csv("ROUND_RES", s_id, "ALL", 0, 0, 0, total_close_pnl, wallet_balance + total_close_pnl, invested, used_margin, total_close_pnl, 0.0, summary_note)
        
        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    current_equity = strategy['balance']
    
    if is_late_close_only:
        strategy['last_trade_date'] = target_date_str
        return "CLOSED_ONLY"

    # 复活/回本逻辑
    if current_equity < MIN_ALIVE_BALANCE:
        print(f"💀 策略 {s_id} 已归零，复活...")
        strategy['balance'] = INITIAL_UNIT
        strategy['total_invested'] += INITIAL_UNIT
        current_equity = strategy['balance']
        log_to_csv("REPLENISH", s_id, "USDT", 0, 0, 0, 0, current_equity, strategy['total_invested'], 0, 0, 0.0, "爆仓复活")
    elif ENABLE_ROI_PAYBACK and current_equity >= (INITIAL_UNIT * 2):
        withdraw_amount = INITIAL_UNIT
        strategy['balance'] -= withdraw_amount
        strategy['total_invested'] -= withdraw_amount
        log_to_csv("WITHDRAW", s_id, "USDT", 0, 0, 0, 0, strategy['balance'], strategy['total_invested'], 0, 0, 0.0, "回本提取")
        current_equity = strategy['balance'] 

    # 开新仓 (做空)
    trading_capital = current_equity
    if not ENABLE_COMPOUNDING and trading_capital > INITIAL_UNIT: trading_capital = INITIAL_UNIT
    
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
                "leverage": LEVERAGE, "entry_time": entry_ts, "max_price": price, "min_price": price,
                "side": "SHORT" # 标记方向
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
        print(f"   >> 策略 {s_id} 触发 (延迟 {delay_str})")
        
        is_late = delay_seconds > MAX_DELAY_SECONDS
        result = execute_single_strategy(s_id, strategy, opener, market_map, top_10, current_utc, target_date_str, is_late, delay_str)
        if result == "ROTATED": rotated_ids.append(s_id)
        elif result == "CLOSED_ONLY": closed_only_info[s_id] = delay_str
            
    return rotated_ids, closed_only_info

# --- 3. 追涨策略执行 (做多) ---
def run_chase_strategy(data, market_map, top_10):
    """
    逻辑：
    1. 检查现有持仓，如果超过11小时则平仓。
    2. 对比上一次Top10和本次Top10，找出新上榜的币。
    3. 对新币开多 (100U * 3倍)。
    """
    strat = data[CHASE_STRAT_ID]
    prev_top10_symbols = set(strat.get("prev_top10", []))
    current_top10_symbols = set([item['symbol'] for item in top_10])
    
    current_ts = int(time.time())
    acted = False
    
    print(f"\n🚀 [追涨] 检查 S_CHASE 策略...")
    
    # 1. 检查平仓 (持仓 > 11小时)
    remaining_positions = []
    positions_closed_pnl = 0.0
    closed_margin = 0.0
    
    if strat['positions']:
        for pos in strat['positions']:
            entry_time = pos.get('entry_time', 0)
            hold_time = (current_ts - entry_time) / 3600.0
            
            if hold_time >= CHASE_HOLD_HOURS:
                # 触发平仓
                symbol = pos['symbol']
                entry = float(pos['entry_price'])
                amount = float(pos['amount'])
                exit_price = market_map.get(symbol, entry)
                
                # 做多盈亏: (平仓 - 开仓) * 数量
                pnl = (exit_price - entry) * amount
                
                max_p = pos.get('max_price', entry)
                if exit_price > max_p: max_p = exit_price
                
                # 记录
                positions_closed_pnl += pnl
                closed_margin += pos.get('margin', 0)
                strat['balance'] += pnl # 结算到余额 (这里只是模拟，实际上保证金释放回余额)
                # 简单处理：余额 = 余额 + 利润 (本金部分在开仓时未扣除，仅计算占用，这里简化处理)
                # 更严谨的逻辑：Balance -= Margin(Open), Balance += Margin + PnL(Close)
                # 本脚本逻辑是：Equity = Balance + Unrealized. 
                # 这里我们假设 Balance 是可用余额。开仓扣 Balance，平仓还 Balance。
                
                # 由于原脚本没严格扣除Balance，我们这里保持一致：Balance 视为净值基数
                # PnL 直接加到 Balance
                
                note = f"追涨平仓({hold_time:.1f}h) | Max:{max_p:.4g}"
                log_to_csv("CLOSE_LONG", CHASE_STRAT_ID, symbol, exit_price, exit_price, amount, pnl, 
                           strat['balance'], strat['total_invested'], 0, pnl, 0, note)
                acted = True
            else:
                remaining_positions.append(pos)
        
        strat['positions'] = remaining_positions

    # 2. 检查开仓 (新上榜)
    # 新币 = 当前Top10 - 上次Top10
    # 注意：首次运行时 prev_top10 可能为空，此时不应全开，应跳过
    if not prev_top10_symbols:
        print("   >> 首次运行或无历史记录，初始化 Top10 列表，不执行开仓。")
    else:
        new_coins = current_top10_symbols - prev_top10_symbols
        for symbol in new_coins:
            # 找到该币的信息
            coin_info = next((x for x in top_10 if x['symbol'] == symbol), None)
            if not coin_info: continue
            
            price = coin_info['price']
            change_pct = coin_info.get('change', 0.0)
            
            # 开仓参数
            margin = CHASE_MARGIN
            amount = (margin * CHASE_LEVERAGE) / price
            
            new_pos = {
                "symbol": symbol,
                "entry_price": price,
                "margin": margin,
                "amount": amount,
                "leverage": CHASE_LEVERAGE,
                "entry_time": current_ts,
                "max_price": price,
                "min_price": price,
                "side": "LONG" # 标记为做多
            }
            strat['positions'].append(new_pos)
            
            # 记录
            log_to_csv("OPEN_LONG", CHASE_STRAT_ID, symbol, price, price, amount, 0, 
                       strat['balance'], strat['total_invested'], margin, 0, change_pct, "新上榜追涨")
            print(f"   >> 发现新币 {symbol}，开多！")
            acted = True

    # 3. 更新状态
    strat['prev_top10'] = list(current_top10_symbols)
    return acted

# ==========================================
#               通知与主程序
# ==========================================

def report_to_wechat(opener, data, market_map, rotated_ids, closed_only_info, liquidated_ids, chase_acted):
    if not SERVERCHAN_KEY: return
    print("\n📤 正在生成报告...")
    
    # 汇总 S0-S23 数据
    total_equity = 0
    total_invested = 0
    
    # 轮动策略表格
    md_table = "| ID | 投入 | 净值 | 盈亏 | 状态 |\n| :--: | :--: | :--: | :--: | :--: |\n"
    
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        eq, _ = calculate_strategy_equity(strat, market_map)
        inv = strat.get('total_invested', INITIAL_UNIT)
        pnl = eq - inv
        
        total_equity += eq
        total_invested += inv
        
        icon = "🔴" if pnl < 0 else "🟢"
        status = ""
        if s_id in rotated_ids: status = "🔄轮动"
        elif s_id in closed_only_info: status = "🛑平仓"
        elif s_id in liquidated_ids: status = "💥爆仓"
        
        # 仅显示有状态变化的，或者前3后3
        if status or i < 3 or i > 20:
            md_table += f"| S{s_id} | {inv:.0f} | {eq:.0f} | {icon}{pnl:+.0f} | {status} |\n"
            
    # 追涨策略数据
    chase_strat = data[CHASE_STRAT_ID]
    c_eq, c_details = calculate_strategy_equity(chase_strat, market_map)
    c_inv = chase_strat.get('total_invested', 1000)
    c_pnl = c_eq - c_inv
    
    # 合并总资金
    total_equity += c_eq
    total_invested += c_inv
    total_pnl = total_equity - total_invested
    pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    
    current_utc_str = datetime.utcnow().strftime("%H:%M")
    title = f"总净{total_equity:.0f} ({pnl_pct:+.1f}%)"
    if rotated_ids: title = f"🔄S{len(rotated_ids)} | {title}"
    if chase_acted: title = f"🚀追涨 | {title}"
    
    # 构造追涨策略详情
    chase_info = ""
    if c_details:
        items = [f"{d['symbol'].replace('USDT','')}({d['pnl']:+.1f})" for d in c_details]
        chase_info = f"\n🚀 **S_CHASE 持仓**: {', '.join(items)}"
    
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
{chase_info}
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
        
        # 0. 更新价格极值
        update_price_stats(data, market_map)
        
        # 1. 风控
        liquidated_ids = check_risk_management(opener, data, market_map)
        
        # 2. 执行轮动策略 (S0-S23)
        rotated_ids, closed_only_info = scan_and_execute_strategies(opener, data, market_map, top_10)
        
        # 3. 执行追涨策略 (S_CHASE)
        chase_acted = run_chase_strategy(data, market_map, top_10)
        
        # 4. 记录净值
        record_equity_snapshot(data, market_map)
        
        # 5. [聚合快照] 记录全网持仓
        if rotated_ids or closed_only_info or liquidated_ids or chase_acted:
            record_aggregated_snapshot(data, market_map)
        
        save_state(data)
        
        # 发送通知
        if rotated_ids or closed_only_info or liquidated_ids or chase_acted:
            report_to_wechat(opener, data, market_map, rotated_ids, closed_only_info, liquidated_ids, chase_acted)
