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
HISTORY_FILE = "strategy_history.csv"  # 统一记录文件
EQUITY_FILE = "equity_curve.csv"

# --- [新功能开关] ---
ENABLE_COMPOUNDING = True  # 复利开关
ENABLE_ROI_PAYBACK = True  # 回本提取开关

# --- [资金参数: 轮动做空 S0-S23] ---
INITIAL_UNIT = 1000.0     # 标准开仓/复活金额
POSITIONS_COUNT = 10      # 持仓数量
LEVERAGE = 3.0            # 杠杆倍数
MIN_ALIVE_BALANCE = 10.0  # “存活”阈值
MAX_DELAY_SECONDS = 3600  # 最大延迟容忍时间(秒)

# --- [资金参数: 追涨做多 S_CHASE] ---
CHASE_STRAT_ID = "S_CHASE" # 策略ID
CHASE_MARGIN = 100.0       # 固定保证金(U)
CHASE_LEVERAGE = 3.0       # 杠杆倍数
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
    """
    计算策略净值，支持 做空(SHORT) 和 做多(LONG) 两种模式
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
            
            # 默认为做空，兼容旧数据
            side = pos.get('side', 'SHORT')
            
            curr = market_map.get(symbol, entry)
            calc_price = curr
            warn_msg = ""
            
            if opener and use_high_price:
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m > 0:
                    calc_price = max(curr, high_15m)
                    if high_15m > entry * 1.05: warn_msg = "⚠️"

            # 核心盈亏计算逻辑
            if side == 'LONG':
                pnl = (curr - entry) * amount  # 做多: (现价 - 均价) * 数量
            else:
                pnl = (entry - calc_price) * amount # 做空: (均价 - 现价) * 数量
                
            total_unrealized_pnl += pnl
            
            # 记录入场时间以便后续计算时长
            entry_time = pos.get('entry_time', 0)
            
            details.append({
                'symbol': symbol,
                'entry': entry,
                'curr': curr,
                'calc_price': calc_price,
                'amount': amount,
                'pnl': pnl,
                'warn': warn_msg,
                'side': side,
                'entry_time': entry_time
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
    
    # === [关键] 允许新的类型通过 ===
    CRITICAL_EVENTS = ["OPEN", "CLOSE", "OPEN_LONG", "CLOSE_LONG", "LIQUIDATION", "REPLENISH", "WITHDRAW", "ROUND_RES", "SNAPSHOT"]
    
    if record_type not in CRITICAL_EVENTS:
        return 

    change_str = ""
    if "OPEN" in record_type: change_str = f"涨:{change_pct_val:>+5.1f}%"
    
    if record_type != "SNAPSHOT":
        print(f"📝 [CSV] {record_type:<10} {strategy_id:<7} {symbol:<8} 净:{equity_val:.0f} 投:{invested_val:.0f} 轮:{round_pnl_val:+.0f} {change_str} | {note}")

    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 如果文件不存在，写入表头
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", "Used_Margin", "Round_PnL", "24h_Change", "Note"])
            
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, pos_pnl, equity_val, invested_val, used_margin_val, round_pnl_val, change_pct_val, note])
    except Exception as e:
        print(f"❌ 写入历史CSV失败: {e}")

# ==========================================
#               聚合快照逻辑
# ==========================================
def record_aggregated_snapshot(data, market_map):
    print(f"📸 [快照] 正在聚合记录持仓币种价格...")
    agg_data = {}
    
    for s_id, strat in data.items():
        if not strat.get('positions'): continue
        
        for pos in strat['positions']:
            sym = pos['symbol']
            entry = float(pos['entry_price'])
            side = pos.get('side', 'SHORT') # 区分多空
            
            key = f"{sym}_{side}"
            
            if key not in agg_data:
                agg_data[key] = {'sym': sym, 'side': side, 'count': 0, 'total_entry': 0.0, 's_ids': []}
            
            agg_data[key]['count'] += 1
            agg_data[key]['total_entry'] += entry
            agg_data[key]['s_ids'].append(str(s_id))
    
    if not agg_data:
        print("📸 [快照] 当前无持仓，跳过。")
        return

    # 写入 CSV
    count = 0
    for key, info in agg_data.items():
        curr_price = float(market_map.get(info['sym'], 0))
        if curr_price == 0: continue
        
        avg_entry = info['total_entry'] / info['count']
        s_list = ",".join(info['s_ids'])
        
        note_str = f"{info['side']} | Hold:{info['count']} | AvgEntry:{avg_entry:.4g} | S:{s_list}"
        
        log_to_csv("SNAPSHOT", "AGG", info['sym'], curr_price, 0, 0, 0, 0, 0, 0, 0, 0, note_str)
        count += 1
            
    print(f"✅ [快照] 完成，共记录 {count} 条独立持仓信息。")

def record_equity_snapshot(data, market_map):
    file_exists = os.path.isfile(EQUITY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    row_data = [current_time]
    total_equity = 0.0
    total_invested_all = 0.0
    
    # 1. 记录 S0-S23 的数据 (保持原格式)
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        eq, _ = calculate_strategy_equity(strat, market_map, opener=None, use_high_price=False)
        if eq < 0: eq = 0
        
        row_data.append(round(eq, 2))
        total_equity += eq
        total_invested_all += strat.get('total_invested', INITIAL_UNIT)
        
    # 2. 将 S_CHASE 的数据加入到 Total 中，但不增加新的列
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
            data[str(i)] = {"balance": INITIAL_UNIT, "positions": [], "last_trade_date": "", "total_invested": INITIAL_UNIT, "liquidation_count": 0}
        return data
        
    with open(STATE_FILE, 'r') as f:
        data = json.load(f)
    
    # 确保 S0-S23 字段完整
    for k, v in data.items():
        if k == CHASE_STRAT_ID: continue
        if "total_invested" not in v: v["total_invested"] = INITIAL_UNIT
        if "liquidation_count" not in v: v["liquidation_count"] = 0
    
    # [升级] 初始化 S_CHASE 结构，增加统计字段
    if CHASE_STRAT_ID not in data:
        data[CHASE_STRAT_ID] = {
            "balance": 1000.0, # 初始虚拟资金
            "positions": [],
            "prev_top10": [],  # 记录上次Top10
            "total_invested": 1000.0,
            "liquidation_count": 0,
            "closed_count": 0, # 新增：已平仓数量
            "wins": 0,         # 新增：盈利次数
            "accumulated_pnl": 0.0 # 新增：累计盈亏
        }
    
    # 补全可能缺失的字段
    chase_data = data.get(CHASE_STRAT_ID, {})
    if "prev_top10" not in chase_data: chase_data["prev_top10"] = []
    if "closed_count" not in chase_data: chase_data["closed_count"] = 0
    if "wins" not in chase_data: chase_data["wins"] = 0
    if "accumulated_pnl" not in chase_data: chase_data["accumulated_pnl"] = 0.0
    data[CHASE_STRAT_ID] = chase_data
        
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
                side_icon = "🟢" if d['side']=='LONG' else "🔴"
                short_symbol = d['symbol'].replace("USDT", "")
                coin_str = f"{side_icon}{short_symbol}({d['pnl']:+.0f})"
                coin_details_list.append(coin_str)
            pnl = equity - invested
            print(f"   >> {s_id:<7} 净:{equity:>5.0f} ({pnl:>+5.0f}) 押:{used_margin:>4.0f} 轮:{round_pnl:>+5.0f} | {' '.join(coin_details_list)}")
        
        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            liquidated_ids.append(s_id)
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, invested, used_margin, -used_margin, 0.0, "全仓强平")
            strategy['balance'] = 0
            strategy['positions'] = []
            strategy['liquidation_count'] = strategy.get('liquidation_count', 0) + 1
            
    return liquidated_ids

# --- 1. S0-S23 轮动策略 (原逻辑不变) ---
def execute_single_strategy(s_id, strategy, opener, market_map, top_10, current_utc, target_date_str, is_late_close_only, delay_str):
    print(f"\n⚡ [轮动] 策略 {s_id} (延迟: {delay_str})")
    
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    invested = strategy['total_invested']
    current_ts = int(time.time())
    
    # 1. 平仓
    if wallet_balance > 0 and strategy['positions']:
        used_margin = sum([p.get('margin', 0) for p in strategy['positions']])
        duration_hours = 0.0
        if strategy['positions']:
            entry_time = strategy['positions'][0].get('entry_time', 0)
            if entry_time > 0: duration_hours = (current_ts - entry_time) / 3600.0

        close_note_base = "轮动平仓"
        if is_late_close_only: close_note_base = f"延迟{delay_str}平仓"
        elif delay_str != "0.0h": close_note_base = f"轮动平仓(延{delay_str})"
            
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry, amount = float(pos['entry_price']), float(pos['amount'])
            exit_price = market_map.get(symbol, entry)
            max_p, min_p = pos.get('max_price', entry), pos.get('min_price', entry)
            if exit_price > max_p: max_p = exit_price
            if exit_price < min_p: min_p = exit_price
            
            pnl = (entry - exit_price) * amount # 做空盈亏
            total_close_pnl += pnl
            temp_equity = wallet_balance + total_close_pnl
            
            note_str = f"{close_note_base} | Max:{max_p:.4g} Min:{min_p:.4g}"
            log_to_csv("CLOSE", s_id, symbol, exit_price, exit_price, amount, pnl, temp_equity, invested, used_margin, pnl, 0.0, note_str)

        roi_pct = (total_close_pnl / used_margin * 100) if used_margin > 0 else 0
        summary_note = f"本轮结算: 利润{total_close_pnl:+.1f}U, ROI:{roi_pct:+.1f}%, 持仓{duration_hours:.1f}h"
        log_to_csv("ROUND_RES", s_id, "ALL", 0, 0, 0, total_close_pnl, wallet_balance + total_close_pnl, invested, used_margin, total_close_pnl, 0.0, summary_note)
        
        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    current_equity = strategy['balance']
    
    if is_late_close_only:
        strategy['last_trade_date'] = target_date_str
        return "CLOSED_ONLY"

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
        log_to_csv("WITHDRAW", s_id, "USDT", 0, 0, 0, 0, strategy['balance'], strategy['total_invested'], 0, 0, 0.0, "回本提取")
        current_equity = strategy['balance'] 

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
                "leverage": LEVERAGE, "entry_time": entry_ts, "max_price": price, "min_price": price,
                "side": "SHORT"
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

# --- 2. [新增/优化] S_CHASE 追涨策略 ---
def run_chase_strategy(data, market_map, top_10):
    """
    逻辑：
    1. 平仓检查 (持仓 > 11h) -> 更新统计数据
    2. 开仓检查 (新上榜 & 无持仓)
    """
    strat = data[CHASE_STRAT_ID]
    prev_top10 = set(strat.get("prev_top10", []))
    curr_top10_set = set([x['symbol'] for x in top_10])
    
    # 获取现有持仓币种列表，用于排重
    current_holding_symbols = set(pos['symbol'] for pos in strat['positions'])
    
    current_ts = int(time.time())
    acted = False
    print(f"\n🚀 [追涨] 检查 S_CHASE 策略...")
    
    # A. 检查平仓 (持仓 > 11h)
    remaining = []
    positions_changed = False
    
    if strat['positions']:
        for pos in strat['positions']:
            entry_time = pos.get('entry_time', 0)
            hold_time = (current_ts - entry_time) / 3600.0
            
            if hold_time >= CHASE_HOLD_HOURS:
                # 触发平仓
                sym, entry, amt = pos['symbol'], float(pos['entry_price']), float(pos['amount'])
                curr = market_map.get(sym, entry)
                
                # 做多盈亏
                pnl = (curr - entry) * amt
                
                # 更新最高价
                max_p = pos.get('max_price', entry)
                if curr > max_p: max_p = curr
                
                # 资金回笼
                strat['balance'] += pnl
                
                # [新增] 更新统计数据
                strat['closed_count'] += 1
                strat['accumulated_pnl'] += pnl
                if pnl > 0: strat['wins'] += 1
                
                note = f"追涨平仓({hold_time:.1f}h) | Max:{max_p:.4g}"
                log_to_csv("CLOSE_LONG", CHASE_STRAT_ID, sym, curr, curr, amt, pnl, 
                           strat['balance'], strat['total_invested'], 0, pnl, 0, note)
                
                print(f"   >> [S_CHASE] 平仓 {sym}: 盈亏 {pnl:+.1f} U, 持有 {hold_time:.1f}h")
                acted = True
                positions_changed = True
            else:
                remaining.append(pos)
        
        if positions_changed:
            strat['positions'] = remaining

    # B. 检查开仓 (新上榜)
    if not prev_top10:
        print("   >> 首次运行或无历史，初始化 Top10 列表，跳过开仓。")
    else:
        new_coins = curr_top10_set - prev_top10
        for sym in new_coins:
            # 1. 检查是否已在持仓中 (防止重复开仓)
            if sym in current_holding_symbols:
                print(f"   >> [S_CHASE] 新上榜 {sym} 已持有，跳过。")
                continue
                
            info = next((x for x in top_10 if x['symbol'] == sym), None)
            if not info: continue
            
            price = info['price']
            chg = info['change']
            
            # 2. 开仓
            margin = CHASE_MARGIN
            amt = (margin * CHASE_LEVERAGE) / price
            
            new_pos = {
                "symbol": sym, 
                "entry_price": price, 
                "margin": margin, 
                "amount": amt,
                "leverage": CHASE_LEVERAGE, 
                "entry_time": current_ts,
                "max_price": price, 
                "min_price": price, 
                "side": "LONG" 
            }
            strat['positions'].append(new_pos)
            # 添加到临时集合，防止同一次运行多次开同一币种(理论上不会但保险)
            current_holding_symbols.add(sym)
            
            log_to_csv("OPEN_LONG", CHASE_STRAT_ID, sym, price, price, amt, 0, 
                       strat['balance'], strat['total_invested'], margin, 0, chg, "新上榜追涨")
            print(f"   >> [S_CHASE] 发现新币 {sym}，执行开多！")
            acted = True
            
    # C. 更新 Top10 记录
    strat['prev_top10'] = list(curr_top10_set)
    return acted

def report_to_wechat(opener, data, market_map, rotated_ids, closed_only_info, liquidated_ids, chase_acted):
    if not SERVERCHAN_KEY: return
    print("\n📤 正在生成报告...")
    total_equity, total_invested_all, total_liquidations, max_profit = 0, 0, 0, -999999
    md_table = "| ID | 投入 | 押金 | 净值 | 总盈 | 轮盈 | 爆 |\n| :--: | :--: | :--: | :--: | :--: | :--: | :--: |\n"
    detail_text, current_ts = "", int(time.time())
    all_action_ids = set(rotated_ids + list(closed_only_info.keys()) + liquidated_ids)
    
    # 1. 原始 S0-S23 报告生成
    for i in range(24):
        s_id = str(i)
        strat = data[s_id]
        invested = strat.get('total_invested', INITIAL_UNIT)
        liq_count = strat.get('liquidation_count', 0)
        equity, details = calculate_strategy_equity(strat, market_map, opener, use_high_price=False)
        used_margin = sum([p.get('margin', 0) for p in strat['positions']])
        round_pnl = equity - strat['balance']
        net_pnl = equity - invested
        total_equity += equity
        total_invested_all += invested
        total_liquidations += liq_count
        if net_pnl > max_profit: max_profit = net_pnl

        icon = "🔴" if net_pnl < 0 else "🟢"
        if equity == 0: icon = "💀" 
        elif s_id in rotated_ids: icon = "🔄"
        elif s_id in closed_only_info: icon = "🛑"
        
        md_table += f"| {s_id} | {invested:.0f} | {used_margin:.0f} | {equity:.0f} | {icon}{net_pnl:+.0f} | {round_pnl:+.0f} | {liq_count} |\n"

        if (len(strat['positions']) > 0) or (s_id in all_action_ids) or (equity==0):
            prefix = "🔄" if s_id in rotated_ids else ("🛑" if s_id in closed_only_info else "")
            duration_str = "-"
            if strat['positions']:
                entry_time = strat['positions'][0].get('entry_time', 0)
                if entry_time > 0: duration_str = f"{(current_ts - entry_time)/3600:.1f}h"
            
            if s_id in closed_only_info:
                detail_text += f"\n🛑 **S{s_id}** (延{closed_only_info[s_id]}): 仅平仓。\n"
            elif len(strat['positions']) > 0:
                detail_text += f"\n🔷 **{prefix}S{s_id}** (净:{equity:.0f} 轮:{round_pnl:+.0f} ⏱️{duration_str}):\n"
                items = [f"{d['symbol'].replace('USDT','')}({d['pnl']:+.1f}){'⚠️' if d.get('warn') else ''}" for d in details]
                detail_text += ", ".join(items) + "\n"
            elif equity == 0:
                detail_text += f"\n💀 **S{s_id}**: 爆仓 {liq_count} 次\n"

    # 2. S_CHASE 数据统计与展示
    if CHASE_STRAT_ID in data:
        c_strat = data[CHASE_STRAT_ID]
        c_eq, c_details = calculate_strategy_equity(c_strat, market_map)
        c_inv = c_strat.get('total_invested', 1000.0)
        
        # 汇总资金
        total_equity += c_eq
        total_invested_all += c_inv
        
        # 计算统计数据
        closed_count = c_strat.get('closed_count', 0)
        wins = c_strat.get('wins', 0)
        acc_pnl = c_strat.get('accumulated_pnl', 0.0)
        
        win_rate = (wins / closed_count * 100) if closed_count > 0 else 0
        avg_pnl = (acc_pnl / closed_count) if closed_count > 0 else 0
        
        # 构造详细持仓显示
        c_holding_str = "无持仓"
        if c_details:
            items = []
            for d in c_details:
                # 计算持仓时间
                dur_h = (current_ts - d.get('entry_time', current_ts)) / 3600.0
                sym = d['symbol'].replace('USDT','')
                items.append(f"{sym}({d['pnl']:+.1f}, {dur_h:.1f}h)")
            c_holding_str = ", ".join(items)
            
        chase_section = f"""
### 🚀 追涨策略 (Top 10 Chase)
* **总投入**: {c_inv:.0f} U | **当前净值**: {c_eq:.0f} U
* **已平仓**: {closed_count} 只 | **胜率**: {win_rate:.1f}%
* **均盈亏**: {avg_pnl:+.1f} U | **累计盈亏**: {acc_pnl:+.1f} U
* **持仓中**: {c_holding_str}
"""
    else:
        chase_section = "\n### 🚀 追涨策略 (未初始化)"

    total_pnl = total_equity - total_invested_all
    total_pnl_pct = (total_pnl / total_invested_all * 100) if total_invested_all > 0 else 0
    current_utc_str = datetime.utcnow().strftime("%H:%M")
    
    title = f"投{total_invested_all:.0f} 剩{total_equity:.0f} ({total_pnl_pct:+.1f}%)"
    if rotated_ids: title = f"🔄S{','.join(rotated_ids)} | {title}"
    if chase_acted: title = f"🚀追涨 | {title}"
    
    description = f"""
**UTC**: {current_utc_str}
**总投**: {total_invested_all:.0f} U
**总净**: {total_equity:.0f} U
**盈亏**: {total_pnl:+.1f} U

---
### 📉 轮动策略 (Top 10 Short)
{md_table}
{detail_text}
---
{chase_section}
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
        
        # 2. 轮动
        rotated_ids, closed_only_info = scan_and_execute_strategies(opener, data, market_map, top_10)
        
        # 3. 追涨 (新增)
        chase_acted = run_chase_strategy(data, market_map, top_10)
        
        # 4. 净值
        record_equity_snapshot(data, market_map)
        
        # 5. 快照
        if rotated_ids or closed_only_info or liquidated_ids or chase_acted:
            record_aggregated_snapshot(data, market_map)
        
        save_state(data)
        
        # 6. 通知
        if rotated_ids or closed_only_info or liquidated_ids or chase_acted:
            report_to_wechat(opener, data, market_map, rotated_ids, closed_only_info, liquidated_ids, chase_acted)
