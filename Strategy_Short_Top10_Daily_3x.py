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
EQUITY_FILE = "equity_curve.csv"

# --- [新功能开关] ---
ENABLE_COMPOUNDING = True  # 复利开关
ENABLE_ROI_PAYBACK = True  # 回本提取开关

# --- [资金参数] ---
INITIAL_UNIT = 1000.0     # 标准开仓/复活金额
POSITIONS_COUNT = 10      # 持仓数量
LEVERAGE = 3.0            # 杠杆倍数
MIN_ALIVE_BALANCE = 10.0  # “存活”阈值
MAX_DELAY_SECONDS = 3600  # 最大延迟容忍时间(秒)

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
    
    equity_val = float(equity)
    invested_val = float(total_invested)
    
    # 控制台打印
    print(f"📝 [CSV] {record_type:<10} 策略{strategy_id:<2} {symbol:<8} 净值:{equity_val:.0f} 投入:{invested_val:.0f} | {note}")

    CRITICAL_EVENTS = ["OPEN", "CLOSE", "LIQUIDATION", "REPLENISH", "WITHDRAW"]
    if record_type not in CRITICAL_EVENTS:
        return

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
        if "total_invested" not in v: v["total_invested"] = INITIAL_UNIT
        if "liquidation_count" not in v: v["liquidation_count"] = 0
    return data

def save_state(data):
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# ==========================================
#               核心逻辑函数
# ==========================================

def check_risk_management(opener, data, market_map):
    print("\n🛡️ [监控] 开始全仓风控检查...")
    liquidated_ids = [] 
    
    for s_id in data:
        strategy = data[s_id]
        if strategy['balance'] <= 0 and not strategy['positions']:
            continue
            
        equity, details = calculate_strategy_equity(strategy, market_map, opener, use_high_price=True)
        invested = strategy.get('total_invested', INITIAL_UNIT)

        # --- [修改点] 紧凑型详细输出 ---
        # 1. 构建币种详情字符串列表: ["TRX(+5.2)", "BTC(-2.0)"]
        coin_details_list = []
        for d in details:
            short_symbol = d['symbol'].replace("USDT", "")
            # 如果有警告(插针风险)，加个!
            warn = "!" if d.get('warn') else ""
            coin_str = f"{short_symbol}({d['pnl']:+.1f}){warn}"
            coin_details_list.append(coin_str)
        
        # 2. 拼接成一行
        all_coins_str = " ".join(coin_details_list)
        
        # 3. 打印汇总行
        pnl = equity - invested
        # 格式: >> S14 净:980(-20) 投:1000 | TRX(+5.2) BTC(-3.1) ...
        print(f"   >> S{s_id:<2} 净:{equity:.0f}({pnl:+.0f}) 投:{invested:.0f} | {all_coins_str}")
        # ---------------------------

        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            liquidated_ids.append(s_id)
            # 只有在爆仓的时候，才详细记录每个币的强平信息到CSV
            for d in details:
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, invested, "全仓强平")
            
            strategy['balance'] = 0
            strategy['positions'] = []
            strategy['liquidation_count'] = strategy.get('liquidation_count', 0) + 1
            
    return liquidated_ids

def execute_single_strategy(s_id, strategy, opener, market_map, top_10, current_utc, target_date_str, is_late_close_only):
    print(f"\n⚡ [操作] 策略 {s_id} (延迟模式: {'是' if is_late_close_only else '否'})")
    
    total_close_pnl = 0
    wallet_balance = strategy['balance']
    invested = strategy['total_invested']
    
    # 1. 平旧仓
    if wallet_balance > 0 and strategy['positions']:
        for pos in strategy['positions']:
            symbol = pos['symbol']
            entry = float(pos['entry_price'])
            amount = float(pos['amount'])
            exit_price = market_map.get(symbol, entry)
            pnl = (entry - exit_price) * amount
            total_close_pnl += pnl
            temp_equity = wallet_balance + total_close_pnl
            log_to_csv("CLOSE", s_id, symbol, exit_price, exit_price, amount, pnl, temp_equity, invested, "轮动平仓")

        strategy['balance'] += total_close_pnl
        strategy['positions'] = []
    
    current_equity = strategy['balance']
    
    # 2. 严重延迟处理
    if is_late_close_only:
        strategy['last_trade_date'] = target_date_str
        print(f"🚫 策略 {s_id} 延迟超过1小时，仅执行平仓。")
        return "CLOSED_ONLY"

    # 3. 复活检测
    if current_equity < MIN_ALIVE_BALANCE:
        print(f"💀 策略 {s_id} 已归零，执行复活程序...")
        strategy['balance'] = INITIAL_UNIT
        strategy['total_invested'] += INITIAL_UNIT
        current_equity = strategy['balance']
        log_to_csv("REPLENISH", s_id, "USDT", 0, 0, 0, 0, current_equity, strategy['total_invested'], "爆仓复活")
    
    # 4. 回本机制
    elif ENABLE_ROI_PAYBACK and current_equity >= (INITIAL_UNIT * 2):
        withdraw_amount = INITIAL_UNIT
        strategy['balance'] -= withdraw_amount
        strategy['total_invested'] -= withdraw_amount
        print(f"💰 策略 {s_id} 触发回本: 提取 {withdraw_amount}U")
        log_to_csv("WITHDRAW", s_id, "USDT", 0, 0, 0, 0, strategy['balance'], strategy['total_invested'], "回本提取")
        current_equity = strategy['balance'] 

    # 5. 开新仓
    trading_capital = current_equity
    if not ENABLE_COMPOUNDING:
        if trading_capital > INITIAL_UNIT:
            trading_capital = INITIAL_UNIT
    
    if trading_capital < 1.0: 
        log_to_csv("SKIP", s_id, "ALL", 0, 0, 0, 0, current_equity, strategy['total_invested'], "资金不足")
    else:
        margin_per_coin = trading_capital / POSITIONS_COUNT
        entry_ts = int(time.time())
        
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
                "leverage": LEVERAGE,
                "entry_time": entry_ts
            })
            log_to_csv("OPEN", s_id, symbol, price, price, amount, 0, current_equity, strategy['total_invested'], "开空")
            
        strategy['positions'] = new_positions

    strategy['last_trade_date'] = target_date_str
    return "ROTATED"

def scan_and_execute_strategies(opener, data, market_map, top_10):
    rotated_ids = []
    closed_only_ids = []
    
    current_utc = datetime.utcnow()
    print(f"\n🔍 [扫描] 当前UTC时间: {current_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    
    for i in range(24):
        s_id = str(i)
        strategy = data[s_id]
        
        sched_time_today = datetime(current_utc.year, current_utc.month, current_utc.day, i, 0, 0)
        
        if current_utc >= sched_time_today:
            target_dt = sched_time_today
        else:
            target_dt = sched_time_today - timedelta(days=1)
            
        target_date_str = target_dt.strftime('%Y-%m-%d')
        
        if strategy['last_trade_date'] == target_date_str:
            continue 
            
        delay_seconds = (current_utc - target_dt).total_seconds()
        print(f"   >> 发现策略 {s_id} 待处理: 应执行时间 {target_dt} (延迟 {delay_seconds/60:.1f} 分钟)")
        
        is_late_close_only = False
        if delay_seconds > MAX_DELAY_SECONDS:
            is_late_close_only = True
            
        result = execute_single_strategy(
            s_id, strategy, opener, market_map, top_10, 
            current_utc, target_date_str, is_late_close_only
        )
        
        if result == "ROTATED":
            rotated_ids.append(s_id)
        elif result == "CLOSED_ONLY":
            closed_only_ids.append(s_id)
            
    return rotated_ids, closed_only_ids

# ==========================================
#               通知与主程序
# ==========================================

def report_to_wechat(opener, data, market_map, rotated_ids, closed_only_ids, liquidated_ids):
    if not SERVERCHAN_KEY: 
        print("⚠️ 未配置 SERVERCHAN_KEY，跳过通知")
        return
        
    print("\n📤 正在生成详细报告...")
    
    total_equity = 0
    total_invested_all = 0
    total_liquidations = 0
    max_profit = -999999
    
    md_table = "| ID | 投入 | 净值 | 盈亏 | 爆 |\n| :--: | :--: | :--: | :--: | :--: |\n"
    detail_text = ""
    current_ts = int(time.time())
    
    all_action_ids = set(rotated_ids + closed_only_ids + liquidated_ids)
    
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
        if net_pnl > max_profit: max_profit = net_pnl

        icon = "🔴" if net_pnl < 0 else "🟢"
        if equity == 0: icon = "💀" 
        elif s_id in rotated_ids: icon = "🔄"
        elif s_id in closed_only_ids: icon = "🛑"
        
        liq_str = str(liq_count) if liq_count > 0 else "-"
        inv_display = f"{invested:.0f}"
        
        md_table += f"| {s_id} | {inv_display} | {equity:.0f} | {icon}{net_pnl:+.0f} | {liq_str} |\n"

        pos_len = len(strat['positions'])
        should_show_detail = (pos_len > 0) or (s_id in all_action_ids) or (equity==0)
        
        if should_show_detail:
            prefix = ""
            if s_id in rotated_ids: prefix = "🔄"
            elif s_id in closed_only_ids: prefix = "🛑"
            
            duration_str = "-"
            used_margin = 0
            if pos_len > 0:
                used_margin = sum([p.get('margin', 0) for p in strat['positions']])
                first_pos = strat['positions'][0]
                entry_time = first_pos.get('entry_time', 0)
                if entry_time > 0:
                    duration_hours = (current_ts - entry_time) / 3600
                    duration_str = f"{duration_hours:.1f}h"
            
            liq_mark = f" 💀x{liq_count}" if liq_count > 0 else ""
            
            if s_id in closed_only_ids:
                detail_text += f"\n🛑 **S{s_id}** (严重延迟 >1h): 仅平仓, 等待明日重启。\n"
            elif pos_len > 0:
                detail_text += f"\n🔷 **{prefix}S{s_id}** (投:{invested:.0f}{liq_mark} 押:{used_margin:.0f} ⏱️{duration_str}):\n"
                simple_items = []
                for d in details:
                    warn_mark = "⚠️" if d.get('warn') else ""
                    short_symbol = d['symbol'].replace("USDT", "")
                    item_str = f"{short_symbol}({d['pnl']:+.1f}){warn_mark}"
                    simple_items.append(item_str)
                detail_text += ", ".join(simple_items) + "\n"
            elif equity == 0:
                detail_text += f"\n💀 **S{s_id}** (待复活): 累计爆仓 {liq_count} 次\n"

    total_pnl = total_equity - total_invested_all
    if total_invested_all <= 0: total_pnl_pct = 999.9 
    else: total_pnl_pct = (total_pnl / total_invested_all) * 100

    current_utc = datetime.utcnow().strftime("%H:%M")
    
    title_parts = []
    if rotated_ids: title_parts.append(f"🔄S{','.join(rotated_ids)}")
    if closed_only_ids: title_parts.append(f"🛑S{','.join(closed_only_ids)}")
    if liquidated_ids: title_parts.append(f"💥{len(liquidated_ids)}个")
    
    title_base = f"投{total_invested_all:.0f} 剩{total_equity:.0f} ({total_pnl_pct:+.1f}%)"
    if title_parts: title = f"{' '.join(title_parts)} | {title_base}"
    else: title = f"策略日报: {title_base}"
    
    switch_status = []
    if ENABLE_COMPOUNDING: switch_status.append("🔥复利开启")
    else: switch_status.append("🔒单利模式")
    if ENABLE_ROI_PAYBACK: switch_status.append("💰回本开启")
    
    description = f"""
**UTC 时间**: {current_utc}
**模式**: {" ".join(switch_status)}
**总投入**: {total_invested_all:.0f} U (含已提取)
**总净值**: {total_equity:.0f} U
**总盈亏**: {total_pnl:+.1f} U

---
{md_table}
---
### 📝 动态与持仓
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
        
        # 1. 风控 (仅输出 summary，除非爆仓)
        liquidated_ids = check_risk_management(opener, data, market_map)
        
        # 2. 智能扫描
        rotated_ids, closed_only_ids = scan_and_execute_strategies(opener, data, market_map, top_10)
        
        # 3. 记录净值
        record_equity_snapshot(data, market_map)
        
        save_state(data)
        
        if rotated_ids or closed_only_ids or liquidated_ids:
            report_to_wechat(opener, data, market_map, rotated_ids, closed_only_ids, liquidated_ids)
