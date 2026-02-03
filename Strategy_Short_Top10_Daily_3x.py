import urllib.request
import urllib.parse
import json
import time
import os
import csv
from datetime import datetime

# ==========================================
#               策略核心配置
# ==========================================
PROXY_ADDR = "127.0.0.1:10808"
STATE_FILE = "strategy_state.json"
HISTORY_FILE = "strategy_history.csv"
EQUITY_FILE = "equity_curve.csv"

# --- [新功能开关] ---
# 1. 复利滚雪球开关
# True = 开启复利 (有多少钱开多少仓)
# False = 关闭复利 (大于1000U时只用1000U开仓，多余的留作缓冲；小于1000U时全仓)
ENABLE_COMPOUNDING = True 

# 2. 回本/提现机制开关
# True = 开启回本 (当余额 >= 2000U 时，提取 1000U 出来，减少累计投入)
# False = 关闭回本 (钱一直留在策略里)
ENABLE_ROI_PAYBACK = True

# --- [资金参数] ---
INITIAL_UNIT = 1000.0     # 标准开仓/复活金额
POSITIONS_COUNT = 10      # 持仓数量
LEVERAGE = 3.0            # 杠杆倍数
MIN_ALIVE_BALANCE = 10.0  # “存活”阈值：低于10U视为无法开单，强制复活

HEADERS = {'User-Agent': 'Mozilla/5.0'}
SERVERCHAN_KEY = os.environ.get("SERVERCHAN_KEY")

# ==========================================
#               网络与基础函数
# ==========================================

def get_proxy_opener():
    """获取带有代理配置的请求Opener"""
    proxy_handler = urllib.request.ProxyHandler({
        'http': f'http://{PROXY_ADDR}',
        'https': f'http://{PROXY_ADDR}'
    })
    return urllib.request.build_opener(proxy_handler)

def get_data(opener, url):
    """通用HTTP请求函数"""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with opener.open(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"❌ 请求失败 [{url}]: {e}")
        return None

def get_market_rank(opener):
    """获取24小时涨幅榜 Top 10"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    data = get_data(opener, url)
    if not data: return {}, []
    
    market_map = {}
    rank_list = []
    current_ts = int(time.time() * 1000)
    
    for item in data:
        symbol = item['symbol']
        # 过滤掉超过10分钟无成交的僵尸币种
        if current_ts - int(item['closeTime']) > 10 * 60 * 1000:
            continue
        price = float(item['lastPrice'])
        change = float(item['priceChangePercent'])
        market_map[symbol] = price
        rank_list.append({'symbol': symbol, 'change': change, 'price': price})
        
    # 按涨幅降序排列，取前10
    rank_list.sort(key=lambda x: x['change'], reverse=True)
    return market_map, rank_list[:POSITIONS_COUNT]

def get_recent_high_price(opener, symbol):
    """获取过去15分钟K线的最高价 (用于插针检测)"""
    safe_symbol = urllib.parse.quote(symbol)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={safe_symbol}&interval=15m&limit=1"
    data = get_data(opener, url)
    if data and len(data) > 0:
        return float(data[0][2])
    return 0.0

def calculate_strategy_equity(strategy, market_map, opener=None, use_high_price=False):
    """
    核心计算函数：计算策略当前的剩余净值
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
            
            curr = market_map.get(symbol, entry)
            calc_price = curr
            warn_msg = ""
            
            # 如果需要插针检测 (Opener 不为空且指定了 use_high_price)
            if opener and use_high_price:
                high_15m = get_recent_high_price(opener, symbol)
                if high_15m > 0:
                    calc_price = max(curr, high_15m)
                    if high_15m > entry * 1.05: warn_msg = "⚠️"

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

def log_to_csv(record_type, strategy_id, symbol, price, high_price, amount, pos_pnl, equity, total_invested, note=""):
    """
    日志记录函数
    优化：控制台打印所有日志，但CSV文件只记录关键交易事件，避免文件膨胀。
    """
    file_exists = os.path.isfile(HISTORY_FILE)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    equity_val = float(equity)
    invested_val = float(total_invested)
    
    # 1. 控制台日志：永远打印，方便在线调试
    print(f"📝 [CSV] {record_type:<10} 策略{strategy_id:<2} {symbol:<8} 净值:{equity_val:.0f} 投入:{invested_val:.0f} | {note}")

    # 2. CSV文件过滤：只记录真正的资金变动
    # 白名单：OPEN(开仓), CLOSE(平仓), LIQUIDATION(爆仓), REPLENISH(补钱), WITHDRAW(提钱)
    # 被过滤：MONITOR(监控), INFO(信息), SKIP(跳过)
    CRITICAL_EVENTS = ["OPEN", "CLOSE", "LIQUIDATION", "REPLENISH", "WITHDRAW"]
    
    if record_type not in CRITICAL_EVENTS:
        return # 如果不是关键事件，直接结束，不写入文件

    try:
        with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", "Note"])
            writer.writerow([current_time, strategy_id, record_type, symbol, price, high_price, amount, pos_pnl, equity_val, invested_val, note])
    except Exception as e:
        print(f"❌ 写入历史CSV失败: {e}")

def record_equity_snapshot(data, market_map):
    """记录净值曲线，用于后续画图"""
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

# ==========================================
#               状态管理函数
# ==========================================

def load_state():
    """加载策略状态，包含数据结构自动升级"""
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
    # 兼容旧版本数据
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
    """
    风控检查：检测是否爆仓
    如果爆仓，只清零余额和仓位，记录次数，不立即补钱（等到轮动时补）
    """
    print("\n🛡️ [监控] 开始全仓风控检查 (含插针检测)...")
    liquidated_ids = [] 
    
    for s_id in data:
        strategy = data[s_id]
        # 如果已经没钱了且没仓位，说明已经死透了等待复活，跳过检查
        if strategy['balance'] <= 0 and not strategy['positions']:
            continue
            
        equity, details = calculate_strategy_equity(strategy, market_map, opener, use_high_price=True)
        invested = strategy.get('total_invested', INITIAL_UNIT)

        # 这里的 MONITOR 日志只会在控制台打印，不会写入CSV文件（被log_to_csv过滤了）
        for d in details:
            log_to_csv("MONITOR", s_id, d['symbol'], d['curr'], d['calc_price'], d['amount'], d['pnl'], equity, invested, "全仓监控")

        # 爆仓判定
        if equity <= 0:
            print(f"💥 策略 {s_id} 触发全仓爆仓! 净值归零")
            liquidated_ids.append(s_id)
            for d in details:
                # 爆仓是关键事件，会写入CSV
                log_to_csv("LIQUIDATION", s_id, d['symbol'], d['calc_price'], d['calc_price'], d['amount'], d['pnl'], 0, invested, "全仓强平")
            
            strategy['balance'] = 0
            strategy['positions'] = []
            strategy['liquidation_count'] = strategy.get('liquidation_count', 0) + 1
            
    return liquidated_ids

def execute_rotation(opener, data, market_map, top_10):
    """
    策略轮动/补单逻辑
    包含：平旧仓、复活补钱、回本提现、开新仓
    """
    current_hour = str(datetime.utcnow().hour)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    strategy = data[current_hour]
    
    # 如果今天已经在这个小时操作过了，直接退出
    if strategy['last_trade_date'] == today_str:
        return None

    print(f"\n🔄 [执行] 策略 {current_hour} 轮动逻辑...")
    
    # 1. 平旧仓
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
    
    current_equity = strategy['balance']
    
    # 2. 复活检测 (只有死透了才补钱)
    if current_equity < MIN_ALIVE_BALANCE:
        print(f"💀 策略 {current_hour} 已归零，执行复活程序...")
        strategy['balance'] = INITIAL_UNIT
        strategy['total_invested'] += INITIAL_UNIT
        current_equity = strategy['balance']
        log_to_csv("REPLENISH", current_hour, "USDT", 0, 0, 0, 0, current_equity, strategy['total_invested'], "爆仓后重新投入")
    
    # 3. 回本机制 (如果开启且赚够了)
    elif ENABLE_ROI_PAYBACK and current_equity >= (INITIAL_UNIT * 2):
        withdraw_amount = INITIAL_UNIT
        strategy['balance'] -= withdraw_amount
        strategy['total_invested'] -= withdraw_amount # 减少投入记录
        print(f"💰 策略 {current_hour} 触发回本机制: 提取 {withdraw_amount}U!")
        log_to_csv("WITHDRAW", current_hour, "USDT", 0, 0, 0, 0, strategy['balance'], strategy['total_invested'], "回本提取")
        current_equity = strategy['balance'] 

    # 4. 开新仓 (根据复利开关决定本金)
    trading_capital = current_equity
    if not ENABLE_COMPOUNDING:
        # 锁定注码模式：无论赚多少，只用初始本金开仓
        if trading_capital > INITIAL_UNIT:
            trading_capital = INITIAL_UNIT
            print(f"🔒 策略 {current_hour} 关闭复利: 余额 {current_equity:.1f}U, 限制开仓资金为 {trading_capital}U")
    
    if trading_capital < 1.0: 
        log_to_csv("SKIP", current_hour, "ALL", 0, 0, 0, 0, current_equity, strategy['total_invested'], "资金不足")
    else:
        margin_per_coin = trading_capital / POSITIONS_COUNT
        
        top10_str = "|".join([x['symbol'] for x in top_10])
        log_to_csv("INFO", current_hour, "TOP10_LIST", 0, 0, 0, 0, current_equity, strategy['total_invested'], top10_str)

        new_positions = []
        # --- [修改点] 获取当前秒级时间戳，用于计算持仓时长 ---
        entry_ts = int(time.time())
        
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
                "entry_time": entry_ts # 记录开仓时间
            })
            log_to_csv("OPEN", current_hour, symbol, price, price, amount, 0, current_equity, strategy['total_invested'], "开空")
            
        strategy['positions'] = new_positions

    strategy['last_trade_date'] = today_str
    return current_hour

# ==========================================
#               通知与主程序
# ==========================================

def report_to_wechat(opener, data, market_map, rotated_id, liquidated_ids):
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

        # 状态图标逻辑
        icon = "🔴" if net_pnl < 0 else "🟢"
        if equity == 0: icon = "💀" 
        elif s_id == rotated_id: icon = "🔄"
        
        liq_str = str(liq_count) if liq_count > 0 else "-"
        inv_display = f"{invested:.0f}"
        
        md_table += f"| {s_id} | {inv_display} | {equity:.0f} | {icon}{net_pnl:+.0f} | {liq_str} |\n"

        # 生成持仓详情
        pos_len = len(strat['positions'])
        if pos_len > 0:
            prefix = "🔄" if s_id == rotated_id else ""
            
            # --- [新增] 计算占用保证金 & 持仓时长 ---
            # 保证金总和 (初始押进去多少钱)
            used_margin = sum([p.get('margin', 0) for p in strat['positions']])
            
            # 持仓时长 (取第一个仓位的时间即可，因为是全仓同开同平)
            first_pos = strat['positions'][0]
            entry_time = first_pos.get('entry_time', 0)
            duration_str = "-"
            if entry_time > 0:
                duration_hours = (current_ts - entry_time) / 3600
                duration_str = f"{duration_hours:.1f}h"
            
            # 爆仓标记 (如果有爆过仓，显眼展示)
            liq_mark = f" 💀x{liq_count}" if liq_count > 0 else ""
            
            # 组合头部信息: 投:2000 爆:x1 押:990 ⏱️4.5h
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

    # 全局统计
    total_pnl = total_equity - total_invested_all
    if total_invested_all <= 0: total_pnl_pct = 999.9 
    else: total_pnl_pct = (total_pnl / total_invested_all) * 100

    current_utc = datetime.utcnow().strftime("%H:%M")
    
    # 动态标题
    title_parts = []
    if rotated_id: title_parts.append(f"🔄S{rotated_id}")
    if liquidated_ids: title_parts.append(f"💥{len(liquidated_ids)}个")
    
    title_base = f"投{total_invested_all:.0f} 剩{total_equity:.0f} ({total_pnl_pct:+.1f}%)"
    if title_parts: title = f"{' '.join(title_parts)} | {title_base}"
    else: title = f"策略日报: {title_base}"
    
    # 描述中的开关状态
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
        
        # 1. 风控 (Monitor 和 Liquidation 会发生在这里)
        liquidated_ids = check_risk_management(opener, data, market_map)
        
        # 2. 轮动 (Open, Close, Replenish, Withdraw 会发生在这里)
        rotated_id = execute_rotation(opener, data, market_map, top_10)
        
        # 3. 记录净值 (仅用于画图，不写history csv)
        record_equity_snapshot(data, market_map)
        
        save_state(data)
        
        # 只有在有重要事件时才发微信
        if rotated_id or liquidated_ids:
            report_to_wechat(opener, data, market_map, rotated_id, liquidated_ids)
