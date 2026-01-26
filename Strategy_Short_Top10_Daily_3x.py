import ccxt
import pandas as pd
import json
import os
import sys
import requests
from datetime import datetime, timedelta

# ================= 🔧 策略配置区域 =================
# 初始资金 (仅用于第一次运行初始化，后续会读取 json 里的余额)
INITIAL_BALANCE = 1000 
# 杠杆倍数
LEVERAGE = 3 
# 持仓数量
TOP_N = 10 
# 强平阈值 (亏损达到保证金的 90% 视为爆仓)
LIQUIDATION_THRESHOLD = 0.9 
# 预估交易手续费 (双边万分之五) + 滑点
FEE_RATE = 0.001 

# Server酱 Key (从环境变量获取，安全)
SERVERCHAN_KEY = os.environ.get('SERVERCHAN_KEY', '')

# 数据文件路径
STATE_FILE = 'data/State_Current_Positions.json'
HISTORY_FILE = 'data/Record_Daily_PnL.csv'
INTRADAY_FILE = 'data/Record_5min_Equity.csv'

# 初始化币安交易所 (仅获取行情，不需要 API Key)
# 修改后的代码
exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'},
    # 👇 增加这一行，利用公共代理绕过 IP 限制
    'proxy': 'https://corsproxy.io/?', 
})

# ================= 🛠️ 辅助函数 =================

def get_beijing_time():
    """获取北京时间 (UTC+8)"""
    return datetime.utcnow() + timedelta(hours=8)

def send_wechat_notification(title, content):
    """发送微信通知"""
    if not SERVERCHAN_KEY:
        print("❌ 未配置 SERVERCHAN_KEY，跳过发送通知")
        return

    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    data = {'title': title, 'desp': content}
    try:
        requests.post(url, data=data, timeout=10)
        print("✅ 微信通知已发送")
    except Exception as e:
        print(f"❌ 微信通知发送失败: {e}")

def load_state():
    """读取账户状态"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    # 初始化状态
    return {
        "balance": INITIAL_BALANCE,
        "positions": [],
        "last_rotation_date": ""
    }

def save_state(state):
    """保存账户状态"""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def append_history(date, balance, pnl, notes):
    """记录每日结算历史"""
    file_exists = os.path.exists(HISTORY_FILE)
    df = pd.DataFrame([{
        "Date": date,
        "Total_Equity": round(balance, 2),
        "Daily_PnL": round(pnl, 2),
        "Notes": notes
    }])
    df.to_csv(HISTORY_FILE, mode='a', header=not file_exists, index=False)

# ================= 📉 核心逻辑：5分钟监控 =================

def run_monitor(state):
    positions = state['positions']
    current_balance = state['balance']
    
    # 如果空仓，直接跳过
    if not positions:
        print("当前无持仓，监控跳过。")
        return

    # 1. 获取最新价格
    symbols = [p['symbol'] for p in positions]
    try:
        tickers = exchange.fetch_tickers(symbols)
    except Exception as e:
        print(f"行情获取失败: {e}")
        return

    total_unrealized_pnl = 0
    active_positions = []
    has_liquidation = False
    liquidation_msgs = []

    print(f"--- 5分钟监控 ({get_beijing_time().strftime('%H:%M:%S')}) ---")

    for pos in positions:
        symbol = pos['symbol']
        if symbol not in tickers:
            # 如果获取不到价格，保留原样
            active_positions.append(pos)
            continue
            
        current_price = tickers[symbol]['close']
        entry_price = pos['entry_price']
        margin = pos['margin']
        position_value = margin * LEVERAGE
        
        # 计算做空盈亏: (Entry - Current) / Entry * Value
        # 价格跌(Current < Entry) -> 盈利
        pnl = (entry_price - current_price) / entry_price * position_value
        
        # === 🚨 爆仓检测 ===
        # 如果亏损超过保证金的 90%
        if pnl < 0 and abs(pnl) >= margin * LIQUIDATION_THRESHOLD:
            loss = margin # 亏光保证金
            msg = f"💥 **爆仓预警**: {symbol}\n现价: {current_price} | 开仓: {entry_price}\n单币亏损: -{loss:.2f} U"
            print(msg)
            liquidation_msgs.append(msg)
            
            current_balance -= margin 
            has_liquidation = True
            # 爆仓后该仓位移除，不再进入 active_positions
        else:
            pos['current_price'] = current_price
            pos['unrealized_pnl'] = pnl
            total_unrealized_pnl += pnl
            active_positions.append(pos)
            # print(f"{symbol}: {pnl:.2f} U") # 调试用，避免日志过长可注释

    # 2. 记录 5分钟 资金曲线
    total_equity = current_balance + total_unrealized_pnl
    
    record = {"Time": get_beijing_time().strftime('%Y-%m-%d %H:%M:%S'), "Total_Equity": round(total_equity, 2)}
    # 记录每个币的明细
    for p in active_positions:
        sym_name = p['symbol'].split('/')[0]
        record[f"{sym_name}_PnL"] = round(p.get('unrealized_pnl', 0), 2)
        
    df = pd.DataFrame([record])
    header = not os.path.exists(INTRADAY_FILE)
    df.to_csv(INTRADAY_FILE, mode='a', header=header, index=False)
    print(f"✅ 监控完成。当前动态权益: {total_equity:.2f} U")

    # 3. 处理爆仓更新
    if has_liquidation:
        state['balance'] = current_balance
        state['positions'] = active_positions
        save_state(state)
        # 发送爆仓通知
        send_wechat_notification("⚠️ 紧急：策略触发强平", "\n\n".join(liquidation_msgs) + f"\n\n当前余额: {current_balance:.2f} U")

# ================= 🔄 核心逻辑：每日换仓 =================

def run_rotation(state):
    print("=== 开始执行每日换仓 (Daily Rotation) ===")
    
    # 1. 结算旧仓位 (Settlement)
    old_positions = state['positions']
    current_balance = state['balance']
    pnl_today = 0
    
    if old_positions:
        print("正在结算昨日持仓...")
        symbols = [p['symbol'] for p in old_positions]
        try:
            tickers = exchange.fetch_tickers(symbols)
            for pos in old_positions:
                symbol = pos['symbol']
                if symbol in tickers:
                    exit_price = tickers[symbol]['close']
                    entry_price = pos['entry_price']
                    margin = pos['margin']
                    pos_val = margin * LEVERAGE
                    
                    # 盈亏计算
                    raw_pnl = (entry_price - exit_price) / entry_price * pos_val
                    # 扣除手续费
                    fee = pos_val * FEE_RATE
                    net_pnl = raw_pnl - fee
                    
                    pnl_today += net_pnl
                    current_balance += net_pnl
                else:
                    # 如果币下架了，假设按原价平仓（极端情况需人工干预）
                    print(f"⚠️ {symbol} 无法获取价格，跳过结算")
        except Exception as e:
            print(f"❌ 结算失败，停止换仓: {e}")
            return # 遇到严重网络错误，中止换仓，等待下一个周期

    print(f"昨日持仓结算盈亏: {pnl_today:.2f} U")
    print(f"最新可用余额: {current_balance:.2f} U")
    
    # 如果余额归零，停止策略
    if current_balance <= 10: # 留点余量
        print("💸 账户余额不足，策略停止。")
        send_wechat_notification("☠️ 策略已破产", f"剩余余额: {current_balance} U")
        return

    # 2. 选新币 (Screening)
    print("正在获取涨幅榜 Top 10...")
    all_tickers = exchange.fetch_tickers()
    valid_tickers = [d for s, d in all_tickers.items() if '/USDT' in s and 'percentage' in d]
    sorted_tickers = sorted(valid_tickers, key=lambda x: x['percentage'] if x['percentage'] else -999, reverse=True)
    top_10 = sorted_tickers[:TOP_N]
    
    # 3. 开新仓 (Opening)
    new_positions = []
    margin_per_coin = current_balance / TOP_N
    msg_lines = []
    
    for t in top_10:
        sym = t['symbol']
        price = t['close']
        change = t['percentage']
        
        new_positions.append({
            "symbol": sym,
            "entry_price": price,
            "margin": margin_per_coin,
            "unrealized_pnl": 0
        })
        msg_lines.append(f"- {sym} (涨幅: {change:.1f}%)")
        print(f"拟开空: {sym} @ {price}")

    # 4. 保存状态
    state['balance'] = current_balance
    state['positions'] = new_positions
    today_str = get_beijing_time().strftime('%Y-%m-%d')
    state['last_rotation_date'] = today_str
    save_state(state)
    
    # 5. 记录历史并发送通知
    append_history(today_str, current_balance, pnl_today, "Auto Rotation")
    
    notify_content = f"""
### 📊 每日结算报告
- **日期**: {today_str}
- **昨日盈亏**: {pnl_today:+.2f} U
- **当前余额**: {current_balance:.2f} U

### 🔫 今日开空目标 (3x)
{chr(10).join(msg_lines)}
    """
    send_wechat_notification(f"📅 策略日报: {current_balance:.0f} U", notify_content)

# ================= 🚀 主程序入口 =================

if __name__ == "__main__":
    # 确保 data 目录存在
    if not os.path.exists('data'):
        os.makedirs('data')

    state = load_state()
    now_bj = get_beijing_time()
    today_str = now_bj.strftime('%Y-%m-%d')
    
    # 逻辑判断：
    # 如果 [今天还没换过仓] 且 [现在是早上8点 (08:00-08:59)] -> 执行换仓
    # 否则 -> 执行5分钟监控
    
    last_rot = state.get('last_rotation_date', '')
    
    # if today_str != last_rot and now_bj.hour == 8:
    #     run_rotation(state)
    # else:
    #     run_monitor(state)
    # 强制执行换仓（测试用，测完记得改回去！）
    # if today_str != last_rot and now_bj.hour == 8: 
    run_rotation(state)  # <--- 直接调用这个函数，不要 if 判断
    # else:
    #    run_monitor(state)
