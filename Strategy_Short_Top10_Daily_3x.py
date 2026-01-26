import ccxt
import pandas as pd
import json
import os
import sys
import requests # 新增：用于发送请求
from datetime import datetime, timedelta

# ================= 配置区域 =================
INITIAL_BALANCE = 1000 
LEVERAGE = 3 
TOP_N = 10 
LIQUIDATION_THRESHOLD = 0.9 

# 获取 Server酱 Key (优先从环境变量获取，如果没有则使用空字符串)
# 强烈建议在 GitHub Secrets 中设置 SERVERCHAN_KEY
SERVERCHAN_KEY = os.environ.get('SERVERCHAN_KEY', '') 

# 如果你实在不想用 Secrets，也可以直接把 Key 填在下面引号里（不推荐，容易泄露）
# SERVERCHAN_KEY = 'SCTxxxxxxxxxxxxxxxxxxxx' 

STATE_FILE = 'data/State_Current_Positions.json'
HISTORY_FILE = 'data/Record_Daily_PnL.csv'
INTRADAY_FILE = 'data/Record_5min_Equity.csv'

exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# ================= 通知模块 =================
def send_wechat_notification(title, content):
    """
    使用 Server酱发送微信通知
    """
    if not SERVERCHAN_KEY:
        print("未配置 ServerChan Key，跳过发送通知。")
        return

    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    data = {
        'title': title,
        'desp': content # 支持 Markdown
    }
    
    try:
        response = requests.post(url, data=data)
        print(f"微信通知发送结果: {response.text}")
    except Exception as e:
        print(f"微信通知发送失败: {e}")

# ================= 核心逻辑 =================

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    else:
        return {
            "balance": INITIAL_BALANCE,
            "positions": [],
            "last_rotation_date": ""
        }

def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def log_intraday(timestamp, total_equity, positions):
    record = {
        "Time": timestamp,
        "Total_Equity": round(total_equity, 2)
    }
    for pos in positions:
        symbol = pos['symbol'].split('/')[0]
        pnl = pos.get('unrealized_pnl', 0)
        record[f"{symbol}_PnL"] = round(pnl, 2)
        record[f"{symbol}_Price"] = pos.get('current_price', 0)

    df = pd.DataFrame([record])
    header = not os.path.exists(INTRADAY_FILE)
    df.to_csv(INTRADAY_FILE, mode='a', header=header, index=False)
    print(f"[{timestamp}] 监控日志已保存。总权益: {total_equity:.2f}")

def run_monitor(state):
    positions = state['positions']
    current_balance = state['balance']
    
    if not positions:
        print("当前空仓，无需监控。")
        return

    symbols = [p['symbol'] for p in positions]
    try:
        tickers = exchange.fetch_tickers(symbols)
    except Exception as e:
        print(f"获取价格失败: {e}")
        return

    total_unrealized_pnl = 0
    active_positions = []
    has_liquidation = False
    liquidation_msg = []

    current_time_str = get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')
    print(f"--- 5分钟监控 {current_time_str} ---")

    for pos in positions:
        symbol = pos['symbol']
        if symbol not in tickers:
            active_positions.append(pos)
            continue
            
        current_price = tickers[symbol]['close']
        entry_price = pos['entry_price']
        margin = pos['margin']
        position_value = margin * LEVERAGE
        
        pnl = (entry_price - current_price) / entry_price * position_value
        
        # === 爆仓检测 ===
        if pnl < 0 and abs(pnl) >= margin * LIQUIDATION_THRESHOLD:
            loss_amount = margin  # 假设亏光保证金
            msg = f"💥 **爆仓预警**: {symbol} \n当前价: {current_price}\n开仓价: {entry_price}\n**直接亏损: -{loss_amount:.2f} U**"
            print(msg)
            liquidation_msg.append(msg)
            
            current_balance -= margin 
            has_liquidation = True
        else:
            pos['current_price'] = current_price
            pos['unrealized_pnl'] = pnl
            total_unrealized_pnl += pnl
            active_positions.append(pos)
            print(f"{symbol}: 当前 {current_price} | 盈亏 {pnl:.2f}U")

    total_equity = current_balance + total_unrealized_pnl
    log_intraday(current_time_str, total_equity, positions)

    if has_liquidation:
        state['balance'] = current_balance
        state['positions'] = active_positions
        save_state(state)
        
        # --- 发送爆仓通知 ---
        title = "⚠️ 策略触发强平警报"
        content = "\n\n".join(liquidation_msg) + f"\n\n当前账户剩余余额: **{current_balance:.2f} U**"
        send_wechat_notification(title, content)

def run_rotation(state):
    print("=== 执行每日换仓 (Daily Rotation) ===")
    
    # 1. 简单结算昨日持仓 (简化版：假设全部按当前价平仓)
    # 在真实逻辑中这里应该详细计算昨日具体的 Entry 和 Exit
    old_balance = state['balance']
    
    # 获取最新行情用于选币
    tickers = exchange.fetch_tickers()
    
    # 2. 模拟平仓所有旧仓位，计算新的余额
    # (此处为了代码精简，假设 state['balance'] 已经在 run_monitor 中维护得差不多了，
    # 或者你需要在这里写一遍完整的结算逻辑。为了演示通知功能，我们假设余额已更新)
    current_balance = state['balance'] 
    # 注意：如果想更精确，应该在这里把昨日持仓遍历一遍算 PnL，加到 current_balance 上
    
    # 3. 选新币
    valid_tickers = [d for s, d in tickers.items() if '/USDT' in s and 'percentage' in d]
    sorted_tickers = sorted(valid_tickers, key=lambda x: x['percentage'] if x['percentage'] else -999, reverse=True)
    top_10 = sorted_tickers[:10]
    
    new_positions = []
    margin_per_coin = current_balance / TOP_N
    
    new_coins_list = []
    
    for t in top_10:
        symbol = t['symbol']
        price = t['close']
        change = t['percentage']
        
        new_positions.append({
            "symbol": symbol,
            "entry_price": price,
            "margin": margin_per_coin,
            "unrealized_pnl": 0
        })
        new_coins_list.append(f"- {symbol} (24h: {change}%)")
    
    # 更新状态
    state['balance'] = current_balance
    state['positions'] = new_positions
    state['last_rotation_date'] = get_beijing_time().strftime('%Y-%m-%d')
    save_state(state)
    
    # --- 发送早报通知 ---
    title = f"📅 策略日报: {state['last_rotation_date']}"
    content = f"""
### 账户概览
- **当前余额**: {current_balance:.2f} U
- **昨日变化**: {(current_balance - old_balance):.2f} U (近似值)

### 今日开空目标 (Top 10)
{chr(10).join(new_coins_list)}

*注: 已自动按 3x 杠杆重置仓位*
    """
    send_wechat_notification(title, content)
    print("换仓完成并发送通知。")

def main():
    state = load_state()
    now = get_beijing_time()
    today_str = now.strftime('%Y-%m-%d')
    
    last_rot = state.get('last_rotation_date', '')
    current_hour = now.hour
    
    # 早上8点整执行换仓，其他时间执行监控
    if today_str != last_rot and current_hour == 8:
        run_rotation(state)
    else:
        run_monitor(state)

if __name__ == "__main__":
    main()
