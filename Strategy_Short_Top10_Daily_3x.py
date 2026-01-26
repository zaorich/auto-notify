import requests
import pandas as pd
import json
import os
import sys
import time
from datetime import datetime, timedelta

# ================= 🔧 策略配置区域 =================
# 初始资金 (仅用于第一次运行初始化)
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

# 如果你在本地或特殊网络环境下运行，可以在这里配置代理
# 例如: PROXIES = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
PROXIES = None 

# 数据文件路径
STATE_FILE = 'data/State_Current_Positions.json'
HISTORY_FILE = 'data/Record_Daily_PnL.csv'
INTRADAY_FILE = 'data/Record_5min_Equity.csv'

# API 基础地址
BASE_URL = "https://fapi.binance.com"

# ================= 🛠️ 辅助函数 =================

def get_beijing_time():
    """获取北京时间 (UTC+8)"""
    return datetime.utcnow() + timedelta(hours=8)

def send_wechat_notification(title, content):
    """发送微信通知"""
    if not SERVERCHAN_KEY:
        # print("❌ 未配置 SERVERCHAN_KEY，跳过发送通知")
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

# ================= 📡 数据获取模块 (参考 HTML 逻辑) =================

def get_valid_symbols():
    """
    获取符合条件的交易对：
    1. 合约类型 = PERPETUAL (永续)
    2. 状态 = TRADING (交易中)
    3. 计价货币 = USDT
    """
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url, timeout=10, proxies=PROXIES)
        response.raise_for_status()
        data = response.json()
        
        valid_set = set()
        for s in data['symbols']:
            if (s['contractType'] == 'PERPETUAL' and 
                s['status'] == 'TRADING' and 
                s['quoteAsset'] == 'USDT'):
                valid_set.add(s['symbol'])
        return valid_set
    except Exception as e:
        print(f"❌ 获取交易规则失败: {e}")
        return set()

def get_current_prices(symbol_list=None):
    """
    获取最新价格
    如果传入 symbol_list，则只返回这些币的价格字典
    """
    url = f"{BASE_URL}/fapi/v1/ticker/price"
    try:
        response = requests.get(url, timeout=10, proxies=PROXIES)
        response.raise_for_status()
        data = response.json()
        
        prices = {}
        for item in data:
            sym = item['symbol']
            # 如果指定了列表，只存列表里的；否则全存
            if symbol_list is None or sym in symbol_list:
                prices[sym] = float(item['price'])
        return prices
    except Exception as e:
        print(f"❌ 获取价格失败: {e}")
        return {}

def get_top_gainers_data(top_n=10):
    """
    获取 24小时涨幅榜 Top N
    """
    # 1. 先获取有效白名单
    valid_symbols = get_valid_symbols()
    if not valid_symbols:
        return []

    # 2. 获取所有 24hr 统计数据
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"
    try:
        response = requests.get(url, timeout=10, proxies=PROXIES)
        response.raise_for_status()
        tickers = response.json()
        
        filtered_data = []
        for t in tickers:
            symbol = t['symbol']
            if symbol in valid_symbols:
                try:
                    # 过滤成交额过小的 (例如小于 1000万 U)
                    quote_vol = float(t['quoteVolume'])
                    if quote_vol < 10000000: 
                        continue
                        
                    filtered_data.append({
                        'symbol': symbol,
                        'price': float(t['lastPrice']),
                        'change': float(t['priceChangePercent']),
                        'volume': quote_vol
                    })
                except:
                    continue
        
        # 3. 排序：按涨幅降序
        df = pd.DataFrame(filtered_data)
        if df.empty:
            return []
            
        df_sorted = df.sort_values(by='change', ascending=False)
        return df_sorted.head(top_n).to_dict('records')

    except Exception as e:
        print(f"❌ 获取行情失败: {e}")
        return []

# ================= 📉 核心逻辑：5分钟监控 =================

def run_monitor(state):
    positions = state['positions']
    current_balance = state['balance']
    
    if not positions:
        print("当前无持仓，监控跳过。")
        return

    # 1. 获取持仓币种的最新价格
    target_symbols = [p['symbol'] for p in positions]
    current_prices = get_current_prices(target_symbols)
    
    if not current_prices:
        print("❌ 无法获取最新价格，本次监控中止")
        return

    total_unrealized_pnl = 0
    active_positions = []
    has_liquidation = False
    liquidation_msgs = []

    print(f"--- 5分钟监控 ({get_beijing_time().strftime('%H:%M:%S')}) ---")

    for pos in positions:
        symbol = pos['symbol']
        
        # 如果获取不到价格，保留原状态
        if symbol not in current_prices:
            active_positions.append(pos)
            continue
            
        current_price = current_prices[symbol]
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
            # 爆仓后该仓位移除
        else:
            pos['current_price'] = current_price
            pos['unrealized_pnl'] = pnl
            total_unrealized_pnl += pnl
            active_positions.append(pos)
            # print(f"{symbol}: 浮动盈亏 {pnl:.2f} U")

    # 2. 记录 5分钟 资金曲线
    total_equity = current_balance + total_unrealized_pnl
    
    record = {"Time": get_beijing_time().strftime('%Y-%m-%d %H:%M:%S'), "Total_Equity": round(total_equity, 2)}
    # 记录每个币的明细
    for p in active_positions:
        sym_name = p['symbol'] #.replace('USDT', '')
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
    
    # 1. 结算旧仓位
    old_positions = state['positions']
    current_balance = state['balance']
    pnl_today = 0
    
    if old_positions:
        print("正在结算昨日持仓...")
        # 获取旧仓位的当前价格用于平仓
        old_symbols = [p['symbol'] for p in old_positions]
        exit_prices = get_current_prices(old_symbols)
        
        for pos in old_positions:
            symbol = pos['symbol']
            if symbol in exit_prices:
                exit_price = exit_prices[symbol]
                entry_price = pos['entry_price']
                margin = pos['margin']
                pos_val = margin * LEVERAGE
                
                # 盈亏计算 (做空)
                raw_pnl = (entry_price - exit_price) / entry_price * pos_val
                # 扣除手续费
                fee = pos_val * FEE_RATE
                net_pnl = raw_pnl - fee
                
                pnl_today += net_pnl
                current_balance += net_pnl
            else:
                print(f"⚠️ {symbol} 无法获取价格，假设平价平仓")
    
    print(f"昨日持仓结算盈亏: {pnl_today:.2f} U")
    print(f"最新可用余额: {current_balance:.2f} U")
    
    if current_balance <= 10:
        print("💸 账户余额不足，策略停止。")
        send_wechat_notification("☠️ 策略已破产", f"剩余余额: {current_balance} U")
        return

    # 2. 选新币 (Top 10 Gainers)
    print("正在获取涨幅榜 Top 10...")
    top_10 = get_top_gainers_data(TOP_N)
    
    if not top_10:
        print("❌ 无法获取涨幅榜数据，换仓失败 (保持空仓)")
        # 保存状态清空持仓，避免数据错乱
        state['balance'] = current_balance
        state['positions'] = []
        save_state(state)
        return

    # 3. 开新仓
    new_positions = []
    margin_per_coin = current_balance / TOP_N
    msg_lines = []
    
    for t in top_10:
        sym = t['symbol']
        price = t['price']
        change = t['change']
        
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
    print("✅ 换仓完成")

# ================= 🚀 主程序入口 =================

if __name__ == "__main__":
    if not os.path.exists('data'):
        os.makedirs('data')

    state = load_state()
    now_bj = get_beijing_time()
    today_str = now_bj.strftime('%Y-%m-%d')
    
    last_rot = state.get('last_rotation_date', '')
    
    # 逻辑判断：每天早上8点 (08:00 - 08:59) 执行且仅执行一次换仓
    if today_str != last_rot and now_bj.hour == 8:
        run_rotation(state)
    else:
        run_monitor(state)
