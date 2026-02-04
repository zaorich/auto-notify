import pandas as pd
import numpy as np
import os
import requests
import urllib.parse
from datetime import datetime

# ================= 配置区域 =================
HISTORY_FILE = 'strategy_history.csv'
EQUITY_FILE = 'equity_curve.csv'
SERVERCHAN_KEY = os.environ.get("SERVERCHAN_KEY")
# ===========================================

def send_wechat_msg(title, content):
    """发送微信通知"""
    if not SERVERCHAN_KEY:
        print(f"⚠️ 未配置 SERVERCHAN_KEY，只打印不发送。\n标题: {title}\n内容:\n{content}")
        return

    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    params = {'title': title, 'desp': content}
    try:
        data = urllib.parse.urlencode(params).encode('utf-8')
        req = requests.post(url, data=params)
        print(f"✅ 微信推送完成: {req.status_code}")
    except Exception as e:
        print(f"❌ 微信发送失败: {e}")

def calculate_max_drawdown(equity_series):
    """计算最大回撤 (Max Drawdown)"""
    if len(equity_series) < 1: return 0.0
    # 强制转为数值型，处理脏数据
    equity_series = pd.to_numeric(equity_series, errors='coerce').fillna(method='ffill')
    if equity_series.empty: return 0.0
    
    # 累计最大值
    peak = equity_series.cummax()
    # 当前回撤幅度
    drawdown = (equity_series - peak) / peak
    # 返回最小的那个值（即跌得最深的点），转为百分比
    return drawdown.min() * 100

def robust_read_csv(filename, col_names):
    """鲁棒的CSV读取函数"""
    if not os.path.exists(filename):
        print(f"❌ 文件不存在: {filename}")
        return pd.DataFrame()
        
    try:
        df = pd.read_csv(
            filename,
            names=col_names,     # 强制使用新表头
            header=None,         # 不读取文件自带表头
            skiprows=1,          # 跳过第一行
            engine='python',     # 使用Python引擎处理变长列
            on_bad_lines='skip'  # 跳过坏行
        )
        return df
    except Exception as e:
        print(f"❌ 读取 {filename} 失败: {e}")
        return pd.DataFrame()

def get_open_time_str(s_id_int):
    """根据策略ID计算东八区开仓时间"""
    # S0=08:00, S1=09:00 ... S23=07:00
    hour = (8 + s_id_int) % 24
    return f"{hour:02d}:00"

def analyze_strategies():
    print("📊 开始生成策略分析报告...")

    # 1. 定义表头结构
    HISTORY_COLS = [
        "Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", 
        "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", 
        "Used_Margin", "Round_PnL", "24h_Change", "Note"
    ]
    # Equity file 可能会有不同列数，这里定义足够覆盖的列
    EQUITY_COLS = ['Time'] + [f'S_{i}' for i in range(24)] + ['Total_Equity', 'Total_Invested']

    # 2. 读取数据
    history_df = robust_read_csv(HISTORY_FILE, HISTORY_COLS)
    equity_df = robust_read_csv(EQUITY_FILE, EQUITY_COLS)

    if history_df.empty: return

    # 3. 数据预处理
    history_df['Strategy_ID'] = pd.to_numeric(history_df['Strategy_ID'], errors='coerce')
    history_df['Round_PnL'] = pd.to_numeric(history_df['Round_PnL'], errors='coerce').fillna(0)
    history_df['Pos_PnL'] = pd.to_numeric(history_df['Pos_PnL'], errors='coerce').fillna(0)
    
    # 备用：如果 ROUND_RES 缺失，预先计算基于 CLOSE 的统计
    close_events = history_df[history_df['Type'] == 'CLOSE'].copy()
    rounds_fallback = pd.DataFrame()
    if not close_events.empty:
        rounds_fallback = close_events.groupby(['Strategy_ID', 'Time'])['Pos_PnL'].sum().reset_index()

    stats_list = []

    # 4. 循环分析 24 个策略
    for i in range(24):
        s_id = str(i)
        open_time = get_open_time_str(i)
        
        # --- A. 基础收益分析 ---
        # 1. 优先尝试读取结算记录 (ROUND_RES)
        rounds_res = history_df[
            (history_df['Strategy_ID'] == i) & 
            (history_df['Type'] == 'ROUND_RES')
        ]
        
        if len(rounds_res) > 0:
            pnl_series = rounds_res['Round_PnL']
            total_rounds = len(pnl_series)
            win_rounds = len(pnl_series[pnl_series > 0])
            total_pnl = pnl_series.sum()
            # 单次最大平仓亏损
            max_realized_loss = pnl_series.min() if len(pnl_series) > 0 else 0
            if max_realized_loss > 0: max_realized_loss = 0 # 全胜
            
        else:
            # 2. 备用方案：通过 CLOSE 事件估算
            if not rounds_fallback.empty:
                strat_rounds = rounds_fallback[rounds_fallback['Strategy_ID'] == i]
                total_rounds = len(strat_rounds)
                if total_rounds > 0:
                    pnl_series = strat_rounds['Pos_PnL']
                    win_rounds = len(pnl_series[pnl_series > 0])
                    total_pnl = pnl_series.sum()
                    max_realized_loss = pnl_series.min()
                    if max_realized_loss > 0: max_realized_loss = 0
                else:
                    # 3. 终极备用：净值差额
                    total_rounds = 0; win_rounds = 0; total_pnl = 0; max_realized_loss = 0
                    col_name = f"S_{i}"
                    if col_name in equity_df.columns:
                        series = pd.to_numeric(equity_df[col_name], errors='coerce').dropna()
                        if len(series) > 0:
                            total_pnl = series.iloc[-1] - 1000
            else:
                 total_rounds = 0; win_rounds = 0; total_pnl = 0; max_realized_loss = 0

        # 胜率计算
        if total_rounds > 0:
            win_rate = (win_rounds / total_rounds) * 100
            win_str = f"{win_rate:.0f}% ({win_rounds}/{total_rounds})"
        else:
            win_rate = 0.0
            win_str = "0/0"

        # --- B. 风险分析 (Max Drawdown) ---
        max_dd = 0.0
        col_name = f"S_{i}"
        if col_name in equity_df.columns:
            max_dd = calculate_max_drawdown(equity_df[col_name])

        stats_list.append({
            'id': s_id,
            'time': open_time,
            'win_str': win_str,
            'pnl': total_pnl,
            'max_dd': max_dd,
            'max_loss': max_realized_loss,
            'win_rate_val': win_rate
        })

    # 5. 排序与评级 (按总盈亏降序)
    stats_list.sort(key=lambda x: x['pnl'], reverse=True)

    # 6. 生成 Markdown 报告
    # 表头精简以适应手机屏幕
    # ID(时间) | 胜率 | 总盈 | 回撤 | 单亏
    md_table = "| ID (开仓) | 胜率 | 总盈 | 回撤 | 单亏 |\n"
    md_table += "| :--: | :--: | :--: | :--: | :--: |\n"
    
    top_performer = ""
    
    for idx, s in enumerate(stats_list):
        # 智能评级逻辑
        tag = "" 
        # 冠军逻辑
        if idx == 0 and s['pnl'] > 0: 
            tag = "🥇冠军"
            top_performer = f"S{s['id']} (+{s['pnl']:.0f}U)"
        # 稳健逻辑：盈利不错，回撤小，胜率高
        elif s['pnl'] > 500 and s['max_dd'] > -20 and s['win_rate_val'] >= 66:
            tag = "💎稳健"
        # 激进逻辑：盈利高，但回撤大
        elif s['pnl'] > 600 and s['max_dd'] < -30:
            tag = "🚀激进"
        # 垃圾逻辑
        elif s['pnl'] < -200 or s['max_dd'] < -50:
            tag = "💀避雷"
            
        # 格式化 ID 列：S22(06:00)
        id_display = f"S{s['id']}<br>{s['time']}"
        
        # 如果有标签，加在 ID 后面或者单独处理，这里为了省空间，如果是冠军直接加粗
        if tag == "🥇冠军": id_display = f"**{id_display}**"

        pnl_str = f"{s['pnl']:+.0f}"
        dd_str = f"{s['max_dd']:.1f}%"
        loss_str = f"{s['max_loss']:.0f}"
        
        md_table += f"| {id_display} | {s['win_str']} | {pnl_str} | {dd_str} | {loss_str} |\n"

    current_time = datetime.now().strftime("%m-%d %H:%M")
    
    title = f"🏆 策略大比武: {top_performer}"
    desp = f"""
**生成时间**: {current_time} (UTC+8)
**核心指标说明**:
1. **回撤**: 运行期间资金浮亏的最大幅度 (越接近0越稳)。
2. **单亏**: 平仓时最大的那一笔实亏金额。

---
{md_table}
---
**💡 每日点评**:
* **稳健之选**: 寻找回撤 > -20% 且 单亏较小的策略 (如 S4, S23)。
* **激进之选**: 寻找总盈最高的策略，但需忍受高回撤 (如 S22)。
    """
    
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
