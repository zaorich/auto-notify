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
    
    # 累计最大值
    peak = equity_series.cummax()
    # 当前回撤幅度
    drawdown = (equity_series - peak) / peak
    # 返回最小的那个值（即跌得最深的点），转为百分比
    return drawdown.min() * 100

def robust_read_csv(filename, col_names):
    """鲁棒的CSV读取函数，专门处理列数不一致的问题"""
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

def analyze_strategies():
    print("📊 开始生成策略分析报告...")

    # 1. 定义最新的表头结构
    HISTORY_COLS = [
        "Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", 
        "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", 
        "Used_Margin", "Round_PnL", "24h_Change", "Note"
    ]
    EQUITY_COLS = ['Time'] + [f'S_{i}' for i in range(24)] + ['Total_Equity', 'Total_Invested']

    # 2. 读取数据
    history_df = robust_read_csv(HISTORY_FILE, HISTORY_COLS)
    equity_df = robust_read_csv(EQUITY_FILE, EQUITY_COLS)

    if history_df.empty: return

    # 3. 数据预处理
    history_df['Strategy_ID'] = pd.to_numeric(history_df['Strategy_ID'], errors='coerce')
    
    stats_list = []

    # 4. 循环分析 24 个策略
    for i in range(24):
        s_id = str(i)
        
        # --- A. 基础收益分析 (基于 History) ---
        # 优先使用 ROUND_RES (本轮结算) 数据
        rounds = history_df[
            (history_df['Strategy_ID'] == i) & 
            (history_df['Type'] == 'ROUND_RES')
        ]
        
        # 如果没有 ROUND_RES (老数据)，尝试用 CLOSE 估算（简略版）
        # 这里为了准确性，我们主要依赖 ROUND_RES，如果没有则显示为 0
        pnl_series = pd.to_numeric(rounds['Round_PnL'], errors='coerce').fillna(0)
        
        total_rounds = len(pnl_series)
        win_rounds = len(pnl_series[pnl_series > 0])
        total_pnl = pnl_series.sum()
        
        # 补救措施：如果 ROUND_RES 为空，尝试从净值曲线取最新值算总盈亏
        if total_rounds == 0:
            col_name = f"S_{i}"
            if col_name in equity_df.columns:
                try:
                    # 取最后一行有效的净值
                    last_equity = pd.to_numeric(equity_df[col_name], errors='coerce').dropna().iloc[-1]
                    # 假设初始投入是 1000
                    total_pnl = last_equity - 1000
                except:
                    pass

        # 胜率计算
        win_rate = (win_rounds / total_rounds * 100) if total_rounds > 0 else 0.0
        
        # 盈亏比
        avg_win = pnl_series[pnl_series > 0].mean() if win_rounds > 0 else 0
        avg_loss = abs(pnl_series[pnl_series <= 0].mean()) if (total_rounds - win_rounds) > 0 else 0
        pnl_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0

        # --- B. 风险分析 (基于 Equity Curve) ---
        max_dd = 0.0
        col_name = f"S_{i}"
        if col_name in equity_df.columns:
            max_dd = calculate_max_drawdown(equity_df[col_name])

        stats_list.append({
            'id': s_id,
            'rounds': total_rounds,
            'wins': win_rounds,
            'win_rate': win_rate,
            'pnl': total_pnl,
            'max_dd': max_dd,
            'pnl_ratio': pnl_ratio
        })

    # 5. 排序与评级 (按总盈亏降序)
    stats_list.sort(key=lambda x: x['pnl'], reverse=True)

    # 6. 生成 Markdown 报告
    # 表头
    md_table = "| ID | 胜率 (赢/总) | 总盈 | 回撤 | 评级 |\n"
    md_table += "| :--: | :--: | :--: | :--: | :--: |\n"
    
    champion_name = "暂无"
    
    for idx, s in enumerate(stats_list):
        # 智能评级标签
        tag = ""
        if s['pnl'] > 0 and s['max_dd'] > -10 and s['win_rate'] >= 66: tag = "🏆稳健"
        elif s['pnl'] > 500: tag = "🚀暴利"
        elif s['pnl'] < -200: tag = "💀巨亏"
        elif s['max_dd'] < -30: tag = "⚠️高危"
        else: tag = "-"
        
        # 记录冠军 (排除没跑过数据的)
        if idx == 0 and s['pnl'] != 0:
            champion_name = f"S{s['id']}"

        # 格式化数据
        # 胜率显示为: 66% (2/3)
        win_str = f"{s['win_rate']:.0f}% ({s['wins']}/{s['rounds']})"
        pnl_str = f"{s['pnl']:+.0f}"
        dd_str = f"{s['max_dd']:.1f}%"
        
        md_table += f"| S{s['id']} | {win_str} | {pnl_str} | {dd_str} | {tag} |\n"

    current_time = datetime.now().strftime("%m-%d %H:%M")
    
    title = f"📊 策略大比武: {champion_name} 领跑"
    desp = f"""
**生成时间**: {current_time}
**统计维度**: 胜率、累计盈亏、最大回撤

---
{md_table}
---
**💡 如何选择最优策略?**
1. **稳健型**: 找 **胜率高** 且 **回撤小** (例如 >-10%) 的。
2. **激进型**: 找 **总盈最高** 的，但要小心回撤。
3. **避雷**: 远离 **盈亏比低** (赢小输大) 的策略。
    """
    
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
