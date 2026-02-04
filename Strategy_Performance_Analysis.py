import pandas as pd
import numpy as np
import os
import requests
import urllib.parse
from datetime import datetime

# =================配置区域=================
HISTORY_FILE = 'strategy_history.csv'
EQUITY_FILE = 'equity_curve.csv'
SERVERCHAN_KEY = os.environ.get("SERVERCHAN_KEY")
# =========================================

def calculate_max_drawdown(equity_series):
    """计算最大回撤"""
    if len(equity_series) < 1: return 0.0
    # 确保数据是数值型
    equity_series = pd.to_numeric(equity_series, errors='coerce').fillna(method='ffill')
    
    peak = equity_series.cummax()
    drawdown = (equity_series - peak) / peak
    return drawdown.min() * 100

def send_wechat_msg(title, content):
    """发送微信通知"""
    if not SERVERCHAN_KEY:
        print("⚠️ 未配置 SERVERCHAN_KEY，只打印不发送。")
        print(f"--- {title} ---\n{content}")
        return

    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    params = {'title': title, 'desp': content}
    try:
        data = urllib.parse.urlencode(params).encode('utf-8')
        req = requests.post(url, data=params)
        print(f"✅ 微信推送状态: {req.status_code}")
    except Exception as e:
        print(f"❌ 微信发送失败: {e}")

def analyze_strategies():
    print(f"正在读取数据文件...")
    
    if not os.path.exists(HISTORY_FILE) or not os.path.exists(EQUITY_FILE):
        print(f"❌ 错误: 找不到数据文件！")
        return

    try:
        # --- [1. 读取历史记录] ---
        # 强制指定最新的 14 个列名
        NEW_HEADERS = [
            "Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", 
            "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", 
            "Used_Margin", "Round_PnL", "24h_Change", "Note"
        ]
        
        history_df = pd.read_csv(
            HISTORY_FILE, 
            names=NEW_HEADERS,   # 使用新列名
            header=None,         # ⚠️ 关键：不读取文件自带的表头
            skiprows=1,          # ⚠️ 关键：物理跳过第一行（旧表头）
            engine='python',     # 使用宽容模式
            on_bad_lines='skip'  # 跳过坏行
        )
        
        # --- [2. 读取净值曲线] ---
        # 强制指定最新的 27 个列名 (Time + 24个策略 + Total_Equity + Total_Invested)
        EQUITY_HEADERS = ['Time'] + [f'S_{i}' for i in range(24)] + ['Total_Equity', 'Total_Invested']
        
        equity_df = pd.read_csv(
            EQUITY_FILE,
            names=EQUITY_HEADERS, # 使用新列名
            header=None,          # 不读旧表头
            skiprows=1,           # 跳过第一行
            engine='python',
            on_bad_lines='skip'
        )
        
    except Exception as e:
        print(f"❌ 读取CSV失败: {e}")
        import traceback
        traceback.print_exc()
        return

    stats_list = []
    
    # 确保 Strategy_ID 是数字类型
    history_df['Strategy_ID'] = pd.to_numeric(history_df['Strategy_ID'], errors='coerce')
    
    # --- 数据分析循环 ---
    for i in range(24):
        s_id = str(i)
        
        # 1. 基础数据 (History)
        rounds = history_df[
            (history_df['Strategy_ID'] == i) & 
            (history_df['Type'] == 'ROUND_RES')
        ]
        
        total_rounds = len(rounds)
        
        if total_rounds > 0:
            # 确保 Round_PnL 是数值型
            pnl_series = pd.to_numeric(rounds['Round_PnL'], errors='coerce').fillna(0)
            
            win_rounds = len(pnl_series[pnl_series > 0])
            loss_rounds = len(pnl_series[pnl_series <= 0])
            win_rate = (win_rounds / total_rounds) * 100
            total_pnl = pnl_series.sum()
            
            avg_win = pnl_series[pnl_series > 0].mean() if win_rounds > 0 else 0
            avg_loss = abs(pnl_series[pnl_series <= 0].mean()) if loss_rounds > 0 else 0
            pnl_ratio = (avg_win / avg_loss) if avg_loss > 0 else 99.9
        else:
            # 如果没有结算数据，尝试用净值估算当前浮动盈亏
            win_rate = 0
            total_pnl = 0
            pnl_ratio = 0
            # 尝试从 equity_df 获取最新净值 - 1000
            col_name = f"S_{i}"
            if col_name in equity_df.columns and len(equity_df) > 0:
                try:
                    last_equity = pd.to_numeric(equity_df[col_name].iloc[-1], errors='coerce')
                    total_pnl = last_equity - 1000
                except:
                    pass

        # 2. 风险数据 (Equity Curve)
        col_name = f"S_{i}"
        max_dd = 0.0
        if col_name in equity_df.columns:
            series = equity_df[col_name]
            max_dd = calculate_max_drawdown(series)
            
        stats_list.append({
            'id': s_id,
            'rounds': total_rounds,
            'win_rate': win_rate,
            'pnl': total_pnl,
            'max_dd': max_dd,
            'pnl_ratio': pnl_ratio
        })

    # --- 生成报告内容 ---
    # 如果所有策略都还没跑完一轮，至少展示当前的浮动盈亏排名
    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    md_content = "| ID | 胜率 | 总盈 | 回撤 | 盈亏比 |\n| :--: | :--: | :--: | :--: | :--: |\n"
    
    top_performer = ""
    
    for idx, s in enumerate(stats_list):
        if idx == 0: top_performer = f"S{s['id']} (收益 {s['pnl']:.0f}U)"
        
        pnl_str = f"{s['pnl']:+.0f}"
        dd_str = f"{s['max_dd']:.1f}%"
        pr_str = f"{s['pnl_ratio']:.1f}"
        
        md_content += f"| S{s['id']} | {s['win_rate']:.0f}% | {pnl_str} | {dd_str} | {pr_str} |\n"

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    title = f"🏆 策略风云榜: {top_performer}"
    desp = f"""
**生成时间**: {current_time} (UTC+8)
**参评策略**: {len(stats_list)} 个

---
{md_content}
---
**指标说明**:
1. **总盈**: 历史累计净利润 (含浮动)。
2. **回撤**: 越接近0越好。
3. **盈亏比**: 平均赚的钱 / 平均亏的钱。
    """
    
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
