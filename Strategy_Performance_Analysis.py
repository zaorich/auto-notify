import pandas as pd
import numpy as np
import os
import requests
import urllib.parse
from datetime import datetime

# =================配置区域=================
HISTORY_FILE = 'strategy_history.csv'
EQUITY_FILE = 'equity_curve.csv'
SERVERCHAN_KEY = os.environ.get("SERVERCHAN_KEY") # 从环境变量读取 Key
# =========================================

def calculate_max_drawdown(equity_series):
    """计算最大回撤"""
    if len(equity_series) < 1: return 0.0
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
    # Server酱支持 Markdown，但表格支持有限，这里用代码块包裹以保持对齐
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
        history_df = pd.read_csv(HISTORY_FILE)
        equity_df = pd.read_csv(EQUITY_FILE)
    except Exception as e:
        print(f"❌ 读取CSV失败: {e}")
        return

    stats_list = []
    
    # --- 数据分析循环 ---
    for i in range(24):
        s_id = str(i)
        
        # 1. 基础数据 (History)
        rounds = history_df[
            (history_df['Strategy_ID'] == i) & 
            (history_df['Type'] == 'ROUND_RES')
        ]
        
        total_rounds = len(rounds)
        if total_rounds == 0: continue
            
        win_rounds = len(rounds[rounds['Round_PnL'] > 0])
        loss_rounds = len(rounds[rounds['Round_PnL'] <= 0])
        win_rate = (win_rounds / total_rounds) * 100
        total_pnl = rounds['Round_PnL'].sum()
        
        avg_win = rounds[rounds['Round_PnL'] > 0]['Round_PnL'].mean() if win_rounds > 0 else 0
        avg_loss = abs(rounds[rounds['Round_PnL'] <= 0]['Round_PnL'].mean()) if loss_rounds > 0 else 0
        pnl_ratio = (avg_win / avg_loss) if avg_loss > 0 else 99.9
        
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
    # 按总收益降序排序
    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    # 构建 Markdown 表格
    # 注意：为了在手机上能看清，精简了列名
    md_content = "| ID | 胜率 | 总盈 | 回撤 | 盈亏比 |\n| :--: | :--: | :--: | :--: | :--: |\n"
    
    top_performer = ""
    
    for idx, s in enumerate(stats_list):
        # 评级标签
        tag = ""
        pnl = s['pnl']
        dd = s['max_dd']
        wr = s['win_rate']
        
        if idx == 0: top_performer = f"S{s['id']} (收益 {pnl:.0f}U)" # 记录冠军
        
        # 格式化数据
        pnl_str = f"{pnl:+.0f}"
        dd_str = f"{dd:.1f}%"
        pr_str = f"{s['pnl_ratio']:.1f}"
        
        md_content += f"| S{s['id']} | {wr:.0f}% | {pnl_str} | {dd_str} | {pr_str} |\n"

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # 组合最终消息
    title = f"🏆 策略风云榜: {top_performer}"
    desp = f"""
**生成时间**: {current_time} (UTC+8)
**参评策略**: {len(stats_list)} 个

---
{md_content}
---
**指标说明**:
1. **回撤**: 越接近0越好（-5% 优于 -20%）。
2. **盈亏比**: 大于 1.5 说明赚大亏小。
3. **稳健冠军**: 需同时满足高胜率+低回撤。
    """
    
    # 发送
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
