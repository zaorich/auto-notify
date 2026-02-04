import pandas as pd
import numpy as np

# 读取数据
history_df = pd.read_csv('strategy_history.csv')
equity_df = pd.read_csv('equity_curve.csv')

def calculate_max_drawdown(equity_series):
    """计算最大回撤"""
    # 将累计最大值作为峰值
    peak = equity_series.cummax()
    # 计算当前回撤
    drawdown = (equity_series - peak) / peak
    # 返回回撤的最小值（因为回撤是负数，越小跌得越狠）
    return drawdown.min() * 100

print(f"{'='*20} 策略大比武排行榜 {'='*20}")
print(f"{'ID':<4} | {'总轮数':<6} | {'胜率':<7} | {'总收益(U)':<10} | {'最大回撤':<9} | {'评价'}")
print("-" * 65)

stats_list = []

for i in range(24):
    s_id = str(i)
    
    # 1. 从 history 中提取胜率和收益
    # 筛选出该策略所有的结算记录(ROUND_RES)
    rounds = history_df[
        (history_df['Strategy_ID'] == i) & 
        (history_df['Type'] == 'ROUND_RES')
    ]
    
    total_rounds = len(rounds)
    if total_rounds == 0:
        continue # 还没跑完一轮，跳过
        
    win_rounds = len(rounds[rounds['Round_PnL'] > 0])
    win_rate = (win_rounds / total_rounds) * 100
    
    # 总收益：取最后一次结算后的净值 - 初始本金(假设1000)
    # 或者直接 sum(Round_PnL)
    total_pnl = rounds['Round_PnL'].sum()
    
    # 2. 从 equity curve 中提取最大回撤
    # equity_curve.csv 列名通常是 S_0, S_1...
    col_name = f"S_{i}"
    max_dd = 0.0
    if col_name in equity_df.columns:
        max_dd = calculate_max_drawdown(equity_df[col_name])
        
    stats_list.append({
        'id': s_id,
        'rounds': total_rounds,
        'win_rate': win_rate,
        'pnl': total_pnl,
        'max_dd': max_dd
    })

# 按总收益降序排列
stats_list.sort(key=lambda x: x['pnl'], reverse=True)

for s in stats_list:
    # 简单评分逻辑
    tag = ""
    if s['pnl'] > 0 and s['max_dd'] > -10: tag = "🏆稳健冠军"
    elif s['pnl'] > 500: tag = "🚀激进派"
    elif s['max_dd'] < -30: tag = "⚠️风险高"
    elif s['pnl'] < 0: tag = "💩垃圾"
    
    print(f"S{s['id']:<3} | {s['rounds']:<6} | {s['win_rate']:>5.1f}% | {s['pnl']:>10.1f} | {s['max_dd']:>8.1f}% | {tag}")
