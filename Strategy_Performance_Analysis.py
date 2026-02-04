import pandas as pd
import numpy as np
import os
import requests
import urllib.parse
from datetime import datetime, timedelta

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

def robust_read_csv(filename, col_names):
    """鲁棒的CSV读取函数"""
    if not os.path.exists(filename):
        print(f"❌ 文件不存在: {filename}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(
            filename,
            names=col_names,
            header=None,
            skiprows=1,
            engine='python',
            on_bad_lines='skip'
        )
        return df
    except Exception as e:
        print(f"❌ 读取 {filename} 失败: {e}")
        return pd.DataFrame()

def calculate_max_drawdown(equity_series):
    if len(equity_series) < 1: return 0.0
    equity_series = pd.to_numeric(equity_series, errors='coerce').fillna(method='ffill')
    if equity_series.empty: return 0.0
    peak = equity_series.cummax()
    drawdown = (equity_series - peak) / peak
    return drawdown.min() * 100

def analyze_market_mechanics(history_df):
    """
    深度分析模块：
    1. 过去24小时市场热度（有多少新币上榜）
    2. 历史最佳做空时间窗口
    """
    # 筛选开仓数据
    df = history_df[history_df['Type'] == 'OPEN'].copy()
    if df.empty: return "", ""

    # 数据转换
    df['Time'] = pd.to_datetime(df['Time'])
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df = df.dropna(subset=['Price'])

    # --- 分析 1: 昨日市场复盘 (Yesterday's Review) ---
    now = datetime.now()
    yesterday = now - timedelta(hours=24)
    
    # 筛选过去24小时的数据
    recent_df = df[df['Time'] > yesterday].copy()
    
    daily_review_md = ""
    if not recent_df.empty:
        # 统计去重后的币种数量
        unique_coins = recent_df['Symbol'].unique()
        coin_count = len(unique_coins)
        
        # 统计最活跃的时间段 (东八区)
        recent_df['Hour_CN'] = (recent_df['Time'] + timedelta(hours=8)).dt.hour
        busy_hour = recent_df['Hour_CN'].mode()[0]
        
        # 列出前5个新上榜的币
        top_coins_str = ", ".join([s.replace('USDT','') for s in unique_coins[:5]])
        
        daily_review_md = f"""
**🔥 过去24h复盘**:
- **上榜数量**: 共 {coin_count} 个新币
- **爆发时间**: {busy_hour}:00 (东八区) 此时上榜最多
- **活跃币种**: {top_coins_str}...
"""
    else:
        daily_review_md = "**🔥 过去24h复盘**: 无开仓数据 (市场冷清)"

    # --- 分析 2: 最佳做空时间 (Time Decay Alpha) ---
    # 使用全量历史数据
    df['Date'] = df['Time'].dt.date
    grouped = df.groupby(['Symbol', 'Date'])
    
    results = []
    
    for (symbol, date), group in grouped:
        if len(group) < 2: continue 
        
        group = group.sort_values('Time')
        t0_price = group.iloc[0]['Price']
        t0_time = group.iloc[0]['Time']
        
        for i in range(1, len(group)):
            curr = group.iloc[i]
            hours_diff = (curr['Time'] - t0_time).total_seconds() / 3600.0
            
            # (当前价 - 初始价) / 初始价
            # 正数 = 涨了 (说明做空早了)
            # 负数 = 跌了 (说明开始赚钱了)
            pct_change = ((curr['Price'] - t0_price) / t0_price) * 100
            
            results.append({
                'delay': int(round(hours_diff)),
                'change': pct_change
            })
            
    best_time_md = ""
    if results:
        res_df = pd.DataFrame(results)
        # 按小时聚合，计算平均涨跌幅
        summary = res_df.groupby('delay')['change'].agg(['mean', 'count']).reset_index()
        summary = summary[summary['count'] >= 3] # 过滤小样本
        
        best_time_md = "| 延时 | 均价变动 | 建议 |\n| :--: | :--: | :--: |\n"
        
        for _, row in summary.iterrows():
            hour = int(row['delay'])
            avg_chg = row['mean']
            
            # 这里的涨幅是相对于第一次上榜时的价格
            # 如果 avg_chg > 0，说明还在涨，空早了
            # 如果 avg_chg 开始下降，说明见顶了
            
            status = ""
            if avg_chg > 5.0: status = "⛔️ 暴涨中"
            elif avg_chg > 1.0: status = "⏳ 还在涨"
            elif avg_chg < -1.0: status = "✅ 已转跌"
            else: status = "👀 观察"
            
            best_time_md += f"| +{hour}h | {avg_chg:+.1f}% | {status} |\n"
    else:
        best_time_md = "数据积累中，暂无足够样本分析时间规律。"

    return daily_review_md, best_time_md

def analyze_strategies():
    print("📊 开始生成详细分析报告...")

    # 1. 定义表头
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

    # 3. 预处理
    history_df['Strategy_ID'] = pd.to_numeric(history_df['Strategy_ID'], errors='coerce')
    history_df['Round_PnL'] = pd.to_numeric(history_df['Round_PnL'], errors='coerce').fillna(0)
    history_df['Pos_PnL'] = pd.to_numeric(history_df['Pos_PnL'], errors='coerce').fillna(0)

    # --- 模块 1: 市场深层分析 (Review & Alpha) ---
    daily_review_str, best_time_str = analyze_market_mechanics(history_df)

    # --- 模块 2: 策略排行榜 ---
    # 备用：计算基于 CLOSE 的统计
    close_events = history_df[history_df['Type'] == 'CLOSE'].copy()
    rounds_fallback = pd.DataFrame()
    if not close_events.empty:
        rounds_fallback = close_events.groupby(['Strategy_ID', 'Time'])['Pos_PnL'].sum().reset_index()

    stats_list = []
    for i in range(24):
        s_id = str(i)
        
        # 优先取 ROUND_RES
        rounds_res = history_df[(history_df['Strategy_ID'] == i) & (history_df['Type'] == 'ROUND_RES')]
        
        if len(rounds_res) > 0:
            pnl = rounds_res['Round_PnL'].sum()
            wins = len(rounds_res[rounds_res['Round_PnL'] > 0])
            total = len(rounds_res)
        elif not rounds_fallback.empty:
            strat_r = rounds_fallback[rounds_fallback['Strategy_ID'] == i]
            pnl = strat_r['Pos_PnL'].sum()
            wins = len(strat_r[strat_r['Pos_PnL'] > 0])
            total = len(strat_r)
        else:
            pnl = 0; wins = 0; total = 0
            # 终极备用：净值差额
            col_name = f"S_{i}"
            if col_name in equity_df.columns:
                series = pd.to_numeric(equity_df[col_name], errors='coerce').dropna()
                if len(series) > 0: pnl = series.iloc[-1] - 1000

        win_rate = (wins/total*100) if total > 0 else 0
        
        # Max DD
        max_dd = 0.0
        col = f"S_{i}"
        if col in equity_df.columns: max_dd = calculate_max_drawdown(equity_df[col])
        
        stats_list.append({'id': s_id, 'pnl': pnl, 'wr': win_rate, 'dd': max_dd, 'total': total})

    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    # 生成排行榜表格
    rank_table = "| ID | 胜率 | 总盈 | 回撤 |\n| :--: | :--: | :--: | :--: |\n"
    top_id = ""
    for idx, s in enumerate(stats_list):
        if idx == 0: top_id = f"S{s['id']}"
        # 只显示前5名和最后3名，避免表格过长
        if idx < 5 or idx >= 21:
            rank_table += f"| S{s['id']} | {s['wr']:.0f}% | {s['pnl']:.0f} | {s['dd']:.1f}% |\n"
        if idx == 5:
            rank_table += "| ... | ... | ... | ... |\n"

    # --- 4. 组装最终报告 ---
    current_time = datetime.now().strftime("%m-%d %H:%M")
    
    title = f"📈 策略日报: {top_id} 领跑"
    desp = f"""
**生成时间**: {current_time} (UTC+8)

---
{daily_review_str}

### ⏳ 最佳做空时机 (Alpha)
*(基于历史全量数据分析: 上榜后N小时的价格变化)*
{best_time_str}

### 🏆 策略排行榜 (Top 5 & Bottom 3)
{rank_table}
    """
    
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
