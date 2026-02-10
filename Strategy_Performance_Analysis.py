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

    # Server酱特定优化：两个换行符才能在微信中正确换行
    content = content.replace('\n', '\n\n')
    
    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    params = {'title': title, 'desp': content}
    try:
        data = urllib.parse.urlencode(params).encode('utf-8')
        req = requests.post(url, data=params)
        print(f"✅ 微信推送完成: {req.status_code}")
    except Exception as e:
        print(f"❌ 微信发送失败: {e}")

def robust_read_csv(filename, col_names):
    if not os.path.exists(filename): return pd.DataFrame()
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
    except: return pd.DataFrame()

def calculate_max_drawdown(equity_series):
    if len(equity_series) < 1: return 0.0
    equity_series = pd.to_numeric(equity_series, errors='coerce').fillna(method='ffill')
    if equity_series.empty: return 0.0
    peak = equity_series.cummax()
    drawdown = (equity_series - peak) / peak
    return drawdown.min() * 100

def get_open_time_str(s_id_int):
    # 返回简短的时间格式，如 "06点"
    hour = (8 + s_id_int) % 24
    return f"{hour:02d}h"

def analyze_market_mechanics(history_df):
    """
    分析市场：昨日复盘 + 统计规律 (向量化高性能版)
    优化说明：移除双重循环，使用 groupby 和 transform 进行全表运算
    """
    # 1. 基础数据清洗
    df = history_df[history_df['Type'] == 'OPEN'].copy()
    if df.empty: return "无数据", "无数据"

    df['Time'] = pd.to_datetime(df['Time'])
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df = df.dropna(subset=['Price'])
    df['Time_CN'] = df['Time'] + timedelta(hours=8)

    # ==========================================
    # 模块一：昨日新币复盘 (Vectorized)
    # ==========================================
    now = datetime.now()
    yesterday = now - timedelta(hours=24)
    
    # 筛选窗口内的数据
    recent_df = df[df['Time'] > yesterday].copy()
    review_md = ""
    
    if not recent_df.empty:
        # 确保按时间排序
        recent_df = recent_df.sort_values(['Symbol', 'Time'])
        g = recent_df.groupby('Symbol')
        
        # 1. 获取基准点 (t0)
        t0 = g.first()
        t0_prices = t0['Price']
        t0_times = t0['Time']
        
        # 2. 获取统计点：最高价(High) 和 现价(Current)
        idx_max = g['Price'].idxmax()
        max_rows = recent_df.loc[idx_max].set_index('Symbol')
        curr_rows = g.last()
        
        # 3. 向量化计算涨跌幅
        pump_pct = (max_rows['Price'] - t0_prices) / t0_prices * 100
        curr_pct = (curr_rows['Price'] - t0_prices) / t0_prices * 100
        
        # 4. 计算延迟 (小时)
        delay_hours = (max_rows['Time'] - t0_times).dt.total_seconds() / 3600
        
        # 5. 汇总数据到 DataFrame
        stats = pd.DataFrame({
            'sym': t0.index.str.replace('USDT', ''),
            'time_str': t0['Time_CN'].dt.strftime("%H:%M"),
            'pump': pump_pct,
            'delay': delay_hours.fillna(0).astype(int),
            'curr': curr_pct
        })
        
        # 排序
        stats = stats.sort_values('pump', ascending=False)
        
        # --- 生成 Markdown (纯文字版) ---
        if not stats.empty:
            # 3列布局：币种(时间) | 最高(耗时) | 现价
            review_md = "| 币种 | 最高(耗时) | 现价 |\n| :-- | :-- | :--: |\n"
            for _, row in stats.iterrows():
                # 1. 币种格式：加粗币种，时间变小
                coin_str = f"**{row['sym']}** ({row['time_str']})"
                
                # 2. 最高涨幅：移除图标，保留数值
                pump_str = f"+{row['pump']:.0f}% `@{row['delay']}h`"
                
                # 3. 现价：移除图标，保留数值
                curr_str = f"{row['curr']:+.0f}%"
                
                review_md += f"| {coin_str} | {pump_str} | {curr_str} |\n"
        else:
             review_md = "无新币上线"
    else:
        review_md = "无新币上线"

    # ==========================================
    # 模块二：最佳做空时机 (Vectorized)
    # ==========================================
    
    df['Date'] = df['Time'].dt.date
    df = df.sort_values(['Symbol', 'Date', 'Time'])
    
    g_short = df.groupby(['Symbol', 'Date'])
    
    t0_prices = g_short['Price'].transform('first')
    t0_times = g_short['Time'].transform('first')
    
    delays = (df['Time'] - t0_times).dt.total_seconds() / 3600.0
    changes = (df['Price'] - t0_prices) / t0_prices * 100.0
    
    row_indices = g_short.cumcount()
    valid_mask = row_indices > 0
    
    best_time_md = ""
    if valid_mask.any():
        analysis_df = pd.DataFrame({
            'delay': delays[valid_mask].round().astype(int),
            'chg': changes[valid_mask]
        })
        
        summary = analysis_df.groupby('delay')['chg'].agg(['mean', 'count']).reset_index()
        summary = summary[summary['count'] >= 3]
        
        # --- 生成 Markdown (纯文字版) ---
        if not summary.empty:
            # 3列布局：节点 | 波动 | 建议
            best_time_md = "| 节点 | 平均波动 | 建议 |\n| :--: | :--: | :--: |\n"
            for _, row in summary.iterrows():
                h = int(row['delay'])
                avg = row['mean']
                
                # 信号系统优化 (纯文字)
                if avg > 8: 
                    sig = "勿空" 
                elif avg > 3: 
                    sig = "观望"
                elif avg < -2: 
                    sig = "**做空**"
                elif avg < -0.5:
                    sig = "尝试"
                else:
                    sig = "震荡"
                
                avg_str = f"{avg:+.1f}%"
                
                best_time_md += f"| T+{h}h | {avg_str} | {sig} |\n"
        else:
            best_time_md = "数据积累中..."
    else:
        best_time_md = "数据积累中..."

    return review_md, best_time_md

def analyze_strategies():
    print("📊 生成精简版 Markdown 报告...")

    HISTORY_COLS = [
        "Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", 
        "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", 
        "Used_Margin", "Round_PnL", "24h_Change", "Note"
    ]
    EQUITY_COLS = ['Time'] + [f'S_{i}' for i in range(24)] + ['Total_Equity', 'Total_Invested', 'Extra']

    history_df = robust_read_csv(HISTORY_FILE, HISTORY_COLS)
    equity_df = robust_read_csv(EQUITY_FILE, EQUITY_COLS)

    if history_df.empty: 
        print("❌ 未找到历史数据或文件为空")
        return

    history_df['Strategy_ID'] = pd.to_numeric(history_df['Strategy_ID'], errors='coerce')
    history_df['Round_PnL'] = pd.to_numeric(history_df['Round_PnL'], errors='coerce').fillna(0)

    # 1. 市场分析 (调用纯文字版函数)
    review_md, best_time_md = analyze_market_mechanics(history_df)

    # 2. 策略排行
    stats_list = []
    
    close_events = history_df[history_df['Type'] == 'CLOSE'].copy()
    history_df['Pos_PnL'] = pd.to_numeric(history_df['Pos_PnL'], errors='coerce').fillna(0)
    rounds_fallback = pd.DataFrame()
    if not close_events.empty:
        rounds_fallback = close_events.groupby(['Strategy_ID', 'Time'])['Pos_PnL'].sum().reset_index()

    for i in range(24):
        s_id = str(i)
        rounds_res = history_df[(history_df['Strategy_ID'] == i) & (history_df['Type'] == 'ROUND_RES')]
        
        pnl = 0; wins = 0; total = 0
        
        if len(rounds_res) > 0:
            pnl = rounds_res['Round_PnL'].sum()
            total = len(rounds_res)
            wins = len(rounds_res[rounds_res['Round_PnL'] > 0])
        elif not rounds_fallback.empty:
            strat_r = rounds_fallback[rounds_fallback['Strategy_ID'] == i]
            if len(strat_r) > 0:
                pnl = strat_r['Pos_PnL'].sum()
                total = len(strat_r)
                wins = len(strat_r[strat_r['Pos_PnL'] > 0])
        else:
            col = f"S_{i}"
            if col in equity_df.columns:
                s = pd.to_numeric(equity_df[col], errors='coerce').dropna()
                if len(s)>0: pnl = s.iloc[-1] - 1000

        win_rate = int(wins/total*100) if total > 0 else 0
        
        max_dd = 0.0
        col = f"S_{i}"
        if col in equity_df.columns: max_dd = calculate_max_drawdown(equity_df[col])
        
        id_str = f"S{s_id}<br>{get_open_time_str(i)}"
        
        stats_list.append({
            "id": id_str,
            "pnl": pnl,
            "win": win_rate,
            "dd": max_dd,
            "count": total
        })

    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    # 3. 生成排行榜 (保留奖牌，移除其他图标)
    rank_md = "| 策略 | 盈(次) | 胜/撤 |\n| :-- | :--: | :--: |\n"
    
    top_n = stats_list[:10]
    
    for i, s in enumerate(top_n):
        parts = s['id'].split('<br>')
        strat_id = parts[0]
        open_time = parts[1]
        
        # 奖牌保留，这属于排名标识
        rank_icon = ""
        if i == 0: rank_icon = "🥇"
        elif i == 1: rank_icon = "🥈"
        elif i == 2: rank_icon = "🥉"
        
        col_name = f"{rank_icon} **{strat_id}** `{open_time}`"
        
        pnl_val = s['pnl']
        count_val = s['count']
        pnl_str = f"**{pnl_val:+.0f}** ({count_val})"
        
        win_dd_str = f"{s['win']}% / {s['dd']:.0f}%"
        
        rank_md += f"| {col_name} | {pnl_str} | {win_dd_str} |\n"

    # 4. 发送
    current_time = datetime.now().strftime("%m-%d %H:%M")
    top_performer = stats_list[0]['id'].split('<br>')[0] if stats_list else "None"
    
    title = f"📈 策略日报: {top_performer} 领跑"
    content = f"""
**{current_time} (UTC+8)**

### 昨日新币
{review_md}

### 做空时机
{best_time_md}

### 策略排行
{rank_md}
    """
    
    send_wechat_msg(title, content)

if __name__ == "__main__":
    analyze_strategies()
