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
    return f"{hour:02d}点"

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
    coin_data = []

    if not recent_df.empty:
        # 确保按时间排序，这对 first()/last() 逻辑至关重要
        recent_df = recent_df.sort_values(['Symbol', 'Time'])
        g = recent_df.groupby('Symbol')
        
        # 1. 获取基准点 (t0)：窗口内的第一笔交易
        t0 = g.first()
        t0_prices = t0['Price']
        t0_times = t0['Time']
        
        # 2. 获取统计点：最高价(High) 和 现价(Current)
        # idxmax() 找到最高价的索引，然后 .loc 取出完整行(包含时间)
        idx_max = g['Price'].idxmax()
        # 注意：这里需要重新通过索引定位回原表以获取对应的时间
        max_rows = recent_df.loc[idx_max].set_index('Symbol')
        curr_rows = g.last()
        
        # 3. 向量化计算涨跌幅
        # 注意：索引必须对齐 (Symbol)
        pump_pct = (max_rows['Price'] - t0_prices) / t0_prices * 100
        curr_pct = (curr_rows['Price'] - t0_prices) / t0_prices * 100
        
        # 4. 计算延迟 (小时)
        delay_hours = (max_rows['Time'] - t0_times).dt.total_seconds() / 3600
        
        # 5. 汇总数据到 DataFrame 用于生成报告
        stats = pd.DataFrame({
            'sym': t0.index.str.replace('USDT', ''),
            'time_str': t0['Time_CN'].dt.strftime("%H:%M"),
            'pump': pump_pct,
            'delay': delay_hours.fillna(0).astype(int), # fillna防止极少数异常
            'curr': curr_pct
        })
        
        # 排序并生成 Markdown
        stats = stats.sort_values('pump', ascending=False)
        
        if not stats.empty:
            review_md = "| 币种(上榜) | 最高涨 | 现价 |\n| :-- | :--: | :--: |\n"
            for _, row in stats.iterrows():
                coin_str = f"{row['sym']}<br>{row['time_str']}"
                pump_str = f"{row['pump']:+.0f}%({row['delay']}h)"
                if row['pump'] > 10: pump_str = f"🔥{pump_str}"
                curr_str = f"{row['curr']:+.0f}%"
                
                review_md += f"| {coin_str} | {pump_str} | {curr_str} |\n"
        else:
             review_md = "无新币数据"
    else:
        review_md = "过去24h无新币"

    # ==========================================
    # 模块二：最佳做空时机 (Vectorized)
    # ==========================================
    # 目标：计算同一天、同一币种下，后续K线相对于首根K线的涨跌幅
    
    df['Date'] = df['Time'].dt.date
    # 全局排序，保证 groupby 后组内时间有序
    df = df.sort_values(['Symbol', 'Date', 'Time'])
    
    g_short = df.groupby(['Symbol', 'Date'])
    
    # 使用 transform('first') 将每组的第一行数据(基准点)广播到全组，避免循环
    t0_prices = g_short['Price'].transform('first')
    t0_times = g_short['Time'].transform('first')
    
    # 向量化计算全表所有行的 diff 和 chg
    delays = (df['Time'] - t0_times).dt.total_seconds() / 3600.0
    changes = (df['Price'] - t0_prices) / t0_prices * 100.0
    
    # 标记每行在组内的序号
    # cumcount() 会给每组的第一行标记 0，第二行标记 1...
    row_indices = g_short.cumcount()
    
    # 筛选非首行数据 (即 row_indices > 0 的行，排除 delay=0 的基准点)
    valid_mask = row_indices > 0
    
    best_time_md = ""
    # 只有当存在有效数据时才进行聚合
    if valid_mask.any():
        # 构建中间表进行聚合
        analysis_df = pd.DataFrame({
            'delay': delays[valid_mask].round().astype(int),
            'chg': changes[valid_mask]
        })
        
        # 聚合：按延迟时间统计均值
        # 筛选至少有3个样本的数据点，保证统计意义
        summary = analysis_df.groupby('delay')['chg'].agg(['mean', 'count']).reset_index()
        summary = summary[summary['count'] >= 3]
        
        if not summary.empty:
            best_time_md = "| 延迟 | 均涨跌 | 建议 |\n| :--: | :--: | :--: |\n"
            for _, row in summary.iterrows():
                h = int(row['delay'])
                avg = row['mean']
                
                s = "👀"
                if avg > 8: s = "⛔️"
                elif avg > 3: s = "🚀"
                elif avg < -1: s = "✅"
                
                best_time_md += f"| +{h}h | {avg:+.1f}% | {s} |\n"
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

    # 1. 市场分析 (调用优化后的函数)
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

        # 胜率
        win_rate = int(wins/total*100) if total > 0 else 0
        
        # 回撤
        max_dd = 0.0
        col = f"S_{i}"
        if col in equity_df.columns: max_dd = calculate_max_drawdown(equity_df[col])
        
        # 格式化 ID: S22(06点)
        id_str = f"S{s_id}<br>{get_open_time_str(i)}"
        
        stats_list.append({
            "id": id_str,
            "pnl": pnl,
            "win": win_rate,
            "dd": max_dd
        })

    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    # 3. 生成排行榜 (强制 3 列布局)
    # 策略(时) | 盈(撤) | 胜率
    rank_md = "| 策略(时) | 盈(撤) | 胜率 |\n| :-- | :--: | :--: |\n"
    
    for s in stats_list:
        # 合并 盈亏和回撤：+734(0%)
        pnl_dd_str = f"{s['pnl']:.0f}({s['dd']:.0f}%)"
        # 胜率：100%
        win_str = f"{s['win']}%"
        
        rank_md += f"| {s['id']} | {pnl_dd_str} | {win_str} |\n"

    # 4. 发送
    current_time = datetime.now().strftime("%m-%d %H:%M")
    top_performer = stats_list[0]['id'].split('<br>')[0] if stats_list else "None"
    
    title = f"📈 策略日报: {top_performer} 领跑"
    content = f"""
**{current_time} (UTC+8)**

### 🔥 昨日新币
{review_md}

### ⏳ 做空时机
{best_time_md}

### 🏆 策略排行
{rank_md}
    """
    
    send_wechat_msg(title, content)

if __name__ == "__main__":
    analyze_strategies()
