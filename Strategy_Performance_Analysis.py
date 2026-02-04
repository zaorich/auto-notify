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

    # Server酱支持直接渲染 HTML，不需要特殊处理换行
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
        df = pd.read_csv(filename, names=col_names, header=None, skiprows=1, engine='python', on_bad_lines='skip')
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
    hour = (8 + s_id_int) % 24
    return f"{hour:02d}:00"

def df_to_html_table(df, title=""):
    """将 DataFrame 转换为漂亮的 HTML 表格"""
    if df.empty: return f"<p>{title}: 无数据</p>"
    
    # CSS 样式：紧凑、居中、带边框、表头灰色背景
    style = """
    <style>
    table { width: 100%; border-collapse: collapse; font-size: 12px; margin-bottom: 15px; }
    th, td { border: 1px solid #ddd; padding: 4px; text-align: center; }
    th { background-color: #f2f2f2; font-weight: bold; }
    .pos { color: red; }
    .neg { color: green; }
    </style>
    """
    
    html = f"<h4>{title}</h4>" + style + "<table><thead><tr>"
    for col in df.columns:
        html += f"<th>{col}</th>"
    html += "</tr></thead><tbody>"
    
    for _, row in df.iterrows():
        html += "<tr>"
        for item in row:
            # 简单的颜色处理
            val_str = str(item)
            color_class = ""
            if "%" in val_str:
                if "+" in val_str or (val_str.replace('%','').replace('.','').isdigit() and float(val_str.replace('%','')) > 0):
                    color_class = 'class="pos"' # 涨显示红色(或根据习惯)
                elif "-" in val_str:
                    color_class = 'class="neg"' # 跌显示绿色
            
            html += f"<td {color_class}>{val_str}</td>"
        html += "</tr>"
    html += "</tbody></table>"
    return html

def analyze_market_mechanics(history_df):
    """分析市场：昨日复盘明细 + 统计规律"""
    df = history_df[history_df['Type'] == 'OPEN'].copy()
    if df.empty: return "", ""

    df['Time'] = pd.to_datetime(df['Time'])
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df = df.dropna(subset=['Price'])
    df['Time_CN'] = df['Time'] + timedelta(hours=8)

    # --- 1. 昨日新币复盘 (详细表格) ---
    now = datetime.now()
    yesterday = now - timedelta(hours=24)
    recent_df = df[df['Time'] > yesterday].copy()
    
    review_html = ""
    coin_data = []
    
    if not recent_df.empty:
        unique_coins = recent_df['Symbol'].unique()
        for symbol in unique_coins:
            # 获取该币种所有数据
            coin_hist = df[df['Symbol'] == symbol].sort_values('Time')
            # 找到过去24h的第一次上榜
            entries_24h = coin_hist[coin_hist['Time'] > yesterday]
            if entries_24h.empty: continue
            
            t0 = entries_24h.iloc[0]
            t0_p = t0['Price']
            t0_t = t0['Time']
            
            # 找后续数据
            subsequent = coin_hist[coin_hist['Time'] >= t0_t]
            if subsequent.empty: continue
            
            max_p = subsequent['Price'].max()
            curr_p = subsequent.iloc[-1]['Price']
            
            max_pump = (max_p - t0_p) / t0_p * 100
            curr_chg = (curr_p - t0_p) / t0_p * 100
            
            # 找到最高点延迟
            max_row = subsequent[subsequent['Price'] == max_p].iloc[0]
            delay = (max_row['Time'] - t0_t).total_seconds() / 3600
            
            coin_data.append({
                "币种": symbol.replace('USDT',''),
                "上榜": t0['Time_CN'].strftime("%H:%M"),
                "最高涨": f"{max_pump:+.1f}%",
                "耗时": f"{int(delay)}h",
                "现价": f"{curr_chg:+.1f}%"
            })
            
        if coin_data:
            review_df = pd.DataFrame(coin_data)
            # 按最高涨幅降序
            review_df['sort_val'] = review_df['最高涨'].apply(lambda x: float(x.strip('%')))
            review_df = review_df.sort_values('sort_val', ascending=False).drop(columns=['sort_val'])
            review_html = df_to_html_table(review_df, "🔥 昨日新币复盘 (详细)")
        else:
            review_html = "<p>无新币数据</p>"
    else:
        review_html = "<p>过去24h无新币上榜</p>"

    # --- 2. 历史最佳做空时机 (统计) ---
    df['Date'] = df['Time'].dt.date
    grouped = df.groupby(['Symbol', 'Date'])
    
    results = []
    for _, group in grouped:
        if len(group) < 2: continue
        group = group.sort_values('Time')
        t0_p = group.iloc[0]['Price']
        t0_t = group.iloc[0]['Time']
        
        for i in range(1, len(group)):
            curr = group.iloc[i]
            diff = (curr['Time'] - t0_t).total_seconds() / 3600.0
            chg = (curr['Price'] - t0_p) / t0_p * 100
            results.append({'delay': int(round(diff)), 'chg': chg})
            
    best_time_html = ""
    if results:
        res_df = pd.DataFrame(results)
        summary = res_df.groupby('delay')['chg'].agg(['mean', 'count']).reset_index()
        summary = summary[summary['count'] >= 3] # 过滤小样本
        
        table_data = []
        for _, row in summary.iterrows():
            h = int(row['delay'])
            avg = row['mean']
            
            s = "👀"
            if avg > 10: s = "⛔️高危"
            elif avg > 5: s = "🚀暴涨"
            elif avg > 0: s = "⏳微涨"
            elif avg < -1: s = "✅转跌"
            
            table_data.append({
                "延迟": f"+{h}h",
                "均涨跌": f"{avg:+.1f}%",
                "建议": s
            })
        
        bt_df = pd.DataFrame(table_data)
        best_time_html = df_to_html_table(bt_df, "⏳ 历史做空规律 (Alpha)")
    else:
        best_time_html = "<p>数据积累中...</p>"

    return review_html, best_time_html

def analyze_strategies():
    print("📊 生成 HTML 报告...")

    HISTORY_COLS = [
        "Time", "Strategy_ID", "Type", "Symbol", "Price", "15m_High", 
        "Amount", "Pos_PnL", "Strategy_Equity", "Total_Invested", 
        "Used_Margin", "Round_PnL", "24h_Change", "Note"
    ]
    EQUITY_COLS = ['Time'] + [f'S_{i}' for i in range(24)] + ['Total_Equity', 'Total_Invested']

    history_df = robust_read_csv(HISTORY_FILE, HISTORY_COLS)
    equity_df = robust_read_csv(EQUITY_FILE, EQUITY_COLS)

    if history_df.empty: return

    history_df['Strategy_ID'] = pd.to_numeric(history_df['Strategy_ID'], errors='coerce')
    history_df['Round_PnL'] = pd.to_numeric(history_df['Round_PnL'], errors='coerce').fillna(0)

    # 1. 市场分析 (HTML)
    review_html, best_time_html = analyze_market_mechanics(history_df)

    # 2. 策略排行
    stats_list = []
    
    # 备用计算
    close_events = history_df[history_df['Type'] == 'CLOSE'].copy()
    history_df['Pos_PnL'] = pd.to_numeric(history_df['Pos_PnL'], errors='coerce').fillna(0)
    rounds_fallback = pd.DataFrame()
    if not close_events.empty:
        rounds_fallback = close_events.groupby(['Strategy_ID', 'Time'])['Pos_PnL'].sum().reset_index()

    for i in range(24):
        s_id = str(i)
        rounds_res = history_df[(history_df['Strategy_ID'] == i) & (history_df['Type'] == 'ROUND_RES')]
        
        pnl = 0; wins = 0; total = 0; max_loss = 0
        
        if len(rounds_res) > 0:
            pnl = rounds_res['Round_PnL'].sum()
            total = len(rounds_res)
            wins = len(rounds_res[rounds_res['Round_PnL'] > 0])
            max_loss = rounds_res['Round_PnL'].min()
        elif not rounds_fallback.empty:
            strat_r = rounds_fallback[rounds_fallback['Strategy_ID'] == i]
            if len(strat_r) > 0:
                pnl = strat_r['Pos_PnL'].sum()
                total = len(strat_r)
                wins = len(strat_r[strat_r['Pos_PnL'] > 0])
                max_loss = strat_r['Pos_PnL'].min()
        else:
            col = f"S_{i}"
            if col in equity_df.columns:
                s = pd.to_numeric(equity_df[col], errors='coerce').dropna()
                if len(s)>0: pnl = s.iloc[-1] - 1000

        if max_loss > 0: max_loss = 0
        win_str = f"{int(wins/total*100)}%({wins}/{total})" if total > 0 else "0/0"
        
        max_dd = 0.0
        col = f"S_{i}"
        if col in equity_df.columns: max_dd = calculate_max_drawdown(equity_df[col])
        
        stats_list.append({
            "策略": f"S{s_id}",
            "开仓": get_open_time_str(i),
            "胜率": win_str,
            "总盈": f"{pnl:.0f}",
            "回撤": f"{max_dd:.1f}%",
            "单亏": f"{max_loss:.0f}"
        })

    stats_list.sort(key=lambda x: float(x['总盈']), reverse=True)
    
    # 生成 HTML 表格
    rank_df = pd.DataFrame(stats_list)
    rank_html = df_to_html_table(rank_df, "🏆 策略排行榜 (全量)")

    # 3. 组装最终 HTML 消息
    current_time = datetime.now().strftime("%m-%d %H:%M")
    top_performer = stats_list[0]['策略'] if stats_list else "None"
    
    title = f"📈 策略日报: {top_performer} 领跑"
    
    # 将所有 HTML 片段拼接
    content = f"""
    <h3>📊 策略分析日报 {current_time}</h3>
    <hr>
    {review_html}
    {best_time_html}
    {rank_html}
    """
    
    send_wechat_msg(title, content)

if __name__ == "__main__":
    analyze_strategies()
