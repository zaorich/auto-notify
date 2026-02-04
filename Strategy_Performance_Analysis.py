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

def get_open_time_str(s_id_int):
    """根据策略ID计算东八区开仓时间"""
    hour = (8 + s_id_int) % 24
    return f"{hour:02d}点"

def analyze_market_mechanics(history_df):
    """分析：昨日复盘 + 最佳做空时机"""
    df = history_df[history_df['Type'] == 'OPEN'].copy()
    if df.empty: return "", ""

    df['Time'] = pd.to_datetime(df['Time'])
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df = df.dropna(subset=['Price'])

    # 1. 昨日复盘
    now = datetime.now()
    yesterday = now - timedelta(hours=24)
    recent_df = df[df['Time'] > yesterday].copy()
    
    daily_review_md = ""
    if not recent_df.empty:
        count = len(recent_df['Symbol'].unique())
        daily_review_md = f"**🔥 昨日复盘**: 新上榜 {count} 个币种"
    else:
        daily_review_md = "**🔥 昨日复盘**: 市场冷清，无新币上榜"

    # 2. 最佳做空时机分析 (Time Decay Alpha)
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
            pct_change = ((curr['Price'] - t0_price) / t0_price) * 100
            results.append({'delay': int(round(hours_diff)), 'change': pct_change})
            
    best_time_md = ""
    if results:
        res_df = pd.DataFrame(results)
        summary = res_df.groupby('delay')['change'].agg(['mean', 'count']).reset_index()
        summary = summary[summary['count'] >= 2] # 至少2个样本才显示
        
        # 手机端优化：极简表头
        best_time_md = "| 延时 | 均涨幅 | 状态 |\n| :--: | :--: | :--: |\n"
        
        for _, row in summary.iterrows():
            hour = int(row['delay'])
            avg_chg = row['mean']
            
            # 状态判定
            status = "👀"
            if avg_chg > 10.0: status = "⛔️高危"
            elif avg_chg > 5.0: status = "🚀暴涨"
            elif avg_chg > 0: status = "⏳微涨"
            elif avg_chg < -1.0: status = "✅转跌"
            elif avg_chg <= 0: status = "📉微跌"
            
            best_time_md += f"| +{hour}h | {avg_chg:+.1f}% | {status} |\n"
    else:
        best_time_md = "数据积累中..."

    return daily_review_md, best_time_md

def analyze_strategies():
    print("📊 开始生成详细分析报告...")

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
    history_df['Pos_PnL'] = pd.to_numeric(history_df['Pos_PnL'], errors='coerce').fillna(0)

    # --- 1. 市场分析 ---
    daily_review_str, best_time_str = analyze_market_mechanics(history_df)

    # --- 2. 策略排行 ---
    # 备用计算：防止 ROUND_RES 缺失
    close_events = history_df[history_df['Type'] == 'CLOSE'].copy()
    rounds_fallback = pd.DataFrame()
    if not close_events.empty:
        rounds_fallback = close_events.groupby(['Strategy_ID', 'Time'])['Pos_PnL'].sum().reset_index()

    stats_list = []
    for i in range(24):
        s_id = str(i)
        
        # 优先取结算数据
        rounds_res = history_df[(history_df['Strategy_ID'] == i) & (history_df['Type'] == 'ROUND_RES')]
        
        # 默认值
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
            # 终极备用：净值差
            col = f"S_{i}"
            if col in equity_df.columns:
                series = pd.to_numeric(equity_df[col], errors='coerce').dropna()
                if len(series) > 0: pnl = series.iloc[-1] - 1000

        # 修正：max_loss 应当是负数，如果全是盈利则为0
        if max_loss > 0: max_loss = 0
        
        # 胜率计算
        win_rate = (wins / total * 100) if total > 0 else 0
        win_str = f"{int(win_rate)}%({wins}/{total})"
        
        # 回撤计算
        max_dd = 0.0
        col = f"S_{i}"
        if col in equity_df.columns: max_dd = calculate_max_drawdown(equity_df[col])
        
        # 格式化 ID 和 时间 (合并以节省表格列)
        open_time = get_open_time_str(i)
        id_display = f"S{s_id}({open_time})"
        
        stats_list.append({
            'id': s_id,
            'id_disp': id_display,
            'pnl': pnl,
            'win_str': win_str,
            'dd': max_dd,
            'max_loss': max_loss
        })

    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    # 生成全量表格 (手机端优化版)
    # 列: 策略(时间) | 胜率 | 总盈 | 回撤 | 单亏
    rank_table = "| 策略(时间) | 胜率 | 总盈 | 回撤 | 单亏 |\n| :-- | :--: | :--: | :--: | :--: |\n"
    
    top_performer = ""
    for idx, s in enumerate(stats_list):
        if idx == 0: top_performer = f"S{s['id']}"
        
        # 数值格式化
        pnl_fmt = f"{s['pnl']:.0f}"
        dd_fmt = f"{s['dd']:.1f}%"
        loss_fmt = f"{s['max_loss']:.0f}"
        
        rank_table += f"| {s['id_disp']} | {s['win_str']} | {pnl_fmt} | {dd_fmt} | {loss_fmt} |\n"

    # --- 3. 发送报告 ---
    current_time = datetime.now().strftime("%m-%d %H:%M")
    
    title = f"📈 策略日报: {top_performer} 领跑"
    desp = f"""
**{current_time} (UTC+8)**
{daily_review_str}

### ⏳ 最佳做空时机 (Alpha)
{best_time_str}

### 🏆 全策略完整排行
{rank_table}
    """
    
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
