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

    # 微信显示优化：确保换行正确
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
        # 尝试读取
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
    hour = (8 + s_id_int) % 24
    return f"{hour:02d}点"

def analyze_market_mechanics(history_df):
    """
    包含两个部分：
    1. 昨日新币详细复盘 (具体的币)
    2. 历史最佳做空时机 (统计规律)
    """
    # 筛选开仓数据
    df = history_df[history_df['Type'] == 'OPEN'].copy()
    if df.empty: return "无数据", "无数据"

    df['Time'] = pd.to_datetime(df['Time'])
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df = df.dropna(subset=['Price'])
    
    # 转换为东八区时间方便阅读
    df['Time_CN'] = df['Time'] + timedelta(hours=8)

    # --- 分析 1: 昨日新币详细复盘 ---
    now = datetime.now()
    yesterday = now - timedelta(hours=24)
    
    # 找到最近24小时内出现过的币种
    recent_records = df[df['Time'] > yesterday].copy()
    
    daily_review_md = ""
    
    if not recent_records.empty:
        # 按币种分组，找到每个币在过去24h的第一次出现
        # 注意：这里我们只关心"新"上榜，或者在该时间段内首次出现的
        unique_coins = recent_records['Symbol'].unique()
        
        coin_stats = []
        for symbol in unique_coins:
            # 找到该币的所有记录（包括历史记录，以便计算涨跌）
            coin_hist = df[df['Symbol'] == symbol].sort_values('Time')
            
            # 找到它在过去24h的第一次出现时间 T0
            entries_in_24h = coin_hist[coin_hist['Time'] > yesterday]
            if entries_in_24h.empty: continue
            
            t0 = entries_in_24h.iloc[0]
            t0_price = t0['Price']
            t0_time = t0['Time']
            t0_time_cn_str = t0['Time_CN'].strftime("%H:%M")
            
            # 在全量历史中找 T0 之后的数据，计算最高涨幅
            subsequent = coin_hist[coin_hist['Time'] >= t0_time]
            
            max_price = subsequent['Price'].max()
            curr_price = subsequent.iloc[-1]['Price']
            
            # 计算指标
            max_pump_pct = ((max_price - t0_price) / t0_price) * 100
            curr_change_pct = ((curr_price - t0_price) / t0_price) * 100
            
            # 找到最高点发生的时间延迟
            max_price_row = subsequent[subsequent['Price'] == max_price].iloc[0]
            delay_hours = (max_price_row['Time'] - t0_time).total_seconds() / 3600
            
            coin_stats.append({
                'Symbol': symbol.replace('USDT', ''),
                'Time': t0_time_cn_str,
                'MaxPump': max_pump_pct,
                'MaxDelay': delay_hours,
                'Curr': curr_change_pct
            })
            
        # 生成复盘表格
        if coin_stats:
            # 按最高涨幅排序，看看谁是妖币
            coin_stats.sort(key=lambda x: x['MaxPump'], reverse=True)
            
            daily_review_md = "| 币种(上榜) | 最高涨幅 | 现价 |\n| :-- | :--: | :--: |\n"
            for c in coin_stats:
                # 格式化
                pump_str = f"{c['MaxPump']:+.1f}%(+{int(c['MaxDelay'])}h)"
                curr_str = f"{c['Curr']:+.1f}%"
                
                # 高亮妖币
                if c['MaxPump'] > 10: pump_str = f"🔥{pump_str}"
                
                daily_review_md += f"| {c['Symbol']}({c['Time']}) | {pump_str} | {curr_str} |\n"
        else:
            daily_review_md = "无新币数据"
    else:
        daily_review_md = "过去24h无新币上榜"


    # --- 分析 2: 历史最佳做空时机 (统计规律) ---
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
        summary = summary[summary['count'] >= 5] # 样本过滤
        
        best_time_md = "| 延迟 | 均涨幅 | 建议 |\n| :--: | :--: | :--: |\n"
        
        for _, row in summary.iterrows():
            hour = int(row['delay'])
            avg_chg = row['mean']
            
            advice = "👀"
            if avg_chg > 8.0: advice = "⛔️快跑"
            elif avg_chg > 4.0: advice = "🔥暴涨"
            elif avg_chg > 0: advice = "⏳微涨"
            elif avg_chg < -1.0: advice = "✅赢麻"
            elif avg_chg <= 0: advice = "📉微跌"
            
            best_time_md += f"| +{hour}h | {avg_chg:+.1f}% | {advice} |\n"
    else:
        best_time_md = "数据不足..."

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

    # 1. 市场分析模块
    daily_review_str, best_time_str = analyze_market_mechanics(history_df)

    # 2. 策略排行模块
    close_events = history_df[history_df['Type'] == 'CLOSE'].copy()
    rounds_fallback = pd.DataFrame()
    if not close_events.empty:
        rounds_fallback = close_events.groupby(['Strategy_ID', 'Time'])['Pos_PnL'].sum().reset_index()

    stats_list = []
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
                series = pd.to_numeric(equity_df[col], errors='coerce').dropna()
                if len(series) > 0: pnl = series.iloc[-1] - 1000

        if max_loss > 0: max_loss = 0
        
        win_rate = (wins / total * 100) if total > 0 else 0
        win_str = f"{int(win_rate)}%({wins}/{total})"
        
        max_dd = 0.0
        col = f"S_{i}"
        if col in equity_df.columns: max_dd = calculate_max_drawdown(equity_df[col])
        
        stats_list.append({
            'id': s_id,
            'open_time': get_open_time_str(i),
            'win_str': win_str,
            'pnl': pnl,
            'dd': max_dd,
            'max_loss': max_loss
        })

    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    # 3. 生成排行榜表格
    rank_table = "| 策略(时间) | 胜率 | 总盈 | 回撤 | 单亏 |\n| :-- | :--: | :--: | :--: | :--: |\n"
    
    top_performer = ""
    for idx, s in enumerate(stats_list):
        if idx == 0: top_performer = f"S{s['id']}"
        
        pnl_fmt = f"{s['pnl']:.0f}"
        dd_fmt = f"{s['dd']:.1f}%"
        loss_fmt = f"{s['max_loss']:.0f}"
        
        rank_table += f"| S{s['id']}({s['open_time']}) | {s['win_str']} | {pnl_fmt} | {dd_fmt} | {loss_fmt} |\n"

    # 4. 发送最终报告
    current_time = datetime.now().strftime("%m-%d %H:%M")
    
    title = f"📈 策略日报: {top_performer} 领跑"
    desp = f"""
**{current_time} (UTC+8)**

### 🔥 昨日新币复盘 (详细)
*记录过去24h上榜币种的表现*
{daily_review_str}

### ⏳ 历史最佳做空时机
*基于所有历史数据统计*
{best_time_str}

### 🏆 全策略排行榜
{rank_table}
    """
    
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
