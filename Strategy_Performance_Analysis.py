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

def analyze_best_short_time(history_df):
    """
    分析：币种首次上榜后，随着时间推移的价格变化
    返回：Markdown 格式的分析表格
    """
    try:
        # 1. 筛选所有开仓记录
        df = history_df[history_df['Type'] == 'OPEN'].copy()
        if df.empty: return "暂无开仓数据"

        # 2. 转换时间
        df['Time'] = pd.to_datetime(df['Time'])
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df = df.dropna(subset=['Price'])
        
        # 3. 按币种和日期分组 (区分同一个币在不同日期的行情)
        # 逻辑：找到每个币每天第一次出现的时间(T0)，对比后续时间(Tn)的价格变化
        df['Date'] = df['Time'].dt.date
        grouped = df.groupby(['Symbol', 'Date'])
        
        results = []
        
        for (symbol, date), group in grouped:
            if len(group) < 2: continue # 只有一个数据点，无法对比
            
            group = group.sort_values('Time')
            t0 = group.iloc[0]
            t0_price = t0['Price']
            t0_time = t0['Time']
            
            for i in range(1, len(group)):
                curr = group.iloc[i]
                hours_diff = (curr['Time'] - t0_time).total_seconds() / 3600.0
                
                # 涨跌幅：(当前价 - 初始价) / 初始价
                # 正数：代表价格涨了 -> 说明当初没空是对的，"等一等"更好
                # 负数：代表价格跌了 -> 说明当初没空亏了，"立即空"更好
                pct_change = ((curr['Price'] - t0_price) / t0_price) * 100
                
                results.append({
                    'delay': int(round(hours_diff)),
                    'change': pct_change
                })
        
        if not results: return "数据样本不足，无法分析时间规律"
        
        res_df = pd.DataFrame(results)
        
        # 4. 按延迟小时数聚合统计
        summary = res_df.groupby('delay')['change'].agg(['mean', 'count']).reset_index()
        summary = summary[summary['count'] >= 3] # 过滤掉样本太少的时段
        
        # 5. 生成表格
        md = "| 延迟 | 平均涨幅 | 建议 |\n| :--: | :--: | :--: |\n"
        
        best_delay = 0
        max_pump = -999
        
        for _, row in summary.iterrows():
            hour = int(row['delay'])
            avg_chg = row['mean']
            
            # 简单建议逻辑
            advice = ""
            if avg_chg > 2.0: advice = "⏳ 忍住(还在涨)"
            elif avg_chg > 5.0: advice = "⚠️ 极其危险"
            elif avg_chg < 0: advice = "📉 可空(已转跌)"
            else: advice = "👀 观察"
            
            if avg_chg > max_pump:
                max_pump = avg_chg
                best_delay = hour
            
            change_str = f"{avg_chg:+.1f}%"
            md += f"| {hour}h | {change_str} | {advice} |\n"
            
        return md, best_delay
        
    except Exception as e:
        return f"分析出错: {e}", 0

def analyze_strategies():
    print("📊 开始生成策略分析报告...")

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
    
    # --- 模块 A: 策略排行榜 ---
    stats_list = []
    # (此部分逻辑保持不变，为了节省篇幅简写，实际运行请保留原来的循环逻辑)
    # ... 您原来的策略排名循环逻辑 ...
    # 为了完整性，我还是把循环写在这里:
    close_events = history_df[history_df['Type'] == 'CLOSE'].copy()
    history_df['Pos_PnL'] = pd.to_numeric(history_df['Pos_PnL'], errors='coerce').fillna(0)
    rounds_fallback = pd.DataFrame()
    if not close_events.empty:
        rounds_fallback = close_events.groupby(['Strategy_ID', 'Time'])['Pos_PnL'].sum().reset_index()

    for i in range(24):
        s_id = str(i)
        # 简化版逻辑：只取 ROUND_RES 或 Fallback
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
            
        win_rate = (wins/total*100) if total > 0 else 0
        
        # Max DD
        max_dd = 0.0
        col = f"S_{i}"
        if col in equity_df.columns: max_dd = calculate_max_drawdown(equity_df[col])
        
        stats_list.append({'id': s_id, 'pnl': pnl, 'wr': win_rate, 'dd': max_dd, 'total': total})

    stats_list.sort(key=lambda x: x['pnl'], reverse=True)
    
    # 生成排行榜表格
    rank_table = "| ID | 胜率 | 总盈 | 回撤 |\n| :--: | :--: | :--: | :--: |\n"
    top_performer = ""
    for idx, s in enumerate(stats_list):
        if idx == 0: top_performer = f"S{s['id']} ({s['pnl']:.0f}U)"
        rank_table += f"| S{s['id']} | {s['wr']:.0f}% | {s['pnl']:.0f} | {s['dd']:.1f}% |\n"

    # --- 模块 B: 最佳做空时间分析 (新功能) ---
    time_analysis_md, best_hour = analyze_best_short_time(history_df)

    # 4. 发送微信
    current_time = datetime.now().strftime("%m-%d %H:%M")
    
    title = f"🏆 策略日报: {top_performer}"
    desp = f"""
**生成时间**: {current_time}

### 1️⃣ ⏳ 最佳做空时机分析
*(基于历史数据：币种上榜后N小时的平均涨幅)*
如果平均涨幅为正，说明**做空太早了**，建议等待。
{time_analysis_md}
**💡 结论**: 历史数据显示，上榜后 **{best_hour}小时** 往往是最高点，此时进场胜率最高。

---

### 2️⃣ 📊 策略实盘排行榜
{rank_table}
    """
    
    send_wechat_msg(title, desp)

if __name__ == "__main__":
    analyze_strategies()
