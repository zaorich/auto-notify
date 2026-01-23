import ccxt
import pandas as pd
from datetime import datetime
import time
import requests
import os

# --- 配置参数 ---
EXCHANGE = 'okx'
QUOTE_CURRENCY = 'USDT'
TURNOVER_THRESHOLD = 100_000_000  # 1亿美元
DAYS_TO_FETCH = 120
LOOKBACK_PERIOD = 30

# --- Server酱配置 ---
SERVER_JIANG_KEY = os.environ.get('SERVER_JIANG_KEY', 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e')

# --- 发送通知函数 ---
def send_server_chan_notification(title, content):
    try:
        url = f"https://sctapi.ftqq.com/{SERVER_JIANG_KEY}.send"
        data = {'title': title, 'desp': content}
        response = requests.post(url, data=data, timeout=30)
        if response.json().get('code') == 0:
            print(f"✅ 微信通知发送成功")
        else:
            print(f"⚠️ 微信通知发送失败: {response.text}")
    except Exception as e:
        print(f"❌ 发送出错: {e}")

# --- 初始化交易所 ---
try:
    exchange = getattr(ccxt, EXCHANGE)()
    if exchange.id == 'okx': exchange.options['defaultType'] = 'spot'
    exchange.load_markets()
    print(f"成功连接 {EXCHANGE}，开始扫描 {QUOTE_CURRENCY} 交易对...")
except Exception as e:
    print(f"连接失败: {e}")
    exit()

symbols = [s for s in exchange.symbols if s.endswith(f'/{QUOTE_CURRENCY}') and exchange.markets[s].get('spot', False)]

# --- 主分析逻辑 ---
all_results = []
total_symbols = len(symbols)

for i, symbol in enumerate(symbols):
    print(f"\r[{i+1}/{total_symbols}] 扫描中: {symbol:<15}", end="", flush=True)
    
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, '1d', limit=DAYS_TO_FETCH)
        if not ohlcv or len(ohlcv) < LOOKBACK_PERIOD + 1: continue

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
        df['turnover_usd'] = df['volume'] * df['close']
        
        for j in range(LOOKBACK_PERIOD, len(df)):
            today_turnover = df.loc[j, 'turnover_usd']
            if today_turnover > TURNOVER_THRESHOLD:
                lookback_df = df.loc[j - LOOKBACK_PERIOD : j - 1]
                if lookback_df['turnover_usd'].max() < TURNOVER_THRESHOLD:
                    # 发现突破！
                    spike_row = df.loc[j]
                    spike_close = spike_row['close']
                    
                    # 定义辅助函数计算涨幅
                    def get_pct(days):
                        if j + days < len(df):
                            return (df.loc[j+days, 'close'] / spike_close - 1) * 100
                        return None

                    # 计算最高潜力
                    future_df = df.loc[j + 1:]
                    peak_gain = None
                    days_to_peak = None
                    if not future_df.empty:
                        peak_high = future_df['high'].max()
                        peak_gain = (peak_high / spike_close - 1) * 100
                        days_to_peak = future_df['high'].idxmax() - j

                    all_results.append({
                        'Trading Pair': symbol,
                        'Spike Date': spike_row['date'],
                        'Spike Day Turnover ($)': spike_row['turnover_usd'],
                        'Price After 1 Day (%)': get_pct(1),
                        'Price After 3 Days (%)': get_pct(3),
                        'Price After 7 Days (%)': get_pct(7),
                        'Price After 30 Days (%)': get_pct(30),
                        'Peak Gain (%)': peak_gain,
                        'Days to Peak': days_to_peak
                    })
                    break 
        time.sleep(0.1) 
    except:
        continue

print("\n") 

# --- 结果处理与发送 ---
if not all_results:
    print("未发现符合条件的交易对。")
    send_server_chan_notification("OKX扫描结果", "本次扫描未发现符合条件的成交额突变交易对。")
else:
    results_df = pd.DataFrame(all_results)
    
    # 1. 格式化数据 (生成两个版本：rawData用于计算，export_df用于展示)
    export_df = results_df.copy()
    
    # 格式化金额
    export_df['Spike Day Turnover ($)'] = export_df['Spike Day Turnover ($)'].apply(lambda x: f"${x:,.0f}")
    
    # 格式化所有百分比列
    pct_cols = ['Price After 1 Day (%)', 'Price After 3 Days (%)', 'Price After 7 Days (%)', 'Price After 30 Days (%)', 'Peak Gain (%)']
    for col in pct_cols:
        export_df[col] = export_df[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "-")
    
    # 格式化天数
    export_df['Days to Peak'] = export_df['Days to Peak'].apply(lambda x: f"{int(x)}天" if pd.notnull(x) else "-")

    # 按日期降序
    export_df = export_df.sort_values(by='Spike Date', ascending=False)
    
    # 打印控制台报告
    print("--- 分析结果 ---")
    print(export_df.to_string()) 
    print("----------------")

    # 保存CSV
    export_df.to_csv('okx_turnover_spikes_analysis.csv', index=False, encoding='utf-8-sig')
    print(f"完整报告已保存: okx_turnover_spikes_analysis.csv")

    # 2. 生成微信通知 (根据条数决定是否精简)
    count = len(export_df)
    notify_title = f"OKX爆量分析: 发现{count}个"
    
    if count <= 20:
        # --- 完整模式 (条数少，发送详细表格) ---
        notify_content = f"### 📊 发现 {count} 个爆量币种 (完整列表)\n\n"
        # 包含更多列：1天/7天/30天/最高/耗时
        notify_content += "| 币种 | 日期 | 爆发额 | 1D | 7D | 30D | 最高 | 耗时 |\n"
        notify_content += "|---|---|---|---|---|---|---|---|\n"
        
        for index, row in export_df.iterrows():
            s = row['Trading Pair'].replace('/USDT', '')
            d = str(row['Spike Date'])[5:] # 简写日期 10-12
            # 金额稍微简化一下去掉$符号，防止表格太挤，但保留完整数字
            v = row['Spike Day Turnover ($)'].replace('$', '')
            if len(v) > 10: v = f"{float(v.replace(',',''))/1000000:.0f}M" # 如果数字太长才变M

            p1 = row['Price After 1 Day (%)']
            p7 = row['Price After 7 Days (%)']
            p30 = row['Price After 30 Days (%)']
            pk = row['Peak Gain (%)']
            day = row['Days to Peak'].replace('天', '')

            notify_content += f"| {s} | {d} | {v} | {p1} | {p7} | {p30} | {pk} | {day} |\n"
            
    else:
        # --- 精简模式 (超过20条，强制精简以防发送失败) ---
        notify_content = f"### 📊 发现 {count} 个爆量币种 (Top 20)\n"
        notify_content += f"> ⚠️ 数据过多，仅显示最近20条，完整版请看CSV\n\n"
        notify_content += "| 币种 | 日期 | 爆发额 | 30天后 | 潜力 |\n"
        notify_content += "|---|---|---|---|---|\n"
        
        for index, row in export_df.head(20).iterrows():
            s = row['Trading Pair'].replace('/USDT', '')
            d = str(row['Spike Date'])[5:]
            # 强制简化金额为 M
            raw_vol = float(row['Spike Day Turnover ($)'].replace('$','').replace(',',''))
            v = f"{raw_vol/1_000_000:.0f}M"
            p30 = row['Price After 30 Days (%)']
            pk = row['Peak Gain (%)']
            
            notify_content += f"| {s} | {d} | {v} | {p30} | {pk} |\n"

    print("正在推送微信通知...")
    send_server_chan_notification(notify_title, notify_content)
