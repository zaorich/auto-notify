import ccxt
import pandas as pd
from datetime import datetime
import time
import requests # 新增：用于发送HTTP请求
import os       # 新增：用于读取环境变量

# --- 配置参数 ---
EXCHANGE = 'okx'
QUOTE_CURRENCY = 'USDT'
TURNOVER_THRESHOLD = 100_000_000  # 成交额阈值：1亿美元
DAYS_TO_FETCH = 120               # 获取过去多少天的数据
LOOKBACK_PERIOD = 30              # 定义“长期”：突破前至少连续30天成交额低于阈值

# --- Server酱配置 (参考您提供的代码) ---
# 优先读取环境变量，如果没有则使用您提供的默认Key
SERVER_JIANG_KEY = os.environ.get('SERVER_JIANG_KEY', 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e')

# --- 新增：发送通知函数 ---
def send_server_chan_notification(title, content):
    """
    通过Server酱发送通知
    """
    try:
        url = f"https://sctapi.ftqq.com/{SERVER_JIANG_KEY}.send"
        data = {
            'title': title,
            'desp': content
        }
        
        response = requests.post(url, data=data, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        if result.get('code') == 0:
            print(f"✅ 通知发送成功: {title}")
        else:
            print(f"⚠️ 通知发送失败: {result}")
            
    except Exception as e:
        print(f"❌ 发送通知时出错: {e}")

# --- 初始化交易所 ---
try:
    exchange = getattr(ccxt, EXCHANGE)()
    if exchange.id == 'okx':
        exchange.options['defaultType'] = 'spot'
    exchange.load_markets()
    print(f"成功连接到 {EXCHANGE} 交易所。")
except Exception as e:
    print(f"连接交易所失败: {e}")
    exit()

# --- 获取所有现货USDT交易对 ---
symbols = [s for s in exchange.symbols if s.endswith(f'/{QUOTE_CURRENCY}') and exchange.markets[s].get('spot', False)]
print(f"共找到 {len(symbols)} 个 {QUOTE_CURRENCY} 现货交易对。开始分析...")

# --- 主分析逻辑 (保持原有逻辑不变) ---
all_results = []
total_symbols = len(symbols)

for i, symbol in enumerate(symbols):
    print(f"\n[{i+1}/{total_symbols}] 正在处理: {symbol}")
    try:
        # 1. 获取历史K线数据
        ohlcv = exchange.fetch_ohlcv(symbol, '1d', limit=DAYS_TO_FETCH)
        
        if not ohlcv or len(ohlcv) < LOOKBACK_PERIOD + 1:
            print(f" -> 数据不足，跳过。")
            continue

        # 2. 将数据转换为Pandas DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
        
        # 3. 计算每日成交额 (Volume * Close Price)
        df['turnover_usd'] = df['volume'] * df['close']
        
        # 4. 寻找突破点
        for j in range(LOOKBACK_PERIOD, len(df)):
            today_turnover = df.loc[j, 'turnover_usd']
            
            # 检查突破条件
            if today_turnover > TURNOVER_THRESHOLD:
                lookback_df = df.loc[j - LOOKBACK_PERIOD : j - 1]
                if lookback_df['turnover_usd'].max() < TURNOVER_THRESHOLD:
                    spike_row = df.loc[j]
                    print(f" *** 发现突破点! 日期: {spike_row['date']}, 成交额: ${spike_row['turnover_usd']:,.0f} ***")

                    # 5. 计算后续走势
                    spike_date = spike_row['date']
                    spike_close_price = spike_row['close']
                    
                    def get_future_price_change(days):
                        future_index = j + days
                        if future_index < len(df):
                            future_close_price = df.loc[future_index, 'close']
                            return (future_close_price / spike_close_price - 1) * 100
                        return None

                    peak_price_after = None
                    days_to_peak = None
                    future_df = df.loc[j + 1:]
                    if not future_df.empty:
                        peak_high = future_df['high'].max()
                        peak_price_after = (peak_high / spike_close_price - 1) * 100
                        days_to_peak = future_df['high'].idxmax() - j
                        
                    result = {
                        'Trading Pair': symbol,
                        'Spike Date': spike_date,
                        'Spike Day Turnover ($)': spike_row['turnover_usd'],
                        'Prev 30d Max Turnover ($)': lookback_df['turnover_usd'].max(),
                        'Spike Day Price Change (%)': (spike_row['close'] / spike_row['open'] - 1) * 100,
                        'Price After 1 Day (%)': get_future_price_change(1),
                        'Price After 3 Days (%)': get_future_price_change(3),
                        'Price After 7 Days (%)': get_future_price_change(7),
                        'Price After 30 Days (%)': get_future_price_change(30),
                        'Peak Price After Spike (%)': peak_price_after,
                        'Days to Peak': days_to_peak
                    }
                    all_results.append(result)
                    break 
                    
        time.sleep(0.2) #稍微降低频率以防太快

    except Exception as e:
        print(f" -> 处理 {symbol} 时出错: {e}")
        continue

# --- 结果整理、保存与发送通知 ---
if not all_results:
    print("\n在过去的时间范围内，没有找到符合条件的交易对。")
    send_server_chan_notification("OKX成交额突变分析", "本次运行未发现符合条件的成交额突变交易对。")
else:
    print("\n分析完成！正在生成结果报告...")
    results_df = pd.DataFrame(all_results)
    
    # 格式化数据用于CSV保存
    export_df = results_df.copy()
    for col in ['Spike Day Turnover ($)', 'Prev 30d Max Turnover ($)']:
        export_df[col] = export_df[col].apply(lambda x: f"${x:,.0f}")
    for col in ['Spike Day Price Change (%)', 'Price After 1 Day (%)', 'Price After 3 Days (%)', 'Price After 7 Days (%)', 'Price After 30 Days (%)', 'Peak Price After Spike (%)']:
        export_df[col] = export_df[col].apply(lambda x: f"{x:.2f}%" if x is not None else "N/A")
    
    # 按突破日期降序排列
    export_df = export_df.sort_values(by='Spike Date', ascending=False)
    
    # 保存CSV
    output_filename = 'okx_turnover_spikes_analysis.csv'
    export_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    print(f"\n报告已成功保存到文件: {output_filename}")

    # --- 生成 Server酱 通知内容 (Markdown格式) ---
    notify_title = f"OKX成交额突变发现: {len(all_results)}个"
    
    # 构建Markdown表格
    notify_content = f"### 📊 发现 {len(all_results)} 个交易对在沉寂后成交额突破 ${TURNOVER_THRESHOLD/1_000_000:,.0f}M\n\n"
    
    # 表头
    notify_content += "| 币种 | 日期 | 爆发额 | 30天后 | 最高涨幅 |\n"
    notify_content += "|---|---|---|---|---|\n"
    
    # 填充表格行 (只发送前20条，防止消息过长发送失败)
    for index, row in export_df.head(20).iterrows():
        symbol_short = row['Trading Pair'].replace('/USDT', '')
        date_str = str(row['Spike Date'])[5:] # 只取 MM-DD
        turnover_short = row['Spike Day Turnover ($)'].replace('$', '').replace(',', '')
        # 简化成交额显示 (例如 120,000,000 -> 120M)
        try:
             turnover_val = float(turnover_short)
             turnover_str = f"{turnover_val/1_000_000:.0f}M"
        except:
             turnover_str = turnover_short

        price_30d = row['Price After 30 Days (%)']
        peak_gain = row['Peak Price After Spike (%)']
        
        notify_content += f"| {symbol_short} | {date_str} | {turnover_str} | {price_30d} | {peak_gain} |\n"
    
    if len(export_df) > 20:
        notify_content += f"\n*注：仅显示前20条，完整数据请查看生成的CSV文件。*"

    # 发送通知
    print("正在发送微信通知...")
    send_server_chan_notification(notify_title, notify_content)
