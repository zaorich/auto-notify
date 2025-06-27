import ccxt
import pandas as pd
from datetime import datetime
import time

# --- 配置参数 ---
EXCHANGE = 'okx'
QUOTE_CURRENCY = 'USDT'
TURNOVER_THRESHOLD = 100_000_000  # 成交额阈值：1亿美元
DAYS_TO_FETCH = 120                # 获取过去多少天的数据（需要大于90+30，确保有足够数据计算未来30天走势）
LOOKBACK_PERIOD = 30               # 定义“长期”：突破前至少连续30天成交额低于阈值

# --- 初始化交易所 ---
try:
    exchange = getattr(ccxt, EXCHANGE)()
    # CCXT的OKX需要这个设置才能获取所有交易对
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

# --- 主分析逻辑 ---
all_results = []
total_symbols = len(symbols)

for i, symbol in enumerate(symbols):
    print(f"\n[{i+1}/{total_symbols}] 正在处理: {symbol}")
    try:
        # 1. 获取历史K线数据
        # fetch_ohlcv返回 [timestamp, open, high, low, close, volume]
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
        # 从第 LOOKBACK_PERIOD 天开始遍历，确保有足够的回看窗口
        for j in range(LOOKBACK_PERIOD, len(df)):
            today_turnover = df.loc[j, 'turnover_usd']
            
            # 检查突破条件：今天 > 阈值
            if today_turnover > TURNOVER_THRESHOLD:
                # 检查“长期低于阈值”条件：回看周期内的最大成交额 < 阈值
                lookback_df = df.loc[j - LOOKBACK_PERIOD : j - 1]
                if lookback_df['turnover_usd'].max() < TURNOVER_THRESHOLD:
                    # 找到了一个合格的突破点！
                    spike_row = df.loc[j]
                    print(f" *** 发现突破点! 日期: {spike_row['date']}, 成交额: ${spike_row['turnover_usd']:,.0f} ***")

                    # 5. 计算后续走势
                    spike_date = spike_row['date']
                    spike_close_price = spike_row['close']
                    
                    # 定义一个辅助函数来计算未来价格变化
                    def get_future_price_change(days):
                        future_index = j + days
                        if future_index < len(df):
                            future_close_price = df.loc[future_index, 'close']
                            return (future_close_price / spike_close_price - 1) * 100
                        return None # 如果数据不够，返回None

                    # 计算后续最高价和所需天数
                    peak_price_after = None
                    days_to_peak = None
                    future_df = df.loc[j + 1:]
                    if not future_df.empty:
                        peak_high = future_df['high'].max()
                        peak_price_after = (peak_high / spike_close_price - 1) * 100
                        # idxmax() 返回第一次出现最大值的索引
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
                    
                    # 找到第一个突破点后，就停止对这个币的分析，继续下一个币
                    break 
                    
        # 防止API请求过于频繁
        time.sleep(0.5) # 0.5秒的延迟

    except Exception as e:
        print(f" -> 处理 {symbol} 时出错: {e}")
        continue

# --- 结果整理与输出 ---
if not all_results:
    print("\n在过去的时间范围内，没有找到符合条件的交易对。")
else:
    print("\n分析完成！正在生成结果报告...")
    results_df = pd.DataFrame(all_results)
    
    # 格式化输出，使其更易读
    for col in ['Spike Day Turnover ($)', 'Prev 30d Max Turnover ($)']:
        results_df[col] = results_df[col].apply(lambda x: f"${x:,.0f}")
        
    for col in ['Spike Day Price Change (%)', 'Price After 1 Day (%)', 'Price After 3 Days (%)', 'Price After 7 Days (%)', 'Price After 30 Days (%)', 'Peak Price After Spike (%)']:
        results_df[col] = results_df[col].apply(lambda x: f"{x:.2f}%" if x is not None else "N/A")
        
    # 按突破日期降序排列
    results_df = results_df.sort_values(by='Spike Date', ascending=False)
    
    # 保存到CSV文件
    output_filename = 'okx_turnover_spikes_analysis.csv'
    results_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print("\n--- 分析结果报告 ---")
    print(results_df.to_string())
    print(f"\n报告已成功保存到文件: {output_filename}")
