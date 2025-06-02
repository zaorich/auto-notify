import os
import time
import pandas as pd
import requests
from binance.client import Client # 确保 Client 被正确导入
from datetime import datetime

# --- 配置参数 ---
# SERVERCHAN_SENDKEY 仍然需要从环境变量中获取
SERVERCHAN_SENDKEY = os.environ.get('SERVERCHAN_SENDKEY')

KLINE_INTERVAL = Client.KLINE_INTERVAL_1HOUR
VOLUME_MULTIPLIER = 10
MA_PERIOD = 20

# 动态获取交易对时的筛选条件
QUOTE_ASSET_FILTER = 'USDT'
CONTRACT_TYPE_FILTER = 'PERPETUAL'
STATUS_FILTER = 'TRADING'

# --- 初始化币安客户端 (无需 API Key/Secret) ---
client = Client()

def send_serverchan_notification(title, content):
    """发送 Server酱 通知"""
    if not SERVERCHAN_SENDKEY:
        print("ServerChan SendKey 未配置，跳过通知。")
        return

    url = f"https://sctapi.ftqq.com/{SERVERCHAN_SENDKEY}.send"
    data = {
        "title": title,
        "desp": content
    }
    try:
        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()
        result = response.json()
        if result.get("code") == 0 or result.get("errno") == 0:
            print(f"ServerChan 通知发送成功: {title}")
        else:
            print(f"ServerChan 通知发送失败: {result.get('message', '未知错误')}")
    except requests.exceptions.RequestException as e:
        print(f"发送 ServerChan 通知时发生网络错误: {e}")
    except Exception as e:
        print(f"发送 ServerChan 通知时发生未知错误: {e}")

def get_tradable_usdt_perpetual_futures_symbols():
    """获取所有可交易的USDT本位永续合约交易对"""
    print(f"正在获取币安 {QUOTE_ASSET_FILTER} 本位永续合约交易对列表 (公共API)...")
    try:
        exchange_info = client.futures_exchange_info() # U本位合约信息
        symbols = []
        for item in exchange_info['symbols']:
            if (item['quoteAsset'] == QUOTE_ASSET_FILTER and
                item['contractType'] == CONTRACT_TYPE_FILTER and
                item['status'] == STATUS_FILTER and
                # 额外的过滤，以防万一API返回一些非标准永续合约名称
                # 正常的永续合约如 BTCUSDT, ETHUSDT
                # 有些平台可能有类似 BTC_PERP 的，或者交割合约 BTCUSDT_240628
                # contractType == 'PERPETUAL' 应该能过滤掉大部分非永续
                # 此处 '_' not in item['pair'] 可能更准确，因为 'pair' 是 'BTCUSDT'
                # 但 'symbol' 也是 'BTCUSDT' for perpetuals.
                # 对于永续合约, symbol 和 pair 通常是一样的。
                # 保留这个过滤，以防出现如 BTCUSDT_ settimestamp 这种奇怪的永续合约变种
                '_' not in item['symbol']
                ):
                symbols.append(item['symbol'])
        
        symbols.sort()
        print(f"获取到 {len(symbols)} 个符合条件的交易对。")
        if len(symbols) > 0:
            print(f"部分交易对示例: {', '.join(symbols[:5])}...")
        return symbols
    except Exception as e:
        print(f"获取合约交易对列表失败: {e}")
        return []

def get_klines_data(symbol, interval, limit=50):
    """获取K线数据并转换为DataFrame"""
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit) # U本位合约K线
        df = pd.DataFrame(klines, columns=[
            'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Close time', 'Quote asset volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
        ])
        df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
        df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')
        for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote asset volume']:
            df[col] = pd.to_numeric(df[col])
        return df
    except Exception as e:
        print(f"获取 {symbol} K线数据失败: {e}")
        return None

def check_volume_alert(symbol):
    """检查指定交易对的交易量警报"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 正在检查 {symbol}...")
    df = get_klines_data(symbol, KLINE_INTERVAL, limit=MA_PERIOD + 10)

    if df is None or len(df) < MA_PERIOD + 2:
        print(f"{symbol} 数据不足，无法进行分析 (需要至少 {MA_PERIOD + 2} 条, 实际 {len(df) if df is not None else 0} 条)。")
        return

    df['Volume_MA'] = df['Volume'].rolling(window=MA_PERIOD).mean()

    if len(df) < 2:
        print(f"{symbol} 数据不足两条，无法比较。")
        return

    current_candle_index = -1
    previous_candle_index = -2

    if abs(previous_candle_index) > len(df):
        print(f"{symbol} 数据条数不足以获取前一根K线。")
        return

    current_volume = df['Volume'].iloc[current_candle_index]
    current_close_time = df['Close time'].iloc[current_candle_index]
    previous_volume = df['Volume'].iloc[previous_candle_index]
    
    if len(df) < MA_PERIOD + 1:
        ma20_volume = float('nan')
    else:
        ma20_volume = df['Volume_MA'].iloc[previous_candle_index]

    print(f"{symbol} @ {current_close_time.strftime('%Y-%m-%d %H:%M')} UTC:")
    print(f"  当前交易量: {current_volume:,.2f}")
    print(f"  前一小时交易量: {previous_volume:,.2f}")
    print(f"  MA{MA_PERIOD}交易量 (基于前一小时及之前): {ma20_volume:,.2f if pd.notna(ma20_volume) else 'N/A'}")

    alert_triggered = False
    alert_reasons = []

    if previous_volume > 0:
        if current_volume >= VOLUME_MULTIPLIER * previous_volume:
            alert_triggered = True
            reason = f"当前交易量 ({current_volume:,.2f}) >= {VOLUME_MULTIPLIER} * 前一小时交易量 ({previous_volume:,.2f})"
            alert_reasons.append(reason)
            print(f"  ALERT: {reason}")
    elif current_volume > 0 :
        print(f"  INFO: 前一小时交易量为0，当前交易量为 {current_volume:,.2f}。")

    if pd.notna(ma20_volume) and ma20_volume > 0:
        if current_volume >= VOLUME_MULTIPLIER * ma20_volume:
            alert_triggered = True
            reason = f"当前交易量 ({current_volume:,.2f}) >= {VOLUME_MULTIPLIER} * MA{MA_PERIOD}交易量 ({ma20_volume:,.2f})"
            if reason not in alert_reasons:
                 alert_reasons.append(reason)
            print(f"  ALERT: {reason}")
    elif pd.notna(ma20_volume) and ma20_volume == 0 and current_volume > 0:
        print(f"  INFO: MA{MA_PERIOD}交易量为0，当前交易量为 {current_volume:,.2f}。")
    elif pd.isna(ma20_volume):
        print(f"  INFO: MA{MA_PERIOD}交易量无法计算。")

    if alert_triggered:
        title = f"币安 {symbol} 合约小时交易量警报!"
        content = (
            f"交易对: {symbol}\n"
            f"时间 (K线收盘): {current_close_time.strftime('%Y-%m-%d %H:%M')} UTC\n"
            f"当前交易量: {current_volume:,.2f}\n"
            f"前一小时交易量: {previous_volume:,.2f}\n"
            f"MA{MA_PERIOD}交易量 (前一小时及之前): {ma20_volume:,.2f if pd.notna(ma20_volume) else 'N/A'}\n\n"
            f"触发原因:\n" + "\n".join([f"- {r}" for r in alert_reasons])
        )
        send_serverchan_notification(title, content)
    else:
        print(f"  {symbol} 未触发警报。")
    print("-" * 30)

if __name__ == "__main__":
    print("开始执行币安合约交易量警报监控 (使用公共API)。")
    
    if not SERVERCHAN_SENDKEY:
        print("警告: SERVERCHAN_SENDKEY 未配置，将无法发送通知。")

    symbols_to_monitor = get_tradable_usdt_perpetual_futures_symbols()

    if not symbols_to_monitor:
        print("未能获取到可监控的合约交易对，脚本终止。")
        exit()
    
    print(f"将监控 {len(symbols_to_monitor)} 个交易对。")
    
    sleep_between_symbols = 0.5 # 每检查一个交易对后暂停的秒数, 避免IP频率限制

    for symbol_item in symbols_to_monitor:
        check_volume_alert(symbol_item)
        time.sleep(sleep_between_symbols) 

    print("所有交易对检查完毕。")
