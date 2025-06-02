import os
import time
import pandas as pd
import requests # 使用 requests 替代 python-binance 的 Client
from datetime import datetime

# --- 配置参数 ---
SERVERCHAN_SENDKEY = os.environ.get('SERVERCHAN_SENDKEY')

# KLINE_INTERVAL 现在直接使用字符串，因为不再用 Client.KLINE_INTERVAL_1HOUR
KLINE_INTERVAL_STR = "1h" # 币安API要求的小时K线间隔字符串
VOLUME_MULTIPLIER = 10
MA_PERIOD = 20

# 动态获取交易对时的筛选条件
QUOTE_ASSET_FILTER = 'USDT'
CONTRACT_TYPE_FILTER = 'PERPETUAL'
STATUS_FILTER = 'TRADING'

# 币安U本位合约公共API基础URL
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"

def send_serverchan_notification(title, content):
    """发送 Server酱 通知"""
    if not SERVERCHAN_SENDKEY:
        print("ServerChan SendKey 未配置，跳过通知。")
        return
    url = f"https://sctapi.ftqq.com/{SERVERCHAN_SENDKEY}.send"
    data = {"title": title, "desp": content}
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
    """使用 requests 获取所有可交易的USDT本位永续合约交易对"""
    print(f"正在获取币安 {QUOTE_ASSET_FILTER} 本位永续合约交易对列表 (直接API请求)...")
    endpoint = "/fapi/v1/exchangeInfo"
    url = BINANCE_FUTURES_BASE_URL + endpoint
    try:
        response = requests.get(url, timeout=15) # 增加超时时间
        response.raise_for_status()  # 如果HTTP请求返回了错误状态码，则抛出异常
        exchange_info = response.json()
        symbols = []
        for item in exchange_info['symbols']:
            if (item['quoteAsset'] == QUOTE_ASSET_FILTER and
                item['contractType'] == CONTRACT_TYPE_FILTER and
                item['status'] == STATUS_FILTER and
                '_' not in item['symbol'] # 进一步过滤，例如 BTCUSDT_230929
               ):
                symbols.append(item['symbol'])
        symbols.sort()
        print(f"获取到 {len(symbols)} 个符合条件的交易对。")
        if len(symbols) > 0:
            print(f"部分交易对示例: {', '.join(symbols[:5])}...")
        return symbols
    except requests.exceptions.Timeout:
        print(f"获取合约交易对列表超时: {url}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"获取合约交易对列表失败: {e}")
        return []
    except Exception as e: # 例如 JSONDecodeError
        print(f"处理合约交易对列表响应数据时出错: {e}")
        return []


def get_klines_data(symbol, interval_str, limit=50):
    """使用 requests 获取K线数据并转换为DataFrame"""
    endpoint = "/fapi/v1/klines"
    url = BINANCE_FUTURES_BASE_URL + endpoint
    params = {
        'symbol': symbol,
        'interval': interval_str,
        'limit': limit
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        klines_raw = response.json()
        # API返回的K线数据格式:
        # [
        #   [
        #     1499040000000,      // Open time
        #     "0.01634790",       // Open
        #     "0.80000000",       // High
        #     "0.01575800",       // Low
        #     "0.01577100",       // Close
        #     "148976.11427815",  // Volume  <-- 我们需要这个
        #     1499644799999,      // Close time
        #     "2434.19055334",    // Quote asset volume
        #     308,                // Number of trades
        #     "1756.87402397",    // Taker buy base asset volume
        #     "28.46694368",      // Taker buy quote asset volume
        #     "17928899.62484339" // Ignore.
        #   ]
        # ]
        df = pd.DataFrame(klines_raw, columns=[
            'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Close time', 'Quote asset volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
        ])
        df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
        df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')
        # 确保交易量等字段是数字类型
        for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote asset volume']:
            df[col] = pd.to_numeric(df[col])
        return df
    except requests.exceptions.Timeout:
        print(f"获取 {symbol} K线数据超时: {url} with params {params}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"获取 {symbol} K线数据失败: {e}")
        return None
    except Exception as e: # 例如 JSONDecodeError 或 DataFrame处理错误
        print(f"处理 {symbol} K线数据时出错: {e}")
        return None


def check_volume_alert(symbol):
    """检查指定交易对的交易量警报"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 正在检查 {symbol}...")
    # 使用 KLINE_INTERVAL_STR
    df = get_klines_data(symbol, KLINE_INTERVAL_STR, limit=MA_PERIOD + 10)

    if df is None or len(df) < MA_PERIOD + 2:
        print(f"{symbol} 数据不足，无法进行分析 (需要至少 {MA_PERIOD + 2} 条, 实际 {len(df) if df is not None else 0} 条)。")
        return

    df['Volume_MA'] = df['Volume'].rolling(window=MA_PERIOD).mean()

    if len(df) < 2:
        print(f"{symbol} 数据不足两条，无法比较。")
        return

    current_candle_index = -1
    previous_candle_index = -2

    if abs(previous_candle_index) > len(df): # 确保有足够的数据行
        print(f"{symbol} 数据条数不足以获取前一根K线 (需要 {abs(previous_candle_index)+1} 条, 实际 {len(df)} 条)。")
        return

    current_volume = df['Volume'].iloc[current_candle_index]
    current_close_time = df['Close time'].iloc[current_candle_index]
    previous_volume = df['Volume'].iloc[previous_candle_index]
    
    if len(df) < MA_PERIOD + 1: # 确保有足够数据计算MA
        ma20_volume = float('nan')
    else:
        # MA20是基于 previous_candle_index 及之前的K线计算得到的
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
    print("开始执行币安合约交易量警报监控 (使用直接API请求)。")
    
    if not SERVERCHAN_SENDKEY:
        print("警告: SERVERCHAN_SENDKEY 未配置，将无法发送通知。")

    symbols_to_monitor = get_tradable_usdt_perpetual_futures_symbols()

    if not symbols_to_monitor:
        print("未能获取到可监控的合约交易对，脚本终止。")
        exit(1) # 明确以非零状态码退出，表示有问题
    
    print(f"将监控 {len(symbols_to_monitor)} 个交易对。")
    
    # 减少休眠时间，因为 requests 可能比完整的 client 库轻量一些
    # 但仍保留以避免对API造成冲击
    sleep_between_symbols = 0.3 

    for symbol_item in symbols_to_monitor:
        check_volume_alert(symbol_item)
        time.sleep(sleep_between_symbols) 

    print("所有交易对检查完毕。")
