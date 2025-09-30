#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import time
import os
from datetime import datetime
import pandas as pd
import numpy as np
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

class OKXMonitor:
    def __init__(self):
        # --- 核心配置 ---
        self.base_url = "https://www.okx.com"
        # 在这里填入你的Server酱SendKey
        self.server_jiang_key = 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e'
        
        # --- 策略参数 (与 test11.html 保持一致) ---
        self.MACD_VOLUME_THRESHOLD = 10_000_000  # 24小时最低成交额
        self.ATR_MULTIPLIER = 2.0  # ATR新鲜度乘数
        self.MAX_CANDLES_AGO = 5   # K线新鲜度阈值
        
        # --- 系统配置 ---
        self.session = self._create_session()
        self.timezone = pytz.timezone('Asia/Shanghai')
        self.state_file = 'watchlist_state.json'
        self.CONCURRENCY_LIMIT = 10 # 并发请求数，可根据网络情况调整
        self.request_timestamps = []
        self.RATE_LIMIT_COUNT = 18  # OKX API 限制: 2秒内20次请求
        self.RATE_LIMIT_WINDOW = 2000 # 2000毫秒

    def _create_session(self):
        session = requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        session.headers.update(headers)
        return session
    
    def get_current_time_str(self):
        return datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M:%S')

    def _rate_limiter(self):
        now = int(time.time() * 1000)
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < self.RATE_LIMIT_WINDOW]
        if len(self.request_timestamps) >= self.RATE_LIMIT_COUNT:
            oldest_request_time = self.request_timestamps[0]
            time_passed = now - oldest_request_time
            delay = (self.RATE_LIMIT_WINDOW - time_passed) / 1000
            if delay > 0:
                time.sleep(delay + 0.05)
        self.request_timestamps.append(int(time.time() * 1000))

    def fetch_with_retry(self, url, params, retries=5, timeout=15):
        self._rate_limiter()
        for i in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                if response.status_code == 429:
                    raise Exception(f"请求频率过高 (429)")
                response.raise_for_status()
                data = response.json()
                if data.get('code') == '0':
                    return data.get('data')
                raise Exception(f"API返回错误: {data.get('msg', '未知错误')}")
            except Exception as e:
                if i == retries - 1:
                    # print(f"获取 {url} 最终失败: {e}")
                    return None
                delay = 2**i + np.random.rand()
                # print(f"获取 {url} 出错: {e}. 在 {delay:.1f}秒 后重试...")
                time.sleep(delay)
        return None

    def send_notification(self, title, content):
        if not self.server_jiang_key:
            print(f"[{self.get_current_time_str()}] 未配置SERVER_JIANG_KEY，通知将打印到控制台。")
            print(f"标题: {title}\n内容:\n{content}")
            return
        
        # Markdown格式化
        desp = content.replace('\n', '\n\n')
        
        try:
            url = f"https://sctapi.ftqq.com/{self.server_jiang_key}.send"
            data = {'title': title, 'desp': desp}
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            result = response.json()
            if result.get('code') == 0:
                print(f"[{self.get_current_time_str()}] 通知发送成功: {title}")
            else:
                print(f"[{self.get_current_time_str()}] 通知发送失败: {result}")
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 发送通知时出错: {e}")

    # --- 数据获取模块 ---
    def get_perpetual_instruments(self):
        data = self.fetch_with_retry(f"{self.base_url}/api/v5/public/instruments", {'instType': 'SWAP'})
        if data:
            instruments = [inst['instId'] for inst in data if inst['state'] == 'live' and inst['instId'].endswith('-USDT-SWAP')]
            print(f"[{self.get_current_time_str()}] 获取到 {len(instruments)} 个活跃的USDT永续合约")
            return instruments
        print(f"[{self.get_current_time_str()}] 获取交易对失败。")
        return []

    def get_kline_data(self, inst_id, bar='1H', limit=100):
        data = self.fetch_with_retry(f"{self.base_url}/api/v5/market/candles", {'instId': inst_id, 'bar': bar, 'limit': limit})
        return data[::-1] if data else []

    def get_ticker_data(self, inst_id):
        data = self.fetch_with_retry(f"{self.base_url}/api/v5/market/ticker", {'instId': inst_id})
        if data and data[0]:
            ticker = data[0]
            last_price = float(ticker.get('last', 0))
            open_24h = float(ticker.get('open24h', 0))
            if open_24h > 0:
                return {'price_change_24h': ((last_price - open_24h) / open_24h) * 100}
        return {'price_change_24h': 0}

    def fetch_all_data_for_instrument(self, inst_id):
        # 并发获取一个币种的所有周期K线
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_h1 = executor.submit(self.get_kline_data, inst_id, '1H', 112)
            future_h4 = executor.submit(self.get_kline_data, inst_id, '4H', 105)
            future_d1 = executor.submit(self.get_kline_data, inst_id, '1D', 102)
            h1, h4, d1 = future_h1.result(), future_h4.result(), future_d1.result()
        if h1 and h4 and d1:
            return inst_id, {'h1': h1, 'h4': h4, 'd1': d1}
        return inst_id, None
        
    # --- 技术指标计算模块 (完全对齐 test11.html) ---

    def _parse_klines_to_df(self, klines):
        if not klines: return pd.DataFrame()
        df = pd.DataFrame(klines, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
        for col in df.columns:
            df[col] = pd.to_numeric(df[col])
        return df

    def calculate_macd(self, prices_series, fast=12, slow=26, signal=9):
        if len(prices_series) < slow: return pd.DataFrame()
        ema_fast = prices_series.ewm(span=fast, adjust=False).mean()
        ema_slow = prices_series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return pd.DataFrame({'macd': macd_line, 'signal': signal_line, 'histogram': histogram})

    def calculate_atr(self, df, period=14):
        if df.empty or len(df) < period + 1: return pd.Series(dtype=float)
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.ewm(span=period, adjust=False).mean()
    
    def calculate_bollinger_bands(self, df, period=20, std_dev=2):
        if len(df) < period: return {}
        sma = df['close'].rolling(window=period).mean().iloc[-1]
        std = df['close'].rolling(window=period).std().iloc[-1]
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        bandwidth = (upper - lower) / sma if sma > 0 else 0
        return {'upper': upper, 'lower': lower, 'bandwidth': bandwidth}

    def _get_change(self, df, period):
        if df is not None and len(df) > period:
            latest_close = df['close'].iloc[-1]
            past_close = df['close'].iloc[-1 - period]
            if past_close > 0:
                return ((latest_close - past_close) / past_close) * 100
        return None

    # --- 评分系统 ---
    
    def calculate_rs_score(self, inst_df, btc_df, eth_df):
        periods = [5, 10, 20]
        excess_performances = []
        for period in periods:
            inst_change = self._get_change(inst_df, period)
            btc_change = self._get_change(btc_df, period)
            eth_change = self._get_change(eth_df, period)
            if inst_change is None or btc_change is None or eth_change is None:
                return None
            benchmark_change = max(btc_change, eth_change)
            excess_performances.append(inst_change - benchmark_change)
        
        weighted_excess = (excess_performances[0] * 3 + excess_performances[1] * 2 + excess_performances[2] * 1) / 6
        raw_score = 50 + weighted_excess * 2.5
        return max(0, min(100, round(raw_score)))

    def calculate_market_leadership_score(self, inst_df, btc_df, eth_df):
        if len(inst_df) < 60: return None
        weights = {'rs': 40, 'resilience': 30, 'capital_flow': 20, 'trend_quality': 10}

        # 1. RS Score
        rs_score = self.calculate_rs_score(inst_df, btc_df, eth_df) or 0
        rs_component = rs_score * (weights['rs'] / 100)

        # 2. Resilience
        inst_returns = inst_df['close'].pct_change().iloc[1:]
        btc_returns = btc_df['close'].pct_change().iloc[1:]
        down_day_returns = inst_returns[btc_returns < -0.02]
        resilience_score = 50
        if not down_day_returns.empty:
            avg_inst_return_on_down_days = down_day_returns.mean()
            resilience_score = 50 + (avg_inst_return_on_down_days * 1000)
        resilience_component = max(0, min(100, resilience_score)) * (weights['resilience'] / 100)

        # 3. Capital Flow
        obv = (np.sign(inst_df['close'].diff()) * inst_df['vol']).fillna(0).cumsum()
        obv_sma20 = obv.rolling(20).mean().iloc[-1]
        obv_sma50 = obv.rolling(50).mean().iloc[-1]
        obv_score = 100 if obv_sma20 > obv_sma50 else 0
        
        vol_sma20 = inst_df['vol'].rolling(20).mean().iloc[-1]
        vol_sma50 = inst_df['vol'].rolling(50).mean().iloc[-1]
        vol_score = 100 if vol_sma20 > vol_sma50 else 0
        capital_flow_component = ((obv_score + vol_score) / 2) * (weights['capital_flow'] / 100)
        
        # 4. Trend Quality
        daily_returns = inst_df['close'].pct_change().dropna()
        std_dev = daily_returns.std()
        volatility_score = max(0, 100 - (std_dev * 2000))
        
        peak = inst_df['close'].expanding(min_periods=1).max()
        drawdown = (inst_df['close'] - peak) / peak
        max_drawdown = drawdown.min()
        drawdown_score = max(0, 100 - (abs(max_drawdown) * 200))
        trend_quality_component = ((volatility_score + drawdown_score) / 2) * (weights['trend_quality'] / 100)
        
        total_score = rs_component + resilience_component + capital_flow_component + trend_quality_component
        return max(0, min(100, round(total_score)))

    def calculate_startup_quality_score(self, opp, metrics):
        score = 0
        weights = {'volume': 30, 'momentum': 25, 'relativeStrength': 15, 'maDistance': 15, 'confirmation': 10, 'volatility': 5}
        
        # Volume, Momentum, RS, MA, Confirmation, Volatility
        vol_ratio = metrics['volume'] / metrics['avg_volume'] if metrics['avg_volume'] > 0 else 1
        vol_score = max(0, min(100, (vol_ratio - 1) * 50))
        
        growth_rate = (metrics['d1_hist'] - metrics['d1_prev_hist']) / abs(metrics['d1_prev_hist']) if metrics['d1_prev_hist'] != 0 else 0
        momentum_score = max(0, min(100, abs(growth_rate) * 100))
        
        rs_score_val = opp.get('rs_score', 50) # Use existing RS score
        
        ma_dist = (metrics['price'] - metrics['ema60']) / metrics['price'] if metrics['ema60'] else 0
        ma_score = max(0, (1 - abs(ma_dist) * 5) * 100)
        
        conf_score = min(100, (abs(metrics['h4_hist']) / metrics['price']) * 20000)
        
        volatility_score = max(0, (1 - metrics['bandwidth']) * 200)

        score += vol_score * weights['volume']
        score += momentum_score * weights['momentum']
        score += rs_score_val * weights['relativeStrength']
        score += ma_score * weights['maDistance']
        score += conf_score * weights['confirmation']
        score += volatility_score * weights['volatility']

        return round(score / 100)

    def calculate_continuation_quality_score(self, opp, d1_df, h4_df, d1_macd, h4_macd):
        weights = {'rs': 40, 'trendHealth': 25, 'volume': 25, 'volatility': 10}
        
        # 1. RS Score
        rs_component = (opp.get('rs_score', 0)) * (weights['rs'] / 100)
        
        # 2. Trend Health
        price = d1_df['close'].iloc[-1]
        macd_pos_score = min(100, (abs(d1_macd['signal'].iloc[-1]) / price) * 3000)
        duration_score = min(100, (opp.get('trend_duration_days', 0) / 30) * 100)
        magnitude_score = min(100, (abs(opp.get('trend_change_pct', 0)) / 100) * 100)
        trend_health_component = ((macd_pos_score + duration_score + magnitude_score) / 3) * (weights['trendHealth'] / 100)
        
        # 3. Volume Contraction
        volume_component = 0
        last_h4_cross = self.find_last_cross_info(h4_macd)
        if last_h4_cross and last_h4_cross['index'] > 0:
            idx = last_h4_cross['index']
            pullback_candles = h4_df.iloc[idx:]
            if len(pullback_candles) > 0 and idx >= len(pullback_candles):
                impulse_candles = h4_df.iloc[idx - len(pullback_candles):idx]
                pullback_avg_vol = pullback_candles['vol'].mean()
                impulse_avg_vol = impulse_candles['vol'].mean()
                if impulse_avg_vol > 0:
                    vol_contraction_score = max(0, (1 - (pullback_avg_vol / impulse_avg_vol))) * 100
                    volume_component = vol_contraction_score * (weights['volume'] / 100)

        # 4. Volatility Squeeze
        h4_bands = self.calculate_bollinger_bands(h4_df)
        volatility_score = max(0, (1 - h4_bands.get('bandwidth', 1)) * 150)
        volatility_component = min(100, volatility_score) * (weights['volatility'] / 100)

        total_score = rs_component + trend_health_component + volume_component + volatility_component
        return round(total_score)


    # --- 信号判断辅助函数 ---
    
    def find_last_cross_info(self, macd_df):
        if len(macd_df) < 2: return None
        macds = macd_df.to_dict('records')
        last_cross_type = 'golden' if macds[-1]['macd'] > macds[-1]['signal'] else 'death'
        for i in range(len(macds) - 2, -1, -1):
            current_cross_type = 'golden' if macds[i]['macd'] > macds[i]['signal'] else 'death'
            if current_cross_type != last_cross_type:
                return {'type': last_cross_type, 'index': i + 1}
        return {'type': last_cross_type, 'index': 0}

    def find_last_dea_zero_cross_info(self, macd_df, lookback=100):
        if len(macd_df) < 2: return None
        macds = macd_df.to_dict('records')
        start = max(0, len(macds) - 1 - lookback)
        for i in range(len(macds) - 2, start -1, -1):
            if macds[i]['signal'] <= 0 and macds[i+1]['signal'] > 0:
                return {'type': 'bullish', 'index': i + 1}
            if macds[i]['signal'] >= 0 and macds[i+1]['signal'] < 0:
                return {'type': 'bearish', 'index': i + 1}
        return None

    def get_signal_freshness_info(self, df, macd_df, cross_type, atr_s):
        last_cross = self.find_last_cross_info(macd_df)
        if not last_cross or last_cross['type'] != cross_type:
            return {'is_fresh': False, 'reason': f"未找到{cross_type}"}
        
        idx = last_cross['index']
        candles_ago = len(df) - 1 - idx
        if candles_ago > self.MAX_CANDLES_AGO:
            return {'is_fresh': False, 'reason': f"信号过久({candles_ago} > {self.MAX_CANDLES_AGO})"}
        
        if idx >= len(atr_s) or atr_s.empty:
            return {'is_fresh': False, 'reason': "ATR不足"}

        signal_price = df['close'].iloc[idx]
        current_price = df['close'].iloc[-1]
        atr_at_signal = atr_s.iloc[idx]
        
        if atr_at_signal > 0 and abs(current_price - signal_price) > (self.ATR_MULTIPLIER * atr_at_signal):
            return {'is_fresh': False, 'reason': f"价格波动过大(>{self.ATR_MULTIPLIER}倍ATR)"}
            
        return {'is_fresh': True, 'reason': '新鲜'}

    def check_freshness_since_zero_cross(self, df, macd_df, cross_type, atr_s):
        zero_cross = self.find_last_dea_zero_cross_info(macd_df)
        if not zero_cross or zero_cross['type'] != cross_type:
            return {'is_fresh': False, 'reason': '未找到0轴穿越'}
            
        idx = zero_cross['index']
        if idx >= len(atr_s) or atr_s.empty:
            return {'is_fresh': False, 'reason': "ATR不足"}
            
        cross_price = df['close'].iloc[idx]
        current_price = df['close'].iloc[-1]
        atr_at_cross = atr_s.iloc[idx]

        if atr_at_cross > 0 and abs(current_price - cross_price) > (self.ATR_MULTIPLIER * atr_at_cross):
            return {'is_fresh': False, 'reason': f"穿越0轴后价格波动过大(>{self.ATR_MULTIPLIER}倍ATR)"}
            
        return {'is_fresh': True, 'reason': '穿越0轴且新鲜'}

    # --- 核心分析函数 ---
    
    def analyze_instrument(self, inst_id, all_instruments_data):
        try:
            instrument_data = all_instruments_data.get(inst_id)
            btc_data = all_instruments_data.get('BTC-USDT-SWAP')
            eth_data = all_instruments_data.get('ETH-USDT-SWAP')

            if not instrument_data or not btc_data or not eth_data: return None

            h1_df = self._parse_klines_to_df(instrument_data['h1'])
            h4_df = self._parse_klines_to_df(instrument_data['h4'])
            d1_df = self._parse_klines_to_df(instrument_data['d1'])
            btc_d1_df = self._parse_klines_to_df(btc_data['d1'])
            eth_d1_df = self._parse_klines_to_df(eth_data['d1'])

            if len(h1_df) < 24 or len(d1_df) < 60: return None
            
            daily_volume = h1_df['volCcyQuote'].iloc[-24:].sum()
            if daily_volume < self.MACD_VOLUME_THRESHOLD: return None
            
            # --- 计算所有指标 ---
            d1_macd = self.calculate_macd(d1_df['close'])
            h4_macd = self.calculate_macd(h4_df['close'])
            h1_macd = self.calculate_macd(h1_df['close'])
            d1_atr = self.calculate_atr(d1_df)
            h4_atr = self.calculate_atr(h4_df)

            if d1_macd.empty or h4_macd.empty or h1_macd.empty or d1_atr.empty or h4_atr.empty: return None

            # --- 计算评分和基础信息 ---
            rs_score = self.calculate_rs_score(d1_df, btc_d1_df, eth_d1_df)
            leader_score = self.calculate_market_leadership_score(d1_df, btc_d1_df, eth_d1_df)
            
            result_base = {'inst_id': inst_id, 'rs_score': rs_score, 'leader_score': leader_score, 'volume': daily_volume}
            
            dea_cross_info = self.find_last_dea_zero_cross_info(d1_macd)
            if dea_cross_info:
                idx = dea_cross_info['index']
                result_base['trend_change_pct'] = ((d1_df['close'].iloc[-1] - d1_df['close'].iloc[idx]) / d1_df['close'].iloc[idx]) * 100
                result_base['trend_duration_days'] = (time.time() - d1_df['ts'].iloc[idx]/1000) / (3600*24)
            
            # --- 信号判断逻辑 ---
            d1_last, d1_prev = d1_macd.iloc[-1], d1_macd.iloc[-2]
            h4_last, h4_prev = h4_macd.iloc[-1], h4_macd.iloc[-2]
            h1_last, h1_prev = h1_macd.iloc[-1], h1_macd.iloc[-2]

            # 1. 启动信号检查 (Trend)
            form_A_long = self.find_last_dea_zero_cross_info(d1_macd, self.MAX_CANDLES_AGO) == 'bullish'
            form_B_long_info = self.check_freshness_since_zero_cross(d1_df, d1_macd, 'bullish', d1_atr)
            momentum_C_long = d1_last['macd'] > d1_last['signal'] and d1_last['histogram'] > d1_prev['histogram']
            if (form_A_long or form_B_long_info['is_fresh']) and momentum_C_long:
                if h4_last['macd'] > h4_last['signal'] and h4_last['histogram'] > h4_prev['histogram']:
                    metrics = { 'volume': daily_volume, 'avg_volume': d1_df['volCcyQuote'].iloc[-20:].mean(), 'd1_hist': d1_last['histogram'], 'd1_prev_hist': d1_prev['histogram'], 'h4_hist': h4_last['histogram'], 'price': d1_df['close'].iloc[-1], 'ema60': d1_df['close'].ewm(span=60, adjust=False).mean().iloc[-1], 'bandwidth': self.calculate_bollinger_bands(d1_df).get('bandwidth', 1) }
                    score = self.calculate_startup_quality_score(result_base, metrics)
                    return {**result_base, 'type': 'Long Trend', 'quality_score': score}
                else:
                    return {**result_base, 'type': 'Long Watchlist', 'quality_score': None}

            form_A_short = self.find_last_dea_zero_cross_info(d1_macd, self.MAX_CANDLES_AGO) == 'bearish'
            form_B_short_info = self.check_freshness_since_zero_cross(d1_df, d1_macd, 'bearish', d1_atr)
            momentum_C_short = d1_last['macd'] < d1_last['signal'] and d1_last['histogram'] < d1_prev['histogram']
            if (form_A_short or form_B_short_info['is_fresh']) and momentum_C_short:
                if h4_last['macd'] < h4_last['signal'] and h4_last['histogram'] < h4_prev['histogram']:
                    metrics = { 'volume': daily_volume, 'avg_volume': d1_df['volCcyQuote'].iloc[-20:].mean(), 'd1_hist': d1_last['histogram'], 'd1_prev_hist': d1_prev['histogram'], 'h4_hist': h4_last['histogram'], 'price': d1_df['close'].iloc[-1], 'ema60': d1_df['close'].ewm(span=60, adjust=False).mean().iloc[-1], 'bandwidth': self.calculate_bollinger_bands(d1_df).get('bandwidth', 1) }
                    score = self.calculate_startup_quality_score(result_base, metrics)
                    return {**result_base, 'type': 'Short Trend', 'quality_score': score}
                else:
                    return {**result_base, 'type': 'Short Watchlist', 'quality_score': None}
            
            # 2. 顺势信号检查 (Continuation, Pullback, Phoenix)
            is_stable_bull = d1_last['macd'] > 0 and d1_last['signal'] > 0 and (d1_macd['signal'].iloc[-5:] > 0).all()
            if is_stable_bull:
                base_opp = None
                h4_fresh_info = self.get_signal_freshness_info(h4_df, h4_macd, 'golden', h4_atr)
                is_h1_golden_cross = h1_last['macd'] > h1_last['signal'] and h1_prev['macd'] <= h1_prev['signal']
                
                if h4_fresh_info['is_fresh'] and is_h1_golden_cross:
                    base_opp = {**result_base, 'type': 'Long Continuation', 'strategy_level': '4H延续'}
                
                if not base_opp and is_h1_golden_cross and h4_last['histogram'] > h4_prev['histogram'] and h1_last['histogram'] > h1_prev['histogram']:
                    base_opp = {**result_base, 'type': 'Long Pullback', 'strategy_level': '1H回调'}
                
                if base_opp:
                    base_opp['quality_score'] = self.calculate_continuation_quality_score(base_opp, d1_df, h4_df, d1_macd, h4_macd)
                    
                    # 凤凰信号检查
                    if dea_cross_info and dea_cross_info['type'] == 'bullish':
                        klines_since_cross = d1_df.iloc[dea_cross_info['index']:]
                        price_at_zero_cross = klines_since_cross['close'].iloc[0]
                        peak_price = klines_since_cross['high'].max()
                        initial_rally_range = peak_price - price_at_zero_cross
                        current_drawdown = peak_price - d1_df['close'].iloc[-1]
                        current_retracement_pct = (current_drawdown / initial_rally_range) * 100 if initial_rally_range > 0 else 0
                        
                        if current_retracement_pct > 70:
                            initial_rally_pct = ((peak_price - price_at_zero_cross) / price_at_zero_cross) * 100
                            bands_history = d1_df['close'].rolling(20).std() / d1_df['close'].rolling(20).mean()
                            low_vol_threshold = bands_history.quantile(0.3)
                            
                            if initial_rally_pct > 15 and self.calculate_bollinger_bands(d1_df).get('bandwidth', 1) < low_vol_threshold:
                                base_opp['type'] = 'Long Phoenix'

                    return base_opp

            is_stable_bear = d1_last['macd'] < 0 and d1_last['signal'] < 0 and (d1_macd['signal'].iloc[-5:] < 0).all()
            if is_stable_bear:
                base_opp = None
                h4_fresh_info = self.get_signal_freshness_info(h4_df, h4_macd, 'death', h4_atr)
                is_h1_death_cross = h1_last['macd'] < h1_last['signal'] and h1_prev['macd'] >= h1_prev['signal']

                if h4_fresh_info['is_fresh'] and is_h1_death_cross:
                    base_opp = {**result_base, 'type': 'Short Continuation', 'strategy_level': '4H延续'}
                
                if not base_opp and is_h1_death_cross and h4_last['histogram'] < h4_prev['histogram'] and h1_last['histogram'] < h1_prev['histogram']:
                    base_opp = {**result_base, 'type': 'Short Pullback', 'strategy_level': '1H回调'}
                
                if base_opp:
                    base_opp['quality_score'] = self.calculate_continuation_quality_score(base_opp, d1_df, h4_df, d1_macd, h4_macd)
                    return base_opp
                    
            return None

        except Exception as e:
            # print(f"[{self.get_current_time_str()}] 分析 {inst_id} 时发生严重错误: {e}")
            return None

    # --- 报告生成模块 ---
    def create_opportunity_report(self, opportunities, market_info, upgraded_signals):
        # 按领袖分降序排序
        opportunities.sort(key=lambda x: x.get('leader_score', 0) or 0, reverse=True)
        upgraded_signals.sort(key=lambda x: x.get('leader_score', 0) or 0, reverse=True)

        type_map = { 'Long Trend': '🚀 多头启动', 'Long Phoenix': '🔥 凤凰信号', 'Long Continuation': '➡️ 多头延续', 'Long Pullback': '🐂 多头回调', 'Long Watchlist': '👀 多头观察', 'Short Trend': '📉 空头启动', 'Short Continuation': '↘️ 空头延续', 'Short Pullback': '🐻 空头回调', 'Short Watchlist': '👀 空头观察' }
        
        content = f"### 市场情绪: {market_info.get('text', 'N/A')}\n<details><summary>点击查看情绪分析依据</summary>\n\n{market_info.get('details', '')}\n\n</details>\n\n"

        def generate_table(title, opp_list):
            if not opp_list: return ""
            table = f"### {title}\n"
            table += "| 领袖分 | 质量分 | RS分 | 交易对 | 机会类型 | 趋势幅度 | 趋势时长 | 24H成交额 | 24H涨跌幅 |\n"
            table += "|:---:|:---:|:---:|:---|:---|:---:|:---:|:---:|:---:|\n"
            for opp in opp_list:
                inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
                opp_type = type_map.get(opp['type'], '未知')
                vol_str = self.format_volume(opp['volume'])
                change_24h_str = f"📈 {opp.get('price_change_24h', 0):.2f}%" if opp.get('price_change_24h', 0) > 0 else f"📉 {opp.get('price_change_24h', 0):.2f}%"
                
                leader_score = f"**{opp.get('leader_score', 'N/A')}**" if opp.get('leader_score', 0) >= 80 else str(opp.get('leader_score', 'N/A'))
                quality_score = f"**{opp.get('quality_score', 'N/A')}**" if opp.get('quality_score', 0) >= 80 else str(opp.get('quality_score', 'N/A'))
                rs_score = f"**{opp.get('rs_score', 'N/A')}**" if opp.get('rs_score', 0) >= 80 else str(opp.get('rs_score', 'N/A'))

                trend_change = opp.get('trend_change_pct', 0)
                trend_change_str = f"📈 {trend_change:.1f}%" if trend_change > 0 else f"📉 {trend_change:.1f}%" if trend_change < 0 else "N/A"
                trend_days = opp.get('trend_duration_days', 0)
                trend_days_str = f"{trend_days:.1f}天" if trend_days > 0 else "N/A"
                
                warning = " (逆大盘)" if (market_info['sentiment'] == 'Bullish' and 'Short' in opp['type']) or \
                                      (market_info['sentiment'] == 'Bearish' and 'Long' in opp['type']) else ""

                table += f"| {leader_score} | {quality_score} | {rs_score} | **{inst_name}** | {opp_type}{warning} | {trend_change_str} | {trend_days_str} | {vol_str} | {change_24h_str} |\n"
            return table

        upgraded_ids = {s['inst_id'] for s in upgraded_signals}
        new_actionable = [opp for opp in opportunities if opp['inst_id'] not in upgraded_ids]

        if upgraded_signals:
            content += generate_table('✨ 信号升级 ✨ (按领袖分排序)', upgraded_signals)
        if new_actionable:
            content += generate_table('💎 新机会信号 (按领袖分排序)', new_actionable)
        
        # 策略说明
        content += "\n---\n**策略说明:** [点击查看详情](https://github.com/your-repo/your-project/wiki/Strategy-Details)" # 建议将长说明放在外部链接
        return content


    def format_volume(self, volume):
        if volume >= 1_000_000_000: return f"{volume/1_000_000_000:.2f}B"
        if volume >= 1_000_000: return f"{volume/1_000_000:.2f}M"
        if volume >= 1_000: return f"{volume/1_000:.2f}K"
        return f"{volume:.2f}"
    
    def get_market_sentiment(self, btc_data):
        klines = {
            '1D': self._parse_klines_to_df(btc_data['d1'])['close'],
            '4H': self._parse_klines_to_df(btc_data['h4'])['close'],
            '1H': self._parse_klines_to_df(btc_data['h1'])['close']
        }
        macds = {tf: self.calculate_macd(prices) for tf, prices in klines.items()}
        if any(m.empty for m in macds.values()): return {'sentiment': 'Neutral', 'text':"BTC数据不足"}

        score, bull_points, bear_points = 0, [], []
        weights = {'1D': 2, '4H': 1, '1H': 0.5}

        for tf in ['1D', '4H', '1H']:
            last, prev = macds[tf].iloc[-1], macds[tf].iloc[-2]
            tf_text = f"**{tf}**:"
            if last['macd'] > 0 and last['signal'] > 0: score += 1 * weights[tf]; bull_points.append(f"{tf_text} 双线位于0轴之上")
            if last['macd'] < 0 and last['signal'] < 0: score -= 1 * weights[tf]; bear_points.append(f"{tf_text} 双线位于0轴之下")
            if last['macd'] > last['signal']: score += 0.5 * weights[tf]
            else: score -= 0.5 * weights[tf]
            if last['histogram'] > prev['histogram']: bull_points.append(f"{tf_text} 动能增强")
            else: bear_points.append(f"{tf_text} 动能减弱")

        if score >= 3.5: sentiment, text = 'Bullish', "强势看涨 🐂"
        elif score >= 1.5: sentiment, text = 'Bullish', "震荡偏多 📈"
        elif score <= -3.5: sentiment, text = 'Bearish', "强势看空 🐻"
        elif score <= -1.5: sentiment, text = 'Bearish', "震荡偏空 📉"
        else: sentiment, text = 'Neutral', "多空胶着 횡보"
        
        details = f"🐂 **看多理由**:\n- " + "\n- ".join(bull_points) if bull_points else ""
        details += f"\n\n🐻 **看空理由**:\n- " + "\n- ".join(bear_points) if bear_points else ""
        return {'sentiment': sentiment, 'text': text, 'details': details}

    def load_watchlist_state(self):
        if not os.path.exists(self.state_file): return {}
        try:
            with open(self.state_file, 'r') as f: return json.load(f)
        except: return {}

    def save_watchlist_state(self, watchlist):
        try:
            with open(self.state_file, 'w') as f: json.dump(watchlist, f)
        except Exception as e: print(f"保存状态文件失败: {e}")

    # --- 主运行函数 ---
    def run(self):
        start_time = time.time()
        print(f"[{self.get_current_time_str()}] === 开始执行监控任务 ===")
        
        # 1. 加载状态和获取交易对
        previous_watchlist = self.load_watchlist_state()
        instruments = self.get_perpetual_instruments()
        if not instruments: return
        
        # 2. 并发获取所有交易对的所有周期K线数据
        print(f"[{self.get_current_time_str()}] 正在并发获取 {len(instruments)} 个币种的K线数据...")
        all_instruments_data = {}
        with ThreadPoolExecutor(max_workers=self.CONCURRENCY_LIMIT) as executor:
            futures = [executor.submit(self.fetch_all_data_for_instrument, inst) for inst in instruments]
            for i, future in enumerate(as_completed(futures)):
                inst_id, data = future.result()
                if data:
                    all_instruments_data[inst_id] = data
                print(f"\r数据获取进度: {i+1}/{len(instruments)}", end="")
        print(f"\n[{self.get_current_time_str()}] 数据获取完毕，耗时 {time.time() - start_time:.2f}秒. 有效数据: {len(all_instruments_data)}个币种。")

        btc_data = all_instruments_data.get('BTC-USDT-SWAP')
        if not btc_data:
            print("未能获取BTC数据，任务终止。")
            return
            
        # 3. 分析市场情绪
        market_info = self.get_market_sentiment(btc_data)
        print(f"[{self.get_current_time_str()}] 当前市场情绪: {market_info['text']}")
        
        # 4. 并发分析所有交易对
        print(f"[{self.get_current_time_str()}] 正在并发分析 {len(all_instruments_data)} 个币种的交易信号...")
        all_opportunities = []
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(self.analyze_instrument, inst_id, all_instruments_data) for inst_id in all_instruments_data]
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                if result:
                    all_opportunities.append(result)
                print(f"\r信号分析进度: {i+1}/{len(all_instruments_data)}", end="")
        print(f"\n[{self.get_current_time_str()}] 信号分析完毕，发现 {len(all_opportunities)} 个潜在信号。")

        # 5. 获取实时价格并处理机会
        if all_opportunities:
            print(f"[{self.get_current_time_str()}] 正在获取 {len(all_opportunities)} 个信号币种的24H涨跌幅...")
            with ThreadPoolExecutor(max_workers=self.CONCURRENCY_LIMIT) as executor:
                ticker_futures = {executor.submit(self.get_ticker_data, opp['inst_id']): opp for opp in all_opportunities}
                for future in as_completed(ticker_futures):
                    opp = ticker_futures[future]
                    ticker_data = future.result()
                    opp['price_change_24h'] = ticker_data.get('price_change_24h', 0)

            upgraded_signals, new_watchlist, actionable_opportunities = [], {}, []
            for opp in all_opportunities:
                inst_id, opp_type = opp['inst_id'], opp['type']
                if 'Watchlist' not in opp_type:
                    actionable_opportunities.append(opp)
                    if inst_id in previous_watchlist:
                        upgraded_signals.append(opp)
                        print(f"[{self.get_current_time_str()}] ✨ 信号升级: {inst_id} 从 {previous_watchlist[inst_id]} 升级为 {opp_type}")
                if 'Watchlist' in opp_type:
                    new_watchlist[inst_id] = opp_type
            
            self.save_watchlist_state(new_watchlist)
            
            if actionable_opportunities:
                title = ""
                new_actionable_count = len(actionable_opportunities) - len(upgraded_signals)
                if upgraded_signals:
                    title += f"✨ {len(upgraded_signals)}个升级"
                    if new_actionable_count > 0:
                        title += f" + {new_actionable_count}个新机会"
                else:
                    title = f"💎 发现 {len(actionable_opportunities)} 个新机会"
                
                content = self.create_opportunity_report(actionable_opportunities, market_info, upgraded_signals)
                self.send_notification(title, content)
            else:
                print(f"[{self.get_current_time_str()}] 仅发现 {len(all_opportunities)} 个观察信号，不发送通知。")
        else:
            print(f"[{self.get_current_time_str()}] 本次未发现任何符合条件的信号。")
            self.save_watchlist_state({}) # 清空观察列表
        
        end_time = time.time()
        print(f"[{self.get_current_time_str()}] === 监控任务执行完毕，总耗时: {end_time - start_time:.2f}秒 ===")

if __name__ == "__main__":
    monitor = OKXMonitor()
    monitor.run()
