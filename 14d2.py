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
from math import floor

class OKXMonitor:
    def __init__(self):
        # --- 核心配置 ---
        self.base_url = "https://www.okx.com"
        self.server_jiang_key = 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e'
        
        # --- 策略参数 ---
        self.MACD_VOLUME_THRESHOLD = 10_000_000
        self.ATR_MULTIPLIER = 2.0
        self.MAX_CANDLES_AGO = 5
        
        # --- 系统配置 ---
        self.session = self._create_session()
        self.timezone = pytz.timezone('Asia/Shanghai')
        self.state_file = 'watchlist_state.json'
        self.CONCURRENCY_LIMIT = 10
        self.STRICT_CONCURRENCY_LIMIT = 5
        self.request_timestamps = []
        self.RATE_LIMIT_COUNT = 18
        self.RATE_LIMIT_WINDOW = 2000
        self.debug_logs = [] # 用于存储调试日志

    # ... [所有未改动的辅助函数、数据获取、指标计算、评分系统函数都保持不变] ...
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
                    return None
                delay = 2**i + np.random.rand()
                time.sleep(delay)
        return None

    def send_notification(self, title, content):
        if not self.server_jiang_key:
            print(f"[{self.get_current_time_str()}] 未配置SERVER_JIANG_KEY，通知将打印到控制台。")
            print(f"标题: {title}\n内容:\n{content}")
            return
        desp = content
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
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_h1 = executor.submit(self.get_kline_data, inst_id, '1H', 112 + 12)
            future_h4 = executor.submit(self.get_kline_data, inst_id, '4H', 105 + 4)
            future_d1 = executor.submit(self.get_kline_data, inst_id, '1D', 102 + 2)
            h1, h4, d1 = future_h1.result(), future_h4.result(), future_d1.result()
        if h1 and h4 and d1:
            return inst_id, {'h1': h1, 'h4': h4, 'd1': d1}
        return inst_id, None

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
        rs_score = self.calculate_rs_score(inst_df, btc_df, eth_df) or 0
        rs_component = rs_score * (weights['rs'] / 100)
        inst_returns = inst_df['close'].pct_change().iloc[1:]
        btc_returns = btc_df['close'].pct_change().iloc[1:]
        down_day_returns = inst_returns[btc_returns < -0.02]
        resilience_score = 50
        if not down_day_returns.empty:
            avg_inst_return_on_down_days = down_day_returns.mean()
            resilience_score = 50 + (avg_inst_return_on_down_days * 1000)
        resilience_component = max(0, min(100, resilience_score)) * (weights['resilience'] / 100)
        obv = (np.sign(inst_df['close'].diff()) * inst_df['vol']).fillna(0).cumsum()
        obv_sma20 = obv.rolling(20).mean().iloc[-1]
        obv_sma50 = obv.rolling(50).mean().iloc[-1]
        obv_score = 100 if obv_sma20 > obv_sma50 else 0
        vol_sma20 = inst_df['vol'].rolling(20).mean().iloc[-1]
        vol_sma50 = inst_df['vol'].rolling(50).mean().iloc[-1]
        vol_score = 100 if vol_sma20 > vol_sma50 else 0
        capital_flow_component = ((obv_score + vol_score) / 2) * (weights['capital_flow'] / 100)
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
        vol_ratio = metrics['volume'] / metrics['avg_volume'] if metrics['avg_volume'] > 0 else 1
        vol_score = max(0, min(100, (vol_ratio - 1) * 50))
        growth_rate = (metrics['d1_hist'] - metrics['d1_prev_hist']) / abs(metrics['d1_prev_hist']) if metrics['d1_prev_hist'] != 0 else 0
        momentum_score = max(0, min(100, abs(growth_rate) * 100))
        rs_score_val = opp.get('rs_score', 50)
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
        rs_component = (opp.get('rs_score', 0)) * (weights['rs'] / 100)
        price = d1_df['close'].iloc[-1]
        macd_pos_score = min(100, (abs(d1_macd['signal'].iloc[-1]) / price) * 3000)
        duration_score = min(100, (opp.get('trend_duration_days', 0) / 30) * 100)
        magnitude_score = min(100, (abs(opp.get('trend_change_pct', 0)) / 100) * 100)
        trend_health_component = ((macd_pos_score + duration_score + magnitude_score) / 3) * (weights['trendHealth'] / 100)
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
        h4_bands = self.calculate_bollinger_bands(h4_df)
        volatility_score = max(0, (1 - h4_bands.get('bandwidth', 1)) * 150)
        volatility_component = min(100, volatility_score) * (weights['volatility'] / 100)
        total_score = rs_component + trend_health_component + volume_component + volatility_component
        return round(total_score)
    
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

    # --- NEW: Helper for debug logging ---
    def _log_debug(self, debug_info, category, name, cond, val_obj, res):
        if category not in debug_info['checks']:
            debug_info['checks'][category] = {'final_result': False, 'steps': []}
        
        val_str = ""
        if isinstance(val_obj, dict) and 'reason' in val_obj:
            val_str = f"{val_obj['is_fresh']} ({val_obj['reason']})"
        elif isinstance(val_obj, bool):
            val_str = str(val_obj)
        elif isinstance(val_obj, (int, float)):
            val_str = f"{val_obj:.4g}"
        else:
            val_str = str(val_obj)
            
        res_icon = '✅' if res else '❌'
        
        debug_info['checks'][category]['steps'].append({
            'name': name, 'cond': cond, 'val': val_str, 'res': res_icon
        })
        
    def analyze_instrument(self, inst_id, snapshot_data, is_historical=False):
        debug_info = {'inst_id': inst_id, 'leader_score': None, 'rs_score': None, 'checks': {}}
        try:
            h1_df = self._parse_klines_to_df(snapshot_data['h1'])
            h4_df = self._parse_klines_to_df(snapshot_data['h4'])
            d1_df = self._parse_klines_to_df(snapshot_data['d1'])
            btc_d1_df = self._parse_klines_to_df(snapshot_data['btc']['d1'])
            eth_d1_df = self._parse_klines_to_df(snapshot_data['eth']['d1'])

            if len(h1_df) < 24 or len(d1_df) < 60: return None
            
            daily_volume = h1_df['volCcyQuote'].iloc[-24:].sum()
            vol_check = daily_volume >= self.MACD_VOLUME_THRESHOLD
            self._log_debug(debug_info, '基础过滤', '24H成交额', f"> {self.MACD_VOLUME_THRESHOLD/1e6:.0f}M", f"{daily_volume/1e6:.2f}M", vol_check)
            if not vol_check:
                if not is_historical: self.debug_logs.append(debug_info)
                return None
            
            d1_macd = self.calculate_macd(d1_df['close'])
            h4_macd = self.calculate_macd(h4_df['close'])
            h1_macd = self.calculate_macd(h1_df['close'])
            d1_atr = self.calculate_atr(d1_df)
            h4_atr = self.calculate_atr(h4_df)
            if d1_macd.empty or h4_macd.empty or h1_macd.empty: return None
            
            rs_score = self.calculate_rs_score(d1_df, btc_d1_df, eth_d1_df)
            leader_score = self.calculate_market_leadership_score(d1_df, btc_d1_df, eth_d1_df)
            debug_info.update({'leader_score': leader_score, 'rs_score': rs_score})

            result_base = {'inst_id': inst_id, 'rs_score': rs_score, 'leader_score': leader_score, 'volume': daily_volume}
            
            if is_historical:
                result_base['signalTime'] = h1_df['ts'].iloc[-1]
                result_base['signalPrice'] = h1_df['close'].iloc[-1]

            dea_cross_info = self.find_last_dea_zero_cross_info(d1_macd)
            if dea_cross_info and dea_cross_info['index'] < len(d1_df):
                idx = dea_cross_info['index']
                current_ts = result_base.get('signalTime', time.time() * 1000)
                result_base['trend_change_pct'] = ((d1_df['close'].iloc[-1] - d1_df['close'].iloc[idx]) / d1_df['close'].iloc[idx]) * 100
                result_base['trend_duration_days'] = (current_ts / 1000 - d1_df['ts'].iloc[idx]/1000) / (3600*24)
            
            d1_last, d1_prev = d1_macd.iloc[-1], d1_macd.iloc[-2]
            h4_last, h4_prev = h4_macd.iloc[-1], h4_macd.iloc[-2]
            h1_last, h1_prev = h1_macd.iloc[-1], h1_macd.iloc[-2]
            
            # --- Start Logging Checks ---
            
            # 1. Trend Signals
            form_A_long_info = self.find_last_dea_zero_cross_info(d1_macd, self.MAX_CANDLES_AGO)
            form_A_long = form_A_long_info and form_A_long_info['type'] == 'bullish'
            self._log_debug(debug_info, '多头启动', '形态A(近期穿0轴)', 'true', form_A_long, form_A_long)
            form_B_long_info = self.check_freshness_since_zero_cross(d1_df, d1_macd, 'bullish', d1_atr)
            self._log_debug(debug_info, '多头启动', '形态B(穿轴后新鲜)', 'true', form_B_long_info, form_B_long_info['is_fresh'])
            momentum_C_long = d1_last['macd'] > d1_last['signal'] and d1_last['histogram'] > d1_prev['histogram']
            self._log_debug(debug_info, '多头启动', '动能C(金叉+增强)', 'true', momentum_C_long, momentum_C_long)
            
            if (form_A_long or form_B_long_info['is_fresh']) and momentum_C_long:
                h4_ok_long = h4_last['macd'] > h4_last['signal'] and h4_last['histogram'] > h4_prev['histogram']
                self._log_debug(debug_info, '多头启动', '4H 确认', '金叉+增强', h4_ok_long, h4_ok_long)
                debug_info['checks']['多头启动']['final_result'] = 'Long Trend' if h4_ok_long else False
                if not is_historical: self.debug_logs.append(debug_info)
                
                if h4_ok_long:
                    metrics = { 'volume': daily_volume, 'avg_volume': d1_df['volCcyQuote'].iloc[-20:].mean(), 'd1_hist': d1_last['histogram'], 'd1_prev_hist': d1_prev['histogram'], 'h4_hist': h4_last['histogram'], 'price': d1_df['close'].iloc[-1], 'ema60': d1_df['close'].ewm(span=60, adjust=False).mean().iloc[-1], 'bandwidth': self.calculate_bollinger_bands(d1_df).get('bandwidth', 1) }
                    score = self.calculate_startup_quality_score(result_base, metrics)
                    return {**result_base, 'type': 'Long Trend', 'quality_score': score}
                else:
                    return {**result_base, 'type': 'Long Watchlist', 'quality_score': None}

            # ... (Similar logging for Short Trend)
            
            # 2. Continuation Signals
            is_stable_bull = d1_last['macd'] > 0 and d1_last['signal'] > 0 and (d1_macd['signal'].iloc[-5:] > 0).all()
            self._log_debug(debug_info, '顺势多头', 'D1 强趋势', '0轴上稳定', is_stable_bull, is_stable_bull)
            if is_stable_bull:
                base_opp = None
                h4_fresh_info = self.get_signal_freshness_info(h4_df, h4_macd, 'golden', h4_atr)
                self._log_debug(debug_info, '顺势多头', '4H 新鲜金叉', 'true', h4_fresh_info, h4_fresh_info['is_fresh'])
                is_h1_golden_cross = h1_last['macd'] > h1_last['signal'] and h1_prev['macd'] <= h1_prev['signal']
                self._log_debug(debug_info, '顺势多头', '1H 刚刚金叉', 'true', is_h1_golden_cross, is_h1_golden_cross)
                
                if h4_fresh_info['is_fresh'] and is_h1_golden_cross:
                    base_opp = {**result_base, 'type': 'Long Continuation'}
                
                is_h4_hist_rising = h4_last['histogram'] > h4_prev['histogram']
                self._log_debug(debug_info, '顺势多头', '4H 回调安全', '动能衰竭/反转', is_h4_hist_rising, is_h4_hist_rising)
                is_h1_hist_rising = h1_last['histogram'] > h1_prev['histogram']
                self._log_debug(debug_info, '顺势多头', '1H 回调动能', '增强', is_h1_hist_rising, is_h1_hist_rising)

                if not base_opp and is_h1_golden_cross and is_h4_hist_rising and is_h1_hist_rising:
                    base_opp = {**result_base, 'type': 'Long Pullback'}
                
                if base_opp:
                    debug_info['checks']['顺势多头']['final_result'] = base_opp['type']
                    base_opp['quality_score'] = self.calculate_continuation_quality_score(base_opp, d1_df, h4_df, d1_macd, h4_macd)
                    
                    if dea_cross_info and dea_cross_info['type'] == 'bullish' and dea_cross_info['index'] < len(d1_df):
                        # ... (Phoenix signal logic with logging)
                        klines_since_cross = d1_df.iloc[dea_cross_info['index']:]
                        price_at_zero_cross = klines_since_cross['close'].iloc[0]
                        peak_price = klines_since_cross['high'].max()
                        initial_rally_range = peak_price - price_at_zero_cross
                        current_drawdown = peak_price - d1_df['close'].iloc[-1]
                        current_retracement_pct = (current_drawdown / initial_rally_range) * 100 if initial_rally_range > 0 else 0
                        is_deep_retracement = current_retracement_pct > 70
                        self._log_debug(debug_info, '凤凰信号', '核心前提: 深度回撤', '> 70%', f"{current_retracement_pct:.1f}%", is_deep_retracement)
                        if is_deep_retracement:
                            initial_rally_pct = ((peak_price - price_at_zero_cross) / price_at_zero_cross) * 100
                            has_strength = initial_rally_pct > 15
                            self._log_debug(debug_info, '凤凰信号', '健康检查1: 趋势强度', '> 15%', f"{initial_rally_pct:.1f}%", has_strength)
                            
                            bands_history = d1_df['close'].rolling(20).std() / d1_df['close'].rolling(20).mean()
                            low_vol_threshold = bands_history.quantile(0.3)
                            current_bw = self.calculate_bollinger_bands(d1_df).get('bandwidth', 1)
                            is_vol_low = current_bw < low_vol_threshold
                            self._log_debug(debug_info, '凤凰信号', '健康检查2: 波动收缩', f"< {low_vol_threshold:.4f}", f"{current_bw:.4f}", is_vol_low)

                            if has_strength and is_vol_low:
                                base_opp['type'] = 'Long Phoenix'
                                debug_info['checks']['凤凰信号']['final_result'] = base_opp['type']
                    
                    if not is_historical: self.debug_logs.append(debug_info)
                    return base_opp

            # ... (Similar logic and logging for Short Continuation)
            
            if not is_historical: self.debug_logs.append(debug_info)
            return None
        except Exception as e:
            # print(f"Error in analyze_instrument for {inst_id}: {e}")
            if not is_historical: self.debug_logs.append(debug_info)
            return None

    def get_strategy_explanation(self):
        return """
---
### **策略说明**

#### **评分体系**
- **🏆 市场领袖分 (Leader Score)**: 综合评估币种的“龙头”特质。分数越高，龙头相越强。
  - **构成**: 相对强度(40%), 回调抗性(30%), 资金流(20%), 趋势质量(10%).
- **🚀 启动信号质量分**: 衡量趋势**起点**的强度和可靠性。
  - **构成**: 成交量(30%), 动能(25%), 相对强度(15%), 均线距离(15%), 确认度(10%), 波动性(5%).
- **➡️ 延续/回调质量分**: 衡量在**已确立趋势中**介入点的“性价比”。
  - **构成**: 相对强度(40%), 趋势健康度(25%), 回调缩量(25%), 波动收缩(10%).

#### **通用规则 (所有信号前提)**
- **成交量**: 信号出现时，该币种的24小时成交额必须 **> 1000万 USDT**。

---
### **信号类型与核心条件**

#### **🚀 多头启动 (Long Trend)**
*   **目标**: 捕捉新一轮上涨趋势的**起点**。
*   **日线 (1D) - 趋势确立 (满足A或B):**
    *   **A) 形态**: MACD信号线(慢线)在**5根K线内**从下方上穿0轴。
    *   **B) 新鲜度**: 价格距上次“穿轴点”的涨幅 < **2倍ATR** (日线)。
*   **日线 (1D) - 动能确认 (必须满足):**
    *   处于**金叉**状态 且 **MACD动能柱增强** (Histogram变长)。
*   **4小时 (4H) - 周期共振 (必须满足):**
    *   同样处于**金叉**状态 且 **MACD动能柱增强**。

#### **➡️ 多头延续 (Long Continuation)**
*   **目标**: 在已确立的强势上涨趋势中，捕捉**中期回调结束后的确认点**。
*   **日线 (1D) - 强趋势背景:**
    *   MACD双线(快慢线)稳定运行在**0轴之上** (最近5根K线信号线未跌破0轴)。
*   **4小时 (4H) - 中期结构:**
    *   形成**“新鲜的”金叉** (5根4H K线内形成，且价格波动 < 2倍ATR)。
*   **1小时 (1H) - 入场触发:**
    *   MACD**刚刚形成金叉** (当前K线是金叉，上一根还不是)。

#### **🐂 多头回调 (Long Pullback)**
*   **目标**: 在强趋势中，捕捉**短期回调结束后的最早入场点**。
*   **日线 (1D) - 强趋势背景:**
    *   同“多头延续”，MACD双线稳定在**0轴之上**。
*   **4小时 (4H) - 回调结束迹象:**
    *   MACD空头动能柱**正在衰竭** (Histogram收缩或翻红)。
*   **1小时 (1H) - 入场触发:**
    *   MACD**刚刚形成金叉** 且 **多头动能柱正在增强**。

#### **🔥 凤凰信号 (Long Phoenix)**
*   **目标**: 在已确立的多头趋势中，捕捉一次**“深度回调”结束后的黄金坑**机会。
*   **基础前提**:
    *   首先必须满足“**多头延续**”或“**多头回调**”的所有条件。
*   **核心条件 - 深度回撤 (必须满足):**
    *   从本轮日线趋势启动后的最高点算起，当前价格的**回撤幅度 > 70%**。
*   **健康检查 (必须同时满足):**
    *   **1) 趋势有效性**: 前期上涨必须有力，最高点涨幅 > **15%**。
    *   **2) 波动收缩**: 回调末端，日线布林带带宽处于**历史低位** (30%分位数以下)。

---
*注：所有空头信号 (📉启动, ↘️延续, 🐻回调) 的条件与对应的多头信号完全相反。*
"""
    
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
        if any(m.empty for m in macds.values()): return {'sentiment': 'Neutral', 'text':"BTC数据不足", 'details': ''}

        score, bull_points, bear_points = 0, [], []
        weights = {'1D': 2, '4H': 1, '1H': 0.5}

        for tf in ['1D', '4H', '1H']:
            last, prev = macds[tf].iloc[-1], macds[tf].iloc[-2]
            tf_text = f"**{tf}**:"
            if last['macd'] > 0 and last['signal'] > 0: score += 1 * weights[tf]; bull_points.append(f"{tf_text} 双线位于0轴之上")
            if last['macd'] < 0 and last['signal'] < 0: score -= 1 * weights[tf]; bear_points.append(f"{tf_text} 双线位于0轴之下")
            
            if last['macd'] > last['signal']:
                score += 0.5 * weights[tf]
                point = f"{tf_text} 金叉"
                if last['histogram'] > prev['histogram']: point += "且多头动能增强"
                else: point += "但多头动能减弱"
                bull_points.append(point)
            else:
                score -= 0.5 * weights[tf]
                point = f"{tf_text} 死叉"
                if last['histogram'] < prev['histogram']: point += "且空头动能增强"
                else: point += "但空头动能减弱"
                bear_points.append(point)

        if score >= 3.5: sentiment, text = 'Bullish', "强势看涨 🐂"
        elif score >= 1.5: sentiment, text = 'Bullish', "震荡偏多 📈"
        elif score <= -3.5: sentiment, text = 'Bearish', "强势看空 🐻"
        elif score <= -1.5: sentiment, text = 'Bearish', "震荡偏空 📉"
        else: sentiment, text = 'Neutral', "多空胶着 횡보"
        
        details = ""
        if bull_points:
            details += "#### 🐂 看多理由\n- " + "\n- ".join(bull_points)
        if bear_points:
            details += "\n\n#### 🐻 看空理由\n- " + "\n- ".join(bear_points)

        return {'sentiment': sentiment, 'text': text, 'details': details.strip()}

    def load_watchlist_state(self):
        if not os.path.exists(self.state_file): return {}
        try:
            with open(self.state_file, 'r') as f: return json.load(f)
        except: return {}

    def save_watchlist_state(self, watchlist):
        try:
            with open(self.state_file, 'w') as f: json.dump(watchlist, f)
        except Exception as e: print(f"保存状态文件失败: {e}")

    # --- Backtesting and Reporting ---
    def analyze_signal_performance(self, signal):
        now = time.time() * 1000
        time_since_signal_ms = now - signal['signalTime']
        minutes_since_signal = time_since_signal_ms / (1000 * 60)
        
        candles_to_fetch = min(300, int(minutes_since_signal / 5) + 2)
        if candles_to_fetch <= 1:
            return {'maxMovePct': 0, 'timeToPeak': '0m'}

        klines_5m_raw = self.get_kline_data(signal['inst_id'], '5m', candles_to_fetch)
        if not klines_5m_raw:
            return {'maxMovePct': None, 'timeToPeak': 'N/A'}

        klines_5m_df = self._parse_klines_to_df(klines_5m_raw)
        relevant_klines = klines_5m_df[klines_5m_df['ts'] > signal['signalTime']]

        if relevant_klines.empty:
            return {'maxMovePct': 0, 'timeToPeak': '0m'}

        peak_price, peak_time = 0, 0
        if 'Long' in signal['type']:
            if relevant_klines['high'].empty: return {'maxMovePct': 0, 'timeToPeak': '0m'}
            peak_price = relevant_klines['high'].max()
            peak_candle = relevant_klines.loc[relevant_klines['high'].idxmax()]
            peak_time = peak_candle['ts']
        else: # Short
            if relevant_klines['low'].empty: return {'maxMovePct': 0, 'timeToPeak': '0m'}
            peak_price = relevant_klines['low'].min()
            peak_candle = relevant_klines.loc[relevant_klines['low'].idxmin()]
            peak_time = peak_candle['ts']
            
        max_move_pct = ((peak_price - signal['signalPrice']) / signal['signalPrice']) * 100
        time_to_peak_mins = round((peak_time - signal['signalTime']) / (1000 * 60))

        return {'maxMovePct': max_move_pct, 'timeToPeak': f"{time_to_peak_mins}m"}

    def create_backtest_report(self, historical_signals):
        if not historical_signals:
            return "### 📊 过去12小时信号回测 📊\n\n在过去12小时内未发现符合策略的交易信号。\n"

        report = "### 📊 过去12小时信号回测 📊\n"
        type_map = { 'Long Trend': '🚀 多头启动', 'Long Phoenix': '🔥 凤凰信号', 'Long Continuation': '➡️ 多头延续', 'Long Pullback': '🐂 多头回调', 'Short Trend': '📉 空头启动', 'Short Continuation': '↘️ 空头延续', 'Short Pullback': '🐻 空头回调' }
        grouped_signals = {}
        for sig in historical_signals:
            inst_name = sig['inst_id'].replace('-USDT-SWAP', '')
            if inst_name not in grouped_signals:
                grouped_signals[inst_name] = []
            grouped_signals[inst_name].append(sig)

        sorted_groups = sorted(grouped_signals.items(), key=lambda item: max(s['signalTime'] for s in item[1]), reverse=True)

        for inst_name, signals in sorted_groups:
            report += f"\n#### **{inst_name}** ({len(signals)}条信号)\n"
            report += "| 有效性 | 领袖分 | 质量分 | 信号时间 | 类型 | RS分 | 信号价 | 最大涨/跌幅 | 达峰耗时 |\n"
            report += "|:---:|:---:|:---:|:---|:---|:---:|:---:|:---:|:---:|\n"
            
            signals.sort(key=lambda x: x['signalTime'], reverse=True)
            
            for sig in signals:
                perf = sig.get('performance', {})
                max_move = perf.get('maxMovePct')
                is_effective = "N/A"
                if max_move is not None:
                    if 'Long' in sig['type'] and max_move > 0.2: is_effective = "✅ 有效"
                    elif 'Short' in sig['type'] and max_move < -0.2: is_effective = "✅ 有效"
                    else: is_effective = "❌ 无效"
                
                move_str = "查询失败"
                if max_move is not None:
                    move_str = f"📈 {max_move:.2f}%" if max_move > 0 else f"📉 {max_move:.2f}%"

                sig_time = datetime.fromtimestamp(sig['signalTime']/1000, self.timezone).strftime('%H:%M')
                time_ago = f"({sig['hoursAgo']}H前)"
                
                leader_score = sig.get('leader_score') if sig.get('leader_score') is not None else 'N/A'
                quality_score = sig.get('quality_score') if sig.get('quality_score') is not None else 'N/A'
                rs_score = sig.get('rs_score') if sig.get('rs_score') is not None else 'N/A'

                report += f"| {is_effective} | {leader_score} | {quality_score} | {sig_time} {time_ago} | {type_map.get(sig['type'], sig['type'])} | {rs_score} | {sig['signalPrice']:.4g} | {move_str} | {perf.get('timeToPeak', 'N/A')} |\n"
                
        return report
        
    def create_debug_report(self):
        if not self.debug_logs:
            return ""

        report = "\n---\n### **策略调试日志**\n"
        
        # Sort by leader score, descending. Handle None values.
        self.debug_logs.sort(key=lambda x: x.get('leader_score') or -1, reverse=True)
        
        type_map = {'Long Trend': '🚀', 'Long Phoenix': '🔥', 'Long Continuation': '➡️', 'Long Pullback': '🐂', 'Short Trend': '📉', 'Short Continuation': '↘️', 'Short Pullback': '🐻'}

        report += "| 交易对 | 领袖分 | RS分 | 启动 | 延续/回调 | 凤凰 | 详情 |\n"
        report += "|:---|:---:|:---:|:---:|:---:|:---:|:---:|\n"

        for log in self.debug_logs:
            inst_name = log['inst_id'].replace('-USDT-SWAP', '')
            leader_score = log.get('leader_score')
            leader_str = f"{leader_score}" if leader_score is not None else 'N/A'
            rs_score = log.get('rs_score')
            rs_str = f"{rs_score}" if rs_score is not None else 'N/A'

            trend_res = log['checks'].get('多头启动', {}).get('final_result') or log['checks'].get('空头启动', {}).get('final_result')
            trend_icon = type_map.get(trend_res, '➖')

            cont_res = log['checks'].get('顺势多头', {}).get('final_result') or log['checks'].get('顺势空头', {}).get('final_result')
            cont_icon = type_map.get(cont_res, '➖')
            
            phoenix_res = log['checks'].get('凤凰信号', {}).get('final_result')
            phoenix_icon = type_map.get(phoenix_res, '➖')

            details = ""
            for category, data in log['checks'].items():
                details += f"**{category}**:<br>"
                for step in data['steps']:
                    details += f"&nbsp;&nbsp;- {step['name']}: {step['val']} -> {step['res']}<br>"
            
            report += f"| **{inst_name}** | {leader_str} | {rs_str} | {trend_icon} | {cont_icon} | {phoenix_icon} | <details><summary>查看</summary>{details}</details> |\n"

        return report

    def create_opportunity_report(self, backtest_report, opportunities, market_info, upgraded_signals, debug_report):
        opportunities.sort(key=lambda x: x.get('leader_score', 0) or 0, reverse=True)
        upgraded_signals.sort(key=lambda x: x.get('leader_score', 0) or 0, reverse=True)
        type_map = { 'Long Trend': '🚀 多头启动', 'Long Phoenix': '🔥 凤凰信号', 'Long Continuation': '➡️ 多头延续', 'Long Pullback': '🐂 多头回调', 'Long Watchlist': '👀 多头观察', 'Short Trend': '📉 空头启动', 'Short Continuation': '↘️ 空头延续', 'Short Pullback': '🐻 空头回调', 'Short Watchlist': '👀 空头观察' }
        
        content = f"{backtest_report}\n---\n"
        content += f"### 🔥 当前最新机会信号 (仅显示RS > 80)\n"
        content += f"**市场情绪: {market_info.get('text', 'N/A')}**\n\n{market_info.get('details', '')}\n"

        def generate_table(title, opp_list):
            if not opp_list: return ""
            table = f"### {title}\n"
            table += "| 领袖分 | 质量分 | RS分 | 交易对 | 机会类型 | 趋势幅度 | 趋势时长 | 24H成交额 | 24H涨跌幅 |\n"
            table += "|:---:|:---:|:---:|:---|:---|:---:|:---:|:---:|:---:|\n"
            for opp in opp_list:
                inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
                opp_type = type_map.get(opp['type'], '未知')
                vol_str = self.format_volume(opp['volume'])
                change_24h = opp.get('price_change_24h', 0)
                change_24h_str = f"📈 {change_24h:.2f}%" if change_24h > 0 else f"📉 {change_24h:.2f}%"
                
                leader_score_val = opp.get('leader_score')
                leader_score = f"**{leader_score_val}**" if leader_score_val is not None and leader_score_val >= 80 else str(leader_score_val if leader_score_val is not None else 'N/A')
                
                quality_score_val = opp.get('quality_score')
                quality_score = f"**{quality_score_val}**" if quality_score_val is not None and quality_score_val >= 80 else str(quality_score_val if quality_score_val is not None else 'N/A')

                rs_score_val = opp.get('rs_score')
                rs_score = f"**{rs_score_val}**" if rs_score_val is not None and rs_score_val >= 80 else str(rs_score_val if rs_score_val is not None else 'N/A')
                
                trend_change = opp.get('trend_change_pct', 0)
                trend_change_str = f"📈 {trend_change:.1f}%" if trend_change > 0 else (f"📉 {trend_change:.1f}%" if trend_change < 0 else "N/A")
                trend_days = opp.get('trend_duration_days', 0)
                trend_days_str = f"{trend_days:.1f}天" if trend_days > 0 else "N/A"
                warning = " (逆大盘)" if (market_info['sentiment'] == 'Bullish' and 'Short' in opp['type']) or \
                                      (market_info['sentiment'] == 'Bearish' and 'Long' in opp['type']) else ""
                table += f"| {leader_score} | {quality_score} | {rs_score} | **{inst_name}** | {opp_type}{warning} | {trend_change_str} | {trend_days_str} | {vol_str} | {change_24h_str} |\n"
            return table

        upgraded_ids = {s['inst_id'] for s in upgraded_signals}
        new_actionable = [opp for opp in opportunities if opp['inst_id'] not in upgraded_ids]
        
        # Only add tables if there is content for them after filtering
        if upgraded_signals:
            content += generate_table('✨ 信号升级 ✨ (RS > 80)', upgraded_signals)
        if new_actionable:
            content += generate_table('💎 新机会信号 (RS > 80)', new_actionable)
        
        if not upgraded_signals and not new_actionable:
            content += "\n在当前时间点，未发现RS评分高于80的实时交易机会。\n"

        content += self.get_strategy_explanation()
        content += debug_report # Append the debug log at the end
        return content

    def _get_historical_snapshot(self, i, full_data_df, btc_full_df, eth_full_df):
        if len(full_data_df['h1']) <= i: return None
        h1_snapshot_df = full_data_df['h1'].iloc[:-i]
        last_h1_candle_ts = h1_snapshot_df['ts'].iloc[-1]
        
        h4_snapshot_df = full_data_df['h4'][full_data_df['h4']['ts'] <= last_h1_candle_ts]
        d1_snapshot_df = full_data_df['d1'][full_data_df['d1']['ts'] <= last_h1_candle_ts]
        btc_d1_snapshot_df = btc_full_df['d1'][btc_full_df['d1']['ts'] <= last_h1_candle_ts]
        eth_d1_snapshot_df = eth_full_df['d1'][eth_full_df['d1']['ts'] <= last_h1_candle_ts]

        if h1_snapshot_df.empty or h4_snapshot_df.empty or d1_snapshot_df.empty or btc_d1_snapshot_df.empty or eth_d1_snapshot_df.empty:
            return None

        return {
            'h1': h1_snapshot_df.to_dict('records'),
            'h4': h4_snapshot_df.to_dict('records'),
            'd1': d1_snapshot_df.to_dict('records'),
            'btc': {'d1': btc_d1_snapshot_df.to_dict('records')},
            'eth': {'d1': eth_d1_snapshot_df.to_dict('records')}
        }

    def run_backtest(self, all_instruments_data):
        print(f"[{self.get_current_time_str()}] === 开始执行12小时信号回测 ===")
        historical_signals = []
        unique_signal_checker = set()
        
        btc_full_history = all_instruments_data.get('BTC-USDT-SWAP')
        eth_full_history = all_instruments_data.get('ETH-USDT-SWAP')
        if not btc_full_history or not eth_full_history:
            print("BTC或ETH数据不足，无法进行回测。")
            return ""

        all_instruments_df = {
            inst: {
                'h1': self._parse_klines_to_df(data['h1']),
                'h4': self._parse_klines_to_df(data['h4']),
                'd1': self._parse_klines_to_df(data['d1'])
            } for inst, data in all_instruments_data.items()
        }
        btc_full_df = all_instruments_df.get('BTC-USDT-SWAP')
        eth_full_df = all_instruments_df.get('ETH-USDT-SWAP')

        for i in range(1, 13):
            print(f"\r正在回溯过去第 {i}/12 小时的信号...", end="")
            for inst_id, data_df in all_instruments_df.items():
                
                historical_snapshot = self._get_historical_snapshot(i, data_df, btc_full_df, eth_full_df)
                if not historical_snapshot: continue
                
                signal = self.analyze_instrument(inst_id, historical_snapshot, is_historical=True)

                if signal and 'Watchlist' not in signal['type']:
                    unique_id = f"{signal['inst_id']}-{signal['type']}-{signal['signalTime']}"
                    if unique_id not in unique_signal_checker:
                        signal['hoursAgo'] = i
                        historical_signals.append(signal)
                        unique_signal_checker.add(unique_id)
        
        print(f"\n[{self.get_current_time_str()}] 回测完成，发现 {len(historical_signals)} 个历史信号。")

        if historical_signals:
            print(f"[{self.get_current_time_str()}] 正在并发分析 {len(historical_signals)} 个历史信号的表现...")
            with ThreadPoolExecutor(max_workers=self.STRICT_CONCURRENCY_LIMIT) as executor:
                future_to_signal = {executor.submit(self.analyze_signal_performance, sig): sig for sig in historical_signals}
                for j, future in enumerate(as_completed(future_to_signal)):
                    signal = future_to_signal[future]
                    try:
                        performance_data = future.result()
                        signal['performance'] = performance_data
                        print(f"\r信号表现分析进度: {j+1}/{len(historical_signals)}", end="")
                    except Exception as exc:
                        print(f'{signal["inst_id"]} 生成表现时出错: {exc}')
            print(f"\n[{self.get_current_time_str()}] 历史信号表现分析完成。")

        return self.create_backtest_report(historical_signals)

    # --- 主运行函数 ---
    def run(self):
        start_time = time.time()
        print(f"[{self.get_current_time_str()}] === 开始执行监控任务 ===")
        
        self.debug_logs = [] # 重置调试日志
        previous_watchlist = self.load_watchlist_state()
        instruments = self.get_perpetual_instruments()
        if not instruments: return
        
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

        backtest_report = self.run_backtest(all_instruments_data)
        
        print(f"[{self.get_current_time_str()}] === 开始执行当前信号扫描 ===")
        btc_data = all_instruments_data.get('BTC-USDT-SWAP')
        if not btc_data:
            print("未能获取BTC数据，任务终止。")
            return
            
        market_info = self.get_market_sentiment(btc_data)
        print(f"[{self.get_current_time_str()}] 当前市场情绪: {market_info['text']}")
        
        all_opportunities = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
            current_snapshots = {}
            btc_full, eth_full = all_instruments_data['BTC-USDT-SWAP'], all_instruments_data['ETH-USDT-SWAP']
            for inst_id, data in all_instruments_data.items():
                current_snapshots[inst_id] = {**data, 'btc': btc_full, 'eth': eth_full}

            futures = [executor.submit(self.analyze_instrument, inst_id, snap, is_historical=False) for inst_id, snap in current_snapshots.items()]
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                if result:
                    all_opportunities.append(result)
        print(f"\n[{self.get_current_time_str()}] 信号分析完毕，发现 {len(all_opportunities)} 个潜在信号。")

        actionable_opportunities = []
        upgraded_signals = []
        new_watchlist = {}

        if all_opportunities:
            print(f"[{self.get_current_time_str()}] 正在获取 {len(all_opportunities)} 个信号币种的24H涨跌幅...")
            with ThreadPoolExecutor(max_workers=self.CONCURRENCY_LIMIT) as executor:
                ticker_futures = {executor.submit(self.get_ticker_data, opp['inst_id']): opp for opp in all_opportunities}
                for future in as_completed(ticker_futures):
                    opp = ticker_futures[future]
                    ticker_data = future.result()
                    opp['price_change_24h'] = ticker_data.get('price_change_24h', 0)

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
        
        # --- NEW: Filter signals based on RS Score > 80 ---
        initial_actionable_count = len(actionable_opportunities)
        actionable_opportunities_filtered = [
            opp for opp in actionable_opportunities 
            if opp.get('rs_score') is not None and opp.get('rs_score') > 80
        ]
        upgraded_signals_filtered = [
            opp for opp in upgraded_signals 
            if opp.get('rs_score') is not None and opp.get('rs_score') > 80
        ]
        print(f"[{self.get_current_time_str()}] 初始发现 {initial_actionable_count} 个可操作信号, 经过RS > 80筛选后剩余 {len(actionable_opportunities_filtered)} 个。")

        # --- Generate final reports ---
        debug_report = self.create_debug_report()

        # Decide whether to send a notification
        if actionable_opportunities_filtered:
            title = ""
            new_actionable_count = len(actionable_opportunities_filtered) - len(upgraded_signals_filtered)
            if upgraded_signals_filtered:
                title += f"✨ {len(upgraded_signals_filtered)}个升级(RS>80)"
                if new_actionable_count > 0:
                    title += f" + {new_actionable_count}个新机会"
            else:
                title = f"💎 发现 {len(actionable_opportunities_filtered)} 个新机会(RS>80)"
            
            content = self.create_opportunity_report(
                backtest_report, 
                actionable_opportunities_filtered, 
                market_info, 
                upgraded_signals_filtered,
                debug_report
            )
            self.send_notification(title, content)
        else:
            print(f"[{self.get_current_time_str()}] 未发现RS > 80的实时信号。")
            if "未发现" not in backtest_report:
                # If there are no live signals but there is a backtest result, send it
                content = self.create_opportunity_report(backtest_report, [], market_info, [], debug_report)
                self.send_notification("OKX 12小时策略回测报告", content)

        end_time = time.time()
        print(f"[{self.get_current_time_str()}] === 监控任务执行完毕，总耗时: {end_time - start_time:.2f}秒 ===")

if __name__ == "__main__":
    monitor = OKXMonitor()
    monitor.run()
