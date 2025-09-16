#!/usr/bin/env python3
# -*- coding: utf-8 -*-  

import requests
import json
import time
import os
from datetime import datetime
import pandas as pd
import pytz
from concurrent.futures import ThreadPoolExecutor

class OKXMonitor:
    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.server_jiang_key = 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e'
        self.ENABLE_MACD_SCANNER = True
        self.MACD_VOLUME_THRESHOLD = 10_000_000
        self.session = self._create_session()
        self.timezone = pytz.timezone('Asia/Shanghai')
        # 新增：状态管理文件
        self.state_file = 'watchlist_state.json'

    def _create_session(self):
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        return session

    def get_current_time_str(self):
        return datetime.now(self.timezone).strftime('%Y-%m-%d H:%M:%S')

    def send_notification(self, title, content):
        if not self.server_jiang_key:
            print(f"[{self.get_current_time_str()}] 未配置SERVER_JIANG_KEY，通知将打印到控制台。")
            print(f"标题: {title}\n内容:\n{content}")
            return False
        try:
            url = f"https://sctapi.ftqq.com/{self.server_jiang_key}.send"
            data = {'title': title, 'desp': content}
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            result = response.json()
            if result.get('code') == 0:
                print(f"[{self.get_current_time_str()}] 通知发送成功: {title}")
                return True
            else:
                print(f"[{self.get_current_time_str()}] 通知发送失败: {result}")
                return False
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 发送通知时出错: {e}")
            return False

    def get_perpetual_instruments(self):
        try:
            url = f"{self.base_url}/api/v5/public/instruments"
            params = {'instType': 'SWAP'}
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data['code'] == '0':
                instruments = [inst['instId'] for inst in data['data'] if inst['state'] == 'live' and 'USDT' in inst['instId']]
                print(f"[{self.get_current_time_str()}] 获取到 {len(instruments)} 个活跃的USDT永续合约")
                return instruments
            return []
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 获取交易对时出错: {e}")
            return []

    def get_kline_data(self, inst_id, bar='1H', limit=100):
        try:
            url = f"{self.base_url}/api/v5/market/candles"
            params = {'instId': inst_id, 'bar': bar, 'limit': limit}
            response = self.session.get(url, params=params, timeout=15)
            if response.status_code == 429:
                time.sleep(2) 
                response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            if data['code'] == '0' and data['data']:
                return data['data'][::-1]
            return []
        except Exception:
            return []

    def get_ticker_data(self, inst_id):
        """新增：获取24小时涨跌幅等信息"""
        try:
            url = f"{self.base_url}/api/v5/market/ticker"
            params = {'instId': inst_id}
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data['code'] == '0' and data['data']:
                ticker = data['data'][0]
                last_price = float(ticker.get('last', 0))
                open_24h = float(ticker.get('open24h', 0))
                if open_24h > 0:
                    change_pct = ((last_price - open_24h) / open_24h) * 100
                    return {'price_change_24h': change_pct}
            return {'price_change_24h': 0}
        except Exception:
            return {'price_change_24h': 0}

    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        if len(prices) < slow: return []
        prices_series = pd.Series(prices)
        ema_fast = prices_series.ewm(span=fast, adjust=False).mean()
        ema_slow = prices_series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return [{'macd': m, 'signal': s, 'histogram': h} for m, s, h in zip(macd_line, signal_line, histogram)]

    def get_market_sentiment(self):
        """新增：分析BTC得出市场情绪"""
        print(f"[{self.get_current_time_str()}] 正在分析市场情绪 (BTC)...")
        btc_id = 'BTC-USDT-SWAP'
        d1_klines = self.get_kline_data(btc_id, '1D', 100)
        h4_klines = self.get_kline_data(btc_id, '4H', 100)
        h1_klines = self.get_kline_data(btc_id, '1H', 100)
        if not d1_klines or not h4_klines or not h1_klines:
            return 'Neutral', "无法获取BTC数据，情绪未知"

        d1_macd = self.calculate_macd([float(k[4]) for k in d1_klines])
        h4_macd = self.calculate_macd([float(k[4]) for k in h4_klines])
        h1_macd = self.calculate_macd([float(k[4]) for k in h1_klines])

        if len(d1_macd) < 2 or len(h4_macd) < 2 or len(h1_macd) < 2:
            return 'Neutral', "BTC数据不足，情绪未知"

        score = 0
        # 日线权重最高
        if d1_macd[-1]['macd'] > 0 and d1_macd[-1]['signal'] > 0: score += 2
        if d1_macd[-1]['macd'] < 0 and d1_macd[-1]['signal'] < 0: score -= 2
        if d1_macd[-1]['macd'] > d1_macd[-1]['signal']: score += 1
        if d1_macd[-1]['macd'] < d1_macd[-1]['signal']: score -= 1
        # 4小时权重其次
        if h4_macd[-1]['macd'] > 0 and h4_macd[-1]['signal'] > 0: score += 1
        if h4_macd[-1]['macd'] < 0 and h4_macd[-1]['signal'] < 0: score -= 1
        if h4_macd[-1]['macd'] > h4_macd[-1]['signal']: score += 0.5
        if h4_macd[-1]['macd'] < h4_macd[-1]['signal']: score -= 0.5
        
        if score >= 3: return 'Bullish', "强势看涨 🐂"
        if score >= 1: return 'Bullish', "震荡偏多 📈"
        if score <= -3: return 'Bearish', "强势看空 🐻"
        if score <= -1: return 'Bearish', "震荡偏空 📉"
        return 'Neutral', "多空胶着 횡보"

    def analyze_instrument_for_opportunities(self, inst_id):
        try:
            h1_klines = self.get_kline_data(inst_id, '1H', 24)
            if len(h1_klines) < 24: return None
            daily_volume = sum(float(kline[7]) for kline in h1_klines)
            if daily_volume < self.MACD_VOLUME_THRESHOLD: return None
            
            # 并发获取其他数据
            d1_klines, h4_klines, ticker_data = None, None, None
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_d1 = executor.submit(self.get_kline_data, inst_id, '1D', 100)
                future_h4 = executor.submit(self.get_kline_data, inst_id, '4H', 100)
                future_ticker = executor.submit(self.get_ticker_data, inst_id)
                d1_klines = future_d1.result()
                h4_klines = future_h4.result()
                ticker_data = future_ticker.result()

            if not d1_klines or not h4_klines: return None
            
            result_base = {
                'inst_id': inst_id, 
                'volume': daily_volume,
                'price_change_24h': ticker_data.get('price_change_24h', 0)
            }
            d1_closes = [float(k[4]) for k in d1_klines]
            h4_closes = [float(k[4]) for k in h4_klines]
            h1_closes = [float(k[4]) for k in h1_klines]
            d1_macd = self.calculate_macd(d1_closes)
            h4_macd = self.calculate_macd(h4_closes)
            h1_macd = self.calculate_macd(h1_closes)

            # ... (策略检查函数 check_xxx_opportunity 保持不变)
            # ... (粘贴之前的6个策略检查函数到这里)
            def check_long_pullback_opportunity(d1_macd, h1_macd):
                if len(d1_macd) < 2 or len(h1_macd) < 2: return False
                d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
                h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
                daily_ok = (d1_last['macd'] > 0 and d1_last['signal'] > 0 and d1_last['macd'] < d1_last['signal'] and d1_last['histogram'] < 0 and d1_prev['histogram'] < d1_last['histogram'])
                hourly_ok = (h1_last['macd'] > 0 and h1_last['signal'] > 0 and h1_last['macd'] > h1_last['signal'] and h1_prev['macd'] < h1_prev['signal'])
                return daily_ok and hourly_ok

            def check_long_trend_opportunity(d1_macd, h4_macd):
                if len(d1_macd) < 2 or len(h4_macd) < 2: return 'None'
                d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
                daily_ok = ((d1_last['macd'] > 0 or d1_last['signal'] > 0) and (d1_prev['macd'] < 0 or d1_prev['signal'] < 0) and d1_last['macd'] > d1_last['signal'])
                if not daily_ok: return 'None'
                h4_last = h4_macd[-1]
                four_hour_ok = (h4_last['macd'] > 0 and h4_last['signal'] > 0 and h4_last['macd'] > h4_last['signal'])
                return 'Long Trend' if four_hour_ok else 'Long Watchlist'
                
            def check_long_continuation_opportunity(d1_macd, h1_macd):
                if len(d1_macd) < 3 or len(h1_macd) < 2: return False
                d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
                h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
                daily_ok = (d1_last['macd'] > 0 and d1_last['signal'] > 0 and d1_last['macd'] > d1_last['signal'] and d1_prev['macd'] < d1_prev['signal'] and d1_last['histogram'] > d1_prev['histogram'])
                hourly_ok = (h1_last['macd'] > 0 and h1_last['signal'] > 0 and h1_last['macd'] > h1_last['signal'] and h1_prev['macd'] < h1_prev['signal'])
                return daily_ok and hourly_ok

            def check_short_pullback_opportunity(d1_macd, h1_macd):
                if len(d1_macd) < 2 or len(h1_macd) < 2: return False
                d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
                h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
                daily_ok = (d1_last['macd'] < 0 and d1_last['signal'] < 0 and d1_last['macd'] > d1_last['signal'] and d1_last['histogram'] > 0 and d1_prev['histogram'] > d1_last['histogram'])
                hourly_ok = (h1_last['macd'] < 0 and h1_last['signal'] < 0 and h1_last['macd'] < h1_last['signal'] and h1_prev['macd'] > h1_prev['signal'])
                return daily_ok and hourly_ok

            def check_short_trend_opportunity(d1_macd, h4_macd):
                if len(d1_macd) < 2 or len(h4_macd) < 2: return 'None'
                d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
                daily_ok = ((d1_last['macd'] < 0 or d1_last['signal'] < 0) and (d1_prev['macd'] > 0 or d1_prev['signal'] > 0) and d1_last['macd'] < d1_last['signal'])
                if not daily_ok: return 'None'
                h4_last = h4_macd[-1]
                four_hour_ok = (h4_last['macd'] < 0 and h4_last['signal'] < 0 and h4_last['macd'] < h4_last['signal'])
                return 'Short Trend' if four_hour_ok else 'Short Watchlist'
                
            def check_short_continuation_opportunity(d1_macd, h1_macd):
                if len(d1_macd) < 3 or len(h1_macd) < 2: return False
                d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
                h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
                daily_ok = (d1_last['macd'] < 0 and d1_last['signal'] < 0 and d1_last['macd'] < d1_last['signal'] and d1_prev['macd'] > d1_prev['signal'] and d1_last['histogram'] < d1_prev['histogram'])
                hourly_ok = (h1_last['macd'] < 0 and h1_last['signal'] < 0 and h1_last['macd'] < h1_last['signal'] and h1_prev['macd'] > h1_prev['signal'])
                return daily_ok and hourly_ok
            
            # --- 策略检查流程 ---
            long_trend_status = check_long_trend_opportunity(d1_macd, h4_macd)
            if long_trend_status != 'None': return {**result_base, 'type': long_trend_status}
            short_trend_status = check_short_trend_opportunity(d1_macd, h4_macd)
            if short_trend_status != 'None': return {**result_base, 'type': short_trend_status}
            if check_long_continuation_opportunity(d1_macd, h1_macd): return {**result_base, 'type': 'Long Continuation'}
            if check_short_continuation_opportunity(d1_macd, h1_macd): return {**result_base, 'type': 'Short Continuation'}
            if check_long_pullback_opportunity(d1_macd, h1_macd): return {**result_base, 'type': 'Long Pullback'}
            if check_short_pullback_opportunity(d1_macd, h1_macd): return {**result_base, 'type': 'Short Pullback'}
            return None
        except Exception:
            return None

    def create_opportunity_report(self, opportunities, market_sentiment, sentiment_text, upgraded_signals):
        rank = {
            'Long Trend': 1, 'Short Trend': 1, 'Long Continuation': 1, 'Short Continuation': 1,
            'Long Pullback': 1, 'Short Pullback': 1, 'Long Watchlist': 2, 'Short Watchlist': 2
        }
        opportunities.sort(key=lambda x: (rank.get(x['type'], 3), -x['volume']))
        
        type_map = {
            'Long Trend': '🚀 多头趋势', 'Long Continuation': '➡️ 多头延续', 'Long Pullback': '🐂 多头回调', 'Long Watchlist': '👀 多头观察',
            'Short Trend': '📉 空头趋势', 'Short Continuation': '↘️ 空头延续', 'Short Pullback': '🐻 空头回调', 'Short Watchlist': '👀 空头观察'
        }
        
        content = f"### 市场情绪: {sentiment_text}\n\n"

        # 升级信号部分
        if upgraded_signals:
            content += "### ✨ 信号升级 ✨\n"
            content += "| 交易对 | 升级信号 | 24H成交额 | 24H涨跌幅 | 图表 |\n|:---|:---|:---|:---|:---|\n"
            for opp in upgraded_signals:
                inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
                opp_type = type_map.get(opp['type'], '未知')
                volume_str = self.format_volume(opp['volume'])
                change_pct_str = f"📈 +{opp['price_change_24h']:.2f}%" if opp['price_change_24h'] > 0 else f"📉 {opp['price_change_24h']:.2f}%"
                tv_symbol = opp['inst_id'].replace('-SWAP', '.P')
                tv_link = f"[图表](https://www.tradingview.com/chart/?symbol=OKX:{tv_symbol})"
                content += f"| **{inst_name}** | {opp_type} | {volume_str} | {change_pct_str} | {tv_link} |\n"
            content += "\n---\n\n"

        # 新机会部分
        new_opportunities = [opp for opp in opportunities if opp['inst_id'] not in [up['inst_id'] for up in upgraded_signals]]
        if new_opportunities:
            content += "### 💎 新机会信号\n"
            content += "| 交易对 | 机会类型 | 24H成交额 | 24H涨跌幅 | 图表 |\n|:---|:---|:---|:---|:---|\n"
            for opp in new_opportunities:
                inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
                opp_type = type_map.get(opp['type'], '未知')
                volume_str = self.format_volume(opp['volume'])
                change_pct_str = f"📈 +{opp['price_change_24h']:.2f}%" if opp['price_change_24h'] > 0 else f"📉 {opp['price_change_24h']:.2f}%"
                tv_symbol = opp['inst_id'].replace('-SWAP', '.P')
                tv_link = f"[图表](https://www.tradingview.com/chart/?symbol=OKX:{tv_symbol})"
                
                # 逆大盘警告
                warning = ""
                if (market_sentiment == 'Bullish' and 'Short' in opp['type']) or \
                   (market_sentiment == 'Bearish' and 'Long' in opp['type']):
                    warning = " (逆大盘)"
                
                content += f"| **{inst_name}** | {opp_type}{warning} | {volume_str} | {change_pct_str} | {tv_link} |\n"
        
        # ... (报告底部策略说明)
        content += "\n---\n**策略说明:**\n- **趋势**: 日线刚穿越0轴 + 4H确认。\n- **延续**: 日线0轴同向盘整后突破 + 1H确认。\n- **回调**: 日线同向趋势中回调 + 1H确认。\n- **观察**: 日线已满足趋势条件，等待4H信号确认。\n"
        return content

    def format_volume(self, volume):
        if volume >= 1_000_000_000: return f"{volume/1_000_000_000:.2f}B"
        if volume >= 1_000_000: return f"{volume/1_000_000:.2f}M"
        if volume >= 1_000: return f"{volume/1_000:.2f}K"
        return f"{volume:.2f}"

    def load_watchlist_state(self):
        """新增：加载上次的观察列表"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 加载状态文件失败: {e}")
        return {}

    def save_watchlist_state(self, watchlist):
        """新增：保存本次的观察列表"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(watchlist, f, indent=4)
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 保存状态文件失败: {e}")

    def run(self):
        current_time = self.get_current_time_str()
        print(f"[{current_time}] 开始执行监控任务...")
        if not self.ENABLE_MACD_SCANNER:
            print(f"[{current_time}] MACD扫描功能已关闭。")
            return

        previous_watchlist = self.load_watchlist_state()
        market_sentiment, sentiment_text = self.get_market_sentiment()
        print(f"[{current_time}] 当前市场情绪: {sentiment_text}")

        instruments = self.get_perpetual_instruments()
        if not instruments: return
        
        all_opportunities = []
        max_workers = 5
        batch_size = 10 
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, len(instruments), batch_size):
                batch = instruments[i:i + batch_size]
                print(f"[{current_time}] 正在处理批次 {i//batch_size + 1}/{(len(instruments) + batch_size - 1)//batch_size}...")
                results = executor.map(self.analyze_instrument_for_opportunities, batch)
                for result in results:
                    if result:
                        all_opportunities.append(result)
                        print(f"[{current_time}] 发现信号: {result['inst_id']} ({result['type']})")
                if i + batch_size < len(instruments):
                    time.sleep(2)
        
        if all_opportunities:
            upgraded_signals = []
            new_watchlist = {}
            actionable_opportunities = []

            for opp in all_opportunities:
                inst_id = opp['inst_id']
                opp_type = opp['type']
                
                if 'Watchlist' not in opp_type:
                    actionable_opportunities.append(opp)
                    # 检查是否从观察列表升级而来
                    if inst_id in previous_watchlist:
                        upgraded_signals.append(opp)
                        print(f"[{current_time}] 信号升级: {inst_id} 从 {previous_watchlist[inst_id]} 升级为 {opp_type}")

                if 'Watchlist' in opp_type:
                    new_watchlist[inst_id] = opp_type

            # 保存当前周期的观察列表
            self.save_watchlist_state(new_watchlist)

            if actionable_opportunities:
                # 即使只有升级信号，也发送通知
                title = ""
                if upgraded_signals:
                    title += f"✨ {len(upgraded_signals)}个信号升级"
                    # 过滤掉已升级的，避免重复计入新机会
                    new_actionable = [opp for opp in actionable_opportunities if opp['inst_id'] not in [up['inst_id'] for up in upgraded_signals]]
                    if new_actionable:
                        title += f" + {len(new_actionable)}个新机会"
                else:
                    title = f"💎 发现 {len(actionable_opportunities)} 个新机会"

                content = self.create_opportunity_report(all_opportunities, market_sentiment, sentiment_text, upgraded_signals)
                self.send_notification(title, content)
            else:
                print(f"[{current_time}] 仅发现 {len(all_opportunities)} 个观察信号，本次不发送通知。")
        else:
            print(f"[{current_time}] 本次未发现任何符合条件的信号。")
        
        print(f"[{current_time}] 监控任务执行完毕。")

if __name__ == "__main__":
    monitor = OKXMonitor()
    monitor.run()
