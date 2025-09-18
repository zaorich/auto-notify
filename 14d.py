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
        self.ATR_MULTIPLIER = 2.0
        self.MAX_CANDLES_AGO = 5
        
        # --- [新] 开发者调试模式开关 ---
        self.DEV_MODE = True  # 设置为 True 来开启详细的调试输出

        self.session = self._create_session()
        self.timezone = pytz.timezone('Asia/Shanghai')
        self.state_file = 'watchlist_state.json'
        # [新] 用于存储调试信息
        self.debug_info = []

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

    def calculate_atr(self, klines_df, period=14):
        if klines_df.empty or len(klines_df) < period: return []
        high_low = klines_df['high'] - klines_df['low']
        high_close = (klines_df['high'] - klines_df['close'].shift()).abs()
        low_close = (klines_df['low'] - klines_df['close'].shift()).abs()
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        atr = true_range.ewm(alpha=1/period, adjust=False).mean()
        return atr.tolist()

    def find_last_cross_info(self, macds):
        if len(macds) < 2: return None
        last_cross_type = 'golden' if macds[-1]['macd'] > macds[-1]['signal'] else 'death'
        for i in range(len(macds) - 2, -1, -1):
            current_cross_type = 'golden' if macds[i]['macd'] > macds[i]['signal'] else 'death'
            if current_cross_type != last_cross_type:
                return {'type': last_cross_type, 'index': i + 1}
        return {'type': last_cross_type, 'index': 0}

    def get_market_sentiment(self):
        # ... (此函数无变化)
        print(f"[{self.get_current_time_str()}] 正在分析市场情绪 (BTC)...")
        btc_id = 'BTC-USDT-SWAP'
        klines = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_d1 = executor.submit(self.get_kline_data, btc_id, '1D', 100)
            future_h4 = executor.submit(self.get_kline_data, btc_id, '4H', 100)
            future_h1 = executor.submit(self.get_kline_data, btc_id, '1H', 100)
            klines['1D'] = future_d1.result()
            klines['4H'] = future_h4.result()
            klines['1H'] = future_h1.result()
        if not all(klines.values()): return 'Neutral', "无法获取BTC数据", "分析失败"
        macds = {tf: self.calculate_macd([float(k[4]) for k in data]) for tf, data in klines.items()}
        if any(len(macd_data) < 2 for macd_data in macds.values()): return 'Neutral', "BTC数据不足", "分析失败"
        score, analysis_details = 0, []
        for tf, weight in [('1D', 2), ('4H', 1), ('1H', 0.5)]:
            last, prev = macds[tf][-1], macds[tf][-2]
            pos_text = "0轴上方" if last['macd'] > 0 else "0轴下方"; score += weight if last['macd'] > 0 else -weight
            cross_text = "金叉" if last['macd'] > last['signal'] else "死叉"; score += weight * 0.5 if last['macd'] > last['signal'] else -weight * 0.5
            hist_text = "动能增强" if abs(last['histogram']) > abs(prev['histogram']) else "动能减弱"
            analysis_details.append(f"**{tf}**: {pos_text}, {cross_text}, {hist_text}")
        if score >= 4: sentiment, text = 'Bullish', "强势看涨 🐂"
        elif score >= 1.5: sentiment, text = 'Bullish', "震荡偏多 📈"
        elif score <= -4: sentiment, text = 'Bearish', "强势看空 🐻"
        elif score <= -1.5: sentiment, text = 'Bearish', "震荡偏空 📉"
        else: sentiment, text = 'Neutral', "多空胶着 횡보"
        details_text = "\n".join([f"- {d}" for d in analysis_details])
        return sentiment, text, details_text

    def get_signal_freshness_info(self, klines_df, macds, cross_type, atr):
        last_cross = self.find_last_cross_info(macds)
        if not last_cross or last_cross['type'] != cross_type:
            return False, 0, 0
        signal_index = last_cross['index']
        candles_ago = len(klines_df) - 1 - signal_index
        if candles_ago > self.MAX_CANDLES_AGO:
            return False, 0, 0
        if signal_index < 0 or signal_index >= len(atr):
            return False, 0, 0
        signal_price = klines_df['close'].iloc[signal_index]
        current_price = klines_df['close'].iloc[-1]
        atr_at_signal = atr[signal_index]
        price_change_since_signal = ((current_price - signal_price) / signal_price) * 100 if signal_price > 0 else 0
        signal_timestamp = klines_df['ts'].iloc[signal_index] / 1000
        hours_since_signal = (time.time() - signal_timestamp) / 3600
        if atr_at_signal > 0 and abs(current_price - signal_price) > (self.ATR_MULTIPLIER * atr_at_signal):
            return False, price_change_since_signal, hours_since_signal
        return True, price_change_since_signal, hours_since_signal

    def analyze_instrument_for_opportunities(self, inst_id):
        try:
            h1_klines = self.get_kline_data(inst_id, '1H', 100)
            if len(h1_klines) < 24: return None
            daily_volume = sum(float(kline[7]) for kline in h1_klines[-24:])
            if daily_volume < self.MACD_VOLUME_THRESHOLD: return None
            
            d1_klines, h4_klines, ticker_data = None, None, None
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_d1 = executor.submit(self.get_kline_data, inst_id, '1D', 100)
                future_h4 = executor.submit(self.get_kline_data, inst_id, '4H', 100)
                future_ticker = executor.submit(self.get_ticker_data, inst_id)
                d1_klines = future_d1.result()
                h4_klines = future_h4.result()
                ticker_data = future_ticker.result()
            if not d1_klines or not h4_klines: return None
            
            result_base = {'inst_id': inst_id, 'volume': daily_volume, 'price_change_24h': ticker_data.get('price_change_24h', 0)}
            
            d1_klines_df = pd.DataFrame(d1_klines, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm']).astype(float)
            h1_klines_df = pd.DataFrame(h1_klines, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm']).astype(float)
            
            all_macds = {
                '1D': self.calculate_macd(d1_klines_df['close']),
                '4H': self.calculate_macd([float(k[4]) for k in h4_klines]),
                '1H': self.calculate_macd(h1_klines_df['close'])
            }
            all_atrs = {
                '1D': self.calculate_atr(d1_klines_df),
                '1H': self.calculate_atr(h1_klines_df)
            }
            
            # --- [重构] 策略检查函数现在返回 (是否满足, 价格变动, 时间, [调试信息]) ---
            def check_long_trend_opportunity():
                d1_macd, h4_macd = all_macds['1D'], all_macds['4H']
                if len(d1_macd) < 2 or len(h4_macd) < 2: return 'None', 0, 0
                d1_last, d1_prev, h4_last = d1_macd[-1], d1_macd[-2], h4_macd[-1]
                is_fresh_cross_zero = ((d1_last['macd'] > 0 or d1_last['signal'] > 0) and (d1_prev['macd'] < 0 or d1_prev['signal'] < 0))
                daily_ok, price_change, hours_since, debug = False, 0, 0, {}
                if d1_last['macd'] > d1_last['signal']:
                    is_fresh, price_change, hours_since = self.get_signal_freshness_info(d1_klines_df, d1_macd, 'golden', all_atrs['1D'])
                    if is_fresh_cross_zero or is_fresh: 
                        daily_ok = True
                        debug['condition'] = "日线刚上穿0轴或启动不久"
                if not daily_ok: return 'None', 0, 0
                four_hour_ok = (h4_last['macd'] > 0 and h4_last['signal'] > 0 and h4_last['macd'] > h4_last['signal'])
                if four_hour_ok: debug['confirmation'] = "4H 0上金叉确认"
                return ('Long Trend' if four_hour_ok else 'Long Watchlist'), price_change, hours_since, debug
            
            # ... (其他策略函数也需要类似修改以返回 debug_info)
            
            # --- Analysis Flow ---
            opp_type, price_change, hours_since, debug_info = check_long_trend_opportunity()
            if opp_type != 'None':
                result = {**result_base, 'type': opp_type, 'price_change_since_signal': price_change, 'hours_since_signal': hours_since}
                if self.DEV_MODE and len(self.debug_info) < 3: self.debug_info.append({**result, 'debug': debug_info, 'macds': all_macds})
                return result

            # ... (对其他策略调用也做类似处理)
            return None
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 分析 {inst_id} 时出错: {e}")
            return None

    def create_opportunity_report(self, opportunities, market_sentiment, sentiment_text, sentiment_details, upgraded_signals):
        rank = {'Long Trend': 1, 'Short Trend': 1, 'Long Continuation': 1, 'Short Continuation': 1, 'Long Pullback': 1, 'Short Pullback': 1, 'Long Watchlist': 2, 'Short Watchlist': 2}
        opportunities.sort(key=lambda x: (rank.get(x['type'], 3), -x['volume']))
        type_map = {'Long Trend': '🚀 多头启动', 'Long Continuation': '➡️ 多头延续', 'Long Pullback': '🐂 多头回调', 'Long Watchlist': '👀 多头观察', 'Short Trend': '📉 空头启动', 'Short Continuation': '↘️ 空头延续', 'Short Pullback': '🐻 空头回调', 'Short Watchlist': '👀 空头观察'}
        content = f"### 市场情绪: {sentiment_text}\n<details><summary>点击查看情绪分析依据</summary>\n\n{sentiment_details}\n\n</details>\n\n"
        
        def generate_table_rows(opp_list):
            rows = ""
            for opp in opp_list:
                inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
                opp_type = type_map.get(opp['type'], '未知')
                volume_str = self.format_volume(opp['volume'])
                change_24h_str = f"📈 +{opp['price_change_24h']:.2f}%" if opp['price_change_24h'] > 0 else f"📉 {opp['price_change_24h']:.2f}%"
                since_signal_change = opp.get('price_change_since_signal', 0)
                since_signal_str = f"📈 +{since_signal_change:.2f}%" if since_signal_change > 0 else f"📉 {since_signal_change:.2f}%"
                hours_since = opp.get('hours_since_signal', 0)
                hours_since_str = f"{hours_since:.1f} H"
                warning = " (逆大盘)" if (market_sentiment == 'Bullish' and 'Short' in opp['type']) or (market_sentiment == 'Bearish' and 'Long' in opp['type']) else ""
                rows += f"| **{inst_name}** | {opp_type}{warning} | {hours_since_str} | {since_signal_str} | {volume_str} | {change_24h_str} |\n"
            return rows

        table_header = "| 交易对 | 机会类型 | 信号时长 | 信号后幅度 | 24H成交额 | 24H涨跌幅 |\n|:---|:---|:---|:---|:---|:---|\n"

        if upgraded_signals:
            content += "### ✨ 信号升级 ✨\n" + table_header
            content += generate_table_rows(upgraded_signals)
            content += "\n---\n\n"
        new_opportunities = [opp for opp in opportunities if opp['inst_id'] not in [up['inst_id'] for up in upgraded_signals]]
        if new_opportunities:
            content += "### 💎 新机会信号\n" + table_header
            content += generate_table_rows(new_opportunities)
        
        # [新] 添加调试信息部分
        if self.DEV_MODE and self.debug_info:
            content += "\n---\n### 🧪 信号调试信息 (前3个)\n"
            for info in self.debug_info:
                content += f"\n#### `{info['inst_id']}` - {type_map.get(info['type'])} \n"
                content += f"- **满足条件**: {info['debug'].get('condition', 'N/A')}\n"
                content += f"- **确认条件**: {info['debug'].get('confirmation', 'N/A')}\n"
                content += f"- **信号后**: {info.get('hours_since_signal', 0):.1f}小时, 价格变动 {info.get('price_change_since_signal', 0):.2f}%\n"
                content += "```\n"
                for tf in ['1D', '4H', '1H']:
                    if tf in info['macds'] and len(info['macds'][tf]) >= 2:
                        last, prev = info['macds'][tf][-1], info['macds'][tf][-2]
                        content += f"[{tf}] MACD:\n"
                        content += f"  - 最新: D:{last['macd']:.4f}, S:{last['signal']:.4f}, H:{last['histogram']:.4f}\n"
                        content += f"  - 上期: D:{prev['macd']:.4f}, S:{prev['signal']:.4f}, H:{prev['histogram']:.4f}\n"
                content += "```\n"

        content += "\n---\n**策略说明:**\n- **启动**: 日线刚穿越0轴(或启动不久) + 4H确认。\n- **延续**: 日线同向盘整后再突破 + 4H配合 + 1H确认入场。\n- **回调**: 日线同向趋势中回调 + 4H&1H确认回调结束。\n- **新鲜度**: '启动不久'指信号K线后价格变动小于`2*ATR`且在`5根`K线内。"
        return content

    def format_volume(self, volume):
        if volume >= 1_000_000_000: return f"{volume/1_000_000_000:.2f}B"
        if volume >= 1_000_000: return f"{volume/1_000_000:.2f}M"
        if volume >= 1_000: return f"{volume/1_000:.2f}K"
        return f"{volume:.2f}"

    def load_watchlist_state(self):
        if not os.path.exists(self.state_file): return {}
        try:
            with open(self.state_file, 'r') as f:
                content = f.read()
                if not content: return {}
                return json.loads(content)
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 加载状态文件失败: {e}")
            return {}

    def save_watchlist_state(self, watchlist):
        try:
            with open(self.state_file, 'w') as f: json.dump(watchlist, f, indent=4)
        except Exception as e: print(f"[{self.get_current_time_str()}] 保存状态文件失败: {e}")

    def run(self):
        current_time = self.get_current_time_str()
        print(f"[{current_time}] 开始执行监控任务...")
        if not self.ENABLE_MACD_SCANNER: return
        previous_watchlist = self.load_watchlist_state()
        market_sentiment, sentiment_text, sentiment_details = self.get_market_sentiment()
        print(f"[{current_time}] 当前市场情绪: {sentiment_text}\n情绪分析依据:\n{sentiment_details}")
        instruments = self.get_perpetual_instruments()
        if not instruments: return
        all_opportunities, max_workers, batch_size = [], 5, 10 
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            instrument_args = [{'inst_id': inst} for inst in instruments]
            results = executor.map(lambda p: self.analyze_instrument_for_opportunities(**p), instrument_args)
            for inst_arg, result in zip(instrument_args, results):
                try:
                    if result:
                        all_opportunities.append(result)
                        print(f"[{current_time}] 发现信号: {result['inst_id']} ({result['type']})")
                except Exception as e:
                    inst_id = inst_arg.get('inst_id', '未知币种')
                    print(f"[{current_time}] 处理币种 {inst_id} 的结果时出错: {e}")

        if all_opportunities:
            upgraded_signals, new_watchlist, actionable_opportunities = [], {}, []
            for opp in filter(None, all_opportunities):
                try:
                    inst_id = opp['inst_id']
                    opp_type = opp['type']
                    if 'Watchlist' not in opp_type:
                        actionable_opportunities.append(opp)
                        if isinstance(previous_watchlist, dict) and inst_id in previous_watchlist:
                            upgraded_signals.append(opp)
                            print(f"[{current_time}] 信号升级: {inst_id} 从 {previous_watchlist[inst_id]} 升级为 {opp_type}")
                    if 'Watchlist' in opp_type:
                        new_watchlist[inst_id] = opp_type
                except (TypeError, KeyError) as e:
                    print(f"[{current_time}] 处理机会列表时遇到无效数据: {opp}, 错误: {e}")
            self.save_watchlist_state(new_watchlist)
            if actionable_opportunities:
                title = ""
                if upgraded_signals:
                    title += f"✨ {len(upgraded_signals)}个信号升级"
                    new_actionable = [opp for opp in actionable_opportunities if opp['inst_id'] not in [up['inst_id'] for up in upgraded_signals]]
                    if new_actionable: title += f" + {len(new_actionable)}个新机会"
                else:
                    title = f"💎 发现 {len(actionable_opportunities)} 个新机会"
                content = self.create_opportunity_report(all_opportunities, market_sentiment, sentiment_text, sentiment_details, upgraded_signals)
                self.send_notification(title, content)
            else:
                print(f"[{current_time}] 仅发现 {len(all_opportunities)} 个观察信号，不发送通知。")
        else:
            print(f"[{current_time}] 本次未发现任何符合条件的信号。")
        print(f"[{current_time}] 监控任务执行完毕。")

if __name__ == "__main__":
    monitor = OKXMonitor()
    monitor.run()
