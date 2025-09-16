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
        # --- 核心配置 ---
        self.base_url = "https://www.okx.com"
        self.server_jiang_key = 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e'
        
        # --- 功能开关 ---
        self.ENABLE_MACD_SCANNER = True
        
        # --- MACD扫描器配置 ---
        self.MACD_VOLUME_THRESHOLD = 10_000_000

        # --- 通用配置 ---
        self.session = self._create_session()
        self.timezone = pytz.timezone('Asia/Shanghai')

    def _create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        return session

    def get_current_time_str(self):
        return datetime.now(self.timezone).strftime('%Y-%m-%d H:%M:%S')

    def send_notification(self, title, content):
        if not self.server_jiang_key:
            print(f"[{self.get_current_time_str()}] 未在GitHub Secrets中配置SERVER_JIANG_KEY，通知将打印到控制台。")
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

    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        if len(prices) < slow: return []
        prices_series = pd.Series(prices)
        ema_fast = prices_series.ewm(span=fast, adjust=False).mean()
        ema_slow = prices_series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return [{'macd': m, 'signal': s, 'histogram': h} for m, s, h in zip(macd_line, signal_line, histogram)]

    # --- 策略函数 ---
    def check_long_pullback_opportunity(self, d1_macd, h1_macd):
        if len(d1_macd) < 2 or len(h1_macd) < 2: return False
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
        daily_ok = (d1_last['macd'] > 0 and d1_last['signal'] > 0 and d1_last['macd'] < d1_last['signal'] and d1_last['histogram'] < 0 and d1_prev['histogram'] < d1_last['histogram'])
        hourly_ok = (h1_last['macd'] > 0 and h1_last['signal'] > 0 and h1_last['macd'] > h1_last['signal'] and h1_prev['macd'] < h1_prev['signal'])
        return daily_ok and hourly_ok

    def check_long_trend_opportunity(self, d1_macd, h4_macd):
        if len(d1_macd) < 2 or len(h4_macd) < 2: return 'None'
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        daily_ok = ((d1_last['macd'] > 0 or d1_last['signal'] > 0) and (d1_prev['macd'] < 0 or d1_prev['signal'] < 0) and d1_last['macd'] > d1_last['signal'])
        if not daily_ok: return 'None'
        h4_last = h4_macd[-1]
        four_hour_ok = (h4_last['macd'] > 0 and h4_last['signal'] > 0 and h4_last['macd'] > h4_last['signal'])
        return 'Long Trend' if four_hour_ok else 'Long Watchlist'
        
    def check_long_continuation_opportunity(self, d1_macd, h1_macd):
        if len(d1_macd) < 3 or len(h1_macd) < 2: return False
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
        # 日线: 0上，刚完成死叉后的再次金叉
        daily_ok = (d1_last['macd'] > 0 and d1_last['signal'] > 0 and 
                    d1_last['macd'] > d1_last['signal'] and # 当前是金叉
                    d1_prev['macd'] < d1_prev['signal'] and # 上一根是死叉
                    d1_last['histogram'] > d1_prev['histogram']) # 动能增强
        # 1小时: 0上刚金叉，提供精确入场点
        hourly_ok = (h1_last['macd'] > 0 and h1_last['signal'] > 0 and 
                     h1_last['macd'] > h1_last['signal'] and 
                     h1_prev['macd'] < h1_prev['signal'])
        return daily_ok and hourly_ok

    def check_short_pullback_opportunity(self, d1_macd, h1_macd):
        if len(d1_macd) < 2 or len(h1_macd) < 2: return False
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
        daily_ok = (d1_last['macd'] < 0 and d1_last['signal'] < 0 and d1_last['macd'] > d1_last['signal'] and d1_last['histogram'] > 0 and d1_prev['histogram'] > d1_last['histogram'])
        hourly_ok = (h1_last['macd'] < 0 and h1_last['signal'] < 0 and h1_last['macd'] < h1_last['signal'] and h1_prev['macd'] > h1_prev['signal'])
        return daily_ok and hourly_ok

    def check_short_trend_opportunity(self, d1_macd, h4_macd):
        if len(d1_macd) < 2 or len(h4_macd) < 2: return 'None'
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        daily_ok = ((d1_last['macd'] < 0 or d1_last['signal'] < 0) and (d1_prev['macd'] > 0 or d1_prev['signal'] > 0) and d1_last['macd'] < d1_last['signal'])
        if not daily_ok: return 'None'
        h4_last = h4_macd[-1]
        four_hour_ok = (h4_last['macd'] < 0 and h4_last['signal'] < 0 and h4_last['macd'] < h4_last['signal'])
        return 'Short Trend' if four_hour_ok else 'Short Watchlist'
        
    def check_short_continuation_opportunity(self, d1_macd, h1_macd):
        if len(d1_macd) < 3 or len(h1_macd) < 2: return False
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
        # 日线: 0下，刚完成金叉后的再次死叉
        daily_ok = (d1_last['macd'] < 0 and d1_last['signal'] < 0 and 
                    d1_last['macd'] < d1_last['signal'] and # 当前是死叉
                    d1_prev['macd'] > d1_prev['signal'] and # 上一根是金叉
                    d1_last['histogram'] < d1_prev['histogram']) # 动能增强
        # 1小时: 0下刚死叉，提供精确入场点
        hourly_ok = (h1_last['macd'] < 0 and h1_last['signal'] < 0 and 
                     h1_last['macd'] < h1_last['signal'] and 
                     h1_prev['macd'] > h1_prev['signal'])
        return daily_ok and hourly_ok
    
    def analyze_instrument_for_opportunities(self, inst_id):
        try:
            h1_klines = self.get_kline_data(inst_id, '1H', 24)
            if len(h1_klines) < 24: return None
            daily_volume = sum(float(kline[7]) for kline in h1_klines)
            if daily_volume < self.MACD_VOLUME_THRESHOLD: return None
            d1_klines = self.get_kline_data(inst_id, '1D', 100)
            h4_klines = self.get_kline_data(inst_id, '4H', 100)
            if not d1_klines or not h4_klines: return None
            
            d1_closes = [float(k[4]) for k in d1_klines]
            h4_closes = [float(k[4]) for k in h4_klines]
            h1_closes = [float(k[4]) for k in h1_klines]
            d1_macd = self.calculate_macd(d1_closes)
            h4_macd = self.calculate_macd(h4_closes)
            h1_macd = self.calculate_macd(h1_closes)
            
            # --- 升级版策略检查流程 ---
            # 优先检查趋势和延续机会
            long_trend_status = self.check_long_trend_opportunity(d1_macd, h4_macd)
            if long_trend_status != 'None':
                return {'inst_id': inst_id, 'type': long_trend_status, 'volume': daily_volume}
            
            short_trend_status = self.check_short_trend_opportunity(d1_macd, h4_macd)
            if short_trend_status != 'None':
                return {'inst_id': inst_id, 'type': short_trend_status, 'volume': daily_volume}

            if self.check_long_continuation_opportunity(d1_macd, h1_macd):
                return {'inst_id': inst_id, 'type': 'Long Continuation', 'volume': daily_volume}

            if self.check_short_continuation_opportunity(d1_macd, h1_macd):
                return {'inst_id': inst_id, 'type': 'Short Continuation', 'volume': daily_volume}

            # 最后检查回调机会
            if self.check_long_pullback_opportunity(d1_macd, h1_macd):
                return {'inst_id': inst_id, 'type': 'Long Pullback', 'volume': daily_volume}
                
            if self.check_short_pullback_opportunity(d1_macd, h1_macd):
                return {'inst_id': inst_id, 'type': 'Short Pullback', 'volume': daily_volume}
                
            return None
        except Exception:
            return None

    def create_opportunity_report(self, opportunities):
        rank = {
            'Long Trend': 1, 'Short Trend': 1, 
            'Long Continuation': 1, 'Short Continuation': 1,
            'Long Pullback': 1, 'Short Pullback': 1,
            'Long Watchlist': 2, 'Short Watchlist': 2
        }
        opportunities.sort(key=lambda x: (rank.get(x['type'], 3), -x['volume']))
        
        type_map = {
            'Long Trend': '🚀 多头趋势', 
            'Long Continuation': '➡️ 多头延续',
            'Long Pullback': '🐂 多头回调', 
            'Long Watchlist': '👀 多头观察',
            'Short Trend': '📉 空头趋势', 
            'Short Continuation': '↘️ 空头延续',
            'Short Pullback': '🐻 空头回调', 
            'Short Watchlist': '👀 空头观察'
        }
        content = f"### 发现 {len(opportunities)} 个多空信号\n\n"
        content += "| 交易对 | 机会类型 | 24H成交额 |\n|:---|:---|:---|\n"
        for opp in opportunities:
            inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
            opp_type = type_map.get(opp['type'], '未知')
            volume_str = self.format_volume(opp['volume'])
            content += f"| **{inst_name}** | {opp_type} | {volume_str} |\n"
        
        content += "\n---\n**策略说明:**\n"
        content += "- **多头趋势**: 日线刚上穿0轴金叉 + 4H已在0上金叉。\n"
        content += "- **多头延续**: 日线0上死叉后再金叉 + 1H在0上刚金叉。\n"
        content += "- **多头回调**: 日线0上死叉回调 + 1H在0上刚金叉。\n"
        content += "- **空头趋势**: 日线刚下穿0轴死叉 + 4H已在0下死叉。\n"
        content += "- **空头延续**: 日线0下金叉后再死叉 + 1H在0下刚死叉。\n"
        content += "- **空头回调**: 日线0下金叉反弹 + 1H在0下刚死叉。\n"
        content += f"- **观察信号**: 指日线已满足趋势条件，等待4H信号确认。\n"
        content += f"- **筛选条件**: 24H成交额 > {self.format_volume(self.MACD_VOLUME_THRESHOLD)} USDT。"
        return content

    def format_volume(self, volume):
        if volume >= 1_000_000_000: return f"{volume/1_000_000_000:.2f}B"
        if volume >= 1_000_000: return f"{volume/1_000_000:.2f}M"
        if volume >= 1_000: return f"{volume/1_000:.2f}K"
        return f"{volume:.2f}"

    def run(self):
        current_time = self.get_current_time_str()
        print(f"[{current_time}] 开始执行监控任务...")
        if not self.ENABLE_MACD_SCANNER:
            print(f"[{current_time}] MACD扫描功能已关闭，退出任务。")
            return
        instruments = self.get_perpetual_instruments()
        if not instruments:
            print(f"[{current_time}] 未能获取交易对列表，退出监控。")
            return
        
        all_opportunities = []
        max_workers = 5
        batch_size = 10 
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, len(instruments), batch_size):
                batch = instruments[i:i + batch_size]
                print(f"[{self.get_current_time_str()}] 正在处理批次 {i//batch_size + 1}/{(len(instruments) + batch_size - 1)//batch_size}...")
                results = executor.map(self.analyze_instrument_for_opportunities, batch)
                for result in results:
                    if result:
                        all_opportunities.append(result)
                        print(f"[{self.get_current_time_str()}] 发现信号: {result['inst_id']} ({result['type']})")
                if i + batch_size < len(instruments):
                    print(f"[{self.get_current_time_str()}] 批次处理完成，暂停2秒...")
                    time.sleep(2)

        if all_opportunities:
            actionable_opportunities = [
                opp for opp in all_opportunities if 'Watchlist' not in opp['type']
            ]
            if actionable_opportunities:
                title = f"🚨 发现 {len(actionable_opportunities)} 个核心交易机会!"
                content = self.create_opportunity_report(all_opportunities)
                self.send_notification(title, content)
                print(f"[{current_time}] 发现 {len(actionable_opportunities)} 个核心机会，已发送通知。")
            else:
                print(f"[{current_time}] 仅发现 {len(all_opportunities)} 个观察信号，本次不发送通知。")
        else:
            print(f"[{current_time}] 本次未发现任何符合条件的信号。")
        
        print(f"[{current_time}] 监控任务执行完毕。")

if __name__ == "__main__":
    monitor = OKXMonitor()
    monitor.run()
