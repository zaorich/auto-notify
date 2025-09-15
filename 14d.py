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
        self.server_jiang_key = os.environ.get('SERVER_JIANG_KEY') 
        
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
        return datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M:%S')

    def send_notification(self, title, content):
        """
        [增强版] 通过Server酱发送微信通知，并带有详细的调试日志。
        """
        # --- 新增的调试打印 ---
        print(f"[{self.get_current_time_str()}] 准备发送通知...")
        if not self.server_jiang_key:
            print(f"[{self.get_current_time_str()}] 错误：SERVER_JIANG_KEY 环境变量未设置或为空。请在GitHub Secrets中配置。")
            print(f"标题: {title}\n内容:\n{content}")
            return False
        else:
            # 打印部分KEY以供验证，但隐藏完整KEY保证安全
            masked_key = self.server_jiang_key[:5] + '...' + self.server_jiang_key[-4:]
            print(f"[{self.get_current_time_str()}] 使用的KEY: {masked_key}")
        # --- 调试打印结束 ---
            
        try:
            url = f"https://sctapi.ftqq.com/{self.server_jiang_key}.send"
            data = {'title': title, 'desp': content}
            response = requests.post(url, data=data, timeout=30)
            
            # --- 新增的调试打印 ---
            print(f"[{self.get_current_time_str()}] Server酱API响应状态码: {response.status_code}")
            print(f"[{self.get_current_time_str()}] Server酱API响应内容: {response.text}")
            # --- 调试打印结束 ---

            response.raise_for_status() # 如果状态码不是2xx，将抛出异常
            result = response.json()
            if result.get('code') == 0:
                print(f"[{self.get_current_time_str()}] 通知发送成功: {title}")
                return True
            else:
                print(f"[{self.get_current_time_str()}] 通知发送失败，Server酱返回错误: {result}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"[{self.get_current_time_str()}] 发送通知时发生网络请求异常: {e}")
            return False
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 发送通知时发生未知异常: {e}")
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
            else:
                print(f"[{self.get_current_time_str()}] 获取交易对失败: {data}")
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

    def check_pullback_opportunity(self, d1_macd, h1_macd):
        if len(d1_macd) < 2 or len(h1_macd) < 2: return False
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
        daily_ok = (d1_last['macd'] > 0 and d1_last['signal'] > 0 and d1_last['macd'] < d1_last['signal'] and d1_last['histogram'] < 0 and d1_prev['histogram'] < d1_last['histogram'])
        hourly_ok = (h1_last['macd'] > 0 and h1_last['signal'] > 0 and h1_last['macd'] > h1_last['signal'] and h1_prev['macd'] < h1_prev['signal'])
        return daily_ok and hourly_ok

    def check_trend_opportunity(self, d1_macd, h4_macd):
        if len(d1_macd) < 2 or len(h4_macd) < 2: return 'None'
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        daily_ok = ((d1_last['macd'] > 0 or d1_last['signal'] > 0) and (d1_prev['macd'] < 0 or d1_prev['signal'] < 0) and d1_last['macd'] > d1_last['signal'])
        if not daily_ok: return 'None'
        h4_last = h4_macd[-1]
        four_hour_ok = (h4_last['macd'] > 0 and h4_last['signal'] > 0 and h4_last['macd'] > h4_last['signal'])
        return 'Trend' if four_hour_ok else 'Watchlist'

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
            if self.check_pullback_opportunity(d1_macd, h1_macd):
                return {'inst_id': inst_id, 'type': 'Pullback', 'volume': daily_volume}
            trend_status = self.check_trend_opportunity(d1_macd, h4_macd)
            if trend_status in ['Trend', 'Watchlist']:
                return {'inst_id': inst_id, 'type': trend_status, 'volume': daily_volume}
            return None
        except Exception:
            return None

    def create_opportunity_report(self, opportunities):
        rank = {'Trend': 1, 'Pullback': 1, 'Watchlist': 2}
        opportunities.sort(key=lambda x: (rank[x['type']], -x['volume']))
        type_map = {'Pullback': '🟢 回调机会', 'Trend': '🔵 趋势机会', 'Watchlist': '🟡 趋势观察'}
        content = f"### 发现 {len(opportunities)} 个MACD交易机会\n\n"
        content += "| 交易对 | 机会类型 | 24H成交额 |\n|:---|:---|:---|\n"
        for opp in opportunities:
            inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
            opp_type = type_map.get(opp['type'], '未知')
            volume_str = self.format_volume(opp['volume'])
            content += f"| **{inst_name}** | {opp_type} | {volume_str} |\n"
        content += "\n---\n**策略说明:**\n- **回调机会**: 日线0上死叉回调 + 1小时0上刚金叉。\n- **趋势机会**: 日线刚上穿0轴金叉 + 4小时已在0上金叉。\n- **趋势观察**: 日线刚上穿0轴金叉，等待4小时金叉信号。\n"
        content += f"- **筛选条件**: 24小时成交额 > {self.format_volume(self.MACD_VOLUME_THRESHOLD)} USDT。"
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
                print(f"[{self.get_current_time_str()}] 正在处理批次 {i//batch_size + 1}/{(len(instruments) + batch_size - 1)//batch_size} ({len(batch)}个交易对)...")
                results = executor.map(self.analyze_instrument_for_opportunities, batch)
                for result in results:
                    if result:
                        all_opportunities.append(result)
                        print(f"[{self.get_current_time_str()}] 发现机会: {result['inst_id']} ({result['type']})")
                if i + batch_size < len(instruments):
                    print(f"[{self.get_current_time_str()}] 批次处理完成，暂停2秒...")
                    time.sleep(2)
        if all_opportunities:
            title = f"🚨 发现 {len(all_opportunities)} 个 MACD 交易机会!"
            content = self.create_opportunity_report(all_opportunities)
            self.send_notification(title, content)
        else:
            print(f"[{current_time}] 本次未发现符合条件的MACD交易机会。")
        print(f"[{current_time}] 监控任务执行完毕。")

if __name__ == "__main__":
    monitor = OKXMonitor()
    monitor.run()
