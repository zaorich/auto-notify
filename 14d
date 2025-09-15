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
        self.server_jiang_key = os.environ.get('SERVER_JIANG_KEY', 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e') # 请替换为你的KEY
        
        # --- 功能开关 ---
        self.ENABLE_MACD_SCANNER = True      # 【新功能】是否开启MACD机会扫描
        self.ENABLE_VOLUME_ALERTS = False    # 是否开启原有的成交量激增警报
        
        # --- MACD扫描器配置 ---
        self.MACD_VOLUME_THRESHOLD = 10_000_000  # MACD机会的最低日成交额阈值 (1000万USDT)

        # --- 通用配置 ---
        self.session = self._create_session()
        self.timezone = pytz.timezone('Asia/Shanghai')
        self.heartbeat_file = 'last_alert_time.txt'
        self.heartbeat_interval = 4 * 60 * 60  # 4小时

    def _create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        return session

    def get_current_time_str(self):
        return datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M:%S')

    def send_notification(self, title, content):
        """通过Server酱发送微信通知"""
        if not self.server_jiang_key or 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e' in self.server_jiang_key:
            print(f"[{self.get_current_time_str()}] 未配置Server酱KEY，通知将打印到控制台。")
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
        """获取所有活跃的USDT永续合约"""
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
        """获取K线数据"""
        try:
            url = f"{self.base_url}/api/v5/market/candles"
            params = {'instId': inst_id, 'bar': bar, 'limit': limit}
            response = self.session.get(url, params=params, timeout=15)
            # 增加对429错误的处理
            if response.status_code == 429:
                print(f"[{self.get_current_time_str()}] 请求过于频繁 (429)，等待5秒后重试...")
                time.sleep(5)
                response = self.session.get(url, params=params, timeout=15)

            response.raise_for_status()
            data = response.json()
            if data['code'] == '0' and data['data']:
                # API返回的数据是倒序的，第一条是最新，将其反转为正序
                return data['data'][::-1]
            return []
        except Exception as e:
            # print(f"[{self.get_current_time_str()}] 获取{inst_id} {bar} K线数据时出错: {e}")
            return []

    # --- 新增MACD计算和策略分析模块 ---
    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        """使用pandas计算MACD"""
        if len(prices) < slow:
            return []
        prices_series = pd.Series(prices)
        ema_fast = prices_series.ewm(span=fast, adjust=False).mean()
        ema_slow = prices_series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        macd_data = []
        for i in range(len(prices)):
            macd_data.append({
                'macd': macd_line.iloc[i],
                'signal': signal_line.iloc[i],
                'histogram': histogram.iloc[i]
            })
        return macd_data

    def check_pullback_opportunity(self, d1_macd, h1_macd):
        """策略1: 检查日线回调机会"""
        if len(d1_macd) < 2 or len(h1_macd) < 2: return False
        
        # 日线条件
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        daily_ok = (d1_last['macd'] > 0 and d1_last['signal'] > 0 and
                    d1_last['macd'] < d1_last['signal'] and # 死叉
                    d1_last['histogram'] < 0 and d1_prev['histogram'] < d1_last['histogram']) # 柱子收缩

        # 1小时条件
        h1_last, h1_prev = h1_macd[-1], h1_macd[-2]
        hourly_ok = (h1_last['macd'] > 0 and h1_last['signal'] > 0 and
                     h1_last['macd'] > h1_last['signal'] and # 金叉
                     h1_prev['macd'] < h1_prev['signal'])   # 刚刚金叉
        
        return daily_ok and hourly_ok

    def check_trend_opportunity(self, d1_macd, h4_macd):
        """策略2: 检查日线趋势机会，返回 'Trend', 'Watchlist', 或 'None'"""
        if len(d1_macd) < 2 or len(h4_macd) < 2: return 'None'

        # 日线条件
        d1_last, d1_prev = d1_macd[-1], d1_macd[-2]
        daily_ok = ((d1_last['macd'] > 0 or d1_last['signal'] > 0) and # 当前在0轴上
                    (d1_prev['macd'] < 0 or d1_prev['signal'] < 0) and # 之前在0轴下
                    d1_last['macd'] > d1_last['signal'])              # 且为金叉

        if not daily_ok:
            return 'None'
            
        # 4小时条件
        h4_last = h4_macd[-1]
        four_hour_ok = (h4_last['macd'] > 0 and h4_last['signal'] > 0 and
                        h4_last['macd'] > h4_last['signal'])

        if four_hour_ok:
            return 'Trend'
        else:
            return 'Watchlist'

    def analyze_instrument_for_opportunities(self, inst_id):
        """对单个交易对进行完整的MACD策略分析"""
        try:
            # 1. 获取1H K线并计算日成交额，进行初步过滤
            h1_klines = self.get_kline_data(inst_id, '1H', 24)
            if len(h1_klines) < 24: return None
            
            # volCcyQuote在索引7
            daily_volume = sum(float(kline[7]) for kline in h1_klines)
            if daily_volume < self.MACD_VOLUME_THRESHOLD:
                return None

            # 2. 获取日线和4小时K线
            d1_klines = self.get_kline_data(inst_id, '1D', 100)
            h4_klines = self.get_kline_data(inst_id, '4H', 100)
            if not d1_klines or not h4_klines: return None

            # 3. 提取收盘价并计算MACD
            d1_closes = [float(k[4]) for k in d1_klines]
            h4_closes = [float(k[4]) for k in h4_klines]
            h1_closes = [float(k[4]) for k in h1_klines]

            d1_macd = self.calculate_macd(d1_closes)
            h4_macd = self.calculate_macd(h4_closes)
            h1_macd = self.calculate_macd(h1_closes)

            # 4. 应用策略
            if self.check_pullback_opportunity(d1_macd, h1_macd):
                return {'inst_id': inst_id, 'type': 'Pullback', 'volume': daily_volume}

            trend_status = self.check_trend_opportunity(d1_macd, h4_macd)
            if trend_status in ['Trend', 'Watchlist']:
                return {'inst_id': inst_id, 'type': trend_status, 'volume': daily_volume}
                
            return None
        except Exception as e:
            # print(f"[{self.get_current_time_str()}] 分析 {inst_id} 时出错: {e}")
            return None

    def create_opportunity_report(self, opportunities):
        """创建交易机会的Markdown报告"""
        # 定义排序优先级
        rank = {'Trend': 1, 'Pullback': 1, 'Watchlist': 2}
        opportunities.sort(key=lambda x: (rank[x['type']], -x['volume']))
        
        type_map = {
            'Pullback': '🟢 回调机会',
            'Trend': '🔵 趋势机会',
            'Watchlist': '🟡 趋势观察'
        }
        
        content = f"### 发现 {len(opportunities)} 个MACD交易机会\n\n"
        content += "| 交易对 | 机会类型 | 24H成交额 |\n"
        content += "|:---|:---|:---|\n"

        for opp in opportunities:
            inst_name = opp['inst_id'].replace('-USDT-SWAP', '')
            opp_type = type_map.get(opp['type'], '未知')
            volume_str = self.format_volume(opp['volume'])
            content += f"| **{inst_name}** | {opp_type} | {volume_str} |\n"
        
        content += "\n---\n"
        content += "**策略说明:**\n"
        content += "- **回调机会**: 日线0上死叉回调 + 1小时0上刚金叉。\n"
        content += "- **趋势机会**: 日线刚上穿0轴金叉 + 4小时已在0上金叉。\n"
        content += "- **趋势观察**: 日线刚上穿0轴金叉，等待4小时金叉信号。\n"
        content += f"- **筛选条件**: 24小时成交额 > {self.format_volume(self.MACD_VOLUME_THRESHOLD)} USDT。"
        
        return content

    def format_volume(self, volume):
        """格式化交易额显示"""
        if volume >= 1_000_000_000: return f"{volume/1_000_000_000:.2f}B"
        if volume >= 1_000_000: return f"{volume/1_000_000:.2f}M"
        if volume >= 1_000: return f"{volume/1_000:.2f}K"
        return f"{volume:.2f}"

    def run_monitor(self):
        """运行监控主程序"""
        current_time = self.get_current_time_str()
        print(f"[{current_time}] 开始执行监控任务...")
        
        if not self.ENABLE_MACD_SCANNER and not self.ENABLE_VOLUME_ALERTS:
            print(f"[{current_time}] 所有功能均已关闭，退出任务。")
            return

        instruments = self.get_perpetual_instruments()
        if not instruments:
            print(f"[{current_time}] 未能获取交易对列表，退出监控。")
            return

        all_opportunities = []

        if self.ENABLE_MACD_SCANNER:
            print(f"[{current_time}] MACD机会扫描已开启，开始分析 {len(instruments)} 个交易对...")
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(self.analyze_instrument_for_opportunities, inst_id) for inst_id in instruments]
                for future in futures:
                    result = future.result()
                    if result:
                        all_opportunities.append(result)
                        print(f"[{self.get_current_time_str()}] 发现机会: {result['inst_id']} ({result['type']})")

            if all_opportunities:
                title = f"🚨 发现 {len(all_opportunities)} 个 MACD 交易机会!"
                content = self.create_opportunity_report(all_opportunities)
                self.send_notification(title, content)
                self.update_last_alert_time() # 发送后更新时间
            else:
                print(f"[{current_time}] 未发现符合条件的MACD交易机会。")
        
        # (原有的爆量监控逻辑可以放在这里，如果需要同时运行)
        # ...

        # 如果本次没有任何警报，检查是否需要发送心跳
        if not all_opportunities: # (以及没有其他警报)
            if self.should_send_heartbeat():
                print(f"[{current_time}] 长时间无信号，发送心跳消息...")
                self.send_heartbeat_notification(len(instruments))
                self.update_last_alert_time() # 发送心跳后也更新时间

        print(f"[{current_time}] 监控任务执行完毕。")

    # --- 心跳功能 ---
    def get_last_alert_time(self):
        try:
            if os.path.exists(self.heartbeat_file):
                with open(self.heartbeat_file, 'r') as f:
                    return float(f.read().strip())
            return 0
        except Exception:
            return 0

    def update_last_alert_time(self):
        try:
            with open(self.heartbeat_file, 'w') as f:
                f.write(str(time.time()))
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 更新上次警报时间失败: {e}")

    def should_send_heartbeat(self):
        return time.time() - self.get_last_alert_time() >= self.heartbeat_interval

    def send_heartbeat_notification(self, monitored_count):
        title = "OKX监控系统心跳 💓"
        content = f"监控系统正常运行中...\n\n"
        content += f"- **监控时间**: {self.get_current_time_str()}\n"
        content += f"- **监控交易对**: {monitored_count} 个\n"
        content += f"- **MACD扫描**: {'开启' if self.ENABLE_MACD_SCANNER else '关闭'}\n"
        content += f"- **爆量监控**: {'开启' if self.ENABLE_VOLUME_ALERTS else '关闭'}\n"
        self.send_notification(title, content)


if __name__ == "__main__":
    monitor = OKXMonitor()
    monitor.run_monitor()
