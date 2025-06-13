import requests
import json
import time
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import threading
import urllib.parse
from io import BytesIO
import base64
import pytz
from functools import lru_cache
from typing import List, Dict, Tuple, Optional, Union

class OKXVolumeMonitor:
    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.server_jiang_key = os.environ.get('SERVER_JIANG_KEY', 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.heartbeat_file = 'last_alert_time.txt'
        self.last_billion_pairs_file = 'last_billion_pairs.txt'
        self.heartbeat_interval = 4 * 60 * 60  # 4小时（秒）
        self.timezone = pytz.timezone('Asia/Shanghai')
        self.chart_group_size = 6
        self.request_delay = 0.2
        self.max_retries = 3
        
        # 爆量信息开关配置
        self.enable_volume_alerts = True
        self.volume_alert_daily_threshold = 50_000_000
        
        # 图表开关配置
        self.enable_bar_chart = True
        self.enable_trend_chart = True
        
        # 图表排除交易对配置
        self.excluded_pairs = ['BTC', 'ETH']
        
        # 新增：缓存配置
        self.cache = {}
        self.cache_timeout = 600  # 10分钟
        self.instrument_cache_key = "perpetual_instruments"
        
        # 异步请求配置
        self.semaphore = asyncio.Semaphore(5)  # 限制并发请求数
        
    def get_current_time_str(self):
        """获取当前UTC+8时间字符串"""
        return datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M:%S')
    
    def get_cached_data(self, key: str) -> Optional[any]:
        """从缓存获取数据"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            if time.time() - timestamp < self.cache_timeout:
                return data
        return None
    
    def set_cached_data(self, key: str, data: any) -> None:
        """设置缓存数据"""
        self.cache[key] = (data, time.time())
    
    def get_perpetual_instruments(self) -> List[Dict]:
        """获取永续合约交易对列表（带缓存）"""
        cached_data = self.get_cached_data(self.instrument_cache_key)
        if cached_data:
            print(f"[{self.get_current_time_str()}] 从缓存获取到 {len(cached_data)} 个活跃的USDT永续合约")
            return cached_data
        
        try:
            url = f"{self.base_url}/api/v5/public/instruments"
            params = {'instType': 'SWAP'}
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data['code'] == '0':
                instruments = data['data']
                # 过滤活跃的USDT永续合约
                active_instruments = [
                    inst for inst in instruments 
                    if inst['state'] == 'live' and 'USDT' in inst['instId']
                ]
                print(f"[{self.get_current_time_str()}] 获取到 {len(active_instruments)} 个活跃的USDT永续合约")
                
                # 缓存结果
                self.set_cached_data(self.instrument_cache_key, active_instruments)
                return active_instruments
            else:
                print(f"[{self.get_current_time_str()}] 获取交易对失败: {data}")
                return []
                
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 获取交易对时出错: {e}")
            return []
    
    def safe_request_with_retry(self, url: str, params: Optional[Dict] = None, timeout: int = 30) -> Optional[requests.Response]:
        """带重试机制的安全请求方法"""
        for attempt in range(self.max_retries):
            try:
                # 添加随机延迟，避免请求过于规律
                time.sleep(self.request_delay)
                
                response = self.session.get(url, params=params, timeout=timeout)
                
                if response.status_code == 429:
                    wait_time = (attempt + 1) * 2  # 指数退避：2s, 4s, 6s
                    print(f"[{self.get_current_time_str()}] 遇到429错误，等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    continue
                    
                response.raise_for_status()
                return response
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise e
                wait_time = (attempt + 1) * 1  # 1s, 2s, 3s
                print(f"[{self.get_current_time_str()}] 请求失败，{wait_time}秒后重试: {e}")
                time.sleep(wait_time)
        
        return None
    
    async def async_get_kline_data(self, session: aiohttp.ClientSession, inst_id: str, bar: str = '1H', limit: int = 20) -> List[List]:
        """异步获取K线数据"""
        async with self.semaphore:  # 限制并发请求数
            cache_key = f"kline_{inst_id}_{bar}_{limit}"
            cached_data = self.get_cached_data(cache_key)
            if cached_data:
                return cached_data
                
            try:
                url = f"{self.base_url}/api/v5/market/candles"
                params = {
                    'instId': inst_id,
                    'bar': bar,
                    'limit': limit
                }
                
                if bar == '1D':
                    params['utc'] = '8'
                
                # 添加请求延迟避免API限流
                await asyncio.sleep(self.request_delay)
                
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 429:
                        wait_time = 2  # 简单退避策略
                        print(f"[{self.get_current_time_str()}] 遇到429错误，等待{wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                        return await self.async_get_kline_data(session, inst_id, bar, limit)
                        
                    response.raise_for_status()
                    data = await response.json()
                    if data['code'] == '0':
                        # 缓存结果
                        self.set_cached_data(cache_key, data['data'])
                        return data['data']
                    else:
                        print(f"[{self.get_current_time_str()}] 获取{inst_id}的K线数据失败: {data}")
                        return []
                        
            except Exception as e:
                print(f"[{self.get_current_time_str()}] 获取{inst_id}的K线数据时出错: {e}")
                return []
    
    def calculate_volume_ratio(self, kline_data: List[List]) -> Tuple[Optional[float], Optional[float]]:
        """计算交易量倍数"""
        if len(kline_data) < 11:  # 需要至少11个数据点（当前+前10个用于MA10）
            return None, None
        
        # OKX K线数据格式: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        # volCcyQuote 是以计价货币计算的交易量（交易额）
        volumes = [float(candle[7]) for candle in kline_data]  # 使用交易额
        
        current_volume = volumes[0]  # 最新的交易量
        prev_volume = volumes[1] if len(volumes) > 1 else 0  # 前一个周期的交易量
        
        # 计算MA10（前10个周期的平均交易量，不包括当前周期）
        ma10_volumes = volumes[1:11] if len(volumes) >= 11 else volumes[1:]
        ma10_volume = np.mean(ma10_volumes) if ma10_volumes else 0
        
        # 计算倍数
        prev_ratio = current_volume / prev_volume if prev_volume > 0 else 0
        ma10_ratio = current_volume / ma10_volume if ma10_volume > 0 else 0
        
        return prev_ratio, ma10_ratio
    
    def calculate_volume_ratio_vectorized(self, kline_data: List[List]) -> Tuple[Optional[float], Optional[float]]:
        """使用pandas向量化计算交易量倍数"""
        if len(kline_data) < 11:
            return None, None
        
        # 转换为DataFrame
        df = pd.DataFrame(kline_data, columns=['ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
        volumes = df['volCcyQuote'].astype(float)
        
        current_volume = volumes.iloc[0]
        prev_volume = volumes.iloc[1] if len(volumes) > 1 else 0
        
        # 计算MA10
        ma10_volume = volumes.iloc[1:11].mean() if len(volumes) >= 11 else volumes.iloc[1:].mean()
        
        prev_ratio = current_volume / prev_volume if prev_volume > 0 else 0
        ma10_ratio = current_volume / ma10_volume if ma10_volume > 0 else 0
        
        return prev_ratio, ma10_ratio
    
    def get_daily_volumes_history(self, inst_id: str, days: int = 7) -> List[Dict]:
        """获取交易对过去N天的日交易额历史"""
        try:
            # 获取日K线数据
            daily_klines = self.get_kline_data(inst_id, '1Dutc', days)
            if daily_klines:
                # 返回每天的交易额列表，按时间从近到远排序
                daily_volumes = []
                for kline in daily_klines:
                    timestamp = int(kline[0]) / 1000  # 转换为秒
                    date = datetime.fromtimestamp(timestamp, self.timezone).strftime('%m-%d')
                    volume = float(kline[7])  # 交易额
                    daily_volumes.append({
                        'date': date,
                        'volume': volume
                    })
                return daily_volumes
            return []
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 获取{inst_id}历史日交易额时出错: {e}")
            return []
    
    def should_send_volume_alert(self, alert: Dict) -> bool:
        """检查是否应该发送爆量警报"""
        if not self.enable_volume_alerts:
            return False
        
        # 检查当天成交额是否超过阈值
        daily_volume = alert.get('daily_volume', 0)
        return daily_volume >= self.volume_alert_daily_threshold
    
    def check_volume_explosion_batch(self, instruments_batch: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """批量检查多个交易对的爆量情况"""
        alerts = []
        billion_volume_alerts = []
        
        # 减少并发数，避免429错误
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 提交所有任务
            future_to_inst = {
                executor.submit(self.check_single_instrument_volume, inst['instId']): inst['instId'] 
                for inst in instruments_batch
            }
            
            # 收集结果
            for future in future_to_inst:
                inst_id = future_to_inst[future]
                try:
                    inst_alerts, billion_alert = future.result(timeout=60)
                    
                    # 过滤爆量警报：只有通过阈值检查的才添加
                    if inst_alerts:
                        filtered_alerts = []
                        for alert in inst_alerts:
                            if self.should_send_volume_alert(alert):
                                filtered_alerts.append(alert)
                                print(f"[{self.get_current_time_str()}] 发现爆量(通过阈值): {inst_id} 当天成交额: {self.format_volume(alert['daily_volume'])}")
                            else:
                                print(f"[{self.get_current_time_str()}] 发现爆量(未达阈值): {inst_id} 当天成交额: {self.format_volume(alert.get('daily_volume', 0))} < {self.format_volume(self.volume_alert_daily_threshold)}")
                        
                        if filtered_alerts:
                            alerts.extend(filtered_alerts)
                    
                    if billion_alert:
                        billion_volume_alerts.append(billion_alert)
                        print(f"[{self.get_current_time_str()}] 发现过亿成交: {inst_id}")
                        
                except Exception as e:
                    print(f"[{self.get_current_time_str()}] 检查 {inst_id} 时出错: {e}")
                    continue
        
        return alerts, billion_volume_alerts
    
    async def check_volume_explosion_batch_async(self, instruments_batch: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """异步批量检查多个交易对的爆量情况"""
        alerts = []
        billion_volume_alerts = []
        
        async with aiohttp.ClientSession() as session:
            # 为每个交易对创建任务
            tasks = []
            for inst in instruments_batch:
                task = asyncio.create_task(self.check_single_instrument_volume_async(session, inst['instId']))
                tasks.append(task)
            
            # 收集结果
            results = await asyncio.gather(*tasks)
            
            for inst_alerts, billion_alert in results:
                # 过滤爆量警报
                if inst_alerts:
                    filtered_alerts = [alert for alert in inst_alerts if self.should_send_volume_alert(alert)]
                    if filtered_alerts:
                        alerts.extend(filtered_alerts)
                
                if billion_alert:
                    billion_volume_alerts.append(billion_alert)
        
        return alerts, billion_volume_alerts
    
    def get_daily_volume(self, inst_id: str) -> float:
        """获取交易对当天的交易额"""
        try:
            # 获取24小时的1小时K线数据
            daily_data = self.get_kline_data(inst_id, '1H', 24)
            if daily_data:
                # 计算当天总交易额（所有小时K线的交易额之和）
                total_volume = sum(float(candle[7]) for candle in daily_data)
                return total_volume
            return 0
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 获取{inst_id}当天交易额时出错: {e}")
            return 0
    
    def check_single_instrument_volume(self, inst_id: str) -> Tuple[List[Dict], Optional[Dict]]:
        """检查单个交易对是否出现爆量和过亿成交"""
        alerts = []
        billion_alert = None
        
        try:
            # 获取当天交易额（通过get_daily_volume方法，即24小时内1小时K线的volCcyQuote字段之和）
            daily_volume = self.get_daily_volume(inst_id)
            
            # 获取过去3天的交易额数据（用于表格显示）
            past_3days_volumes = self.get_daily_volumes_history(inst_id, 3)
            
            # 获取24小时K线数据计算涨跌幅
            daily_klines = self.get_kline_data(inst_id, '1H', 24)
            price_change_24h = 0
            if daily_klines and len(daily_klines) >= 24:
                current_price = float(daily_klines[0][4])  # 最新收盘价
                price_24h_ago = float(daily_klines[23][4])  # 24小时前收盘价
                if price_24h_ago > 0:
                    price_change_24h = (current_price - price_24h_ago) / price_24h_ago * 100
            
            # 检查是否过亿
            if daily_volume >= 100_000_000:  # 1亿USDT
                # 获取过去7天的日交易额历史
                daily_volumes_history = self.get_daily_volumes_history(inst_id, 7)
                billion_alert = {
                    'inst_id': inst_id,
                    'current_daily_volume': daily_volume,
                    'daily_volumes_history': daily_volumes_history,
                    'price_change_24h': price_change_24h  # 添加涨跌幅
                }
            
            # 检查1小时爆量
            hour_data = self.get_kline_data(inst_id, '1H', 20)
            if hour_data:
                prev_ratio, ma10_ratio = self.calculate_volume_ratio_vectorized(hour_data)
                if prev_ratio and ma10_ratio:
                    # 当前交易额来源：最新1小时K线的volCcyQuote字段（hour_data[0][7]）
                    current_volume = float(hour_data[0][7])
                    
                    # 小时爆量标准：10倍
                    if prev_ratio >= 10 or ma10_ratio >= 10:
                        alert_data = {
                            'inst_id': inst_id,
                            'timeframe': '1H',
                            'current_volume': current_volume,
                            'prev_ratio': prev_ratio if prev_ratio >= 10 else None,
                            'ma10_ratio': ma10_ratio if ma10_ratio >= 10 else None,
                            'daily_volume': daily_volume,
                            'past_3days_volumes': past_3days_volumes,
                            'price_change_24h': price_change_24h  # 添加涨跌幅
                        }
                        alerts.append(alert_data)
            
            # 检查4小时爆量
            four_hour_data = self.get_kline_data(inst_id, '4H', 20)
            if four_hour_data:
                prev_ratio, ma10_ratio = self.calculate_volume_ratio_vectorized(four_hour_data)
                if prev_ratio and ma10_ratio:
                    # 当前交易额来源：最新4小时K线的volCcyQuote字段（four_hour_data[0][7]）
                    current_volume = float(four_hour_data[0][7])
                    
                    # 4小时爆量标准：5倍  修改成4倍
                    if prev_ratio >= 4 or ma10_ratio >= 4:
                        alert_data = {
                            'inst_id': inst_id,
                            'timeframe': '4H',
                            'current_volume': current_volume,
                            'prev_ratio': prev_ratio if prev_ratio >= 4 else None,
                            'ma10_ratio': ma10_ratio if ma10_ratio >= 4 else None,
                            'daily_volume': daily_volume,
                            'past_3days_volumes': past_3days_volumes,
                            'price_change_24h': price_change_24h  # 添加涨跌幅
                        }
                        alerts.append(alert_data)
            
            return alerts, billion_alert
            
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 检查 {inst_id} 时出错: {e}")
            return [], None
    
    async def check_single_instrument_volume_async(self, session: aiohttp.ClientSession, inst_id: str) -> Tuple[List[Dict], Optional[Dict]]:
        """异步检查单个交易对是否出现爆量和过亿成交"""
        alerts = []
        billion_alert = None
        
        try:
            # 获取当天交易额
            daily_volume = self.get_daily_volume(inst_id)
            
            # 获取过去3天的交易额数据
            past_3days_volumes = self.get_daily_volumes_history(inst_id, 3)
            
            # 获取24小时K线数据计算涨跌幅
            daily_klines = await self.async_get_kline_data(session, inst_id, '1H', 24)
            price_change_24h = 0
            if daily_klines and len(daily_klines) >= 24:
                current_price = float(daily_klines[0][4])
                price_24h_ago = float(daily_klines[23][4])
                if price_24h_ago > 0:
                    price_change_24h = (current_price - price_24h_ago) / price_24h_ago * 100
            
            # 检查是否过亿
            if daily_volume >= 100_000_000:
                daily_volumes_history = await self.async_get_kline_data(session, inst_id, '1Dutc', 7)
                if daily_volumes_history:
                    formatted_history = []
                    for kline in daily_volumes_history:
                        timestamp = int(kline[0]) / 1000
                        date = datetime.fromtimestamp(timestamp, self.timezone).strftime('%m-%d')
                        volume = float(kline[7])
                        formatted_history.append({
                            'date': date,
                            'volume': volume
                        })
                    
                    billion_alert = {
                        'inst_id': inst_id,
                        'current_daily_volume': daily_volume,
                        'daily_volumes_history': formatted_history,
                        'price_change_24h': price_change_24h
                    }
            
            # 检查1小时爆量
            hour_data = await self.async_get_kline_data(session, inst_id, '1H', 20)
            if hour_data:
                prev_ratio, ma10_ratio = self.calculate_volume_ratio_vectorized(hour_data)
                if prev_ratio and ma10_ratio:
                    current_volume = float(hour_data[0][7])
                    
                    if prev_ratio >= 10 or ma10_ratio >= 10:
                        alert_data = {
                            'inst_id': inst_id,
                            'timeframe': '1H',
                            'current_volume': current_volume,
                            'prev_ratio': prev_ratio if prev_ratio >= 10 else None,
                            'ma10_ratio': ma10_ratio if ma10_ratio >= 10 else None,
                            'daily_volume': daily_volume,
                            'past_3days_volumes': past_3days_volumes,
                            'price_change_24h': price_change_24h
                        }
                        alerts.append(alert_data)
            
            # 检查4小时爆量
            four_hour_data = await self.async_get_kline_data(session, inst_id, '4H', 20)
            if four_hour_data:
                prev_ratio, ma10_ratio = self.calculate_volume_ratio_vectorized(four_hour_data)
                if prev_ratio and ma10_ratio:
                    current_volume = float(four_hour_data[0][7])
                    
                    if prev_ratio >= 4 or ma10_ratio >= 4:
                        alert_data = {
                            'inst_id': inst_id,
                            'timeframe': '4H',
                            'current_volume': current_volume,
                            'prev_ratio': prev_ratio if prev_ratio >= 4 else None,
                            'ma10_ratio': ma10_ratio if ma10_ratio >= 4 else None,
                            'daily_volume': daily_volume,
                            'past_3days_volumes': past_3days_volumes,
                            'price_change_24h': price_change_24h
                        }
                        alerts.append(alert_data)
            
            return alerts, billion_alert
            
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 检查 {inst_id} 时出错: {e}")
            return [], None
    
    def get_last_alert_time(self) -> float:
        """获取上次发送爆量警报的时间"""
        try:
            if os.path.exists(self.heartbeat_file):
                with open(self.heartbeat_file, 'r') as f:
                    timestamp = float(f.read().strip())
                    return timestamp
            return 0
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 读取上次警报时间失败: {e}")
            return 0
    
    def update_last_alert_time(self) -> None:
        """更新上次发送爆量警报的时间"""
        try:
            with open(self.heartbeat_file, 'w') as f:
                f.write(str(time.time()))
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 更新上次警报时间失败: {e}")
    
    def should_send_heartbeat(self) -> bool:
        """检查是否需要发送心跳消息"""
        last_alert_time = self.get_last_alert_time()
        current_time = time.time()
        time_since_last_alert = current_time - last_alert_time
        
        return time_since_last_alert >= self.heartbeat_interval
    
    def format_volume(self, volume: float) -> str:
        """格式化交易额显示"""
        if volume >= 1_000_000_000:  # 10亿
            return f"{volume/1_000_000_000:.2f}B"
        elif volume >= 1_000_000:  # 100万
            return f"{volume/1_000_000:.0f}M"
        elif volume >= 1_000:  # 1千
            return f"{volume/1_000:.0f}K"
        else:
            return f"{volume:.0f}"
    
    def generate_chart_url_quickchart(self, billion_alerts: List[Dict]) -> List[str]:
        """使用QuickChart生成图表URL"""
        if not billion_alerts or len(billion_alerts) == 0:
            return []
        
        try:
            # 按成交额分组：10亿以上、3-10亿、1-3亿
            above_10b = []
            between_3_10b = []
            between_1_3b = []
            
            for alert in billion_alerts:
                volume = alert['current_daily_volume']
                if volume >= 10_000_000_000:
                    above_10b.append(alert)
                elif volume >= 3_000_000_000:
                    between_3_10b.append(alert)
                else:
                    between_1_3b.append(alert)
            
            chart_urls = []
            colors = [
                '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0',
                '#9966FF', '#FF9F40', '#FF6384', '#C9CBCF',
                '#FF5733', '#33FF57', '#3357FF', '#FF33A1',
                '#A133FF', '#33FFF5', '#F5FF33', '#FF8C33'
            ]
            
            # 生成10亿以上的图表
            if above_10b:
                above_10b.sort(key=lambda x: x['current_daily_volume'], reverse=True)
                
                labels = []
                current_data = []
                
                for i, alert in enumerate(above_10b):
                    inst_name = alert['inst_id'].replace('-SWAP', '').replace('-USDT', '')
                    labels.append(inst_name)
                    current_data.append(round(alert['current_daily_volume'] / 1_000_000_000, 2))
                
                chart_config = {
                    "type": "bar",
                    "data": {
                        "labels": labels,
                        "datasets": [{
                            "label": "当天成交额 (十亿USDT)",
                            "data": current_data,
                            "backgroundColor": [colors[i % len(colors)] for i in range(len(above_10b))],
                            "borderColor": [colors[i % len(colors)] for i in range(len(above_10b))],
                            "borderWidth": 1
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False,
                        "plugins": {
                            "title": {
                                "display": True,
                                "text": "OKX 过亿成交额排行 - 10亿以上",
                                "font": {
                                    "size": 16,
                                    "weight": "bold"
                                }
                            },
                            "legend": {
                                "display": True,
                                "position": "top"
                            }
                        },
                        "scales": {
                            "y": {
                                "beginAtZero": True,
                                "title": {
                                    "display": True,
                                    "text": "成交额 (十亿USDT)"
                                }
                            },
                            "x": {
                                "title": {
                                    "display": True,
                                    "text": "交易对"
                                }
                            }
                        }
                    }
                }
                
                chart_json = json.dumps(chart_config)
                encoded_chart = urllib.parse.quote(chart_json)
                chart_url = f"https://quickchart.io/chart?c={encoded_chart}&width=1200&height=400&format=png"
                chart_urls.append(chart_url)
            
            # 生成3-10亿的图表
            if between_3_10b:
                between_3_10b.sort(key=lambda x: x['current_daily_volume'], reverse=True)
                
                labels = []
                current_data = []
                
                for i, alert in enumerate(between_3_10b):
                    inst_name = alert['inst_id'].replace('-SWAP', '').replace('-USDT', '')
                    labels.append(inst_name)
                    current_data.append(round(alert['current_daily_volume'] / 1_000_000_000, 2))
                
                chart_config = {
                    "type": "bar",
                    "data": {
                        "labels": labels,
                        "datasets": [{
                            "label": "当天成交额 (十亿USDT)",
                            "data": current_data,
                            "backgroundColor": [colors[i % len(colors)] for i in range(len(between_3_10b))],
                            "borderColor": [colors[i % len(colors)] for i in range(len(between_3_10b))],
                            "borderWidth": 1
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False,
                        "plugins": {
                            "title": {
                                "display": True,
                                "text": "OKX 过亿成交额排行 - 3-10亿区间",
                                "font": {
                                    "size": 16,
                                    "weight": "bold"
                                }
                            },
                            "legend": {
                                "display": True,
                                "position": "top"
                            }
                        },
                        "scales": {
                            "y": {
                                "beginAtZero": True,
                                "title": {
                                    "display": True,
                                    "text": "成交额 (十亿USDT)"
                                }
                            },
                            "x": {
                                "title": {
                                    "display": True,
                                    "text": "交易对"
                                }
                            }
                        }
                    }
                }
                
                chart_json = json.dumps(chart_config)
                encoded_chart = urllib.parse.quote(chart_json)
                chart_url = f"https://quickchart.io/chart?c={encoded_chart}&width=1200&height=400&format=png"
                chart_urls.append(chart_url)
            
            # 生成1-3亿的图表
            if between_1_3b:
                between_1_3b.sort(key=lambda x: x['current_daily_volume'], reverse=True)
                
                labels = []
                current_data = []
                
                for i, alert in enumerate(between_1_3b):
                    inst_name = alert['inst_id'].replace('-SWAP', '').replace('-USDT', '')
                    labels.append(inst_name)
                    current_data.append(round(alert['current_daily_volume'] / 1_000_000, 1))
                
                chart_config = {
                    "type": "bar",
                    "data": {
                        "labels": labels,
                        "datasets": [{
                            "label": "当天成交额 (百万USDT)",
                            "data": current_data,
                            "backgroundColor": [colors[i % len(colors)] for i in range(len(between_1_3b))],
                            "borderColor": [colors[i % len(colors)] for i in range(len(between_1_3b))],
                            "borderWidth": 1
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False,
                        "plugins": {
                            "title": {
                                "display": True,
                                "text": "OKX 过亿成交额排行 - 1-3亿区间",
                                "font": {
                                    "size": 16,
                                    "weight": "bold"
                                }
                            },
                            "legend": {
                                "display": True,
                                "position": "top"
                            }
                        },
                        "scales": {
                            "y": {
                                "beginAtZero": True,
                                "title": {
                                    "display": True,
                                    "text": "成交额 (百万USDT)"
                                }
                            },
                            "x": {
                                "title": {
                                    "display": True,
                                    "text": "交易对"
                                }
                            }
                        }
                    }
                }
                
                chart_json = json.dumps(chart_config)
                encoded_chart = urllib.parse.quote(chart_json)
                chart_url = f"https://quickchart.io/chart?c={encoded_chart}&width=1200&height=400&format=png"
                chart_urls.append(chart_url)
            
            print(f"[{self.get_current_time_str()}] 生成柱状图URL成功: 10亿以上 {len(above_10b)} 个，3-10亿 {len(between_3_10b)} 个，1-3亿 {len(between_1_3b)} 个")
            return chart_urls
            
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 生成图表URL时出错: {e}")
            return []

    def generate_trend_chart_urls(self, billion_alerts: List[Dict]) -> List[str]:
        """生成多个趋势图表URL"""
        if not billion_alerts or len(billion_alerts) == 0:
            return []
        
        try:
            # 过滤掉指定的交易对
            filtered_alerts = []
            for alert in billion_alerts:
                inst_name = alert['inst_id'].replace('-SWAP', '').replace('-USDT', '')
                if inst_name not in self.excluded_pairs:
                    filtered_alerts.append(alert)
            
            if not filtered_alerts:
                print(f"[{self.get_current_time_str()}] 过滤{'/'.join(self.excluded_pairs)}后，没有交易对可显示趋势图")
                return []
            
            # 获取所有可用的日期
            all_dates = set()
            for alert in filtered_alerts:
                if alert['daily_volumes_history']:
                    for vol_data in alert['daily_volumes_history']:
                        all_dates.add(vol_data['date'])
            
            # 按日期排序
            sorted_dates = sorted(list(all_dates))[-7:]  # 最近7天
            
            # 按每N个币种分组
            chart_urls = []
            colors = [
                '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                '#FF9F40', '#FF6384', '#C9CBCF', '#FF5733', '#33FF57',
                '#3357FF', '#FF33A1', '#A133FF', '#33FFF5', '#F5FF33',
                '#FF8C33', '#8C33FF', '#33FF8C', '#FF3333', '#3333FF'
            ]
            
            # 每N个币种生成一个图表
            for group_index in range(0, len(filtered_alerts), self.chart_group_size):
                group = filtered_alerts[group_index:group_index + self.chart_group_size]
                datasets = []
                
                # 为当前组的每个交易对准备数据
                for i, alert in enumerate(group):
                    inst_name = alert['inst_id'].replace('-SWAP', '').replace('-USDT', '')
                    data = []
                    
                    # 创建日期到成交额的映射
                    volume_map = {}
                    if alert['daily_volumes_history']:
                        for vol_data in alert['daily_volumes_history']:
                            volume_map[vol_data['date']] = vol_data['volume']
                    
                    # 按排序后的日期填充数据
                    for date in sorted_dates:
                        volume = volume_map.get(date, 0)
                        data.append(round(volume / 1_000_000, 1))  # 转换为百万
                    
                    datasets.append({
                        "label": inst_name,
                        "data": data,
                        "borderColor": colors[i % len(colors)],
                        "backgroundColor": colors[i % len(colors)] + "20",
                        "fill": False,
                        "tension": 0.4
                    })
                
                excluded_text = f" (排除{'/'.join(self.excluded_pairs)})" if self.excluded_pairs else ""
                chart_config = {
                    "type": "line",
                    "data": {
                        "labels": sorted_dates,
                        "datasets": datasets
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False,
                        "plugins": {
                            "title": {
                                "display": True,
                                "text": f"OKX 成交额趋势对比 第{group_index//self.chart_group_size + 1}组{excluded_text}",
                                "font": {
                                    "size": 16,
                                    "weight": "bold"
                                }
                            },
                            "legend": {
                                "display": True,
                                "position": "top"
                            }
                        },
                        "scales": {
                            "y": {
                                "beginAtZero": True,
                                "title": {
                                    "display": True,
                                    "text": "成交额 (百万USDT)"
                                }
                            },
                            "x": {
                                "title": {
                                    "display": True,
                                    "text": "日期"
                                }
                            }
                        }
                    }
                }
                
                # 添加1亿USDT基准线数据到datasets中
                baseline_data = [100] * len(sorted_dates)  # 100百万 = 1亿
                datasets.append({
                    "label": "1亿USDT基准线",
                    "data": baseline_data,
                    "borderColor": "#ff0000",
                    "backgroundColor": "rgba(255, 0, 0, 0.1)",
                    "borderWidth": 2,
                    "borderDash": [5, 5],
                    "fill": False,
                    "pointRadius": 0,
                    "pointHoverRadius": 0
                })
                
                chart_json = json.dumps(chart_config)
                encoded_chart = urllib.parse.quote(chart_json)
                chart_url = f"https://quickchart.io/chart?c={encoded_chart}&width=1200&height=400&format=png"
                chart_urls.append(chart_url)
            
            excluded_pairs_text = '/'.join(self.excluded_pairs)
            print(f"[{self.get_current_time_str()}] 生成{len(chart_urls)}个趋势图表URL，每{self.chart_group_size}个币种一组，总共包含 {len(filtered_alerts)} 个交易对（已排除{excluded_pairs_text}）")
            return chart_urls
            
        except Exception as e:
            print(f"[{self.get_current_time_str()}] 生成趋势图表URL时出错: {e}")
            return []
    
    def create_billion_volume_table(self, billion_alerts: List[Dict]) -> str:
        """创建过亿成交额的表格格式消息"""
        if not billion_alerts:
            return ""
        
        # 按当天交易额从高到低排序
        billion_alerts.sort(key=lambda x: x['current_daily_volume'], reverse=True)
        
        content = "## 💰 日成交过亿信号\n\n"
        
        chart_urls = []
        trend_chart_urls = []
        
        if self.enable_bar_chart:
            chart_urls = self.generate_chart_url_quickchart(billion_alerts)
            print(f"[{self.get_current_time_str()}] 柱状图开关已开启，生成柱状图")
        else:
            print(f"[{self.get_current_time_str()}] 柱状图开关已关闭，跳过柱状图生成")
        
        # 添加图表
        if self.enable_bar_chart and chart_urls:
            content += f"### 📊 成交额排行图\n"
            for i, chart_url in enumerate(chart_urls):
                if i == 0:
                    content += f"![成交额排行-10亿以上]({chart_url})\n\n"
                elif i == 1:
                    content += f"![成交额排行-3到10亿]({chart_url})\n\n"
                elif i == 2:
                    content += f"![成交额排行-1到3亿]({chart_url})\n\n"
        
        if self.enable_trend_chart:
            trend_chart_urls = self.generate_trend_chart_urls(billion_alerts)
            print(f"[{self.get_current_time_str()}] 趋势图开关已开启，生成趋势图")
        else:
            print(f"[{self.get_current_time_str()}] 趋势图开关已关闭，跳过趋势图生成")
        
        if self.enable_trend_chart and trend_chart_urls:
            content += f"### 📈 成交额趋势图\n"
            for i, trend_url in enumerate(trend_chart_urls):
                content += f"![成交额趋势第{i+1}组]({trend_url})\n\n"
        
        # 构建表头
        header = "### 📋 详细数据表格\n\n"
        header += "| 交易对 | 当天成交额 | 24H涨跌幅 |"
        separator = "|--------|------------|-----------|"
        
        # 获取最多的历史天数
        max_history_days = 0
        for alert in billion_alerts:
            if alert['daily_volumes_history']:
                max_history_days = max(max_history_days, len(alert['daily_volumes_history']) - 1)
        
        # 添加历史日期的表头
        for i in range(1, min(max_history_days + 1, 7)):
            if billion_alerts[0]['daily_volumes_history'] and len(billion_alerts[0]['daily_volumes_history']) > i:
                date = billion_alerts[0]['daily_volumes_history'][i]['date']
                header += f" {date} |"
                separator += "--------|"
        
        content += header + "\n"
        content += separator + "\n"
        
        # 填充数据
        for alert in billion_alerts:
            inst_id = alert['inst_id']
            current_vol = self.format_volume(alert['current_daily_volume'])
            price_change = alert.get('price_change_24h', 0)
            
            # 格式化涨跌幅显示
            if price_change > 0:
                price_change_str = f"📈+{price_change:.2f}%"
            elif price_change < 0:
                price_change_str = f"📉{price_change:.2f}%"
            else:
                price_change_str = "➖0.00%"
            
            row = f"| {inst_id} | **{current_vol}** | {price_change_str} |"
            
            # 添加历史数据
            history = alert['daily_volumes_history']
            for i in range(1, min(max_history_days + 1, 7)):
                if history and len(history) > i:
                    hist_vol = self.format_volume(history[i]['volume'])
                    row += f" {hist_vol} |"
                else:
                    row += " - |"
            
            content += row + "\n"
        
        content += "\n"
        return content
    
    def create_alert_table(self, alerts: List[Dict]) -> str:
        """创建爆量警报的表格格式消息"""
        if not alerts:
            return ""
        
        # 按时间框架分组
        hour_alerts = [alert for alert in alerts if alert['timeframe'] == '1H']
        four_hour_alerts = [alert for alert in alerts if alert['timeframe'] == '4H']
        
        # 按当前交易额从高到低排序
        hour_alerts.sort(key=lambda x: x['current_volume'], reverse=True)
        four_hour_alerts.sort(key=lambda x: x['current_volume'], reverse=True)
        
        content = ""
        
        if hour_alerts:
            content += "## 🔥 1小时爆量信号\n\n"
            content += "| 交易对 | 当前交易额 | 24H涨跌幅 | 相比上期 | 相比MA10 | 当天总额 | 昨天 | 前天 | 3天前 |\n"
            content += "|--------|------------|-----------|----------|----------|----------|------|------|------|\n"
            
            for alert in hour_alerts:
                inst_id = alert['inst_id']
                current_vol = self.format_volume(alert['current_volume'])
                daily_vol = self.format_volume(alert['daily_volume'])
                price_change = alert.get('price_change_24h', 0)
                
                # 格式化涨跌幅显示
                if price_change > 0:
                    price_change_str = f"📈+{price_change:.2f}%"
                elif price_change < 0:
                    price_change_str = f"📉{price_change:.2f}%"
                else:
                    price_change_str = "➖0.00%"
                
                prev_ratio_str = f"{alert['prev_ratio']:.1f}x 📈" if alert['prev_ratio'] else "-"
                ma10_ratio_str = f"{alert['ma10_ratio']:.1f}x 📈" if alert['ma10_ratio'] else "-"
                
                # 获取过去3天的交易额数据
                past_volumes = alert.get('past_3days_volumes', [])
                day1_vol = self.format_volume(past_volumes[0]['volume']) if len(past_volumes) > 0 else "-"
                day2_vol = self.format_volume(past_volumes[1]['volume']) if len(past_volumes) > 1 else "-"
                day3_vol = self.format_volume(past_volumes[2]['volume']) if len(past_volumes) > 2 else "-"
                
                content += f"| {inst_id} | {current_vol} | {price_change_str} | {prev_ratio_str} | {ma10_ratio_str} | {daily_vol} | {day1_vol} | {day2_vol} | {day3_vol} |\n"
            
            content += "\n"
        
        if four_hour_alerts:
            content += "## 🚀 4小时爆量信号\n\n"
            content += "| 交易对 | 当前交易额 | 24H涨跌幅 | 相比上期 | 相比MA10 | 当天总额 | 昨天 | 前天 | 3天前 |\n"
            content += "|--------|------------|-----------|----------|----------|----------|------|------|------|\n"
            
            for alert in four_hour_alerts:
                inst_id = alert['inst_id']
                current_vol = self.format_volume(alert['current_volume'])
                daily_vol = self.format_volume(alert['daily_volume'])
                price_change = alert.get('price_change_24h', 0)
                
                # 格式化涨跌幅显示
                if price_change > 0:
                    price_change_str = f"📈+{price_change:.2f}%"
                elif price_change < 0:
                    price_change_str = f"📉{price_change:.2f}%"
                else:
                    price_change_str = "➖0.00%"
                
                prev_ratio_str = f"{alert['prev_ratio']:.1f}x 📈" if alert['prev_ratio'] else "-"
                ma10_ratio_str = f"{alert['ma10_ratio']:.1f}x 📈" if alert['ma10_ratio'] else "-"
                
                # 获取过去3天的交易额数据
                past_volumes = alert.get('past_3days_volumes', [])
                day1_vol = self.format_volume(past_volumes[0]['volume']) if len(past_volumes) > 0 else "-"
                day2_vol = self.format_volume(past_volumes[1]['volume']) if len(past_volumes) > 1 else "-"
                day3_vol = self.format_volume(past_volumes[2]['volume']) if len(past_volumes) > 2 else "-"
                
                content += f"| {inst_id} | {current_vol} | {price_change_str} | {prev_ratio_str} | {ma10_ratio_str} | {daily_vol} | {day1_vol} | {day2_vol} | {day3_vol} |\n"
            
            content += "\n"
        
        return content
    
    def send_heartbeat_notification(self, monitored_count: int) -> bool:
        """发送心跳监测消息"""
        current_time = self.get_current_time_str()
