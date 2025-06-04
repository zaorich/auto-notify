#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

class OKXVolumeMonitor:
    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.server_jiang_key = os.environ.get('SERVER_JIANG_KEY', 'SCT281228TBF1BQU3KUJ4vLRkykhzIE80e')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.heartbeat_file = 'last_alert_time.txt'
        self.heartbeat_interval = 4 * 60 * 60  # 4小时（秒）
        
    def get_perpetual_instruments(self):
        """获取永续合约交易对列表"""
        try:
            url = f"{self.base_url}/api/v5/public/instruments"
            params = {
                'instType': 'SWAP'  # 永续合约
            }
            
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
                print(f"获取到 {len(active_instruments)} 个活跃的USDT永续合约")
                return active_instruments
            else:
                print(f"获取交易对失败: {data}")
                return []
                
        except Exception as e:
            print(f"获取交易对时出错: {e}")
            return []
    
    def get_kline_data(self, inst_id, bar='1H', limit=20):
        """获取K线数据"""
        try:
            url = f"{self.base_url}/api/v5/market/candles"
            params = {
                'instId': inst_id,
                'bar': bar,
                'limit': limit
            }
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data['code'] == '0':
                return data['data']
            else:
                print(f"获取{inst_id}的K线数据失败: {data}")
                return []
                
        except Exception as e:
            print(f"获取{inst_id}的K线数据时出错: {e}")
            return []
    
    def calculate_volume_ratio(self, kline_data):
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
    
    def get_daily_volumes_history(self, inst_id, days=7):
        """获取交易对过去N天的日交易额历史"""
        try:
            # 获取日K线数据
            daily_klines = self.get_kline_data(inst_id, '1D', days)
            if daily_klines:
                # 返回每天的交易额列表，按时间从近到远排序
                daily_volumes = []
                for kline in daily_klines:
                    timestamp = int(kline[0]) / 1000  # 转换为秒
                    date = datetime.fromtimestamp(timestamp).strftime('%m-%d')
                    volume = float(kline[7])  # 交易额
                    daily_volumes.append({
                        'date': date,
                        'volume': volume
                    })
                return daily_volumes
            return []
        except Exception as e:
            print(f"获取{inst_id}历史日交易额时出错: {e}")
            return []
    
    def check_volume_explosion_batch(self, instruments_batch):
        """批量检查多个交易对的爆量情况"""
        alerts = []
        billion_volume_alerts = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            # 提交所有任务
            future_to_inst = {
                executor.submit(self.check_single_instrument_volume, inst['instId']): inst['instId'] 
                for inst in instruments_batch
            }
            
            # 收集结果
            for future in future_to_inst:
                inst_id = future_to_inst[future]
                try:
                    inst_alerts, billion_alert = future.result(timeout=30)  # 30秒超时
                    if inst_alerts:
                        alerts.extend(inst_alerts)
                        print(f"发现爆量: {inst_id}")
                    if billion_alert:
                        billion_volume_alerts.append(billion_alert)
                        print(f"发现过亿成交: {inst_id}")
                except Exception as e:
                    print(f"检查 {inst_id} 时出错: {e}")
                    continue
        
        return alerts, billion_volume_alerts
    
    def get_daily_volume(self, inst_id):
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
            print(f"获取{inst_id}当天交易额时出错: {e}")
            return 0
    
    def check_single_instrument_volume(self, inst_id):
        """检查单个交易对是否出现爆量和过亿成交"""
        alerts = []
        billion_alert = None
        
        try:
            # 获取当天交易额
            daily_volume = self.get_daily_volume(inst_id)
            
            # 检查是否过亿
            if daily_volume >= 100_000_000:  # 1亿USDT
                # 获取过去7天的日交易额历史
                daily_volumes_history = self.get_daily_volumes_history(inst_id, 7)
                billion_alert = {
                    'inst_id': inst_id,
                    'current_daily_volume': daily_volume,
                    'daily_volumes_history': daily_volumes_history
                }
            
            # 检查1小时爆量
            hour_data = self.get_kline_data(inst_id, '1H', 20)
            if hour_data:
                prev_ratio, ma10_ratio = self.calculate_volume_ratio(hour_data)
                if prev_ratio and ma10_ratio:
                    current_volume = float(hour_data[0][7])
                    
                    # 小时爆量标准：10倍
                    if prev_ratio >= 10 or ma10_ratio >= 10:
                        alert_data = {
                            'inst_id': inst_id,
                            'timeframe': '1H',
                            'current_volume': current_volume,
                            'prev_ratio': prev_ratio if prev_ratio >= 10 else None,
                            'ma10_ratio': ma10_ratio if ma10_ratio >= 10 else None,
                            'daily_volume': daily_volume
                        }
                        alerts.append(alert_data)
            
            # 检查4小时爆量
            four_hour_data = self.get_kline_data(inst_id, '4H', 20)
            if four_hour_data:
                prev_ratio, ma10_ratio = self.calculate_volume_ratio(four_hour_data)
                if prev_ratio and ma10_ratio:
                    current_volume = float(four_hour_data[0][7])
                    
                    # 4小时爆量标准：5倍
                    if prev_ratio >= 5 or ma10_ratio >= 5:
                        alert_data = {
                            'inst_id': inst_id,
                            'timeframe': '4H',
                            'current_volume': current_volume,
                            'prev_ratio': prev_ratio if prev_ratio >= 5 else None,
                            'ma10_ratio': ma10_ratio if ma10_ratio >= 5 else None,
                            'daily_volume': daily_volume
                        }
                        alerts.append(alert_data)
            
            return alerts, billion_alert
            
        except Exception as e:
            print(f"检查 {inst_id} 时出错: {e}")
            return [], None
    
    def get_last_alert_time(self):
        """获取上次发送爆量警报的时间"""
        try:
            if os.path.exists(self.heartbeat_file):
                with open(self.heartbeat_file, 'r') as f:
                    timestamp = float(f.read().strip())
                    return timestamp
            return 0
        except Exception as e:
            print(f"读取上次警报时间失败: {e}")
            return 0
    
    def update_last_alert_time(self):
        """更新上次发送爆量警报的时间"""
        try:
            with open(self.heartbeat_file, 'w') as f:
                f.write(str(time.time()))
        except Exception as e:
            print(f"更新上次警报时间失败: {e}")
    
    def should_send_heartbeat(self):
        """检查是否需要发送心跳消息"""
        last_alert_time = self.get_last_alert_time()
        current_time = time.time()
        time_since_last_alert = current_time - last_alert_time
        
        return time_since_last_alert >= self.heartbeat_interval
    
    def format_volume(self, volume):
        """格式化交易额显示"""
        if volume >= 1_000_000_000:  # 10亿
            return f"{volume/1_000_000_000:.2f}B"
        elif volume >= 1_000_000:  # 100万
            return f"{volume/1_000_000:.2f}M"
        elif volume >= 1_000:  # 1千
            return f"{volume/1_000:.2f}K"
        else:
            return f"{volume:.2f}"
    
    def generate_chart_url_quickchart(self, billion_alerts):
        """使用QuickChart生成图表URL"""
        if not billion_alerts or len(billion_alerts) == 0:
            return None
        
        try:
            # 显示所有过亿成交的交易对
            top_alerts = billion_alerts
            
            # 准备数据和颜色
            labels = []
            current_data = []
            # 动态生成足够的颜色
            base_colors = [
                '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0',
                '#9966FF', '#FF9F40', '#FF6B6B', '#4ECDC4',
                '#45B7D1', '#FFA07A', '#98D8C8', '#F7DC6F',
                '#BB8FCE', '#85C1E9', '#F8C471', '#82E0AA',
                '#F1948A', '#AED6F1', '#F9E79F', '#A9DFBF'
            ]
            
            # 如果交易对数量超过预定义颜色，则循环使用
            colors = []
            for i in range(len(top_alerts)):
                colors.append(base_colors[i % len(base_colors)])
            
            for i, alert in enumerate(top_alerts):
                inst_name = alert['inst_id'].replace('-SWAP', '').replace('USDT', '')
                labels.append(inst_name)
                current_data.append(round(alert['current_daily_volume'] / 1_000_000, 1))  # 转换为百万
            
            # 动态调整图表尺寸
            num_items = len(top_alerts)
            chart_width = max(800, min(1200, 100 * num_items))  # 根据数量调整宽度
            chart_height = max(400, min(800, 50 * num_items))   # 根据数量调整高度
            
            # 构建Chart.js配置
            chart_config = {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [{
                        "label": "当天成交额 (百万USDT)",
                        "data": current_data,
                        "backgroundColor": colors[:len(top_alerts)],
                        "borderColor": colors[:len(top_alerts)],
                        "borderWidth": 1
                    }]
                },
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False,
                    "plugins": {
                        "title": {
                            "display": True,
                            "text": "OKX 过亿成交额排行",
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
                            },
                            "ticks": {
                                "maxRotation": 45,
                                "minRotation": 0
                            }
                        }
                    }
                }
            }
            
            # 将配置转换为JSON字符串并编码
            chart_json = json.dumps(chart_config)
            encoded_chart = urllib.parse.quote(chart_json)
            
            # 生成QuickChart URL，使用动态尺寸
            chart_url = f"https://quickchart.io/chart?c={encoded_chart}&width={chart_width}&height={chart_height}&format=png"
            
            print(f"生成图表URL成功，包含 {len(top_alerts)} 个交易对，尺寸: {chart_width}x{chart_height}")
            return chart_url
            
        except Exception as e:
            print(f"生成图表URL时出错: {e}")
            return None
    
    def generate_trend_chart_url(self, billion_alerts):
        """生成趋势图表URL（显示历史对比）"""
        if not billion_alerts or len(billion_alerts) == 0:
            return None
        
        try:
            # 显示所有交易对的趋势
            top_alerts = billion_alerts
            
            # 获取所有可用的日期
            all_dates = set()
            for alert in top_alerts:
                if alert['daily_volumes_history']:
                    for vol_data in alert['daily_volumes_history']:
                        all_dates.add(vol_data['date'])
            
            # 按日期排序
            sorted_dates = sorted(list(all_dates))[-7:]  # 最近7天
            
            # 为每个交易对准备数据
            datasets = []
            # 扩展颜色列表以支持更多交易对
            base_colors = [
                '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                '#FF9F40', '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A',
                '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9', '#F8C471',
                '#82E0AA', '#F1948A', '#AED6F1', '#F9E79F', '#A9DFBF',
                '#D7BDE2', '#A3E4D7', '#F9E79F', '#FADBD8', '#D5DBDB'
            ]
            
            for i, alert in enumerate(top_alerts):
                inst_name = alert['inst_id'].replace('-SWAP', '').replace('USDT', '')
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
                
                color = base_colors[i % len(base_colors)]
                datasets.append({
                    "label": inst_name,
                    "data": data,
                    "borderColor": color,
                    "backgroundColor": color + "20",  # 添加透明度
                    "fill": False,
                    "tension": 0.4
                })
            
            # 动态调整图表尺寸
            num_items = len(top_alerts)
            chart_width = max(800, min(1400, 80 * num_items))
            chart_height = max(500, min(900, 40 * num_items))
            
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
                            "text": "OKX 成交额趋势对比",
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
            
            chart_json = json.dumps(chart_config)
            encoded_chart = urllib.parse.quote(chart_json)
            chart_url = f"https://quickchart.io/chart?c={encoded_chart}&width={chart_width}&height={chart_height}&format=png"
            
            print(f"生成趋势图表URL成功，包含 {len(top_alerts)} 个交易对，尺寸: {chart_width}x{chart_height}")
    def generate_volume_explosion_chart(self, alerts):
        """为爆量信号生成图表"""
        if not alerts or len(alerts) == 0:
            return None
        
        try:
            # 按时间框架分组
            hour_alerts = [alert for alert in alerts if alert['timeframe'] == '1H']
            four_hour_alerts = [alert for alert in alerts if alert['timeframe'] == '4H']
            
            all_chart_alerts = hour_alerts + four_hour_alerts
            if not all_chart_alerts:
                return None
            
            # 按交易额排序
            all_chart_alerts.sort(key=lambda x: x['current_volume'], reverse=True)
            
            # 准备数据
            labels = []
            volume_data = []
            ratio_data = []
            colors = []
            
            # 扩展颜色列表
            base_colors = [
                '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                '#FF9F40', '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A',
                '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9', '#F8C471',
                '#82E0AA', '#F1948A', '#AED6F1', '#F9E79F', '#A9DFBF'
            ]
            
            for i, alert in enumerate(all_chart_alerts):
                inst_name = alert['inst_id'].replace('-SWAP', '').replace('USDT', '')
                timeframe = alert['timeframe']
                label = f"{inst_name}({timeframe})"
                
                labels.append(label)
                volume_data.append(round(alert['current_volume'] / 1_000_000, 1))  # 转换为百万
                
                # 取较大的倍数
                max_ratio = max(
                    alert.get('prev_ratio', 0) or 0,
                    alert.get('ma10_ratio', 0) or 0
                )
                ratio_data.append(round(max_ratio, 1))
                
                # 根据时间框架选择颜色深浅
                base_color = base_colors[i % len(base_colors)]
                if timeframe == '4H':
                    colors.append(base_color + 'AA')  # 4H稍微透明
                else:
                    colors.append(base_color)
            
            # 动态调整图表尺寸
            num_items = len(all_chart_alerts)
            chart_width = max(900, min(1400, 100 * num_items))
            chart_height = max(500, min(800, 50 * num_items))
            
            # 构建双轴图表配置
            chart_config = {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [
                        {
                            "label": "交易额(百万USDT)",
                            "data": volume_data,
                            "backgroundColor": colors,
                            "borderColor": colors,
                            "borderWidth": 1,
                            "yAxisID": "y"
                        },
                        {
                            "label": "爆量倍数",
                            "data": ratio_data,
                            "type": "line",
                            "borderColor": "#FF0000",
                            "backgroundColor": "#FF000030",
                            "borderWidth": 3,
                            "fill": False,
                            "yAxisID": "y1",
                            "pointBackgroundColor": "#FF0000",
                            "pointBorderColor": "#FFFFFF",
                            "pointRadius": 5
                        }
                    ]
                },
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False,
                    "plugins": {
                        "title": {
                            "display": True,
                            "text": "OKX 爆量信号分析",
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
                            "type": "linear",
                            "display": True,
                            "position": "left",
                            "beginAtZero": True,
                            "title": {
                                "display": True,
                                "text": "交易额 (百万USDT)"
                            }
                        },
                        "y1": {
                            "type": "linear",
                            "display": True,
                            "position": "right",
                            "beginAtZero": True,
                            "title": {
                                "display": True,
                                "text": "爆量倍数"
                            },
                            "grid": {
                                "drawOnChartArea": False
                            }
                        },
                        "x": {
                            "title": {
                                "display": True,
                                "text": "交易对(时间框架)"
                            },
                            "ticks": {
                                "maxRotation": 45,
                                "minRotation": 0
                            }
                        }
                    }
                }
            }
            
            chart_json = json.dumps(chart_config)
            encoded_chart = urllib.parse.quote(chart_json)
            chart_url = f"https://quickchart.io/chart?c={encoded_chart}&width={chart_width}&height={chart_height}&format=png"
            
            print(f"生成爆量图表URL成功，包含 {len(all_chart_alerts)} 个信号，尺寸: {chart_width}x{chart_height}")
            return chart_url
            
        except Exception as e:
            print(f"生成爆量图表URL时出错: {e}")
            return None
    
    def create_billion_volume_table(self, billion_alerts):
        """创建过亿成交额的表格格式消息"""
        if not billion_alerts:
            return ""
        
        # 按当天交易额从高到低排序
        billion_alerts.sort(key=lambda x: x['current_daily_volume'], reverse=True)
        
        content = "## 💰 日成交过亿信号\n\n"
        
        # 生成图表
        chart_url = self.generate_chart_url_quickchart(billion_alerts)
        trend_chart_url = self.generate_trend_chart_url(billion_alerts)
        
        # 添加图表
        if chart_url:
            content += f"### 📊 成交额排行图\n"
            content += f"![成交额排行]({chart_url})\n\n"
        
        if trend_chart_url:
            content += f"### 📈 成交额趋势图\n"
            content += f"![成交额趋势]({trend_chart_url})\n\n"
        
        # 构建表头
        header = "### 📋 详细数据表格\n\n"
        header += "| 交易对 | 当天成交额 |"
        separator = "|--------|------------|"
        
        # 获取最多的历史天数
        max_history_days = 0
        for alert in billion_alerts:
            if alert['daily_volumes_history']:
                max_history_days = max(max_history_days, len(alert['daily_volumes_history']) - 1)  # 减1因为第一个是当天
        
        # 添加历史日期的表头
        for i in range(1, min(max_history_days + 1, 7)):  # 最多显示过去6天
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
            
            row = f"| {inst_id} | **{current_vol}** |"
            
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
    
    def create_alert_table(self, alerts):
        """创建爆量警报的表格格式消息"""
        if not alerts:
            return ""
        
        content = ""
        
        # 生成爆量图表
        explosion_chart_url = self.generate_volume_explosion_chart(alerts)
        if explosion_chart_url:
            content += "### 🔥 爆量信号图表\n"
            content += f"![爆量信号图表]({explosion_chart_url})\n\n"
        
        # 按时间框架分组
        hour_alerts = [alert for alert in alerts if alert['timeframe'] == '1H']
        four_hour_alerts = [alert for alert in alerts if alert['timeframe'] == '4H']
        
        # 按当前交易额从高到低排序
        hour_alerts.sort(key=lambda x: x['current_volume'], reverse=True)
        four_hour_alerts.sort(key=lambda x: x['current_volume'], reverse=True)
        
        if hour_alerts:
            content += "## 🔥 1小时爆量信号\n\n"
            content += "| 交易对 | 当前交易额 | 相比上期 | 相比MA10 | 当天总额 |\n"
            content += "|--------|------------|----------|----------|----------|\n"
            
            for alert in hour_alerts:
                inst_id = alert['inst_id']
                current_vol = self.format_volume(alert['current_volume'])
                daily_vol = self.format_volume(alert['daily_volume'])
                
                prev_ratio_str = f"{alert['prev_ratio']:.1f}x 📈" if alert['prev_ratio'] else "-"
                ma10_ratio_str = f"{alert['ma10_ratio']:.1f}x 📈" if alert['ma10_ratio'] else "-"
                
                content += f"| {inst_id} | {current_vol} | {prev_ratio_str} | {ma10_ratio_str} | {daily_vol} |\n"
            
            content += "\n"
        
        if four_hour_alerts:
            content += "## 🚀 4小时爆量信号\n\n"
            content += "| 交易对 | 当前交易额 | 相比上期 | 相比MA10 | 当天总额 |\n"
            content += "|--------|------------|----------|----------|----------|\n"
            
            for alert in four_hour_alerts:
                inst_id = alert['inst_id']
                current_vol = self.format_volume(alert['current_volume'])
                daily_vol = self.format_volume(alert['daily_volume'])
                
                prev_ratio_str = f"{alert['prev_ratio']:.1f}x 📈" if alert['prev_ratio'] else "-"
                ma10_ratio_str = f"{alert['ma10_ratio']:.1f}x 📈" if alert['ma10_ratio'] else "-"
                
                content += f"| {inst_id} | {current_vol} | {prev_ratio_str} | {ma10_ratio_str} | {daily_vol} |\n"
            
            content += "\n"
        
        return content
    
    def send_heartbeat_notification(self, monitored_count):
        """发送心跳监测消息"""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_alert_time = self.get_last_alert_time()
        
        if last_alert_time > 0:
            last_alert_datetime = datetime.fromtimestamp(last_alert_time)
            time_since_alert = datetime.now() - last_alert_datetime
            hours_since = int(time_since_alert.total_seconds() / 3600)
            
            title = "OKX监控系统心跳 💓"
            content = f"监控系统正常运行中...\n\n"
            content += f"📊 监控状态: 正常\n"
            content += f"📈 监控交易对: {monitored_count} 个\n"
            content += f"⏰ 检查时间: {current_time}\n"
            content += f"🔕 距离上次爆量警报: {hours_since} 小时\n"
            content += f"📅 上次警报时间: {last_alert_datetime.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            content += f"💡 提示: 已连续 {hours_since} 小时无爆量信号"
        else:
            title = "OKX监控系统心跳 💓"
            content = f"监控系统正常运行中...\n\n"
            content += f"📊 监控状态: 正常\n"
            content += f"📈 监控交易对: {monitored_count} 个\n"
            content += f"⏰ 检查时间: {current_time}\n"
            content += f"🔕 暂无爆量警报记录\n\n"
            content += f"💡 提示: 系统首次运行或记录文件不存在"
        
        success = self.send_notification(title, content)
        if success:
            print("心跳消息发送成功")
        return success
    
    def send_notification(self, title, content):
        """通过Server酱发送微信通知"""
        try:
            url = f"https://sctapi.ftqq.com/{self.server_jiang_key}.send"
            data = {
                'title': title,
                'desp': content
            }
            
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            if result.get('code') == 0:
                print(f"通知发送成功: {title}")
                return True
            else:
                print(f"通知发送失败: {result}")
                return False
                
        except Exception as e:
            print(f"发送通知时出错: {e}")
            return False
    
    def run_monitor(self):
        """运行监控主程序"""
        print(f"开始监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取交易对列表
        instruments = self.get_perpetual_instruments()
        if not instruments:
            print("未能获取交易对列表，退出监控")
            return
        
        # 监控所有活跃的交易对，分批处理
        batch_size = 10
        total_batches = (len(instruments) + batch_size - 1) // batch_size
        print(f"开始监控所有 {len(instruments)} 个交易对，分 {total_batches} 批处理")
        
        all_alerts = []
        all_billion_alerts = []
        
        # 分批处理交易对
        for batch_num in range(0, len(instruments), batch_size):
            batch = instruments[batch_num:batch_num + batch_size]
            batch_index = batch_num // batch_size + 1
            
            print(f"处理第 {batch_index}/{total_batches} 批 ({len(batch)} 个交易对)")
            
            try:
                batch_alerts, batch_billion_alerts = self.check_volume_explosion_batch(batch)
                all_alerts.extend(batch_alerts)
                all_billion_alerts.extend(batch_billion_alerts)
                
                # 批次间添加短暂延迟
                if batch_index < total_batches:
                    time.sleep(2)
                    
            except Exception as e:
                print(f"处理第 {batch_index} 批时出错: {e}")
                continue
        
        # 发送汇总通知
        has_any_signal = len(all_alerts) > 0 or len(all_billion_alerts) > 0
        
        if has_any_signal:
            # 构建标题
            if len(all_alerts) > 0 and len(all_billion_alerts) > 0:
                title = f"🚨 OKX监控 - {len(all_alerts)}个爆量+{len(all_billion_alerts)}个过亿"
            elif len(all_alerts) > 0:
                title = f"🚨 OKX监控 - 发现{len(all_alerts)}个爆量信号"
            else:
                title = f"💰 OKX监控 - 发现{len(all_billion_alerts)}个过亿信号"
            
            content = f"**监控时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            content += f"**监控范围**: {len(instruments)} 个交易对\n\n"
            
            # 先创建爆量表格
            if all_alerts:
                table_content = self.create_alert_table(all_alerts)
                content += table_content
            
            # 再创建过亿成交额表格（包含图表）
            if all_billion_alerts:
                billion_table_content = self.create_billion_volume_table(all_billion_alerts)
                content += billion_table_content
            
            # 添加说明
            content += "---\n\n"
            content += "**说明**:\n"
            content += "- **爆量信号**: 1H需10倍增长，4H需5倍增长\n"
            content += "- **过亿信号**: 当天成交额超过1亿USDT\n"
            content += "- **相比上期**: 与上一个同周期的交易额对比\n"
            content += "- **相比MA10**: 与过去10个周期平均值对比\n"
            content += "- **K/M/B**: 千/百万/十亿 USDT\n"
            content += "- **图表**: 由QuickChart.io生成，显示排行和趋势对比"
            
            success = self.send_notification(title, content)
            if success:
                # 更新上次发送爆量警报的时间
                self.update_last_alert_time()
        else:
            print("未发现爆量或过亿成交情况")
            
            # 检查是否需要发送心跳消息
            if self.should_send_heartbeat():
                print("距离上次爆量警报已超过4小时，发送心跳消息")
                heartbeat_success = self.send_heartbeat_notification(len(instruments))
                if heartbeat_success:
                    # 更新心跳时间（避免频繁发送心跳）
                    self.update_last_alert_time()
        
        print(f"监控完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    monitor = OKXVolumeMonitor()
    monitor.run_monitor()
