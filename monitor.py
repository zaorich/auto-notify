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

class OKXVolumeMonitor:
    def __init__(self):
        self.base_url = "https://www.okx.com"
        # 请确保设置正确的SERVER_JIANG_KEY环境变量
        self.server_jiang_key = os.environ.get('SERVER_JIANG_KEY', '')
        if not self.server_jiang_key:
            print("警告：未设置SERVER_JIANG_KEY环境变量")
        
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
                        'volume': volume,
                        'timestamp': timestamp
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
    
    def create_ascii_trend(self, volumes):
        """创建ASCII趋势图"""
        if not volumes or len(volumes) < 2:
            return ""
        
        # 计算变化趋势
        trend = []
        for i in range(1, len(volumes)):
            if volumes[i] > volumes[i-1] * 1.1:  # 增长超过10%
                trend.append("📈")
            elif volumes[i] < volumes[i-1] * 0.9:  # 下降超过10%
                trend.append("📉")
            else:
                trend.append("➡️")
        
        return " ".join(trend)
    
    def create_billion_volume_table(self, billion_alerts):
        """创建过亿成交额的表格格式消息"""
        if not billion_alerts:
            return ""
        
        # 按当天交易额从高到低排序
        billion_alerts.sort(key=lambda x: x['current_daily_volume'], reverse=True)
        
        # 限制显示数量，避免消息过长
        max_display = 15
        if len(billion_alerts) > max_display:
            billion_alerts = billion_alerts[:max_display]
            truncated = True
        else:
            truncated = False
        
        content = "## 💰 日成交过亿信号\n\n"
        
        # 简化表格，只显示最近3天的数据
        content += "| 交易对 | 当天 | 昨天 | 前天 | 趋势 |\n"
        content += "|--------|------|------|------|------|\n"
        
        for alert in billion_alerts:
            inst_id = alert['inst_id'].replace('-SWAP', '')  # 简化显示
            current_vol = self.format_volume(alert['current_daily_volume'])
            
            history = alert['daily_volumes_history']
            yesterday = self.format_volume(history[1]['volume']) if len(history) > 1 else "-"
            day_before = self.format_volume(history[2]['volume']) if len(history) > 2 else "-"
            
            # 计算趋势
            if len(history) >= 3:
                recent_volumes = [history[2]['volume'], history[1]['volume'], history[0]['volume']]
                trend = self.create_ascii_trend(recent_volumes)
            else:
                trend = "-"
            
            content += f"| {inst_id} | **{current_vol}** | {yesterday} | {day_before} | {trend} |\n"
        
        if truncated:
            content += f"\n*注：仅显示前{max_display}个交易对*\n"
        
        content += "\n"
        return content


    def create_billion_volume_chart(self, billion_alerts):
        # """创建过亿成交额的折线图"""
        if not billion_alerts:
            return ""
        
        # 按当天交易额从高到低排序
        billion_alerts.sort(key=lambda x: x['current_daily_volume'], reverse=True)
        
        content = "## 💰 日成交过亿信号\n\n"
        
        # 为每个交易对生成一个折线图
        for alert in billion_alerts:
            inst_id = alert['inst_id']
            current_vol = self.format_volume(alert['current_daily_volume'])
            
            # 准备数据
            history = alert['daily_volumes_history']
            if not history:
                continue
                
            # 获取最近7天的数据
            days_data = history[:7]
            days_data.reverse()  # 反转使时间从左到右
            
            # 提取日期和数值
            dates = [d['date'] for d in days_data]
            values = [d['volume'] for d in days_data]
            
            # 创建SVG折线图
            svg_width = 500
            svg_height = 200
            margin = {'top': 20, 'right': 40, 'bottom': 40, 'left': 80}
            chart_width = svg_width - margin['left'] - margin['right']
            chart_height = svg_height - margin['top'] - margin['bottom']
            
            # 计算Y轴范围
            max_value = max(values) * 1.1  # 留10%空间
            min_value = 0
            
            # 开始构建SVG字符串
            svg_lines = []
            svg_lines.append(f'<svg width="{svg_width}" height="{svg_height}" xmlns="http://www.w3.org/2000/svg">')
            
            # 背景
            svg_lines.append(f'  <rect width="{svg_width}" height="{svg_height}" fill="#f8f9fa"/>')
            
            # 标题
            svg_lines.append(f'  <text x="{svg_width/2}" y="15" text-anchor="middle" font-size="14" font-weight="bold" fill="#333">')
            svg_lines.append(f'    {inst_id} - 当前: {current_vol}')
            svg_lines.append('  </text>')
            
            # 图表区域背景
            svg_lines.append(f'  <rect x="{margin["left"]}" y="{margin["top"]}" width="{chart_width}" height="{chart_height}" fill="white" stroke="#e0e0e0" stroke-width="1"/>')
            
            # 网格线
            svg_lines.append('  <g stroke="#f0f0f0" stroke-width="1">')
            for i in range(5):
                y = margin['top'] + (chart_height * i / 4)
                svg_lines.append(f'    <line x1="{margin["left"]}" y1="{y}" x2="{margin["left"] + chart_width}" y2="{y}"/>')
            svg_lines.append('  </g>')
            
            # Y轴标签
            svg_lines.append('  <!-- Y轴标签 -->')
            for i in range(5):
                y = margin['top'] + (chart_height * i / 4)
                value = max_value - (max_value * i / 4)
                label = self.format_volume(value)
                svg_lines.append(f'  <text x="{margin["left"] - 10}" y="{y + 5}" text-anchor="end" font-size="11" fill="#666">{label}</text>')
            
            # X轴标签
            svg_lines.append('  <!-- X轴标签 -->')
            x_step = chart_width / (len(dates) - 1) if len(dates) > 1 else chart_width
            for i, date in enumerate(dates):
                x = margin['left'] + (i * x_step)
                y = margin['top'] + chart_height + 20
                svg_lines.append(f'  <text x="{x}" y="{y}" text-anchor="middle" font-size="11" fill="#666">{date}</text>')
            
            # 数据点和折线
            points = []
            for i, value in enumerate(values):
                x = margin['left'] + (i * x_step)
                y = margin['top'] + chart_height - (value / max_value * chart_height)
                points.append(f"{x},{y}")
            
            # 绘制折线
            svg_lines.append('  <!-- 折线 -->')
            svg_lines.append(f'  <polyline points="{" ".join(points)}" fill="none" stroke="#1890ff" stroke-width="2"/>')
            
            # 绘制数据点
            svg_lines.append('  <!-- 数据点 -->')
            for i, value in enumerate(values):
                x = margin['left'] + (i * x_step)
                y = margin['top'] + chart_height - (value / max_value * chart_height)
                
                # 最后一个点（当天）用不同颜色标记
                color = "#ff4d4f" if i == len(values) - 1 else "#1890ff"
                radius = "4" if i == len(values) - 1 else "3"
                
                svg_lines.append(f'  <circle cx="{x}" cy="{y}" r="{radius}" fill="{color}" stroke="white" stroke-width="1"/>')
                
                # 显示数值
                label = self.format_volume(value)
                y_offset = -10 if y > margin['top'] + 20 else 15
                svg_lines.append(f'  <text x="{x}" y="{y + y_offset}" text-anchor="middle" font-size="10" fill="#333">{label}</text>')
            
            svg_lines.append('</svg>')
            
            # 将SVG包装在代码块中
            content += "svg\n"
            content += "\n".join(svg_lines)
            content += "\n
    
    def create_alert_table(self, alerts):
        """创建爆量警报的表格格式消息"""
        if not alerts:
            return ""
        
        # 按时间框架分组
        hour_alerts = [alert for alert in alerts if alert['timeframe'] == '1H']
        four_hour_alerts = [alert for alert in alerts if alert['timeframe'] == '4H']
        
        # 按当前交易额从高到低排序
        hour_alerts.sort(key=lambda x: x['current_volume'], reverse=True)
        four_hour_alerts.sort(key=lambda x: x['current_volume'], reverse=True)
        
        # 限制显示数量
        max_display = 10
        content = ""
        
        if hour_alerts:
            display_alerts = hour_alerts[:max_display]
            content += "## 🔥 1小时爆量信号\n\n"
            content += "| 交易对 | 当前 | 倍数 | 当天 |\n"
            content += "|--------|------|------|------|\n"
            
            for alert in display_alerts:
                inst_id = alert['inst_id'].replace('-SWAP', '')  # 简化显示
                current_vol = self.format_volume(alert['current_volume'])
                daily_vol = self.format_volume(alert['daily_volume'])
                
                # 显示最高的倍数
                ratio = max(alert['prev_ratio'] or 0, alert['ma10_ratio'] or 0)
                ratio_str = f"{ratio:.1f}x" if ratio > 0 else "-"
                
                content += f"| {inst_id} | {current_vol} | {ratio_str} | {daily_vol} |\n"
            
            if len(hour_alerts) > max_display:
                content += f"\n*仅显示前{max_display}个*\n"
            content += "\n"
        
        if four_hour_alerts:
            display_alerts = four_hour_alerts[:max_display]
            content += "## 🚀 4小时爆量信号\n\n"
            content += "| 交易对 | 当前 | 倍数 | 当天 |\n"
            content += "|--------|------|------|------|\n"
            
            for alert in display_alerts:
                inst_id = alert['inst_id'].replace('-SWAP', '')  # 简化显示
                current_vol = self.format_volume(alert['current_volume'])
                daily_vol = self.format_volume(alert['daily_volume'])
                
                # 显示最高的倍数
                ratio = max(alert['prev_ratio'] or 0, alert['ma10_ratio'] or 0)
                ratio_str = f"{ratio:.1f}x" if ratio > 0 else "-"
                
                content += f"| {inst_id} | {current_vol} | {ratio_str} | {daily_vol} |\n"
            
            if len(four_hour_alerts) > max_display:
                content += f"\n*仅显示前{max_display}个*\n"
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
            
            title = "OKX监控心跳"
            content = f"系统运行正常\n"
            content += f"监控: {monitored_count}个\n"
            content += f"时间: {current_time}\n"
            content += f"距上次: {hours_since}小时"
        else:
            title = "OKX监控心跳"
            content = f"系统运行正常\n"
            content += f"监控: {monitored_count}个\n"
            content += f"时间: {current_time}"
        
        success = self.send_notification(title, content)
        if success:
            print("心跳消息发送成功")
        return success
    
    def send_notification(self, title, content):
        """通过Server酱发送微信通知"""
        if not self.server_jiang_key:
            print("错误：未设置Server酱密钥")
            print(f"标题: {title}")
            print(f"内容预览: {content[:200]}...")
            return False
            
        try:
            # Server酱的消息长度限制
            max_content_length = 30000  # 留些余量
            
            # 如果内容过长，进行截断
            if len(content) > max_content_length:
                content = content[:max_content_length] + "\n\n*消息过长已截断*"
            
            url = f"https://sctapi.ftqq.com/{self.server_jiang_key}.send"
            data = {
                'title': title,
                'desp': content
            }
            
            response = requests.post(url, data=data, timeout=30)
            
            # 检查响应
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    print(f"通知发送成功: {title}")
                    return True
                else:
                    print(f"Server酱返回错误: {result}")
                    return False
            else:
                print(f"HTTP错误 {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"发送通知时网络错误: {e}")
            return False
        except Exception as e:
            print(f"发送通知时出错: {e}")
            return False
    
    def run_monitor(self):
        """运行监控主程序"""
        print(f"开始监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 检查Server酱密钥
        if not self.server_jiang_key:
            print("\n警告：未设置Server酱密钥！")
            print("请设置环境变量 SERVER_JIANG_KEY")
            print("例如: export SERVER_JIANG_KEY='你的密钥'")
            print("\n将以打印模式运行...\n")
        
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
                title = f"OKX {len(all_alerts)}爆量+{len(all_billion_alerts)}过亿"
            elif len(all_alerts) > 0:
                title = f"OKX 发现{len(all_alerts)}个爆量"
            else:
                title = f"OKX 发现{len(all_billion_alerts)}个过亿"
            
            content = f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            content += f"范围: {len(instruments)}个交易对\n\n"
            
            # 先创建爆量表格
            if all_alerts:
                table_content = self.create_alert_table(all_alerts)
                content += table_content
            
            # 再创建过亿成交额表格（放在最后）
            if all_billion_alerts:
                # billion_table_content = self.create_billion_volume_table(all_billion_alerts)
                billion_table_content = self.create_billion_volume_chart(all_billion_alerts)
                content += billion_table_content
            
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
    # 设置Server酱密钥的方法：
    # 1. 通过环境变量: export SERVER_JIANG_KEY='你的密钥'
    # 2. 或者直接在这里设置（不推荐）:
    # os.environ['SERVER_JIANG_KEY'] = '你的密钥'
    
    monitor = OKXVolumeMonitor()
    monitor.run_monitor()
