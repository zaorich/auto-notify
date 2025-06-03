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
    
    def check_volume_explosion_batch(self, instruments_batch):
        """批量检查多个交易对的爆量情况"""
        alerts = []
        
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
                    inst_alerts = future.result(timeout=30)  # 30秒超时
                    if inst_alerts:
                        alerts.extend(inst_alerts)
                        print(f"发现爆量: {inst_id}")
                except Exception as e:
                    print(f"检查 {inst_id} 时出错: {e}")
                    continue
        
        return alerts
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
        """检查单个交易对是否出现爆量"""
        alerts = []
        
        try:
            # 获取当天交易额
            daily_volume = self.get_daily_volume(inst_id)
            
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
            
            return alerts
            
        except Exception as e:
            print(f"检查 {inst_id} 时出错: {e}")
            return []
    
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
    
    def create_alert_table(self, alerts):
        """创建爆量警报的表格格式消息"""
        if not alerts:
            return ""
        
        # 按时间框架分组
        hour_alerts = [alert for alert in alerts if alert['timeframe'] == '1H']
        four_hour_alerts = [alert for alert in alerts if alert['timeframe'] == '4H']
        
        content = ""
        
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
        
        # 分批处理交易对
        for batch_num in range(0, len(instruments), batch_size):
            batch = instruments[batch_num:batch_num + batch_size]
            batch_index = batch_num // batch_size + 1
            
            print(f"处理第 {batch_index}/{total_batches} 批 ({len(batch)} 个交易对)")
            
            try:
                batch_alerts = self.check_volume_explosion_batch(batch)
                all_alerts.extend(batch_alerts)
                
                # 批次间添加短暂延迟
                if batch_index < total_batches:
                    time.sleep(2)
                    
            except Exception as e:
                print(f"处理第 {batch_index} 批时出错: {e}")
                continue
        
        # 发送汇总通知
        if all_alerts:
            title = f"🚨 OKX爆量监控 - 发现{len(all_alerts)}个信号"
            
            content = f"**监控时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            content += f"**监控范围**: {len(instruments)} 个交易对\n\n"
            
            # 创建表格格式的警报信息
            table_content = self.create_alert_table(all_alerts)
            content += table_content
            
            # 添加说明
            content += "---\n\n"
            content += "**说明**:\n"
            content += "- **相比上期**: 与上一个同周期的交易额对比\n"
            content += "- **相比MA10**: 与过去10个周期平均值对比\n"
            content += "- **当天总额**: 过去24小时总交易额\n"
            content += "- **K/M/B**: 千/百万/十亿 USDT"
            
            success = self.send_notification(title, content)
            if success:
                # 更新上次发送爆量警报的时间
                self.update_last_alert_time()
        else:
            print("未发现爆量情况")
            
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
