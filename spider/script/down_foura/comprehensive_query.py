import sys
sys.path.append('F:/newtowerV2')
sys.path.append(r'F:/newtowerV2\venv\Lib\site-packages')
from config import comprehensive_query_path
from utils.sql_utils import sql_orm
import time
import requests
import tempfile
import shutil
import os
import pandas as pd
from datetime import datetime


def clear_folder(folder_temp):
    for file in os.listdir(folder_temp):
        file = os.path.join(folder_temp, file)
        os.remove(file)

# 监控点：梯级电池,不定时更新
class cascade_battery():
    def __init__(self):
        self.down_name = '梯级电池'
        self.folder_temp1 = os.path.join(comprehensive_query_path, self.down_name, 'temp1')
        self.concat_name1 = os.path.join(comprehensive_query_path, self.down_name)
        self.search_ids_dantidianya = ['0447104001', '0447104002', '0447104003', '0447104004', '0447104005', '0447104006',
                             '0447104007', '0447104008', '0447104009', '0447104010', '0447104011', '0447104012',
                             '0447104013', '0447104014', '0447104015', '0447104016']

        self.folder_temp2 = os.path.join(comprehensive_query_path, self.down_name, 'temp2')
        self.concat_name2 = os.path.join(comprehensive_query_path, self.down_name)

        self.folder_temp3 = os.path.join(comprehensive_query_path, self.down_name, 'temp3')
        self.concat_name3 = os.path.join(comprehensive_query_path, self.down_name)

        self.cities=['0099977','0099978','0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']
        self.data2 = {
            'cities': self.cities,
            'search_id': '0447105001',
            'folder_temp': self.folder_temp2,
            'out_put': os.path.join(self.concat_name2, '剩余容量.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data3 = {
            'cities': self.cities,
            'search_id': '0447103001',
            'folder_temp': self.folder_temp3,
            'out_put': os.path.join(self.concat_name3, '电池组总电压.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
    def down(self):
        for search_id in self.search_ids_dantidianya:
            clear_folder(self.folder_temp1)
            data1 = {
                'cities': self.cities,
                'search_id': search_id,
                'folder_temp': self.folder_temp1,
                'out_put': os.path.join(self.folder_temp1, f'电池单体电压_{search_id}.xlsx'),
                'hours': 24,
                'content_len': 102400,
                'cookie_user': 1
            }
            res = requests.post(url='http://10.19.6.250:5002/download', data=data1)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data2)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data3)

# 监控点：整流模块电流、温度,不定时更新
class Rectifier_module():
    def __init__(self):
        self.start_time = datetime.now()
        self.down_name = '整流模块'
        self.folder_temp1 = os.path.join(comprehensive_query_path, self.down_name, 'temp1')
        self.folder_temp2 = os.path.join(comprehensive_query_path, self.down_name, 'temp2')
        self.concat_name = os.path.join(comprehensive_query_path, self.down_name)

        self.folder_temp3 = os.path.join(comprehensive_query_path, self.down_name, 'temp3')
        self.folder_temp4 = os.path.join(comprehensive_query_path, self.down_name, 'temp4')
        self.cities=['0099977','0099978','0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']
        # 信号量列表
        self.search_ids_current = ['0406113001','0406113002','0406113003','0406113004', '0406113005', '0406113006',
                                   '0406113007', '0406113008', '0406113009', '0406113010', '0406113011', '0406113012']
        self.search_ids_temperature = ['0406114001', '0406114002', '0406114003', '0406114004', '0406114005', '0406114006',
                                       '0406114007', '0406114008', '0406114009', '0406114010', '0406114011', '0406114012']

    def check_file_exists(self, file_path, initial_wait=600, retry_wait=120, max_attempts=50):
        """检查文件是否存在，支持初始等待和重试机制"""
        print(f"开始检查文件: {file_path}")

        # 首次等待10分钟
        print(f"初始等待 {initial_wait / 60} 分钟...")
        time.sleep(initial_wait)

        attempt = 1
        while attempt <= max_attempts:
            if os.path.exists(file_path):
                print(f"文件已找到: {file_path}")
                return True

            print(f"第 {attempt} 次检查: 文件不存在，等待 {retry_wait / 60} 分钟后再次检查")
            time.sleep(retry_wait)
            attempt += 1

        print(f"警告: 已达到最大尝试次数，文件未找到: {file_path}")
        return False

    def down(self):
        # 下载电流数据
        for search_id in self.search_ids_current:
            clear_folder(self.folder_temp1)
            output_file = os.path.join(self.folder_temp2, f'电流_{search_id}.xlsx')
            data = {
                'cities': self.cities,
                'search_id': search_id,
                'folder_temp': self.folder_temp1,
                'out_put': output_file,
                'hours': 24,
                'content_len': 20480,
                'cookie_user': 1
            }
            res = requests.post(url='http://10.19.6.250:5002/download', data=data)
            print(f"请求响应: {res.text}")

            # 检查文件是否存在
            file_exists = self.check_file_exists(output_file)
            if not file_exists:
                print(f"错误: 文件未生成，跳过当前请求: {output_file}")
                continue  # 继续处理下一个请求

        # 下载温度数据
        for search_id in self.search_ids_temperature:
            clear_folder(self.folder_temp3)
            output_file = os.path.join(self.folder_temp4, f'温度_{search_id}.xlsx')
            data = {
                'cities': self.cities,
                'search_id': search_id,
                'folder_temp': self.folder_temp3,
                'out_put': output_file,
                'hours': 24,
                'content_len': 20480,
                'cookie_user': 1
            }
            res = requests.post(url='http://10.19.6.250:5002/download', data=data)
            print(f"请求响应: {res.text}")

            # 检查文件是否存在
            file_exists = self.check_file_exists(output_file)
            if not file_exists:
                print(f"错误: 文件未生成，跳过当前请求: {output_file}")
                continue  # 继续处理下一个请求
    #
    # def merger(self):
    #     # 合并电流数据
    #     current_files = [f for f in os.listdir(self.folder_temp2)
    #                      if f.startswith('电流_') and f.endswith('.xlsx')]
    #     if current_files:
    #         current_dfs = []
    #         for file in current_files:
    #             file_path = os.path.join(self.folder_temp2, file)
    #             try:
    #                 # 读取Excel文件
    #                 df = pd.read_excel(file_path)
    #                 current_dfs.append(df)
    #                 print(f"已读取电流文件: {file}")
    #             except Exception as e:
    #                 print(f"读取电流文件 {file} 失败: {str(e)}")
    #
    #         if current_dfs:
    #             # 合并所有数据框
    #             merged_current = pd.concat(current_dfs, ignore_index=True)
    #             # 保存合并后的文件
    #             output_path = os.path.join(self.folder_temp2, '整流电流.xlsx')
    #             merged_current.to_excel(output_path, index=False)
    #             print(f"电流文件合并完成，保存至: {output_path}")
    #         else:
    #             print("没有可合并的电流数据")
    #     else:
    #         print("未找到电流文件")
    #
    #     # 合并温度数据
    #     temperature_files = [f for f in os.listdir(self.folder_temp4)
    #                          if f.startswith('温度_') and f.endswith('.xlsx')]
    #
    #     if temperature_files:
    #         temperature_dfs = []
    #         for file in temperature_files:
    #             file_path = os.path.join(self.folder_temp4, file)
    #             try:
    #                 # 读取Excel文件
    #                 df = pd.read_excel(file_path)
    #                 temperature_dfs.append(df)
    #                 print(f"已读取温度文件: {file}")
    #             except Exception as e:
    #                 print(f"读取温度文件 {file} 失败: {str(e)}")
    #
    #         if temperature_dfs:
    #             # 合并所有数据框
    #             merged_temperature = pd.concat(temperature_dfs, ignore_index=True)
    #             # 保存合并后的文件
    #             output_path = os.path.join(self.folder_temp4, '整流温度.xlsx')
    #             merged_temperature.to_excel(output_path, index=False)
    #             print(f"温度文件合并完成，保存至: {output_path}")
    #         else:
    #             print("没有可合并的温度数据")
    #     else:
    #         print("未找到温度文件")
    # def main(self):
    #     self.down()
    #     self.merger()

# 监控点：环境温度,每天9：00、15：00更新，16：00存档
class temperature():
    def __init__(self):
        self.down_name='环境温度'
        self.folder_temp = os.path.join(comprehensive_query_path, self.down_name, 'temp')
        self.concat_path = os.path.join(comprehensive_query_path, self.down_name)
        self.db_fields = ['省', '市', '区县', '站址', '站址运维ID', '设备名称', '设备厂家','设备型号', '设备ID', '设备资源编码',
                          '信号量ID', '监控点','时间', '实测值', '单位', '状态', '性能数据来源']
        self.cities=['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                 '0099986', '0099987', '0099988', '0099989', '0099990']
        self.data = {
            'cities': self.cities,
            'search_id': '0418101001',
            'folder_temp': self.folder_temp,
            'out_put': os.path.join(self.concat_path, '环境温度.xlsx'),
            'hours': 24,
            'content_len': 50 * 1024,
            'cookie_user': 1,
            'csv': True
        }

    def down(self):
        """下载环境温度文件"""
        clear_folder(self.folder_temp)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data)

    def check_file_is_today(self, file_path):
        """检查文件修改日期是否为今天"""
        if not os.path.exists(file_path):
            return False

        # 获取文件修改时间
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # 检查是否为今天
        return file_mtime >= today

    def wait_and_check_file(self, max_retries=12):
        """
        先等待15分钟，然后每5分钟检查文件是否为今天的
        最多重试12次（共60分钟），如果仍不是今天的文件则抛出异常
        """
        file_path = self.data['out_put']

        # 初始等待15分钟
        print(f"开始等待 15 分钟，等待文件下载完成...")
        time.sleep(15 * 60)

        # 然后每5分钟检查一次
        for attempt in range(1, max_retries + 1):
            print(f"第 {attempt} 次检查文件...")

            if self.check_file_is_today(file_path):
                print(f"文件已更新为今天 ({datetime.now().strftime('%Y-%m-%d')})，开始处理...")
                return True
            else:
                if os.path.exists(file_path):
                    print(f"文件存在，但不是今天的文件")
                else:
                    print(f"文件不存在: {file_path}")

                if attempt < max_retries:
                    print(f"等待 5 分钟后再次检查...")
                    time.sleep(5 * 60)

        raise TimeoutError(f"等待超时：文件在 {15 + max_retries * 5} 分钟后仍不是今天的文件")

    def df_process(self):
        """处理数据并插入数据库"""
        df = pd.read_excel(self.data['out_put'], dtype=str)
        df.columns = df.columns.str.strip()
        df_db = pd.DataFrame()
        for field in self.db_fields:
            df_db[field] = df[field] if field in df.columns else None
        df_db = df_db[(df_db["设备ID"].notna()) & (df_db["设备ID"] != "")]
        df_db = df_db.where(pd.notna(df_db), None)

        # 1.清空
        with sql_orm(database='tower').session_scope() as temp:
            sql, Base = temp
            sql.query(Base.classes.temperature).delete()
            sql.commit()

            # 2.插入
            pojo = Base.classes.temperature
            for _, row in df_db.iterrows():
                sql.add(pojo(**row.to_dict()))
            sql.commit()
        print(f"数据插入完成，共 {len(df_db)} 条记录")

    def run(self):
        """完整流程：下载 -> 等待检查 -> 处理入库"""
        self.down()
        self.wait_and_check_file()
        self.df_process()

class temperature_copy():
    def __init__(self):
        # 时间与路径配置
        self.current_time = datetime.now()
        self.formatted_time = self.current_time.strftime('%Y%m%d15')
        self.down_name = '环境温度'
        self.copy_file = r"F:\newtowerV2\message\ID_serch\xls\环境温度.xlsx"
        self.target_path = os.path.join(comprehensive_query_path, self.down_name, '环境温度存档')
        self.target_file_name = os.path.join(self.target_path, f"{self.formatted_time}.xlsx")
        self.station_file = r"F:\newtowerV2\websource\spider_download\station\站址信息.xlsx"

        # 中文列名配置（请根据实际Excel列名调整）
        self.temp_columns = {
            '站址名称列': '站址',  # 环境温度表D列的中文列名
            '运维ID列': '站址运维ID',  # 环境温度表E列的中文列名
            '需转文本列': ['站址运维ID', '设备ID']  # 需要转为文本的列（包括E列和I列）
        }

        self.station_columns = {
            '站址名称列': '名称',  # 站址信息表B列的中文列名
            '运维ID列': '运维ID',  # 站址信息表c列的中文列名
            '机房类型列': '机房类型'  # 站址信息表DR列的中文列名
        }

        # 确保目标目录存在
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)

    def add_room_type_to_temp_file(self):
        """使用中文列名进行双重匹配，添加机房类型列"""
        temp_path = None
        try:
            # 创建临时文件
            temp_fd, temp_path = tempfile.mkstemp(suffix='.xlsx')
            os.close(temp_fd)
            shutil.copy2(self.copy_file, temp_path)

            # 读取环境温度表，指定需要转为文本的列
            converters = {col: str for col in self.temp_columns['需转文本列']}
            temp_df = pd.read_excel(temp_path, converters=converters)

            # 读取站址信息表，指定相关列转为文本
            station_converters = {
                self.station_columns['运维ID列']: str,
                self.station_columns['站址名称列']: str
            }
            station_df = pd.read_excel(self.station_file, converters=station_converters)

            # 清理字符串，去除空格和特殊字符
            temp_df['clean_name'] = temp_df[self.temp_columns['站址名称列']].astype(str).str.strip().str.replace(r'\s+', ' ',
                                                                                                            regex=True)
            temp_df['clean_id'] = temp_df[self.temp_columns['运维ID列']].astype(str).str.strip()

            station_df['clean_name'] = station_df[self.station_columns['站址名称列']].astype(str).str.strip().str.replace(
                r'\s+', ' ', regex=True)
            station_df['clean_id'] = station_df[self.station_columns['运维ID列']].astype(str).str.strip()

            # 创建匹配键
            temp_df['match_key'] = temp_df['clean_id'] + "|" + temp_df['clean_name']
            station_df['match_key'] = station_df['clean_id'] + "|" + station_df['clean_name']

            # 创建映射字典
            key_to_room_type = dict(zip(station_df['match_key'], station_df[self.station_columns['机房类型列']]))

            # 在R列（第18列）插入机房类型列
            temp_df.insert(17, '机房类型', temp_df['match_key'].map(key_to_room_type))

            # 清理临时列
            temp_df.drop(columns=['match_key', 'clean_name', 'clean_id'], inplace=True)

            # 保存文件并设置格式，避免科学计数法
            with pd.ExcelWriter(temp_path, engine='openpyxl', mode='w') as writer:
                temp_df.to_excel(writer, index=False)

                # 设置指定列为文本格式
                worksheet = writer.sheets['Sheet1']
                for col_name in self.temp_columns['需转文本列']:
                    # 找到列名对应的字母列标
                    col_idx = temp_df.columns.get_loc(col_name)
                    col_letter = chr(65 + col_idx)  # 65是'A'的AScII码
                    # 设置整列格式为文本
                    for cell in worksheet[col_letter]:
                        cell.number_format = '@'

            return temp_path

        except Exception as e:
            print(f"处理文件时发生错误：{e}")
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"已清理异常临时文件：{temp_path}")
            return None

    def copy_to_archive(self):
        # 处理文件并获取临时文件路径
        temp_file = self.add_room_type_to_temp_file()

        if temp_file and os.path.exists(temp_file):
            try:
                shutil.copy2(temp_file, self.target_file_name)
                print(f"文件已成功复制到存档：{self.target_file_name}")
            except Exception as e:
                print(f"复制文件时发生错误：{e}")
            finally:
                os.remove(temp_file)

# 监控点：信号强度，周一9:30更新
class Signal_strength():
    def __init__(self):
        self.down_name = '信号强度'
        self.folder_temp = os.path.join(comprehensive_query_path, self.down_name, 'temp')
        self.concat_path = os.path.join(comprehensive_query_path, self.down_name)
        self.save_path = os.path.join(comprehensive_query_path, self.down_name, '存档')
        self.cities = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                 '0099986', '0099987', '0099988', '0099989', '0099990']
        self.data = {
            'cities': self.cities,
            'search_id': '0438104001',
            'folder_temp': self.folder_temp,
            'out_put': os.path.join(self.concat_path, '信号强度.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
    def down(self):
        clear_folder(self.folder_temp)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data)
        print(res.text)
    def copy_to_archive(self):
        temp_file = os.path.join(self.concat_path, '信号强度.xlsx')
        if not os.path.exists(temp_file):
            print('临时文件不存在，跳过存档')
            return
        # 目标文件名
        arc_name = f'信号强度_{datetime.now().strftime("%Y%m%d15")}.xlsx'
        arc_file = os.path.join(self.save_path, arc_name)
        try:
            shutil.copy2(temp_file, arc_file)
            print(f'文件已成功复制到存档：{arc_file}')
        except Exception as e:
            print(f'复制文件时发生错误：{e}')
        finally:
            os.remove(temp_file)

# 监控点：交流电流、电压,不定时更新
class AC_input():
    def __init__(self):
        # 路径：运行监控-性能查询-[监控点：整流模块]-查询-导出
        self.now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = datetime.now()
        self.down_name='交流输入'
        self.concat_name = os.path.join(comprehensive_query_path, self.down_name)

        self.down_name1='交流输入01相电压Ua'
        self.folder_temp1 = os.path.join(comprehensive_query_path, self.down_name, 'temp1')
        self.output_name1 = os.path.join(self.concat_name, self.down_name1, f'Ua{self.now}.xlsx')

        self.down_name2='交流输入01相电压Ub'
        self.folder_temp2 = os.path.join(comprehensive_query_path, self.down_name, 'temp2')
        self.output_name2 = os.path.join(self.concat_name, self.down_name2, f'Ub{self.now}.xlsx')

        self.down_name3='交流输入01相电压Uc'
        self.folder_temp3 = os.path.join(comprehensive_query_path, self.down_name, 'temp3')
        self.output_name3 = os.path.join(self.concat_name, self.down_name3, f'Uc{self.now}.xlsx')

        self.down_name4='交流输入01相电流Ia'
        self.folder_temp4 = os.path.join(comprehensive_query_path, self.down_name, 'temp4')
        self.output_name4 = os.path.join(self.concat_name, self.down_name4, f'Ia{self.now}.xlsx')

        self.down_name5='交流输入01相电流Ib'
        self.folder_temp5 = os.path.join(comprehensive_query_path, self.down_name, 'temp5')
        self.output_name5 = os.path.join(self.concat_name, self.down_name5, f'Ib{self.now}.xlsx')

        self.down_name6='交流输入01相电流Ic'
        self.folder_temp6 = os.path.join(comprehensive_query_path, self.down_name, 'temp6')
        self.output_name6 = os.path.join(self.concat_name, self.down_name6, f'Ic{self.now}.xlsx')

        self.cities=['0099977','0099978','0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']
        self.data1 = {
            'cities': self.cities,
            'search_id': '0406101001',
            'folder_temp': self.folder_temp1,
            'out_put': self.output_name1,
            'hours': 2,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data2 = {
            'cities': self.cities,
            'search_id': '0406102001',
            'folder_temp': self.folder_temp2,
            'out_put': self.output_name2,
            'hours': 2,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data3 = {
            'cities': self.cities,
            'search_id': '0406103001',
            'folder_temp': self.folder_temp3,
            'out_put': self.output_name3,
            'hours': 2,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data4 = {
            'cities': self.cities,
            'search_id': '0406107001',
            'folder_temp': self.folder_temp4,
            'out_put': self.output_name4,
            'hours': 2,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data5 = {
            'cities': self.cities,
            'search_id': '0406108001',
            'folder_temp': self.folder_temp5,
            'out_put': self.output_name5,
            'hours': 2,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data6 = {
            'cities': self.cities,
            'search_id': '0406109001',
            'folder_temp': self.folder_temp6,
            'out_put': self.output_name6,
            'hours': 2,
            'content_len': 102400,
            'cookie_user': 1
        }

    def check_file_exists(self, file_path, initial_wait=420, retry_wait=120, max_attempts=30):
        """检查文件是否存在，支持初始等待和重试机制"""
        print(f"开始检查文件: {file_path}")

        # 首次等待10分钟
        print(f"初始等待 {initial_wait / 60} 分钟...")
        time.sleep(initial_wait)

        attempt = 1
        while attempt <= max_attempts:
            if os.path.exists(file_path):
                print(f"文件已找到: {file_path}")
                return True

            print(f"第 {attempt} 次检查: 文件不存在，等待 {retry_wait / 60} 分钟后再次检查")
            time.sleep(retry_wait)
            attempt += 1

        print(f"警告: 已达到最大尝试次数，文件未找到: {file_path}")
        return False

    def down(self):
        while True:
            self.now = datetime.now().strftime("%Y%m%d_%H%M%S")  # 每次循环更新时间戳
            # 每次循环都重新生成输出文件路径
            self.output_name1 = os.path.join(self.concat_name, self.down_name1, f'Ua{self.now}.xlsx')
            self.output_name2 = os.path.join(self.concat_name, self.down_name2, f'Ub{self.now}.xlsx')
            self.output_name3 = os.path.join(self.concat_name, self.down_name3, f'Uc{self.now}.xlsx')
            self.output_name4 = os.path.join(self.concat_name, self.down_name4, f'Ia{self.now}.xlsx')
            self.output_name5 = os.path.join(self.concat_name, self.down_name5, f'Ib{self.now}.xlsx')
            self.output_name6 = os.path.join(self.concat_name, self.down_name6, f'Ic{self.now}.xlsx')

            # 同时更新数据字典中的out_put值
            self.data1['out_put'] = self.output_name1
            self.data2['out_put'] = self.output_name2
            self.data3['out_put'] = self.output_name3
            self.data4['out_put'] = self.output_name4
            self.data5['out_put'] = self.output_name5
            self.data6['out_put'] = self.output_name6

            clear_folder(self.folder_temp1)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data1)
            print(res.text)
            if not self.check_file_exists(self.output_name1):
                print(f"文件 {self.output_name1} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp2)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data2)
            print(res.text)
            if not self.check_file_exists(self.output_name2):
                print(f"文件 {self.output_name2} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp3)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data3)
            print(res.text)
            if not self.check_file_exists(self.output_name3):
                print(f"文件 {self.output_name3} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp4)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data4)
            print(res.text)
            if not self.check_file_exists(self.output_name4):
                print(f"文件 {self.output_name4} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp5)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data5)
            print(res.text)
            if not self.check_file_exists(self.output_name5):
                print(f"文件 {self.output_name5} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp6)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data6)
            print(res.text)
            # 检查文件是否生成
            if not self.check_file_exists(self.output_name6):
                print(f"文件 {self.output_name6} 未成功生成，停止后续操作")
                break
            print("1分钟后开始下一轮循环")
            time.sleep(60)

# 监控点：开关电源电压，周一7:00更新
class battery():
    def __init__(self):
        # 路径：运行监控-性能查询-[监控点：信号强度]-查询-导出
        self.file_name1 = '均充电压设定值'
        self.file_name2 = '浮充电压设定值'
        self.file_name3 = '一级低压脱离设定值'
        self.file_name4 = '二级低压脱离设定值'
        self.down_name_en1 = '开关电源电压'
        self.concat_name = os.path.join(comprehensive_query_path, self.down_name_en1)
        self.file_path1 = os.path.join(comprehensive_query_path, self.down_name_en1, self.file_name1)
        self.file_path2 = os.path.join(comprehensive_query_path, self.down_name_en1, self.file_name2)
        self.file_path3 = os.path.join(comprehensive_query_path, self.down_name_en1, self.file_name3)
        self.file_path4 = os.path.join(comprehensive_query_path, self.down_name_en1, self.file_name4)
        self.cities = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                       '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']
        self.data1 = {
            'cities': self.cities,
            'search_id': '0406143001',
            'folder_temp': self.file_path1,
            'out_put': os.path.join(self.concat_name, '均充电压设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data2 = {
            'cities': self.cities,
            'search_id': '0406144001',
            'folder_temp': self.file_path2,
            'out_put': os.path.join(self.concat_name, '浮充电压设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data3 = {
            'cities': self.cities,
            'search_id': '0406146001',
            'folder_temp': self.file_path3,
            'out_put': os.path.join(self.concat_name, '一级低压脱离设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data4 = {
            'cities': self.cities,
            'search_id': '0406147001',
            'folder_temp': self.file_path4,
            'out_put': os.path.join(self.concat_name, '二级低压脱离设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }

    def down(self):
        for path in [self.file_path1, self.file_path2, self.file_path3, self.file_path4]:
            clear_folder(path)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data1)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data2)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data3)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data4)

# 监控点：开关电源电压（加直流负载电流），周一周三周日7:00更新
class battery_plus():
    def __init__(self):
        # 路径：运行监控-性能查询-[监控点：信号强度]-查询-导出
        self.file_name1 = '均充电压设定值'
        self.file_name2 = '浮充电压设定值'
        self.file_name3 = '一级低压脱离设定值'
        self.file_name4 = '二级低压脱离设定值'
        self.file_name5 = '直流负载电流'
        self.down_path = r"F:\newtowerV2\message\ID_serch\xls"
        self.file_path1 = os.path.join(self.down_path, self.file_name1)
        self.file_path2 = os.path.join(self.down_path, self.file_name2)
        self.file_path3 = os.path.join(self.down_path, self.file_name3)
        self.file_path4 = os.path.join(self.down_path, self.file_name4)
        self.file_path5 = os.path.join(self.down_path, self.file_name5)
        self.cities = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                       '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']
        self.data1 = {
            'cities': self.cities,
            'search_id': '0406143001',
            'folder_temp': self.file_path1,
            'out_put': os.path.join(self.down_path, '均充电压设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1,
            'csv': True

        }
        self.data2 = {
            'cities': self.cities,
            'search_id': '0406144001',
            'folder_temp': self.file_path2,
            'out_put': os.path.join(self.down_path, '浮充电压设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1,
            'csv': True

        }
        self.data3 = {
            'cities': self.cities,
            'search_id': '0406146001',
            'folder_temp': self.file_path3,
            'out_put': os.path.join(self.down_path, '一级低压脱离设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1,
            'csv': True

        }
        self.data4 = {
            'cities': self.cities,
            'search_id': '0406147001',
            'folder_temp': self.file_path4,
            'out_put': os.path.join(self.down_path, '二级低压脱离设定值.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1,
            'csv': True

        }
        self.data5 = {
            'cities': self.cities,
            'search_id': '0406112001',
            'folder_temp': self.file_path5,
            'out_put': os.path.join(self.down_path, '直流负载电流.xlsx'),
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1,
            'csv': True

        }

    def down(self):
        for path in [self.file_path1, self.file_path2, self.file_path3, self.file_path4, self.file_path5]:
            clear_folder(path)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data1)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data2)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data3)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data4)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data5)

        self.check_dc_load_today()

    def check_dc_load_today(self):
        """整合文件存在检查和日期检查（最多重试 10 次，否则抛异常）"""
        target_file = self.data5['out_put']  # 目标文件路径
        today = datetime.today().date()  # 今天的日期（仅日期部分）

        print(f"开始等待 50 分钟，等待文件下载完成...")
        time.sleep(50 * 60)  # 初始等待 50 分钟

        max_retry = 10
        retry_cnt = 0  # 已重试次数

        while True:
            # 检查文件是否存在
            if not os.path.exists(target_file):
                retry_cnt += 1
                if retry_cnt >= max_retry:
                    raise RuntimeError(
                        f"文件 {target_file} 连续 {max_retry} 次检查均不存在，终止等待。"
                    )
                print(f"文件 {target_file} 不存在，5 分钟后再次检查（第 {retry_cnt}/{max_retry} 次）...")
                time.sleep(5 * 60)
                continue

            # 获取文件最后修改时间并提取日期
            modify_timestamp = os.path.getmtime(target_file)
            modify_date = datetime.fromtimestamp(modify_timestamp).date()

            # 检查是否为今天的文件
            if modify_date == today:
                print(f"文件 {target_file} 是最新的，程序结束。")
                break
            else:
                retry_cnt += 1
                if retry_cnt >= max_retry:
                    raise RuntimeError(
                        f"文件 {target_file} 连续 {max_retry} 次检查均非今日修改（最后修改日期：{modify_date}），终止等待。"
                    )
                print(
                    f"文件 {target_file} 非今天更新（最后修改日期：{modify_date}），"
                    f"5 分钟后再次检查（第 {retry_cnt}/{max_retry} 次）..."
                )
                time.sleep(5 * 60)

#监控点：直流电压、直流负载电流,不定时更新
class direct_current():
    def __init__(self):
        self.now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.down_name='直流电压直流负载总电流'
        self.down_name1='直流电压'
        self.folder_temp1 = os.path.join(comprehensive_query_path, self.down_name, 'temp1')
        self.concat_name1 = os.path.join(comprehensive_query_path, self.down_name, self.down_name1)
        self.output_name1 = os.path.join(self.concat_name1, f'直流电压{self.now}.xlsx')

        self.down_name2='直流负载总电流'
        self.folder_temp2 = os.path.join(comprehensive_query_path, self.down_name, 'temp2')
        self.concat_name2 = os.path.join(comprehensive_query_path, self.down_name, self.down_name2)
        self.output_name2 = os.path.join(self.concat_name2, f'直流负载总电流{self.now}.xlsx')
        self.cities=['0099977','0099978','0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']

        self.data1 = {
            'cities': self.cities,
            'search_id': '0406111001',
            'folder_temp': self.folder_temp1,
            'out_put': self.output_name1,
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
        self.data2 = {
            'cities': self.cities,
            'search_id': '0406112001',
            'folder_temp': self.folder_temp2,
            'out_put': self.output_name2,
            'hours': 24,
            'content_len': 102400,
            'cookie_user': 1
        }
    def down(self):
        # 路径：运行监控-性能查询-[监控点：直流电压]-查询-导出
        clear_folder(self.folder_temp1)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data1)
        print(res.text)

        # 路径：运行监控-性能查询-[监控点：直流负载总电压]-查询-导出
        clear_folder(self.folder_temp2)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data2)
        print(res.text)

# 监控点：交流电流、电压，直流电压、电流,不定时更新
class AC_input_plus():
    def __init__(self):
        # 路径：运行监控-性能查询-[监控点：整流模块]-查询-导出
        self.now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = datetime.now()
        self.down_name='交流输入'
        self.down_name9='直流电压直流负载总电流'
        self.concat_name = os.path.join(comprehensive_query_path, self.down_name)
        self.concat_name1 = os.path.join(comprehensive_query_path, self.down_name9)

        self.down_name1='交流输入01相电压Ua'
        self.folder_temp1 = os.path.join(comprehensive_query_path, self.down_name, 'temp1')
        self.output_name1 = os.path.join(self.concat_name, self.down_name1, f'Ua{self.now}.xlsx')

        self.down_name2='交流输入01相电压Ub'
        self.folder_temp2 = os.path.join(comprehensive_query_path, self.down_name, 'temp2')
        self.output_name2 = os.path.join(self.concat_name, self.down_name2, f'Ub{self.now}.xlsx')

        self.down_name3='交流输入01相电压Uc'
        self.folder_temp3 = os.path.join(comprehensive_query_path, self.down_name, 'temp3')
        self.output_name3 = os.path.join(self.concat_name, self.down_name3, f'Uc{self.now}.xlsx')

        self.down_name4='交流输入01相电流Ia'
        self.folder_temp4 = os.path.join(comprehensive_query_path, self.down_name, 'temp4')
        self.output_name4 = os.path.join(self.concat_name, self.down_name4, f'Ia{self.now}.xlsx')

        self.down_name5='交流输入01相电流Ib'
        self.folder_temp5 = os.path.join(comprehensive_query_path, self.down_name, 'temp5')
        self.output_name5 = os.path.join(self.concat_name, self.down_name5, f'Ib{self.now}.xlsx')

        self.down_name6='交流输入01相电流Ic'
        self.folder_temp6 = os.path.join(comprehensive_query_path, self.down_name, 'temp6')
        self.output_name6 = os.path.join(self.concat_name, self.down_name6, f'Ic{self.now}.xlsx')

        self.down_name7='直流电压'
        self.folder_temp7 = os.path.join(comprehensive_query_path, self.down_name9, 'temp7')
        self.concat_name7 = os.path.join(comprehensive_query_path, self.down_name9, self.down_name7)
        self.output_name7 = os.path.join(self.concat_name7, f'直流电压{self.now}.xlsx')

        self.down_name8='直流负载总电流'
        self.folder_temp8 = os.path.join(comprehensive_query_path, self.down_name9, 'temp8')
        self.concat_name8 = os.path.join(comprehensive_query_path, self.down_name9, self.down_name8)
        self.output_name8 = os.path.join(self.concat_name8, f'直流负载总电流{self.now}.xlsx')
        self.cities=['0099977','0099978','0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']


        self.data1 = {
            'cities': self.cities,
            'search_id': '0406101001',
            'folder_temp': self.folder_temp1,
            'out_put': self.output_name1,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
        self.data2 = {
            'cities': self.cities,
            'search_id': '0406102001',
            'folder_temp': self.folder_temp2,
            'out_put': self.output_name2,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
        self.data3 = {
            'cities': self.cities,
            'search_id': '0406103001',
            'folder_temp': self.folder_temp3,
            'out_put': self.output_name3,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
        self.data4 = {
            'cities': self.cities,
            'search_id': '0406107001',
            'folder_temp': self.folder_temp4,
            'out_put': self.output_name4,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
        self.data5 = {
            'cities': self.cities,
            'search_id': '0406108001',
            'folder_temp': self.folder_temp5,
            'out_put': self.output_name5,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
        self.data6 = {
            'cities': self.cities,
            'search_id': '0406109001',
            'folder_temp': self.folder_temp6,
            'out_put': self.output_name6,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
        self.data7 = {
            'cities': self.cities,
            'search_id': '0406111001',
            'folder_temp': self.folder_temp7,
            'out_put': self.output_name7,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
        self.data8 = {
            'cities': self.cities,
            'search_id': '0406112001',
            'folder_temp': self.folder_temp8,
            'out_put': self.output_name8,
            'hours': 2,
            'content_len': 1024*50,
            'cookie_user': 1
        }
    def check_file_exists(self, file_path, initial_wait=420, retry_wait=120, max_attempts=30):
        """检查文件是否存在，支持初始等待和重试机制"""
        print(f"开始检查文件: {file_path}")

        # 首次等待10分钟
        print(f"初始等待 {initial_wait / 60} 分钟...")
        time.sleep(initial_wait)

        attempt = 1
        while attempt <= max_attempts:
            if os.path.exists(file_path):
                print(f"文件已找到: {file_path}")
                return True

            print(f"第 {attempt} 次检查: 文件不存在，等待 {retry_wait / 60} 分钟后再次检查")
            time.sleep(retry_wait)
            attempt += 1

        print(f"警告: 已达到最大尝试次数，文件未找到: {file_path}")
        return False

    def down(self):
        while True:
            self.now = datetime.now().strftime("%Y%m%d_%H%M%S")  # 每次循环更新时间戳
            # 每次循环都重新生成输出文件路径
            self.output_name1 = os.path.join(self.concat_name, self.down_name1, f'Ua{self.now}.xlsx')
            self.output_name2 = os.path.join(self.concat_name, self.down_name2, f'Ub{self.now}.xlsx')
            self.output_name3 = os.path.join(self.concat_name, self.down_name3, f'Uc{self.now}.xlsx')
            self.output_name4 = os.path.join(self.concat_name, self.down_name4, f'Ia{self.now}.xlsx')
            self.output_name5 = os.path.join(self.concat_name, self.down_name5, f'Ib{self.now}.xlsx')
            self.output_name6 = os.path.join(self.concat_name, self.down_name6, f'Ic{self.now}.xlsx')
            self.output_name7 = os.path.join(self.concat_name1, self.down_name7, f'直流电压{self.now}.xlsx')
            self.output_name8 = os.path.join(self.concat_name1, self.down_name8, f'直流负载总电流{self.now}.xlsx')

            # 同时更新数据字典中的out_put值
            self.data1['out_put'] = self.output_name1
            self.data2['out_put'] = self.output_name2
            self.data3['out_put'] = self.output_name3
            self.data4['out_put'] = self.output_name4
            self.data5['out_put'] = self.output_name5
            self.data6['out_put'] = self.output_name6
            self.data7['out_put'] = self.output_name7
            self.data8['out_put'] = self.output_name8

            clear_folder(self.folder_temp1)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data1)
            print(res.text)
            if not self.check_file_exists(self.output_name1):
                print(f"文件 {self.output_name1} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp2)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data2)
            print(res.text)
            if not self.check_file_exists(self.output_name2):
                print(f"文件 {self.output_name2} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp3)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data3)
            print(res.text)
            if not self.check_file_exists(self.output_name3):
                print(f"文件 {self.output_name3} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp4)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data4)
            print(res.text)
            if not self.check_file_exists(self.output_name4):
                print(f"文件 {self.output_name4} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp5)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data5)
            print(res.text)
            if not self.check_file_exists(self.output_name5):
                print(f"文件 {self.output_name5} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp6)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data6)
            print(res.text)
            # 检查文件是否生成
            if not self.check_file_exists(self.output_name6):
                print(f"文件 {self.output_name6} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp7)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data7)
            print(res.text)
            if not self.check_file_exists(self.output_name7):
                print(f"文件 {self.output_name7} 未成功生成，停止后续操作")
                break

            clear_folder(self.folder_temp8)
            res = requests.post(url='http://10.19.6.250:5002/download', data=self.data8)
            print(res.text)
            # 检查文件是否生成
            if not self.check_file_exists(self.output_name8):
                print(f"文件 {self.output_name8} 未成功生成，停止后续操作")
                break
            print("1分钟后开始下一轮循环")
            time.sleep(60)

class temp():
    def __init__(self):
        self.now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.down_name='临时'
        self.folder_temp = os.path.join(comprehensive_query_path, self.down_name, 'temp')
        self.concat_name = os.path.join(comprehensive_query_path, self.down_name)
        self.output_name = os.path.join(self.concat_name, f'{self.now}.xlsx')

        self.cities=['0099977','0099978','0099979', '0099980', '0099981', '0099982', '0099983', '0099984',
                '0099985', '0099986', '0099987', '0099988', '0099989', '0099990']

        self.data = {
            'cities': self.cities,
            'search_id': '0415102001',
            'folder_temp': self.folder_temp,
            'out_put': self.output_name,
            'hours': 24,
            'content_len': 1024*20,
            'cookie_user': 1
        }
    def down(self):
        # 路径：运行监控-性能查询-[监控点：直流电压]-查询-导出
        clear_folder(self.folder_temp)
        res = requests.post(url='http://10.19.6.250:5002/download', data=self.data)
        print(res.text)

if __name__ == '__main__':
    # temperature().run()
    # temperature_copy().copy_to_archive()
    # Signal_strength().down()
    Signal_strength().copy_to_archive()
    # AC_input().down()
    # AC_input_plus().down()
    # battery().down()
    # battery_plus().down()
    # direct_current().down()
    # Rectifier_module().down()
    # temp().down()
