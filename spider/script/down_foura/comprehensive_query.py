from core.sql import sql_orm
import shutil
import os
import pandas as pd
import time
import pythoncom
import win32com.client as win32
from datetime import datetime
from core.config import settings
from spider.script.down_foura.foura_spider_universal import Performence,PerformenceBySiteList

"""
性能查询监控点-环境温度(0418101001)-每天15:00更新+存档
"""
class Temperature:
    def __init__(self):
        self.cities = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982',
                       '0099983', '0099984', '0099985', '0099986', '0099987', '0099988',
                       '0099989', '0099990']
        self.db_fields = ['省', '市', '区县', '站址', '站址运维ID', '设备名称', '设备厂家',
                          '设备型号', '设备ID', '设备资源编码', '信号量ID', '监控点',
                          '时间', '实测值', '单位', '状态', '性能数据来源']
        self.formatted_time = datetime.now().strftime('%Y%m%d15')
        self.csv_path = settings.resolve_path(f'spider/down/comprehensive_query/环境温度/环境温度.csv')
        self.station_file = settings.resolve_path(f'spider/down/station/站址信息.xlsx')
        self.archive_file = settings.resolve_path(f'updatenas/temperature/"{self.formatted_time}.xlsx')

    def down(self):
        """下载温度数据"""
        out_dir = settings.resolve_path('spider/down/comprehensive_query/环境温度')
        Performence().main(
            self.cities,
            '0418101001',
            os.path.join(out_dir, 'temp'),
            f"{out_dir}/环境温度.xlsx",
            csv=True
        )

    def df_process(self):
        """CSV数据清洗并存入数据库"""
        df = pd.read_csv(self.csv_path, dtype=str)
        df.columns = df.columns.str.strip()

        # 提取数据库字段
        df_db = pd.DataFrame({field: df[field] if field in df.columns else None
                              for field in self.db_fields})
        df_db = df_db[df_db["设备ID"].notna() & (df_db["设备ID"] != "")]
        df_db = df_db.where(pd.notna(df_db), None)

        # 清空并插入
        with sql_orm(database='core').session_scope() as temp:
            sql, Base = temp
            sql.query(Base.classes.temperature).delete()
            for _, row in df_db.iterrows():
                sql.add(Base.classes.temperature(**row.to_dict()))
            sql.commit()

        return df_db  # 返回数据供后续使用

    def archive(self, df):
        """添加机房类型列并保存存档"""
        # 读取站址信息
        station_df = pd.read_excel(self.station_file, dtype=str)

        # 找到关键列
        station_id_col = [c for c in station_df.columns if '运维ID' in c][0]
        station_name_col = [c for c in station_df.columns if '站址名称' in c][0]
        room_type_col = [c for c in station_df.columns if '机房类型' in c][0]

        # 创建匹配键映射
        station_df['key'] = station_df[station_id_col].str.strip() + "|" + station_df[station_name_col].str.strip()
        room_map = dict(zip(station_df['key'], station_df[room_type_col]))

        # 匹配机房类型
        df['key'] = df['站址运维ID'].str.strip() + "|" + df['站址'].str.strip()
        df.insert(17, '机房类型', df['key'].map(room_type_col))
        df.drop('key', axis=1, inplace=True)

        # 保存存档
        os.makedirs(os.path.dirname(self.archive_file), exist_ok=True)
        df.to_excel(self.archive_file, index=False)

    def main(self):
        self.down()
        df = self.df_process()
        self.archive(df)

"""
性能查询监控点-信号强度(0438104001)-每周一早上9:30更新+存档
"""
class SignalStrength:
    def __init__(self):
        self.cities = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982',
                       '0099983', '0099984', '0099985', '0099986', '0099987', '0099988',
                       '0099989', '0099990']
        self.down_name_en = "signal_strength"
        self.down_name = '信号强度.xlsx'
        self.model_name = '模板.xlsx'
        self.output_name = '信号强度_结果.xlsx'
        self.out_dir = settings.resolve_path('spider/down/comprehensive_query/信号强度')
        self.save_path = settings.resolve_path('updatenas/signal_strength')
        self.fsu_path = settings.resolve_path('spider/down/fsu_chaxun_all/fsu清单.xlsx')
        self.station_path = settings.resolve_path('spider/down/station/站址信息.xlsx')
        self.temp_file = os.path.join(self.out_dir, '信号强度.xlsx')
        self.model_path = os.path.join(self.out_dir,self.model_name)
        self.output_path = os.path.join(self.out_dir,'output',self.output_name)

    def down(self):
        """下载信号强度数据"""
        Performence().main(
            self.cities,
            '0418101001',
            os.path.join(self.out_dir, 'temp'),
            self.temp_file,
            csv=True
        )
    def excel_process(self):
        # 初始化 COM 库
        pythoncom.CoInitialize()
        try:
            xl = win32.gencache.EnsureDispatch('Excel.Application')  # 开启excel软件
            xl.Visible = False
            xl.DisplayAlerts = False

            # 打开模板文件
            workbook_main = xl.Workbooks.Open(self.model_path)

            # === 清单 ===
            workbook_data = xl.Workbooks.Open(self.temp_file)
            sheet_data = workbook_data.Sheets('sheet1')
            sheet_main = workbook_main.Sheets('清单')

            # 动态获取数据的实际范围
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A2:Q{last_row}')

            # 1. 只清除E2-U列的数据（不包含第一行表头）
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"E2:U{last_clear_row}").ClearContents()

            # 2. 复制新数据到 E-U 列
            source_range.Copy()
            sheet_main.Range('E2').PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            xl.CalculateFull()

            workbook_data.Close(SaveChanges=False)
            # 3. 使用AutoFill填充AJ-AN列的数据到last_row
            if last_row > 2:  # 确保有足够的行进行填充
                # 获取AJ-AN列的第二行数据范围
                fill_source = sheet_main.Range('A2:D2')
                # 确定目标范围（AJ-AN列从第2行到last_row行）
                fill_target = sheet_main.Range(f'A2:D{last_row}')
                # 使用AutoFill方法填充数据
                fill_source.AutoFill(Destination=fill_target, Type=win32.constants.xlFillDefault)
            time.sleep(3)

            # 4. 对R列实测值做分列操作，但不实际分列
            # 设置分列的参数（固定宽度和列数据类型为常规）
            last_row_main = sheet_main.Cells(sheet_main.Rows.Count, 1).End(win32.constants.xlUp).Row
            column_r_range = sheet_main.Range(f'R2:R{last_row_main}')
            column_r_range.TextToColumns(Destination=sheet_main.Range(f'R2'), DataType=win32.constants.xlFixedWidth, FieldInfo=[(1, win32.constants.xlGeneralFormat)])

            # === FSU ===
            workbook_data = xl.Workbooks.Open(self.fsu_path)
            sheet_data = workbook_data.Sheets('Sheet1')
            sheet_main = workbook_main.Sheets('fsu')

            # 动态获取数据的实际范围
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A2:CL{last_row}')

            # 1. 清除数据（不包含第一行表头）
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A2:CL{last_clear_row}").ClearContents()

            # 2. 复制新数据
            source_range.Copy()
            sheet_main.Range('A2').PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False

            # === station ===
            workbook_data = xl.Workbooks.Open(self.station_path)
            sheet_data = workbook_data.Sheets('Sheet1')
            sheet_main = workbook_main.Sheets('站址信息')

            # 动态获取数据的实际范围
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A3:FE{last_row}')

            # 1. 清除数据（不包含第一行表头）
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A2:FE{last_clear_row}").ClearContents()

            # 2. 复制新数据
            source_range.Copy()
            sheet_main.Range('A2').PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            workbook_main.SaveAs(self.output_path)
            workbook_main.Close()
            xl.Quit()  # 关闭Excel应用程序
            print('物联卡信号强度通报更新完毕')
        except Exception as e:
            raise
        finally:
            # 释放 COM 库
            pythoncom.CoUninitialize()
    def archive(self):
        """复制到存档（带时间戳）"""
        if not os.path.exists(self.temp_file):
            print('下载文件不存在，跳过存档')
            return False
        filename = f'{datetime.now().strftime("%Y%m%d")}.xlsx'
        target = os.path.join(self.save_path, filename)

        try:
            shutil.copy2(self.temp_file, target)
            print(f'已存档：{target}')
            return True
        except Exception as e:
            print(f'存档失败：{e}')
            return False

    def main(self):
        self.down()
        self.archive()

"""
爬取性能查询-直流负载电流(0406112001)+均充电压设定值(0406143001)+直流负载电流(0406112001)+
二级低压脱离设定值(0406147001)+一级低压脱离设定值(0406146001)+浮充电压设定值(0406144001)-
每周一早上6:00更新
"""
class BatteryOrder:
    def __init__(self):
        self.cities = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982',
                       '0099983', '0099984', '0099985', '0099986', '0099987', '0099988',
                       '0099989', '0099990']
        self.out_dir1 = settings.resolve_path('spider/down/comprehensive_query/均充电压设定值')
        self.out_dir2 = settings.resolve_path('spider/down/comprehensive_query/浮充电压设定值')
        self.out_dir3 = settings.resolve_path('spider/down/comprehensive_query/一级低压脱离设定值')
        self.out_dir4 = settings.resolve_path('spider/down/comprehensive_query/二级低压脱离设定值')
        self.out_dir5 = settings.resolve_path('spider/down/comprehensive_query/直流负载电流')
        self.down_name_en = "battery_order"
        self.file_name1 = '均充电压设定值.xlsx'
        self.file_name2 = '浮充电压设定值.xlsx'
        self.file_name3 = '一级低压脱离设定值.xlsx'
        self.file_name4 = '二级低压脱离设定值.xlsx'
        self.file_path1 = os.path.join(self.out_dir1, self.file_name1)
        self.file_path2 = os.path.join(self.out_dir2, self.file_name2)
        self.file_path3 = os.path.join(self.out_dir3, self.file_name3)
        self.file_path4 = os.path.join(self.out_dir4, self.file_name4)
        self.output_name = settings.resolve_path('spider/down/battery__order', 'output', '开关电源电压设置修改情况通报-结果.xlsx')
        self.model_path = settings.resolve_path('spider/down/battery__order', '模板.xlsx')
    def down(self):
        """下载信号强度数据"""
        Performence().main(self.cities, '0406143001', os.path.join(self.out_dir1, 'temp'),f'{self.out_dir1}/均充电压设定值.xlsx', csv=True)
        Performence().main(self.cities, '0406144001', os.path.join(self.out_dir2, 'temp'),f'{self.out_dir2}/浮充电压设定值.xlsx', csv=True)
        Performence().main(self.cities, '0406146001', os.path.join(self.out_dir3, 'temp'),f'{self.out_dir3}/一级低压脱离设定值.xlsx', csv=True)
        Performence().main(self.cities, '0406147001', os.path.join(self.out_dir4, 'temp'),f'{self.out_dir4}/二级低压脱离设定值.xlsx', csv=True)
        Performence().main(self.cities, '0406112001', os.path.join(self.out_dir5, 'temp'),f'{self.out_dir5}/直流负载电流.xlsx', csv=True)
    def excel_process(self):
        """
        处理Excel文件，将指定文件夹中的数据文件内容复制到主表文件中。

        :param index_path: 文件夹路径
        """
        print('1、把数据文件和通报模板放在同一文件夹下')
        print('2、打开上述文件，如果提示保护视图则取消（报错大概率是这个问题），如果提示别的东西请点击掉，保证程序能够编辑文档')
        # 初始化 COM 库
        pythoncom.CoInitialize()
        try:
            # xl = win32.Dispatch('Excel.Application')
            xl = win32.gencache.EnsureDispatch('Excel.Application')  # 开启excel软件
            xl.Visible = False
            xl.DisplayAlerts = False  # 禁用警告提示
            # 打开模板文件
            workbook_main = xl.Workbooks.Open(self.model_path)

            # === 处理均充电压设定值 ===
            workbook_data = xl.Workbooks.Open(self.file_path1)
            workbook_data.Application.DisplayAlerts = False  # 单独禁用警告
            sheet_data = workbook_data.Sheets('sheet1')
            sheet_main = workbook_main.Sheets('均充电压清单')

            # 动态获取数据的实际范围
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A2:P{last_row}')

            # 1. 只清除A-P列的数据（不包含第一行表头）
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A2:P{last_clear_row}").ClearContents()

            # 2. 复制新数据到 A-P 列
            source_range.Copy()
            sheet_main.Range('A2').PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            xl.CalculateFull()
            workbook_data.Close(SaveChanges=False)
            # 3. 对N列执行分列操作（固定宽度，常规格式，但不实际分列）
            last_data_row = sheet_main.Cells(sheet_main.Rows.Count, "N").End(win32.constants.xlUp).Row
            if last_data_row > 1:
                # 选择N列数据范围
                n_column_range = sheet_main.Range(f"N2:N{last_data_row}")

                # 执行分列操作但不实际分列
                n_column_range.TextToColumns(
                    Destination=n_column_range,
                    DataType=win32.constants.xlFixedWidth,
                    FieldInfo=[(0, 1)],  # 只设置一个字段
                    DecimalSeparator=".",
                    TrailingMinusNumbers=True
                )
                time.sleep(3)

            # === 处理浮充电压设定值 ===
            workbook_data = xl.Workbooks.Open(self.file_path2)
            workbook_data.Application.DisplayAlerts = False  # 单独禁用警告
            sheet_data = workbook_data.Sheets('sheet1')
            sheet_main = workbook_main.Sheets('浮充电压清单')

            # 动态获取数据的实际范围
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A2:P{last_row}')

            # 1. 只清除A-P列的数据（不包含第一行表头）
            last_clear_row = sheet_main.UsedRange.Rows.Count
            if last_clear_row > 1:
                sheet_main.Range(f"A2:P{last_clear_row}").ClearContents()

            # 2. 复制新数据到 A-P 列
            source_range.Copy()
            sheet_main.Range('A2').PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False
            # 重新计算工作表以确保公式更新
            xl.CalculateFull()
            workbook_data.Close(SaveChanges=False)
            # 3. 对N列执行分列操作（固定宽度，常规格式，但不实际分列）
            last_data_row = sheet_main.Cells(sheet_main.Rows.Count, "N").End(win32.constants.xlUp).Row
            if last_data_row > 1:
                # 选择N列数据范围
                n_column_range = sheet_main.Range(f"N2:N{last_data_row}")

                # 执行分列操作但不实际分列
                n_column_range.TextToColumns(
                    Destination=n_column_range,
                    DataType=win32.constants.xlFixedWidth,
                    FieldInfo=[(0, 1)],  # 只设置一个字段
                    DecimalSeparator=".",
                    TrailingMinusNumbers=True
                )
            time.sleep(3)

            # === 处理一级低压脱离设定值 ===
            workbook_data = xl.Workbooks.Open(self.file_path3)
            workbook_data.Application.DisplayAlerts = False

            sheet_data = workbook_data.Sheets('sheet1')
            sheet_main = workbook_main.Sheets('一级低压脱离设定值清单')
            # 动态获取数据的实际范围
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:P{last_row}')  # 从A2开始复制
            # 清空目标表的内容
            sheet_main.Cells.ClearContents()
            # 复制和粘贴
            source_range.Copy()
            sheet_main.Range('A1').PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False  # 释放剪切板
            workbook_data.Close(SaveChanges=False)
            # 3. 对N列执行分列操作（固定宽度，常规格式，但不实际分列）
            last_data_row = sheet_main.Cells(sheet_main.Rows.Count, "N").End(win32.constants.xlUp).Row
            if last_data_row > 1:
                # 选择N列数据范围
                n_column_range = sheet_main.Range(f"N2:N{last_data_row}")

                # 执行分列操作但不实际分列
                n_column_range.TextToColumns(
                    Destination=n_column_range,
                    DataType=win32.constants.xlFixedWidth,
                    FieldInfo=[(0, 1)],  # 只设置一个字段
                    DecimalSeparator=".",
                    TrailingMinusNumbers=True
                )
            time.sleep(3)

            # 处理第四个数据文件（二级低压脱离设定值）
            workbook_data = xl.Workbooks.Open(self.file_path4)
            workbook_data.Application.DisplayAlerts = False  # 单独禁用警告

            sheet_data = workbook_data.Sheets('sheet1')
            sheet_main = workbook_main.Sheets('二级低压脱离设定值清单')
            # 动态获取数据的实际范围
            last_row = sheet_data.Cells(sheet_data.Rows.Count, 1).End(win32.constants.xlUp).Row
            source_range = sheet_data.Range(f'A1:P{last_row}')  # 从A2开始复制
            # 清空目标表的内容
            sheet_main.Cells.ClearContents()
            # 复制和粘贴
            source_range.Copy()
            sheet_main.Range('A1').PasteSpecial(Paste=win32.constants.xlPasteValues)
            xl.CutCopyMode = False  # 释放剪切板
            workbook_data.Close(SaveChanges=False)
            # 3. 对N列执行分列操作（固定宽度，常规格式，但不实际分列）
            last_data_row = sheet_main.Cells(sheet_main.Rows.Count, "N").End(win32.constants.xlUp).Row
            if last_data_row > 1:
                # 选择N列数据范围
                n_column_range = sheet_main.Range(f"N2:N{last_data_row}")

                # 执行分列操作但不实际分列
                n_column_range.TextToColumns(
                    Destination=n_column_range,
                    DataType=win32.constants.xlFixedWidth,
                    FieldInfo=[(0, 1)],  # 只设置一个字段
                    DecimalSeparator=".",
                    TrailingMinusNumbers=True
                )
            workbook_main.SaveAs(self.output_name)
            workbook_main.Close()
            xl.Quit()  # 关闭Excel应用程序
            print('已全部完成')
        except Exception as e:
            raise
        finally:
            # 释放 COM 库
            pythoncom.CoUninitialize()

    def main(self):
        self.down()

if __name__ == '__main__':
    Temperature().main()
    # Signal_strength().main()
    # AC_input().down()
    # AC_input_plus().down()
    # battery().down()
    # BatteryOrder().down()
    # direct_current().down()
    # Rectifier_module().down()
    # temp().down()
