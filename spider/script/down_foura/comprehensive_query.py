from core.sql import sql_orm
import shutil
import os
import pandas as pd
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
        # df = self.df_process()
        # self.archive(df)

"""
性能查询监控点-信号强度(0438104001)-每周一早上9:30更新+存档
"""
class SignalStrength:
    def __init__(self):
        self.cities = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982',
                       '0099983', '0099984', '0099985', '0099986', '0099987', '0099988',
                       '0099989', '0099990']
        self.out_dir = settings.resolve_path('spider/down/comprehensive_query/信号强度')
        self.save_path = settings.resolve_path('updatenas/signal_strength')
        self.temp_file = os.path.join(self.out_dir, '信号强度.xlsx')

    def down(self):
        """下载信号强度数据"""
        Performence().main(
            self.cities,
            '0418101001',
            os.path.join(self.out_dir, 'temp'),
            self.temp_file,
            csv=True
        )

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
        self.out_dir1 = settings.resolve_path('spider/down/comprehensive_query/直流负载电流')
        self.out_dir2 = settings.resolve_path('spider/down/comprehensive_query/均充电压设定值')
        self.out_dir3 = settings.resolve_path('spider/down/comprehensive_query/二级低压脱离设定值')
        self.out_dir4 = settings.resolve_path('spider/down/comprehensive_query/一级低压脱离设定值')
        self.out_dir5 = settings.resolve_path('spider/down/comprehensive_query/浮充电压设定值')

    def down(self):
        """下载信号强度数据"""
        Performence().main(self.cities, '0406112001', os.path.join(self.out_dir1, 'temp'),f'{self.out_dir1}/直流负载电流.xlsx', csv=True)
        Performence().main(self.cities, '0406143001', os.path.join(self.out_dir2, 'temp'),f'{self.out_dir2}/均充电压设定值.xlsx', csv=True)
        Performence().main(self.cities, '0406147001', os.path.join(self.out_dir3, 'temp'),f'{self.out_dir3}/二级低压脱离设定值.xlsx', csv=True)
        Performence().main(self.cities, '0406146001', os.path.join(self.out_dir4, 'temp'),f'{self.out_dir4}/一级低压脱离设定值.xlsx', csv=True)
        Performence().main(self.cities, '0406144001', os.path.join(self.out_dir5, 'temp'),f'{self.out_dir5}/浮充电压设定值.xlsx', csv=True)

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
