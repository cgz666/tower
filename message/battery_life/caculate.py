from spider.script.down_foura.foura_spider_universal import PerformenceBySiteList,AlarmNow
from core.sql import sql_orm
import datetime
import pandas as pd
from sqlalchemy import and_, text
import os
import re
import requests
from core.utils.send_ding_msg import dingmsg
import uuid
import time
from core.config import settings

class BatteryLifeCaculate():
    def __init__(self):
        self.down_path = settings.resolve_path("message/battery_life/xls/活动告警.csv")
    def init_pojo(self, table_name, **kwargs):
        with sql_orm(database='battery_life').session_scope() as (sql, Base):
            pojo = getattr(Base.classes, table_name)
            temp = pojo(**kwargs)
            sql.merge(temp)
    def calculate_voltage(self):
        df = pd.read_csv(self.down_path, dtype=str, usecols=['站址运维ID', '告警发生时间', '告警名称'])
        df = df[df['告警名称'] == '交流输入停电告警']
        site_ids = df['站址运维ID'].tolist()
        result_df = PerformenceBySiteList().main(site_ids, '0406111001',timedelta=20)
        df = df.merge(result_df[['站址运维ID', '实测值']], on='站址运维ID', how='left')
        df['实测值'] = df['实测值'].fillna('')
        df.rename(columns={'实测值': '直流电压'}, inplace=True)
        df = df[df['直流电压'] != '']
        df['告警发生时间'] = pd.to_datetime(df['告警发生时间'])
        df['续航'] = round((datetime.datetime.now() - df['告警发生时间']).dt.total_seconds() / 3600, 2)
        df['类型'] = '运维停电告警历时'
        df = df[df['续航'] >= 0]
        df = df[df['直流电压'].astype(float) >= 53]

        with sql_orm(database='battery_life').session_scope() as (sql, Base):
            pojo = Base.classes.voltage
            for _, row in df.iterrows():
                existing_record = sql.query(pojo).filter(pojo.id == row['站址运维ID'], pojo.caculate_type == row['类型']).first()
                if (existing_record is None) or (row['续航'] >= existing_record.battery_life) or ((datetime.datetime.now() - existing_record.outage_time).total_seconds() >= 15768000) or (existing_record.battery_life > 10):
                    self.init_pojo('voltage', id=row['站址运维ID'], outage_time=row['告警发生时间'], caculate_type=row['类型'], voltage=row['直流电压'], voltage_get_time=datetime.datetime.now(), battery_life=row['续航'])
    def calculate_zhiliu_voltage(self):
        df = pd.read_csv(self.down_path, dtype=str, usecols=['站址运维ID', '告警发生时间', '告警名称', '告警详情'])
        df1 = df[df['告警名称'] == '交流输入停电告警'][['站址运维ID', '告警发生时间']]
        df2 = df[df['告警名称'] == '直流输出电压过低告警'][['站址运维ID', '告警发生时间', '告警详情']]
        df2['告警详情'] = df2['告警详情'].apply(lambda x: re.search(r'\d+\.?\d*', x).group() if re.search(r'\d+\.?\d*', x) else None)
        df2 = df2[df2['告警详情'].astype(float) < 50]
        df1['告警发生时间'] = pd.to_datetime(df1['告警发生时间'])
        df2['告警发生时间'] = pd.to_datetime(df2['告警发生时间'])
        df = pd.merge(df1, df2, on='站址运维ID', suffixes=('_df1', '_df2'))
        df['续航'] = round((df['告警发生时间_df2'] - df['告警发生时间_df1']).dt.total_seconds() / 3600, 2)
        df['类型'] = '直流输出电压过低告警历时'
        df = df[df['续航'] >= 0]

        with sql_orm(database='battery_life').session_scope() as (sql, Base):
            pojo = Base.classes.zhiliu_voltage
            for _, row in df.iterrows():
                existing_record = sql.query(pojo).filter(pojo.id == row['站址运维ID'], pojo.caculate_type == row['类型']).first()
                if (existing_record is None) or (row['续航'] >= existing_record.battery_life) or ((datetime.datetime.now() - existing_record.outage_time).total_seconds() >= 15768000) or (existing_record.battery_life > 10):
                    self.init_pojo('zhiliu_voltage', id=row['站址运维ID'], outage_time=row['告警发生时间_df1'], caculate_type=row['类型'], zhiliu_voltage_time=row['告警发生时间_df2'], battery_life=row['续航'])
    def calculate_offline(self):
        with sql_orm(database='自助取数').session_scope() as (sql, Base):
            sql_str = """
            SELECT outage.站址运维ID, offline.告警名称 as 退服告警名称, outage.告警发生时间,
                   offline.告警发生时间 as 退服时间,
                   TIMESTAMPDIFF(MINUTE, outage.告警发生时间, offline.告警发生时间) as 续航
            FROM hbase AS outage
            LEFT JOIN (
                SELECT 站址运维ID, 告警名称, 告警发生时间,
                       ROW_NUMBER() OVER(PARTITION BY 站址运维ID ORDER BY 告警发生时间 ASC) as rn
                FROM hbase
                WHERE (告警名称 = '一级低压脱离告警' OR 告警名称 = '二级低压脱离告警')
                  AND 告警发生日期 >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
            ) AS offline
            ON outage.站址运维ID = offline.站址运维ID
            WHERE outage.告警名称 = '交流输入停电告警'
              AND offline.rn = 1
              AND TIMESTAMPDIFF(MINUTE, outage.告警发生时间, offline.告警发生时间) BETWEEN 0 AND 120;
            """
            res = sql.execute(sql_str)
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        df['告警发生时间'] = pd.to_datetime(df['告警发生时间'])
        df['续航'] = round((df['退服时间'] - df['告警发生时间']).dt.total_seconds() / 3600, 2)
        df['类型'] = '运维一级二级低压脱离'
        df = df[df['续航'] >= 0]

        with sql_orm(database='battery_life').session_scope() as (sql, Base):
            pojo = Base.classes.offline
            for _, row in df.iterrows():
                existing_record = sql.query(pojo).filter(pojo.id == row['站址运维ID'], pojo.caculate_type == row['类型']).first()
                if (existing_record is None) or (row['续航'] > existing_record.battery_life) or ((datetime.datetime.now() - existing_record.offline_time).total_seconds() >= 15768000) or (existing_record.battery_life > 10):
                    self.init_pojo('offline', id=row['站址运维ID'], outage_time=row['告警发生时间'], caculate_type=row['类型'], offline_alarm_name=row['退服告警名称'], offline_time=row['退服时间'], battery_life=row['续航'])
    def calculate_order(self):
        folder = settings.resolve_path('spider_download/yidong_api_order_history/caculate_battery_life')
        for file in os.listdir(folder):
            try:
                path = os.path.join(folder, file)
                df = pd.read_excel(path, dtype=str, usecols=['故障单编码', '工单状态', '派单时间', '站址名称', '申告工单故障分类', '回复内容'])
                df = df.rename(columns={'站址名称': 'site_name', '故障单编码': 'order_id', '派单时间': 'order_time', '申告工单故障分类': 'order_alarm_type', '回复内容': 'order_response'})
                df = df[df['工单状态'] == '归档'].drop(columns=['工单状态'])
                df = df[~df['order_alarm_type'].isin(['非铁塔配套原因故障', '非铁塔原因-其它', '未移交机房站点'])]
                df = df[~df['order_response'].isin(['运营商', '非铁塔'])].fillna('')

                with sql_orm(database='自助取数').session_scope() as (sql, Base):
                    pojo = Base.classes.order_for_battery_life
                    for _, row in df.iterrows():
                        sql.merge(pojo(**row.to_dict()))
            except Exception as e:
                print(e)
                print(file)

        with sql_orm(database='自助取数').session_scope() as (sql, Base):
            sql_str = """
            SELECT outage.站址, outage.站址运维ID, outage.告警发生时间, order_list.order_id, order_list.order_time,
                   TIMESTAMPDIFF(MINUTE, outage.告警发生时间, order_list.order_time) as 续航
            FROM hbase AS outage
            LEFT JOIN (
                SELECT site_name, order_id, order_time,
                       ROW_NUMBER() OVER(PARTITION BY site_name ORDER BY order_time ASC) as rn
                FROM order_for_battery_life
            ) AS order_list
            ON outage.站址 = order_list.site_name
            WHERE outage.告警名称 = '交流输入停电告警'
              AND order_list.rn = 1
              AND TIMESTAMPDIFF(MINUTE, outage.告警发生时间, order_list.order_time) BETWEEN 0 AND 60;
            """
            res = sql.execute(sql_str)
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        df['告警发生时间'] = pd.to_datetime(df['告警发生时间'])
        df['order_time'] = pd.to_datetime(df['order_time'])
        df['续航'] = round((df['order_time'] - df['告警发生时间']).dt.total_seconds() / 3600, 2)
        df['类型'] = '运营商接口工单'
        df = df[df['续航'] >= 0]

        with sql_orm(database='battery_life').session_scope() as (sql, Base):
            pojo = Base.classes.order
            for _, row in df.iterrows():
                existing_record = sql.query(pojo).filter(pojo.id == row['站址运维ID'], pojo.caculate_type == row['类型']).first()
                if (existing_record is None) or (row['续航'] > existing_record.battery_life) or ((datetime.datetime.now() - existing_record.order_time).total_seconds() >= 15768000) or (existing_record.battery_life > 10):
                    self.init_pojo('order', id=row['站址运维ID'], outage_time=row['告警发生时间'], caculate_type=row['类型'], order_id=row['order_id'], order_time=row['order_time'], battery_life=row['续航'])
    def generate_result(self):
        with open(settings.resolve_path( r'message\battery_life\out_put_sql.txt'), 'r', encoding='utf-8') as file:
            sql_script = file.read().replace('\uFEFF', '')
            with sql_orm(database='battery_life').session_scope() as (sql, Base):
                result = sql.execute(text(sql_script))
                df = pd.DataFrame(result.fetchall(), columns=result.keys()).fillna('')
                sql_orm(database='battery_life').save_data_with_delete(df, 'result')
                data = df.to_json(orient='records')
                headers = {'Content-Type': 'application/json'}
                response = requests.post('http://clound.gxtower.cn:3980/tt/wechat_battery_life_save_data', data=data, headers=headers)
    def generate_result_shangdan(self):
        with open(settings.resolve_path( r'message\battery_life\out_put_sql_shangdan.txt'), 'r', encoding='utf-8') as file:
            sql_script = file.read().replace('\uFEFF', '')
        df = sql_orm(database='battery_life').excute_sql(sql_script,return_df=True)
        df = df.fillna('')
        time_mask = pd.to_datetime(df["续航统计时间"], errors='coerce') <= datetime.datetime.now() - datetime.timedelta(
            days=1)
        city_mask = df["地市"] == "南宁"
        mask = time_mask & city_mask
        df = df[mask]

        outage = pd.read_csv(self.down_path, dtype=str, usecols=['站址运维ID', '告警发生时间', '告警名称']).rename(columns={"告警发生时间":"本次停电发生时间"})
        outage = (
            outage[outage['告警名称'] == '交流输入停电告警']
                .drop(columns=['告警名称'])
                .drop_duplicates(subset=['站址运维ID'])
        )
        df = df.merge(outage, on='站址运维ID', how='inner')
        df['unique_key'] = df['站址名称'] + '_' + df['本次停电发生时间']
        key_to_id = {key: str(uuid.uuid5(uuid.NAMESPACE_DNS, key)) for key in df['unique_key'].unique()}
        df['id'] = df['unique_key'].map(key_to_id)
        df["电池续航(小时)"]=df["电池续航(小时)"].astype(float)
        df = df.sort_values(by=["续航计算方法", "站址运维ID", "电池续航(小时)"])
        df = df.drop_duplicates(subset=["续航计算方法", "站址运维ID"], keep='first')
        df.drop(columns=['unique_key'], inplace=True)
        grouped = df.groupby('站址名称')

        def build_battery_message(site_name, group):
            first_row = group.iloc[0]

            common_info = (
                f"站址名称：{site_name}(站址编码：{first_row['站址编码']})\n"
                f"停电时间：{first_row['本次停电发生时间']}"
                f"站址等级：{first_row['运营商站址等级']}\n"
                f"共享关系：{first_row['运营商归属']}\n"
                f"上站距离：{first_row['上站距离']}\n"
                f"上站时间：{first_row['上站时间']}\n"
            )
            battery_lines = [
                f"续航计算方法：{first_row['续航计算方法']}\n续航统计时间：{row['续航统计时间']}\n电池续航：{row['电池续航(小时)']}小时"
                for _, row in group.iterrows()
            ]
            separator = "\n" + "-" * 12 + "\n"
            summary = f"停电告警&续航小于2小时工单，请立即上单！\n\n"
            return summary + common_info + separator + separator.join(battery_lines)

        with sql_orm(database="battery_life").session_scope() as (sql, base):
            BatteryShangdan = base.classes.battery_shangdan
            for site_name, group in grouped:
                id_val = group['id'].iloc[0]
                if sql.query(BatteryShangdan).filter(BatteryShangdan.id == id_val).first():
                    continue
                records = group.to_dict(orient='records')
                sql.add_all(BatteryShangdan(**r) for r in records)
                sql.commit()
                msg = build_battery_message(site_name, group)
                d=dingmsg()
                d.text_at(webhook=d.BATTERY_SHANGDAN, msg=msg)
                time.sleep(5)
                # detail_url = f"http://10.19.6.250:5000/battery_shangdan?id={id}"
                # d = dingmsg()
                # d.card(
                #     webhook=d.TEST,
                #     title="蓄电池续航不足上单提醒",
                #     message=full_message,
                #     detail_url=detail_url
                # )
                print(1)


if __name__ == '__main__':
    # BatteryLifeCaculate().calculate_zhiliu_voltage()
    # BatteryLifeCaculate().calculate_voltage()
    # BatteryLifeCaculate().calculate_offline()
    BatteryLifeCaculate().generate_result()
    # BatteryLifeCaculate().generate_result_shangdan()