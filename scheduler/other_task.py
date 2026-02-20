import pandas as pd
from core.sql import sql_orm
import shutil
import os
import datetime
from sqlalchemy import text
from core.config import settings

TEMP_PATH_ONE_MONTH=settings.resolve_path("spider/down/temp_folder_one_month")
TEMP_PATH_ONE_DAY=settings.resolve_path("spider/down/temp_folder_one_day")
def gen_fsu_static(path):
    with sql_orm().session_scope() as temp:
        session, Base = temp
        pojo = Base.classes.fsu_brokentime_log
        pojo_brokentimes = Base.classes.fsu_brokentimes_log
        query = session.query(pojo).filter(pojo.begin_time>datetime.datetime.now().replace(hour=0,minute=0,second=0)).statement
        df = pd.read_sql(query, session.bind)
        query = session.query(pojo_brokentimes).statement
        df_brokentimes = pd.read_sql(query, session.bind)
        df_brokentimes = df_brokentimes.loc[df_brokentimes['broken_times'] != 0]
    df = df.rename(columns={'id': '站址', 'begin_time': '离线时间'})
    df_brokentimes = df_brokentimes.rename(
        columns={'id': '站址', 'begin_time': '最近离线时间', 'broken_times': '今日发生次数(7点至现在)'})
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='离线记录(今日)', index=False)
        df_brokentimes.to_excel(writer, sheet_name='统计次数(今日)', index=False)

def task_2359():
    # 导出地震记录
    with sql_orm().session_scope() as temp:
        session, Base = temp
        pojo = Base.classes.earthquake_dialog
        query = session.query(pojo).statement
        df = pd.read_sql(query, session.bind)
    df=df.rename(columns={'sitecode':'站址编码', 'sitename':'故障站址','happen':'发生时间', 'recover':'恢复时间', 'duration':'故障持续时间'})
    df=df.drop(columns=['id'])
    df.to_excel(settings.resolve_path('updatenas/earthquake/地震故障记录.xlsx'), index=False)

    # 删除今日下载产生临时文件
    shutil.rmtree(TEMP_PATH_ONE_DAY)
    os.makedirs(TEMP_PATH_ONE_DAY)
    # 删除本月下载产生临时文件
    if datetime.datetime.now().day==1:
        shutil.rmtree(TEMP_PATH_ONE_MONTH)
        os.makedirs(TEMP_PATH_ONE_MONTH)

def task_7():
    # 导出每日fsu离线情况
    path=settings.resolve_path(f'updatenas/fsu/fsu_每日离线统计/fsu每日离线统计{datetime.datetime.now().strftime("%Y%m%d")}.xlsx')
    gen_fsu_static(path=path)

def task_month_1():
    with sql_orm(database='battery_life').session_scope() as temp:
        sql, Base = temp
        now=datetime.datetime.now().replace(day=1)-datetime.timedelta(days=1)
        path=f'F:/newtowerV2/websource/spider_download/DC/{now.strftime("%Y%m")}基站负载电流.xlsx'
        df=pd.read_excel(path,dtype=str,usecols=['运维监控站址ID','月度平均值','信号量名称'])
        pojo=Base.classes.station_dc
        df = df.where(pd.notnull(df), None)  # nan改为空
        df=df.drop_duplicates()
        df=df[df['信号量名称']=='直流负载总电流'].drop(columns=['信号量名称'])
        df = df.rename(columns={'运维监控站址ID':'id','月度平均值':'dc'})
        rows = []
        for index, row in df.iterrows():
            temp = pojo(**row.to_dict())
            rows.append(temp)
        sql.execute(text("truncate station_dc"))
        sql.bulk_save_objects(rows)

