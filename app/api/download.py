# app/api/download.py
from fastapi import APIRouter, Query
from fastapi.responses import FileResponse
import datetime
import shutil
from core.config import settings
from core.sql import sql_orm

router = APIRouter(tags=["文件下载"])

TEMP_PATH_ONE_DAY = settings.resolve_path("spider/down/temp_folder_one_day")


@router.get("/fsu_hafhour")
async def fsu_hafhour():
    from scheduler.other_task import gen_fsu_static

    folder = settings.resolve_path('spider/down/fsu_hafhour')
    path = f'{folder}/fsu每日离线统计.xlsx'
    zip_filename = settings.resolve_path(
        f"spider/down/temp_folder_one_day/{datetime.datetime.now().timestamp()}_FSU信息")

    try:
        gen_fsu_static(path)
        shutil.make_archive(zip_filename, 'zip', folder)
        return FileResponse(zip_filename + '.zip', filename="FSU信息.zip")
    except Exception as e:
        return {"error": str(e)}


@router.get("/down_wendu_guogao")
async def down_wendu_guogao(
        begin: str = Query(...),
        end: str = Query(...)
):
    folder = settings.resolve_path("spider/down/temp_folder_one_day")
    file_name = f'告警{datetime.datetime.now().timestamp()}.xlsx'

    try:
        df = sql_orm().excute_sql(
            f'select * from hbase where (告警名称 in ("温度过高","温度过高（预告警）"))and(告警发生日期 between "{begin}" and "{end}")',
            return_df=True
        )
        file_path = f'{folder}/{file_name}'
        df.to_excel(file_path, index=False)
        return FileResponse(file_path, filename=file_name)
    except Exception as e:
        return {"error": "输入错误", "detail": str(e)}


@router.get("/down_alarm_history")
async def down_alarm_history(
        begin: str = Query(...),
        end: str = Query(...),
        alarm: str = Query(...)
):
    folder = settings.resolve_path("spider/down/temp_folder_one_day")
    file_name = f'告警{datetime.datetime.now().timestamp()}.xlsx'

    alarm_mapping = {
        '温度过高': ['温度过高', '温度过高（预告警）'],
        '交流输入停电告警': ['交流输入停电告警'],
        '门类': [
            '智能门禁通信状态告警', '设备故障告警(智能门禁)', '长时间门开告警',
            '长时间门开告警(智能门禁)', '长时间门开告警(非智能门禁)', '门磁开关状态',
            '门磁开关状态(智能门禁)', '门磁开关状态(非智能门禁)', '门锁开关状态(智能门禁)',
            '门锁开关状态(非智能门禁)', '非法进入告警(智能门禁)', '非法进入告警(非智能门禁)'
        ],
        '离线类': ['FSU离线', '一级低压脱离告警', '二级低压脱离告警']
    }

    if alarm not in alarm_mapping:
        return {"error": "输入的告警名称不在支持范围内"}

    alarm_list = alarm_mapping[alarm]
    alarm_condition = f"告警名称 = '{alarm_list[0]}'" if len(alarm_list) == 1 else f"告警名称 IN {tuple(alarm_list)}"

    try:
        df = sql_orm().excute_sql(
            f"select * from hbase where ({alarm_condition}) and (告警发生日期 between '{begin}' and '{end}')",
            return_df=True
        )
        file_path = f'{folder}/{file_name}'
        df.to_excel(file_path, index=False)
        return FileResponse(file_path, filename=file_name)
    except Exception as e:
        return {"error": "输入错误"}