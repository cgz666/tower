# app/api/station.py
from fastapi import APIRouter, UploadFile, File, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
import os
import datetime
from core.config import settings
from message.ID_serch.operate import get_table as station_shouzi_get_table

router = APIRouter(prefix="/station", tags=["站址"])
templates = Jinja2Templates(directory=settings.resolve_path("app/templates"))


def zip_file_and_send(folder, file_list):
    import shutil
    from fastapi.responses import FileResponse

    zip_folder = os.path.join(folder, 'zip')
    os.makedirs(zip_folder, exist_ok=True)

    # 清空旧文件
    for file in os.listdir(zip_folder):
        os.remove(os.path.join(zip_folder, file))

    # 复制文件
    for file in file_list:
        path = os.path.join(folder, file)
        zip_path = os.path.join(zip_folder, file)
        if os.path.exists(path):
            shutil.copy(path, zip_path)

    # 创建 zip
    zip_path = f"{settings.resolve_path('spider/down/temp_folder_one_day')}{str(datetime.datetime.now().timestamp())}"
    shutil.make_archive(zip_path, 'zip', zip_folder)
    return FileResponse(zip_path + '.zip', filename="站址数据.zip")


@router.get("/shouzi", response_class=HTMLResponse)
async def station_shouzi_index(request: Request):
    files = ['直流负载电流.xlsx', '浮充电压设定值.xlsx', '均充电压设定值.xlsx',
             '二级低压脱离设定值.xlsx', '一级低压脱离设定值.xlsx']

    file_times = {}
    for file in files:
        file_path = settings.resolve_path('message/ID_serch/xls', file)
        if os.path.exists(file_path):
            stat = os.stat(file_path)
            created_time = datetime.datetime.fromtimestamp(stat.st_mtime)
            file_times[file] = created_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            file_times[file] = '文件不存在'

    return templates.TemplateResponse("station_shouzi.html", {
        "request": request,
        "file_times": file_times
    })


@router.post("/shouzi")
async def station_shouzi(file: UploadFile = File(...)):
    path = settings.resolve_path('message/ID_serch/xls/查询用站址运维ID.xlsx')
    with open(path, "wb") as buffer:
        buffer.write(await file.read())

    result_path = station_shouzi_get_table(path)
    if result_path != '失败':
        return FileResponse(result_path, filename="站址查询结果.xlsx")
    else:
        return {"error": "处理失败"}


@router.get("/shouzi/down")
async def station_shouzi_down():
    folder = settings.resolve_path('message/ID_serch/xls')
    file_list = ['直流负载电流.xlsx', '浮充电压设定值.xlsx', '均充电压设定值.xlsx',
                 '二级低压脱离设定值.xlsx', '一级低压脱离设定值.xlsx']
    try:
        return zip_file_and_send(folder, file_list)
    except Exception as e:
        return {"error": str(e)}