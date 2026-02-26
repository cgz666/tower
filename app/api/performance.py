# app/api/performance.py
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
import os
import glob
import datetime
import subprocess
from core.config import settings

router = APIRouter(tags=["报表"])
templates = Jinja2Templates(directory=settings.resolve_path("app/templates"))


def newest_file(hour: str):
    OUTPUT_DIR = settings.resolve_path(r'message/performance_sheet/output')
    today = datetime.datetime.now().strftime('%Y%m%d')
    pattern = os.path.join(OUTPUT_DIR, f'{today}_{hour}.xlsx')
    files = glob.glob(pattern)
    return files[0] if files else None


def file_mtime(hour: str):
    f = newest_file(hour)
    if not f:
        return '-'
    t = datetime.datetime.fromtimestamp(os.path.getmtime(f))
    return t.strftime('%m-%d %H:%M')


@router.get("/performance_sheet", response_class=HTMLResponse)
async def performance_sheet(request: Request):
    hours = ['08', '14', '17']
    times = {h: file_mtime(h) for h in hours}
    return templates.TemplateResponse("performance_sheet.html", {
        "request": request,
        "hours": hours,
        "times": times
    })


@router.post("/performance_sheet")
async def update_and_info():
    SCRIPT_PATH = settings.resolve_path('message/performance_sheet/script.py')
    PYTHON_EXE = settings.resolve_path(r'E:\miniconda3\envs\tower\python.exe')
    # 启动脚本（不阻塞）
    subprocess.Popen([PYTHON_EXE, SCRIPT_PATH], shell=True)

    hours = ['08', '14', '17']
    times = {h: file_mtime(h) for h in hours}
    return {"status": "success", "times": times}


@router.get("/download/{hour}")
async def download(hour: str):
    if hour not in {'08', '14', '17'}:
        return {"error": "Invalid hour"}

    real = newest_file(hour)
    if not real or not os.path.isfile(real):
        return {"error": "File not found"}

    return FileResponse(real, filename=f"{hour}_performance.xlsx")