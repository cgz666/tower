# app/main.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse

# 导入所有路由
from app.api.oa_auth import router as oa_auth_router
from app.api.download import router as download_router
from app.api.battery import router as battery_router
from app.api.station import router as station_router
from app.api.performance import router as performance_router
from core.config import settings

app = FastAPI(title="TOWER")

# Session 中间件
app.add_middleware(
    SessionMiddleware,
    secret_key="a1b2c3d4e5f678901234567890abcdef1234567890abcdef1234567890abcdef",
    session_cookie="session",
    same_site="lax",
)

# 静态文件和模板
templates = Jinja2Templates(directory=settings.resolve_path("app/templates"))

# 注册所有路由
app.include_router(oa_auth_router)
app.include_router(download_router)
app.include_router(battery_router)
app.include_router(station_router)
app.include_router(performance_router)


#  启动命令：uvicorn app.main:app --host 0.0.0.0 --port 5000 --workers 4

