# app/main.py
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from app.api.remote_trigger import router as remote_trigger_router

app = FastAPI(title="TOWER")

app.add_middleware(
    SessionMiddleware,
    secret_key="a1b2c3d4e5f678901234567890abcdef1234567890abcdef1234567890abcdef",  # 必须设置，用于加密 cookie
    session_cookie="session",           # cookie 名称
    same_site="lax",                    # 防 XSS 设置
)
app.include_router(remote_trigger_router)

#  启动命令：uvicorn app.main:app --host 0.0.0.0 --port 5000 --workers 4

