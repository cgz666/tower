from fastapi import APIRouter, Request
import redis
from typing import Optional

router = APIRouter(tags=["OA认证"])

redis_client = redis.Redis(
    host="localhost",  # Redis 服务器IP
    port=6379,         # Redis 端口
    decode_responses=True,  # 自动将 bytes 转为字符串
    password="123456"        # 如果有密码请填写，无则留空
)

# 定义 Redis Key 前缀（避免键名冲突）
PREFIX = "oa_auth:"

# 封装 Redis 操作函数
def set_redis_key(key: str, value: str, expire: Optional[int] = None):
    """设置 Redis 键值对，可选过期时间（秒）"""
    redis_client.set(PREFIX + key, value)
    if expire:
        redis_client.expire(PREFIX + key, expire)

def get_redis_key(key: str) -> str:
    """获取 Redis 键值，默认返回空字符串"""
    return redis_client.get(PREFIX + key) or ""

# ========== 原有接口改造 ==========
@router.post("/save_oa_auth")
async def save_auth(request: Request):
    data = await request.json()
    auth_value = data.get("authorization", "")
    set_redis_key("authorization", auth_value)
    print("收到Authorization:", auth_value)
    return {"status": "ok"}

@router.get("/get_oa_auth")
async def get_auth():
    return get_redis_key("authorization")

@router.post("/save_oa_token")
async def save_token(request: Request):
    data = await request.json()
    token_value = data.get("token", "")
    set_redis_key("token", token_value)
    print("收到Token:", token_value)
    return {"status": "ok"}

@router.get("/get_oa_token")
async def get_token():
    return get_redis_key("token")

@router.post("/save_oa_sysToken")
async def save_sysToken(request: Request):
    data = await request.json()
    sys_token_value = data.get("sysToken", "")
    set_redis_key("sysToken", sys_token_value)
    print("收到SysToken:", sys_token_value)
    return {"status": "ok"}

@router.get("/get_oa_sysToken")
async def get_sysToken():
    return get_redis_key("sysToken")

@router.post("/save_oa_XCsrfToken")
async def save_XCsrfToken(request: Request):
    data = await request.json()
    csrf_token = data.get("csrfToken", "")
    cookie = data.get("cookie", "")
    set_redis_key("csrfToken", csrf_token)
    set_redis_key("cookie", cookie)
    print("收到X-Csrf-Token:", csrf_token)
    print("收到Cookie:", cookie)
    return {"status": "ok"}

@router.get("/get_oa_XCsrfToken")
async def get_XCsrfToken():
    return {
        "csrfToken": get_redis_key("csrfToken"),
        "cookie": get_redis_key("cookie")
    }

@router.post("/get_OA")
async def get_OA(request: Request):
    from sqlalchemy import text
    from core.sql import sql_orm

    form = await request.form()
    Cookie = form.get('Cookie')
    EIP = form.get('EIP', '')

    try:
        pj = {'Cookie': Cookie, 'ID': 1} if EIP == '' else {'Cookie': EIP, 'ID': 2}

        with sql_orm().session_scope() as re:
            session, Base = re
            pojo = Base.classes.oa
            a = pojo(**pj)
            session.merge(a)

        return "success"
    except Exception as e:
        return str(e)