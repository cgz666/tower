# app/api/oa_auth.py
from fastapi import APIRouter, Request

router = APIRouter(tags=["OA认证"])

# 内存缓存（生产环境建议用 Redis）
auth_cache = ""
token_cache = ""
sysToken_cache = ""
csrftoken_cache = ""
cookie_cache = ""


@router.post("/save_oa_auth")
async def save_auth(request: Request):
    global auth_cache
    data = await request.json()
    auth_cache = data.get("authorization")
    return {"status": "ok"}


@router.get("/get_oa_auth")
async def get_auth():
    return {"authorization": auth_cache}


@router.post("/save_oa_token")
async def save_token(request: Request):
    global token_cache
    data = await request.json()
    token_cache = data.get("token")
    return {"status": "ok"}


@router.get("/get_oa_token")
async def get_token():
    return {"token": token_cache}


@router.post("/save_oa_sysToken")
async def save_sysToken(request: Request):
    global sysToken_cache
    data = await request.json()
    sysToken_cache = data.get("sysToken")
    return {"status": "ok"}


@router.get("/get_oa_sysToken")
async def get_sysToken():
    return {"sysToken": sysToken_cache}


@router.post("/save_oa_XCsrfToken")
async def save_XCsrfToken(request: Request):
    global csrftoken_cache, cookie_cache
    data = await request.json()
    csrftoken_cache = data.get("csrfToken")
    cookie_cache = data.get("cookie")
    return {"status": "ok"}


@router.get("/get_oa_XCsrfToken")
async def get_XCsrfToken():
    return {"csrfToken": csrftoken_cache, "cookie": cookie_cache}


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