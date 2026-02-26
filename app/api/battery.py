# app/api/battery.py
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
import pandas as pd
import datetime
import tempfile
from core.sql import sql_orm
from core.config import settings

router = APIRouter(tags=["电池续航"])
templates = Jinja2Templates(directory=settings.resolve_path("app/templates"))


@router.get("/battery_life", response_class=HTMLResponse)
async def battery_life(
        request: Request,
        page: int = Query(1),
        search: str = Query("")
):
    per_page = 10

    with sql_orm().session_scope() as (sql, Base):
        # 主查询
        sql_str = 'select * from result'
        result = sql.execute(text(sql_str))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        df = df.reset_index(drop=True)
        df.index += 1
        df = df.reset_index()

        # 获取上个月负载电流
        last_month = datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)
        sql_str = f"""select 运维监控站址ID as 站址运维ID,月度平均值 from 基站负载电流 
                    where 年={last_month.year} and 月份={last_month.month}"""
        result = sql.execute(text(sql_str))
        df_dc = pd.DataFrame(result.fetchall(), columns=result.keys())
        df_dc = df_dc.drop_duplicates(subset=['站址运维ID'], keep='first')

        # 列名映射
        df = df.rename(columns={
            "city": "市", "area": "区", "site_name": "站址名称",
            "site_code": "站址编码", "id": "站址运维ID",
            "level": "站点等级", "belong": "站点共享情况",
            "fsu_status": "FSU状态", "voltage": "当时直流电压值",
            "dc": "负载电流", "voltage_get_time": "直流电压值获取时间",
            "offline_time": "运维退服（或运营商）时间",
            "outage_time": "设备（或停电）告警/核容开始时间",
            "battery_life": "基站电池续航时长（小时）",
            "caculate_type": "数据来源",
            "battery_life_final": "最终续航（小时）"
        })

        # 合并负载电流
        df = df.merge(df_dc, on='站址运维ID', how='left')
        df['负载电流'] = df['负载电流'].where(
            df['负载电流'].notna() & (df['负载电流'] != ''),
            df['月度平均值']
        )
        df = df.drop(columns=['月度平均值'])

        # 调整列顺序
        new_order = [
            "市", "区", "站址名称", "站址编码", "站址运维ID", "站点等级", "站点共享情况", "FSU状态",
            "当时直流电压值", "负载电流", "直流电压值获取时间", "运维退服（或运营商）时间",
            "设备（或停电）告警/核容开始时间", "基站电池续航时长（小时）", "最终续航（小时）", "数据来源"
        ]
        df = df.reindex(columns=new_order)

        # 模糊查询
        if search:
            string_columns = df.select_dtypes(include=['object']).columns.tolist()
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
            mask = pd.Series(False, index=df.index)

            for col in string_columns:
                mask |= df[col].astype(str).str.contains(search, case=False, na=False)
            for col in numeric_columns:
                mask |= df[col].astype(str).str.contains(search, case=False, na=False)

            df = df[mask].reset_index(drop=True)

        # 统计计算
        df['基站电池续航时长（小时）'] = pd.to_numeric(df['基站电池续航时长（小时）'], errors='coerce')
        total_duration = df['基站电池续航时长（小时）'].sum(skipna=True)
        valid_count = df['基站电池续航时长（小时）'].count()
        average_duration = round(total_duration / valid_count, 1) if valid_count > 0 else 0
        abnormal_count = df[df['基站电池续航时长（小时）'] < 1.2]['基站电池续航时长（小时）'].count()

        # 分页
        total = len(df)
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        df_str = df.astype(str)
        data = df_str.iloc[start_index:end_index].to_dict('records')

        # 存入 session（FastAPI 需要用其他方式，这里简化）
        request.session['battery_life_data'] = df_str.to_dict('records')

    if len(data) == 0:
        return "无数据"

    return templates.TemplateResponse("battery_life.html", {
        "request": request,
        "data": data,
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": (total + per_page - 1) // per_page,
        "average_duration": average_duration,
        "abnormal_count": abnormal_count,
        "search_keyword": search
    })


@router.get("/battery_life_city", response_class=HTMLResponse)
async def battery_life_city(request: Request):
    # 获取全区站址数
    with sql_orm().session_scope() as (sql, Base):
        res = sql.execute(
            text("select city as 市,count(*) as 站址数 from station where fsu_status='交维' group by city")).fetchall()
        station = pd.DataFrame([dict(row) for row in res])

    # 从 session 获取数据（简化版，实际应该用 Redis 或数据库）
    df = pd.DataFrame(request.session.get('battery_life_data', []))
    df = df.loc[df['FSU状态'] == '交维']
    df['基站电池续航时长（小时）'] = pd.to_numeric(df['基站电池续航时长（小时）'], errors='coerce').fillna(0)

    res = df.groupby('市')['基站电池续航时长（小时）'].agg([
        ('基站电池续航时长<1小时', lambda x: (x < 1).sum()),
        ('1小时≤基站电池续航时长<2小时', lambda x: ((x >= 1) & (x < 2)).sum()),
        ('2小时≤基站电池续航时长且<3小时', lambda x: ((x >= 2) & (x < 3)).sum()),
        ('3小时≤基站电池续航时长且<6小时', lambda x: ((x >= 3) & (x < 6)).sum()),
        ('基站电池续航时长≤6小时', lambda x: (x >= 6).sum())
    ]).reset_index()

    res = pd.merge(station, res, on='市', how='left')
    res = res.reindex(columns=['市', '站址数', '基站电池续航时长<1小时', '1小时≤基站电池续航时长<2小时',
                               '2小时≤基站电池续航时长<3小时', '3小时≤基站电池续航时长且<6小时',
                               '基站电池续航时长≤6小时'])

    cities_order = ['南宁', '桂林', '百色', '柳州', '玉林', '河池', '贵港', '梧州', '北海', '崇左', '钦州', '来宾',
                    '贺州', '防城港']
    res['市'] = pd.Categorical(res['市'], categories=cities_order, ordered=True)
    res = res.sort_values('市')

    # 添加合计行
    total_sum = res.sum()
    total_df = pd.DataFrame([total_sum], columns=res.columns)
    total_df.iloc[0, 0] = '全区'
    res = pd.concat([total_df, res], ignore_index=True)
    res = res.fillna(0)

    for col in res.columns:
        if col != '市':
            res[col] = res[col].astype(int)

    data = res.to_dict(orient='records')
    return templates.TemplateResponse("battery_life_city.html", {"request": request, "data": data})


@router.get("/battery_life_download_excel")
async def battery_life_download_excel(request: Request):
    data = request.session.get('battery_life_data', [])
    if not data:
        return {"error": "无数据"}

    df = pd.DataFrame(data)
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp:
        df.to_excel(temp.name, index=False)
        return FileResponse(temp.name, filename='蓄电池续航能力.xlsx')


@router.get("/battery_shangdan", response_class=HTMLResponse)
async def battery_shangdan(request: Request, id: str = Query(...)):
    df = sql_orm().excute_sql(
        f"select * from battery_shangdan where id='{id}'",
        return_df=True
    )
    data = df.to_dict(orient='records')
    return templates.TemplateResponse("battery_shangdan.html", {"request": request, "data": data})


@router.get("/tt/get_battery")
async def get_battery():
    try:
        with sql_orm().session_scope() as temp:
            sql, Base = temp
            sql_str = text("select * from result")
            res = sql.execute(sql_str)
            colnames = [column[0] for column in res.cursor.description]
            rows = res.fetchall()
        data_list = [dict(zip(colnames, row)) for row in rows]
        return {"data": data_list}
    except Exception as e:
        return {"error": str(e)}