from core.utils.retry_wrapper import requests_get
from spider.script.down_nenghao import config_equiment_constitution as CONFIG,config_lixian as CONFIG2
from core.sql import sql_orm
import datetime
# 能耗系统设备情况下载
#导出路径
#设备管理：4A-能耗系统-远程抄表-设备管理-缴费类型（缴费电表）-查询-Excel导出（设备+电量）
#设备查询（长期离线）：4A-能耗系统-远程抄表-设备查询（长期离线）-缴费类型（缴费电表）-查询-Excel导出（设备+电量）
# 报错如下大概率是atrust 0信任掉了，重新登录
# requests.exceptions.ConnectionError: HTTPConnectionPool(host='chntenergy.chinatowercom.cn', port=50080): Max retries exceeded with url:
def down_equiment_consitution():
    down_path=f'{SPIDER_PATH}nenghao_equiment_constitution/设备信息{datetime.datetime.now().strftime("%Y-%m-%d")}.xlsx'
    url=CONFIG.URL
    headers=CONFIG.HEADERS
    with sql_orm().session_scope() as temp:
        session,Base=temp
        pojo=Base.classes.foura
        res=session.query(pojo).first()
        headers['Cookie']=res.Cookie
    res=requests_get(url=url,headers=headers)
    if len(res.content) < 3000: raise ValueError("Content size is less than 3KB")
    with open(down_path, "wb") as codes:
        codes.write(res.content)
    try:
        with sql_orm().session_scope() as temp:
            session, Base = temp
            pojo = Base.classes.update_downhour_log
            res = session.query(pojo).filter(pojo.type == 'nenghao_equiment_constitution').first()
            res.time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        pass

def down_lixian():
    down_path=f'{SPIDER_PATH}nenghao_lixian/设备信息{datetime.datetime.now().strftime("%Y-%m-%d")}.xlsx'
    url=CONFIG2.URL
    headers=CONFIG2.HEADERS
    with sql_orm().session_scope() as temp:
        session,Base=temp
        pojo=Base.classes.foura
        res=session.query(pojo).first()
        headers['Cookie']=res.Cookie
    res=requests_get(url=url,headers=headers)
    if len(res.content) < 3000: raise ValueError("Content size is less than 3KB")
    with open(down_path, "wb") as codes:
        codes.write(res.content)
    try:
        with sql_orm().session_scope() as temp:
            session, Base = temp
            pojo = Base.classes.update_downhour_log
            res = session.query(pojo).filter(pojo.type == 'nenghao_lixian').first()
            res.time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        pass


# down_equiment_consitution()
# down_lixian()