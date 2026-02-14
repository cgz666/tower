from core.utils.retry_wrapper import requests_post
from bs4 import BeautifulSoup
import os
import pandas as pd
import datetime
from core.sql import sql_orm
from xlsx2csv import Xlsx2csv
import time
import requests
from functools import wraps
from sqlalchemy import text
from spider.script.down_foura import foura_data
import calendar
from core.config import settings
import shutil

"""
通用函数模块，包含项目中常用的工具函数，如重试装饰器、请求函数、文件操作函数等。
"""
def retry(max_attempts=1, delay=2):
    """
    重试装饰器，用于对指定函数进行重试操作。

    :param max_attempts: 最大重试次数，默认为15次。
    :param delay: 每次重试之间的等待时间（秒），默认为2秒。
    :return: 装饰器函数。
    """

    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal max_attempts  # 声明为非局部变量以便在内部函数中修改
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise
                    time.sleep(delay)  # 等待一段时间后重试

        return wrapper

    return decorator_retry
@retry()
def requests_post(url, headers={}, data={}, cookies={}, timeout=300):
    """
    发送POST请求，带有重试机制。

    :param url: 请求的URL地址。
    :param headers: 请求头，默认为空字典。
    :param data: 请求数据，默认为空字典。
    :param cookies: 请求的cookies，默认为空字典。
    :param timeout: 请求超时时间（秒），默认为300秒。
    :return: 请求响应对象。
    """
    return requests.post(url, headers=headers, data=data, cookies=cookies, timeout=timeout)
@retry()
def requests_get(url, headers={}, params={}, cookies={}):
    """
    发送GET请求，带有重试机制。

    :param url: 请求的URL地址。
    :param headers: 请求头，默认为空字典。
    :param params: 请求参数，默认为空字典。
    :param cookies: 请求的cookies，默认为空字典。
    :return: 请求响应对象。
    """
    return requests.get(url, headers=headers, params=params, cookies=cookies)
def get_foura_cookie(ID=1):
    """
    从数据库中获取指定ID的foura cookie信息。

    :param ID: 数据库中cookie记录的ID，默认为1（对应foura1）。
    :return: 解析后的cookie字典；若ID不存在/格式错误，返回空字典。
    """
    db = sql_orm()
    cookie_result = db.get_cookies(id=f"foura{ID}")
    return cookie_result["cookies"]
def clear_folder(folder_temp):
    """
    清空指定文件夹下的所有文件。

    :param folder_temp: 要清空的文件夹路径。
    """
    for file in os.listdir(folder_temp):
        file = os.path.join(folder_temp, file)
        os.remove(file)
def xlsx_to_csv(folder):
    """
    将指定文件夹下的所有.xlsx文件转换为.csv文件。

    :param folder: 包含.xlsx文件的文件夹路径。
    """
    for file in os.listdir(folder):
        path = os.path.join(folder, file)
        if file.endswith('.xlsx'):
            csv_path = path.replace(".xlsx", ".csv")  # 对应的 .csv 文件路径
            if not os.path.exists(csv_path):
                Xlsx2csv(path, outputencoding="utf-8").convert(csv_path)
def concat_df(folder, output_path, gen_csv=False):
    """
    合并指定文件夹下的所有.csv和.xls文件，并保存为一个新的Excel文件。

    :param folder: 包含.csv和.xls文件的文件夹路径。
    :param output_path: 合并后文件的输出路径。
    :param gen_csv: 是否生成.csv文件，默认为False。
    :return: 合并后的DataFrame和输出路径。
    """
    xlsx_to_csv(folder)
    df_list = []
    for file in os.listdir(folder):
        path = os.path.join(folder, file)
        if '.csv' in file:
            temp = pd.read_csv(path, dtype=str)
            df_list.append(temp)
        elif file.endswith(('.xls')):
            temp = pd.read_excel(path, dtype=str, engine='xlrd')
            df_list.append(temp)

    merge = pd.concat(df_list)
    output_path_str = str(output_path)
    merge.to_excel(output_path_str, index=False)
    if gen_csv:
        csv_output_path = output_path_str.replace('.xlsx', '.csv')
        merge.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
    return merge, output_path
def log_downtime(fuc_name):
    """
    记录指定操作的下载时间到数据库。

    :param fuc_name: 操作类型名称。
    """
    with sql_orm().session_scope() as temp:
        session, Base = temp
        pojo = Base.classes.update_downhour_log
        res = session.query(pojo).filter(pojo.type == fuc_name).first()
        res.time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
def down_file(url, data, path, conten_len_error=3000, xlsx_juge=False, cookie_user=1):
    """
    下载文件，支持重试机制和文件验证。

    :param url: 文件下载的URL地址。
    :param data: 请求数据。
    :param path: 文件保存路径。
    :param conten_len_error: 内容长度验证阈值，默认为3000。
    :param xlsx_juge: 是否验证文件类型为xlsx或xls，默认为False。
    """
    retry = 3
    while retry >= 0:
        try:
            headers = {
                'Host': 'omms.chinatowercom.cn:9000',
                'Origin': 'http://omms.chinatowercom.cn:9000 ',
                'Referer': url,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
            }
            cookies = get_foura_cookie(cookie_user)
            res = requests_post(url, headers=headers, cookies=cookies)
            html = BeautifulSoup(res.text, 'html.parser')
            javax = html.find('input', id='javax.faces.ViewState')['value']
            for key, into_data in data.items():
                into_data['javax.faces.ViewState'] = javax
                res = requests_post(url, headers=headers, data=into_data, cookies=cookies)
                if 'FINAL' in key:
                    if len(res.content) < conten_len_error:
                        raise ValueError("内容小于给定大小")
                    if xlsx_juge:
                        xlsx_signature = b'\x50\x4B\x03\x04'  # xlsx 文件的签名
                        xls_signature = b'\x09\x08\x04\x00\x10\x00\x00\x00'  # xls 文件的签名
                        if not res.content.startswith(xlsx_signature) and not res.content.startswith(xls_signature):
                            raise ValueError("内容不是 xlsx 或 xls")
                    with open(path, "wb") as codes:
                        codes.write(res.content)
            return
        except ValueError as e:
            if "内容小于给定大小" in str(e) and retry:
                retry -= 1
                continue
            raise
        except Exception:
            raise
def down_file_no_save(url, data, cookie_user=1):
    """
    下载文件，支持重试机制和文件验证。

    :param url: 文件下载的URL地址。
    :param data: 请求数据。
    :param path: 文件保存路径。
    :param conten_len_error: 内容长度验证阈值，默认为3000。
    :param xlsx_juge: 是否验证文件类型为xlsx或xls，默认为False。
    """
    try:
        headers = {
            'Host': 'omms.chinatowercom.cn:9000',
            'Origin': 'http://omms.chinatowercom.cn:9000',
            'Referer': url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }
        cookies = get_foura_cookie(cookie_user)
        res = requests_post(url, headers=headers, cookies=cookies)
        html = BeautifulSoup(res.text, 'html.parser')
        javax = html.find('input', id='javax.faces.ViewState')['value']
        for key, into_data in data.items():
            into_data['javax.faces.ViewState'] = javax
            res = requests_post(url, headers=headers, data=into_data, cookies=cookies)
        soup = BeautifulSoup(res.text, 'html.parser')
        tbody = soup.find('tbody', {'id': 'listForm:list:tb'})
        headers = []
        thead = soup.find('thead')
        if thead:
            header_cells = thead.find_all('th')
            for th in header_cells:
                text = th.get_text(strip=True)
                headers.append(text)
        rows = []
        for tr in tbody.find_all('tr', class_=['rich-table-row']):
            try:
                cells = tr.find_all('td')
                row = []
                for td in cells:
                    text = td.get_text(strip=True)
                    row.append(text)
                rows.append(row)
            except:
                pass
        df = pd.DataFrame(rows, columns=headers if len(headers) == len(rows[0]) else None)
        return df
    except Exception as e:
        print(e)

"""
爬取站址信息，路径：资源管理-站址管理-取消FSU工程状态：交维，改为全选-导出
"""
class Station():
    def __init__(self):
        self.data = foura_data.station
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/siteMge/listSite.xhtml'
        self.down_name = '站址信息'
        self.down_name_en = 'station'
        self.down_suffix = '.xlsx'
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        down_list = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                     '0099986', '0099987', '0099988', '0099989', '0099990']
        for city in down_list:
            for key in ['1', '2', '4', '5']:
                self.data[key]['queryForm:unitHidden'] = city
            path = os.path.join(self.folder_temp, f"{city}{self.down_suffix}")
            if city in ['0099982', '0099983']:
                down_file(self.URL, self.data, path, conten_len_error=3000000)
            else:
                down_file(self.URL, self.data, path)

    def read_file(self):
        xlsx_to_csv(self.folder_temp)
        df_list = []
        for file in os.listdir(self.folder_temp):
            path = os.path.join(self.folder_temp, file)
            if '.csv' in file:
                temp = pd.read_csv(path, dtype=str)
                df_list.append(temp)
        return df_list

    def save_as_sql_xlsx_csv(self, df_list):
        use_cols = {'所属省': 'province', '所属市': 'city', '区县（行政区划）': 'area', '所属区': 'area2',
                    "乡镇（街道）": "village",
                    '行政村（居委会）': 'countryside', '名称': 'site_name',
                    '站址编码': 'site_code', '运维ID': 'site_maitan_code', '站址状态': 'site_status',
                    'FSU工程状态': 'fsu_status',
                    "站址保障等级": 'level', "所属运营商": "belong"}

        def standardized_station(df):
            df = df.where(pd.notnull(df), None)  # nan改为空
            df = df.dropna(subset=['站址编码'])  # 去除主键空值
            df = df.rename(columns=use_cols)
            df = df[list(use_cols.values())]
            df['city'] = df['city'].str.replace('市', '')
            df['city'] = df['city'].str.replace('分公司', '')
            df = df.drop_duplicates(subset=['site_maitan_code'], keep='last')
            return df

        orm = sql_orm()

        # 更新area信息
        with orm.session_scope() as (session, Base):
            # 更新station信息
            session.execute(text('TRUNCATE station'))
            for df in df_list:
                df = standardized_station(df)
                orm.save_data(df, 'station')
            session.execute(text('TRUNCATE station_with_area'))
            session.commit()
            try:
                session.execute(text('insert into station_with_area select site_maitan_code,area from tower.station'))
            except Exception as e:
                raise
        # 保存为文件xlsx/csv
        merge = pd.concat(df_list)
        merge.to_excel(self.output_path, index=False)
        merge.to_csv(self.output_path.replace('xlsx', 'csv'), index=False, encoding='utf-8-sig')

    def main(self):
        self.down()
        if len(os.listdir(self.folder_temp)) >= 14:
            df_list = self.read_file()
            self.save_as_sql_xlsx_csv(df_list)
            log_downtime(self.down_name_en)
        else:
            raise FileNotFoundError("文件下载不全，当前仅下载了 {} 个文件".format(len(os.listdir(self.folder_temp))))

"""
爬取两翼站址信息，路径：资源管理-站址管理-两翼-导出
"""
class StationLiangYi():
    def __init__(self):
        self.data = foura_data.station_liangyi
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/siteMge/listSite.xhtml'
        self.down_name = '站址信息'
        self.down_name_en = 'station_liangyi'
        self.down_suffix = '.xls'
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        down_list = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                     '0099986', '0099987', '0099988', '0099989', '0099990']
        for city in down_list:
            for key in ['1', '2']:
                self.data[key]['queryZlForm:unitZLHidden'] = city
            path = os.path.join(self.folder_temp, f"{city}{self.down_suffix}")
            down_file(self.URL, self.data, path)

    def read_file(self):
        df_list = []
        for file in os.listdir(self.folder_temp):
            path = os.path.join(self.folder_temp, file)
            if '.xls' in file:
                temp = pd.read_excel(path, dtype=str)
                df_list.append(temp)
        return df_list

    def main(self):
        self.down()
        if len(os.listdir(self.folder_temp)) >= 14:
            df_list = self.read_file()
            merge = pd.concat(df_list)
            merge.to_excel(self.output_path, index=False)
            log_downtime(self.down_name_en)
        else:
            raise FileNotFoundError("文件下载不全，当前仅下载了 {} 个文件".format(len(os.listdir(self.folder_temp))))

"""
爬取站址信息，路径：运行监控-基站直流负载总电流-查询-导出
"""
class StationDC():
    def __init__(self):
        self.data = foura_data.station_DC
        self.now = datetime.datetime.now()
        if self.now.day == 1:
            self.now = self.now - datetime.timedelta(days=1)
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/siteMge/staPerDataList.xhtml'
        self.down_name = '基站负载电流'
        self.down_name_en = 'DC'
        self.down_suffix = '.xls'
        # 改动7：统一使用settings.resolve_path解析路径
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_folder = settings.resolve_path(f"spider/down/{self.down_name_en}")
        self.output_path = settings.resolve_path(
            f"{self.output_folder}/{self.now.strftime('%Y%m')}{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        for index, city in enumerate(['0099977,0099978,0099979', '0099980,0099981,0099982',
                                      '0099983,0099984,0099985', '0099986,0099987,0099988', '0099989,0099990']):
            for key in ['1', '2']:
                self.data[key]['queryForm:unitHidden'] = city
                self.data[key]['queryForm:j_id19'] = self.now.year
                self.data[key]['queryForm:queryMonthid'] = self.now.strftime('%m').lstrip('0')
                self.data[key]['queryForm:queryMonthid_hiddenValue'] = self.now.strftime('%m').lstrip('0')
            # 改动8：用os.path.join拼接路径
            path = os.path.join(self.folder_temp, f"{index}{self.down_suffix}")
            down_file(self.URL, self.data, path)

    def sql_process(self, df):
        df = df.drop(columns=["备注"]).fillna("")
        orm = sql_orm(database="自助取数")
        orm.save_data_merge(df, '基站负载电流')

    def main(self):
        self.down()
        merge, _ = concat_df(self.folder_temp, self.output_path)
        self.sql_process(merge)
        log_downtime(self.down_name_en)

    def temp(self):
        folder = self.output_folder
        for path in os.listdir(folder):
            if '.xlsx' in path:
                xlsx_path = os.path.join(folder, path)
                df = pd.read_excel(xlsx_path, dtype=str)
                self.sql_process(df)

"""
爬取fsu查询，路径：运行监控-fsu查询-导出
"""
class FsuChaXun():
    def __init__(self):
        self.data = foura_data.fsu_chaxun
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/fsuMge/listQuertFsu.xhtml'
        self.down_name = 'fsu清单'
        self.down_name_en = 'fsu_chaxun_all'
        self.down_suffix = '.xlsx'
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        for index, city in enumerate(['0099977,0099978,0099979,0099980,0099981,0099982',
                                      '0099983,0099984,0099985,0099986,0099987,0099988,0099989,0099990,2710377449']):
            for key in ['1', '2']:
                self.data[key]['queryForm:unitHidden'] = city
            path = os.path.join(self.folder_temp, f"{index}{self.down_suffix}")
            down_file(self.URL, self.data, path)

    def main(self):
        self.down()
        concat_df(self.folder_temp, self.output_path, gen_csv=True)
        log_downtime(self.down_name_en)

"""
爬取fsu查询，路径：工单管理-日常修理-隐患库-[归档起止时间:清除，地市:全选]-导出
"""
class YinHuanOrder():
    def __init__(self):
        self.data = foura_data.yinhuan_order
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/hiddenFixMge/monitorList.xhtml'
        self.down_name = '隐患工单'
        self.down_name_en = 'yinhuan_order'
        self.down_suffix = '.xls'
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self, guidang):
        clear_folder(self.folder_temp)
        # for month in range(1, self.now.month + 1):
        #     last_day = calendar.monthrange(self.now.year, month)[1]
        #     start = datetime.datetime(self.now.year, month, 1)
        #     end = datetime.datetime(self.now.year, month, last_day, 23, 59, 59)
        start_year = self.now.year - 1
        end_year = self.now.year
        end_month = self.now.month

        for year in range(start_year, end_year + 1):
            max_month = 12 if year < self.now.year else end_month
            for month in range(1, max_month + 1):  # 月份循环
                last_day = calendar.monthrange(year, month)[1]
                start = datetime.datetime(year, month, 1)
                end = datetime.datetime(year, month, last_day, 23, 59, 59)

                for key in ['1', '2', '3']:
                    self.data[key]['queryForm:queryType'] = guidang
                    self.data[key]['queryForm:findtimeStartInputDate'] = start.strftime('%Y-%m-%d %H:%M')
                    self.data[key]['queryForm:findtimeEndInputDate'] = end.strftime('%Y-%m-%d %H:%M')
                    self.data[key]['queryForm:findtimeStartInputCurrentDate'] = start.strftime('%m/%Y')
                    self.data[key]['queryForm:findtimeEndInputCurrentDate'] = end.strftime('%m/%Y')

                    self.data[key]['queryForm:finishtimeStartInputCurrentDate'] = start.strftime('%m/%Y')
                    self.data[key]['queryForm:hiddenAuditDateStartInputCurrentDate'] = start.strftime('%m/%Y')
                    self.data[key]['queryForm:hiddenRecordDateStartInputCurrentDate'] = start.strftime('%m/%Y')
                    self.data[key]['queryForm:finishtimeEndInputCurrentDate'] = end.strftime('%m/%Y')
                    self.data[key]['queryForm:hiddenAuditDateEndInputCurrentDate'] = end.strftime('%m/%Y')
                    self.data[key]['queryForm:hiddenRecordDateEndInputCurrentDate'] = end.strftime('%m/%Y')
                    self.data[key]['queryForm:finishtimeStartTimeHours'] = datetime.datetime.now().strftime('%H')
                    self.data[key]['queryForm:finishtimeStartTimeMinutes'] = datetime.datetime.now().strftime('%M')

                # 改动10：用os.path.join拼接路径
                path = os.path.join(self.folder_temp, f"{guidang}_{year}_{month}{self.down_suffix}")
                down_file(self.URL, self.data, path)

    def main(self):
        for guidang in ['Y', 'N']:
            self.down(guidang)
            if guidang == 'Y':
                output_path = self.output_path.replace('.xlsx', '已归档.xlsx')
            else:
                output_path = self.output_path.replace('.xlsx', '未归档.xlsx')
            concat_df(self.folder_temp, output_path)
        log_downtime(self.down_name_en)

"""
爬取移动接口工单，路径：我的工作台-运维管理综合查询-[故障来源：移动运营商接口][当天工单][工单状态：待故障确认]-查询-工单列表-导出
"""
class YiDongOrder():
    def __init__(self):
        self.data = foura_data.yidong_order
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/billDeal/monitoring/list/billList.xhtml'
        self.down_name = '移动接口工单'
        self.down_name_en = 'yidong_order'
        self.down_suffix = '.xls'
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        for index, city in enumerate(
                ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                 '0099986', '0099987', '0099988', '0099989', '0108648', '0108649', '0108650', '0108651']):
            for key in ['1', '2']:
                self.data[key]['queryForm:dealendtimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                self.data[key]['queryForm:dealstarttimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                self.data[key]['queryForm:endtimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                self.data[key]['queryForm:revertendtimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                self.data[key]['queryForm:revertstarttimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                self.data[key]['queryForm:starttimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                self.data[key]['queryForm:queryUnitId'] = city
            # 改动12：用os.path.join拼接路径
            path = os.path.join(self.folder_temp, f"{index}{self.down_suffix}")
            down_file(self.URL, self.data, path)

    def main(self):
        self.down()
        concat_df(self.folder_temp, self.output_path)
        log_downtime(self.down_name_en)

"""
爬取历史所有运营商接口工单，路径：我的工作台-运维管理综合查询-[故障来源：联通+移动+电信运营商接口][当天前历史工单][回单和受理时间：上月1日到本月1日]-查询-导出
每次爬取上个月整月的，本月的不爬取
"""
class YunYingShangOrderHistory():
    def __init__(self, end=datetime.datetime.now()):
        self.data = foura_data.yunyingshang_order_history
        self.end = end.replace(day=1, hour=0, minute=0, second=0)
        self.begin = (self.end - datetime.timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0)
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/billDeal/monitoring/list/billList.xhtml'
        self.down_name = '运营商接口工单'
        self.down_name_en = 'yunyingshang_order_history'
        self.down_suffix = '.xls'
        # 改动13：统一使用settings.resolve_path解析路径
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(
            f"spider/down/{self.down_name_en}/{self.down_name}{self.begin.strftime('%Y-%m')}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        for index, city in enumerate(
                ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                 '0099986', '0099987', '0099988', '0099989', '0108648', '0108649', '0108650', '0108651']):
            for belong in ['移动运营商接口', '联通运营商接口', '电信运营商接口']:
                for key in ['1', '2']:
                    self.data[key]['queryForm:dealendtimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                    self.data[key]['queryForm:dealstarttimeInputCurrentDate'] = self.now.strftime('%m/%Y')
                    self.data[key]['queryForm:endtimeInputCurrentDate'] = self.end.strftime('%m/%Y')
                    self.data[key]['queryForm:revertendtimeInputCurrentDate'] = self.end.strftime('%m/%Y')
                    self.data[key]['queryForm:revertstarttimeInputCurrentDate'] = self.begin.strftime('%m/%Y')
                    self.data[key]['queryForm:starttimeInputCurrentDate'] = self.begin.strftime('%m/%Y')

                    self.data[key]['queryForm:revertendtimeInputDate'] = self.end.strftime('%Y-%m-%d %H:%M')
                    self.data[key]['queryForm:revertstarttimeInputDate'] = self.begin.strftime('%Y-%m-%d %H:%M')
                    self.data[key]['queryForm:starttimeInputDate'] = self.begin.strftime('%Y-%m-%d %H:%M')
                    self.data[key]['queryForm:endtimeInputDate'] = self.end.strftime('%Y-%m-%d %H:%M')
                    self.data[key]['queryForm:queryUnitId'] = city
                    self.data[key]['queryForm:faultSrc_hiddenValue'] = belong
                    self.data[key]['queryForm:faultSrc'] = belong
                # 改动14：用os.path.join拼接路径
                path = os.path.join(self.folder_temp, f"{city}{belong}{self.down_suffix}")
                down_file(self.URL, self.data, path)

    def main(self):
        self.down()
        concat_df(self.folder_temp, self.output_path)
        log_downtime(self.down_name_en)

"""
爬取录入异常设备清单，路径：资源管理-录入异常设备清单-导出
"""
class LuRuYiChang():
    def __init__(self):
        self.data = foura_data.lururyichang
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/dataMge/listInputEx.xhtml'
        self.down_name = '录入异常清单'
        self.down_name_en = 'luruyichang'
        self.down_suffix = '.xls'
        # 改动15：统一使用settings.resolve_path解析路径
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        for index, city in enumerate(
                ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                 '0099986', '0099987', '0099988', '0099989', '0099990']):
            for key in ['1']:
                self.data[key]['queryForm:unitHidden'] = city
            # 改动16：用os.path.join拼接路径
            path = os.path.join(self.folder_temp, f"{index}{self.down_suffix}")
            down_file(self.URL, self.data, path)

    def main(self):
        self.down()
        concat_df(self.folder_temp, self.output_path)
        log_downtime(self.down_name_en)

"""
爬取运营商站址关系，路径：客户关系-运营商站址关系匹配-导出
"""
class StationAlias():
    def __init__(self):
        self.data = foura_data.station_alias
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/operatorShipMge/listOperator.xhtml'
        self.down_name = '运营商站址关系匹配'
        self.down_name_en = 'stationalias'
        self.down_suffix = '.xls'
        # 改动17：统一使用settings.resolve_path解析路径
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        for index, city in enumerate(
                ['0099977,0099978', '0099979,0099980,0099981', '0099982,0099983,0099984', '0099985,0099986,0099987',
                 '0099988,0099989,0099990']):
            for key in ['1']:
                self.data[key]['queryForm:unitHidden'] = city
            # 改动18：用os.path.join拼接路径
            path = os.path.join(self.folder_temp, f"{index}{self.down_suffix}")
            down_file(self.URL, self.data, path)

    def main(self):
        self.down()
        concat_df(self.folder_temp, self.output_path)
        log_downtime(self.down_name_en)

"""
爬取运营商站址关系，路径：运行监控-FSU监控-[注册状态：离线]-查询-导出
"""
class FsuJianKong():
    def __init__(self):
        self.data=foura_data.fsu_jiankong
        self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/fsuMge/listFsu.xhtml'
        self.down_name_en='fsu_hafhour'
        self.down_name_en1='fsu_jiankong_5min'
        self.output_path = settings.resolve_path(f'updatenas/fsu/每半小时fsu离线/{datetime.datetime.now().strftime("%Y%m%d_%H%M")}fsu离线.xlsx')
        self.temp_path=f'{INDEX}websource/temp_folder_one_day/fsu5分钟.xlsx'
    def down(self):
        down_file(self.URL, self.data, self.output_path)
        if datetime.datetime.now().minute < 30:
            shutil.copy(self.output_path, self.output_path.replace('每半小时', '每小时'))
        log_downtime(self.down_name_en)
    def down_5min(self):
        down_file(self.URL, self.data, self.temp_path)
        self.sql_process(self.temp_path)
        log_downtime(self.down_name_en1)
    def sql_process(self,path):
        df = pd.read_excel(path, dtype=str)
        df=df.fillna('')
        df=df.loc[df['离线时间']!='']
        with sql_orm().session_scope() as temp:
            sql, Base = temp
            pojo_brokentime = Base.classes.fsu_brokentime_log
            pojo_brokentimes = Base.classes.fsu_brokentimes_log
            now = datetime.datetime.now()
            if now.hour == 7:
                res = sql.query(pojo_brokentimes).all()
                for log in res:
                    log.broken_times = 0
            for index, row in df.iterrows():
                res = sql.query(pojo_brokentimes).filter(pojo_brokentimes.id == row['站址']).first()
                if res == None:
                    # 次数统计
                    log = pojo_brokentimes()
                    log.id = row['站址']
                    log.begin_time = row['离线时间']
                    log.broken_times = 1
                    sql.merge(log)
                    # 离线记录
                    log = pojo_brokentime()
                    log.id = row['站址']
                    log.begin_time = row['离线时间']
                    sql.add(log)
                else:
                    begin_time = datetime.datetime.strptime(res.begin_time, '%Y/%m/%d  %H:%M:%S')
                    begin_time_row = datetime.datetime.strptime(row['离线时间'], '%Y/%m/%d  %H:%M:%S')
                    if begin_time_row > begin_time:
                        # 次数统计
                        res.begin_time = row['离线时间']
                        res.broken_times += 1
                        # 离线记录
                        log = pojo_brokentime()
                        log.id = row['站址']
                        log.begin_time = row['离线时间']
                        sql.add(log)

"""
爬取历史告警Hbase，路径：运行监控-历史告警Hbase-[告警发生时间区间,告警名称]-导出excel
"""
class AlarmHistoryHbase():
    def __init__(self, year=0, month=0):
        self.data = foura_data.alarm_history_Hbase
        if month != 0:
            self.now = datetime.datetime.now().replace(year=year, month=month, day=1)
        else:
            self.now = datetime.datetime.now()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/alarmHisHbaseMge/listHisAlarmHbase.xhtml'
        self.down_name = '历史告警'
        self.down_name_en = 'Hbase'
        self.down_suffix = '.xlsx'
        # 改动19：统一使用settings.resolve_path解析路径
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def df_sql_process(self):
        for file in os.listdir(self.folder_temp):
            file = os.path.join(self.folder_temp, file)
            try:
                df = pd.read_excel(file, dtype=str)
                df = df.drop_duplicates()
                df = df.fillna('')
                df['告警发生日期'] = df['告警发生时间']
                with sql_orm(database='自助取数').session_scope() as temp:
                    sql, Base = temp
                    pojo = Base.classes.hbase
                    for index, row in df.iterrows():
                        temp = pojo(**row.to_dict())
                        sql.merge(temp)
            except Exception as e:
                pass

    def down(self):
        clear_folder(self.folder_temp)
        for alarm_name in ['一级低压脱离告警', '二级低压脱离告警', 'FSU离线', '温度超高', '温度过高',
                           '交流输入停电告警', '总电压过低', '直流输出电压过高告警', '直流输出电压过低告警'
            , '门', '电池供电告警']:
            end = self.now.replace(hour=0, minute=0, second=0)
            if end.day == 1:
                start = (self.now - datetime.timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0)
            else:
                start = self.now.replace(day=1, hour=0, minute=0, second=0)
            # 日期循环
            while start < end:
                start_next = (start + datetime.timedelta(days=1))
                for key in ['1']:
                    self.data[key]['queryForm:queryalarmName'] = alarm_name
                    self.data[key]['queryForm:firststarttimeInputCurrentDate'] = start.strftime('%m/%Y')
                    self.data[key]['queryForm:firstendtimeInputCurrentDate'] = start_next.strftime('%m/%Y')
                    self.data[key]['queryForm:firststarttimeInputDate'] = start.strftime('%Y-%m-%d %H:%M')
                    self.data[key]['queryForm:firstendtimeInputDate'] = start_next.strftime('%Y-%m-%d %H:%M')

                    # 改动20：用os.path.join拼接路径
                    path = os.path.join(self.folder_temp, f"{alarm_name}{start.strftime('%Y%m%d')}{self.down_suffix}")
                    down_file(self.URL, self.data, path)
                start = start_next

    def main(self):
        self.down()
        self.df_sql_process()
        log_downtime(self.down_name_en)

"""
爬取性能查询，路径：运行监控-性能查询-查询-导出
"""
class Performence():
    # 路径：运行监控-性能查询-查询-导出
    def __init__(self):
        self.data = foura_data.performence
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/performanceMge/perfdata.xhtml'

    def main(self, cities, search_id, folder_temp, out_put, hours=24, content_len=102400, cookie_user=1, csv=False):
        def down_by_city(city):
            for key in ['1', '2']:
                # 当前时间
                now = datetime.datetime.now()
                # 一天前的时间
                start_time = now - datetime.timedelta(hours=int(hours))

                # 格式时间化
                end_time_input_date = now.strftime("%Y-%m-%d %H:%M")
                start_time_input_date = start_time.strftime("%Y-%m-%d %H:%M")
                end_time_input_current_date = now.strftime("%m/%Y")
                start_time_input_current_date = start_time.strftime("%m/%Y")

                self.data[key]['queryForm:endtimeInputCurrentDate'] = end_time_input_current_date
                self.data[key]['queryForm:endtimeInputDate'] = end_time_input_date
                self.data[key]['queryForm:starttimeInputCurrentDate'] = start_time_input_current_date
                self.data[key]['queryForm:starttimeInputDate'] = start_time_input_date
                self.data[key]['queryForm:querySpeId'] = search_id
                self.data[key]['queryForm:querySpeIdShow'] = search_id + "..."
                self.data[key]['queryForm:unitHidden2'] = city

            # 此处path拼接已用os.path.join，无需修改
            path = os.path.join(folder_temp, f"{city}_{search_id}.xlsx")
            down_file(self.URL, self.data, path, xlsx_juge=True, conten_len_error=int(content_len),
                      cookie_user=int(cookie_user))

        for city in cities:
            down_by_city(city)
        # 用于临时下载没成功的文件
        # for city in cities:
        #     flag = 0
        #     for file in os.listdir(folder_temp):
        #         if city in file:
        #             flag=1
        #     if flag==0:
        #         down_by_city(city)
        concat_df(folder_temp, out_put, gen_csv=csv)

"""
爬取性能查询，路径：运行监控-性能查询-查询-导出
"""
class PerformenceBySiteList():
    # 路径：运行监控-性能查询-查询-导出
    # 站址运维ID入参
    def __init__(self):
        self.data = foura_data.performence.copy()
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/performanceMge/perfdata.xhtml'
        del self.data['FINAL']

    def main(self, site_list, search_id, cookie_user=1, timedelta=1440):
        df_list = []
        page_size = 200
        for i in range(0, len(site_list), page_size):
            chunk = site_list[i:i + page_size]
            for key in ['1', '2']:
                now = datetime.datetime.now()
                start_time = now - datetime.timedelta(minutes=timedelta)

                def format_datetime(dt):
                    return dt.strftime("%Y-%m-%d %H:%M"), dt.strftime("%m/%Y")

                start_time_input_date, start_time_input_current_date = format_datetime(start_time)
                end_time_input_date, end_time_input_current_date = format_datetime(now)
                self.data[key].update({
                    'queryForm:starttimeInputDate': start_time_input_date,
                    'queryForm:starttimeInputCurrentDate': start_time_input_current_date,
                    'queryForm:endtimeInputDate': end_time_input_date,
                    'queryForm:endtimeInputCurrentDate': end_time_input_current_date,
                })
                self.data[key]['queryForm:endtimeTimeHours'] = now.strftime("%H")
                self.data[key]['queryForm:endtimeTimeMinutes'] = now.strftime("%M")
                self.data[key]['queryForm:querySpeId'] = search_id
                self.data[key]['queryForm:querySpeIdShow'] = search_id + "..."
                self.data[key]['queryForm:queryStationId'] = ','.join(chunk)
                self.data[key]['queryForm:queryStationIdShow'] = f'{chunk[0]}...'
                self.data[key]['queryForm:pageSizeText'] = page_size * 4
            df = down_file_no_save(self.URL, self.data, cookie_user=int(cookie_user))
            df_list.append(df)
        return pd.concat(df_list)

"""
爬取某个站的性能查询，路径：运行监控-性能查询-查询-导出
"""
class SerchPerformence():
    def __init__(self):
        self.data = foura_data.performence
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/performanceMge/perfdata.xhtml'
        self.headers = {
            'Host': 'omms.chinatowercom.cn:9000',
            'Origin': 'http://omms.chinatowercom.cn:9000',
            'Referer': 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/performanceMge/perfdata.xhtml',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }

    def serch_performence_by_id(self, stationid, serch_id):
        cookies = get_foura_cookie()
        signal = ''
        self.data['2']['queryForm:querySpeId'] = f'{serch_id}'
        self.data['2']['queryForm:querySpeIdShow'] = f'{serch_id}...'
        try:
            res = requests_post(self.URL, headers=self.headers, cookies=cookies)
            html = BeautifulSoup(res.text, 'html.parser', from_encoding='utf-8')
            javax = html.find('input', id='javax.faces.ViewState')['value']
            into_data = self.data['2']
            into_data['javax.faces.ViewState'] = javax
            now = datetime.datetime.now()
            before = now - datetime.timedelta(days=1)

            into_data['queryForm:queryStationId'] = f'{stationid}'
            into_data['queryForm:queryStationIdShow'] = f'{stationid}...'

            into_data['queryForm:starttimeInputDate'] = before.strftime('%Y-%m-%d %H:%M')
            into_data['queryForm:starttimeInputCurrentDate'] = before.strftime('%m/%Y')
            into_data['queryForm:endtimeInputDate'] = now.strftime('%Y-%m-%d %H:%M')
            into_data['queryForm:endtimeInputCurrentDate'] = now.strftime('%m/%Y')

            res = requests_post(self.URL, headers=self.headers, data=into_data, cookies=cookies)
            html = BeautifulSoup(res.text, 'html.parser', from_encoding='utf-8')
            tbody = html.find('tbody', id='listForm:list:tb')
            tr = tbody.find_all('tr')
            for tr_item in tr:
                td = tr_item.find_all('td')
                if td[11].center.text == serch_id:
                    signal = td[14].center.text
                    signal = signal[:6]
            return signal
        except Exception as e:
            return signal

"""
爬取当前活动告警，从数据库拿，字段不完全
"""
class AlarmNow():
    def __init__(self):
        self.db_fields = [
            "ID", "告警类别", "告警对象类型", "告警对象ID", "告警对象名称",
            "告警子对象类型", "告警子对象ID", "告警子对象名称", "原始告警等级", "告警等级",
            "告警原因", "告警摘要", "告警详情", "告警附加信息", "告警发生时间",
            "首次告警的系统时间", "末次告警的系统时间", "告警重复次数", "是否已确认", "确认时间",
            "确认人", "告警类型", "用户备注信息", "是否已恢复", "是否转故障",
            "故障单号", "是否关注", "告警来源", "告警名称", "运维监控站址ID",
            "FSU名称", "告警流水号", "工单状态", "工单编号", "站址名称",
            "告警发生时站址状态", "省", "地市", "站址运维ID", "维修室人员编号",
            "所属主要告警ID", "告警上报时的站址维护状态", "首次告警时间至目前经过时间（分）",
            "离首次告警时间是否超过24小时", "信号量说明",
            "信号量解释", "任务状态", "站址来源", "确认原因", "信号量"
        ]
        self.path_csv = r'F:\newtowerV2\message\battery_life\xls\活动告警.csv'

    def main(self):
        res = requests.get(r'http://clound.gxtower.cn:3980/tt/get_alarm')
        with open(self.path_csv, "wb") as codes:
            codes.write(res.content)
        df = pd.read_csv(self.path_csv, dtype=str).rename(
            columns={"运维ID": "站址运维ID", "告警入库时间": "告警发生时间"})
        df.to_csv(self.path_csv, index=False, encoding='utf-8-sig')

        df.columns = df.columns.str.strip()
        df_db = pd.DataFrame()
        for field in self.db_fields:
            df_db[field] = df[field] if field in df.columns else None
        df_db = df_db[(df_db["ID"].notna()) & (df_db["ID"] != "")]
        df_db = df_db.where(pd.notna(df_db), None)

        with sql_orm(database='tower').session_scope() as temp:
            sql, Base = temp
            # 1. 清空表
            sql.query(Base.classes.alarm_now).delete()
            sql.commit()

            # 2. 插入新数据
            pojo = Base.classes.alarm_now
            for _, row in df_db.iterrows():
                sql.add(pojo(**row.to_dict()))
            sql.commit()
        # 南分备份需求
        if datetime.datetime.now().minute >= 40:
            backup_dir = r'F:\newtowerV2\message\nanfen_overtime\alarm_backup'
            # 改动21：增加备份目录创建
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
            current_time = datetime.datetime.now().strftime("%Y%m%d%H")
            new_filename = f'{current_time}.xlsx'
            target_path = os.path.join(backup_dir, new_filename)
            df.to_excel(target_path, index=False)
        return

"""
爬取4A活动告警，允许自定义参数，单独调用
"""
class AlarmNow4AByCity():
    def __init__(self):
        self.data = foura_data.alarm_now
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/alarmMge/listAlarm.xhtml'

    def down(self, city, path):
        for key in ['1']:
            self.data[key]['queryForm:unitHidden'] = city
        down_file(self.URL, self.data, path)

"""
爬取故障监控，路径：4A运维监控系统-运行监控-故障管理-故障监控-[活动\历史故障监控,故障场景:退服场景,时间范围：当月一号到今天]-查询-导出
"""
class FaultMonitoring():
    def __init__(self):
        self.data = foura_data.fault_monitoring
        self.URL = 'http://omms.chinatowercom.cn:9000/business/resMge/faultAlarmMge/listFaultActive.xhtml"'
        self.now = datetime.datetime.now()
        self.start_date_str = datetime.datetime(self.now.year, self.now.month, 1, 0, 0)
        self.end_date_str = datetime.datetime(self.now.year, self.now.month, self.now.day, 0, 0)
        self.down_name = '故障监控'
        self.down_name_en = 'fault_monitoring'
        self.down_suffix = '.xls'
        self.folder_temp = settings.resolve_path(f'spider/down/{self.down_name_en}/temp/')
        self.output_path = settings.resolve_path(f"spider/down/{self.down_name_en}/{self.down_name}.xlsx")

    def down(self):
        clear_folder(self.folder_temp)
        down_list = ['0099977', '0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                     '0099986', '0099987', '0099988', '0099989', '0099990']
        for city in down_list:
            for key in ['1', '2']:
                self.data[key]['hisQueryForm:unitHidden'] = city
                self.data[key]["hisQueryForm:treeCityId"] = city
                self.data[key]["hisQueryForm:firststarttimeInputDate"] = self.start_date_str.strftime('%Y-%m-%d %H:%M'),
                self.data[key]["hisQueryForm:firststarttimeInputCurrentDate"] = self.now.strftime('%m/%Y'),
                self.data[key]["hisQueryForm:firstendtimeInputDate"] = self.end_date_str.strftime('%Y-%m-%d %H:%M'),
                self.data[key]["hisQueryForm:firstendtimeInputCurrentDate"] = self.now.strftime('%m/%Y'),
                self.data[key]["hisQueryForm:recoverstarttimeInputCurrentDate"] = self.now.strftime('%m/%Y'),
                self.data[key]["hisQueryForm:recoverendtimeInputCurrentDate"] = self.now.strftime('%m/%Y'),
            path = os.path.join(self.folder_temp, f"{city}{self.down_suffix}")
            down_file(self.URL, self.data, path)

    def read_file(self):
        df_list = []
        for file in os.listdir(self.folder_temp):
            path = os.path.join(self.folder_temp, file)
            if '.xls' in file:
                temp = pd.read_excel(path, dtype=str)
                df_list.append(temp)
        return df_list

    def main(self):
        self.down()
        if len(os.listdir(self.folder_temp)) >= 14:
            df_list = self.read_file()
            merge = pd.concat(df_list)
            merge.to_excel(self.output_path, index=False)
            log_downtime(self.down_name_en)
        else:
            raise FileNotFoundError("文件下载不全，当前仅下载了 {} 个文件".format(len(os.listdir(self.folder_temp))))

if __name__ == '__main__':
    # alarm_now().main()
    # station_liangyi().main()
    # fsu_jiankong().down_5min()
    # station().main()
    # station_DC().main()
    # alarm_history_Hbase(year=2026, month=1).main()
    # FsuChaXun().main()
    # yidong_order().main()
    # yinhuan_order().main()
    FaultMonitoring().main()