import pandas as pd
from utils.sql_utils import sql_orm
from config import SPIDER_PATH
import pythoncom
import shutil
import os
import re
import datetime
import win32com.client as win32
from bs4 import BeautifulSoup
from utils.retry_wrapper import requests_get,requests_post
from websource.spider.down_foura.foura_spider_universal import get_foura_cookie,clear_folder,log_downtime
import psutil

def kill_excel():
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] == 'EXCEL.EXE':
            proc.kill()

class down_yitihua_order():
    def __init__(self,day=0):
        # 路径1：我的工作台-运维管理综合查询-[工单类型：历史工单，故障来源：移动/联通/电信运营商接口+铁塔集团接口，受理/回单起/始时间]-查询-导出-列表及详情
        # 路径2：我的工作台-运维管理综合查询-[工单类型：当前工单，故障来源：移动/联通/电信运营商接口+铁塔集团接口，工单状态：待故障确认]-查询-导出-列表及详情
        self.now = datetime.datetime.now().replace(hour=0, minute=0, second=0)- datetime.timedelta(days=day)
        self.start=self.now-datetime.timedelta(days=1)
        self.db_fields = ["故障单编码", "工单状态", "派单时间", "接单时间", "告警时间", "回单时间", "归档时间", "故障来源", "告警描述", "故障标题", "故障描述",
                          "站址运维ID", "站址名称", "所属省份", "所属地市", "所属区县", "故障原因", "是否免责", "申告工单故障分类", "告警清除时间", "回复内容"]

        INTO_DATA1 ={'AJAXREQUEST': '_viewRoot',
            'javax.faces.ViewState': 'j_id8',
            'queryForm': 'queryForm',
            'queryForm:addOrEditAreaNameId': '',
            'queryForm:aid': '',
            'queryForm:alarmlevel_hiddenValue': '',
            'queryForm:billStatus_hiddenValue': '',
            'queryForm:btn': 'queryForm:btn',
            'queryForm:dealendtimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:dealendtimeInputDate': '',
            'queryForm:dealstarttimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:dealstarttimeInputDate': '',
            'queryForm:deletecityIdHidden': '',
            'queryForm:deletecountryIdHidden': '',
            'queryForm:deleteproviceIdHidden': '',
            'queryForm:deviceidText': '',
            'queryForm:endtimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:endtimeInputDate': self.now.strftime('%Y-%m-%d %H:%M'),
            'queryForm:faultDevType_hiddenValue': '',
            'queryForm:faultSrc': '铁塔集团动环网管',
            'queryForm:faultSrc_hiddenValue': '铁塔集团动环网管,移动运营商接口,联通运营商接口,电信运营商接口',
            'queryForm:faultTypeId_hiddenValue': '',
            'queryForm:hideFlag': '',
            'queryForm:isHasten_hiddenValue': '',
            'queryForm:isOverTime_hiddenValue': '',
            'queryForm:isQueryHis': 'W',
            'queryForm:isReplyOver_hiddenValue': '',
            'queryForm:isTransitNodeId_hiddenValue': '',
            'queryForm:isTurnBack_hiddenValue': '',
            'queryForm:j_id139': '',
            'queryForm:j_id143': '',
            "queryForm:j_id147": "",
            'queryForm:j_id48': '',
            'queryForm:j_id58': '',
            'queryForm:msg': '0',
            'queryForm:operatorLevel_hiddenValue': '',
            'queryForm:panelOpenedState': '',
            'queryForm:queryAlarmId': '',
            'queryForm:queryAlarmName': '',
            'queryForm:queryBillId': '',
            'queryForm:queryBillSn': '',
            'queryForm:queryCrewAreaId': '',
            'queryForm:queryCrewCityId': '',
            'queryForm:queryCrewProvinceId': '',
            'queryForm:queryCrewVillageId': '',
            'queryForm:queryCrewVillageName': '',
            'queryForm:queryDWCompany': '',
            'queryForm:queryDWCompanyName': '',
            'queryForm:queryDeleteCountyName': '',
            'queryForm:queryStationId': '',
            'queryForm:queryUnitId': '0099977',
            'queryForm:querystationstatus_hiddenValue': '',
            'queryForm:refreshTime': '',
            'queryForm:revertendtimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:revertendtimeInputDate': self.now.strftime('%Y-%m-%d %H:%M'),
            'queryForm:revertstarttimeInputCurrentDate': self.start.strftime('%m/%Y'),
            'queryForm:revertstarttimeInputDate': self.start.strftime('%Y-%m-%d %H:%M'),
            'queryForm:sitesource_hiddenValue': '',
            'queryForm:sortSelect_hiddenValue': '',
            'queryForm:starttimeInputCurrentDate': self.start.strftime('%m/%Y'),
            'queryForm:starttimeInputDate': self.start.strftime('%Y-%m-%d %H:%M'),
            'queryForm:subOperatorHid_hiddenValue': '',
            'queryForm:turnSend_hiddenValue': ''}
        INTO_DATA2 = {'AJAX:EVENTS_COUNT': '1',
            'AJAXREQUEST': '_viewRoot',
            'javax.faces.ViewState': 'j_id8',
            'queryForm': 'queryForm',
            'queryForm:addOrEditAreaNameId': '',
            'queryForm:aid': '',
            'queryForm:alarmlevel_hiddenValue': '',
            'queryForm:billStatus_hiddenValue': '',
            'queryForm:dealendtimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:dealendtimeInputDate': '',
            'queryForm:dealstarttimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:dealstarttimeInputDate': '',
            'queryForm:deletecityIdHidden': '',
            'queryForm:deletecountryIdHidden': '',
            'queryForm:deleteproviceIdHidden': '',
            'queryForm:deviceidText': '',
            'queryForm:endtimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:endtimeInputDate': self.now.strftime('%Y-%m-%d %H:%M'),
            'queryForm:faultDevType_hiddenValue': '',
            'queryForm:faultSrc': '铁塔集团动环网管',
            'queryForm:faultSrc_hiddenValue': '铁塔集团动环网管,移动运营商接口,联通运营商接口,电信运营商接口',
            'queryForm:faultTypeId_hiddenValue': '',
            'queryForm:hideFlag': '',
            'queryForm:isHasten_hiddenValue': '',
            'queryForm:isOverTime_hiddenValue': '',
            'queryForm:isQueryHis': 'W',
            'queryForm:isReplyOver_hiddenValue': '',
            'queryForm:isTransitNodeId_hiddenValue': '',
            'queryForm:isTurnBack_hiddenValue': '',
            'queryForm:j_id139': '',
            'queryForm:j_id143': '',
            "queryForm:j_id147": "",
            'queryForm:j_id154': 'queryForm:j_id154',
            'queryForm:j_id48': '',
            'queryForm:j_id58': '',
            'queryForm:msg': '0',
            'queryForm:operatorLevel_hiddenValue': '',
            'queryForm:panelOpenedState': '',
            'queryForm:queryAlarmId': '',
            'queryForm:queryAlarmName': '',
            'queryForm:queryBillId': '',
            'queryForm:queryBillSn': '',
            'queryForm:queryCrewAreaId': '',
            'queryForm:queryCrewCityId': '',
            'queryForm:queryCrewProvinceId': '',
            'queryForm:queryCrewVillageId': '',
            'queryForm:queryCrewVillageName': '',
            'queryForm:queryDWCompany': '',
            'queryForm:queryDWCompanyName': '',
            'queryForm:queryDeleteCountyName': '',
            'queryForm:queryStationId': '',
            'queryForm:queryUnitId': '0099977',
            'queryForm:querystationstatus_hiddenValue': '',
            'queryForm:refreshTime': '',
            'queryForm:revertendtimeInputCurrentDate': self.now.strftime('%m/%Y'),
            'queryForm:revertendtimeInputDate': self.now.strftime('%Y-%m-%d %H:%M'),
            'queryForm:revertstarttimeInputCurrentDate': self.start.strftime('%m/%Y'),
            'queryForm:revertstarttimeInputDate': self.start.strftime('%Y-%m-%d %H:%M'),
            'queryForm:sitesource_hiddenValue': '',
            'queryForm:sortSelect_hiddenValue': '',
            'queryForm:starttimeInputCurrentDate': self.start.strftime('%m/%Y'),
            'queryForm:starttimeInputDate': self.start.strftime('%Y-%m-%d %H:%M'),
            'queryForm:subOperatorHid_hiddenValue': '',
            'queryForm:turnSend_hiddenValue': ''}
        INTO_DATA_FINAL ={'j_id1962': 'j_id1962',
            'j_id1962:devExport': '全部',
            'j_id1962:j_id1964': 'Y',
            'javax.faces.ViewState': 'j_id8'}
        INTO_DATA3={'AJAXREQUEST': '_viewRoot',
            'javax.faces.ViewState': 'j_id3',
            'queryForm': 'queryForm',
            'queryForm:addOrEditAreaNameId': '',
            'queryForm:aid': '',
            'queryForm:alarmlevel_hiddenValue': '',
            'queryForm:billStatus': 'SUPPORTCONFIRM',
            'queryForm:billStatus_hiddenValue': 'SUPPORTCONFIRM',
            'queryForm:btn': 'queryForm:btn',
            'queryForm:dealendtimeInputCurrentDate': '10/2024',
            'queryForm:dealendtimeInputDate': '',
            'queryForm:dealstarttimeInputCurrentDate': '10/2024',
            'queryForm:dealstarttimeInputDate': '',
            'queryForm:deletecityIdHidden': '',
            'queryForm:deletecountryIdHidden': '',
            'queryForm:deleteproviceIdHidden': '',
            'queryForm:deviceidText': '',
            'queryForm:endtimeInputCurrentDate': '10/2024',
            'queryForm:endtimeInputDate': '',
            'queryForm:faultDevType_hiddenValue': '',
            'queryForm:faultSrc': '移动运营商接口',
            'queryForm:faultSrc_hiddenValue': '移动运营商接口,联通运营商接口,电信运营商接口',
            'queryForm:faultTypeId_hiddenValue': '',
            'queryForm:hideFlag': '',
            'queryForm:isHasten_hiddenValue': '',
            'queryForm:isOverTime_hiddenValue': '',
            'queryForm:isQueryHis': 'N',
            'queryForm:isReplyOver_hiddenValue': '',
            'queryForm:isTransitNodeId_hiddenValue': '',
            'queryForm:isTurnBack_hiddenValue': '',
            'queryForm:j_id139': '',
            'queryForm:j_id143': '',
            "queryForm:j_id147": "",
            'queryForm:j_id48': '',
            'queryForm:j_id58': '',
            'queryForm:msg': '0',
            'queryForm:operatorLevel_hiddenValue': '',
            'queryForm:panelOpenedState': '',
            'queryForm:queryAlarmId': '',
            'queryForm:queryAlarmName': '',
            'queryForm:queryBillId': '',
            'queryForm:queryBillSn': '',
            'queryForm:queryCrewAreaId': '',
            'queryForm:queryCrewCityId': '',
            'queryForm:queryCrewProvinceId': '',
            'queryForm:queryCrewVillageId': '',
            'queryForm:queryCrewVillageName': '',
            'queryForm:queryDWCompany': '',
            'queryForm:queryDWCompanyName': '',
            'queryForm:queryDeleteCountyName': '',
            'queryForm:queryStationId': '',
            'queryForm:queryUnitId': '',
            'queryForm:querystationstatus_hiddenValue': '',
            'queryForm:refreshTime': '',
            'queryForm:revertendtimeInputCurrentDate': '10/2024',
            'queryForm:revertendtimeInputDate': '2024-10-28 11:20',
            'queryForm:revertstarttimeInputCurrentDate': '10/2024',
            'queryForm:revertstarttimeInputDate': '2024-10-21 11:20',
            'queryForm:sitesource_hiddenValue': '',
            'queryForm:sortSelect_hiddenValue': '',
            'queryForm:starttimeInputCurrentDate': '10/2024',
            'queryForm:starttimeInputDate': '2024-10-25 11:20',
            'queryForm:subOperatorHid_hiddenValue': '',
            'queryForm:turnSend_hiddenValue': ''}
        INTO_DATA4={'AJAX:EVENTS_COUNT': '1',
        'AJAXREQUEST': '_viewRoot',
        'javax.faces.ViewState': 'j_id3',
        'queryForm': 'queryForm',
        'queryForm:addOrEditAreaNameId': '',
        'queryForm:aid': '',
        'queryForm:alarmlevel_hiddenValue': '',
        'queryForm:billStatus': 'SUPPORTCONFIRM',
        'queryForm:billStatus_hiddenValue': 'SUPPORTCONFIRM',
        'queryForm:dealendtimeInputCurrentDate': '10/2024',
        'queryForm:dealendtimeInputDate': '',
        'queryForm:dealstarttimeInputCurrentDate': '10/2024',
        'queryForm:dealstarttimeInputDate': '',
        'queryForm:deletecityIdHidden': '',
        'queryForm:deletecountryIdHidden': '',
        'queryForm:deleteproviceIdHidden': '',
        'queryForm:deviceidText': '',
        'queryForm:endtimeInputCurrentDate': '10/2024',
        'queryForm:endtimeInputDate': '',
        'queryForm:faultDevType_hiddenValue': '',
        'queryForm:faultSrc': '移动运营商接口',
        'queryForm:faultSrc_hiddenValue': '移动运营商接口,联通运营商接口,电信运营商接口',
        'queryForm:faultTypeId_hiddenValue': '',
        'queryForm:hideFlag': '',
        'queryForm:isHasten_hiddenValue': '',
        'queryForm:isOverTime_hiddenValue': '',
        'queryForm:isQueryHis': 'N',
        'queryForm:isReplyOver_hiddenValue': '',
        'queryForm:isTransitNodeId_hiddenValue': '',
        'queryForm:isTurnBack_hiddenValue': '',
        'queryForm:j_id139': '',
        'queryForm:j_id143': '',
        'queryForm:j_id147': '',
        'queryForm:j_id154': 'queryForm:j_id154',
        'queryForm:j_id48': '',
        'queryForm:j_id58': '',
        'queryForm:msg': '0',
        'queryForm:operatorLevel_hiddenValue': '',
        'queryForm:panelOpenedState': '',
        'queryForm:queryAlarmId': '',
        'queryForm:queryAlarmName': '',
        'queryForm:queryBillId': '',
        'queryForm:queryBillSn': '',
        'queryForm:queryCrewAreaId': '',
        'queryForm:queryCrewCityId': '',
        'queryForm:queryCrewProvinceId': '',
        'queryForm:queryCrewVillageId': '',
        'queryForm:queryCrewVillageName': '',
        'queryForm:queryDWCompany': '',
        'queryForm:queryDWCompanyName': '',
        'queryForm:queryDeleteCountyName': '',
        'queryForm:queryStationId': '',
        'queryForm:queryUnitId': '',
        'queryForm:querystationstatus_hiddenValue': '',
        'queryForm:refreshTime': '',
        'queryForm:revertendtimeInputCurrentDate': '10/2024',
        'queryForm:revertendtimeInputDate': '2024-10-28 11:20',
        'queryForm:revertstarttimeInputCurrentDate': '10/2024',
        'queryForm:revertstarttimeInputDate': '2024-10-21 11:20',
        'queryForm:sitesource_hiddenValue': '',
        'queryForm:sortSelect_hiddenValue': '',
        'queryForm:starttimeInputCurrentDate': '10/2024',
        'queryForm:starttimeInputDate': '2024-10-25 11:20',
        'queryForm:subOperatorHid_hiddenValue': '',
        'queryForm:turnSend_hiddenValue': ''}
        INTO_DATA_FINAL2={'j_id1962': 'j_id1962',
        'j_id1962:devExport': '全部',
        'j_id1962:j_id1964': 'Y',
        'javax.faces.ViewState': 'j_id3'}

        self.data={
            '1':INTO_DATA1,
            '2':INTO_DATA2,
            'FINAL':INTO_DATA_FINAL,
        }
        self.data2 = {
            '1': INTO_DATA3,
            '2': INTO_DATA4,
            'FINAL': INTO_DATA_FINAL2,
        }
        self.city_dict={
            0: "南宁",
            1: "柳州",
            2: "桂林",
            3: "玉林",
            4: "贵港",
            5: "百色",
            6: "河池",
            7: "钦州",
            8: "梧州",
            9: "北海",
            10: "防城港",
            11: "崇左",
            12: "来宾",
            13: "贺州"
        }

        self.URL = 'http://omms.chinatowercom.cn:9000/billDeal/monitoring/list/billList.xhtml'
        self.down_name='一体化工单表'
        self.down_name_en='yitihua_order'
        self.down_suffix='.xls'

        self.folder_temp=f'{SPIDER_PATH}{self.down_name_en}/temp/'
        self.output_path = f"{SPIDER_PATH}{self.down_name_en}/{self.start.strftime('%Y%m%d')}"
        self.folder_temp_process=f'{SPIDER_PATH}{self.down_name_en}/process/'
        self.sheet1=os.path.join(self.folder_temp_process, '1.xlsx')
        self.sheet2=os.path.join(self.folder_temp_process, '2.xlsx')
        self.model=os.path.join(self.folder_temp_process, '模板.xlsx')
        self.result=os.path.join(self.folder_temp, '结果.xlsx')
    def down_post(self,url, data, path, conten_len_error=300000):
        i=0
        while i<3:
            i+=1
            try:
                # 重写该方法
                headers = {
                    "Accept-Encoding": "gzip, deflate",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Host": "omms.chinatowercom.cn:9000",
                    "Origin": "http://omms.chinatowercom.cn:9000",
                    "Pragma": "no-cache",
                    "Referer": "http://omms.chinatowercom.cn:9000/billDeal/monitoring/list/billList.xhtml",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0"
                }
                cookies = get_foura_cookie()
                res = requests_post(url, headers=headers, cookies=cookies)
                html = BeautifulSoup(res.text, 'html.parser')
                javax = html.find('input', id='javax.faces.ViewState')['value']
                for key, into_data in data.items():
                    into_data['javax.faces.ViewState'] = javax
                    if 'FINAL' in key:
                        headers['Upgrade-Insecure-Requests'] = '1'
                        headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
                    res = requests_post(url, headers=headers, data=into_data, cookies=cookies)
                    if 'FINAL' in key:
                        if len(res.content) < conten_len_error: raise ValueError("Content size is less than 300KB")
                        with open(path, "wb") as codes:
                            codes.write(res.content)
                break
            except Exception as e:pass

    def down(self):
        clear_folder(self.folder_temp)

        now = datetime.datetime.now()

        # 修复：1号时查询上个月整月数据
        if now.day == 1:
            # 1号时：上个月1号到本月1号（即上个月整月）
            month_1st = (now.replace(day=1) - datetime.timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0,
                                                                                  microsecond=0)
            today_0am = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)  # 本月1号0时
            yesterday_0am = today_0am - datetime.timedelta(days=1)  # 上月最后一天0时
            # 月份字符串使用上个月的
            month_str = month_1st.strftime('%m/%Y')
        else:
            # 非1号时：保持原有逻辑
            month_1st = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            today_0am = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_0am = today_0am - datetime.timedelta(days=1)
            month_str = now.strftime('%m/%Y')

        for index, city in enumerate(['0099977', '0099978', '0099979', '0099980', '0099981',
                                      '0099982', '0099983', '0099984', '0099985', '0099986',
                                      '0099987', '0099988', '0099989', '0099990']):
            for key in ['1', '2']:  # 1、2 为查询请求
                d = self.data[key]

                # 受理时间
                d['queryForm:starttimeInputDate'] = month_1st.strftime("%Y-%m-%d %H:%M")
                d['queryForm:endtimeInputDate'] = today_0am.strftime("%Y-%m-%d %H:%M")

                # 回单时间
                d['queryForm:revertstarttimeInputDate'] = month_1st.strftime("%Y-%m-%d %H:%M")
                d['queryForm:revertendtimeInputDate'] = today_0am.strftime("%Y-%m-%d %H:%M")

                # 归档时间
                d['queryForm:dealstarttimeInputDate'] = yesterday_0am.strftime("%Y-%m-%d %H:%M")
                d['queryForm:dealendtimeInputDate'] = today_0am.strftime("%Y-%m-%d %H:%M")

                # 月份控件
                d['queryForm:starttimeInputCurrentDate'] = month_str
                d['queryForm:endtimeInputCurrentDate'] = month_str
                d['queryForm:revertstarttimeInputCurrentDate'] = month_str
                d['queryForm:revertendtimeInputCurrentDate'] = month_str
                d['queryForm:dealstarttimeInputCurrentDate'] = month_str
                d['queryForm:dealendtimeInputCurrentDate'] = month_str

                # 城市
                d['queryForm:queryUnitId'] = city
            path = f"{self.folder_temp}{self.city_dict[index]}{self.down_suffix}"
            self.down_post(self.URL, self.data, path)
    def down2(self):
        for key in ['1', '2']:
            self.data2[key]['queryForm:dealstarttimeInputCurrentDate'] = datetime.datetime.now().strftime('%m/%Y')
            self.data2[key]['queryForm:dealendtimeInputCurrentDate'] = datetime.datetime.now().strftime('%m/%Y')
            self.data2[key]['queryForm:starttimeInputCurrentDate'] = self.start.strftime('%m/%Y')
            self.data2[key]['queryForm:endtimeInputCurrentDate'] = self.now.strftime('%m/%Y')
            self.data2[key]['queryForm:revertstarttimeInputCurrentDate'] = self.start.strftime('%m/%Y')
            self.data2[key]['queryForm:revertendtimeInputCurrentDate'] = self.now.strftime('%m/%Y')
            self.data2[key]['queryForm:starttimeInputDate'] = self.start.strftime('%Y-%m-%d %H:%M')
            self.data2[key]['queryForm:endtimeInputDate'] = self.now.strftime('%Y-%m-%d %H:%M')
            self.data2[key]['queryForm:revertstarttimeInputDate'] = self.start.strftime('%Y-%m-%d %H:%M')
            self.data2[key]['queryForm:revertendtimeInputDate'] = self.now.strftime('%Y-%m-%d %H:%M')
        path = f"{self.folder_temp}待故障确认{self.down_suffix}"
        self.down_post(self.URL, self.data2, path)
    def down_not_process(self):
        #下载非铁塔文件
        for key in ['1','2']:
            self.data[key]['queryForm:queryUnitId'] = ''
            self.data[key]['queryForm:isQueryHis']='N'
            self.data[key]['queryForm:faultSrc_hiddenValue']='移动运营商接口,联通运营商接口,电信运营商接口'
            self.data[key]['queryForm:dealstarttimeInputCurrentDate'] = datetime.datetime.now().strftime('%m/%Y')
            self.data[key]['queryForm:dealendtimeInputCurrentDate'] = datetime.datetime.now().strftime('%m/%Y')
            self.data[key]['queryForm:starttimeInputCurrentDate'] = self.start.strftime( '%m/%Y')
            self.data[key]['queryForm:endtimeInputCurrentDate'] = self.now.strftime( '%m/%Y')
            self.data[key]['queryForm:revertstarttimeInputCurrentDate'] = self.start.strftime( '%m/%Y')
            self.data[key]['queryForm:revertendtimeInputCurrentDate'] = self.now.strftime( '%m/%Y')
            self.data[key]['queryForm:starttimeInputDate'] = self.start.strftime( '%Y-%m-%d %H:%M')
            self.data[key]['queryForm:endtimeInputDate'] = self.now.strftime( '%Y-%m-%d %H:%M')
            self.data[key]['queryForm:revertstarttimeInputDate'] = self.start.strftime( '%Y-%m-%d %H:%M')
            self.data[key]['queryForm:revertendtimeInputDate'] = self.now.strftime( '%Y-%m-%d %H:%M')
        path = f"{self.folder_temp}全区运营商工单{self.down_suffix}"
        self.down_post(self.URL, self.data, path)
    def df_process(self):
        def save_sql(df):
            df.columns = df.columns.str.strip()
            df_db = pd.DataFrame()
            for field in self.db_fields:
                df_db[field] = df[field] if field in df.columns else None
            df_db = df_db[(df_db["故障单编码"].notna()) & (df_db["故障单编码"] != "")]
            df_db = df_db.where(pd.notna(df_db), None)
            with sql_orm(database='自助取数').session_scope() as temp:
                sql, Base = temp
                pojo = getattr(Base.classes,"一体化工单")
                for index, row in df_db.iterrows():
                    temp = pojo(**row.to_dict())
                    sql.merge(temp)

        df_list = []
        for file in os.listdir(self.folder_temp):
            df = pd.read_excel(os.path.join(self.folder_temp, file), dtype=str, sheet_name=0)
            df_list.append(df)
            save_sql(df)
        df = pd.concat(df_list)
        df.to_excel(self.sheet1, index=False)


        df_list = []
        for file in os.listdir(self.folder_temp):
            df = pd.read_excel(os.path.join(self.folder_temp, file), dtype=str, sheet_name=1)
            df_list.append(df)
        df = pd.concat(df_list)
        df.to_excel(self.sheet2, index=False)
    def excel_process(self):
        # 关闭所有Excel进程
        kill_excel()
        pythoncom.CoInitialize()
        excel = win32.Dispatch('Excel.Application')
        excel.Visible = False
        excel.DisplayAlerts = False

        try:
            # 打开sheet1、sheet2和模板
            wb1 = excel.Workbooks.Open(os.path.abspath(self.sheet1))
            wb2 = excel.Workbooks.Open(os.path.abspath(self.sheet2))
            wb_sum = excel.Workbooks.Open(os.path.abspath(self.model))

            # 复制sheet1数据到“工单清单”表（从第1行开始）
            ws1 = wb1.Worksheets('Sheet1')
            ws_sum_1 = wb_sum.Worksheets('工单清单')
            # 清空I列及之后的所有数据
            ws_sum_1.Range("I:DA").ClearContents()
            ws1.UsedRange.Copy(ws_sum_1.Range('I1'))

            # 复制sheet2数据到“留痕催办超时质检”表
            ws2 = wb2.Worksheets('Sheet1')
            ws_sum_2 = wb_sum.Worksheets('留痕催办超时质检')
            # 清空A到H列的所有数据
            ws_sum_2.Range("A:H").ClearContents()  # 加括号执行
            ws2.UsedRange.Copy(ws_sum_2.Range('A1'))

            # 保存结果文件
            wb_sum.SaveAs(os.path.abspath(self.result))  # 用win32com的SaveAs确保格式正确

        except Exception as e:
            print(f"Excel合并错误: {e}")
        finally:
            # 关闭所有工作簿和Excel
            for wb in [wb1, wb2, wb_sum]:
                if wb:
                    wb.Close(SaveChanges=False)
            excel.Quit()
            del wb1, wb2, wb_sum, excel
            pythoncom.CoUninitialize()

        self.down_not_process()
        shutil.make_archive(self.output_path, 'zip', self.folder_temp)
    def temp(self):
        self.df_process()
        self.excel_process()
        log_downtime(self.down_name_en)
    def main(self):
        self.down()
        self.down2()
        self.df_process()
        self.excel_process()
        log_downtime(self.down_name_en)


if __name__ == '__main__':
# down_yitihua_order().temp()
    for i in [0]:
        down_yitihua_order(day=i).main()