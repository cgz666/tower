import sys
import os
sys.path.append('F:/newtowerV2')
sys.path.append(r'F:/newtowerV2/venv/Lib/site-packages')
from config import INDEX
from message.ID_serch.config import config_serch_li_battery,config_serch_battery,config_serch_kaiguan
from websource.spider.down_foura.foura_spider_universal import performence as down_performence,down_file,performence_by_site_list
import threading
import pandas as pd
import shutil
from utils.sql_utils import sql_orm


class get_li_battery():
    def __init__(self):
        CONFIG=config_serch_li_battery

        self.data = {
            '1': CONFIG.INTO_DATA1,
            '1_5': CONFIG.INTO_DATA1_5,
            'FINAL': CONFIG.INTO_DATA_FINAL,
        }
        self.URL=CONFIG.URL
        self.folder=f'{INDEX}message/ID_serch/xls/电池/'


    def down(self):
        i=0
        for city in ['0099977,0099978,0099979','0099980,0099981,0099982,0099983,0099984,0099985','0099986,0099987,0099988,0099989,0099990']:
            i+=1
            self.data['1']['queryForm:unitHidden']=city
            self.data['1_5']['queryForm:unitHidden'] = city
            down_file(self.URL,self.data,self.folder+f"{i}锂电池.xls")

class get_battery():
    def __init__(self):
        CONFIG=config_serch_battery

        self.data = {
            '1': CONFIG.INTO_DATA1,
            '2': CONFIG.INTO_DATA2,
            'FINAL': CONFIG.INTO_DATA_FINAL,
        }
        self.URL=CONFIG.URL
        self.path=os.path.join(INDEX,"message/ID_serch/xls/电池/蓄电池.xls")

    def down(self):
        down_file(self.URL,self.data,self.path)

class get_kaiguan():
    def __init__(self):
        CONFIG=config_serch_kaiguan

        self.data = {
            '1': CONFIG.INTO_DATA1,
            '1_5': CONFIG.INTO_DATA1_5,
            '2': CONFIG.INTO_DATA2,
            'FINAL': CONFIG.INTO_DATA_FINAL,
        }
        self.URL=CONFIG.URL
        self.path1=os.path.join(INDEX,"message/ID_serch/xls/开关电源1.xls")
        self.path2=os.path.join(INDEX,"message/ID_serch/xls/开关电源2.xls")

    def down(self):
        self.data['1']['queryForm:unitHidden']='0099977,0099978,0099979,0099980,0099981,0099982'
        self.data['1_5']['queryForm:unitHidden'] = '0099977,0099978,0099979,0099980,0099981,0099982'
        down_file(self.URL,self.data,self.path1)

        self.data['1']['queryForm:unitHidden'] = '0099983,0099984,0099985,0099986,0099987,0099988,0099989,0099990'
        self.data['1_5']['queryForm:unitHidden'] = '0099983,0099984,0099985,0099986,0099987,0099988,0099989,0099990'
        down_file(self.URL, self.data, self.path2)

class temperature():
    def __init__(self):
        self.cities=['0099977','0099978', '0099979', '0099980', '0099981', '0099982', '0099983', '0099984', '0099985',
                 '0099986', '0099987', '0099988', '0099989', '0099990']
        self.db_fields = ['省', '市', '区县', '站址', '站址运维ID', '设备名称', '设备厂家','设备型号', '设备ID', '设备资源编码',
                          '信号量ID', '监控点','时间', '实测值', '单位', '状态', '性能数据来源']
        self.path_csv = r"F:\newtowerV2\message\ID_serch\xls\环境温度.csv"
    def down(self):
        out_dir = f"{INDEX}message/ID_serch/xls/环境温度"
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        down_performence().main(
            self.cities,
            '0418101001',
            out_dir,
            f"{out_dir}.xlsx",
            csv=True
        )
    def df_process(self):
        df = pd.read_csv(self.path_csv, dtype=str)
        df.columns = df.columns.str.strip()
        df_db = pd.DataFrame()
        for field in self.db_fields:
            df_db[field] = df[field] if field in df.columns else None
        df_db = df_db[(df_db["设备ID"].notna()) & (df_db["设备ID"] != "")]
        df_db = df_db.where(pd.notna(df_db), None)
            # 1.清空
        with sql_orm(database='tower').session_scope() as temp:
            sql, Base = temp
            sql.query(Base.classes.temperature).delete()
            sql.commit()

            # 2.插入
            pojo = Base.classes.temperature
            for _, row in df_db.iterrows():
                sql.add(pojo(**row.to_dict()))
            sql.commit()

    def main(self):
        self.down()
        self.df_process()

def down_by_site_list(site_list):
    ids = ['0406112001', '0406143001', '0406147001', '0406146001', '0406144001']
    dfs=[]
    for serch_id in ids:
        df=performence_by_site_list().main(site_list, serch_id)
        dfs.append(df)
    return tuple(dfs)
def run_thread(func, *args):
    thread = threading.Thread(target=func, args=args)
    thread.start()
def wrapper1(cities):
    down_performence().main(cities,'0406112001', f"{INDEX}message/ID_serch/xls/直流负载电流", f"{INDEX}message/ID_serch/xls/直流负载电流.xlsx",csv=True)
    down_performence().main(cities,'0406143001', f"{INDEX}message/ID_serch/xls/均充电压设定值", f"{INDEX}message/ID_serch/xls/均充电压设定值.xlsx",csv=True)
    down_performence().main(cities,'0406147001', f"{INDEX}message/ID_serch/xls/二级低压脱离设定值", f"{INDEX}message/ID_serch/xls/二级低压脱离设定值.xlsx",csv=True)
    down_performence().main(cities,'0406146001', f"{INDEX}message/ID_serch/xls/一级低压脱离设定值", f"{INDEX}message/ID_serch/xls/一级低压脱离设定值.xlsx",csv=True)
    down_performence().main(cities,'0406144001', f"{INDEX}message/ID_serch/xls/浮充电压设定值", f"{INDEX}message/ID_serch/xls/浮充电压设定值.xlsx",csv=True)
def station_shouzi_down_file(cities):
    run_thread(wrapper1, cities)

if __name__ == '__main__':
    # get_kaiguan().down()
    # get_battery().down()
    # get_li_battery().down()
    temperature().main()