import os
from message.ID_serch.config import config_serch_li_battery,config_serch_battery,config_serch_kaiguan
from spider.script.down_foura.foura_spider_universal import down_file
import threading
import pandas as pd
import shutil
from core.sql import sql_orm
from core.config import settings


class GetLiBattery():
    def __init__(self):
        CONFIG=config_serch_li_battery

        self.data = {
            '1': CONFIG.INTO_DATA1,
            '1_5': CONFIG.INTO_DATA1_5,
            'FINAL': CONFIG.INTO_DATA_FINAL,
        }
        self.URL=CONFIG.URL
        self.folder=settings.resolve_path(f'message/ID_serch/xls/电池/')


    def down(self):
        i=0
        for city in ['0099977,0099978,0099979','0099980,0099981,0099982,0099983,0099984,0099985','0099986,0099987,0099988,0099989,0099990']:
            i+=1
            self.data['1']['queryForm:unitHidden']=city
            self.data['1_5']['queryForm:unitHidden'] = city
            down_file(self.URL,self.data,self.folder+f"{i}锂电池.xls")

class GetBattery():
    def __init__(self):
        CONFIG=config_serch_battery

        self.data = {
            '1': CONFIG.INTO_DATA1,
            '2': CONFIG.INTO_DATA2,
            'FINAL': CONFIG.INTO_DATA_FINAL,
        }
        self.URL=CONFIG.URL
        self.path=settings.resolve_path(f'message/ID_serch/xls/电池/蓄电池.xls')

    def down(self):
        down_file(self.URL,self.data,self.path)

class GetKaiGuan():
    def __init__(self):
        CONFIG=config_serch_kaiguan

        self.data = {
            '1': CONFIG.INTO_DATA1,
            '1_5': CONFIG.INTO_DATA1_5,
            '2': CONFIG.INTO_DATA2,
            'FINAL': CONFIG.INTO_DATA_FINAL,
        }
        self.URL=CONFIG.URL
        self.path1=settings.resolve_path(f'message/ID_serch/xls/开关电源1.xls')
        self.path2=settings.resolve_path(f'message/ID_serch/xls/开关电源2.xls')

    def down(self):
        self.data['1']['queryForm:unitHidden']='0099977,0099978,0099979,0099980,0099981,0099982'
        self.data['1_5']['queryForm:unitHidden'] = '0099977,0099978,0099979,0099980,0099981,0099982'
        down_file(self.URL,self.data,self.path1)

        self.data['1']['queryForm:unitHidden'] = '0099983,0099984,0099985,0099986,0099987,0099988,0099989,0099990'
        self.data['1_5']['queryForm:unitHidden'] = '0099983,0099984,0099985,0099986,0099987,0099988,0099989,0099990'
        down_file(self.URL, self.data, self.path2)


if __name__ == '__main__':
    GetKaiGuan().down()
    # GetBattery().down()
    # GetLiBattery().down()
