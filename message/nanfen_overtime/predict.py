import pandas as pd
from config import INDEX
from utils.send_ding_msg import dingmsg
from utils.sql_utils import sql_orm
from sqlalchemy import or_, and_
from sqlalchemy import text
from websource.spider.down_foura.foura_spider_universal import alarm_now_4A_by_city


class predict():
    def __init__(self):
        self.alarm_path=f"{INDEX}message/nanfen_overtime/xls/活动告警.xls"

    def send_alarm_msg(self, df, msg_type, send_list=None):
        """统一处理告警消息发送"""
        sqlsession, Base = self.temp
        pojo_phone = Base.classes.phone_predict
        jkzx_phone = sqlsession.query(pojo_phone).filter(pojo_phone.name == '南宁市监控中心').first().phone

        if msg_type == 'highlevel_jiaoliu':
            pojo = Base.classes.predict_high
            # 重置不在当前告警列表中的站址状态
            for item in sqlsession.query(pojo).filter(pojo.send_or_not == '1').all():
                if item.sitename not in df['站址名称'].values:
                    item.send_or_not = '0'

            for _, row in df.iterrows():
                res = sqlsession.query(pojo).filter(pojo.sitename == row['站址名称']).first()
                if res is None or res.send_or_not == '0':
                    if res is None:
                        temp = pojo(sitename=row['站址名称'], send_or_not='1')
                        sqlsession.merge(temp)
                    else:
                        res.send_or_not = '1'

                    text = f"{row['市']}{row['区县']}({row['站址保障等级']})：{row['站址名备注']}，{row['告警发生时间']}发生交流停电告警，告警历时{row['告警历时(分钟)']}分钟"
                    nums = [jkzx_phone]
                    names = ['南宁市监控中心']

                    for res in sqlsession.query(pojo_phone).filter(
                            and_(pojo_phone.area == row['区县'], pojo_phone.level == '代维维护主管')).all():
                        nums.append(res.phone)
                        names.append(res.name)

                    dingmsg().text_at(dingmsg().SHISHI, text, nums, names)
        else:
            # 其他告警通报
            pojo = Base.classes.predict_other
            df['id'] = df['站址名称'] + df['告警名称']

            # 重置不在当前告警列表中的记录状态
            for item in sqlsession.query(pojo).filter(pojo.send_or_not == '1').all():
                if item.id not in df['id'].values:
                    item.send_or_not = 0

            for _, row in df.iterrows():
                for broken_time, levels in send_list.items():
                    if row['告警历时(分钟)'] >= broken_time:
                        res = sqlsession.query(pojo).filter(pojo.id == row['id']).first()
                        if res is None or res.send_or_not < broken_time:
                            if res is None:
                                temp = pojo(id=row['id'], send_or_not=broken_time)
                                sqlsession.merge(temp)
                            else:
                                res.send_or_not = broken_time

                            text = f"【重要】故障督办：{row['市']}{row['区县']}({row['站址保障等级']})：{row['站址名备注']}，{row['告警发生时间']}发生{row['告警名称']}，历时{row['告警历时(分钟)']}分钟未恢复，请督办处理。"
                            nums = [jkzx_phone]
                            names = ['南宁市监控中心']

                            for level in levels:
                                # 获取指定级别的所有联系人
                                contacts = sqlsession.query(pojo_phone).filter(pojo_phone.level == level).all()
                                for contact in contacts:
                                    # 如果联系人区域是南宁市则不筛选区县，否则需要匹配区县
                                    if contact.area == '南宁市' or (contact.area == row['区县']):
                                        nums.append(contact.phone)
                                        names.append(contact.name)

                            dingmsg().text_at(dingmsg().SHISHI, text, nums, names)
                            break

    def get_area(self, x):
        sql, Base = self.temp
        try:
            res = sql.execute(text(f"select * from tower.station_with_area where site_maitan_code={x}")).first()
            return res.area
        except:
            return ''

    def process(self):
        # 读取并预处理数据
        df = pd.read_excel(self.alarm_path,
                         usecols=['市', '站址名称', '站址名备注', '告警名称', '站址运维ID', '站址保障等级', '告警历时(分钟)', '告警发生时间', '设备告警开始时间'])
        df = df.loc[df['市'] == '南宁市分公司']
        df['区县'] = df['站址运维ID'].apply(self.get_area)
        df['站址保障等级'] = df['站址保障等级'].fillna('')
        df['市'] = df['市'].str.replace('分公司', '')

        # 按保障等级分类处理
        df_highlevel = df[df['站址保障等级'].str.contains('L4|L3|L1')]
        df_normal = df[~df['站址保障等级'].str.contains('L4|L3|L1')]

        # 处理三种告警情况
        alarm_types = ['交流输入停电告警', '一级低压脱离告警']
        df_highlevel = df_highlevel.loc[df_highlevel['告警名称'].isin(alarm_types)]
        df_normal = df_normal.loc[df_normal['告警名称'].isin(alarm_types)]

        self.send_alarm_msg(df_highlevel, 'highlevel_other', {
            180: ['铁塔分管副总经理'],
            120: ['代维项目经理', '区域经理', '监控主管', '代维分管领导'],
            60: ['代维维护主管', '维护助理']
        })

        self.send_alarm_msg(df_normal, 'normal_other', {
            240: ['运维部门经理'],
            180: ['区域经理', '监控主管', '代维分管领导'],
            120: ['市监控', '代维项目经理', '维护助理', '代维维护主管']
        })
        df_highlevel = df_highlevel.loc[(df['告警名称'] == '交流输入停电告警') & (df['告警历时(分钟)'] >= 10)]
        self.send_alarm_msg(df_highlevel, 'highlevel_jiaoliu')

    def run_thread(self):
        alarm_now_4A_by_city().down("0099977",self.alarm_path)
        with sql_orm(database='nanfen').session_scope() as self.temp:
            self.process()

# predict().run_thread()