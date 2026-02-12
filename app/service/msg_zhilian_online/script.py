import pandas as pd
import numpy as np
from core.msg.msg_text import AddressBookManagement
from core.config import settings
class ZhilianOnlineMsg:
    def __init__(self):
        self.ali_msg_code="SMS_500800084"
    def run_thread(self):
        # 构造文件路径
        file_path =settings.resolve_path(r"app/service/msg_zhilian_online/data/智联在线情况.xlsx")
        df = pd.read_excel(file_path, header=None)
        # 构建两层表头
        header_rows = df.iloc[:2].copy()
        data_rows = df.iloc[2:].reset_index(drop=True)
        header_rows.iloc[0] = header_rows.iloc[0].ffill()
        multi_cols = pd.MultiIndex.from_arrays(header_rows.values, names=['Group', 'Metric'])
        df = data_rows.copy()
        df.columns = multi_cols
        # 初始化
        result = df[[('管理区域（市名称）', np.nan),('管理区域（区名称）', np.nan), ('总部可监控', '在线率')]]
        result.columns = ['市','区', '在线率']
        result = result.loc[pd.isna(result['区']) & (~pd.isna(result['市']))].drop(columns='区')
        if len(result)!=14:raise ValueError("数据不为14个地市，有错误停止执行短信")
        # 计算数据
        result['在线率'] = pd.to_numeric(
            result['在线率'].astype(str).str.rstrip('%'),
            errors='coerce'
        )
        result = result[result['在线率'] < 95].copy()
        result['在线率'] = result['在线率'].apply(lambda x: f"{x:.2f}%")
        result['市'] = (
            result['市']
            .astype(str)
            .str.replace('市', '', regex=False)
            .str.replace('分公司', '', regex=False)
            .str.strip()  # 去除可能的多余空格
        )
        for _, row in result.iterrows():
            data = {
                'city': row['市'],
                'rate': row['在线率']
            }
            manager_df = AddressBookManagement().get_address_book(city=row['市'],level="二级督办对象",businessCategory="拓展",  tasks="智联设备在线率提醒")
            if len(manager_df) > 0:
                manager_df = manager_df[(manager_df['position']=='运维部拓展主管')|(manager_df['position']=='运维部监控主管')]
                manager_df = manager_df.drop_duplicates(subset=['phone'])
                AddressBookManagement().send_msg(manager_df, data, self.ali_msg_code)
def main():
    ZhilianOnlineMsg().run_thread()
# 示例调用
if __name__ == "__main__":
    main()