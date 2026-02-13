import pandas as pd
from config import INDEX
import os
from message.ID_serch.down_file import down_by_site_list

def get_table(path):
    try:
        out_put_path = path.replace('.xlsx', '结果.xlsx')
        df = pd.read_excel(path, dtype=str)
        df1, df2, df3, df4, df5 = down_by_site_list(df["站址运维ID"].to_list())

        df1.to_excel("df1.xlsx",index=False)
        df2.to_excel("df2.xlsx", index=False)
        df3.to_excel("df3.xlsx", index=False)
        df4.to_excel("df4.xlsx", index=False)
        df5.to_excel("df5.xlsx", index=False)
        # df1 = pd.read_excel("df1.xlsx",dtype=str)
        # df2 = pd.read_excel("df2.xlsx",dtype=str)
        # df3 = pd.read_excel("df3.xlsx",dtype=str)
        # df4 = pd.read_excel("df4.xlsx",dtype=str)
        # df5 = pd.read_excel("df5.xlsx",dtype=str)
        usecols = ['站址运维ID', '设备ID', '设备名称', '实测值', '时间']
        def process_add_df(add_df, value_col_name, no_first=True):
            add_df=add_df[usecols]
            add_df['时间'] = pd.to_datetime(add_df['时间'])
            add_df = add_df.sort_values(by='时间', ascending=False).drop_duplicates(subset=['站址运维ID', '设备ID'],
                                                                                  keep='first')
            add_df = add_df.rename(columns={'时间': f'时间_{value_col_name}'})
            if no_first:
                add_df = add_df.drop(columns=['设备名称'])
            return add_df.rename(columns={'实测值': value_col_name})

        df1 = process_add_df(df1, '直流负载电流(A)', no_first=False)
        df2 = process_add_df(df2, '均充电压设定值')
        df3 = process_add_df(df3, '二级低压脱离设定值')
        df4 = process_add_df(df4, '一级低压脱离设定值')
        df5 = process_add_df(df5, '浮充电压设定值')
        # 构建完整映射
        mapping_dfs = [df[['站址运维ID', '设备ID']].drop_duplicates() for df in [df1, df2, df3, df4, df5]]
        full_mapping = pd.concat(mapping_dfs, ignore_index=True).drop_duplicates()
        df = pd.merge(df, full_mapping, on='站址运维ID', how='left')
        for add_df in [df1, df2, df3, df4, df5]:
            df = pd.merge(df, add_df, on=['站址运维ID','设备ID'], how='left')
        # 处理站址信息
        usecols = ['所属省', '所属市', '区县（行政区划）', '乡镇（街道）', '名称', '站址编码', '运维ID', '站址经度', '站址纬度', '站址细分类型', '供电方式（一级）']
        add_df = pd.read_csv(f"{INDEX}websource/spider_download/station/站址信息.csv", dtype=str, usecols=usecols)
        add_df = add_df.rename(columns={
            '所属省': '省份',
            '所属市': '地市',
            '乡镇（街道）': '乡镇',
            '名称': '站址名称',
            '运维ID': '站址运维ID',
            '站址经度': '经度(小数点后6位)',
            '站址纬度': '纬度(小数点后6位)',
            '站址细分类型': '站址类型',
            '供电方式（一级）': '供电方式'
        })
        df = pd.merge(df, add_df, on='站址运维ID', how='left')

        # 处理FSU信息
        usecols = ['站址运维ID', 'FSU硬件厂家', '设备型号']
        add_df = pd.read_csv(f"{INDEX}websource/spider_download/fsu_chaxun_all/fsu清单.csv", dtype=str, usecols=usecols)
        add_df = add_df.rename(columns={'FSU硬件厂家': 'FSU厂家', '设备型号': 'FSU规格型号'})
        df = pd.merge(df, add_df, on='站址运维ID', how='left')

        # 处理电池信息
        folder = f'{INDEX}message/ID_serch/xls/电池/'
        df_list = []
        for path in os.listdir(folder):
            if '锂' in path:
                usecols = ['站址运维ID', '入网状态', '类型', '生产厂商', '型号']
                temp_df = pd.read_excel(folder + path, dtype=str, usecols=usecols)
                # 修正原代码bug：添加赋值操作
                temp_df = temp_df.rename(columns={'型号': '蓄电池型号'})
            else:
                usecols = ['站址运维ID', '入网状态', '类型', '生产厂商', '蓄电池型号']
                temp_df = pd.read_excel(folder + path, dtype=str, usecols=usecols)
            df_list.append(temp_df)

        add_df = pd.concat(df_list)
        add_df = add_df.loc[(add_df['入网状态'] == '在网') | (add_df['入网状态'] == '初始录入')]
        add_df = add_df.drop(columns='入网状态')
        add_df = add_df.rename(columns={'类型': '电池类型', '生产厂商': '蓄电池厂家'})
        df = pd.merge(df, add_df, on='站址运维ID', how='left')

        # 处理开关电源信息
        usecols = ['站址运维ID', '生产厂商', '型号', '监控模块型号', '入网状态']
        path1 = os.path.join(INDEX, "message/ID_serch/xls/开关电源1.xls")
        path2 = os.path.join(INDEX, "message/ID_serch/xls/开关电源2.xls")
        add_df = pd.concat(pd.read_excel(p, dtype=str, usecols=usecols) for p in [path1, path2])
        add_df = add_df.loc[(add_df['入网状态'] == '在网') | (add_df['入网状态'] == '初始录入')]
        add_df = add_df.rename(columns={'生产厂商': '开关电源厂家', '型号': '开关电源型号'})
        df = pd.merge(df, add_df, on='站址运维ID', how='left')

        # 新增电池状况列，固定值为"电池正常"
        df['电池状况'] = '电池正常'

        # 重命名列名以匹配需求
        df = df.rename(columns={
            '区县（行政区划）': '国家行政区县',
            '站址运维ID': '运维ID',
            '均充电压设定值': '均充电压值',
            '浮充电压设定值': '浮充电压值'
        })
        df=df.drop_duplicates(subset=[ '运维ID','设备ID', '浮充电压值',
            '直流负载电流(A)', '均充电压值', '一级低压脱离设定值', '二级低压脱离设定值', '电池类型', '蓄电池型号', '电池状况',
            'FSU规格型号', '开关电源型号', '监控模块型号'
        ])
        # 新增序号列（从1开始）
        df['序号'] = range(1, len(df) + 1)
        new_columns_order = [
            '序号', '省份', '地市', '国家行政区县', '乡镇', '站址名称', '站址编码', '运维ID','设备ID',
            '经度(小数点后6位)', '纬度(小数点后6位)', '浮充电压值',
            '直流负载电流(A)', '均充电压值', '一级低压脱离设定值', '二级低压脱离设定值', '站址类型',
            '供电方式', '电池类型', '蓄电池厂家', '蓄电池型号', '电池状况', 'FSU厂家',
            'FSU规格型号', '开关电源厂家', '开关电源型号', '监控模块型号'
        ]
        time_columns = [col for col in df.columns if '时间' in col]
        final_columns_order = new_columns_order + [col for col in time_columns if col not in new_columns_order]
        df = df.reindex(columns=final_columns_order)

        # 导出主表
        df.to_excel(out_put_path, index=False, engine='openpyxl')

        # 统计每个站址运维ID具有的直流负载总电流个数
        count_df = df.groupby('运维ID')['直流负载电流(A)'].nunique().reset_index()
        count_df.rename(columns={'直流负载电流(A)': '开关电源组数'}, inplace=True)

        # 统计电池
        battery_count = df.groupby('运维ID')['电池类型'].apply(
            lambda x: (x.str.contains('锂', na=False).sum(), (~x.str.contains('锂', na=False)).sum())).apply(pd.Series)
        battery_count.columns = ['锂电池组数', '蓄电池组数']
        count_df = count_df.merge(battery_count, on='运维ID', how='left')

        # 将统计结果保存为子表
        with pd.ExcelWriter(out_put_path, engine='openpyxl', mode='a') as writer:
            count_df.to_excel(writer, sheet_name='直流负载电流统计', index=False)

        return out_put_path
    except Exception as e:
        print(e)
        return '失败'

# get_table(r"F:\newtowerV2\message\ID_serch\xls\查询用站址运维ID.xlsx")