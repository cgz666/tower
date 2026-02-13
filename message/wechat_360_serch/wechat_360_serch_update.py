import json
import os
import shutil
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from config import INDEX
from utils.excel_operate import xlsxtocsv
from utils.retry_wrapper import requests_post
from utils.sql_utils import sql_orm


@dataclass
class FileConfig:
    """文件配置类"""
    name: str
    columns: List[str]
    type_name: str
    filters: Dict[str, str] = None
    msg_process: str = None


class Wechat360SearchUpdate:
    """
    更新微信360°检查数据，数据来源为虚拟机八爪鱼RPA爬虫，OA报表中心
    """
    FILE_CONFIGS = {
        'offline': FileConfig('综合设备-离线率-明细表', ['站址编码', '异常类型'], 'offline'),
        'equipment': FileConfig('综合设备-覆盖率-明细表', ['站址编码', '设备类型', '是否已覆盖'], 'equipment', 
                              {'是否已覆盖': '否'}),
        'fsu': FileConfig('FSU-覆盖率-明细表', ['站址编码', '是否已覆盖', '是否为白名单站址'], 'fsu',
                         {'是否为白名单站址': '否', '是否已覆盖': '否'}),
        'inaccuracy': FileConfig('综合设备-准确率-明细表', ['站址编码', '异常大类', '故障信息'], 'inaccuracy',
                                msg_process='异常大类-故障信息')
    }

    def __init__(self):
        self.move_path = os.path.join(INDEX, 'message/wechat_360_serch/xls/')
        self.file_times = {key: '' for key in self.FILE_CONFIGS}
        self.latest_files = {}

    def _get_latest_files(self) -> None:
        """获取每种类型最新的文件"""
        files = os.listdir(self.move_path)
        file_candidates = {key: [] for key in self.FILE_CONFIGS}
        
        for file in files:
            for key, config in self.FILE_CONFIGS.items():
                if config.name in file and file.endswith('.xlsx'):
                    file_path = os.path.join(self.move_path, file)
                    file_candidates[key].append((file, os.path.getctime(file_path)))
        
        for key in self.FILE_CONFIGS:
            if file_candidates[key]:
                latest_file = max(file_candidates[key], key=lambda x: x[1])[0]
                self.latest_files[key] = latest_file
                self.file_times[key] = latest_file.split('广西_')[1].replace('.xlsx', '').replace('-', '/')

    def _process_file(self, file_type: str, file_path: str) -> Optional[pd.DataFrame]:
        """处理单个文件"""
        config = self.FILE_CONFIGS[file_type]
        try:
            csv_path = xlsxtocsv(file_path)
            df = pd.read_csv(csv_path, dtype=str, usecols=config.columns)
            
            if config.filters:
                for col, value in config.filters.items():
                    df = df.loc[df[col] == value]
            
            df['类型'] = config.type_name
            df['time'] = self.file_times[file_type]
            
            if config.msg_process:
                cols = config.msg_process.split('-')
                df['msg'] = df[cols].apply(lambda x: f"{x[cols[0]]}-{x[cols[1]]}", axis=1)
            elif file_type == 'fsu':
                df['msg'] = '未覆盖'
            else:
                df = df.rename(columns={df.columns[1]: 'msg'})
                
            df = df.rename(columns={'站址编码': 'site_code', '类型': 'type'})
            return df[['site_code', 'type', 'msg', 'time']]
            
        except Exception as e:
            print(f"处理文件 {file_path} 时发生错误: {str(e)}")
            return None

    def _sync_to_vm(self, result: pd.DataFrame) -> None:
        """同步数据至虚拟机"""
        try:
            rows = [row.to_dict() for _, row in result.iterrows()]
            data = json.dumps(rows)
            SERVER_ENDPOINT = 'http://clound.gxtower.cn:3980/tt/wechat_360_dict'
            headers = {'Content-Type': 'application/json'}
            requests_post(SERVER_ENDPOINT, data=data, headers=headers)
        except Exception as e:
            print(f"同步到虚拟机失败: {str(e)}")
            raise

    def update(self):
        """更新数据主函数"""
        self._get_latest_files()
        
        with sql_orm().session_scope() as (session, Base):
            try:
                pojo = Base.classes.wechat_data
                result = None

                for file_type, file_name in self.latest_files.items():
                    full_path = os.path.join(self.move_path, file_name)
                    if not os.path.exists(full_path):
                        print(f"警告: 文件 {full_path} 不存在，跳过处理")
                        continue

                    df = self._process_file(file_type, full_path)
                    if df is not None:
                        result = pd.concat([result, df], ignore_index=True) if result is not None else df

                if result is None:
                    return

                # 更新数据库
                session.query(pojo).delete()
                session.execute(r"ALTER TABLE wechat_data AUTO_INCREMENT = 1")
                for _, row in result.iterrows():
                    session.add(pojo(**row.to_dict()))

                # 清理文件夹
                shutil.rmtree(self.move_path, ignore_errors=True)
                os.makedirs(self.move_path, exist_ok=True)

                # 同步到虚拟机
                self._sync_to_vm(result)

            except Exception as e:
                raise

# Wechat360SearchUpdate().update()4