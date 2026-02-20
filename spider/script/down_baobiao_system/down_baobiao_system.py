import os
import requests
import datetime
from spider.script.down_foura.foura_spider_universal import log_downtime
from core.config import settings


class BaoBiaoSystem():
    def send_file(self):
        self.down_name_en = 'baobiao_system'
        file_path = settings.resolve_path("updatenas/fsu_lixian_qingkuang/FSU离线情况明细_日.xlsx")
        url = 'http://clound.gxtower.cn:3980/tt/fsu_lixian_qingkuang'

        # 1. 检查文件修改日期
        file_mtime = datetime.date.fromtimestamp(os.path.getmtime(file_path))
        if file_mtime != datetime.date.today():
            raise RuntimeError(f"文件修改日期不是今天（{file_mtime}），禁止上传！")

        # 2. 上传
        with open(file_path, 'rb') as f:
            files = {'file': f}
            response = requests.post(url, files=files)
            print(response)

    def main(self):
        self.send_file()
        log_downtime(self.down_name_en)


if __name__ == '__main__':
    BaoBiaoSystem().main()