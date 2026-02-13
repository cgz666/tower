import os
import sys
from typing import List
from alibabacloud_dysmsapi20170525.client import Client as Dysmsapi20170525Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dysmsapi20170525 import models as dysmsapi_20170525_models
from alibabacloud_tea_util import models as util_models
import datetime
from utils_or_config.mysql_connecter import sql_orm_tower
import json

class Sample():
    def __init__(self):
        pass

    @staticmethod
    def create_client(
            access_key_id: str,
            access_key_secret: str,
    ) -> Dysmsapi20170525Client:
        config = open_api_models.Config(
            # 必填，您的 AccessKey ID,
            access_key_id=access_key_id,
            # 必填，您的 AccessKey Secret,
            access_key_secret=access_key_secret
        )
        config.endpoint = f'dysmsapi.aliyuncs.com'
        return Dysmsapi20170525Client(config)

    @staticmethod
    def main(
            phone,json_text,code
    ) -> None:
        # 请确保代码运行环境设置了环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET。
        # 工程代码泄露可能会导致 AccessKey 泄露，并威胁账号下所有资源的安全性。以下代码示例使用环境变量获取 AccessKey 的方式进行调用，仅供参考，建议使用更安全的 STS 方式，更多鉴权访问方式请参见：https://help.aliyun.com/document_detail/378659.html
        client = Sample.create_client(os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'],
                                      os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'])
        send_sms_request = dysmsapi_20170525_models.SendSmsRequest(
            phone_numbers=phone,
            sign_name='润建股份',
            template_code=code,
            template_param=json_text
        )
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            ali_response=client.send_sms_with_options(send_sms_request, runtime)
            with sql_orm_tower().session_scope() as temp:
                sqlsession, Base = temp
                pojo = Base.classes.alimsg_log
                temp = pojo(time=datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'), msg=f"{json.loads(json_text)}",phone=phone,code=code,ali_response=ali_response.body.message)
                sqlsession.add(temp)
        except Exception as error:
            # 如有需要，请打印 error
            print('发送失败'+str(error))
