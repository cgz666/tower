import requests
import json

class dingmsg():
    def __init__(self):
        #南分聚焦群
        self.JUJIAO = '7c2066272a37f54796ac2f8a1b6e6a86b2be6fc44ebe576b71f8d5d12b78652a'
        #南分实时群
        self.SHISHI = 'a8ef0067cf818d7654b1e620ca6a66827ef7c05da95401a675d871e463f7fb22'
        # 测试群
        self.TEST = '145d6470b96fc1e39461c92fa3fca7f2cdfc79798cf97f1f8c083007360a34f0'
        # 两翼工单群
        self.LYGD = '3837f4dc1cb2ed6917c0633ba141dc3669d04ec955eaed36201583cc74deedc5'
        # 地震通知群
        self.DZ='c3719b1e39b0fad3c93594275aa2c005e1e8146e593470e6ffe944b9ff650dba'
        # 地震工单群
        self.DZGD = '57140bf1e5796751338c7ecf482ad05924304b39d14f8f3a628c78358ee09627'
        # 退服停电查询群
        self.TF='e801be375ec14c92cc0fd1013bd034335377905300a2617b4b02b72271cb4ebb'
        # bug群
        # self.BUG='0285d0ccda073f351ff66fc9fd7595e3c5c41c133f940cedbe0bb770dbdf0079'
        self.BUG='951655553a93c30b66eedf3853ddabdb0440caa31333c918e654526512d835a4'
        # 电池续航派单群
        self.BATTERY_SHANGDAN='e954f3d25228951d15dfba7c8b06b17d03b29837aacf14e669d2ea327434e8eb'
    def text_at(self,webhook ,msg, num=[], men=[]):
        for phone in num:
            msg+=f'@{phone}'
        webhook='https://oapi.dingtalk.com/robot/send?access_token='+webhook
        headers = {'Content-Type': 'application/json;charset=utf-8'}
        data = {
            "at": {
                "atMobiles": num,
                "isAtAll": False
            },
            "msgtype": "text",
            "text": {
                "content": msg
            }
        }
        r = requests.post(url=webhook, headers=headers, data=json.dumps(data))
        return r.text

    def picture(self,webhook,down_url,text):
        webhook='https://oapi.dingtalk.com/robot/send?access_token='+webhook
        headers = {'Content-Type': 'application/json;charset=utf-8'}
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": "停电历时",
                "text": "![](" + down_url + ")\n" + text
            }
        }
        r = requests.post(url=webhook, headers=headers, data=json.dumps(data))
        print(r.text)
        return r.text

    def card(self,webhook,title,message,detail_url):
        webhook='https://oapi.dingtalk.com/robot/send?access_token=' + webhook
        card_msg = {
            "msgtype": "actionCard",
            "actionCard": {
                "title": title,
                "text": message,
                "btnOrientation": "0",  # 横向排列按钮
                "singleTitle": "查看详情",
                "singleURL": detail_url
            }
        }

        # 发送钉钉消息
        r = requests.post(
            webhook,
            data=json.dumps(card_msg),
            headers={'Content-Type': 'application/json'}
        )
        print(r.text)
        return r.text
