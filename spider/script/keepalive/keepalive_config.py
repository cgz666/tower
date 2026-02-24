KEEP_FA_URL ='http://omms.chinatowercom.cn:9000/portal/TaskDefineController/exportExcel'
KEEP_FA_URL2 = 'http://omms.chinatowercom.cn:9000/business/resMge/alarmMge/listAlarm.xhtml'

KEEP_FA_HEADERS = {
    'Host': 'omms.chinatowercom.cn:9000',
    'Origin': 'http://omms.chinatowercom.cn:9000',
    'Referer': 'http://omms.chinatowercom.cn:9000/portal/iframe.html?modules/domian/views/listIndexTask',
    'Cookie':'',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
}
GET_OA_HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                  'Accept-Encoding': 'gzip, deflate',
                  'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                  'Cache-Control': 'max-age=0', 'Connection': 'keep-alive',
                  'Cookie': '',
                  'Host': '4a.chinatowercom.cn',
                  'Upgrade-Insecure-Requests': '1',
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0'}
GET_FA_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '10',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': '',
    'Host': '4a.chinatowercom.cn:20000',
    'Origin': 'http://4a.chinatowercom.cn:20000',
    'Pragma': 'no-cache',
    'Referer': 'http://4a.chinatowercom.cn:20000/uac_oa/sso',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.200',
    'X-Requested-With': 'XMLHttpRequest'
}

OA_TO_DASHUJU_HEADERS=HEADERS = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": "",
            "Host": "eip.chinatowercom.cn:30801",
            "Referer": "http://4a.chinatowercom.cn/",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.0.0"
        }
GET_DASHUJU_HEADERS={
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh,en;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': '',
            'Host': '180.153.49.232:58280',
            'Referer': 'http://180.153.49.232:58280/business/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.0.0',
            'X-Csrf-Token': '',
            'X-Requested-With': 'XMLHttpRequest'
        }

