from message.station_name_serch import down_config as CONFIG
from utils.sql_utils import sql_orm
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import INDEX



def main():
    def down(site_name, serch_name, serch_id, download_dir):
        max_retries = 3  # 最大重试次数
        retry_count = 0  # 当前重试次数
        timeout = 15  # 请求超时时间（秒）

        while retry_count < max_retries:
            try:
                # 爬取
                with sql_orm().session_scope() as (sql, Base):
                    pojo = Base.classes.foura
                    fa = sql.query(pojo).first()
                    cookies_str = fa.Cookie
                    cookies = {}
                    for cookie in cookies_str.split(';'):
                        key, value = cookie.split('=')
                        cookies[key] = value

                url = 'http://omms.chinatowercom.cn:9000/business/resMge/pwMge/performanceMge/perfdata.xhtml'

                headers = {
                    'Host': 'omms.chinatowercom.cn:9000',
                    'Origin': 'http://omms.chinatowercom.cn:9000',
                    'Referer': url,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
                }

                response = requests.post(url, headers=headers, cookies=cookies, timeout=timeout)
                response.raise_for_status()  # 检查请求是否成功

                soup = BeautifulSoup(response.text, 'html.parser')
                input_tag = soup.find('input', {'id': 'javax.faces.ViewState'})
                javax = input_tag.get('value')

                data1 = {'AJAX:EVENTS_COUNT': '1',
                         'AJAXREQUEST': '_viewRoot',
                         'autoScroll': '',
                         'javax.faces.ViewState': javax,
                         'stationListFormB': 'stationListFormB',
                         'stationListFormB:currPageObjId': '0',
                         'stationListFormB:j_id723': 'stationListFormB:j_id723',
                         'stationListFormB:nameText': site_name,
                         'stationListFormB:queryStatusId': '2',
                         'stationListFormB:stationidText': ''}

                response1 = requests.post(url, headers=headers, data=data1, cookies=cookies, timeout=timeout)
                response1.raise_for_status()  # 检查请求是否成功

                soup1 = BeautifulSoup(response1.text, 'html.parser')
                result_dict = {}

                # 提取 queryForm2B（动态值）
                target_input = soup1.find('input', {'name': 'selectFlagB'})
                try:
                    queryForm2B = target_input.get('id')
                except:
                    return
                result_dict['queryForm2B'] = queryForm2B

                # 提取 provIdHiddenB
                prov_id_hidden = soup1.find('input', {'name': 'provIdHiddenB'})
                if prov_id_hidden:
                    provIdHiddenB = prov_id_hidden.get('value')
                    result_dict['provIdHiddenB'] = provIdHiddenB

                # 提取 selectFlagB
                select_flag = soup1.find('input', {'name': 'selectFlagB'})
                if select_flag:
                    selectFlagB = select_flag.get('value')
                    result_dict['selectFlagB'] = selectFlagB

                # 发起新的 POST 请求
                data2 = {
                    'AJAX:EVENTS_COUNT': '1',
                    'AJAXREQUEST': '_viewRoot',
                    'javax.faces.ViewState': javax,  # 使用从响应中提取的 javax.faces.ViewState
                    'queryForm2B': 'queryForm2B',
                    'queryForm2B:aid': result_dict['queryForm2B'],  # 使用提取的动态值
                    'queryForm2B:aname': '',
                    'queryForm2B:j_id679': 'queryForm2B:j_id679',
                    'queryForm2B:panel2OpenedState': ''
                }

                response2 = requests.post(url, headers=headers, data=data2, cookies=cookies, timeout=timeout)
                response2.raise_for_status()  # 检查请求是否成功

                soup2 = BeautifulSoup(response2.text, 'html.parser')
                radio_inputs = soup2.find_all('input', {'type': 'radio'})
                # 提取设备名称和对应的 id
                device_info = []
                for radio in radio_inputs:
                    device_id = radio.get('id')  # 获取 id 属性值
                    device_name = radio.get('value')  # 获取 value 属性值（设备名称）
                    if device_id and device_name:
                        device_info.append((device_id, device_name))

                # 根据 serch_name 的位置限制 device_info 中的 item[1] 的内容
                if serch_name in list(serch_dict.keys())[:4]:  # 前四个
                    device_info = [(device_id, device_name) for device_id, device_name in device_info if
                                   '开关电源' in device_name]
                elif serch_name in list(serch_dict.keys())[4:8]:  # 中间四个
                    device_info = [(device_id, device_name) for device_id, device_name in device_info if
                                   '智能备电控制设备' in device_name]
                elif serch_name in list(serch_dict.keys())[8:]:  # 最后两个
                    device_info = [(device_id, device_name) for device_id, device_name in device_info if
                                   '分路计量设备' in device_name]

                now = datetime.now()
                start_time = now - timedelta(days=2)
                start_time_str = start_time.strftime("%Y-%m-%d 00:00")
                end_time_str = now.strftime("%Y-%m-%d %H:%M")
                start_time_month = start_time.strftime("%m/%Y")
                end_time_month = now.strftime("%m/%Y")

                for item in device_info:
                    data3 = {'AJAXREQUEST': '_viewRoot',
                             'javax.faces.ViewState': javax,
                             'queryFormB': 'queryFormB',
                             'queryFormB:aid': queryForm2B,
                             'queryFormB:currPageObjId': '0',
                             'queryFormB:deviceName': item[1],
                             'queryFormB:did': item[0],
                             'queryFormB:endtimeInputCurrentDate': end_time_month,
                             'queryFormB:endtimeInputDate': end_time_str,
                             'queryFormB:j_id184': 'queryFormB:j_id184',
                             'queryFormB:mid': serch_id,
                             'queryFormB:midName': serch_name,
                             'queryFormB:midType': '遥测',
                             'queryFormB:pageSizeText': '35',
                             'queryFormB:panelOpenedState': '',
                             'queryFormB:queryFlag': '3',
                             'queryFormB:queryFsuId': '',
                             'queryFormB:querySiteSourceCode': '',
                             'queryFormB:siteNameId': selectFlagB,
                             'queryFormB:siteProvinceId': provIdHiddenB,
                             'queryFormB:starttimeInputCurrentDate': start_time_month,
                             'queryFormB:starttimeInputDate': start_time_str,
                             'queryFormB:unitHidden1': '',
                             'queryFormB:unitTypeHidden': ''}

                    response3 = requests.post(url, headers=headers, data=data3, cookies=cookies, timeout=timeout)
                    response3.raise_for_status()  # 检查请求是否成功

                    data4 = {'AJAX:EVENTS_COUNT': '1',
                             'AJAXREQUEST': '_viewRoot',
                             'javax.faces.ViewState': javax,
                             'queryFormB': 'queryFormB',
                             'queryFormB:aid': queryForm2B,
                             'queryFormB:currPageObjId': '0',
                             'queryFormB:deviceName': item[1],
                             'queryFormB:did': item[0],
                             'queryFormB:endtimeInputCurrentDate': end_time_month,
                             'queryFormB:endtimeInputDate': end_time_str,
                             'queryFormB:j_id185': 'queryFormB:j_id185',
                             'queryFormB:mid': serch_id,
                             'queryFormB:midName': serch_name,
                             'queryFormB:midType': '遥测',
                             'queryFormB:pageSizeText': '35',
                             'queryFormB:panelOpenedState': '',
                             'queryFormB:queryFlag': '3',
                             'queryFormB:queryFsuId': '',
                             'queryFormB:querySiteSourceCode': '',
                             'queryFormB:siteNameId': selectFlagB,
                             'queryFormB:siteProvinceId': provIdHiddenB,
                             'queryFormB:starttimeInputCurrentDate': start_time_month,
                             'queryFormB:starttimeInputDate': start_time_str,
                             'queryFormB:unitHidden1': '',
                             'queryFormB:unitTypeHidden': ''}

                    response4 = requests.post(url, headers=headers, data=data4, cookies=cookies, timeout=timeout)
                    response4.raise_for_status()  # 检查请求是否成功

                    data5 = {'j_id430': 'j_id430',
                             'j_id430:j_id432': '全部',
                             'javax.faces.ViewState': javax}

                    response5 = requests.post(url, headers=headers, data=data5, cookies=cookies, timeout=timeout)
                    response5.raise_for_status()  # 检查请求是否成功

                    if len(response5.content) < 3000:
                        raise ValueError("Content size is less than 3KB")

                    # 创建独立文件夹
                    site_folder = os.path.join(download_dir, site_name)
                    os.makedirs(site_folder, exist_ok=True)

                    # 保存文件
                    file_name = f"{item[1].split('/')[1]}-{serch_name}.xls"
                    file_path = os.path.join(site_folder, file_name)
                    with open(file_path, "wb") as codes:
                        codes.write(response5.content)
                return True  # 成功完成下载
            except requests.exceptions.RequestException as e:
                print(f"Request failed for {site_name}: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"Failed to download {site_name} after {max_retries} retries. Skipping...")
                    return False  # 下载失败
    def process_downloaded_files(df, download_dir):
        """
        处理下载后的文件，更新 df 中的第5、6、7列。
        """
        try:
            df=df.fillna('')
            df['站址名称']=df['站址名称'].str.replace(' ','')
            # 初始化设备状态
            df.at[:, df.columns[5]] = '否'  # 第5列：开关电源
            df.at[:, df.columns[6]] = '否'  # 第6列：分路计量设备
            df.at[:, df.columns[7]] = '否'  # 第7列：智能备电控制设备
            for index, row in df.iterrows():
                site_name = row['站址名称']
                site_folder = os.path.join(download_dir, site_name)
                if not os.path.exists(site_folder):
                    continue

                devices = {
                    "开关电源": {"has_current": False, "has_power": False, "column_index": 5},
                    "分路计量设备": {"has_current": False, "has_power": False, "column_index": 6},
                    "智能备电控制设备": {"has_current": False, "has_power": False, "column_index": 7}
                }

                for filename in os.listdir(site_folder):
                    for device_name, device_info in devices.items():
                        if device_name in filename:
                            df.at[index, df.columns[device_info["column_index"]]] = '是'  # 设备存在
                            file_path = os.path.join(site_folder, filename)
                            try:
                                data = pd.read_excel(file_path)
                                if data.empty:  # 如果文件内容为空，跳过
                                    continue

                                # 检查实测值列是否有非零值
                                if not data['实测值'].eq(0).all():
                                    if '电流' in filename:
                                        device_info["has_current"] = True
                                    if '电量' in filename:
                                        device_info["has_power"] = True

                                    # 如果已经找到电流和电量，直接跳出循环
                                    if device_info["has_current"] and device_info["has_power"]:
                                        break
                            except Exception as e:
                                print(f"Error reading file {filename}: {e}")

                # 更新设备状态
                for device_name, device_info in devices.items():
                    if df.at[index, df.columns[device_info["column_index"]]] == '是':
                        df.at[index, df.columns[device_info[
                            "column_index"]]] += f"，{'有电流' if device_info['has_current'] else '无电流'}，{'有电量' if device_info['has_power'] else '无电量'}"
        except Exception as e:
            print(e)
            print(1)

        # 保存更新后的 df 到新的 Excel 文件
        df.to_excel(r'F:\newtowerV2\message\station_name_serch\更新后的查询结果.xlsx', index=False)

    # 读取站址名称清单
    serch_dict = {
        "行业外租户电流": "0406171001",
        "行业外租户电量": "0406172001",
        "计量型新业务租户XX电流": "0406162001",
        "计量型新业务租户XX电量": "0406163001",
        "行业外租户直流负载上月电量": "0455193021",
        "行业外租户直流负载电流": "0455135021",
        "行业外租户直流负载累计电量": "0455137021",
        "行业外租户直流负载当月电量": "0455138021",
        "计量型新业务租户1#电流": "0445110001",
        "计量型新业务租户1#电量": "0445111001"
    }
    df = pd.read_excel(r'F:\newtowerV2\message\station_name_serch\站址名称清单.xlsx', dtype=str)
    download_dir = r'F:\newtowerV2\message\station_name_serch\xls'

    # # 创建线程池并发执行下载任务
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for index, row in df.iterrows():
            site_name = row['站址名称']
            for key, value in serch_dict.items():
                futures.append(executor.submit(down, site_name, key, value, download_dir))

        # 等待所有任务完成
        for future in as_completed(futures):
            if future.result() is False:
                print(f"Failed to download: {future.result()}")

    # 统一处理下载文件
    process_downloaded_files(df, download_dir)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)