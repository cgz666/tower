import os
from seleniumwire import webdriver
import requests
import datetime
from websource.keepalive.foura_selenium_operation import \
    open_4a,login_menhu,login_baobiao,login_yunjian,choose_table,\
    login_menhu_and_login_yunjian,refresh_fail,login_baobiao_fail,choose_table_fail
from websource.spider.down_foura.foura_spider_universal import log_downtime
BROWSER_DOWN='C:/Users/Administrator/Downloads/'

class BaoBiaoSystem():
    def test(self,driver):
        while True:
            try:
                code = input('输入新语句')
                exec(code)
            except Exception as e:
                print(e)

    def run_thread(self):
        option = webdriver.ChromeOptions()
        option.add_argument("--start-maximized")
        option.add_argument("--incognito")
        # option.add_argument('--headless')
        option.add_argument(r'--user-data-dir=C:\Users\Administrator\AppData\Local\Google\Chrome for Testing\User Data')

        driver = webdriver.Chrome(chrome_options=option, executable_path=f'{INDEX}chromedriver.exe')
        driver.implicitly_wait(0)
        driver.set_page_load_timeout(30)
        driver.delete_all_cookies()
        try:
            j=0
            while j<30:
                j+=1
                flag=0
                driver.switch_to.window(driver.window_handles[-1])
                try:
                    alert = driver.switch_to.alert
                    alert_text = alert.text
                    print(alert_text)
                    alert.accept()
                except:
                    pass
                if 'http://4a.chinatowercom.cn:20000/uac/' in driver.current_url:
                    func = login_menhu_and_login_yunjian(driver)
                    func_fail = refresh_fail(driver)
                elif 'http://omms.chinatowercom.cn:9000/From4A.jsp' in driver.current_url:
                    func=login_baobiao
                    func_fail = login_baobiao_fail
                elif driver.current_url=='http://ywbb.chinatowercom.cn:8080/bi/?proc=0&action=viewerManager':
                    func=choose_table
                    func_fail = choose_table_fail
                else:
                    func = open_4a
                    func_fail = refresh_fail(driver)
                try:
                    result = func(driver)
                    flag = 1
                    if result != None: break
                except Exception as e:print(e)
                if flag==0:
                    try:
                        func_fail(driver)
                    except Exception as e: print(e)

        except Exception as e:
            raise
        finally:
          driver.quit()
    def send_file(self):
        self.down_name_en = 'baobiao_system'
        file_path = r"F:\newtowerV2\updatenas\fsu_lixian_qingkuang\FSU离线情况明细_日.xlsx"
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
        # self.run_thread()
        self.send_file()
        log_downtime(self.down_name_en)


if __name__ == '__main__':
    BaoBiaoSystem().main()