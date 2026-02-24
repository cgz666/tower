# spider/script/cookies_keep_foura.py
import time
import json
import datetime
import threading
import requests
from contextlib import contextmanager
from playwright.sync_api import sync_playwright, Page
from bs4 import BeautifulSoup
from core.sql import sql_orm
from core.utils.retry_wrapper import requests_get, requests_post
from spider.script.keepalive import keepalive_config as CONFIG


class KeepFourA:
    def __init__(self):
        # 常量定义 - 保留所有 FourA 参数
        self.LOGIN_URL = 'http://4a.chinatowercom.cn:20000/uac/'
        self.KEEP_FA_URL = CONFIG.KEEP_FA_URL
        self.KEEP_FA_URL2 = CONFIG.KEEP_FA_URL2
        self.USERNAME_LIST = {
            0: {'username': 'wx-huangwl14', 'password': 'Cgz275694332@@@'},
            # 1: {'username': 'wx-zhangzj102', 'password': '040213zzJ!'}
        }
        self.USER_DATA_DIR = r'C:\Users\Administrator\AppData\Local\Google\Chrome for Testing\User Data'
        self.YZM_URL = 'http://clound.gxtower.cn:3980/tt/get_yzm'

    def _setup_browser(self):
        """配置并返回 Playwright 浏览器实例"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=['--start-maximized', '--incognito']
        )
        self.context = self.browser.new_context(
            user_data_dir=self.USER_DATA_DIR,
            viewport=None
        )
        self.page = self.context.new_page()
        self.page.set_default_timeout(30000)
        return self.page

    def _close_browser(self):
        """关闭浏览器"""
        try:
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except Exception as e:
            print(f"关闭浏览器失败: {e}")

    def _open_4a(self, page: Page):
        """打开4A登录页面"""
        page.goto(self.LOGIN_URL)
        page.wait_for_timeout(5000)

    def _login_menhu(self, page: Page, cookie_user: int = 0):
        """登录门户"""
        user_info = self.USERNAME_LIST.get(cookie_user, self.USERNAME_LIST[0])
        username = user_info['username']
        password = user_info['password']

        # 使用 locator 替代 find_element
        page.locator('#username').fill(username)
        page.locator('#password').fill(password)

        page.wait_for_timeout(30000)  # 等待30秒

        # 点击登录按钮
        page.locator('.login_btn').click()

        page.wait_for_timeout(2000)

        # 执行 JavaScript
        page.evaluate("refreshMsg();")

        try:
            # 尝试找到验证码输入框
            yzm_input = page.locator('#msgCode')
            yzm_input.wait_for(timeout=2000)

            # 获取验证码并输入
            yzm = requests.get(self.YZM_URL).text
            yzm_input.fill(yzm)

            page.wait_for_timeout(2000)
            page.evaluate("doLogin();")
            page.wait_for_timeout(5000)

        except Exception as e:
            print("未检测到验证码输入框，跳过验证码步骤")

    def _login_yunjian(self, page: Page):
        """登录云鉴"""
        page.evaluate("gores('200007','20988','');")
        page.wait_for_timeout(10000)

    def _login_menhu_and_login_yunjian(self, page: Page, cookie_user: int = 0):
        """组合登录门户和云鉴，返回 cookie"""
        try:
            self._login_yunjian(page)
        except Exception as e:
            print(f"云鉴登录失败，尝试门户登录: {e}")
            self._login_menhu(page, cookie_user)

        # 获取 cookie
        cookies = self.context.cookies()
        cookie_str = '; '.join([f"{c['name']}={c['value']}" for c in cookies])
        return {'cookie': cookie_str}

    def _refresh_fail(self, page: Page):
        """刷新页面"""
        page.reload()

    def _login_and_get_cookies(self, page: Page, cookie_user: int = 0):
        """执行登录流程并获取 cookies"""
        j = 0
        result = None

        while j < 10:
            j += 1
            flag = 0

            # 获取当前页面
            current_page = self.context.pages[-1] if self.context.pages else page
            current_url = current_page.url

            if 'http://4a.chinatowercom.cn:20000/uac/' in current_url:
                func = self._login_menhu_and_login_yunjian
                func_fail = self._refresh_fail
            else:
                func = self._open_4a
                func_fail = self._refresh_fail

            try:
                result = func(current_page, cookie_user)
                flag = 1
                if result is not None:
                    break
            except Exception as e:
                print(f"登录尝试失败: {e}")

            if flag == 0:
                try:
                    func_fail(current_page)
                except:
                    pass

        return result

    def get_cookies(self, cookie_user: int = 0):
        """执行 FourA 登录并获取 Cookie 信息，保存到数据库"""
        page = None
        try:
            page = self._setup_browser()
            self.context.clear_cookies()

            result = self._login_and_get_cookies(page, cookie_user)

            if result:
                with sql_orm().session_scope() as temp:
                    sqlsession, Base = temp
                    pojo = Base.classes.foura
                    # 根据 cookie_user 选择对应的记录
                    fa_list = sqlsession.query(pojo).all()
                    if fa_list and len(fa_list) > cookie_user:
                        fa = fa_list[cookie_user]
                        fa.Cookie = result.get('cookie')
                        fa.LastLoginTime = datetime.datetime.now()
                        print(f'4A账号{cookie_user}登录成功')
                    else:
                        print(f"数据库中找不到账号{cookie_user}的记录")
            else:
                print(f'4A账号{cookie_user}登录失败，未获取到cookie')

        except Exception as e:
            print(f"获取 FourA Cookie 失败: {e}")
            raise
        finally:
            self._close_browser()

    def keep_cookies(self):
        """检查 FourA 登录状态，若失效则重新登录"""
        with sql_orm().session_scope() as temp:
            sqlsession, Base = temp
            pojo = Base.classes.foura
            fa_list = sqlsession.query(pojo).all()

            for cookie_user in range(len(fa_list)):
                try:
                    fa = fa_list[cookie_user]
                    cookies_str = fa.Cookie
                    headers = CONFIG.KEEP_FA_HEADERS.copy()
                    headers['Cookie'] = cookies_str
                    headers['Referer'] = CONFIG.KEEP_FA_URL2

                    res = requests_post(CONFIG.KEEP_FA_URL2, headers=headers, timeout=20)
                    html = BeautifulSoup(res.text, 'html.parser', from_encoding='utf-8')
                    javax = html.find('input', id='javax.faces.ViewState')['value']

                    print(f'4A账号{cookie_user}保活成功{datetime.datetime.now().strftime("%Y%m%d %H%M")}')

                except Exception as e:
                    print(f'4A账号{cookie_user}保活不成功{datetime.datetime.now().strftime("%Y%m%d %H%M")}')
                    try:
                        self.get_cookies(cookie_user)
                    except Exception as e:
                        print(f"重新登录失败: {e}")

    def keep_OA(self):
        """OA 保活"""
        try:
            with self._get_oa_dashuju() as res:
                if '"result":"success"' in res.text:
                    pass
                else:
                    print('oa保活不成功')
        except Exception as e:
            print(f"OA保活异常: {e}")

    @contextmanager
    def _get_oa_dashuju(self):
        """获取 OA 大数据 token"""
        try:
            # 初始化
            HEADERS = CONFIG.OA_TO_DASHUJU_HEADERS.copy()
            with sql_orm().session_scope() as temp:
                session, Base = temp
                pojo = Base.classes.oa
                oa = session.query(pojo).first()
                HEADERS['Cookie'] = oa.Cookie

            into_data = {
                'appId': 'CHNTBUS',
                'userId': '10059182',
                'superUserCode': '',
                'superRandom': ''
            }

            # 获取跳转 Token
            res = requests_post(
                url='http://4a.chinatowercom.cn:20000/uac_oa/ssoV3Module',
                headers=HEADERS,
                data=into_data
            )
            yield res

            loginKey = json.loads(res.text)['v3MenuParamsJson']
            loginKey = json.loads(loginKey)[0]['loginKey']

            pwdaToken = json.loads(res.text)['v3MenuParamsJson']
            pwdaToken = json.loads(pwdaToken)[0]['pwdaToken']

            # 获取跳转后 session
            into_data = {
                'acctId': '10059182',
                'flag': '',
                'loginKey': loginKey,
                'menuCode': 'CHNTBUS',
                'pwdaToken': pwdaToken,
                'superUserCode': ''
            }
            res = requests_post(
                url="http://180.153.49.232:58280/business/ssoLogin",
                data=into_data
            )

            SESSION = res.request.headers['Cookie']

            # 获取 X-Csrf-Token
            HEADERS = CONFIG.GET_DASHUJU_HEADERS.copy()
            HEADERS['Cookie'] = SESSION
            now = datetime.datetime.now().timestamp()
            now = str(int(now * 1000))
            res = requests_get(
                url='http://180.153.49.232:58280/business/logged?=' + now,
                headers=HEADERS
            )
            XTOKEN = json.loads(res.text)['_csrf']['token']

            with sql_orm().session_scope() as temp:
                session, Base = temp
                pojo = Base.classes.oa
                oa = session.query(pojo).first()
                oa.Session = SESSION
                oa.XTOKEN = XTOKEN

        except Exception as e:
            raise


def run_thread(func):
    """在线程中运行函数"""
    thread = threading.Thread(target=func)
    thread.start()


def run_task_min():
    """每分钟执行的任务"""
    keep_foura = KeepFourA()
    run_thread(keep_foura.keep_OA)
    run_thread(keep_foura.keep_cookies)


def main_keep_cookies():
    """主函数：保活"""
    KeepFourA().keep_cookies()


def main_get_cookies():
    """主函数：获取 cookie"""
    KeepFourA().get_cookies(cookie_user=0)
