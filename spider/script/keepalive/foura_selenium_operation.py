import time
import requests
from playwright.sync_api import Page, expect


def open_4a(page: Page):
    """打开4A登录页面"""
    # Playwright 不需要 switch_to.window，直接操作当前页
    # 清除 cookie 通过 context 操作，这里简化为直接跳转
    page.goto('http://4a.chinatowercom.cn:20000/uac/')
    page.wait_for_timeout(5000)  # 等待5秒


def login_menhu(page: Page, cookie_user: int):
    """登录门户"""
    if cookie_user == 0:
        username = 'wx-huangwl14'
        password = 'Cgz275694332@@@'
    else:
        username = 'wx-zhangzj102'
        password = '040213zzJ!'

    # 使用 locator 替代 find_element_by_id
    page.locator('#username').fill(username)
    page.locator('#password').fill(password)

    page.wait_for_timeout(30000)  # 等待30秒

    # 使用 class 选择器
    page.locator('.login_btn').click()

    page.wait_for_timeout(2000)

    # 执行 JavaScript
    page.evaluate("refreshMsg();")

    try:
        # 尝试找到验证码输入框，使用 wait_for 设置超时
        yzm_input = page.locator('#msgCode')

        # 检查元素是否存在（2秒超时）
        yzm_input.wait_for(timeout=2000)

        # 如果存在，执行验证码操作
        yzm = requests.get('http://clound.gxtower.cn:3980/tt/get_yzm').text
        yzm_input.fill(yzm)

        page.wait_for_timeout(2000)
        page.evaluate("doLogin();")
        page.wait_for_timeout(5000)

    except Exception as e:
        print("未检测到验证码输入框，跳过验证码步骤")


def login_yunjian(page: Page):
    """登录云鉴"""
    # 执行 JavaScript
    page.evaluate("gores('200007','20988','');")
    page.wait_for_timeout(10000)


def login_menhu_and_login_yunjian(page: Page, cookie_user: int = 0):
    """组合登录"""
    try:
        login_yunjian(page)
    except Exception as e:
        print(f"云鉴登录失败，尝试门户登录: {e}")
        login_menhu(page, cookie_user)


def refresh_fail(page: Page):
    """刷新页面"""
    page.reload()
