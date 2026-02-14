# scheduler/scheduler_main.py
import schedule
import time
import threading
import random
from scheduler.task_logger import log_task_execution
from app.service.msg_energy_offline.script import main as msg_energy_offline
from app.service.msg_energy_order.script import main as msg_energy_order
from app.service.msg_zhilian_order.script import main as msg_zhilian_order
from spider.script.down_foura import down_yitihua_order,foura_spider_universal
from message.performance_sheet.script import PerformanceSheet
from message.ID_serch.down_file import GetBattery,GetLiBattery,GetKaiGuan

def run_task_in_thread(task_func, task_name):
    """在独立线程中运行任务并记录日志"""
    def wrapper():
        try:
            log_task_execution(task_name, task_func)
        except Exception as e:
            print(f"{task_name}: {e}")
    thread = threading.Thread(target=wrapper, daemon=True)
    thread.start()
    return thread

def schedule_loop():
    """主调度循环"""
    print("循环开始")
    schedule.every(5).minutes.do(run_task_in_thread,msg_energy_offline,"能源设备离线短信")
    schedule.every(5).minutes.do(run_task_in_thread,msg_energy_order,"能源工单短信")
    schedule.every(5).minutes.do(run_task_in_thread,msg_zhilian_order,"智联工单短信")
    # 每5分钟的其他任务（需要确认 log_to_file 和对应函数可用）
    schedule.every(5).minutes.do(run_task_in_thread,lambda: foura_spider_universal.alarm_now().main(), "活动告警")
    schedule.every(5).minutes.do(run_task_in_thread,predict().run_thread, "预测任务")  # 需要确认 predict 导入
    schedule.every(5).minutes.do(run_task_in_thread,lambda: foura_spider_universal.fsu_jiankong().down_5min(), "FSU监控-5分钟")

    # ==================== 每10分钟执行 ====================
    schedule.every(10).minutes.do(run_task_in_thread,lambda: battery_life_caculate().run_calculate(), "电池寿命计算")
    # 注意：原代码中 battery_life_caculate 调用了多个方法，建议封装成一个入口方法

    # ==================== 每6小时执行 ====================
    schedule.every(6).hours.do(run_task_in_thread,Wechat360SearchUpdate().update, "微信360搜索更新")
    schedule.every(6).hours.do(run_task_in_thread,lambda: foura_spider_universal.alarm_history_Hbase().main(), "历史告警Hbase")
    schedule.every(6).hours.do(run_task_in_thread,lambda: battery_life_caculate().calculate_offline(), "离线电池计算")

    # ==================== 每小时执行（整点） ====================
    schedule.every().hour.at(":00").do(run_task_in_thread,lambda: foura_spider_universal.fsu_jiankong().down(), "FSU监控下载(整点)")

    # ==================== 每小时执行（半点） ====================
    schedule.every().hour.at(":30").do(run_task_in_thread,lambda: foura_spider_universal.fsu_jiankong().down(), "FSU监控下载(半点)")

    # ==================== 每天执行（特定时间） ====================
    # 0:00
    schedule.every().day.at("00:00").do(run_task_in_thread,lambda: foura_spider_universal.station_alias().main(), "站址别名更新")

    # 1:00
    schedule.every().day.at("01:00").do(run_task_in_thread,lambda: (foura_spider_universal.station().main(), foura_spider_universal.station_liangyi().main()),"站址数据下载")

    # 3:00
    schedule.every().day.at("03:00").do(run_task_in_thread,lambda: (get_battery().down(), get_kaiguan().down(), get_li_battery().down()),"电池开关锂电下载")

    # 7:00
    schedule.every().day.at("07:00").do(run_task_in_thread, task_7, "任务7")
    schedule.every().day.at("07:00").do(run_task_in_thread,lambda: foura_spider_universal.yidong_order().main(), "移动工单")

    # 7:40
    schedule.every().day.at("07:40").do(run_task_in_thread,lambda: foura_spider_universal.fsu_chaxun().main(), "FSU查询")
    schedule.every().day.at("07:40").do(run_task_in_thread,down_baobiao_system().main, "报表系统下载")

    # 8:00
    schedule.every().day.at("08:00").do(run_task_in_thread,lambda: foura_spider_universal.YinHuanOrder().main(), "隐患工单")
    schedule.every().day.at("08:00").do(run_task_in_thread,lambda: down_yitihua_order.YiTiHuaOrder().main(), "一体化工单下载")

    # 8:00, 14:00, 17:00（多时间点）
    schedule.every().day.at("08:00").do(run_task_in_thread,lambda: (time.sleep(random.uniform(5, 20)), performance_sheet().run()), "性能查询表")
    schedule.every().day.at("14:00").do(run_task_in_thread,lambda: (time.sleep(random.uniform(5, 20)), performance_sheet().run()), "性能查询表")
    schedule.every().day.at("17:00").do(run_task_in_thread,lambda: (time.sleep(random.uniform(5, 20)), performance_sheet().run()), "性能查询表")

    # 13:40
    schedule.every().day.at("13:40").do(run_task_in_thread,lambda: foura_spider_universal.fsu_chaxun().main(), "FSU查询(下午)")

    # 14:30
    schedule.every().day.at("14:30").do(run_task_in_thread,lambda: (time.sleep(random.uniform(5, 20)), temperature().main()), "温度采集")

    # 16:00
    schedule.every().day.at("16:00").do(run_task_in_thread,lambda: foura_spider_universal.station_DC().main(), "站址直流采集")

    # 16:40
    schedule.every().day.at("16:40").do(run_task_in_thread,lambda: foura_spider_universal.fsu_chaxun().main(), "FSU查询(傍晚)")

    # 23:00
    schedule.every().day.at("23:00").do(run_task_in_thread,lambda: BatteryLevel().main(), "电池电量采集")

    # 23:54
    schedule.every().day.at("23:54").do(run_task_in_thread,lambda: (down_equiment_consitution(), down_equiment_lixian()), "设备构成与离线")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule_loop()

