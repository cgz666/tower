# scheduler/scheduler_main.py
import schedule
import time
import threading
import openpyxl
import xlrd
from scheduler.task_logger import log_task_execution
from app.service.msg_energy_offline.script import main as msg_energy_offline
from app.service.msg_energy_order.script import main as msg_energy_order
from app.service.msg_zhilian_order.script import main as msg_zhilian_order


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

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule_loop()

