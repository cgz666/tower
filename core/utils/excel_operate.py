import win32com.client as win32
import win32com.client
from PIL import ImageGrab
import time
import pythoncom
from xlsx2csv import Xlsx2csv
def capture_excel_range(filename, sheet_name,picture_path):
    # 初始化Excel应用程序
    pythoncom.CoInitialize()
    excel = win32com.client.Dispatch('Excel.Application')
    # 打开Excel文件
    workbook = excel.Workbooks.Open(filename)
    # 获取工作表
    sheet = workbook.Sheets(sheet_name)

    try:
        img_name = '截图'
        screen_area = sheet.UsedRange  # 有内容的区域
        time.sleep(5)
        screen_area.CopyPicture()  # 复制图片区域
        time.sleep(5)
        sheet.Paste()  # 粘贴
        time.sleep(5)
        excel.Selection.ShapeRange.Name = img_name  # 将刚刚选择的Shape重命名，避免与已有图片混淆
        sheet.Shapes(img_name).Copy()  # 选择图片
        time.sleep(5)
        img = ImageGrab.grabclipboard()# 获取剪贴板的图片数据
        img.save(picture_path)

    finally:
        # 关闭Excel文件
        try:
            workbook.Close(False)
            # 退出Excel应用程序
            excel.Quit()
            pythoncom.CoUninitialize()
        except:pass

def xlsxtocsv(file_path):
    file_path_csv = file_path.replace(".xlsx", ".csv")
    Xlsx2csv(file_path, outputencoding="utf-8").convert(file_path_csv)
    return file_path_csv








