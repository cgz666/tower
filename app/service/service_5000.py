# coding=utf-8
import os
import sys
import shutil
import os, glob
import pandas as pd
import datetime
from flask import Flask, request,render_template, send_file,redirect,session,jsonify,abort,Blueprint
from flask_cors import CORS
from flask_session import Session
from redis import StrictRedis
from scheduler.other_task import gen_fsu_static
from sqlalchemy import text
import tempfile
import json
import subprocess
from message.ID_serch.operate import get_table as station_shouzi_get_table
from core.sql import sql_orm
from core.config import settings
app = Flask(__name__, template_folder=settings.resolve_path(f"service/templates"))
CORS(app, resources=r'/*')
app.secret_key = 'xgxtt'
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = datetime.timedelta(seconds=1)
app.config['SESSION_TYPE'] = 'redis'
app.static_folder = 'templates'
# 创建Redis客户端并设置密码
redis_host = 'localhost'
redis_port = 6379
redis_password = 123456  # 按实际配置设置密码
redis_client = StrictRedis(host=redis_host, port=redis_port, password=redis_password)
app.config['SESSION_REDIS'] = redis_client
# 初始化会话扩展
Session(app)
TEMP_PATH_ONE_DAY = settings.resolve_path("spider/down/temp_folder_one_day")
def zip_file_and_send(folder,file_list):
    zip_folder=os.path.join(folder,'zip')
    for file in os.listdir(zip_folder):
        file = os.path.join(zip_folder, file)
        os.remove(file)
    for file in file_list:
        path=os.path.join(folder,file)
        zip_path=os.path.join(zip_folder,file)
        shutil.copy(path,zip_path)
    zip_path=f"{TEMP_PATH_ONE_DAY}{str(datetime.datetime.now().timestamp())}"
    shutil.make_archive(zip_path, 'zip', zip_folder)
    return send_file(zip_path + '.zip', as_attachment=True, cache_timeout=0)

# @app.route('/index', methods=['get'])
# def index():
#     with sql_orm().session_scope() as temp:
#         session,Base=temp
#         pojo=Base.classes.update_downhour_log
#         listt={}
#         try:
#             res=session.query(pojo).all()
#             for item in res:
#                 listt[item.type]=item.time
#             print(1)
#         except Exception as e:print(e)
#     return render_template('index.html',**listt)
@app.route('/fsu_hafhour', methods=['get'])
def fsu_hafhour():
    folder=settings.resolve_path('spider/down/fsu_hafhour')
    path=f'{folder}/fsu每日离线统计.xlsx'
    zip_filename = settings.resolve_path(f"spider/down/temp_folder_one_day/{datetime.datetime.now().timestamp()}_FSU信息")
    try:
        gen_fsu_static(path)
        shutil.make_archive(zip_filename, 'zip', folder)
        return send_file(zip_filename + '.zip', as_attachment=True, cache_timeout=0)
    except Exception as e:
        print('FSU信息出错' + str(e))


@app.route('/get_OA', methods=['post'])
def get_OA():
    try:
        Cookie = str(request.form.get('Cookie'))
        EIP=str(request.form.get('EIP'))
        if EIP=='':
            pj = {'Cookie': Cookie,'ID':1}
        else:
            pj = {'Cookie': EIP,'ID':2}
        msg = 'success'
        with sql_orm().session_scope() as re:
            session, Base = re
            pojo = Base.classes.oa
            a = pojo(**pj)
            session.merge(a)
        print(pj)
    except Exception as e:
        msg = str(e)
        print(str(e))
    return msg

@app.route('/down_wendu_guogao', methods=['get'])
def down_wendu_guogao():
    try:
        begin=request.args.get('begin')
        end = request.args.get('end')
        folder = settings.resolve_path("spider/down/temp_folder_one_day")
        file_name=f'告警{datetime.datetime.now().timestamp()}.xlsx'
        df=sql_orm().excute_sql(f'select * from 自助取数.hbase where (告警名称 in ("温度过高","温度过高（预告警）"))and(告警发生日期 between "{begin}" and "{end}")',return_df=True)
        df.to_excel(f'{folder}/{file_name}',index=False)
        return send_file(f'{folder}/{file_name}', as_attachment=True, cache_timeout=0)
    except Exception as e:
        print(str(e))
        return '输入错误'

@app.route('/down_alarm_history', methods=['get'])  
def down_alarm_history():
    try:
        begin = request.args.get('begin')
        end = request.args.get('end')
        alarm = request.args.get('alarm')
        folder = settings.resolve_path("spider/down/temp_folder_one_day")
        file_name = f'告警{datetime.datetime.now().timestamp()}.xlsx'
        
        if alarm:
            # 定义各类告警对应的告警名称列表
            alarm_mapping = {
                '温度过高': ['温度过高', '温度过高（预告警）'],
                '交流输入停电告警': ['交流输入停电告警'],
                '门类': [
                    '智能门禁通信状态告警', '设备故障告警(智能门禁)', '长时间门开告警', 
                    '长时间门开告警(智能门禁)', '长时间门开告警(非智能门禁)', '门磁开关状态', 
                    '门磁开关状态(智能门禁)', '门磁开关状态(非智能门禁)', '门锁开关状态(智能门禁)', 
                    '门锁开关状态(非智能门禁)', '非法进入告警(智能门禁)', '非法进入告警(非智能门禁)'
                ],
                '离线类': ['FSU离线', '一级低压脱离告警', '二级低压脱离告警']
            }
            
            if alarm in alarm_mapping:
                alarm_list = alarm_mapping[alarm]
                if len(alarm_list) == 1:
                    alarm_condition = f"告警名称 = '{alarm_list[0]}'"
                else:
                    alarm_condition = f"告警名称 IN {tuple(alarm_list)}"

                df = sql_orm().excute_sql(
                    f"select * from 自助取数.hbase where ({alarm_condition}) and (告警发生日期 between '{begin}' and '{end}')",
                    return_df=True
                )
            else:
                return '输入的告警名称不在支持范围内，请输入[温度过高、交流输入停电告警、门类、离线类]'
        else:
            return '请输入告警名称'

        df.to_excel(f'{folder}/{file_name}', index=False)
        return send_file(f'{folder}/{file_name}', as_attachment=True, cache_timeout=0)
    except Exception as e:
        print(str(e))
        return '输入错误'


@app.route('/battery_life', methods=['get'])
def battery_life():
    page = request.args.get('page', 1, type=int)
    per_page = 10  # 每页显示的记录数
    search_keyword = request.args.get('search', '', type=str).strip()  # 获取查询关键词

    # 从数据库查询完整数据
    with sql_orm(database='battery_life').session_scope() as (sql, Base):
        sql_str = 'select * from result'
        result = sql.execute(text(sql_str))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        df = df.reset_index(drop=True)
        df.index += 1
        df = df.reset_index()
        # 提取上个月基站负载电流
        last_month = datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)
        sql_str = f"""select 运维监控站址ID as 站址运维ID,月度平均值 from 自助取数.基站负载电流 
                    where 年={last_month.year} and 月份={last_month.month}"""
        result = sql.execute(text(sql_str))
        df_dc = pd.DataFrame(result.fetchall(), columns=result.keys())
        df_dc = df_dc.drop_duplicates(subset=['站址运维ID'], keep='first')
        # 列名映射
        df = df.rename(columns={
            "city": "市",
            "area": "区",
            "site_name": "站址名称",
            "site_code": "站址编码",
            "id": "站址运维ID",
            "level": "站点等级",
            "belong": "站点共享情况",
            "fsu_status": "FSU状态",
            "voltage": "当时直流电压值",
            "dc": "负载电流",
            "voltage_get_time": "直流电压值获取时间",
            "offline_time": "运维退服（或运营商）时间",
            "outage_time": "设备（或停电）告警/核容开始时间",
            "battery_life": "基站电池续航时长（小时）",
            "caculate_type": "数据来源",
            "battery_life_final": "最终续航（小时）"
        })
        # 将dc为null或""的行用df_dc中的对应值替换，否则保留原值
        df = df.merge(df_dc, on='站址运维ID', how='left')
        df['负载电流'] = df['负载电流'].where(
            df['负载电流'].notna() & (df['负载电流'] != ''),
            df['月度平均值']
        )
        df = df.drop(columns=['月度平均值'])

        # 调整列顺序
        new_order = [
            "市", "区", "站址名称", "站址编码", "站址运维ID", "站点等级", "站点共享情况", "FSU状态",
            "当时直流电压值", "负载电流", "直流电压值获取时间", "运维退服（或运营商）时间",
            "设备（或停电）告警/核容开始时间", "基站电池续航时长（小时）", "最终续航（小时）", "数据来源"
        ]
        df = df.reindex(columns=new_order)

        # 模糊查询处理
        if search_keyword:
            # 构建查询条件：所有字符串类型的列都参与模糊查询
            string_columns = df.select_dtypes(include=['object']).columns.tolist()
            # 数值类型列也支持部分匹配（如站址编码、电流值等）
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

            # 初始化查询掩码
            mask = pd.Series(False, index=df.index)

            # 字符串列模糊匹配（不区分大小写）
            for col in string_columns:
                mask |= df[col].astype(str).str.contains(search_keyword, case=False, na=False)

            # 数值列模糊匹配（转换为字符串后匹配）
            for col in numeric_columns:
                mask |= df[col].astype(str).str.contains(search_keyword, case=False, na=False)

            # 应用查询条件
            df = df[mask].reset_index(drop=True)

        # 1. 转换续航时长为数值类型（处理可能的非数值数据）
        df['基站电池续航时长（小时）'] = pd.to_numeric(
            df['基站电池续航时长（小时）'],
            errors='coerce'  # 无效值转为NaN
        )

        # 2. 计算平均续航时长（排除NaN值）
        total_duration = df['基站电池续航时长（小时）'].sum(skipna=True)
        valid_count = df['基站电池续航时长（小时）'].count()  # 非NaN值的数量
        average_duration = round(total_duration / valid_count, 1) if valid_count > 0 else 0

        # 3. 计算异常设备数（续航时长 < 1.2小时，排除NaN）
        abnormal_count = df[df['基站电池续航时长（小时）'] < 1.2]['基站电池续航时长（小时）'].count()

        # 分页处理
        total = len(df)
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        df = df.astype(str)
        data = df.iloc[start_index:end_index].to_dict('records')
        session['battery_life_data'] = df.to_dict('records')
    if len(data)>0:
        # 传递统计值和查询关键词到模板
        return render_template(
            'battery_life.html',
            data=data,
            page=page,
            per_page=per_page,
            total=total,
            total_pages=(total + per_page - 1) // per_page,
            average_duration=average_duration,  # 平均续航时长
            abnormal_count=abnormal_count,  # 异常设备数
            search_keyword=search_keyword  # 回显查询关键词
        )
    else:return "无数据"

@app.route('/battery_life_city')
def battery_life_city():
    with sql_orm(database='tower').session_scope() as (sql, Base):
        res=sql.execute(text("select city as 市,count(*) as 站址数 from station where fsu_status='交维' group by city")).fetchall()
        station=pd.DataFrame([dict(row) for row in res])
    df=pd.DataFrame(session['battery_life_data'])
    df=df.loc[df['FSU状态']=='交维']
    df['基站电池续航时长（小时）'] = pd.to_numeric(df['基站电池续航时长（小时）'], errors='coerce')
    df['基站电池续航时长（小时）'].fillna(0, inplace=True)
    res= df.groupby('市')['基站电池续航时长（小时）'].agg([
        ('基站电池续航时长<1小时', lambda x: (x < 1).sum()),  # 计算续航时长小于1小时的数量
        ('1小时≤基站电池续航时长<2小时', lambda x: ((x >= 1) & (x < 2)).sum()),
        ('2小时≤基站电池续航时长且<3小时', lambda x: ((x >= 2) & (x < 3)).sum()),
        ('3小时≤基站电池续航时长且<6小时', lambda x: ((x >= 3) & (x < 6)).sum()),
        ('基站电池续航时长≤6小时', lambda x: (x >= 6).sum())  # 计算续航时长大于等于6小时的数量
    ]).reset_index()
    res = pd.merge(station,res,  on='市', how='left')
    res = res.reindex(columns=['市','站址数','基站电池续航时长<1小时','1小时≤基站电池续航时长<2小时','2小时≤基站电池续航时长<3小时','3小时≤基站电池续航时长<6小时','基站电池续航时长≤6小时'])

    cities_order = ['南宁', '桂林', '百色', '柳州', '玉林', '河池', '贵港', '梧州', '北海', '崇左', '钦州', '来宾', '贺州', '防城港']
    res = res.sort_values('市', inplace=False)
    res['市'] = pd.Categorical(res['市'], categories=cities_order, ordered=True)
    res = res.sort_values('市')
    #
    total_sum = res.sum()
    total_df = pd.DataFrame([total_sum], columns=res.columns)
    total_df.iloc[0, 0] = '全区'
    res = pd.concat([total_df, res], ignore_index=True)
    res=res.fillna(0)
    for col in res.columns:
        if col != '市':
            res[col] = res[col].astype(int)
    data=res.to_dict(orient='records')
    return render_template('battery_life_city.html', data=data)
@app.route('/battery_life_download_excel')
def battery_life_download_excel():
    # 假设data是你想要下载的数据
    data = session['battery_life_data']
    df = pd.DataFrame(data)
    temp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    temp_filename = temp.name

    # 将DataFrame写入Excel文件
    df.to_excel(temp_filename, index=False)

    # 关闭临时文件
    temp.close()

    # 使用Flask的send_file函数发送文件
    return send_file(temp_filename, as_attachment=True, attachment_filename='蓄电池续航能力.xlsx')
@app.route('/battery_shangdan', methods=['get'])
def show_table():
    id = request.args.get('id')
    df=sql_orm(database="battery_life").excute_sql(f"select * from battery_shangdan where id='{id}'",return_df=True)
    data = df.to_dict(orient='records')  # 转为字典列表，每行一个 dict
    return render_template('battery_shangdan.html', data=data)
@app.route('/get_4a_cookie', methods=['get'])
def get_4a_cookie():
    try:
        res=sql_orm().get_cookie('foura')
        return json.dumps(res)
    except Exception as e:
        print(str(e))

@app.route('/station_shouzi_index', methods=['get'])
def station_shouzi_index():
    # 文件列表
    files = ['直流负载电流.xlsx', '浮充电压设定值.xlsx','均充电压设定值.xlsx','二级低压脱离设定值.xlsx','一级低压脱离设定值.xlsx']

    # 用于存储文件生成时间的字典
    file_times = {}
    # 遍历文件列表，获取每个文件的生成时间
    for file in files:
        # 构建文件路径
        file_path = settings.resolve_path('message/ID_serch/xls', file)

        # 检查文件是否存在
        if os.path.exists(file_path):
            # 获取文件状态信息
            stat = os.stat(file_path)

            # 获取文件的创建时间
            created_time = datetime.datetime.fromtimestamp(stat.st_mtime)

            # 将文件名和创建时间添加到字典中
            file_times[file] = created_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # 如果文件不存在，可以设置一个默认值或者错误信息
            file_times[file] = '文件不存在'

    # 将文件生成时间信息传递给模板
    return render_template('station_shouzi.html', file_times=file_times)
@app.route('/station_shouzi', methods=['post'])
def station_shouzi():
    if 'file' not in request.files:
        return '未上传文件'
    file = request.files['file']
    if file:
        path=settings.resolve_path('message/ID_serch/xls/查询用站址运维ID.xlsx')
        file.save(path)
        path=station_shouzi_get_table(path)
        if path!='失败':
            return send_file(path, as_attachment=True)
        else:
            return '失败'

@app.route('/station_shouzi_down', methods=['get'])
def station_shouzi_down():
    folder = settings.resolve_path('message/ID_serch/xls')
    file_list = ['直流负载电流.xlsx', '浮充电压设定值.xlsx','均充电压设定值.xlsx','二级低压脱离设定值.xlsx','一级低压脱离设定值.xlsx']
    try:
        return zip_file_and_send(folder, file_list)
    except Exception as e:
        print(f'{folder}出错' + str(e))


@app.route('/save_oa_auth', methods=['POST'])
def save_auth():
    global auth_cache
    auth_cache = request.json.get("authorization")
    print("收到Authorization:", auth_cache)
    return jsonify({"status": "ok"}), 200
@app.route('/get_oa_auth', methods=['GET'])
def get_auth():
    return jsonify(auth_cache), 200

@app.route('/save_oa_token', methods=['POST'])
def save_token():
    global token_cache
    token_cache = request.json.get("token")
    print("收到Token:", token_cache)
    return jsonify({"status": "ok"}), 200
@app.route('/get_oa_token', methods=['GET'])
def get_token():
    return jsonify(token_cache), 200

@app.route('/save_oa_sysToken', methods=['POST'])
def save_sysToken():
    global sysToken_cache
    sysToken_cache = request.json.get("sysToken")
    print("收到SysToken:", sysToken_cache)
    return jsonify({"status": "ok"}), 200
@app.route('/get_oa_sysToken', methods=['GET'])
def get_sysToken():
    return jsonify(sysToken_cache), 200

@app.route('/save_oa_XCsrfToken', methods=['POST'])
def save_XCsrfToken():
    global csrftoken_cache, cookie_cache
    data = request.get_json(silent=True) or {}
    csrftoken_cache = data.get("csrfToken")  # 前端字段名
    cookie_cache = data.get("cookie")
    print("收到X-Csrf-Token:", csrftoken_cache)
    print("收到Cookie:", cookie_cache)
    return jsonify({"status": "ok"}), 200

@app.route('/get_oa_XCsrfToken', methods=['GET'])
def get_XCsrfToken():
    return jsonify({"csrfToken": csrftoken_cache, "cookie": cookie_cache}), 200


def newest_file(hour: str):
    """返回当天对应 hour 的最新文件绝对路径，找不到返回 None"""
    OUTPUT_DIR = settings.resolve_path(r'message/performance_sheet/output')
    today = datetime.datetime.now().strftime('%Y%m%d')
    pattern = os.path.join(OUTPUT_DIR, f'{today}_{hour}.xlsx')
    files = glob.glob(pattern)
    return files[0] if files else None

def file_mtime(hour: str):
    """返回文件修改时间字符串（前端直接显示），无文件返回 '-'"""
    f = newest_file(hour)
    if not f:
        return '-'
    t = datetime.datetime.fromtimestamp(os.path.getmtime(f))
    return t.strftime('%m-%d %H:%M')

@app.route('/performance_sheet')
def index():
    """首次进入页面"""
    hours = ['08', '14', '17']
    # 初始时间
    times = {h: file_mtime(h) for h in hours}
    return render_template('performance_sheet.html', hours=hours, times=times)

@app.route('/performance_sheet', methods=['POST'])
def update_and_info():
    """AJAX 统一入口：启动脚本 + 返回最新时间"""
    SCRIPT_PATH = settings.resolve_path('message/performance_sheet/script.py')
    PYTHON_EXE = settings.resolve_path(r'F:\newtowerV2\venv\Scripts\python.exe')
    # 1. 启动脚本（不阻塞）
    subprocess.Popen([PYTHON_EXE, SCRIPT_PATH], shell=True)
    # 2. 立即把最新时间返回给前端
    hours = ['08', '14', '17']
    times = {h: file_mtime(h) for h in hours}
    return jsonify({'status': 'success', 'times': times})

@app.route('/download/<hour>')
def download(hour):
    """下载对应小时文件"""
    if hour not in {'08', '14', '17'}:
        abort(404)
    real = newest_file(hour)
    if not real or not os.path.isfile(real):
        abort(404)
    return send_file(real, as_attachment=True)
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
@app.route('/tt/get_battery')
def get_battery():
    try:
        # 获取时间
        with sql_orm(database='battery_life').session_scope() as temp:
            sql, Base = temp
            sql_str = text(f"select * from result")
            res = sql.execute(sql_str)
            colnames = [column[0] for column in res.cursor.description]
            rows = res.fetchall()
        data_list = [dict(zip(colnames, row)) for row in rows]
        return json.dumps({'data': data_list}, ensure_ascii=False)
    except Exception as e:
        print(e)

#
# if __name__ == '__main__':
#     app.run(host=IP_SERVICE,debug=False,port=5000)
