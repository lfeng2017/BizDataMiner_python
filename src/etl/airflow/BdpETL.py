# -*- coding: utf-8 -*-
'''
外部依赖数据源ETL任务，数据源如下：
一、BDP Hive表export
1 yc_bit.ods_service_order
2 yc_bit.fo_service_order
3 yc_bit.fo_service_order_ext
4 yc_bit.ods_service_order_charge
5 yc_ods.fdcs_device

二、0公里风控，风控组每日scp至跳板机: /tmp/zero_order_new
zero_order_DT.txt

'''

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import HivePartitionSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

# 文件是在airflow/dags下面, 必须手动指定待执行文件的绝对路径
BASE_PATH = Variable.get("BizDataMinerPath").strip()
TSV_PATH = Variable.get("TsvTmpFilePath").strip()
RC_TMP_PATH = Variable.get("RiskControlOrderTmpPath")
IS_GET_RC = Variable.get("isGetRiskControl")

ENV = Variable.get("ENV").strip()
DB_URI_YONGCHE = Variable.get("DB_URI_YONGCHE").strip()

DEVICE_FNAME = Variable.get("DEVICE_FNAME").strip()
DEVICE_TITLE = Variable.get("DEVICE_TITLE").strip()

args = {
    'owner': 'lujin',
    'start_date': datetime(2017, 02, 27, 10, 0, 0),
    'email': ['lujin@yongche.com'],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(
    dag_id='BdpETL',
    default_args=args,
    schedule_interval='0 10 * * *'
    # schedule_interval=timedelta(days=1)
)

bdpETL_OK = DummyOperator(task_id='bdpETL_OK', dag=dag)

EnvShell = "source /home/lujin/.bashrc && PYTHONIOENCODING=utf-8 && cd {dir}".format(dir=BASE_PATH)

'''
动态构造operator，暂时弃用
tables = [
    "fo_service_order",
    "fo_service_order_ext",
    "ods_service_order",
    "ods_service_order_charge"
]

templated_command = """
{{ params.baseShell }} && cd etl && ENV={{ params.env }} \
python ETLJobManager.py --date={{ ds_nodash }} --job={{params.jobName}} --tsv={{params.tsvPath}}
"""

#订单相关的整表导入hive
for tbl in tables:
    task = BashOperator(
        task_id='ETL_' + tbl + "_hive",
        bash_command=templated_command,
        params={'baseShell': EnvShell, "jobName": tbl, "tsvPath": TSV_PATH, "env": ENV},
        dag=dag)
    task >> bdpETL_OK
'''

# ------------------------------------------------------------------------------
# LEVEL=1-1，tsv导入Hive
# ------------------------------------------------------------------------------

loadHiveOnly_OK = DummyOperator(task_id='loadHiveOnly_OK', dag=dag)
loadHiveOnly_OK >> bdpETL_OK

templated_command = """
{{ params.baseShell }} && cd etl && ENV={{ params.env }} \
python ETLJobManager.py --date={{ ds_nodash }} --job={{params.jobName}} --tsv={{params.tsvPath}}
"""

# step 1
ETL_foOrder = BashOperator(
    task_id='ETL_foOrder',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "jobName": "fo_service_order", "tsvPath": TSV_PATH, "env": ENV},
    retries=2,
    retry_delay=timedelta(hours=1),
    dag=dag)

ETL_foOrder >> loadHiveOnly_OK

# step 2
ETL_foOrderExt = BashOperator(
    task_id='ETL_foOrderExt',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "jobName": "fo_service_order_ext", "tsvPath": TSV_PATH, "env": ENV},
    retries=2,
    retry_delay=timedelta(hours=1),
    dag=dag)

foOrderExtOK = HivePartitionSensor(
    table="yc_bit.fo_service_order_ext",
    partition="dt={{ ds_nodash }}",
    task_id='foOrderExtOK',
    poke_interval=60 * 10,
    timeout=60 * 60 * 2,
    dag=dag)

ETL_foOrderExt >> foOrderExtOK

# step 3
ETL_OdsOrder = BashOperator(
    task_id='ETL_OdsOrder',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "jobName": "ods_service_order", "tsvPath": TSV_PATH, "env": ENV},
    retries=2,
    retry_delay=timedelta(hours=1),
    dag=dag)

odsOrderOK = HivePartitionSensor(
    table="yc_bit.ods_service_order",
    partition="dt={{ ds_nodash }}",
    task_id='odsOrderOK',
    poke_interval=60 * 10,
    timeout=60 * 60 * 2,
    dag=dag)

ETL_OdsOrder >> odsOrderOK

# step 4
ETL_OdsOrderCharge = BashOperator(
    task_id='ETL_OdsOrderCharge',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "jobName": "ods_service_order_charge", "tsvPath": TSV_PATH, "env": ENV},
    retries=2,
    retry_delay=timedelta(hours=1),
    dag=dag)

ETL_OdsOrderCharge >> loadHiveOnly_OK

# ------------------------------------------------------------------------------
# LEVEL=1-2，device.tsv导入mysql
# ------------------------------------------------------------------------------

import os
import pandas as pd
import logging
import time
from sqlalchemy import create_engine


# 设备表单独导入mysql
def load_device(**kwargs):
    date = kwargs.get('ds_nodash')

    # 构造输入文件绝对路径
    file = os.path.join(TSV_PATH, "{name}_{dt}.tsv".format(name=DEVICE_FNAME, dt=date))

    # 构造pandas表头
    title = DEVICE_TITLE.split(",")
    df = pd.read_csv(file, delimiter='\t', names=title, encoding="utf-8")

    # 增加日期列
    df['dt'] = int(date)
    logging.info("load from path={f} size={s} title={t}".format(f=file, s=df.shape[0], t=title))

    # 写入数据库
    t = time.time()
    engine = create_engine(DB_URI_YONGCHE, echo=False)
    table = "ods_device"
    logging.info("begin to write table={t} URI={u} ...".format(t=table, u=DB_URI_YONGCHE))
    df.to_sql(name=table, con=engine, if_exists='replace', index=False, chunksize=2000)
    logging.info("write db is done, elapsed={t}".format(t=(time.time() - t)))

    logging.info("all is done")


ETL_device2mysql = PythonOperator(
    task_id='ETL_load_device_mysql',
    provide_context=True,
    python_callable=load_device,
    dag=dag)

device_OK = DummyOperator(task_id='device_OK', dag=dag)

ETL_device2mysql >> device_OK

# ------------------------------------------------------------------------------
# LEVEL=2-1，ods_service_order导入完成后，进行订单基本属性统计
# ------------------------------------------------------------------------------

# step 2: 执行统计脚本
templated_command = """
{{ params.baseShell }} && cd OrderStat && ENV={{ params.env }} \
python StatByCity.py --start={{ ds_nodash }} --end={{ ds_nodash }}
"""
baseStat = BashOperator(
    task_id='orderBaseStat',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "env": ENV},
    dag=dag)

odsOrderOK >> baseStat >> bdpETL_OK

# ------------------------------------------------------------------------------
# LEVEL=2-2，ods_service_order, fo_service_order_ext 导入完成后，进行疑义订单统计
# ------------------------------------------------------------------------------

# step 2: 执行统计脚本
templated_command = """
{{ params.baseShell }} && cd SuspOrderMiner && ENV={{ params.env }} \
python SuspOrderStat.py --date={{ ds_nodash }} --verbose=True --mail=True
"""
dumpSuspDetails = BashOperator(
    task_id='dumpSuspDetails',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "env": ENV},
    dag=dag)

odsOrderOK >> dumpSuspDetails
foOrderExtOK >> dumpSuspDetails
device_OK >> dumpSuspDetails
dumpSuspDetails >> bdpETL_OK


# ------------------------------------------------------------------------------
# LEVEL=2，ods_service_order, fo_service_order_ext 导入完成后，进行疑义订单统计
# ------------------------------------------------------------------------------

def isGetRcData(ds, **kwargs):
    print("------------- exec dt = {} and isGetRc = {}".format(kwargs['execution_date'], IS_GET_RC))
    if IS_GET_RC == "True":
        return "getRiskCtrlOrders"
    else:
        return "zeroDistStatNoRc"


getRcDataCondition = BranchPythonOperator(
    task_id='isGetRcData',
    provide_context=True,
    python_callable=isGetRcData,
    dag=dag)

# 分支1： 无风控数据版本
templated_command = """
{{ params.baseShell }} && cd ZeroDistanceMiner && ENV={{ params.env }} \
python ZeroDistExportor.py --date={{ ds_nodash }} --mail=True
"""
zeroDistStatNoRc = BashOperator(
    trigger_rule='all_success',
    task_id='zeroDistStatNoRc',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "env": ENV},
    dag=dag)

foOrderExtOK >> zeroDistStatNoRc
odsOrderOK >> zeroDistStatNoRc
getRcDataCondition >> zeroDistStatNoRc
zeroDistStatNoRc >> bdpETL_OK

# 分支2：风控数据版本
# 2-1 从跳板机获取0公里风控订单明细数据tsv
templated_command = """
scp lujin@login1.ops.bj2.yongche.com:{{ params.tmp_path }}/zero_order_{{ tomorrow_ds_nodash }}.txt  /home/lujin/tmp/
"""
etlRc1 = BashOperator(
    task_id='getRiskCtrlOrders',
    bash_command=templated_command,
    params={'tmp_path': RC_TMP_PATH},
    retries=3,
    retry_delay=timedelta(hours=3),
    dag=dag)

# 2-2 风控明细数据更新入库
templated_command = """
{{ params.baseShell }} && cd etl && ENV={{ params.env }} \
python updateRcOrders.py --date={{ tomorrow_ds_nodash }}
"""
etlRc2 = BashOperator(
    task_id='updateRcOrders',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "env": ENV},
    dag=dag)

templated_command = """
{{ params.baseShell }} && cd ZeroDistanceMiner && ENV={{ params.env }} \
python ZeroDistExportor.py --date={{ ds_nodash }} --mail=True --use_rc=True --mail_both=True
"""
zeroDistStatWithRc = BashOperator(
    trigger_rule='all_success',
    task_id='zeroDistStatWithRc',
    bash_command=templated_command,
    params={'baseShell': EnvShell, "env": ENV},
    dag=dag)

foOrderExtOK >> zeroDistStatWithRc
odsOrderOK >> zeroDistStatWithRc
getRcDataCondition >> etlRc1 >> etlRc2 >> zeroDistStatWithRc >> bdpETL_OK
