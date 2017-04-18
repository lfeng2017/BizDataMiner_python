# -*- coding: utf-8 -*-

'''
batch mode:
ENV=pro python ETLJobManager.py --batch=True --start_date=20170303 --end_date=20170305 --tsv=/home/lujin/tmp --job=fo_service_order

single mode:
ENV=pro python ETLJobManager.py --date=20170303 --tsv=/home/lujin/tmp --job=fo_service_order

'''

import logging
import os
import subprocess
import sys
import time
import traceback

import arrow
import click
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

import BdpHiveDesc as Hive

parDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parDir)
import src.common.logger as logger
import config as cfg

NAME = (os.path.splitext(os.path.basename(__file__))[0])

log = logger.init(NAME)

'''
NAME = (os.path.splitext(os.path.basename(__file__))[0])

logbook.StreamHandler(sys.stdout).push_application()
log = logbook.Logger(NAME)
log.level = logbook.INFO
'''

JobMeta = {
    "fo_service_order": {
        "db": "yc_bit",
        "table": "fo_service_order",
        "title": Hive.FoOrderDesc,
        "dropDuplicates": True
    },
    "fo_service_order_ext": {
        "db": "yc_bit",
        "table": "fo_service_order_ext",
        "title": Hive.FoOrderExtDesc,
        "dropDuplicates": True
    },
    "ods_service_order": {
        "db": "yc_bit",
        "table": "ods_service_order",
        "title": Hive.OdsOrderDesc,
        "dropDuplicates": False
    },
    "ods_service_order_charge": {
        "db": "yc_bit",
        "table": "ods_service_order_charge",
        "title": Hive.OrderChargeDesc,
        "dropDuplicates": False
    }
}


def getSchema(hive_desc_str):
    '''
    从hive表结构提取meta信息（dataframe的columns）
    '''
    schema = []
    for line in hive_desc_str.split('\n'):
        valid_segs = []
        for tag in line.split(' '):
            if tag != '':
                valid_segs.append(tag)
        if len(valid_segs) >= 2:
            colType = valid_segs[1]
            if colType.find("int") > 0:
                colType = "int"
            elif colType.find("timestamp") > 0:
                colType = "int"
            elif colType.find("string") > 0:
                colType = "str"
            elif colType.find("double") > 0:
                colType = "float"
            elif colType.find("float") > 0:
                colType = "float"
            elif colType.find("decimal") > 0:
                colType = "float"
            else:
                colType = "str"
            schema.append((valid_segs[0], colType))
    return schema


def tuple2Row(segs, schema):
    ' 根据hive表的meta信息, 直接构造Row对象 '
    buff = dict()
    for i, meta in enumerate(schema):
        value = segs[i]
        try:
            if meta[1] == "int":
                value = int(value)
            elif meta[1] == "float":
                value = float(value)
        except:
            value = None
        buff[meta[0]] = value
    return Row(**buff)


def tsv2HiveParquet(tsvPath, date, meta, debug=False):
    ' 将ods_service_order_charge导入本地hive '

    db = meta['db']
    table = meta['table']
    dropDuplicates = meta['dropDuplicates']
    schema = getSchema(meta['title'])
    # schema = schema[:-1]  # 最后一个dt是partition, 需要删除

    # 判断文件是否存在
    tsv_file_path = os.path.join(tsvPath, "{tbl}_{dt}.tsv".format(tbl=table, dt=date))
    if not os.path.exists(tsv_file_path):
        log.error("sourcr tsv file is not exist! path=" + tsv_file_path)
        exit(-1)

    try:
        spark = None
        if not debug:
            spark = SparkSession.builder \
                .master(cfg.setting.SPARK_MASTER) \
                .appName("ETL_tsv2parquet") \
                .enableHiveSupport() \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.default.parallelism", "10") \
                .config("spark.sql.shuffle.partitions", "100") \
                .getOrCreate()
            spark.sparkContext.setLogLevel('WARN')
            sc = spark.sparkContext
            logger = sc._jvm.org.apache.log4j
            logger.LogManager.getLogger("org").setLevel(logger.Level.OFF)
            logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)

            df = sc.textFile("file:///" + tsv_file_path) \
                .map(lambda line: line.split("\t")) \
                .map(lambda segs: tuple2Row(segs, schema)) \
                .toDF()

            if dropDuplicates:
                df = df.coalesce(20) \
                    .orderBy("service_order_id", desc("update_time")) \
                    .dropDuplicates(["service_order_id", "update_time"])
            else:
                df = df.coalesce(20)

            df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            df.createOrReplaceTempView("tempDF")

            # df.printSchema()

            # 转换为parquet, 最后1列是dt
            cols = ','.join([seg[0] for seg in schema if seg[0] != "dt"])
            sql = "insert overwrite table {db}.{tbl} partition (dt={dt}) select {cols} from tempDF where dt like '{dt}%'" \
                .format(db=db, tbl=table, cols=cols, dt=date)
            log.info("tempView写入parquet: " + sql)
            if not debug:
                spark.sql(sql)

            # 删除1个月之前的记录
            _30daysAgo = arrow.get(date, "YYYYMMDD").replace(days=-30).format("YYYYMMDD")
            sql = "ALTER TABLE {db}.{tbl} DROP IF EXISTS PARTITION(dt={dt})".format(db=db, tbl=table, dt=_30daysAgo)
            log.info("删除30天之前的partition: " + sql)
            if not debug:
                spark.sql(sql)
            # 删除hdfs文件
            shell = "hadoop fs -rm /user/hive/warehouse/{db}.db/{tbl}/dt={dt}".format(db=db, tbl=table, dt=_30daysAgo)
            log.info("删除30天之前的partition对应的HDFS: " + shell)
            if not debug:
                subprocess.call(shell, shell=True)

    except Exception, e:
        traceback.print_exc()
        exit(-1)

    finally:
        if not debug:
            spark.stop()


yesterday = arrow.now().replace(days=-1)
_7daysAgo = arrow.now().replace(days=-7)


@click.command()
@click.option('--job', type=str, required=True, help='ETL任务名')
@click.option('--tsv', type=str, required=True, help='tsv源文件所在目录')
@click.option('--date', type=str, default=yesterday.format("YYYYMMDD"), help='开始任务的日期（默认为昨天）')
@click.option('--verbose', type=bool, default=False, help='输出明细')
@click.option('--debug', type=bool, default=False, help='调试开关, 打开后不执行具体的语句')
@click.option('--batch', type=bool, default=False, help='批量模式')
@click.option('--start_date', type=str, default=_7daysAgo.format("YYYYMMDD"), help='批量的开始日期')
@click.option('--end_date', type=str, default=yesterday.format("YYYYMMDD"), help='批量的结束日期')
def main(job, tsv, date, verbose, debug, batch, start_date, end_date):
    ' 将ods_service_order_charge导入本地hive '

    if verbose:
        log.setLevel(logging.DEBUG)
        #log.level = logbook.DEBUG

    if job not in JobMeta.keys():
        log.error("[ERROR] input job=" + job + " not in meta, please check your job name first!")
        exit(-1)

    meta = JobMeta[job]

    if not batch:
        log.info("job=" + job + " tsv=" + tsv + " date=" + date + " debug=" + str(debug))
        t = time.time()
        # 执行具体的导入任务
        tsv2HiveParquet(tsv, date, meta, debug=debug)
        log.info("etl for" + job + " is complete! elasped: " + str(time.time() - t))
    else:
        log.info("job=" + job + " tsv=" + tsv + " start=" + start_date + " end=" + end_date + " debug=" + str(debug))
        t = time.time()
        for dt in pd.date_range(start_date, end_date, freq="D"):
            dt = dt.strftime("%Y%m%d")
            tsv2HiveParquet(tsv, dt, meta, debug=debug)
            log.info("etl for " + job + "dt=" + dt + " is complete!")

    log.info("all is done! elasped: " + str(time.time() - t))

    exit(0)


if __name__ == '__main__':
    main()
