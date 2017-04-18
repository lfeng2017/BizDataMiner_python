# -*- coding: utf-8 -*-
import os.path as op
import sys
import time
import traceback
import random
from collections import defaultdict

import arrow
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

sys.path.append(op.abspath(op.join(op.abspath(__file__), "../../")))
from common.utils import get_logger

NAME = (op.splitext(op.basename(__file__))[0])


def time2str_safe(timestamp):
    '''
    时间戳 int -> string，excel输出梅花，供pandas apply方法使用

    :param timestamp: 待转换的时间戳
    :return: 转换后的字符串
    '''
    if not timestamp or np.isnan(timestamp):
        return ""
    try:
        return arrow.get(timestamp).to("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss")
    except Exception:
        return ""


class SuspOrdeStat:
    def __init__(self, db_uri, driver_track_uri, logger=None):
        self.db_uri = db_uri
        self.driver_track_uri = driver_track_uri
        if logger is None:
            self.log = get_logger(NAME)
        else:
            self.log = logger

    def __dumpFormat(self, df):
        '''
        明细数据输出excel格式化，删除多余的列，进行时间格式转换

        :param df: 待输出的dataframe
        :return: 转换后的dataframe
        '''

        # 字段转换
        df.loc[:, ['arrival_time']] = df.arrival_time.apply(time2str_safe)
        df.loc[:, ['start_time']] = df.start_time.apply(time2str_safe)
        # lambda x: arrow.get(x).to("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"))
        df.loc[:, ['end_time']] = df.end_time.apply(time2str_safe)
        # lambda x: arrow.get(x).to("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"))
        df.loc[:, ['update_time']] = df.update_time.apply(time2str_safe)
        # lambda x: arrow.get(x).to("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"))
        df.loc[:, ['device_update']] = df.device_update.apply(time2str_safe)
        # lambda x: arrow.get(x).to("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"))

        df['date'] = df.start_time.copy().str.slice(0, 10)

        # 多余字段删除
        if 'execute_date' in df.columns:
            del df['execute_date']
        if 'isDeviationPositive' in df.columns:
            del df['isDeviationPositive']
        if 'pct_for_sort' in df.columns:
            del df['pct_for_sort']
        if 'device_type' in df.columns:
            del df['device_type']

        # 疑异分类辅助字段
        '''
        if 'predict_distance' in df.columns:
            del df['predict_distance']
        if 'arrival_time' in df.columns:
            del df['arrival_time']
        '''

        return df

    def __cacheOrderExt(self, spark, dates):
        '''
        查询关联的service_order_ext, 去重（按update时间戳取最新1条）, 缓存为spark临时表，供订单表join使用

        :param spark:  spark session对象
        :param dates:  执行日期
        :return:
        '''
        sql = """
    SELECT service_order_id,driver_version,runtime, cast(j.distance as int) as predict_distance
    FROM (
    	SELECT service_order_id,driver_version,runtime, estimate_snap,
    	ROW_NUMBER() OVER (DISTRIBUTE BY service_order_id SORT BY update_time DESC) rank
    	FROM ods.service_order_ext
    	WHERE dt in ({dates})
    ) tmp LATERAL VIEW json_tuple(tmp.estimate_snap, 'distance') j as distance
    where tmp.rank=1
    """.format(dates=dates)
        spark.sql(sql).dropDuplicates().createOrReplaceTempView("extDF")
        spark.catalog.cacheTable("extDF")
        self.log.info("register newset fo_service_order_ext as TEMP_VIEW extDF")

    def __getDistErrDF(self, spark, dates, startDt, endDt):
        '''
        获取里程疑异订单基准数据（系统记录里程 和 调整后里程不一致）

        :param spark: spark session对象
        :param dates: 查询的时间范围（ e.g. 20170329, 20170330），供hive筛选partition使用
        :param startDt: 查询的开始时间 (e.g. 2017-03-29 00:00:00)
        :param endDt:  查询的结束时间 (e.g. 2017-03-30 23:29:29)
        :return:  pandas dataframe
        '''

        sql = """
     SELECT * FROM (
          SELECT cast(o.service_order_id as string),
                 cast(o.driver_id as string),
                 cast(o.arrival_time as int),
                 cast(o.start_time as int),
                 cast(o.end_time as int),
                 cast(o.update_time as int),
                 ext.driver_version,
                 ext.predict_distance,
                 round(o.system_distance/1000,1) AS system_distance,
                 round(o.dependable_distance/1000,1) AS dependable_distance,
                 round(o.dependable_distance/1000,1)-round(o.system_distance/1000,1) AS deviation_distance,
                 round(100*(o.dependable_distance-o.system_distance)/o.dependable_distance,0) as pct_for_sort,
                 concat(round(100*(o.dependable_distance-o.system_distance)/o.dependable_distance,0),'%') AS percent_deviation_distance,
                 row_number() over (partition by o.service_order_id order by o.update_time desc ) as rank
          FROM ods.service_order AS o, extDF AS ext
          WHERE o.dt in ({dates})
              AND o.service_order_id = ext.service_order_id
              --基本条件
              AND ext.driver_version !=''
              AND o.status = 7
              AND o.passenger_name NOT LIKE '%测试%'
              --业务条件
              AND round(o.dependable_distance/1000, 1) > round(o.system_distance/1000, 1) --里程有调整
              AND ext.runtime = o.actual_time_length --时间未调整
              AND o.start_time BETWEEN UNIX_TIMESTAMP(\"{startDt}\") AND UNIX_TIMESTAMP(\"{endDt}\")
      ) tmp
      where rank=1
      ORDER BY pct_for_sort DESC, deviation_distance DESC
            """.format(dates=dates, startDt=startDt.format("YYYY-MM-DD HH:mm:ss"),
                       endDt=endDt.format("YYYY-MM-DD HH:mm:ss"))
        # 输出结果
        self.log.info(sql)
        distErrDF = spark.sql(sql).toPandas()
        return distErrDF

    def __getOptErrDF(self, spark, dates, startDt, endDt):
        '''
        操作异常订单 （订单时间被调整，调短->忘记结束，调长->忘记开始）

        :param spark: spark session对象
        :param dates: 查询的时间范围（ e.g. 20170329, 20170330），供hive筛选partition使用
        :param startDt: 查询的开始时间 arrow对象 (e.g. 2017-03-29 00:00:00)
        :param endDt:  查询的结束时间 arrow对象 (e.g. 2017-03-30 23:29:29)
        :return:  pandas dataframe
        '''

        # 操作异常订单
        sql = """
     SELECT * FROM (
        SELECT cast(o.service_order_id as string),
            cast(o.driver_id as string),
            cast(o.arrival_time as int),
            cast(o.start_time as int),
            cast(o.end_time as int),
            cast(o.update_time as int),
            ext.predict_distance,
            ext.driver_version,
            ext.runtime AS system_time,
            o.actual_time_length AS dependable_time,
            round(100*(o.actual_time_length - ext.runtime)/o.actual_time_length,0) as pct_for_sort,
            concat(round(100*(o.actual_time_length - ext.runtime)/o.actual_time_length,0),'%') AS percent_time,
            round(o.system_distance/1000,1) AS system_distance,
            round(o.dependable_distance/1000,1) AS dependable_distance,
            round(o.dependable_distance/1000,1)-round(o.system_distance/1000,1) AS deviation_distance,
            -- 调整后里程 与 系统记录里程 的差异，作为判断标志
            CASE
                WHEN round(o.dependable_distance/1000,1)-round(o.system_distance/1000,1) > 0 THEN 1
                ELSE 0
            END AS isDeviationPositive,
            row_number() over (partition by o.service_order_id order by o.update_time desc ) as rank
        FROM ods.service_order o, extDF AS ext
        WHERE o.dt in ({dates})
            AND o.service_order_id = ext.service_order_id
            --基本条件
            AND ext.driver_version !=''
            AND o.status = 7
            AND o.passenger_name NOT LIKE '%测试%'
            --业务条件
            AND round(o.dependable_distance/1000,1) != round(o.system_distance/1000,1)
            AND ext.runtime != o.actual_time_length
            AND o.start_time BETWEEN UNIX_TIMESTAMP(\"{startDt}\") AND UNIX_TIMESTAMP(\"{endDt}\")
    ) tmp
    where rank=1
    ORDER BY pct_for_sort DESC, deviation_distance DESC
            """.format(dates=dates, startDt=startDt.format("YYYY-MM-DD HH:mm:ss"),
                       endDt=endDt.format("YYYY-MM-DD HH:mm:ss"))

        self.log.info(sql)
        return spark.sql(sql).toPandas()

    def __updateByHist(self, df, date, table):
        '''
        用数据库中的历史记录, 标示出纯新增的记录, 并更新至数据库

        :param df: 待更新dataframe
        :param date: 执行日期
        :param table: 更新的表名称
        :return: 更新后的数据集
        '''

        # 修正字段
        if 'rank' in df.columns:
            del df['rank']
        df['execute_date'] = date
        df['is_new'] = 1

        # 从数据库加载原有记录,与最新的进行合并
        engine = create_engine(self.db_uri, echo=False)
        if engine.dialect.has_table(engine, table):
            with engine.connect() as conn:
                _10DaysAgo = arrow.get(date, "YYYYMMDD").replace(days=-10).format("YYYYMMDD")
                # 删除10天前的记录
                conn.execute("DELETE FROM {tbl} WHERE execute_date<{dt}".format(tbl=table, dt=_10DaysAgo))
                # 清除当天的执行记录，防止重复写入
                conn.execute("DELETE FROM {tbl} WHERE execute_date={dt}".format(tbl=table, dt=date))
            # 读取最近10天的历史记录, 均标为0 （历史记录）
            histDF = pd.read_sql_table(table, con=engine, index_col=None)
            histDF['is_new'] = 0
            df['is_new'] = np.where(df.service_order_id.isin(histDF.service_order_id.values), 0, 1)
            # 将今天新增的记录增加入数据库
            mergedDF = pd.concat([histDF, df[df.is_new == 1]])
            mergedDF.to_sql(name=table, con=engine, if_exists='replace', index=False)
            self.log.info("update {tbl} dt={dt} histSize={histSize} new={newSize} total={total}" \
                          .format(tbl=table, dt=date, histSize=histDF.shape[0],
                                  newSize=df[df.is_new == 1].shape[0], total=mergedDF.shape[0]))
        else:
            df.to_sql(name=table, con=engine, if_exists='replace', index=False)
            self.log.info("create {tbl} size={sz}".format(tbl=table, sz=df.shape[0]))

        # 返回标记后的结果
        return df

    def __addDeviceInfo(self, df):
        '''
        对于能过查到设备信息的订单，增加设备信息

        :param df: 待添加的dataframe
        :return: 添加后的dataframe
        '''

        engine = create_engine(self.db_uri, echo=False)
        if engine.dialect.has_table(engine, "ods_device"):
            with engine.connect() as conn:
                driver_ids = ','.join(map(lambda x: str(x), df.driver_id))
                cols = "CONVERT(user_id, char) as driver_id, os_type, os_version,os_name , device_type, app_version, update_time as device_update"
                sql = "SELECT {cols} FROM ods_device WHERE user_type='DR' and user_id in ({driver_ids})" \
                    .format(cols=cols, driver_ids=driver_ids)
                deviceDF = pd.read_sql_query(sql, conn)

                self.log.debug(
                    "records={total} deviceFind={finded}".format(total=df.shape[0], finded=deviceDF.shape[0]))

            # 合并设备信息列
            if deviceDF is None or deviceDF.empty:
                self.log.warn("no device records find, please check your datasource firstly")
                return df
            else:
                self.log.info("add device records success")
                return pd.merge(df, deviceDF, how='left', on=['driver_id'])

        else:
            self.log.warn("table ods_device no exist! can not attach device info")
            return df

    def __addLbsDistInfo(self, df, date):
        '''
        对于能查到LBS里程矫正的，填补里程矫正相关字段 （鹰眼距离、实时计算距离、司机端距离，矫正后距离，矫正策略开关）
        这些字段是LBS的同学需要使用

        :param df:  待添加字段的dataframe
        :param date:  添加的日期（LBS hive数据的日期）
        :return: 添加了字段的dataframe
        '''

        from LbsDistanceHelper import LbsHiveCliClient

        hiveClient = LbsHiveCliClient(self.log)

        if not hiveClient.checkPartition("geo.log_distance_dt", "day={}".format(date)):
            self.log.error("LBS dist data not arrive day={}".format(date))
            exit(-1)

        # 构造查询参数
        queryParams = defaultdict(list)
        for index, row in df.iterrows():
            dt = arrow.get(row.end_time).to("Asia/Shanghai").format("YYYYMMDD")
            queryParams[dt].append(int(row.service_order_id))

        self.log.debug("LBS dist query params={}".format(queryParams))

        # [{'r_yy_dis': 18.805, 'r_sys_dis': 18.747, 'driver_dis': 18.667, 'response_dis': 18.805, 'is_enable': 1}]
        hiveResult = hiveClient.getLbsDistInHive(queryParams);
        lbsDistDF = pd.DataFrame.from_dict(hiveResult)
        lbsDistDF.drop_duplicates(['service_order_id'])
        self.log.info("get LBS dist result={hive} dropDuplicates={uniq} inputSize={inSize}" \
                      .format(hive=len(hiveResult), uniq=lbsDistDF.shape[0], inSize=df.shape[0]))

        # 合并结果
        return pd.merge(df, lbsDistDF, how='left', on=['service_order_id'])

    def __addSuspicionType(self, df):
        # TODO 此部分需要和冯博对接，继续完善

        '''
        根据轨迹，增加疑异类型

        :param df:  待添加字段的dataframe
        :return: 添加了字段的dataframe
        '''

        from LbsSuspClassifier import LbsSuspClassifier
        t = time.time()
        classifier = LbsSuspClassifier(self.driver_track_uri, logger=self.log)
        self.log.info("begin to add suspicion type by track data, it will fetch track by LBS Hbase api, size={} ... ".format(df.shape[0]))
        df['susp_type'] = df.apply(classifier.suspClassify, axis=1)
        self.log.info("add suspicion type complete, elasped={}mins".format((time.time() - t) / 60))
        return df

    def generateReport(self, date, dump_dir):
        '''
        进行疑异订单统计，生成excel

        :param dt:  统计日期
        :param use_rc:  是否区分风控数据（若是会生成2分报告）
        :return:
        '''

        self.log.info("execute_date={dt}".format(dt=date))

        t = time.time()

        # 日期转换
        execDt = arrow.get(date, "YYYYMMDD")
        _10daysAgo = execDt.replace(days=-9)
        startDt = _10daysAgo.span("day")[0]
        endDt = execDt.span("day")[1]

        dates = pd.date_range(_10daysAgo.format("YYYYMMDD"), date, freq="D")
        dates = ','.join([dt.strftime("%Y%m%d") for dt in dates])

        self.log.debug("from={f} to={t} dates={dts}".format(f=startDt, t=endDt, dts=date))

        distErrDF = None
        optErrDF = None
        distErrSize = 0
        optErrSize = 0

        # 为避免端口冲突，随机设置spark端口
        port = 4040 + random.randint(100, 1000)

        # 从hive加载基础数据源

        spark = SparkSession.builder \
            .master("local[4]") \
            .config("spark.ui.port", str(port)) \
            .appName("DistOutlierStat") \
            .enableHiveSupport() \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "100") \
            .getOrCreate()

        try:
            # 查询关联的service_order_ext, 去重, 缓存到内存
            self.__cacheOrderExt(spark, dates)
            self.log.info("service_order_ext is cached")

            # 里程异常订单
            distErrDF = self.__getDistErrDF(spark, dates, startDt, endDt)
            distErrSize = 0 if distErrDF is None or distErrDF.empty else distErrDF.shape[0]
            self.log.info("里程异议订单 sparkSQL -> pandas, size: " + str(distErrSize))

            # 操作异常订单
            optErrDF = self.__getOptErrDF(spark, dates, startDt, endDt)
            optErrSize = 0 if optErrDF is None or optErrDF.empty else optErrDF.shape[0]
            self.log.info("操作异议订单 sparkSQL -> pandas, size: " + str(optErrSize))

            spark.catalog.clearCache()

        except Exception, e:
            traceback.print_exc()
        finally:
            if spark is not None:
                spark.stop()
                del spark

        if distErrSize == 0:
            self.log.warning("distErrDF is empty")

        if optErrSize == 0:
            self.log.warning("optErrDF is empty")

        if distErrSize == 0 and optErrSize == 0:
            self.log.fatal(
                "both result from spark is empty, please check your datasource! program will exit with code=-1")
            exit(-1)

        # 用DB中的历史记录更新, 识别出新增的记录, 打上tag
        if distErrSize > 0:
            distErrDF = self.__addDeviceInfo(distErrDF)
            distErrDF = self.__addSuspicionType(distErrDF)
            distErrDF = self.__addLbsDistInfo(distErrDF, date)
            distErrDF = self.__updateByHist(distErrDF, date, "susp_dist_err")
            distErrDF = self.__dumpFormat(distErrDF)

        noStartDF = None
        noEndDF = None
        if optErrSize > 0:
            # 更新DB中的历史记录
            optErrDF = self.__addDeviceInfo(optErrDF)
            optErrDF = self.__addSuspicionType(optErrDF)
            optErrDF = self.__addLbsDistInfo(optErrDF, date)
            optErrDF = self.__updateByHist(optErrDF, date, "susp_opt_err")
            # 偏离里程是正的,属于忘记开始
            noStartDF = optErrDF[optErrDF.isDeviationPositive == 1].copy()
            noStartDF = self.__dumpFormat(noStartDF)

            # 偏离里程是负的,属于绕路或忘记结束
            noEndDF = optErrDF[optErrDF.isDeviationPositive == 0].copy()
            noEndDF = noEndDF.sort_values(['pct_for_sort', 'deviation_distance'], ascending=[True, True])
            noEndDF = self.__dumpFormat(noEndDF)

        # 总体记录
        summaryDF = pd.concat([distErrDF.groupby("date").size().to_frame(u"里程疑异"), \
                               noStartDF.groupby("date").size().to_frame(u"误操作(忘开始)"), \
                               noEndDF.groupby("date").size().to_frame(u"误操作(绕路|忘结束)")], axis=1)
        summaryDF = summaryDF.sort_index(axis=0, ascending=True)

        excel_name = "suspicious_orders_{dt}.xlsx".format(dt=date)
        excel_path = op.join(dump_dir, excel_name)
        with pd.ExcelWriter(excel_path, options={'encoding': 'utf-8', 'engine': 'xlsxwriter'}) as writer:
            summaryDF.to_excel(writer, sheet_name=unicode('总体情况', "utf-8"), index=True)
            if distErrDF is not None:
                distErrDF.to_excel(writer, sheet_name=unicode('里程疑异', "utf-8"), index=False)
            if noStartDF is not None:
                noStartDF.to_excel(writer, sheet_name=unicode('误操作(忘开始)', "utf-8"), index=False)
            if noEndDF is not None:
                noEndDF.to_excel(writer, sheet_name=unicode('误操作(绕路|忘结束)', "utf-8"), index=False)

        self.log.info("write to excel={name}".format(name=excel_name))

        return excel_path
