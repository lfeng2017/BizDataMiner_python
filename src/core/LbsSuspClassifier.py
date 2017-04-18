# -*- coding: utf-8 -*-

import os.path as op
import sys

import arrow
import pandas as pd
import requests

sys.path.append(op.abspath(op.join(op.abspath(__file__), "../../")))
from common.utils import get_logger

NAME = (op.splitext(op.basename(__file__))[0])

log = get_logger(NAME)

TYPES = {
    0: unicode("unknow", "utf-8"),
    1: unicode("开始和结束正常，中途丢点", "utf-8"),
    2: unicode("轨迹正常，司机忘记点击出发", "utf-8"),
    3: unicode("行驶途中丢点", "utf-8"),
    4: unicode("轨迹、里程无误，但司机异议", "utf-8")
}


class LbsSuspClassifier:
    def __init__(self, driver_track_url, logger=None):
        self.driver_track_url = driver_track_url
        if logger is None:
            self.log = get_logger(NAME)
        else:
            self.log = logger

    def suspClassify(self, row):
        order_id = row.service_order_id
        arrival_time = row.arrival_time
        start_time = row.start_time
        end_time = row.end_time
        system_distance = row.system_distance
        predict_distance = float(row.predict_distance) / 1000.0

        # 轨迹范围：订单开始结束前后5分钟
        fullTrackDF = self.getLbsTrack(order_id, before_secs=300, after_secs=300)
        self.log.debug("get track orderId={id}".format(id=order_id))

        # hbase 取不到数据的情况，无法判断
        if fullTrackDF.empty:
            return unicode("无轨迹数据", "utf-8"),

        # 实际订单轨迹
        orderTrackDF = fullTrackDF[(fullTrackDF.timestamp >= start_time) & (fullTrackDF.timestamp <= end_time)]

        if self.__isHaveStartEndLostMiddle(fullTrackDF, start_time, end_time):
            return unicode("开始和结束正常，中途丢点", "utf-8")
        elif self.__isNormButForgetStart(orderTrackDF, arrival_time, start_time, system_distance, predict_distance):
            return unicode("轨迹正常，司机忘记点击出发", "utf-8")
        elif self.__isLostMiddle(orderTrackDF, system_distance, predict_distance):
            return unicode("行驶途中丢点", "utf-8")
        elif self.__isNormButDriverSusp(orderTrackDF, system_distance, predict_distance):
            return unicode("轨迹、里程无误，但司机异议", "utf-8")
        else:
            return unicode("unknow", "utf-8")

    def __avgLocInterval(self, dfOfLoc):
        '''
        平均定位间隔：(结束时间 - 开始时间 / 定位次数)
        :param dfOfLoc:
        :return:
        '''
        if dfOfLoc is None or dfOfLoc.empty:
            return 0

        size = dfOfLoc.shape[0]
        maxTs = dfOfLoc['timestamp'].max()
        minTs = dfOfLoc['timestamp'].min()

        return (int)(maxTs - minTs) / size

    def __addInterval(self, dfOfLoc):
        '''
        增加采用间距字段，下一行的时间戳 - 本行的时间戳，移除最后一行记录
        :param dfOfLoc:
        :return:
        '''
        if dfOfLoc is None or dfOfLoc.empty:
            return -1

        # 确保时间升序
        dfOfLoc.sort_values(['timestamp'], ascending=True, inplace=True)

        # 错行计算间距
        dfOfLoc['nextTimestamp'] = dfOfLoc['timestamp'].shift(-1)
        dfOfLoc.dropna(inplace=True)
        dfOfLoc['nextTimestamp'] = dfOfLoc['nextTimestamp'].astype(int)
        dfOfLoc['inteval'] = dfOfLoc['nextTimestamp'] - dfOfLoc['timestamp']

        return dfOfLoc

    def __locIntervalBound(self, dfOfLoc):
        '''
        获取采用间距的min和max
        :param dfOfLoc:
        :return:
        '''
        if dfOfLoc is None or dfOfLoc.empty:
            return -1, -1

        dfOfLoc = self.__addInterval(dfOfLoc)

        return dfOfLoc['inteval'].min(), dfOfLoc['inteval'].max()

    def __locIntervalGap(self, dfOfLoc, gap):
        '''
        获取间距大于gap的间距序列，降序排序
        :param dfOfLoc:
        :param gap:
        :return:
        '''
        if dfOfLoc is None or dfOfLoc.empty:
            return []
        if gap <= 0:
            return []

        dfOfLoc = self.__addInterval(dfOfLoc)

        intevals = dfOfLoc[dfOfLoc.inteval > gap]['inteval']

        return list(intevals.drop_duplicates().sort_values(ascending=False).values)

    def getLbsTrack(self, order_id, before_secs=300, after_secs=300):
        url = self.driver_track_url.format(id=order_id, before=before_secs, after=after_secs)
        resp = requests.get(url)
        if resp.status_code != 200:
            log.warn("get track failed for orderId={}".format(order_id))
            return pd.DataFrame()
        results = ""
        try:
            results = resp.json()['results']
            if len(results) == 0:
                return pd.DataFrame()
            # print len(results), results
            df = pd.DataFrame.from_dict(results)
            df['timestamp'] = df['timestamp'] / 1000
            df['provider'] = df['provider'].astype(str)
            df['timestamp'] = df['timestamp'].astype(int)
            df = df[['timestamp', 'provider', 'accuracy', 'latitude', 'longitude']]
            # print df.head()
            # print df.dtypes
            return df
        except Exception, e:
            log.exception("parse track results failed, results={resp}, msg={msg}".format(resp=results, msg=e.message))
            return pd.DataFrame()

    def __isHaveStartEndLostMiddle(self, trackDF, start_time, end_time):
        # 条件①：Tavg_loc_interval(Tstart - 300, Tstart) < 15
        # 开始前5分钟，平均采用间隔小余15秒
        beforeStartTrackDF = trackDF[trackDF.timestamp <= start_time]
        isMatch1 = True if self.__avgLocInterval(beforeStartTrackDF) < 15 else False

        # 条件②：Tavg_loc_interval (Tstop, Tstop+300)<15
        # 结束后5分钟，平均采用间隔小余15秒
        afterEndTrackDF = trackDF[trackDF.timestamp >= end_time]
        isMatch2 = True if self.__avgLocInterval(afterEndTrackDF) < 15 else False

        # 实际订单行驶轨迹
        orderTrackDF = trackDF[(trackDF.timestamp >= start_time) & (trackDF.timestamp <= end_time)]
        duration = end_time - start_time

        # 条件③：Tmax_loc_interval(Tstart, Tstop)/(Tstop-Tstart)>0.5 且Tstop-Tstart>300
        # 丢点的最大间隔占总时长的50%以上，且丢点间隔大于5分钟
        if duration == 0:
            return False
        else:
            isMatch3_1 = True if float(self.__locIntervalBound(orderTrackDF)[1]) / float(duration) > 0.5 else False
        isMatch3_2 = True if duration > 300 else False
        isMatch3 = isMatch3_1 & isMatch3_2

        # 条件④：GapList(Tstart, Tstop, 300).size()<=2
        # 大于5分钟的间隔数量小余2
        isMatch4 = True if self.__locIntervalGap(orderTrackDF, 300) < 2 else False

        return isMatch1 & isMatch2 & isMatch3 & isMatch4

    def __isLostMiddle(self, trackDF, system_distance, predict_distance):
        # 条件①：0.2<=客户端计算里程/起终点路径规划里程<=0.8
        isMatch1 = True if 0.2 <= system_distance / predict_distance <= 0.8 else False

        # 条件②：存在至少二个经纬度不同的网络定位点
        # 网络定位点个数大于2，且经纬度不同
        isMatch2 = True if trackDF.drop_duplicates(['latitude', 'longitude']).shape[0] > 2 else False

        return isMatch1 & isMatch2

    def __isNormButDriverSusp(self, trackDF, system_distance, predict_distance):
        # 条件①：客户端计算里程/起终点路径规划里程∈(0.8,1.1)
        isMatch1 = True if 0.8 <= system_distance / predict_distance <= 1.1 else False

        # 条件②：Tavg_loc_interval(Tstart, Tstop)<=10s
        isMatch2 = True if self.__avgLocInterval(trackDF) <= 10 else False

        return isMatch1 & isMatch2

    def __isNormButForgetStart(self, trackDF, arrival_time, start_time, system_distance, predict_distance):
        # 条件①：开始时间-就位时间>10min
        isMatch1 = True if (start_time - arrival_time) > 600 else False

        # 条件②：客户端计算里程/起终点路径规划里程<0.8
        isMatch2 = True if system_distance / predict_distance < 0.8 else False

        # 条件③：Tavg_loc_interval(Tstart, Tstop)<=10s
        isMatch3 = True if self.__avgLocInterval(trackDF) <= 10 else False

        return isMatch1 & isMatch2 & isMatch3

    def __suspId2Str(self, row):
        susp_type = row.susp_type
        return TYPES.get(susp_type, "unknow")


if __name__ == "__main__":
    classifier = LbsSuspClassifier(
        "http://127.0.0.1:9969/track/getByOrderId?order_id={id}&before_start={before}&after_end={after}&sort_order=asc")
    df = classifier.getLbsTrack("6397733765181810544", before_secs=300, after_secs=300)
    print classifier.__avgLocInterval(df)
    print classifier.__locIntervalBound(df)
    print classifier.__locIntervalGap(df, 5)

    df = pd.read_excel("/Users/lujin/Downloads/suspicious_orders_20170316.xlsx", sheetname=1)
    # df['arrival_time'] = df.start_time.map(lambda x: arrow.get(x).to("Asia/Shanghai").timestamp)
    df['start_time'] = df.start_time.map(lambda x: arrow.get(x).to("Asia/Shanghai").timestamp)
    df['end_time'] = df.start_time.map(lambda x: arrow.get(x).to("Asia/Shanghai").timestamp)
    print df.shape

    validTs = int(arrow.now().replace(days=-1).timestamp)
    df = df[df.start_time > validTs][:10]
    print df.shape

    df['susp_type'] = df.apply(classifier.suspClassify, axis=1)

    print df['susp_type'].head()
