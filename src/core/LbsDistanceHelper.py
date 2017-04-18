# -*- coding: utf-8 -*-

'''
从BI的hive导出LBS计算的订单辅助距离相关参数（鹰眼距离、实时计算距离、司机端距离，矫正后距离，矫正策略开关）
'''
import os
import os.path as op
import sys
from subprocess import Popen, PIPE, STDOUT

sys.path.append(op.abspath(op.join(op.abspath(__file__), "../../")))
from common.utils import get_logger

NAME = (op.splitext(op.basename(__file__))[0])


class LbsHiveCliClient:
    # DM1上hive客户端的环境变量 （连接的是BI的hive）
    HADOOP_HOME = "/home/y/usr/local/hadoop"
    HIVE_HOME = "/home/y/usr/local/hive"

    # 公用的python和java环境变量
    PATH = "/var/tmp/crontab/bin/anaconda2/bin:/var/tmp/crontab/bin/jdk1.8.0_101/bin:/home/y/usr/local/hive/bin:/home/y/usr/local/hadoop/bin:/bin:/usr/sbin:/usr/bin:/root/bin:/home/lujin/bin"

    def __init__(self, logger=None):
        if logger is None:
            self.log = get_logger(NAME)
        else:
            self.log = logger

    def getLbsDistInHive(self, queryParams):
        '''
        根据传入的日期和订单id，查询对于的LBS计算里程相关字段
        :param queryParams:
            e.g. -> {
                '20170302' : [serive_order_id, serive_order_id, ... , serive_order_id]
                '20170303' : [serive_order_id, serive_order_id, ... , serive_order_id]
            }
        :return: e.g. [{'r_sys_dis': 18.747, 'driver_dis': 18.667, 'response_dis': 18.805, 'is_enable': 1, 'service_order_id': '6395470206447011702', 'r_yy_dis': 18.805}]
        '''

        result = list()
        for dt, ids in queryParams.iteritems():
            inSize = len(ids)
            ids = ','.join(map(lambda x: str(x), ids))
            sql = "select * from geo.log_distance_dt where day={dt} and service_order_id in ({ids});".format(dt=dt,
                                                                                                             ids=ids)
            hiveRawResult = self.__queryHive(sql)
            self.log.debug("dt={dt} hive={rs}".format(dt=dt, rs=hiveRawResult))
            extractedResult = self.__extractLbsDistResult(hiveRawResult)
            for rs in extractedResult:
                result.append(rs)
            self.log.info("LBS distance data, dt={dt} inSize={inSize} getSize={getSize}" \
                     .format(dt=dt, inSize=inSize, getSize=len(extractedResult)))
        else:
            self.log.info("get LBS distance data from hive size={}".format(len(result)))
            return result

    def __extractLbsDistResult(self, lines):
        '''
        解析hive返回的字符串，获取表数据，转换为dict
        :param lines: hive命令返回的字符串
        :return: e.g. [{'r_sys_dis': 18.747, 'driver_dis': 18.667, 'response_dis': 18.805, 'is_enable': 1, 'service_order_id': '6395470206447011702', 'r_yy_dis': 18.805}]
        '''

        # 判断浮点数使用
        def isfloat(value):
            try:
                float(value)
                return True
            except:
                return False

        result = list()
        try:
            for line in lines:
                # 找到实际的数据行
                if line.find('\t') > 0:
                    segs = line.split("\t")
                    result.append({
                        "service_order_id": segs[2],
                        "r_yy_dis": float(segs[4]) if isfloat(segs[4]) else 0.0,
                        "r_sys_dis": float(segs[6]) if isfloat(segs[6]) else 0.0,
                        "driver_dis": float(segs[8]) if isfloat(segs[8]) else 0.0,
                        "response_dis": float(segs[9]) if isfloat(segs[9]) else 0.0,
                        "is_enable": int(segs[13]) if segs[13].isdigit() else 0
                    })
            else:
                self.log.debug("extractLbsDistResult -> {}".format(result))
                return result
        except Exception, e:
            self.log.exception("extract LBS result from hive cmd result failed, msg={}".format(e.message))
            exit(-1)

    def __queryHive(self, sql):
        '''
        切换线上环境变量，执行hive查询
        :param sql:
        :return:
        '''

        # 切换环境变量为线上hive
        current_hadoop_home = os.environ['HADOOP_HOME']
        current_hive_home = os.environ['HIVE_HOME']
        current_path = os.environ['PATH']
        os.environ['HADOOP_HOME'] = self.HADOOP_HOME
        os.environ['HIVE_HOME'] = self.HIVE_HOME
        os.environ['PATH'] = self.PATH

        self.log.debug("HADOOP_HOME={}".format(os.environ['HADOOP_HOME']))
        self.log.debug("HIVE_HOME={}".format(os.environ['HIVE_HOME']))
        self.log.debug("PATH={}".format(os.environ['PATH']))

        try:
            cmd = "hive -e '{}'".format(sql)
            self.log.debug(cmd)
            proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
            result = list()
            for line in proc.stdout.readlines():
                if "FAILED" in line:
                    self.log.error("query hive failed, return [] as default {}".format(line))
                    return []
                result.append(line.strip())
        except Exception, e:
            self.log.exception("query hive by cmd failed, msg={}".format(e.message))
            exit(-1)
        finally:
            # 还原环境变量
            os.environ['HADOOP_HOME'] = current_hadoop_home
            os.environ['HIVE_HOME'] = current_hive_home
            os.environ['PATH'] = current_path

        return result

    def checkPartition(self, table, partition):
        '''
        确认需要查询的表对应的分区是否存在
        :param table:  DB.TABLE_NAME
        :param partition:  e.g. dt=20170309
        :return:  True / False
        '''

        # 切换环境变量为线上hive
        current_hadoop_home = os.environ['HADOOP_HOME']
        current_hive_home = os.environ['HIVE_HOME']
        current_path = os.environ['PATH']
        os.environ['HADOOP_HOME'] = self.HADOOP_HOME
        os.environ['HIVE_HOME'] = self.HIVE_HOME
        os.environ['PATH'] = self.PATH

        self.log.debug("HADOOP_HOME={}".format(os.environ['HADOOP_HOME']))
        self.log.debug("HIVE_HOME={}".format(os.environ['HIVE_HOME']))
        self.log.debug("PATH={}".format(os.environ['PATH']))

        try:
            cmd = "hive -e 'SHOW PARTITIONS {tbl} PARTITION({part});'".format(tbl=table, part=partition)
            self.log.debug(cmd)
            proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
            for line in proc.stdout.readlines():
                if "FAILED" in line:
                    self.log.error("check hive parition failed, return False as default {}".format(line))
                    return False
                elif partition in line:
                    return True
            else:
                return False
        except Exception, e:
            self.log.exception("check hive parition by cmd failed, msg={}".format(e.message))
            exit(-1)
        finally:
            # 还原环境变量
            os.environ['HADOOP_HOME'] = current_hadoop_home
            os.environ['HIVE_HOME'] = current_hive_home
            os.environ['PATH'] = current_path


if __name__ == "__main__":

    client = LbsHiveCliClient()

    data = {
        "20170308": [6394973188166712431, 6394978578025352192, 6394972084744457040],
        "20170309": [6395462763244420321, 6395470206447011702, 6395490100044010826, 6395468131973645282],
    }

    for data in client.getLbsDistInHive(data):
        print data
