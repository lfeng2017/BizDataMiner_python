# -*- coding: utf-8 -*-
'''
usage:
ENV=pro/dev/test python ZeroDistStat.py [--date=20171223] [--mail=False] [--use_rc=False] [--mail_both=False]
ENV -> 环境变量参数，控制config的加载
--date -> 执行日期，默认为昨天
--mail -> 控制是否发送邮件，邮件列表在config中设置
--use_rc -> 是否使用风控数据，True的话将单独生成一份T+2的过滤风控订单的报表（风控数据存在mysql yongche.rc_order表中）
--mail_both -> 和use_rc参数合用，同时发送2个附件
'''
from __future__ import division
import os.path as op
import arrow
import sys
import pandas as pd
import numpy as np
import time
import random
from math import radians, cos, sin, asin, sqrt
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.types import Integer, String
from pyspark.sql import SparkSession

sys.path.append(op.abspath(op.join(op.abspath(__file__), "../../")))
from common.utils import get_logger

NAME = (op.splitext(op.basename(__file__))[0])

# 获取前一天日期字符串，格式：20161010
yesterday = arrow.now().replace(days=-1)
start, end = yesterday.span('day')

CITY_DICT = {
    unicode("lasa", "utf-8"): unicode("拉萨", "utf-8"),
    unicode("as", "utf-8"): unicode("鞍山", "utf-8"),
    unicode("als", "utf-8"): unicode("阿拉善盟", "utf-8"),
    unicode("anqing", "utf-8"): unicode("安庆", "utf-8"),
    unicode("ay", "utf-8"): unicode("安阳", "utf-8"),
    unicode("ab", "utf-8"): unicode("阿坝", "utf-8"),
    unicode("anshun", "utf-8"): unicode("安顺", "utf-8"),
    unicode("al", "utf-8"): unicode("阿里", "utf-8"),
    unicode("ankang", "utf-8"): unicode("安康", "utf-8"),
    unicode("aks", "utf-8"): unicode("阿克苏", "utf-8"),
    unicode("alt", "utf-8"): unicode("阿勒泰", "utf-8"),
    unicode("alaer", "utf-8"): unicode("阿拉尔", "utf-8"),
    unicode("bj", "utf-8"): unicode("北京", "utf-8"),
    unicode("baoji", "utf-8"): unicode("宝鸡", "utf-8"),
    unicode("baod", "utf-8"): unicode("保定", "utf-8"),
    unicode("bt", "utf-8"): unicode("包头", "utf-8"),
    unicode("benxi", "utf-8"): unicode("本溪", "utf-8"),
    unicode("baishan", "utf-8"): unicode("白山", "utf-8"),
    unicode("byne", "utf-8"): unicode("巴彦淖尔", "utf-8"),
    unicode("bc", "utf-8"): unicode("白城", "utf-8"),
    unicode("bengbu", "utf-8"): unicode("蚌埠", "utf-8"),
    unicode("bozhou", "utf-8"): unicode("亳州", "utf-8"),
    unicode("bz", "utf-8"): unicode("滨州", "utf-8"),
    unicode("bh", "utf-8"): unicode("北海", "utf-8"),
    unicode("baise", "utf-8"): unicode("百色", "utf-8"),
    unicode("baisha", "utf-8"): unicode("白沙", "utf-8"),
    unicode("baoting", "utf-8"): unicode("保亭", "utf-8"),
    unicode("bazhong", "utf-8"): unicode("巴中", "utf-8"),
    unicode("bijie", "utf-8"): unicode("毕节", "utf-8"),
    unicode("bs", "utf-8"): unicode("保山", "utf-8"),
    unicode("by", "utf-8"): unicode("白银", "utf-8"),
    unicode("betl", "utf-8"): unicode("博尔塔拉", "utf-8"),
    unicode("bayinguoleng", "utf-8"): unicode("巴音郭楞", "utf-8"),
    unicode("cq", "utf-8"): unicode("重庆", "utf-8"),
    unicode("cd", "utf-8"): unicode("成都", "utf-8"),
    unicode("chs", "utf-8"): unicode("长沙", "utf-8"),
    unicode("cc", "utf-8"): unicode("长春", "utf-8"),
    unicode("cz", "utf-8"): unicode("常州", "utf-8"),
    unicode("chengde", "utf-8"): unicode("承德", "utf-8"),
    unicode("cangzhou", "utf-8"): unicode("沧州", "utf-8"),
    unicode("changzhi", "utf-8"): unicode("长治", "utf-8"),
    unicode("chifeng", "utf-8"): unicode("赤峰", "utf-8"),
    unicode("cy", "utf-8"): unicode("朝阳", "utf-8"),
    unicode("chuzhou", "utf-8"): unicode("滁州", "utf-8"),
    unicode("ch", "utf-8"): unicode("巢湖", "utf-8"),
    unicode("chizhou", "utf-8"): unicode("池州", "utf-8"),
    unicode("changde", "utf-8"): unicode("常德", "utf-8"),
    unicode("chenzhou", "utf-8"): unicode("郴州", "utf-8"),
    unicode("chaozhou", "utf-8"): unicode("潮州", "utf-8"),
    unicode("chongzuo", "utf-8"): unicode("崇左", "utf-8"),
    unicode("chengmai", "utf-8"): unicode("澄迈", "utf-8"),
    unicode("cx", "utf-8"): unicode("楚雄", "utf-8"),
    unicode("changdu", "utf-8"): unicode("昌都", "utf-8"),
    unicode("changji", "utf-8"): unicode("昌吉", "utf-8"),
    unicode("dg", "utf-8"): unicode("东莞", "utf-8"),
    unicode("dt", "utf-8"): unicode("大同", "utf-8"),
    unicode("dl", "utf-8"): unicode("大连", "utf-8"),
    unicode("dali", "utf-8"): unicode("大理", "utf-8"),
    unicode("dq", "utf-8"): unicode("大庆", "utf-8"),
    unicode("dandong", "utf-8"): unicode("丹东", "utf-8"),
    unicode("dxal", "utf-8"): unicode("大兴安岭", "utf-8"),
    unicode("dy", "utf-8"): unicode("东营", "utf-8"),
    unicode("dz", "utf-8"): unicode("德州", "utf-8"),
    unicode("danzhou", "utf-8"): unicode("儋州", "utf-8"),
    unicode("dongfang", "utf-8"): unicode("东方", "utf-8"),
    unicode("dingan", "utf-8"): unicode("定安", "utf-8"),
    unicode("deyang", "utf-8"): unicode("德阳", "utf-8"),
    unicode("dazhou", "utf-8"): unicode("达州", "utf-8"),
    unicode("dh", "utf-8"): unicode("德宏", "utf-8"),
    unicode("diqing", "utf-8"): unicode("迪庆", "utf-8"),
    unicode("dx", "utf-8"): unicode("定西", "utf-8"),
    unicode("erds", "utf-8"): unicode("鄂尔多斯", "utf-8"),
    unicode("ez", "utf-8"): unicode("鄂州", "utf-8"),
    unicode("es", "utf-8"): unicode("恩施", "utf-8"),
    unicode("fz", "utf-8"): unicode("福州", "utf-8"),
    unicode("fs", "utf-8"): unicode("佛山", "utf-8"),
    unicode("fushun", "utf-8"): unicode("抚顺", "utf-8"),
    unicode("fx", "utf-8"): unicode("阜新", "utf-8"),
    unicode("fy", "utf-8"): unicode("阜阳", "utf-8"),
    unicode("fuz", "utf-8"): unicode("抚州", "utf-8"),
    unicode("fcg", "utf-8"): unicode("防城港", "utf-8"),
    unicode("gz", "utf-8"): unicode("广州", "utf-8"),
    unicode("gl", "utf-8"): unicode("桂林", "utf-8"),
    unicode("gy", "utf-8"): unicode("贵阳", "utf-8"),
    unicode("ganzhou", "utf-8"): unicode("赣州", "utf-8"),
    unicode("gg", "utf-8"): unicode("贵港", "utf-8"),
    unicode("guangyuan", "utf-8"): unicode("广元", "utf-8"),
    unicode("ga", "utf-8"): unicode("广安", "utf-8"),
    unicode("ganzi", "utf-8"): unicode("甘孜", "utf-8"),
    unicode("gn", "utf-8"): unicode("甘南", "utf-8"),
    unicode("guoluo", "utf-8"): unicode("果洛", "utf-8"),
    unicode("guyuan", "utf-8"): unicode("固原", "utf-8"),
    unicode("hz", "utf-8"): unicode("杭州", "utf-8"),
    unicode("haikou", "utf-8"): unicode("海口", "utf-8"),
    unicode("hf", "utf-8"): unicode("合肥", "utf-8"),
    unicode("hrb", "utf-8"): unicode("哈尔滨", "utf-8"),
    unicode("hu", "utf-8"): unicode("呼和浩特", "utf-8"),
    unicode("huizhou", "utf-8"): unicode("惠州", "utf-8"),
    unicode("ha", "utf-8"): unicode("淮安", "utf-8"),
    unicode("huzhou", "utf-8"): unicode("湖州", "utf-8"),
    unicode("hlbe", "utf-8"): unicode("呼伦贝尔", "utf-8"),
    unicode("hy", "utf-8"): unicode("衡阳", "utf-8"),
    unicode("huangshan", "utf-8"): unicode("黄山", "utf-8"),
    unicode("hd", "utf-8"): unicode("邯郸", "utf-8"),
    unicode("hs", "utf-8"): unicode("衡水", "utf-8"),
    unicode("hld", "utf-8"): unicode("葫芦岛", "utf-8"),
    unicode("hegang", "utf-8"): unicode("鹤岗", "utf-8"),
    unicode("heihe", "utf-8"): unicode("黑河", "utf-8"),
    unicode("hn", "utf-8"): unicode("淮南", "utf-8"),
    unicode("huaibei", "utf-8"): unicode("淮北", "utf-8"),
    unicode("heze", "utf-8"): unicode("菏泽", "utf-8"),
    unicode("hb", "utf-8"): unicode("鹤壁", "utf-8"),
    unicode("hshi", "utf-8"): unicode("黄石", "utf-8"),
    unicode("hg", "utf-8"): unicode("黄冈", "utf-8"),
    unicode("hh", "utf-8"): unicode("怀化", "utf-8"),
    unicode("heyuan", "utf-8"): unicode("河源", "utf-8"),
    unicode("hezhou", "utf-8"): unicode("贺州", "utf-8"),
    unicode("hc", "utf-8"): unicode("河池", "utf-8"),
    unicode("honghe", "utf-8"): unicode("红河", "utf-8"),
    unicode("hanzhong", "utf-8"): unicode("汉中", "utf-8"),
    unicode("haidong", "utf-8"): unicode("海东", "utf-8"),
    unicode("haibei", "utf-8"): unicode("海北", "utf-8"),
    unicode("huangnan", "utf-8"): unicode("黄南", "utf-8"),
    unicode("hnz", "utf-8"): unicode("海南", "utf-8"),
    unicode("hx", "utf-8"): unicode("海西", "utf-8"),
    unicode("hami", "utf-8"): unicode("哈密", "utf-8"),
    unicode("ht", "utf-8"): unicode("和田", "utf-8"),
    unicode("jn", "utf-8"): unicode("济南", "utf-8"),
    unicode("jz", "utf-8"): unicode("晋中", "utf-8"),
    unicode("jx", "utf-8"): unicode("嘉兴", "utf-8"),
    unicode("jh", "utf-8"): unicode("金华", "utf-8"),
    unicode("jl", "utf-8"): unicode("吉林", "utf-8"),
    unicode("jj", "utf-8"): unicode("九江", "utf-8"),
    unicode("jining", "utf-8"): unicode("济宁", "utf-8"),
    unicode("jincheng", "utf-8"): unicode("晋城", "utf-8"),
    unicode("jinzhou", "utf-8"): unicode("锦州", "utf-8"),
    unicode("jixi", "utf-8"): unicode("鸡西", "utf-8"),
    unicode("jms", "utf-8"): unicode("佳木斯", "utf-8"),
    unicode("jdz", "utf-8"): unicode("景德镇", "utf-8"),
    unicode("ja", "utf-8"): unicode("吉安", "utf-8"),
    unicode("jiaozuo", "utf-8"): unicode("焦作", "utf-8"),
    unicode("jiyuan", "utf-8"): unicode("济源", "utf-8"),
    unicode("jingmen", "utf-8"): unicode("荆门", "utf-8"),
    unicode("jingzhou", "utf-8"): unicode("荆州", "utf-8"),
    unicode("jm", "utf-8"): unicode("江门", "utf-8"),
    unicode("jy", "utf-8"): unicode("揭阳", "utf-8"),
    unicode("jyg", "utf-8"): unicode("嘉峪关", "utf-8"),
    unicode("jinchang", "utf-8"): unicode("金昌", "utf-8"),
    unicode("jq", "utf-8"): unicode("酒泉", "utf-8"),
    unicode("km", "utf-8"): unicode("昆明", "utf-8"),
    unicode("kaifeng", "utf-8"): unicode("开封", "utf-8"),
    unicode("klmy", "utf-8"): unicode("克拉玛依", "utf-8"),
    unicode("kz", "utf-8"): unicode("克孜勒苏", "utf-8"),
    unicode("ks", "utf-8"): unicode("喀什", "utf-8"),
    unicode("luoyang", "utf-8"): unicode("洛阳", "utf-8"),
    unicode("lz", "utf-8"): unicode("兰州", "utf-8"),
    unicode("liangshan", "utf-8"): unicode("凉山", "utf-8"),
    unicode("lj", "utf-8"): unicode("丽江", "utf-8"),
    unicode("liuzhou", "utf-8"): unicode("柳州", "utf-8"),
    unicode("luzhou", "utf-8"): unicode("泸州", "utf-8"),
    unicode("ls", "utf-8"): unicode("乐山", "utf-8"),
    unicode("lishui", "utf-8"): unicode("丽水", "utf-8"),
    unicode("lf", "utf-8"): unicode("廊坊", "utf-8"),
    unicode("ld", "utf-8"): unicode("娄底", "utf-8"),
    unicode("ly", "utf-8"): unicode("龙岩", "utf-8"),
    unicode("linyi", "utf-8"): unicode("临沂", "utf-8"),
    unicode("linfen", "utf-8"): unicode("临汾", "utf-8"),
    unicode("lvliang", "utf-8"): unicode("吕梁", "utf-8"),
    unicode("liaoyang", "utf-8"): unicode("辽阳", "utf-8"),
    unicode("liaoyuan", "utf-8"): unicode("辽源", "utf-8"),
    unicode("lyg", "utf-8"): unicode("连云港", "utf-8"),
    unicode("la", "utf-8"): unicode("六安", "utf-8"),
    unicode("lw", "utf-8"): unicode("莱芜", "utf-8"),
    unicode("lc", "utf-8"): unicode("聊城", "utf-8"),
    unicode("luohe", "utf-8"): unicode("漯河", "utf-8"),
    unicode("lb", "utf-8"): unicode("来宾", "utf-8"),
    unicode("lingao", "utf-8"): unicode("临高", "utf-8"),
    unicode("ledong", "utf-8"): unicode("乐东", "utf-8"),
    unicode("lingshui", "utf-8"): unicode("陵水", "utf-8"),
    unicode("lps", "utf-8"): unicode("六盘水", "utf-8"),
    unicode("lincang", "utf-8"): unicode("临沧", "utf-8"),
    unicode("linzhi", "utf-8"): unicode("林芝", "utf-8"),
    unicode("ln", "utf-8"): unicode("陇南", "utf-8"),
    unicode("linxia", "utf-8"): unicode("临夏", "utf-8"),
    unicode("mianyang", "utf-8"): unicode("绵阳", "utf-8"),
    unicode("manzl", "utf-8"): unicode("满洲里", "utf-8"),
    unicode("mdj", "utf-8"): unicode("牡丹江", "utf-8"),
    unicode("mas", "utf-8"): unicode("马鞍山", "utf-8"),
    unicode("mm", "utf-8"): unicode("茂名", "utf-8"),
    unicode("mz", "utf-8"): unicode("梅州", "utf-8"),
    unicode("ms", "utf-8"): unicode("眉山", "utf-8"),
    unicode("nj", "utf-8"): unicode("南京", "utf-8"),
    unicode("nb", "utf-8"): unicode("宁波", "utf-8"),
    unicode("nn", "utf-8"): unicode("南宁", "utf-8"),
    unicode("nanchong", "utf-8"): unicode("南充", "utf-8"),
    unicode("nt", "utf-8"): unicode("南通", "utf-8"),
    unicode("nd", "utf-8"): unicode("宁德", "utf-8"),
    unicode("nc", "utf-8"): unicode("南昌", "utf-8"),
    unicode("np", "utf-8"): unicode("南平", "utf-8"),
    unicode("ny", "utf-8"): unicode("南阳", "utf-8"),
    unicode("nujiang", "utf-8"): unicode("怒江", "utf-8"),
    unicode("nq", "utf-8"): unicode("那曲", "utf-8"),
    unicode("pj", "utf-8"): unicode("盘锦", "utf-8"),
    unicode("pt", "utf-8"): unicode("莆田", "utf-8"),
    unicode("px", "utf-8"): unicode("萍乡", "utf-8"),
    unicode("pds", "utf-8"): unicode("平顶山", "utf-8"),
    unicode("puyang", "utf-8"): unicode("濮阳", "utf-8"),
    unicode("panzhihua", "utf-8"): unicode("攀枝花", "utf-8"),
    unicode("pe", "utf-8"): unicode("普洱", "utf-8"),
    unicode("pl", "utf-8"): unicode("平凉", "utf-8"),
    unicode("qd", "utf-8"): unicode("青岛", "utf-8"),
    unicode("qz", "utf-8"): unicode("泉州", "utf-8"),
    unicode("qxn", "utf-8"): unicode("黔西南", "utf-8"),
    unicode("qj", "utf-8"): unicode("曲靖", "utf-8"),
    unicode("qqhr", "utf-8"): unicode("齐齐哈尔", "utf-8"),
    unicode("quzhou", "utf-8"): unicode("衢州", "utf-8"),
    unicode("qhd", "utf-8"): unicode("秦皇岛", "utf-8"),
    unicode("qf", "utf-8"): unicode("曲阜", "utf-8"),
    unicode("qth", "utf-8"): unicode("七台河", "utf-8"),
    unicode("qianjiang", "utf-8"): unicode("潜江", "utf-8"),
    unicode("qingyuan", "utf-8"): unicode("清远", "utf-8"),
    unicode("qinzhou", "utf-8"): unicode("钦州", "utf-8"),
    unicode("qionghai", "utf-8"): unicode("琼海", "utf-8"),
    unicode("qiongzhong", "utf-8"): unicode("琼中", "utf-8"),
    unicode("qdn", "utf-8"): unicode("黔东南", "utf-8"),
    unicode("qn", "utf-8"): unicode("黔南", "utf-8"),
    unicode("qingyang", "utf-8"): unicode("庆阳", "utf-8"),
    unicode("rizhao", "utf-8"): unicode("日照", "utf-8"),
    unicode("rkz", "utf-8"): unicode("日喀则", "utf-8"),
    unicode("sh", "utf-8"): unicode("上海", "utf-8"),
    unicode("sz", "utf-8"): unicode("深圳", "utf-8"),
    unicode("sanya", "utf-8"): unicode("三亚", "utf-8"),
    unicode("sy", "utf-8"): unicode("沈阳", "utf-8"),
    unicode("su", "utf-8"): unicode("苏州", "utf-8"),
    unicode("sjz", "utf-8"): unicode("石家庄", "utf-8"),
    unicode("st", "utf-8"): unicode("汕头", "utf-8"),
    unicode("sx", "utf-8"): unicode("绍兴", "utf-8"),
    unicode("shaoyang", "utf-8"): unicode("邵阳", "utf-8"),
    unicode("sm", "utf-8"): unicode("三明", "utf-8"),
    unicode("shuozhou", "utf-8"): unicode("朔州", "utf-8"),
    unicode("sp", "utf-8"): unicode("四平", "utf-8"),
    unicode("songyuan", "utf-8"): unicode("松原", "utf-8"),
    unicode("sys", "utf-8"): unicode("双鸭山", "utf-8"),
    unicode("suihua", "utf-8"): unicode("绥化", "utf-8"),
    unicode("suqian", "utf-8"): unicode("宿迁", "utf-8"),
    unicode("suzhousz", "utf-8"): unicode("宿州", "utf-8"),
    unicode("sr", "utf-8"): unicode("上饶", "utf-8"),
    unicode("smx", "utf-8"): unicode("三门峡", "utf-8"),
    unicode("sq", "utf-8"): unicode("商丘", "utf-8"),
    unicode("shiyan", "utf-8"): unicode("十堰", "utf-8"),
    unicode("suizhou", "utf-8"): unicode("随州", "utf-8"),
    unicode("shennongjia", "utf-8"): unicode("神农架", "utf-8"),
    unicode("sg", "utf-8"): unicode("韶关", "utf-8"),
    unicode("sw", "utf-8"): unicode("汕尾", "utf-8"),
    unicode("sansha", "utf-8"): unicode("三沙", "utf-8"),
    unicode("suining", "utf-8"): unicode("遂宁", "utf-8"),
    unicode("scnj", "utf-8"): unicode("内江", "utf-8"),
    unicode("sn", "utf-8"): unicode("山南", "utf-8"),
    unicode("sl", "utf-8"): unicode("商洛", "utf-8"),
    unicode("szs", "utf-8"): unicode("石嘴山", "utf-8"),
    unicode("shz", "utf-8"): unicode("石河子", "utf-8"),
    unicode("tj", "utf-8"): unicode("天津", "utf-8"),
    unicode("ty", "utf-8"): unicode("太原", "utf-8"),
    unicode("ts", "utf-8"): unicode("唐山", "utf-8"),
    unicode("tz", "utf-8"): unicode("台州", "utf-8"),
    unicode("ta", "utf-8"): unicode("泰安", "utf-8"),
    unicode("tongliao", "utf-8"): unicode("通辽", "utf-8"),
    unicode("tl", "utf-8"): unicode("铁岭", "utf-8"),
    unicode("th", "utf-8"): unicode("通化", "utf-8"),
    unicode("taizhou", "utf-8"): unicode("泰州", "utf-8"),
    unicode("tongling", "utf-8"): unicode("铜陵", "utf-8"),
    unicode("tianmen", "utf-8"): unicode("天门", "utf-8"),
    unicode("tunchang", "utf-8"): unicode("屯昌", "utf-8"),
    unicode("tr", "utf-8"): unicode("铜仁", "utf-8"),
    unicode("tc", "utf-8"): unicode("铜川", "utf-8"),
    unicode("tianshui", "utf-8"): unicode("天水", "utf-8"),
    unicode("tlf", "utf-8"): unicode("吐鲁番", "utf-8"),
    unicode("tac", "utf-8"): unicode("塔城", "utf-8"),
    unicode("tumushuke", "utf-8"): unicode("图木舒克", "utf-8"),
    unicode("wh", "utf-8"): unicode("武汉", "utf-8"),
    unicode("wz", "utf-8"): unicode("温州", "utf-8"),
    unicode("weihai", "utf-8"): unicode("威海", "utf-8"),
    unicode("wx", "utf-8"): unicode("无锡", "utf-8"),
    unicode("wuhu", "utf-8"): unicode("芜湖", "utf-8"),
    unicode("wuhai", "utf-8"): unicode("乌海", "utf-8"),
    unicode("wlcb", "utf-8"): unicode("乌兰察布", "utf-8"),
    unicode("wf", "utf-8"): unicode("潍坊", "utf-8"),
    unicode("wuzhou", "utf-8"): unicode("梧州", "utf-8"),
    unicode("wzs", "utf-8"): unicode("五指山", "utf-8"),
    unicode("wenchang", "utf-8"): unicode("文昌", "utf-8"),
    unicode("wanning", "utf-8"): unicode("万宁", "utf-8"),
    unicode("ws", "utf-8"): unicode("文山", "utf-8"),
    unicode("wn", "utf-8"): unicode("渭南", "utf-8"),
    unicode("wuwei", "utf-8"): unicode("武威", "utf-8"),
    unicode("wuzhong", "utf-8"): unicode("吴忠", "utf-8"),
    unicode("wujiaqu", "utf-8"): unicode("五家渠", "utf-8"),
    unicode("xa", "utf-8"): unicode("西安", "utf-8"),
    unicode("xm", "utf-8"): unicode("厦门", "utf-8"),
    unicode("xj", "utf-8"): unicode("乌鲁木齐", "utf-8"),
    unicode("xichang", "utf-8"): unicode("西昌", "utf-8"),
    unicode("xianyang", "utf-8"): unicode("咸阳", "utf-8"),
    unicode("xn", "utf-8"): unicode("西宁", "utf-8"),
    unicode("xz", "utf-8"): unicode("徐州", "utf-8"),
    unicode("xx", "utf-8"): unicode("新乡", "utf-8"),
    unicode("xt", "utf-8"): unicode("邢台", "utf-8"),
    unicode("xinzhou", "utf-8"): unicode("忻州", "utf-8"),
    unicode("xan", "utf-8"): unicode("兴安盟", "utf-8"),
    unicode("xlgl", "utf-8"): unicode("锡林郭勒", "utf-8"),
    unicode("xuancheng", "utf-8"): unicode("宣城", "utf-8"),
    unicode("xinyu", "utf-8"): unicode("新余", "utf-8"),
    unicode("xc", "utf-8"): unicode("许昌", "utf-8"),
    unicode("xy", "utf-8"): unicode("信阳", "utf-8"),
    unicode("xf", "utf-8"): unicode("襄阳", "utf-8"),
    unicode("xiaogan", "utf-8"): unicode("孝感", "utf-8"),
    unicode("xianning", "utf-8"): unicode("咸宁", "utf-8"),
    unicode("xiantao", "utf-8"): unicode("仙桃", "utf-8"),
    unicode("xiangtan", "utf-8"): unicode("湘潭", "utf-8"),
    unicode("xiangxi", "utf-8"): unicode("湘西", "utf-8"),
    unicode("yuncheng", "utf-8"): unicode("运城", "utf-8"),
    unicode("yt", "utf-8"): unicode("烟台", "utf-8"),
    unicode("yangshuo", "utf-8"): unicode("阳朔", "utf-8"),
    unicode("yb", "utf-8"): unicode("宜宾", "utf-8"),
    unicode("yl", "utf-8"): unicode("榆林", "utf-8"),
    unicode("yinchuan", "utf-8"): unicode("银川", "utf-8"),
    unicode("yz", "utf-8"): unicode("扬州", "utf-8"),
    unicode("yiwu", "utf-8"): unicode("义乌", "utf-8"),
    unicode("yy", "utf-8"): unicode("岳阳", "utf-8"),
    unicode("yq", "utf-8"): unicode("阳泉", "utf-8"),
    unicode("yk", "utf-8"): unicode("营口", "utf-8"),
    unicode("yanbian", "utf-8"): unicode("延边", "utf-8"),
    unicode("yich", "utf-8"): unicode("伊春", "utf-8"),
    unicode("yancheng", "utf-8"): unicode("盐城", "utf-8"),
    unicode("yingtan", "utf-8"): unicode("鹰潭", "utf-8"),
    unicode("yichun", "utf-8"): unicode("宜春", "utf-8"),
    unicode("yc", "utf-8"): unicode("宜昌", "utf-8"),
    unicode("yiyang", "utf-8"): unicode("益阳", "utf-8"),
    unicode("yongzhou", "utf-8"): unicode("永州", "utf-8"),
    unicode("yj", "utf-8"): unicode("阳江", "utf-8"),
    unicode("yf", "utf-8"): unicode("云浮", "utf-8"),
    unicode("yulin", "utf-8"): unicode("玉林", "utf-8"),
    unicode("ya", "utf-8"): unicode("雅安", "utf-8"),
    unicode("yx", "utf-8"): unicode("玉溪", "utf-8"),
    unicode("yanan", "utf-8"): unicode("延安", "utf-8"),
    unicode("ys", "utf-8"): unicode("玉树", "utf-8"),
    unicode("yili", "utf-8"): unicode("伊犁", "utf-8"),
    unicode("zh", "utf-8"): unicode("珠海", "utf-8"),
    unicode("zz", "utf-8"): unicode("郑州", "utf-8"),
    unicode("zunyi", "utf-8"): unicode("遵义", "utf-8"),
    unicode("zs", "utf-8"): unicode("中山", "utf-8"),
    unicode("zj", "utf-8"): unicode("镇江", "utf-8"),
    unicode("zhoushan", "utf-8"): unicode("舟山", "utf-8"),
    unicode("zhangzhou", "utf-8"): unicode("漳州", "utf-8"),
    unicode("zjk", "utf-8"): unicode("张家口", "utf-8"),
    unicode("zb", "utf-8"): unicode("淄博", "utf-8"),
    unicode("zaozhuang", "utf-8"): unicode("枣庄", "utf-8"),
    unicode("zk", "utf-8"): unicode("周口", "utf-8"),
    unicode("zmd", "utf-8"): unicode("驻马店", "utf-8"),
    unicode("zhuzhou", "utf-8"): unicode("株洲", "utf-8"),
    unicode("zjj", "utf-8"): unicode("张家界", "utf-8"),
    unicode("zhanjiang", "utf-8"): unicode("湛江", "utf-8"),
    unicode("zq", "utf-8"): unicode("肇庆", "utf-8"),
    unicode("zg", "utf-8"): unicode("自贡", "utf-8"),
    unicode("zy", "utf-8"): unicode("资阳", "utf-8"),
    unicode("zt", "utf-8"): unicode("昭通", "utf-8"),
    unicode("zhangye", "utf-8"): unicode("张掖", "utf-8"),
    unicode("zw", "utf-8"): unicode("中卫", "utf-8"),
    unicode("bn", "utf-8"): unicode("西双版纳", "utf-8"),
    unicode("hk", "utf-8"): unicode("香港", "utf-8"),
    unicode("am", "utf-8"): unicode("澳门", "utf-8")
}


def version_transform(df):
    '''
    将设备信号由数字转换为字符串
    :return: 待转换的dataframe
    '''
    if str(df['driver_version']).find('_') < 0:
        return "other"
    else:
        tags = str(df['driver_version']).split('_')
        if tags[1] == "1":
            return "And" + tags[0]
        elif tags[1] == "2":
            return "iOS" + tags[0]
        else:
            return "other"


def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    球面距离计算
    """

    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r * 1000


class ZeroDistStat:
    def __init__(self, db_uri, logger = None):
        self.db_uri = db_uri
        if logger is None:
            self.log = get_logger(NAME)
        else:
            self.log = logger

    def __save(self, df, table_name):
        '''
        将0公里订单统计数据更新至mysql，已存在的日期记录将被覆盖（确保订单信息是最新的，因为订单会被update）

        :param df:  待更新的dataframe
        :param table_name:  表名
        :return:  从hive转换出来的pandas dataframe
        '''
        engine = create_engine(self.db_uri, echo=False)
        # 获取日期信息
        dates = ','.join([str(x) for x in list(df.dt.unique())])
        # 删除重复日期的记录
        if engine.dialect.has_table(engine, table_name):
            self.log.info("table already exist, drop data the same dates={dates}".format(dates=dates))
            engine.execute("delete from {tbl} where dt in ({dates})".format(tbl=table_name, dates=dates))
        # 添加至数据库
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        self.log.info("save df[{name}] size={sz}".format(name=table_name, sz=df.shape[0]))

    def __select(self, sql):
        '''
        执行查询SQL
        :param sql:
        :return:
        '''
        engine = create_engine(self.db_uri, echo=False)
        df = pd.read_sql(sql, engine)
        return df

    def __loadBaseDataFromHive(self, execute_date):
        '''
        从hive中获取0公里统计的基础数据，使用service_order + service_order_ext
        :param sparkMaster:  spark提交的master
        :param execute_date:  执行日期
        :return:
        '''

        # 为避免端口冲突，随机设置spark端口
        port = 4040 + random.randint(100, 1000)

        spark = SparkSession.builder \
            .master("local[4]") \
            .config("spark.ui.port", str(port)) \
            .appName("ZeroDistanceStat") \
            .enableHiveSupport() \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.shuffle.partitions", "100") \
            .getOrCreate()

        start, end = arrow.get(execute_date, "YYYYMMDD").span("day")

        sql = """
            SELECT
            a.service_order_id, a.city, a.status, a.system_distance, a.dependable_distance, b.driver_version, a.driver_id, a.start_longitude, a.start_latitude, a.expect_end_longitude, a.expect_start_latitude, a.start_time, a.end_time
            FROM
            ods. service_order a left join (select distinct service_order_id, driver_version from  ods. service_order_ext where dt={dt}) b on a.service_order_id=b.service_order_id
            WHERE
            a.dt={dt} and
            b.driver_version !='' and
            a.status=7 and
            a.passenger_name not like '%测试%' and
            a.start_time between UNIX_TIMESTAMP('{start}') and UNIX_TIMESTAMP('{end}')
                """.format(dt=execute_date, start=start.format('YYYY-MM-DD HH:mm:ss'),
                           end=end.format('YYYY-MM-DD HH:mm:ss'))

        # 导出为pandas
        self.log.info(sql)
        baseDF = spark.sql(sql).toPandas()
        spark.stop()
        del spark

        # 区分被分控的订单
        baseDF["is_rc"] = np.where(baseDF.service_order_id.isin(self.__getRcOrderIds(execute_date)), True, False)

        self.log.info("read data from hive complete! baseDF={base_sz}".format(base_sz=baseDF.shape[0]))

        return baseDF

    def __getRcOrderIds(self, dt):
        '''
        从mysql中加载最近10天的风控订单数据，风控数据每天上午10点ETL至mysql
        :param dt:  加载的日期
        :return:  风控订单的id集合
        '''

        ten_days_ago = arrow.get(dt, "YYYYMMDD").replace(days=-10).format("YYYYMMDD")

        from sqlalchemy import create_engine
        engine = create_engine(self.db_uri, echo=False)
        table_name = "rc_order"
        if engine.dialect.has_table(engine, table_name):
            sql = "select service_order_id from {tbl} where is_rc=\'T\' and dt>{offset_day}" \
                .format(tbl=table_name, offset_day=ten_days_ago)
            self.log.info(sql)
            df = pd.read_sql_query(sql, con=engine)
            self.log.info("get rc_orders={sz}".format(sz=df.shape[0]))
            return set(df.service_order_id.values)
        else:
            self.log.info("no cache rc_orders")
            return set()

    def __summaryStatRecently(self, execute_date, use_rc=False, preDays=10):
        '''
        最近10天的概要信息统计，使用sparkSQL直接统计
        :param execute_date:
        :param use_rc:
        :param preDays:
        :return:
        '''

        start_time = time.time()

        port = 4040 + random.randint(100, 1000)

        spark = SparkSession.builder \
            .master("local[4]") \
            .config("spark.ui.port", str(port)) \
            .appName("ZeroDistanceStat") \
            .enableHiveSupport() \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "100") \
            .getOrCreate()
        try:
            # 提取0公里订单
            end_dt = arrow.get(execute_date, "YYYYMMDD")
            start_dt = end_dt.replace(days=-9)
            dates = ','.join(map(lambda x: x.strftime("%Y%m%d"),
                                 pd.date_range(start_dt.format("YYYY-MM-DD"), end_dt.format("YYYY-MM-DD"))))
            sql = """
    SELECT
    dt, service_order_id, system_distance, dependable_distance, start_time
    FROM (
        SELECT
        a.dt, a.service_order_id, a.system_distance, a.dependable_distance, a.start_time,
        ROW_NUMBER() OVER (DISTRIBUTE BY a.service_order_id SORT BY a.update_time DESC) rank
        FROM
        ods.service_order a left join (select distinct service_order_id, driver_version from  ods.service_order_ext where dt in ({dates})) b on a.service_order_id=b.service_order_id
        WHERE
        a.dt in ({dates}) and
        b.driver_version !='' and
        a.status=7 and
        a.passenger_name not like '%测试%' and
        a.start_time between UNIX_TIMESTAMP('{start}') and UNIX_TIMESTAMP('{end}')
    ) tmp
    WHERE tmp.rank=1
          """.format(dates=dates,
                     start=start_dt.span("day")[0].format('YYYY-MM-DD HH:mm:ss'),
                     end=end_dt.span("day")[1].format('YYYY-MM-DD HH:mm:ss'))
            self.log.info(sql)
            orderDF = spark.sql(sql).toPandas()

            # 转换dt为按照start_time划分
            orderDF['dt'] = orderDF.start_time.map(lambda x: int(arrow.get(x).to('Asia/Shanghai').format("YYYYMMDD")))
            orderDF = orderDF.loc[orderDF['dt'].isin(map(lambda x: int(x), dates.split(",")))]

            # 如果需要，先从mysql加载风控数据
            if use_rc:
                riskDF = self.__select("select service_order_id from rc_order".format(execute_date))
                orderDF['is_rc'] = np.where(orderDF.service_order_id.isin(riskDF.service_order_id.values), True, False)
            else:
                orderDF['is_rc'] = True

            # summary统计
            no_rc = orderDF.is_rc == False
            is_sys_0 = orderDF.system_distance == 0
            is_adjust_0 = orderDF.dependable_distance == 0

            # 概要信息统计
            outDF = pd.DataFrame()
            outDF['total'] = orderDF.groupby('dt')['service_order_id'].count()
            outDF.sort_index(ascending=True)
            outDF.insert(0, "dt", outDF.index.values)
            outDF.index.names = [unicode("日期", "utf-8")]
            # 0公里
            outDF['sys_zero'] = orderDF[is_sys_0].groupby('dt')['service_order_id'].count()
            # 0公里非风控
            outDF['no_rc_zero'] = orderDF[is_sys_0 & no_rc].groupby('dt')['service_order_id'].count() if use_rc else 0
            if use_rc:
                # 低消数据
                outDF['adjust_zero'] = orderDF[no_rc & is_sys_0 & is_adjust_0].groupby('dt')['service_order_id'].count()
                # 非低消：0公里 - 低消
                outDF['ext_zero'] = outDF.no_rc_zero - outDF.adjust_zero
            else:
                # 低消数据
                outDF['adjust_zero'] = orderDF[is_sys_0 & is_adjust_0].groupby('dt')['service_order_id'].count()
                # 非低消：0公里 - 低消
                outDF['ext_zero'] = outDF.sys_zero - outDF.adjust_zero

            # rate caculation
            outDF["sys_zero_rate"] = np.round(outDF.sys_zero / outDF.total, 4) * 1000
            outDF["no_rc_rate"] = np.round(outDF.no_rc_zero / outDF.total, 4) * 1000
            outDF["adjust_zero_rate"] = np.round(outDF.adjust_zero / outDF.total, 4) * 1000
            outDF["ext_zero_rate"] = np.round(outDF.ext_zero / outDF.total, 4) * 1000

            outDF = outDF.rename(columns={
                'dt': unicode("日期", "utf-8"),
                'sys_zero': unicode("0公里订单", "utf-8"),
                'no_rc_zero': unicode("0公里订单(非风控)", "utf-8"),
                'adjust_zero': unicode("低消数据", "utf-8"),
                'ext_zero': unicode("非低消数据", "utf-8"),
                'sys_zero_rate': unicode("0公里占比", "utf-8"),
                'no_rc_rate': unicode("0公里(非风控)占比", "utf-8"),
                'adjust_zero_rate': unicode("低消数据占比", "utf-8"),
                'ext_zero_rate': unicode("非低消数据占比", "utf-8"),
                'total': unicode("总订单", "utf-8")
            })

            self.log.info("总报表统计 elapsed={t}, size={sz}".format(t=str(time.time() - start_time), sz=outDF.shape[0]))
            return outDF
        except Exception, e:
            self.log.exception("summary statistics failed, return empty dataframe as default")
            return pd.DataFrame()
        finally:
            if spark is not None:
                spark.stop()
                del spark

    def __version_stats(self, both_0_DF):
        '''
        按设备类型统计
        '''

        s = time.time()

        both_0_DF['version'] = both_0_DF.apply(version_transform, axis=1)
        outputDF = pd.DataFrame()
        outputDF[unicode('个数', "utf-8")] = both_0_DF['version'].value_counts().sort_index()
        outputDF.index.rename(unicode("设备型号", "utf-8"))
        outputDF.insert(0, unicode("设备型号", "utf-8"), outputDF.index.values)

        self.log.info("设备型号 time={t}, size={sz}".format(t=str(time.time() - s), sz=outputDF.shape[0]))
        return outputDF

    def __city_stats(self, both_0_DF):
        s = time.time()

        both_0_DF['city_trans'] = both_0_DF.apply(lambda df: CITY_DICT.get(df['city'], df['city']), axis=1)

        outputDF = pd.DataFrame()
        outputDF[unicode('个数', "utf-8")] = both_0_DF['city_trans'].value_counts()
        outputDF.index.rename(unicode('城市', "utf-8"))
        outputDF.insert(0, unicode("城市", "utf-8"), outputDF.index.values)
        # outputDF.to_csv("city_{dt}.csv".format(dt=dt))
        self.log.info("城市 time={t}, size={sz}".format(t=str(time.time() - s), sz=outputDF.shape[0]))

        return outputDF

    def __driver_stats(self, dt, both_0_DF, baseDF):
        '''
        司机状态统计，当天的司机状态将会存入mysql，供历史累加使用

        :param dt: 执行日期
        :param both_0_DF:  纯0公里datafrane
        :param baseDF:  基础数据dataframe
        :return:
        '''

        s = time.time()

        # init database
        engine = create_engine(self.db_uri)
        table_name = "driver_order_hist"
        if not engine.dialect.has_table(engine, table_name):
            self.log.debug("table {tbl} not exists, create it!".format(tbl=table_name))
            metadata = MetaData(engine)  # Create a table with the appropriate Columns
            Table(table_name,
                  metadata,
                  Column('dt', String),
                  Column('driver_id', String),
                  Column('zero_cnt', Integer),
                  Column('total_cnt', Integer))
            metadata.create_all()

        # 防止重复执行当天数据,先清除再统计
        engine.execute("delete from {tbl} where dt={dt}".format(tbl=table_name, dt=dt))
        # 从数据库查出所有记录
        histDF = pd.read_sql_table(table_name, engine)

        # 本日异常订单个数
        outputDF = pd.DataFrame()
        outputDF[unicode('本日异常订单个数', "utf-8")] = both_0_DF['driver_id'].value_counts()
        outputDF.index.rename(unicode("司机id", "utf-8"))
        outputDF.insert(0, unicode("司机id", "utf-8"), outputDF.index.values)

        # 本日总订单数
        outputDF[unicode('本日总订单数', "utf-8")] = baseDF.groupby("driver_id")['service_order_id'].count()

        # 计算累计上板次数
        sql = "SELECT driver_id, count(DISTINCT dt) FROM {tbl} where zero_cnt>0 group by driver_id" \
            .format(tbl=table_name)
        outputDF[unicode('累计上板次数', "utf-8")] = pd.read_sql_query(sql, engine, index_col='driver_id')

        # 累计订单数
        outputDF[unicode('累计订单数', "utf-8")] = histDF.groupby("driver_id")['total_cnt'].sum() \
                                              + outputDF[unicode('本日总订单数', "utf-8")]

        # 更新至数据库
        dumpToSQL = pd.DataFrame(index=outputDF.index)
        dumpToSQL['dt'] = dt
        dumpToSQL['driver_id'] = outputDF.index
        dumpToSQL['zero_cnt'] = outputDF[unicode('本日异常订单个数', "utf-8")]
        dumpToSQL['total_cnt'] = outputDF[unicode('本日总订单数', "utf-8")]
        histDF = pd.concat([histDF, dumpToSQL], ignore_index=True)
        histDF.to_sql(table_name, engine, index=False, if_exists="replace", chunksize=1000)

        self.log.info("司机 time={t}, size={sz}".format(t=str(time.time() - s), sz=outputDF.shape[0]))
        return outputDF

    def __distance_diff_stats(self, both_0_DF):
        s = time.time()

        idx = []
        diff = []
        for index, row in both_0_DF.iterrows():
            distance = haversine(row.start_longitude, row.start_latitude, row.expect_end_longitude,
                                 row.expect_start_latitude)
            idx.append(index)
            diff.append(np.round(distance / 1000, 1))

        bins = [0, 0.2, 0.5, 0.8, 1, 1.5, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10000000]
        labels = ["0", "0.2", "0.5", "0.8", "1", "1.5", "2", "3", "4", "5", "6", "7", "8", "9", "10", "10+"]

        tmpDF = pd.DataFrame(data=diff, index=idx, columns=['diff'])
        tmpDF[unicode('上车距离', "utf-8")] = pd.cut(tmpDF['diff'], bins, labels=labels[1:])
        tmpDF[unicode('上车距离', "utf-8")].value_counts().sort_index()

        outputDF = pd.DataFrame()
        outputDF[unicode('数量', "utf-8")] = tmpDF[unicode('上车距离', "utf-8")].value_counts().sort_index()
        outputDF.index.rename(u"上车距离（公里）")
        outputDF.insert(0, unicode("上车距离（公里）", "utf-8"), outputDF.index.values)

        self.log.info("上车距离 time={t}, size={sz}".format(t=str(time.time() - s), sz=outputDF.shape[0]))
        return outputDF

    def __hour_stats(self, both_0_DF):
        s = time.time()

        both_0_DF['ts_start_time'] = pd.to_datetime(both_0_DF['start_time'], unit='s', utc=True) + pd.Timedelta(hours=8)
        ts_series = both_0_DF.groupby("ts_start_time").size().resample("1H").sum()
        outputDF = pd.DataFrame(data=ts_series.values, index=range(0, 24), columns=[unicode("数量", "utf-8")])
        outputDF.index.rename(unicode("小时", "utf-8"))
        outputDF.insert(0, unicode("小时", "utf-8"), outputDF.index.values)

        self.log.info("小时分布 time={t}, size={sz}".format(t=str(time.time() - s), sz=outputDF.shape[0]))
        return outputDF

    def __service_interval_stats(self, both_0_DF):
        s = time.time()

        both_0_DF['service_interval'] = (both_0_DF.end_time - both_0_DF.start_time) / 60

        bins = range(0, 21)
        labels = []
        for i in bins:
            labels.append(str(i))
        bins.append(100000)
        labels.append("20+")

        both_0_DF[unicode('订单时长分布', "utf-8")] = pd.cut(both_0_DF.service_interval, bins, labels=labels[1:])

        outputDF = pd.DataFrame()
        outputDF[unicode("数量", "utf-8")] = both_0_DF[unicode('订单时长分布', "utf-8")].value_counts().sort_index()
        outputDF.index.rename(unicode("时长分布(min)", "utf-8"))
        outputDF.insert(0, unicode("时长分布(min)", "utf-8"), outputDF.index.values)

        self.log.info("订单时长分布 time={t}, size={sz}".format(t=str(time.time() - s), sz=outputDF.shape[0]))
        return outputDF

    def generateReport(self, dt, use_rc, dump_dir):
        '''
        进行0公里订单各明细项统计，生成excel报表
        :param dt:  统计日期
        :param use_rc:  是否区分风控数据（若是会生成2分报告）
        :param dump_dir:  导出文件的保持路径
        :return:
        '''

        # step 1: 总体统计（报表首页），历史数据保存至mysql：zero_summary
        summary_result_df = self.__summaryStatRecently(dt, use_rc=use_rc)
        if summary_result_df.shape[0] == 0:
            self.log.error("summary_result_df is empty")
            exit(-1)
        dump2MysqlDF = summary_result_df.copy()
        # 修正标题为英文的，否则存mysql有问题
        dump2MysqlDF.columns = ['dt', 'total', 'zero_cnt', 'zero_cnt_no_rc', 'dual_zero_cnt', 'non_dual_zero',
                                'zero_rate',
                                'zero_no_rc_rate', 'dual_zero_rate', 'non_dual_zero_rate']
        self.__save(dump2MysqlDF, "zero_summary")
        # 日期格式转换，确保excel中显示的是正确格式
        summary_result_df.index = summary_result_df.index.map(
            lambda x: str(x)[0:4] + "-" + str(x)[4:6] + "-" + str(x)[6:8])
        summary_result_df[unicode("日期", "utf-8")] = summary_result_df.index

        #---------------------------------------------------

        # 从hive加载0公里统计的基础数据
        baseDF = self.__loadBaseDataFromHive(dt)
        if baseDF.shape[0] == 0:
            self.log.error("baseDF is empty")
            exit(-1)
        # dataframe 判断条件
        no_rc = baseDF.is_rc == False  # 是否风控
        is_sys_0 = baseDF.system_distance == 0  # 系统记录里程为0
        is_adjust_0 = baseDF.dependable_distance == 0  # 调整后的里程为0
        if use_rc:  # 纯低消数据（不含风控）
            pure_0_DF = baseDF[no_rc & is_sys_0 & is_adjust_0]
        else:
            pure_0_DF = baseDF[is_sys_0 & is_adjust_0]
        self.log.info("风控={rc} 0公里={sys_0} 双0={dual_0} 双0（不含风控）={all}"
                      .format(rc=baseDF[~no_rc].shape[0], sys_0=baseDF[is_sys_0].shape[0],
                              dual_0=baseDF[is_sys_0 & is_adjust_0].shape[0], all=pure_0_DF.shape[0]))
        pure_0_DF['dt'] = int(dt)
        self.__save(pure_0_DF, "zero_detail")

        # step 2: 按设备型号统计
        version_result_df = self.__version_stats(pure_0_DF)
        if version_result_df.shape[0] == 0:
            self.log.error("version_result_df is empty")
            exit(-1)
        dump2MysqlDF = version_result_df.copy()
        dump2MysqlDF['dt'] = int(dt)
        # 修正标题为英文的，否则存mysql有问题
        dump2MysqlDF.columns = ['driver_version', 'cnt', 'dt']
        self.__save(dump2MysqlDF, "zero_version")

        # step 3: 按城市统计
        city_result_df = self.__city_stats(pure_0_DF)
        if city_result_df.shape[0] == 0:
            self.log.error("city_result_df is empty")
            exit(-1)
        dump2MysqlDF = city_result_df.copy()
        dump2MysqlDF['dt'] = int(dt)
        # 修正标题为英文的，否则存mysql有问题
        dump2MysqlDF.columns = ['city', 'cnt', 'dt']
        self.__save(dump2MysqlDF, "zero_city")

        # step 4: 按司机统计
        driver_result_df = self.__driver_stats(dt, pure_0_DF, baseDF)
        if driver_result_df.shape[0] == 0:
            self.log.error("driver_result_df is empty")
            exit(-1)
        dump2MysqlDF = driver_result_df.copy()
        dump2MysqlDF['dt'] = int(dt)
        print dump2MysqlDF.head()
        # 修正标题为英文的，否则存mysql有问题
        dump2MysqlDF.columns = ['driver_id', 'today_zero_cnt', 'today_cnt', 'acc_zero_days', 'total_cnt', 'dt']
        print dump2MysqlDF.dtypes
        print dump2MysqlDF.head()

        self.__save(dump2MysqlDF, "zero_driver")

        # step 5: 按上下车地点统计
        distance_diff_result_df = self.__distance_diff_stats(pure_0_DF)
        if distance_diff_result_df.shape[0] == 0:
            self.log.error("distance_diff_result_df is empty")
            exit(-1)
        dump2MysqlDF = distance_diff_result_df.copy()
        dump2MysqlDF['dt'] = int(dt)
        # 修正标题为英文的，否则存mysql有问题
        dump2MysqlDF.columns = ['start_pos_diff', 'cnt', 'dt']
        self.__save(dump2MysqlDF, "zero_pos_diff")

        # step 6: 按小时分布
        hour_result_df = self.__hour_stats(pure_0_DF)
        if hour_result_df.shape[0] == 0:
            self.log.error("hour_result_df is empty")
            exit(-1)
        dump2MysqlDF = hour_result_df.copy()
        dump2MysqlDF['dt'] = int(dt)
        # 修正标题为英文的，否则存mysql有问题
        dump2MysqlDF.columns = ['hour', 'cnt', 'dt']
        self.__save(dump2MysqlDF, "zero_hour_distribute")

        # step 7: 服务时长分布
        service_interval_result_df = self.__service_interval_stats(pure_0_DF)
        if service_interval_result_df.shape[0] == 0:
            self.log.error("service_interval_result_df is empty")
            exit(-1)
        dump2MysqlDF = service_interval_result_df.copy()
        dump2MysqlDF['dt'] = int(dt)
        # 修正标题为英文的，否则存mysql有问题
        dump2MysqlDF.columns = ['duration', 'cnt', 'dt']
        self.__save(dump2MysqlDF, "zero_order_duration")

        # 导出到excel
        if use_rc:
            excel_name = "0_distance_stat_exclude_RiskCtrl_{dt}.xlsx".format(dt=dt)
        else:
            excel_name = "0_distance_stat_{dt}.xlsx".format(dt=dt)
        excel_path = op.join(dump_dir, excel_name)
        with pd.ExcelWriter(excel_path, options={'encoding': 'utf-8', 'engine': 'xlsxwriter'}) as writer:
            summary_result_df.to_excel(writer, sheet_name=unicode('0km', "utf-8"), index=False)
            version_result_df.to_excel(writer, sheet_name=unicode('设备版本分布', "utf-8"), index=False)
            city_result_df.to_excel(writer, sheet_name=unicode('城市分布', "utf-8"), index=False)
            driver_result_df.to_excel(writer, sheet_name=unicode('司机分布', "utf-8"), index=False)
            distance_diff_result_df.to_excel(writer, sheet_name=unicode('上下车位置分布', "utf-8"), index=False)
            hour_result_df.to_excel(writer, sheet_name=unicode('时间分布', "utf-8"), index=False)
            service_interval_result_df.to_excel(writer, sheet_name=unicode('订单时长分布', "utf-8"), index=False)
        self.log.info("write to excel={name}".format(name=excel_name))

        return excel_path
