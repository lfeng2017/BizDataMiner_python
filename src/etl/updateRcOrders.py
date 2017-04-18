# -*- coding:utf-8 -*-
import os
import sys

import arrow
import click
import pandas as pd
from sqlalchemy import create_engine

parDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parDir)
import src.common.logger as logger
import config as cfg

NAME = (os.path.splitext(os.path.basename(__file__))[0])

log = logger.init(NAME)

yesterday = arrow.now().replace(days=-1)

rc_title = ["订单号", "城市", "司机id", "用户id", "金额", "创建日期", "完成日期", "车型", "租赁公司", "产品类型", "计费里程", "计费时长", "系统记录里程",
            "司机手动录入公里", "系统记录时长", "是否风控"]
rc_title_en = ["service_order_id", "city", "driver_id", "user_id", "price", "create_time", "complete_time", "car_type",
               "company", "product_type", "charge_distance", "charge_time_length", "system_distance", "mileage",
               "actual_time_length", "is_rc"]


@click.command()
@click.option('--date', default=yesterday.format("YYYYMMDD"), help='开始任务的日期')
@click.option('--test', default=False, help='drop db anyway')
def update_rc_orders(date, test):
    '''
    '定时更新风控订单数据'
    :param dt: 更新的日期
    :return:
    '''

    rc_file = os.path.join(cfg.setting.TSV_PATH, "{name}_{dt}.txt".format(name=cfg.setting.RC_FNAME, dt=date))
    df = pd.read_csv(rc_file, delimiter='\t', names=rc_title_en, index_col=0,
                     parse_dates=['create_time', 'complete_time'], encoding="utf-8")
    df = df[df.is_rc == "T"]
    df['dt'] = int(date)
    today_len = df.shape[0]

    # 从数据库加载原有风控记录,与最新的进行合并
    engine = create_engine(cfg.setting.DB_URL, echo=False)
    table_name = "rc_order"
    if engine.dialect.has_table(engine, table_name):
        oldDF = pd.read_sql_table(table_name, con=engine, index_col='service_order_id',
                                  parse_dates=['create_time', 'complete_time'])
        history_len = oldDF.shape[0]
        oldDF.drop(oldDF[oldDF.index.isin(df.index)].index, inplace=True)
        outDF = pd.concat([oldDF, df])
        new_len = outDF.shape[0]
        log.info("update rc_orders today={today} history={history} update={new}"
                 .format(today=today_len, history=history_len, new=new_len))
    else:
        outDF = df
        log.info("create rc_orders size={sz}".format(sz=outDF.shape[0]))

    # 更新至数据库
    outDF.to_sql(name='rc_order', con=engine, if_exists='replace', index=True)


if __name__ == '__main__':
    update_rc_orders()
