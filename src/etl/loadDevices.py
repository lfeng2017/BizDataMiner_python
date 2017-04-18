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

title = ['device_id','user_id','status','user_type','update_time','app_version','os_type','os_version','mac_address','device_type','device_serialno','exception_flag','os_name','app_series','dt']


@click.command()
@click.option('--date', default=yesterday.format("YYYYMMDD"), help='开始任务的日期')
def main(date):

    file = os.path.join(cfg.setting.TSV_PATH, "{name}_{dt}.tsv".format(name=cfg.setting.DEVICE_FNAME, dt=date))
    df = pd.read_csv(file, delimiter='\t', names=title, encoding="utf-8")
    df['dt'] = int(date)
    print df.shape
    engine = create_engine(cfg.setting.DB_URL, echo=False)
    table = "ods_device"
    df.to_sql(name=table, con=engine, if_exists='replace', index=False)
    print "all is done"

if __name__ == '__main__':
    main()
