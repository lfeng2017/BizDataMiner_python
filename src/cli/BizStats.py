# -*- coding:utf-8 -*-

'''

python /var/tmp/crontab/jobs/BizDataMiner/src/main.py DumpReport zeroDistReport -E prod [ --execute_date 20170329 ] [ --send_email ]

python /var/tmp/crontab/jobs/BizDataMiner/src/main.py DumpReport suspOrderReport -E prod [ --execute_date 20170329 ] [ --send_email ]

'''

import os.path as op
import sys
import time
import arrow
import pandas as pd
import yagmail
from cement.core.controller import CementBaseController, expose

sys.path.append(op.dirname(op.dirname(op.abspath(__file__))))
from common.utils import runShell
from core.ZeroDistStat import ZeroDistStat
from core.SuspOrderStat import SuspOrdeStat


class ReportController(CementBaseController):
    """
    进行业务数据统计，生成报表，包括：
    1、0公里报表
    2、疑异订单报表
    """

    class Meta:
        label = 'DumpReport'
        description = "执行统计任务，导出报表"
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--execute_date'],
             dict(help='执行日期（默认昨天）', type=str,
                  default=arrow.get().to(tz="Asia/Shanghai").replace(days=-1).format("YYYYMMDD"),
                  action='store')),
            (['--use_rc'], dict(help='是否使用风控数据', action='store_true')),
            (['--send_email'], dict(help='是否发送邮件', action='store_true')),
        ]

    @expose(hide=True)
    def default(self):
        self.app.log.info("no command specify, only show input parameters")
        self.app.log.info(self.app.pargs)

    @expose(help="生成0公里统计报表")
    def zeroDistReport(self):
        log = self.app.log

        execute_date = self.app.pargs.execute_date
        use_rc = self.app.pargs.use_rc
        send_email = self.app.pargs.send_email

        db_url = self.app.cfg['db_yongche_url']
        excel_out_dir = self.app.cfg['outfile']

        # 构造统计类对象
        stat = ZeroDistStat(db_url, logger=self.app.log)

        log.info("begin to generate ZeroDistStat excel, dt={dt} isUseRc={rc} db_uri={db}"
                 .format(dt=execute_date, rc=use_rc, db=db_url))
        start_time = time.time()

        # 生成的excel缓存
        excel_list = []

        # 统计当天的不适用风控数据
        outfile = stat.generateReport(execute_date, use_rc=False, dump_dir=excel_out_dir)
        excel_list.append(outfile)
        log.info("ZeroDistStat without RickControl generate complete")

        if use_rc:
            # 使用1天前的数据统计风控的结果
            date_pre1 = arrow.get(execute_date, "YYYYMMDD").replace(days=-1).format("YYYYMMDD")
            outfile_rc = stat.generateReport(date_pre1, use_rc=True, dump_dir=excel_out_dir)
            excel_list.append(outfile_rc)
            log.info("ZeroDistStat with RickControl generate complete")

        if send_email:
            mail_from = self.app.cfg['mail_from']
            mail_psw = self.app.cfg['mail_psw']
            mail_host = self.app.cfg['mail_host']
            mail_port = self.app.cfg['mail_port']
            mail_to = self.app.cfg['zero_dist_maillist'].split(",")
            # 构造邮件对象
            mail = yagmail.SMTP(user=mail_from, password=mail_psw, host=mail_host, port=mail_port)
            # 发送邮件
            mail.send(mail_to,
                      subject="0_distance_{dt}".format(dt=execute_date),
                      contents=u"dasboard：http://10.0.11.91:8000/login, 需要开通帐号请与我联系，谢谢".encode("utf-8"),
                      attachments=excel_list)
            log.info("send excel={name} to {emails}".format(name=excel_list, emails=mail_to))
        else:
            log.info("do not send mail")

        log.info("job=ZeroDistStat excel is done, elasped:{t}mins".format(t=(time.time() - start_time) / 60.0))

    @expose(help="疑异订单统计报表")
    def suspOrderReport(self):
        log = self.app.log

        execute_date = self.app.pargs.execute_date
        send_email = self.app.pargs.send_email

        db_url = self.app.cfg['db_yongche_url']
        driver_track_uri = self.app.cfg['driver_track_uri']
        excel_out_dir = self.app.cfg['outfile']

        # 构造统计类对象
        stat = SuspOrdeStat(db_url, driver_track_uri, logger=self.app.log)

        log.info("begin to generate SuspOrdeStat excel, dt={dt} db_uri={db}".format(dt=execute_date, db=db_url))
        start_time = time.time()

        # 生成的excel缓存
        excel_list = []

        # 统计当天的不适用风控数据
        outfile = stat.generateReport(execute_date, dump_dir=excel_out_dir)
        excel_list.append(outfile)
        log.info("ZeroDistStat without RickControl generate complete")

        if send_email:
            mail_from = self.app.cfg['mail_from']
            mail_psw = self.app.cfg['mail_psw']
            mail_host = self.app.cfg['mail_host']
            mail_port = self.app.cfg['mail_port']
            mail_to = self.app.cfg['susp_order_maillist'].split(",")

            subject = unicode("里程疑异订单", "utf-8") + "_{dt}".format(dt=execute_date)
            # 构造邮件对象
            mail = yagmail.SMTP(user=mail_from, password=mail_psw, host=mail_host, port=mail_port)
            # 发送邮件
            mail.send(mail_to,
                      subject=subject, contents=u"详情见附件(已添加设备信息)".encode("utf-8"),
                      attachments=excel_list)
            log.info("send excel={name} to {emails}".format(name=excel_list, emails=mail_to))
        else:
            log.info("do not send mail")

        log.info("job=SuspOrdeStat is done, elasped:{t}mins".format(t=(time.time() - start_time) / 60.0))
