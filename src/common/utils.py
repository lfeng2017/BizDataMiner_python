# -*- coding: utf-8 -*-

'''
一些工具模块的封装
'''

import os.path as op
import logging
import subprocess


NAME = op.splitext(op.basename(__file__))[0]

def get_logger(name=None, log_level=logging.DEBUG):
    '''
    获取utils模块的logger handler

    :param log_level:  log等级
    :return:  logger
    '''

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers or len(logger.handlers) == 0:
        if log_level is None:
            logger.handlers = [logging.NullHandler()]
            return logger
        else:
            ch = logging.StreamHandler()

        ch.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s [utils] : %(message)s", "%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)
        logger.handlers = [ch]

    return logger


def runShell(cmd):
    '''
    使用子进程执行shell脚本

    :param cmd: 待执行的命令
    :return:
        retval => 状态码
        messages => 命令的输出字符串
    '''

    log = get_logger(NAME)

    log.info("CMD => " + cmd)

    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    messages = "\n".join([line.strip("\n") for line in proc.stdout.readlines()])
    retval = proc.wait()

    return retval, messages


if __name__ == "__main__":
    code, msg = runShell("rm ttt")
    print "ret:", code
    print msg
