# -*- coding: utf-8 -*-
#run in py37

import time
import os
import logging

class Log(object):
    def __init__(self, module_name='main'):
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(logging.DEBUG)

        self.log_time = time.strftime('%Y%m%d')
        self.log_path = '/data1/ck/scaled/logs'
        self.log_name = self.log_path + '/' + module_name + '.' + self.log_time + '.log'

        fh = logging.FileHandler(self.log_name, 'a', encoding='utf-8')  # 这个是python3的
        fh.setLevel(logging.INFO)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # 定义handler的输出格式
        formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] [%(filename)s->%(funcName)s line:%(lineno)d] %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)
        #self.logger.addHandler(ch)

        #  添加下面一句，在记录日志之后移除句柄
        #self.logger.removeHandler(ch)
        #self.logger.removeHandler(fh)
        # 关闭打开的文件
        fh.close()
        ch.close()

    def getlog(self):
        return self.logger
