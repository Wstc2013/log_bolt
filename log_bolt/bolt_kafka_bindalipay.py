#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.bindalipayhandlelogic import BindalipayHandleLogic
import logging
import time
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")
log = logging.getLogger('test_kafka')
class BindalipayBolt(SimpleBolt):
    
    def process_tuple(self,tup):
         value = tup.values
         if value != [''] and value != ['\x03']:
         	log.debug("kafka获取到的数据为:%s" % (value))
         	bindalipay_handle_logic_obj = BindalipayHandleLogic(value)
         	log.debug(u"开始绑定支付宝次数处理!!!!")
         	bindalipay_handle_logic_obj.count('BINDALIPAY')
            	log.debug(u"开始绑定支付宝插入数据库处理!!!!")
            	bindalipay_handle_logic_obj.insertMysqlBindalipay()

    

if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '%s/test_bindalipay_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(lineno)d]%(message)s",
                filemode='a',
    )
    BindalipayBolt().run()
