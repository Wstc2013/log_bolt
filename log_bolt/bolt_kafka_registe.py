#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.registehandlelogic import RegisteHandleLogic
import logging
import time
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")
log = logging.getLogger('test_kafka')
class RegisteBolt(SimpleBolt):
    
    def process_tuple(self,tup):
         value = tup.values
         if value != [''] and value != ['\x03']:
         	log.debug("kafka获取到的数据为:%s" % (value))
         	registe_handle_logic_obj = RegisteHandleLogic(value)
         	log.debug(u"开始注册次数处理!!!!")
         	registe_handle_logic_obj.count('REGISTECOUNT')
            	log.debug(u"开始注册插入数据库处理!!!!")
            	registe_handle_logic_obj.insertMysqlRegister()

    

if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '%s/test_registe_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(lineno)d]%(message)s",
                filemode='a',
    )
    RegisteBolt().run()
