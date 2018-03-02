#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.onlinehandlelogic import OnlineHandleLogic
import logging
import time
import configparser
log = logging.getLogger('test_kafka')
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")
log = logging.getLogger('test_kafka')
class onlineBolt(SimpleBolt):
    
    
    def process_tuple(self,tup):
         value = tup.values
         log.debug("kafka获取到的数据为:%s" % (value))
         online_handle_logic_obj = OnlineHandleLogic(value)
         log.debug("开始在线人数处理!!!!")
         online_handle_logic_obj.onlinenum()


if __name__ == '__main__':
    	log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
        log_filename = '%s/test_online_%s.log' % (logdir,log_time)
    	logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(process)d][%(thread)d][%(lineno)d]%(message)s",
                filemode='a',
    	)
        onlineBolt().run()
