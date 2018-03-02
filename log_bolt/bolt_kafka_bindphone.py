#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.bindphonehandlelogic import BindphoneHandleLogic
import logging
import time
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")


log = logging.getLogger('test_kafka')
class BindPhoneBolt(SimpleBolt):
    
    def process_tuple(self,tup):
         value = tup.values
         if value != [''] and value != ['\x03']:
         	log.debug("kafka获取到的数据为:%s" % (value))
         	bindphone_handle_logic_obj = BindphoneHandleLogic(value)
         	log.debug(u"开始绑定手机次数处理!!!!")
         	bindphone_handle_logic_obj.count('BINDPHONE')
            	log.debug(u"开始绑定手机插入数据库处理!!!!")
            	bindphone_handle_logic_obj.insertMysqlBindphone()

    

if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '%s/test_bindphone_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(lineno)d]%(message)s",
                filemode='a',
    )
    BindPhoneBolt().run()
