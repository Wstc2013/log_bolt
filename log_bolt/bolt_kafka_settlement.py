#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.sethandlelogic import SetHandleLogic
import logging
import time
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")

log = logging.getLogger('test_kafka')
class SetBolt(SimpleBolt):
    
    
    def process_tuple(self,tup):
         value = tup.values
         log.debug("kafka获取到的数据为:%s" % (value))
         if value != [''] and value != ['\x03']:
         	set_handle_logic_obj = SetHandleLogic(value)
                try:     
                    log.debug(u"开始结算插入数据库处理!!!!")
                    set_handle_logic_obj.insertMysqlset()
                except Exception as e:
                    message = str(e)
                    if  'Duplicate entry' in message:
                        return
                    else:
                        log.debug("数据库插入报错:%s" % (message))
         	log.debug(u"开启结算账号处理!!!!")
         	set_handle_logic_obj.account('SETTLEMENTACCOUNT','UUID')
         	log.debug(u"开启结算次数处理!!!!")
         	set_handle_logic_obj.count('SETTLEMENTCOUNT')
         	log.debug(u"开启结算人数处理!!!!")
         	set_handle_logic_obj.num('SETTLEMENTNUM','UUID')
         	log.debug(u"开启结算金额处理!!!!")
         	set_handle_logic_obj.sum('SETTLESUM', 'Amount')
         	log.debug(u"开启结算行为处理!!!!")
         	set_handle_logic_obj.behavior()
         	log.debug(u"开启结算用户金额处理!!!!")
         	set_handle_logic_obj.account_amount_sum()
         	if set_handle_logic_obj.tup_value.has_key('OS'):
            		log.debug(u"开始os结算次数处理!!!!")
            		set_handle_logic_obj.setoscount()
            		log.debug(u"开始os结算人数处理!!!!")
            		set_handle_logic_obj.setosnum()
            		log.debug(u"开始os结算金额处理!!!!")
            		set_handle_logic_obj.setossum()



if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    #log_filename = '/data/log_bolt/logs/test_settle_%s.log' % (log_time)
    log_filename = '%s/test_settle_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(process)d][%(thread)d][%(lineno)d]%(message)s",
                filemode='a',
    )
    SetBolt().run()
