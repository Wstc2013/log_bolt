#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.payhandlelogic import PayHandleLogic
import logging
import time
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")

log = logging.getLogger('test_kafka')
class PayBolt(SimpleBolt):

    
    def process_tuple(self,tup):
         value = tup.values
         if value != [''] and value != ['\x03']:
         	log.debug("kafka获取到的数据为:%s" % (value))
         	pay_handle_logic_obj = PayHandleLogic(value)
                if pay_handle_logic_obj.tup_value["PayMode"] != "Direct":
                   try:     
                        log.debug(u"开始付费插入数据库处理!!!!")
                        pay_handle_logic_obj.insertMysqlpay()
                   except Exception as e:
                        message = str(e)
                        if  'Duplicate entry' in message:
                           return
                        else:
                           log.debug("数据库插入报错:%s" % (message))
         	   log.debug(u"开始付费账号处理!!!!")
         	   pay_handle_logic_obj.account()
                   log.debug(u"开始付费次数处理!!!!")
         	   pay_handle_logic_obj.count('PAYCOUNT')
                   log.debug(u"开始付费人数处理!!!!")
         	   pay_handle_logic_obj.num('PAYNUM','UUID')
         	   log.debug(u"开始付费金额处理!!!!")
         	   pay_handle_logic_obj.sum('PAYSUM', 'Amount')
         	   log.debug(u"开始付费行为处理!!!!")
         	   pay_handle_logic_obj.behavior()
         	   log.debug(u"开始新增用户充值金额处理!!!!")
         	   pay_handle_logic_obj.newuserpaysum()
         	   log.debug(u"开始新增用户充值人数处理!!!!")
         	   pay_handle_logic_obj.newuserpaynum()
                   log.debug(u"开始mode付费次数处理!!!!")
                   pay_handle_logic_obj.paymodecount()
                   log.debug(u"开始mode付费人数处理!!!!")
                   pay_handle_logic_obj.paymodenum()
                   log.debug(u"开始mode付费金额处理!!!!")
                   pay_handle_logic_obj.paymodesum()
         	   if pay_handle_logic_obj.tup_value.has_key('OS'):
            	       log.debug(u"开始os付费次数处理!!!!")
            	       pay_handle_logic_obj.payoscount()
            	       log.debug(u"开始os付费人数处理!!!!")
            	       pay_handle_logic_obj.payosnum()
            	       log.debug(u"开始os付费金额处理!!!!")
            	       pay_handle_logic_obj.payossum()
            	       log.debug(u"开始新增用户OS充值金额处理!!!!")
            	       pay_handle_logic_obj.newuserospaysum()
            	       log.debug(u"开始新增用户OS充值人数处理!!!!")
            	       pay_handle_logic_obj.newuserospaynum()
                   if pay_handle_logic_obj.tup_value.has_key('PayChannel'):
                       log.debug(u"开始paychannel付费次数处理!!!!")
                       pay_handle_logic_obj.paychannelcount()
                       log.debug(u"开始paychannel付费人数处理!!!!")
                       pay_handle_logic_obj.paychannelnum()
                       log.debug(u"开始paychannel付费金额处理!!!!")
                       pay_handle_logic_obj.paychannelsum()
                       log.debug(u"开始paychannelmode付费金额处理!!!!")
                       pay_handle_logic_obj.paychannelmodesum()
                       log.debug(u"开始paychannelmode付费笔数处理!!!!")
                       pay_handle_logic_obj.paychannelmodecount()

    

if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '%s/test_pay_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(lineno)d]%(message)s",
                filemode='a',
    )
    PayBolt().run()
