#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.preorderhandlelogic import PreorderHandleLogic
import logging
import time
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")

log = logging.getLogger('test_kafka')
class PreOrderBolt(SimpleBolt):
    
    def process_tuple(self,tup):
         value = tup.values
         if value != [''] and value != ['\x03']:
         	log.debug("kafka获取到的数据为:%s" % (value))
         	preorder_handle_logic_obj = PreorderHandleLogic(value)
                if preorder_handle_logic_obj.tup_value["PayMode"] != "Direct":
         		log.debug(u"开始预订单付费次数处理!!!!")
         		preorder_handle_logic_obj.count('PREORDERCOUNT')
                        log.debug(u"开始预订单mode付费次数处理!!!!")
                        preorder_handle_logic_obj.preordermodecount()
                        log.debug(u"开始预订单mode付费人数处理!!!!")
                        preorder_handle_logic_obj.preordermodenum()
                        log.debug(u"开始预订单mode付费金额处理!!!!")
                        preorder_handle_logic_obj.preordermodesum()
         		if preorder_handle_logic_obj.tup_value.has_key('OS'):
            			log.debug(u"开始os预订单付费次数处理!!!!")
            			preorder_handle_logic_obj.preorderoscount()
                        if preorder_handle_logic_obj.tup_value.has_key('PayChannel'):
                                log.debug(u"开始preorderchannelmode付费笔数处理!!!!")
                                preorder_handle_logic_obj.preorderchannelmodecount()
                        

    

if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '%s/test_preorder_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(lineno)d]%(message)s",
                filemode='a',
    )
    PreOrderBolt().run()
