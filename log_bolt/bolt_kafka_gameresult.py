#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.gamehandlelogic import GameHandleLogic
import logging
import time
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")


log = logging.getLogger('test_kafka')
class GameBolt(SimpleBolt):
    
    
    def process_tuple(self,tup):
         value = tup.values
         if value != [''] and value != ['\x03']:
             log.debug("kafka获取到的数据为:%s" % (value))
             game_handle_logic_obj = GameHandleLogic(value)
             if game_handle_logic_obj.tup_value.has_key('servicerevenue'):
                 log.debug("开始明税收处理!!!!")
                 game_handle_logic_obj.mtax()
                 log.debug("开始佣金处理!!!!")
                 game_handle_logic_obj.commission()
             log.debug("开始暗税收处理!!!!")
             game_handle_logic_obj.atax()
             log.debug("开始系统输赢处理!!!!")
             game_handle_logic_obj.syscore()
             log.debug("开始佣金行为处理!!!!")
             game_handle_logic_obj.behavior()
             #log.debug("开始佣金处理!!!!")
             #game_handle_logic_obj.commission()
             #log.debug("开始统计处理!!!!")
             #game_handle_logic_obj.statistics()
             log.debug("开始系统输赢日志记录!!!!")
             game_handle_logic_obj.insertMysqlSysscore()



if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '%s/test_gameresult_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(process)d][%(thread)d][%(lineno)d]%(message)s",
                filemode='a',
    )
    GameBolt().run()
