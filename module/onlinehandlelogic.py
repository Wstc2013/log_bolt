#-*-coding:utf-8-*-
import time
from lib.mysql import mysql_operation
import logging
import json
from handlelogic_game import HandleLogic
log = logging.getLogger('test_kafka')
#from field.handlefield import HandleField
#from key.handlekey import HandleKey
from field.handlefield_game import HandleField
from key.handlekey_game import HandleKey

class OnlineHandleLogic(HandleLogic):
    
    
    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tiemfield_solt(self.tup_value['curTime'])
        super(OnlineHandleLogic, self).__init__()

    def tiemfield_solt(self, tiemfield):
        time_local = time.localtime(tiemfield)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        return dt

    def tup_value_solt(self, tup):
        value = {}
        new_value = tup[0]
        if new_value:
           try:
               value = eval(new_value)
           except Exception as e:
               value = json.loads(new_value)
           curtime = value['curTime']
           self.redis_obj.hset('curTime',"now",curtime)
        else:
           curtime = self.redis_obj.hget('curTime',"now")
           #value['curTime'] = float(curtime.split('.')[0])
           value['curTime'] = int(curtime)
        return value


    def onlinenum(self):
        log.debug(self.tup_value)
        timestamp_fen = self.timefield.split()[1].split(':')[1]
        hour_onlinenum_key = self.handle_key_obj.custom_time_key("ONLINE","%Y%m%d%H")
        if self.tup_value.has_key('kindID'):
            log.debug("key为%s" % (hour_onlinenum_key))
            hour_onlinenum_kindid_field = self.handle_field_obj.kindid_fen(timestamp_fen)
            log.debug("field为%s" % (hour_onlinenum_kindid_field))
            totalonline = self.tup_value['totalOnline']
            log.debug("当前在线人数为%s" % (totalonline))
            for field in hour_onlinenum_kindid_field:
                self.redis_obj.hincrby(hour_onlinenum_key,field,totalonline)
        else:
            all_fen = 'all:%s' % (timestamp_fen)
            onlinenum = int(self.redis_obj.hget(hour_onlinenum_key,all_fen))
            onlinenum_key = self.handle_key_obj.custom_time_key("ONLINE","%Y%m%d")
            log.debug(onlinenum_key)
            max_onlinenum = self.redis_obj.hget(onlinenum_key,"all:max")
            min_onlinenum = self.redis_obj.hget(onlinenum_key,"all:min")
            if max_onlinenum == None:
                max_onlinenum = 0
            else:
                max_onlinenum = int(max_onlinenum)
            if min_onlinenum == None:
                min_onlinenum = 0
            else:
                min_onlinenum = int(min_onlinenum)
            if onlinenum > max_onlinenum:
                self.redis_obj.hset(onlinenum_key,"all:max",onlinenum)
            if min_onlinenum == 0:
                self.redis_obj.hset(onlinenum_key,"all:min",onlinenum)
            if onlinenum < min_onlinenum:
                self.redis_obj.hset(onlinenum_key,"all:min",onlinenum)
