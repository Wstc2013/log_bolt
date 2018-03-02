#-*-coding:utf8-*-
import time
import redis
import configparser
from field.handlefield import HandleField
from key.handlekey import HandleKey
import logging
log = logging.getLogger('test_kafka')

class HandleLogic(object):
    config = configparser.ConfigParser()
    config.read("config/config.ini",encoding='utf-8')
    redis_obj = redis.Redis(host=config.get("redis", "hostname"), port=int(config.get("redis", "port")), db=0)

    def __init__(self):
        self.handle_key_obj = HandleKey(self.tup_value, self.timefield)
        self.handle_field_obj = HandleField(self.tup_value)


    #def numsolt(self,num_type_key_list,field,s_value):
        #for num_type_key in num_type_key_list:
            #num_type_key_split = num_type_key.split(':')
            #num_type_key_del = num_type_key_split.pop()
            #num_key = ':'.join(num_type_key_split)
            #log.debug(u"num的key为:%s,field为:%s" % (num_key,field))
            #self.redis_obj.sadd(num_type_key, s_value)
            #value = self.redis_obj.scard(num_type_key)
            #log.debug(u"存放uuid或IMEI的集合的key为:%s" % (num_type_key))
            #log.debug(u"集合的总数为:%s" % (value))
            #self.redis_obj.hset(num_key, field, value)
        
    def account(self, account, type_value):
        account_key_list = self.handle_key_obj.base_key_list(account)
        fields = self.handle_field_obj.channel_type_list(type_value)
        log.debug(u"对应的key为:%s,filed为:%s" % (account_key_list,fields))
        for account_key in account_key_list:
            for field in fields:
                self.redis_obj.hincrby(account_key, field)

    def newaccount(self, account_key_list, fields):
        log.debug(u"对应的key为:%s,filed为:%s" % (account_key_list, fields))
        for account_key in account_key_list:
            for field in fields:
                self.redis_obj.hincrby(account_key, field)

    def count(self, count):
        count_key_list = self.handle_key_obj.base_key_list(count)
        fields = self.handle_field_obj.channel_list()
        log.debug(u"对应的key为:%s,filed为:%s" % (count_key_list,fields))
        for count_key in count_key_list:
            for field in fields:
                self.redis_obj.hincrby(count_key, field)

    def newcount(self, count_key_list, fields):
        log.debug(u"对应的key为:%s,filed为:%s" % (count_key_list,fields))
        for count_key in count_key_list:
            for field in fields:
                self.redis_obj.hincrby(count_key, field)

    #def num(self, num, type_value):
        #num_channel_key_list = self.handle_key_obj.channel_key_list(num)
        #field = self.handle_field_obj.channel()
        #num_all_key_list = self.handle_key_obj.all_key_list(num)
        #log.debug(u"存放uuid或IMEI的集合key为:%s" % (num_channel_key_list))
        #log.debug(u"存放uuid或IMEI的集合key为:%s" % (num_all_key_list))
        #if type_value == 'UUID':
            #s_value = self.tup_value['UUID']
        #elif type_value == 'IMEI':
            #s_value = self.tup_value['DeviceInfo']['IMEI']
        #self.numsolt(num_channel_key_list,field,s_value)
        #self.numsolt(num_all_key_list,'all',s_value)
          
    def num(self, num, type_value):
        num_type_key_list = self.handle_key_obj.channel_key_list(num)
        fields = self.handle_field_obj.channel_list()
        log.debug(u"存放uuid或IMEI的集合key为:%s" % (num_type_key_list))
        if type_value == 'UUID':
            s_value = self.tup_value['UUID']
        elif type_value == 'IMEI':
            s_value = self.tup_value['DeviceInfo']['IMEI']
        for num_type_key in num_type_key_list:
            log.debug(u"判断%s是否为活跃用户或活跃设备" % (s_value))
            if not  self.redis_obj.sismember(num_type_key,s_value):
                log.debug(u"为活跃用户或活跃设备进行处理")
            	num_type_key_split = num_type_key.split(':')
            	num_type_key_del = num_type_key_split.pop()
            	num_key = ':'.join(num_type_key_split)
            	log.debug(u"num的key为:%s,field为:%s" % (num_key,fields))
                for field in fields:
                   self.redis_obj.hincrby(num_key, field)
            	self.redis_obj.sadd(num_type_key, s_value)

    def newnum(self, num_type_key_list,fields,type_value):
        log.debug(u"存放uuid或IMEI的集合key为:%s" % (num_type_key_list))
        if type_value == 'UUID':
            s_value = self.tup_value['UUID']
        elif type_value == 'IMEI':
            s_value = self.tup_value['DeviceInfo']['IMEI']
        for num_type_key in num_type_key_list:
            log.debug(u"判断%s是否为活跃用户或活跃设备" % (s_value))
            if not  self.redis_obj.sismember(num_type_key,s_value):
                log.debug(u"为活跃用户或活跃设备进行处理")
                num_type_key_split = num_type_key.split(':')
                num_type_key_split.pop()
                num_type_key_split.pop()
                num_key = ':'.join(num_type_key_split)
                log.debug(u"num的key为:%s,field为:%s" % (num_key,fields))
                for field in fields:
                   self.redis_obj.hincrby(num_key, field)
                self.redis_obj.sadd(num_type_key, s_value)

    def sum(self, sum, sumfield):
        sum_key_list = self.handle_key_obj.base_key_list(sum)
        sumvaule = self.tup_value[sumfield]
        fields = self.handle_field_obj.channel_list()
        log.debug(u"对应的key为:%s,filed为:%s,付费金额为:%s" % (sum_key_list,fields,sumvaule))
        for sum_key in sum_key_list:
            for field in fields:
                self.redis_obj.hincrbyfloat(sum_key, field, sumvaule)
    
    def newsum(self, sum_key_list, fields, value):
        for sum_key in sum_key_list:
            for field in fields:
                self.redis_obj.hincrbyfloat(sum_key, field, value)


    def behavior(self):
        ret = {}
        if self.tup_value.has_key('ChannelID'):
            ret["channel"] = self.tup_value["ChannelID"]
        else:
            ret["channel"] = self.tup_value['DeviceInfo']['Channel']
        if self.tup_value.has_key('GameID'):
            ret["game_id"] = self.tup_value['GameID']
        else:
            ret["game_id"] = self.tup_value['DeviceInfo']['GameID']
        ret["uuid"] = self.tup_value['UUID']
        timestamp = self.timefield
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        ret["timestamp"] = time.strftime("%Y%m%d%H%M%S", time_struct)
        return ret
