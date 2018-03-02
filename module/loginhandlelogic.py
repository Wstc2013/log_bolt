#-*-coding:utf8-*-
from handlelogic import HandleLogic
import time,datetime
from lib.mysql import mysql_operation
import logging
import json
log = logging.getLogger('test_kafka')


class LoginHandleLogic(HandleLogic):

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tup_value['TimeStamp']
        super(LoginHandleLogic, self).__init__()

    def tup_value_solt(self, tup):
        try:
           new_value = eval(tup[0][""])
        except Exception as e:
           new_value = eval(tup[0])
        if not new_value['DeviceInfo'].has_key('GameID'):
	   new_value['DeviceInfo']['GameID'] = 'XXXXXXX'
        return new_value

    def firstlogin(self, firstlogin, type_value):
            s_value = ''
            first_type_key_list = self.handle_key_obj.channel_key_list(firstlogin)
            log.debug("用于存放首登用户或首登设备的key为:%s" % (first_type_key_list))
            first_key_list = self.handle_key_obj.base_key_list(firstlogin)
            account_key = self.handle_key_obj.login_account_key(type_value)
            account_field = self.handle_field_obj.channel_type(type_value)
            log.debug("登陆的账号key:%s,field:%s" % (account_key,account_field))
            fields = self.handle_field_obj.channel_list()
            log.debug("首登key:%s,field:%s" % (first_key_list,fields))
            if type_value == 'UUID':
                s_value = self.tup_value['UUID']
            elif type_value == 'IMEI':
                s_value = self.tup_value['DeviceInfo']['IMEI']
            if not self.redis_obj.hexists(account_key, account_field):
                log.debug("该登陆账号不存在为首次登陆")
                for first_type_key in first_type_key_list:
                	self.redis_obj.sadd(first_type_key, s_value)
                	log.debug("将%s放入相应的首登集合中%s" % (s_value,first_type_key))
                for first_key in first_key_list:
                    for field in fields:
                        self.redis_obj.hincrby(first_key, field)
    
    def newfirstlogin(self,account_key,account_field,first_key_list,fields,first_type_key_list,type_value):
            s_value = ''
            log.debug("用于存放首登用户或首登设备的key为:%s" % (first_type_key_list))
            #account_key = self.handle_key_obj.login_account_key(type_value)
            #account_field = self.handle_field_obj.channel_type(type_value)
            log.debug("登陆的账号key:%s,field:%s" % (account_key,account_field))
            log.debug("首登key:%s,field:%s" % (first_key_list,fields))
            if type_value == 'UUID':
                s_value = self.tup_value['UUID']
            elif type_value == 'IMEI':
                s_value = self.tup_value['DeviceInfo']['IMEI']
            if not self.redis_obj.hexists(account_key, account_field):
                log.debug("该登陆账号不存在为首次登陆")
                for first_type_key in first_type_key_list:
                        self.redis_obj.sadd(first_type_key, s_value)
                        log.debug("将%s放入相应的首登集合中%s" % (s_value,first_type_key))
                for first_key in first_key_list:
                    for field in fields:
                        self.redis_obj.hincrby(first_key, field)


    def date_day_count(self, time_struct, offset_day):
        date_day = []
        for day in offset_day:
            delta = datetime.timedelta(days=day-1)
            n_date = time_struct - delta
            date = n_date.strftime('%Y%m%d')
            date_day.append({'date': date, 'day': day})
        return date_day

    def retained(self, retained, type_value):
            s_value = ''
            datatype = ''
            retained_key = self.handle_key_obj.base_key(retained, self.timefield)
            retained_channel_key = self.handle_key_obj.channel_key(retained, self.timefield)
            log.debug("留存key为:%s" % (retained_key))
            if type_value == 'UUID':
                s_value = self.tup_value['UUID']
                datatype = 'FIRSTLOGIN'
            elif type_value == 'IMEI':
                s_value = self.tup_value['DeviceInfo']['IMEI']
                datatype = 'FIRSTLOGINDEVICE'
            if not self.redis_obj.sismember(retained_channel_key, s_value):
            	timestamp = self.timefield
            	timestamp = timestamp.split(' ')[0]
            	log.debug("当前时间为:%s" % (timestamp))
            	time_struct = datetime.datetime.strptime(timestamp, '%Y-%m-%d')
            	offset_day = [2, 3, 7, 15, 30]
            	log.debug("记录2,3,7,15,30留数据")
            	date_days = self.date_day_count(time_struct, offset_day)
            	log.debug("2,3,7,15留对应的时间为:%s" % (date_days))
            	for date_day in date_days:
                	firstlogin_key = self.handle_key_obj.firstlogin_key(datatype, date_day['date'])
                	log.debug("某留对应的首登集合key为:%s" % (firstlogin_key))
                	fields = self.handle_field_obj.channel_day_list(date_day['day'])
                	log.debug("某留对应的fields为:%s" % (fields))
                	log.debug("判断用户或设备在某留的首登集合里")
                	if self.redis_obj.sismember(firstlogin_key, s_value):
                    		for field in fields:
                        		log.debug(retained_key)
                        		self.redis_obj.hincrby(retained_key, field)
                self.redis_obj.sadd(retained_channel_key, s_value)

    def newretained(self,retained_datatype,firstlogin_datatype,retained_os_channel_key,type_value):
            s_value = ''
            if type_value == 'UUID':
                s_value = self.tup_value['UUID']
            elif type_value == 'IMEI':
                s_value = self.tup_value['DeviceInfo']['IMEI']
            if not self.redis_obj.sismember(retained_os_channel_key, s_value):
                timestamp = self.timefield
                timestamp = timestamp.split(' ')[0]
                log.debug("当前时间为:%s" % (timestamp))
                time_struct = datetime.datetime.strptime(timestamp, '%Y-%m-%d')
                offset_day = [2, 3, 7, 15, 30]
                log.debug("记录2,3,7,15,30留数据")
                date_days = self.date_day_count(time_struct, offset_day)
                log.debug("2,3,7,15留对应的时间为:%s" % (date_days))
                for date_day in date_days:
                        firstlogin_key = self.handle_key_obj.firstlogin_os_channel(firstlogin_datatype, date_day['date'])
                        retained_key = self.handle_key_obj.data_base_key(retained_datatype,date_day['date'])
                        log.debug("留存key为:%s" % (retained_key))
                        log.debug("某留对应的首登集合key为:%s" % (firstlogin_key))
                        fields = self.handle_field_obj.os_channel_day_list(date_day['day'])
                        log.debug("某留对应的fields为:%s" % (fields))
                        log.debug("判断用户或设备在某留的首登集合里")
                        if self.redis_obj.sismember(firstlogin_key, s_value):
                                for field in fields:
                                        log.debug(retained_key)
                                        self.redis_obj.hincrby(retained_key, field)
                self.redis_obj.sadd(retained_os_channel_key, s_value)

    def behavior(self):
        ret = super(LoginHandleLogic, self).behavior()
        account_key = self.handle_key_obj.account_key("LOGINACCOUNT")
        field = self.handle_field_obj.channel_type("UUID")
        be_type = 'login'
        if not self.redis_obj.hexists(account_key, field):
            be_type = 'register'
        strjson = str({'GameID': ret["game_id"], 'ChannelID': ret["channel"]})
        sql = """INSERT INTO behavior(uuid,type, strjson, date) VALUES ("%s", "%s", "%s", "%s")""" % (ret["uuid"], be_type, strjson, ret["timestamp"])
        log.debug("用户行为执行的sql语句为:%s" % (sql))
        #print sql
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)
   
    #def onlinenum(self):
        #now_onlinenum_key = self.handle_key_obj.all_time_key("ONLINENUM")
        #now_fields = self.handle_field_obj.channel_list()
        #hour_onlinenum_key = self.handle_key_obj.custom_time_key("HOURONLINENUM","%Y%m%d%H")
        #timestamp = self.tup_value['TimeStamp'].split()[1].split(':')[2]
        #hour_onlinenum_all_field = self.handle_field_obj.allchannel_fen(timestamp)
        #hour_onlinenum_channel_field = self.handle_field_obj.channel_fen(timestamp)
        #max_onlinenum_key_list = self.handle_key_obj.base_key_list("MAXONLINENUM")
        #max_onlinenum_fields = self.handle_field_obj.channel_list()
        #totalonline = self.tup_value['totalOnline']
        #channelid = self.tup_value['DeviceInfo']['Channel']
        #for field in now_fields:
                #self.redis_obj.hincrby(onlinenum_key, field)
        #else:
            #log.debug("进行当前用户下线处理")
            #for field in now_fields:
                #self.redis_obj.hincrby(onlinenum_key, field, -1)
        #log.debug("取出当前渠道以及所以渠道的当前在线人数")
        #now_onlinenum_all_value = self.redis_obj.hget(now_onlinenum_key,'all')
        #now_onlinenum_channel_value = self.redis_obj.hget(now_onlinenum_key,channelid)
        #log.debug("进行每小时每分钟在线人数处理")
        #self.redis_obj.hset(hour_onlinenum_key,hour_onlinenum_all_field,now_onlinenum_all_value)
        #self.redis_obj.hset(hour_onlinenum_key,hour_onlinenum_channel_field,now_onlinenum_channel_value)
	#log.debug("进行最高在线人数处理")
        #for max_onlinenum_key in max_onlinenum_key_list:
            #max_onlinenum_all_value = self.redis_obj.hget(max_onlinenum_key,'all')
            #max_onlinenum_channel_value = self.redis_obj.hget(max_onlinenum_key,channelid)
            #if now_onlinenum_all_value > max_onlinenum_all_value:
               #self.redis_obj.hset(max_onlinenum_key,'all',now_onlinenum_all_value)
            #if now_onlinenum_channel_value > max_onlinenum_channel_value:
               #self.redis_obj.hset(max_onlinenum_key,channelid,now_onlinenum_channel_value)

    def loginosaccount(self):
        account_key_list = self.handle_key_obj.base_key_list("LOGINOSACCOUNT")
        fields = self.handle_field_obj.os_channel_type_list("UUID")
        self.newaccount(account_key_list,fields)

    def loginoscount(self):
        key = self.handle_key_obj.base_key_list("LOGINOSCOUNT")
        field = self.handle_field_obj.os_channel()
        self.newcount(key,field)

    def loginosnum(self):
        key = self.handle_key_obj.os_channel_key_list("LOGINOSNUM")
        field = self.handle_field_obj.os_channel()
        self.newnum(key,field,'UUID')

    def loginosfirstlogin(self):
        key = self.handle_key_obj.base_key_list("LOGINOSFIRSTLOGIN")
        os_key = self.handle_key_obj.os_channel_key_list("LOGINOSFIRSTLOGIN")
        field = self.handle_field_obj.os_channel()
        account_key = self.handle_key_obj.account_key("LOGINOSACCOUNT")
        account_field = self.handle_field_obj.os_channel_type("UUID")
        self.newfirstlogin(account_key,account_field,key,field,os_key,'UUID')

    def loginosretained(self):
        retained_os_channel_key = self.handle_key_obj.os_channel_key("RETAINEDOS")
        self.newretained("RETAINEDOS","LOGINOSFIRSTLOGIN",retained_os_channel_key,"UUID")
   
    def insertMysqlLogin(self):
        '''gameid, channel, devicetype,IMEI, mac,IPv4,os, uuid, userid, timestamp, Latitude":"0.0","Longitude":"0.0"'''
        if self.tup_value["DeviceInfo"].has_key('GameID'):
            gameid = self.tup_value["DeviceInfo"]["GameID"]
        else:
            gameid = ''
        if self.tup_value["DeviceInfo"].has_key("Channel"):
            channel = self.tup_value["DeviceInfo"]["Channel"]
        else:
            channel = ''
        if self.tup_value["DeviceInfo"].has_key("DeviceType"):
            devicetype = self.tup_value["DeviceInfo"]["DeviceType"]
        else:
            devicetype = ''
        if self.tup_value["DeviceInfo"].has_key("IMEI"):
            imei = self.tup_value["DeviceInfo"]["IMEI"]
        else:
            imei = ''
        if self.tup_value["DeviceInfo"].has_key('MAC'):
            mac = self.tup_value["DeviceInfo"]["MAC"]
        else:
            mac = ''
        if self.tup_value["DeviceInfo"].has_key("IPv4"):
            ipv4 = self.tup_value["DeviceInfo"]["IPv4"]
        else:
            ipv4 = ''
        os = self.tup_value["DeviceInfo"]["OS"]
        uuid = self.tup_value["UUID"]
        if self.tup_value.has_key("Username"):
           username = self.tup_value["Username"]
        else:
           username = ''
        if self.tup_value["DeviceInfo"].has_key("Longitude"):
           latitude = self.tup_value["DeviceInfo"]["Longitude"]
        else:
           latitude = ''
        if self.tup_value["DeviceInfo"].has_key("Latitude"):
           longitude = self.tup_value["DeviceInfo"]["Latitude"]
        else:
           longitude = ''
        timestamp = self.timefield
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime("%Y%m%d%H%M%S", time_struct)
        sql = """INSERT INTO login(gameid,channel,devicetype,imei,mac,ipv4,os,uuid,username,latitude,longitude,timestamp) VALUES ("%s", "%s", "%s", "%s","%s","%s","%s","%s","%s","%s","%s","%s")""" % (gameid,channel,devicetype,imei,mac,ipv4,os,uuid,username,latitude,longitude,timestamp)
        log.debug(sql)
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)
