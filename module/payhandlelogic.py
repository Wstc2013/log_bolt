#-*-coding:utf-8-*-
from handlelogic import HandleLogic
import time
from lib.mysql import mysql_operation
import logging
log = logging.getLogger('test_kafka')

class PayHandleLogic(HandleLogic):

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tup_value['TimeStamp']
        super(PayHandleLogic, self).__init__()
    
    def tup_value_solt(self, tup):
        try:
           new_value = eval(tup[0][""])
        except Exception as e:
           #new_value = eval(tup[0])
           new_value = eval(tup[0].values()[0])
        if not new_value.has_key('GameID'):
           new_value['GameID'] = 'XXXXXXX'
        return new_value


    def behavior(self):
        ret = super(PayHandleLogic, self).behavior()
        amount = self.tup_value['Amount']
        be_type = 'pay'
        strjson = str({'GameID': ret["game_id"], 'ChannelID': ret["channel"], 'Amount': amount})
        sql = """INSERT INTO behavior(uuid,type, strjson, date) VALUES ("%s", "%s", "%s", "%s")""" % (ret["uuid"], be_type, strjson, ret["timestamp"])
        log.debug(u"付费行为sql为:%s" % (sql))
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)

    def account(self):
        account_key_list = self.handle_key_obj.payaccount_key_list(self.timefield)
        fields = self.handle_field_obj.channel_type_list("UUID")
        log.debug(u"付费账号key为%s,field为%s" % (account_key_list,fields))
        for account_key in account_key_list:
            for field in fields:
                self.redis_obj.hincrby(account_key, field)

    #def newusersum(self):
        #uuid = self.tup_value['UUID']
        #first_type_key_list = self.handle_key_obj.channel_key_list("FIRSTLOGIN")
        #fields = self.handle_field_obj.channel_list()
        #sumvaule = self.tup_value['Amount']
        #log.debug(u"新增用户集合key为%s" % (first_type_key_list))
        #for first_type_key in first_type_key_list:
        	#log.debug(u"判断用户是否为新增用户")
        	#if  self.redis_obj.sismember(first_type_key, uuid):
          		#log.debug(u"该用户为新增用户进行充值金额处理")
                        #sum_type_key_split = first_type_key.split(':')
            		#sum_type_key_del = sum_type_key_split.pop()
                        #sum_type_key_split[1] = "NEWUSERPAYSUM"
            		#sum_key = ':'.join(sum_type_key_split)
                        #log.debug(u"新增用户充值key为%s" % (sum_key))
                        #for field in fields:
                        	#self.redis_obj.hincrbyfloat(sum_key, field, sumvaule)

    def newusersum(self,first_type_key_list,fields,datatype,type_value,delete_num,sumvaule):
        if type_value == 'UUID':
                s_value = self.tup_value['UUID']
        elif type_value == 'IMEI':
                s_value = self.tup_value['DeviceInfo']['IMEI']
        #first_type_key_list = self.handle_key_obj.channel_key_list("FIRSTLOGIN")
        #fields = self.handle_field_obj.channel_list()
        #sumvaule = self.tup_value['Amount']
        log.debug(u"新增用户集合key为%s" % (first_type_key_list))
        for first_type_key in first_type_key_list:
                log.debug(u"判断用户是否为新增用户")
                if  self.redis_obj.sismember(first_type_key, s_value):
                        log.debug(u"该用户为新增用户进行充值金额处理")
                        sum_type_key_split = first_type_key.split(':')
                        for i in range(delete_num):
                        	sum_type_key_split.pop()
                        sum_type_key_split[1] = datatype
                        sum_key = ':'.join(sum_type_key_split)
                        log.debug(u"新增用户充值key为%s" % (sum_key))
                        for field in fields:
                                self.redis_obj.hincrbyfloat(sum_key, field, sumvaule)

    #def newusernum(self):
        #uuid = self.tup_value['UUID']
        #first_type_key_list = self.handle_key_obj.channel_key_list("FIRSTLOGIN")
        #fields = self.handle_field_obj.channel_list()
        #log.debug(u"登陆账号key为%s" % (first_type_key_list))
        #for first_type_key in first_type_key_list:
        	#log.debug(u"判断用户是否为新增用户")
                #if  self.redis_obj.sismember(first_type_key, uuid):
                	#log.debug(u"该用户为新增用户进行充值人数处理")
                        #sum_channel_key_split = first_type_key.split(':')
                        #sum_channel_key_split[1] = "NEWUSERPAYNUM"
                        #sum_channel_key = ':'.join(sum_channel_key_split)
                        #sum_type_key_del = sum_channel_key_split.pop()
                        #sum_key = ':'.join(sum_channel_key_split)
                        #if not  self.redis_obj.sismember(sum_channel_key,uuid):
                           	#for field in fields:
                   			#self.redis_obj.hincrby(sum_key, field)
                                #self.redis_obj.sadd(sum_channel_key, uuid)

    def newusernum(self,first_type_key_list,fields,datatype,type_value,delete_num):
        if type_value == 'UUID':
                s_value = self.tup_value['UUID']
        elif type_value == 'IMEI':
                s_value = self.tup_value['DeviceInfo']['IMEI']
        uuid = self.tup_value['UUID']
        #first_type_key_list = self.handle_key_obj.channel_key_list("FIRSTLOGIN")
        #fields = self.handle_field_obj.channel_list()
        log.debug(u"登陆账号key为%s" % (first_type_key_list))
        for first_type_key in first_type_key_list:
                log.debug(u"判断用户是否为新增用户")
                if  self.redis_obj.sismember(first_type_key, uuid):
                        log.debug(u"该用户为新增用户进行充值人数处理")
                        sum_channel_key_split = first_type_key.split(':')
                        sum_channel_key_split[1] = datatype
                        sum_channel_key = ':'.join(sum_channel_key_split)
                        for i in range(delete_num):
                                sum_channel_key_split.pop()
                        sum_key = ':'.join(sum_channel_key_split)
                        if not  self.redis_obj.sismember(sum_channel_key,s_value):
                                for field in fields:
                                        self.redis_obj.hincrby(sum_key, field)
                                self.redis_obj.sadd(sum_channel_key, s_value)
   
    #def proxypaynum(self):
        #paymode = self.tup_value["PayMode"]
        #log.debug(u"判断是否为代理充值")
        #if paymode == "DLPAY":
           #log.debug(u"该用户为代理充值进行代理充值人数处理")
           #self.num("PROXYPAYNUM","UUID")

    def proxylog(self):
        paymode = self.tup_value["PayMode"]
        log.debug(u"判断是否为代理充值")
        if paymode == "DLPAY":
           ret = self.behavior()
           ret['amount'] = self.tup_value["Amount"]
           ret['paymode'] = paymode
           sql = """INSERT INTO proxylog(uuid,amount, gameid, channelid,timestamp,paymode) VALUES ("%s", "%s", "%s", "%s","%s","%s")""" % (ret["uuid"], ret["amount"], ret["game_id"],ret["channel"],ret["timestamp"],ret["paymode"])
           log.debug("用户行为执行的sql语句为:%s" % (sql))
           #print sql
           mysql_operation_obj = mysql_operation('logserver')
           mysql_operation_obj.mysql_insert(sql)

    #def proxypaycount(self):
        #paymode = self.tup_value["PayMode"]
        #log.debug(u"判断是否为代理充值")
        #if paymode == "DLPAY":
           #log.debug(u"该用户为代理充值进行代理充值笔数处理")
           #self.count("PROXYPAYCOUNT")

    def payoscount(self):
        key = self.handle_key_obj.base_key_list("PAYOSCOUNT")
        field = self.handle_field_obj.os_channel()
        self.newcount(key,field)

    def payosnum(self):
        key = self.handle_key_obj.os_channel_key_list("PAYOSNUM")
        field = self.handle_field_obj.os_channel()
        self.newnum(key,field,'UUID')

    def payossum(self):
        key = self.handle_key_obj.base_key_list("PAYOSSUM")
        field = self.handle_field_obj.os_channel()
        amount = self.tup_value['Amount']
        self.newsum(key,field,amount)

    def newuserpaysum(self):
        key  = self.handle_key_obj.channel_key_list("FIRSTLOGIN")
        field = self.handle_field_obj.channel_list()
        amount = self.tup_value['Amount']
        delete_num = 1
        self.newusersum(key,field,"NEWUSERPAYSUM","UUID",delete_num,amount)

    def newuserospaysum(self):
        key = self.handle_key_obj.os_channel_key_list("LOGINOSFIRSTLOGIN")
        field = self.handle_field_obj.os_channel()
        amount = self.tup_value['Amount']
        delete_num = 2
        self.newusersum(key,field,"NEWUSEROSPAYSUM","UUID",delete_num,amount)

    def newuserpaynum(self):
        key = self.handle_key_obj.channel_key_list("FIRSTLOGIN")
        field = self.handle_field_obj.channel_list()
        delete_num = 1
        self.newusernum(key,field,"NEWUSERPAYNUM","UUID",delete_num)

    def newuserospaynum(self):
        key = self.handle_key_obj.os_channel_key_list("LOGINOSFIRSTLOGIN")
        field = self.handle_field_obj.os_channel()
        delete_num = 2
        self.newusernum(key,field,"NEWUSEROSPAYNUM","UUID",delete_num)

    def paymodecount(self):
        key = self.handle_key_obj.base_key_list("PAYMODECOUNT")
        field = self.handle_field_obj.mode_channel()
        self.newcount(key,field)

    def paymodenum(self):
        key = self.handle_key_obj.mode_channel_key_list("PAYMODENUM")
        field = self.handle_field_obj.mode_channel()
        self.newnum(key,field,'UUID')

    def paymodesum(self):
        key = self.handle_key_obj.base_key_list("PAYMODESUM")
        field = self.handle_field_obj.mode_channel()
        amount = self.tup_value['Amount']
        self.newsum(key,field,amount)


    def paychannelcount(self):
        key = self.handle_key_obj.base_key_list("PAYCHANNELCOUNT")
        field = self.handle_field_obj.paychannel_channel()
        self.newcount(key,field)

    def paychannelnum(self):
        key = self.handle_key_obj.paychannel_channel_key_list("PAYCHANNELNUM")
        field = self.handle_field_obj.paychannel_channel()
        self.newnum(key,field,'UUID')

    def paychannelsum(self):
        key = self.handle_key_obj.base_key_list("PAYCHANNELSUM")
        field = self.handle_field_obj.paychannel_channel()
        amount = self.tup_value['Amount']
        self.newsum(key,field,amount)

    def paychannelmodecount(self):
        key = self.handle_key_obj.base_key_list("PAYCHANNELMODECOUNT")
        field = self.handle_field_obj.paychannel_mode()
        self.newcount(key,field)

    def paychannelmodesum(self):
        key = self.handle_key_obj.base_key_list("PAYCHANNELMODESUM")
        field = self.handle_field_obj.paychannel_mode()
        amount = self.tup_value['Amount']
        self.newsum(key,field,amount)

    def insertMysqlpay(self):
        if self.tup_value.has_key("OrderID"):
            orderid = self.tup_value["OrderID"]
        else:
            orderid = None
        uuid = self.tup_value["UUID"]
        amount = self.tup_value["Amount"]
        gameid = self.tup_value["GameID"]
        channelid = self.tup_value["ChannelID"]
        timestamp = self.timefield
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime("%Y%m%d%H%M%S", time_struct)
        if self.tup_value.has_key("PayMode"):
            paymode = self.tup_value["PayMode"]
        else:
            paymode = ''
        if self.tup_value.has_key("PayClass"):
           payclass = self.tup_value["PayClass"]
        else:
           payclass = ''
        if self.tup_value.has_key("PayChannel"):
           paychannel = self.tup_value["PayChannel"]
        else:
           paychannel = ''
        if self.tup_value.has_key("OS"):
           os = self.tup_value["OS"]
        else:
           os = ''
        if not orderid:
           sql = """INSERT INTO pay(uuid,amount,gameid,channelid,paymode,payclass,paychannel,os,date) VALUES ("%s","%s", "%s", "%s","%s","%s","%s","%s","%s")""" % (uuid,amount,gameid,channelid,paymode,payclass,paychannel,os,timestamp)
        else:
           sql = """INSERT INTO pay(orderid,uuid,amount,gameid,channelid,paymode,payclass,paychannel,os,date) VALUES ("%s","%s","%s", "%s", "%s","%s","%s","%s","%s","%s")""" % (orderid,uuid,amount,gameid,channelid,paymode,payclass,paychannel,os,timestamp)
        log.debug("付费sql为:%s" % (sql))
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.payset_insert(sql)
