#-*-coding:utf8-*-
from handlelogic import HandleLogic
import time
from lib.mysql import mysql_operation
import logging
log = logging.getLogger('test_kafka')


class SetHandleLogic(HandleLogic):

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tiemfield_solt(self.tup_value['SettlementTime'])
        super(SetHandleLogic, self).__init__()

    def tup_value_solt(self, tup):
        try:
           new_value = eval(tup[0][""])
        except Exception as e:
           #new_value = eval(tup[0])
           new_value = eval(tup[0].values()[0])
        if not new_value.has_key('GameID'):
           new_value['GameID'] = 'XXXXXXX'
        return new_value

    def tiemfield_solt(self, tiemfield):
        if tiemfield == "":
            dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        else:
            dt = tiemfield
        return dt

    def account_amount_sum(self):
        sum_key_list = []
        fields = []
        sum_key = self.handle_key_obj.account_key("SETTLEMENTACCOUNTSUM")
        sum_key_list.append(sum_key)
        field = self.tup_value["UUID"]
        fields.append(field)
        sumvaule = self.tup_value["Amount"]
        log.debug(u"对应的key为:%s,filed为:%s,付费金额为:%s" % (sum_key_list, fields, sumvaule))
        super(SetHandleLogic, self).newsum(sum_key_list, fields, sumvaule)

    def behavior(self):
        ret = super(SetHandleLogic, self).behavior()
        amount = self.tup_value['Amount']
        be_type = 'settlement'
        strjson = str({'GameID': ret["game_id"], 'ChannelID': ret["channel"], 'Amount': amount})
        sql = """INSERT INTO behavior(uuid,type, strjson, date) VALUES ("%s", "%s", "%s", "%s")""" % (ret["uuid"], be_type, strjson, ret["timestamp"])
        log.debug(u"结算行为sql为:%s" % (sql)) 
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)

   
    def setoscount(self):
        key = self.handle_key_obj.base_key_list("SETTLEMENTOSCOUNT")
        field = self.handle_field_obj.os_channel()
        self.newcount(key,field)

    def setosnum(self):
        key = self.handle_key_obj.os_channel_key_list("SETTLEMENTOSNUM")
        field = self.handle_field_obj.os_channel()
        self.newnum(key,field,'UUID')

    def setossum(self):
        key = self.handle_key_obj.base_key_list("SETTLEMENTOSSUM")
        field = self.handle_field_obj.os_channel()
        amount = self.tup_value['Amount']
        self.newsum(key,field,amount)

    def insertMysqlset(self):
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
        if self.tup_value.has_key("OS"):
           os = self.tup_value["OS"]
        else:
           os = ''
        if not orderid:
           sql = """INSERT INTO settlement(uuid,amount,gameid,channelid,os,date) VALUES ("%s", "%s", "%s","%s","%s","%s")""" % (uuid,amount,gameid,channelid,os,timestamp)
        else:
           sql = """INSERT INTO settlement(orderid,uuid,amount,gameid,channelid,os,date) VALUES ("%s","%s", "%s", "%s","%s","%s","%s")""" % (orderid,uuid,amount,gameid,channelid,os,timestamp)
        log.debug("付费sql为:%s" % (sql))
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.payset_insert(sql)
