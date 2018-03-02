#-*-coding:utf-8-*-
#from handlelogic import HandleLogic
from handlelogic_game import HandleLogic
from field.handlefield_game import HandleField
from key.handlekey_game import HandleKey
import time
from lib.mysql import mysql_operation
import logging
log = logging.getLogger('test_kafka')

class BindphoneHandleLogic(HandleLogic):

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tup_value['date']
        super(BindphoneHandleLogic, self).__init__()
    
    def tup_value_solt(self, tup):
        try:
           new_value = eval(tup[0][""])
        except Exception as e:
           new_value = eval(tup[0])
        #if not new_value.has_key('gameid'):
           #new_value['gameid'] = 'ENG-HJDWC-001'
        if new_value['gameid'] == '':
           new_value['gameid'] = 'ENG-HJDWC-001'
        return new_value

    def insertMysqlBindphone(self):
        uuid = self.tup_value["uuid"]
        phone = self.tup_value["phone"]
        channelid = self.tup_value["channelid"]
        gameid = self.tup_value["gameid"]
        timestamp = self.timefield
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime("%Y%m%d%H%M%S", time_struct)
        sql = """INSERT INTO bindphone(uuid,phone, channelid, gameid,date) VALUES ("%s", "%s", "%s", "%s","%s")""" % (uuid, phone,channelid,gameid,timestamp)
        log.debug("绑定手机sql为:%s" % (sql))
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)
