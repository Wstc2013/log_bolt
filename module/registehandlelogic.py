#-*-coding:utf-8-*-
from handlelogic import HandleLogic
import time
from lib.mysql import mysql_operation
import logging
log = logging.getLogger('test_kafka')

class RegisteHandleLogic(HandleLogic):

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tup_value['TimeStamp']
        super(RegisteHandleLogic, self).__init__()
    
    def tup_value_solt(self, tup):
        try:
           new_value = eval(tup[0][""])
        except Exception as e:
           new_value = eval(tup[0])
        if not new_value['DeviceInfo'].has_key('GameID'):
	   new_value['DeviceInfo']['GameID'] = 'XXXXXXX'
        return new_value

    def insertMysqlRegister(self):
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
        sql = """INSERT INTO register(gameid,channel,devicetype,imei,mac,ipv4,os,uuid,username,latitude,longitude,timestamp) VALUES ("%s", "%s", "%s", "%s","%s","%s","%s","%s","%s","%s","%s","%s")""" % (gameid,channel,devicetype,imei,mac,ipv4,os,uuid,username,latitude,longitude,timestamp)
        log.debug(sql)
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)
