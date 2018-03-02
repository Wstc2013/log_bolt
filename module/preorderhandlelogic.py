#-*-coding:utf-8-*-
from handlelogic import HandleLogic
import time
from lib.mysql import mysql_operation
import logging
log = logging.getLogger('test_kafka')

class PreorderHandleLogic(HandleLogic):

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tup_value['TimeStamp']
        super(PreorderHandleLogic, self).__init__()
    
    def tup_value_solt(self, tup):
        try:
           new_value = eval(tup[0][""])
        except Exception as e:
           new_value = eval(tup[0])
        if not new_value.has_key('GameID'):
           new_value['GameID'] = 'XXXXXXX'
        return new_value

    def preorderoscount(self):
        key = self.handle_key_obj.base_key_list("PREORDEROSCOUNT")
        field = self.handle_field_obj.os_channel()
        self.newcount(key,field)

    def preordermodecount(self):
        key = self.handle_key_obj.base_key_list("PREORDERMODECOUNT")
        field = self.handle_field_obj.mode_channel()
        self.newcount(key,field)

    def preordermodenum(self):
        key = self.handle_key_obj.mode_channel_key_list("PREORDERMODENUM")
        field = self.handle_field_obj.mode_channel()
        self.newnum(key,field,'UUID')

    def preordermodesum(self):
        key = self.handle_key_obj.base_key_list("PREORDERMODESUM")
        field = self.handle_field_obj.mode_channel()
        amount = self.tup_value['Amount']
        self.newsum(key,field,amount)

    def preorderchannelmodecount(self):
        key = self.handle_key_obj.base_key_list("PREORDERCHANNELMODECOUNT")
        field = self.handle_field_obj.paychannel_mode()
        self.newcount(key,field)
