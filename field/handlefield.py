import time

class HandleField(object):

    def __init__(self, tup_value):
        self.tup_value = tup_value

    def channel(self):
        if self.tup_value.has_key('ChannelID'):
            channel = self.tup_value["ChannelID"]
        else:
            channel = self.tup_value['DeviceInfo']['Channel']
        return channel

    def os(self):
        if self.tup_value.has_key('OS'):
            os = self.tup_value["OS"]
        else:
            os = self.tup_value['DeviceInfo']['OS']
        return os
 
    def mode(self):
        mode = self.tup_value["PayMode"]
        return mode

    def paychannel(self):
        paychannel = self.tup_value["PayChannel"]
        return paychannel

    def channel_list(self):
        field_list = ['all']
        channel = self.channel()
        field_list.append(channel)
        return field_list

    def channel_type(self, type_value):
        if type_value == 'UUID':
            s_value = self.tup_value["UUID"]
        elif type_value == 'IMEI':
            s_value = self.tup_value['DeviceInfo']['IMEI']
        channel = self.channel()
        field = '%s:%s' % (channel, s_value)
        return field

    def channel_type_list(self, type_value):
        field_list = []
        if type_value == 'UUID':
            s_value = self.tup_value["UUID"]
        elif type_value == 'IMEI':
            s_value = self.tup_value['DeviceInfo']['IMEI']
        channel = self.channel()
        field_list.append('%s:%s' % (channel, s_value))
        field_list.append('all:%s' % (s_value,))
        return field_list
    
    def os_channel_type_list(self, type_value):
        field_list = []
        if type_value == 'UUID':
            s_value = self.tup_value["UUID"]
        elif type_value == 'IMEI':
            s_value = self.tup_value['DeviceInfo']['IMEI']
        channel = self.channel()
        os = self.os()
        field_list.append('%s:%s:%s' % (os, channel, s_value))
        field_list.append('%s:all:%s' % (os, s_value,))
        return field_list

    def os_channel_type(self, type_value):
        if type_value == 'UUID':
            s_value = self.tup_value["UUID"]
        elif type_value == 'IMEI':
            s_value = self.tup_value['DeviceInfo']['IMEI']
        channel = self.channel()
        os = self.os()
        field = '%s:%s:%s' % (os, channel, s_value)
        return field

    def channel_day_list(self, day):
        field_list = []
        channel = self.channel()
        field_list.append('%s:%s' % (channel, day))
        field_list.append('all:%s' % (day,))
        return field_list

    def os_channel_day_list(self, day):
        field_list = []
        channel = self.channel()
        os = self.tup_value['DeviceInfo']['OS']
        field_list.append('%s:%s:%s' % (os, channel, day))
        field_list.append('%s:all:%s' % (os, day))
        return field_list
        

    def channel_kindld_list(self):
        field_list = ['all']
        kindle = self.tup_value["gameroundID"]
        channel = self.channel()
        field_list.append('%s:%s' % (channel, kindle))
        field_list.append('all:%s' % (kindle,))
        return field_list

    def kindld_bet_channel(self):
        field_list = ['all','all:bet']
        kindle = self.tup_value["gameroundID"]
        channel = self.channel()
        field_list.append('%s:%s' % (channel, kindle))
        field_list.append('%s:bet' % (kindle,))
        return field_list

    def kindld_tax_channel(self):
        field_list = ['all','all:tax']
        kindle = self.tup_value["gameroundID"]
        channel = self.channel()
        field_list.append('%s:%s' % (channel, kindle))
        field_list.append('%s:tax' % (kindle,))
        return field_list
     
    def type_serverid(self, type_value):
        field_list = []
        serverid = self.tup_value["serverID"]
        field_list.append('%s:%s' % (type_value, serverid))
        field_list.append('%s:all' % (type_value,))
        return field_list
   

    def kindid_fen(self,time):
        field_list = []
        kindid = self.tup_value["kindID"]
        field_list.append('%s:%s' % (kindid,time))
        field_list.append('all:%s' % (time))
        return field_list
    
    def os_channel(self):
        field_list = []
        channel = self.channel()
        os = self.os()
        field_list.append('%s:%s' % (os,channel))
        field_list.append('%s:all' % (os))
        return field_list

    def mode_channel(self):
        field_list = []
        channel = self.channel()
        mode = self.mode()
        field_list.append('%s:%s' % (mode,channel))
        field_list.append('%s:all' % (mode))
        return field_list

    def paychannel_channel(self):
        field_list = []
        channel = self.channel()
        paychannel = self.paychannel()
        field_list.append('%s:%s' % (paychannel,channel))
        field_list.append('%s:all' % (paychannel))
        return field_list

    def paychannel_mode(self):
        field_list = []
        mode = self.mode()
        paychannel = self.paychannel()
        field_list.append('%s:%s' % (paychannel,mode))
        field_list.append('%s:all' % (paychannel))
        return field_list

    def kindid_max(self):
        kindid = self.tup_value["kindID"]
        field = '%s:max' % (kindid)
        return field

    def kindid_min(self):
        kindid = self.tup_value["kindID"]
        field = '%s:min' % (kindid)
        return field
