import time
class HandleKey(object):

    def __init__(self, tup_value, timefieldname):
        self.timefieldname = timefieldname
        self.tup_value = tup_value
        self.timestamp_list = self.timestamp_list_solt(timefieldname)
        self.game_id = self.game_id_solt()
        self.channel = self.channel()

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

    def timestamp_list_format(self, timestamp_format_list, timefieldname):
        timestamp_list = ['all']
        timestamp = timefieldname
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        for timestamp_format in timestamp_format_list:
            timestamp_list.append(time.strftime(timestamp_format, time_struct))
        return timestamp_list
    
    def timestamp_format(self, timestamp_format, timefieldname):
        time_struct = time.strptime(timefieldname, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime(timestamp_format, time_struct)
        return timestamp

    def timestamp_list_solt(self, timefieldname):
        timestamp_format_list = ["%Y", "%Y%m", "%Y%m%d", "%Y%m%d%H"]
        timestamp_list = self.timestamp_list_format(timestamp_format_list, timefieldname)
        return timestamp_list

    def timestamp_solt(self, timefieldname):
        timestamp = timefieldname
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.strftime("%Y%m%d", time_struct)
        return timestamp

    def game_id_solt(self):
        if self.tup_value.has_key('GameID'):
            game_id = self.tup_value['GameID']
        else:
            game_id = self.tup_value['DeviceInfo']['GameID']
        return game_id

    def base_key_list(self, datatype):
        redis_key_list = []
        for timestamp in self.timestamp_list:
            redis_key = 'ROYAL:%s:%s:%s' % (datatype, self.game_id, timestamp)
            redis_key_list.append(redis_key)
        return redis_key_list

    def base_key(self, datatype, timefieldname):
        timestamp = self.timestamp_solt(timefieldname)
        redis_key = 'ROYAL:%s:%s:%s' % (datatype, self.game_id, timestamp)
        return redis_key

    def channel_key_list(self, datatype):
        redis_channel_key_list = []
        channel = self.channel
        for timestamp in self.timestamp_list:
            redis_channel_key = 'ROYAL:%s:%s:%s:%s' % (datatype, self.game_id, timestamp, channel)
            redis_channel_key_list.append(redis_channel_key)
        return redis_channel_key_list
    
    def os_channel_key_list(self, datatype):
        redis_channel_key_list = []
        os = self.os()
        channel = self.channel
        for timestamp in self.timestamp_list:
            redis_channel_key = 'ROYAL:%s:%s:%s:%s:%s' % (datatype, self.game_id, timestamp, os,channel)
            redis_channel_key_list.append(redis_channel_key)
        return redis_channel_key_list
    
    def mode_channel_key_list(self, datatype):
        redis_channel_key_list = []
        mode = self.mode()
        channel = self.channel
        for timestamp in self.timestamp_list:
            redis_channel_key = 'ROYAL:%s:%s:%s:%s:%s' % (datatype, self.game_id, timestamp, mode,channel)
            redis_channel_key_list.append(redis_channel_key)
        return redis_channel_key_list

    def paychannel_channel_key_list(self, datatype):
        redis_channel_key_list = []
        paychannel = self.paychannel()
        channel = self.channel
        for timestamp in self.timestamp_list:
            redis_channel_key = 'ROYAL:%s:%s:%s:%s:%s' % (datatype, self.game_id, timestamp, paychannel,channel)
            redis_channel_key_list.append(redis_channel_key)
        return redis_channel_key_list
    
    def os_channel_key(self,datatype):
        timestamp = self.timestamp_solt(self.timefieldname)
        channel = self.channel
        os = self.os()
        redis_channel_key = 'ROYAL:%s:%s:%s:%s:%s' % (datatype, self.game_id, timestamp, os,channel)
        return redis_channel_key
        
    def all_key_list(self, datatype):
	redis_channel_key_list = []
        channel = self.channel
        for timestamp in self.timestamp_list:
            redis_channel_key = 'ROYAL:%s:%s:%s:all' % (datatype, self.game_id, timestamp)
            redis_channel_key_list.append(redis_channel_key)
        return redis_channel_key_list

    def channel_key(self, datatype, timefieldname):
        timestamp = self.timestamp_solt(timefieldname)
        channel = self.channel
        redis_channel_key = 'ROYAL:%s:%s:%s:%s' % (datatype, self.game_id, timestamp, channel)
        return redis_channel_key

    def account_key(self, datatype):
        account_key = 'ROYAL:%s:%s:all' % (datatype, self.game_id)
        return account_key

    def login_account_key(self, type_value):
        if type_value == 'UUID':
            login_account_key = 'ROYAL:LOGINACCOUNT:%s:all' % (self.game_id)
            return login_account_key
        elif type_value == 'IMEI':
            login_account_key = 'ROYAL:ACCOUNTDEVICE:%s:all' % (self.game_id)
            return login_account_key

    def data_base_key(self,datatype,date):
        data_base_key = 'ROYAL:%s:%s:%s' % (datatype, self.game_id, date)
        return data_base_key

    def firstlogin_key(self, datatype, date):
        channel = self.channel
        firstlogin_key = 'ROYAL:%s:%s:%s:%s' % (datatype, self.game_id, date, channel)
        return firstlogin_key
    
    def firstlogin_os_channel(self,datatype,date):
        channel = self.channel
        os = self.os()
        firstlogin_os_channel = 'ROYAL:%s:%s:%s:%s:%s' % (datatype, self.game_id, date, os,channel)
        return firstlogin_os_channel

    def payaccount_key_list(self, timefieldname):
        redis_key_list = []
        timestamp_format_list = ["%Y", "%Y%m", "%Y%m%d", "%Y%m%d%H", "%Y%m%d%H%M%S"]
        timestamp_list = self.timestamp_list_format(timestamp_format_list, timefieldname)
        for timestamp in timestamp_list:
            redis_key = 'ROYAL:%s:%s:%s' % ("PAYACCOUNT", self.game_id, timestamp)
            redis_key_list.append(redis_key)
        return redis_key_list

    def payaccount_key(self):
        game_id = self.game_id
        payaccount_key = 'ROYAL:PAYACCOUNT:%s:all' % (game_id)
        return payaccount_key
    
    def all_time_key(self,datatype):
        all_time_key = 'ROYAL:%s:%s:all' % (datatype,game_id)
        return all_time_key
    
    def custom_time_key(self,datatype,timestamp_format):
        #timestamp_format_list = ["%Y%m%d%H"]
        timestamp = self.timestamp_format(timestamp_format, self.timefieldname)
        custom_time_key = 'ROYAL:%s:%s:%s' % (datatype, self.game_id, timestamp)
        return custom_time_key
