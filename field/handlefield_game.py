import time

class HandleField(object):

    def __init__(self, tup_value):
        self.tup_value = tup_value

    def channel(self):
        if self.tup_value.has_key('ChannelID'):
            channel = self.tup_value["ChannelID"]
        elif self.tup_value.has_key('channelid'):
            channel = self.tup_value["channelid"]
        else:
            channel = "all"
        return channel

    def channel_list(self):
        field_list = ['all']
        channel = self.channel()
        field_list.append(channel)
        return field_list

    def channel_uuid(self):
        uuid = self.tup_value["uuid"]
        channel = "all"
        field = '%s:%s' % (channel, uuid)
        return field

    def channel_uuid_list(self):
        field_list = []
        uuid = self.tup_value["UUID"]
        channel = self.channel()
        field_list.append('%s:%s' % (channel, uuid))
        field_list.append('all:%s' % (uuid,))
        return field_list

    def channel_day_list(self, day):
        field_list = []
        channel = self.channel()
        field_list.append('%s:%s' % (channel, day))
        field_list.append('all:%s' % (day,))
        return field_list

    def channel_kindld_list(self):
        field_list = ['all']
        kindle = self.tup_value["gameKindId"]
        channel = self.channel()
        field_list.append('%s:%s' % (channel, kindle))
        return field_list
 
    def kindld_bet_channel(self):
        field_list = ['all','all:bet']
        kindle = self.tup_value["gameKindId"]
        channel = self.channel()
        field_list.append('%s:%s' % (channel, kindle))
        field_list.append('%s:bet' % (kindle,))
        return field_list

    def kindld_tax_channel(self):
        field_list = ['all','all:tax']
        kindle = self.tup_value["gameKindId"]
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

    def kindid_max(self):
        kindid = self.tup_value["kindID"]
        field = '%s:max' % (kindid)
        return field

    def kindid_min(self):
        kindid = self.tup_value["kindID"]
        field = '%s:min' % (kindid)
        return field
