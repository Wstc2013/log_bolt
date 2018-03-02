#-*-coding:utf-8-*-
from handlelogic_game import HandleLogic
from lib import mysql
import configparser
import time
from lib.mysql import mysql_operation
from lib.lpsystem import lpsystem_operation
import requests
import json
import logging
log = logging.getLogger('test_kafka')


class GameHandleLogic(HandleLogic):
    config = configparser.ConfigParser()
    config.read("config/commission.ini",encoding='utf-8')

    def __init__(self, tup):
        self.tup_value = self.tup_value_solt(tup)
        self.timefield = self.tiemfield_solt(self.tup_value['curTime'])
        super(GameHandleLogic, self).__init__()

    def tiemfield_solt(self, tiemfield):
        time_local = time.localtime(tiemfield)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        return dt
    
    #def tup_value_solt(self, tup):
        #value = eval(tup[0][""])
        #return value

    
    def tup_value_solt(self, tup):
        new_value = tup[0]
        if '\xef\xbb\xbf' in new_value:
            value = eval(new_value)
        else:
            value = json.loads(new_value)
        return value

    def sum(self, sum, sumfield):
        if self.tup_value["androidFlag"] == 0:
        	sum_key_list = self.handle_key_obj.base_key_list(sum)
        	sumvaule = self.tup_value[sumfield]
        	log.debug(u"对应的值为:%s" % (sumvaule))
        	fields = self.handle_field_obj.channel_kindld_list()
        	log.debug(u"对应的key为:%s,field为:%s" % (sum_key_list,fields))
        	for sum_key in sum_key_list:
            		for field in fields:
                		self.redis_obj.hincrbyfloat(sum_key, field, sumvaule)

    def syscore(self):
        if self.tup_value["androidFlag"] == 0:
            sumvaule = -self.tup_value['addscore']
            log.debug(u"syscore对应的值为:%s" % (sumvaule))
            fields = self.handle_field_obj.channel_kindld_list()
            sum_key_list = self.handle_key_obj.base_key_list("SYSSCORE")
            log.debug(u"对应的key为:%s,field为:%s" % (sum_key_list,fields))
            super(GameHandleLogic, self).newsum(sum_key_list, fields, sumvaule)

    def mtax(self):
        if self.tup_value["androidFlag"] == 0:
            sumvaule = self.tup_value['servicerevenue']
            log.debug(u"servicerevenue对应的值为:%s" % (sumvaule))
            fields = self.handle_field_obj.channel_kindld_list()
            sum_key_list = self.handle_key_obj.base_key_list("MREVENUESUM")
            log.debug(u"对应的key为:%s,field为:%s" % (sum_key_list,fields))
            super(GameHandleLogic, self).newsum(sum_key_list, fields, sumvaule)
    
    def atax(self):
        if self.tup_value["androidFlag"] == 0:
            sumvaule = self.tup_value['addscore']
            if sumvaule < 0:
                sumvaule = -sumvaule
            sumvaule = sumvaule*0.05
            log.debug(u"addscore对应的值为:%s" % (sumvaule))
            fields = self.handle_field_obj.channel_kindld_list()
            sum_key_list = self.handle_key_obj.base_key_list("AREVENUESUM")
            log.debug(u"对应的key为:%s,field为:%s" % (sum_key_list,fields))
            super(GameHandleLogic, self).newsum(sum_key_list, fields, sumvaule)

    def behavior(self):
        ret = super(GameHandleLogic, self).behavior()
        be_type = 'game'
        strjson = str({'GameID': ret["game_id"], 'ChannelID': ret["channel"]})
        sql = """INSERT INTO behavior(uuid,type, strjson, date) VALUES ("%s", "%s", "%s", "%s")""" % (ret["uuid"], be_type, strjson, ret["timestamp"])
        log.debug(u"佣金行为sql为:%s" % (sql))
        mysql_operation_obj = mysql_operation('logserver')
        mysql_operation_obj.mysql_insert(sql)

    def insertMysqlSysscore(self):
        if self.tup_value["androidFlag"] == 0:
                timestamp = self.timefield
        	time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        	timestamp = time.strftime("%Y%m%d%H%M%S", time_struct)
        	game_id = "ENG-HJDWC-001"
        	gamekindId = self.tup_value["gameKindId"]
        	uuid = self.tup_value['uuid']
        	sysscore = -self.tup_value['addscore']
        	sql = """INSERT INTO sysscore(uuid,gameid, gamekindid, sysscore,timestamp) VALUES ("%s", "%s", "%s", "%s","%s")""" % (uuid, game_id, gamekindId,sysscore,timestamp)
        	log.debug(u"系统输赢sql为:%s" % (sql))
        	mysql_operation_obj = mysql_operation('logserver')
        	mysql_operation_obj.mysql_insert(sql)


    def commission(self):
        uuid = self.tup_value['uuid']
        log.debug(u"佣金uuid为:%s" %(uuid))
        game_id = "ENG-HJDWC-001"
        account_field = self.handle_field_obj.channel_uuid()
        timestamp = self.timefield
        time_struct = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        #timestamp = time.strftime("%Y%m%d%H%M%S", time_struct)
        timestamp_rbd = time.strftime("%Y-%m-%d %H:%M:%S", time_struct)
        account_key = self.handle_key_obj.payaccount_key()
        log.debug(u"佣金付费的key为:%s,field为:%s" % (account_key,account_field))
        kindld = str(self.tup_value['gameKindId'])
        if not self.redis_obj.hexists(account_key, account_field):
            log.debug(u"佣金账号未付费")
            return
        log.debug("佣金账号已付费")
        type_name = self.config.get(kindld, "Type")
        log.debug(u"佣金kindleid为:%s,佣金类型为:%s" %(kindld,type_name))
        rate_id = 0
        while rate_id < 2:
            rate_list = self.config.get(kindld, 'Rates').split(';')
            if  "BET" in type_name :
                log.debug("is bet")
                commission = self.tup_value['totalbet']*float(rate_list[rate_id])
            else:
                log.debug("is commission")
                commission = self.tup_value['servicerevenue']*float(rate_list[rate_id])
            log.debug(u"佣金值为:%s" % (commission))
            if commission < 1:
               return
            select_ta = """SELECT iu from ta where ud='%s'""" % (uuid)
            log.debug(u"查询佣金代理人sql为:%s" % (select_ta))
            mysql_operation_ta_obj = lpsystem_operation('lpsystem')
            i_uuid = mysql_operation_ta_obj.mysql_select_single(select_ta)
            if not i_uuid:
                log.debug(u"佣金代理人不存在")
                return
            if uuid == i_uuid:
                log.debug(u"佣金代理人相等")
                return
            log.debug(u"佣金代理人存在为:%s" % (i_uuid))
            select_rb = """SELECT am from rb where ud='%s'""" % (i_uuid)
            mysql_operation_rb_select_obj = lpsystem_operation('lpsystem')
            select_ret_rb = mysql_operation_rb_select_obj.mysql_select_single(select_rb)
            log.debug(u"代理人%s已有佣金值为%s" % (i_uuid,select_ret_rb))
            if select_ret_rb != '':
                select_ret_rb = float(select_ret_rb) + commission
            	updata_rb = """UPDATE rb set am='%s' where ud='%s'""" % (select_ret_rb, i_uuid)
                mysql_operation_rb_update_obj = lpsystem_operation('lpsystem')
                mysql_operation_rb_update_obj.mysql_updata(updata_rb)
                log.debug(u"已经更新佣金,更新佣金表sql为:%s" % (updata_rb))
            else:
            	insert_rb = """INSERT INTO rb(ud,gi,am) VALUES ("%s", "%s", "%s")""" % (i_uuid, game_id, commission)
                mysql_operation_rb_insert_obj = lpsystem_operation('lpsystem')
                mysql_operation_rb_insert_obj.mysql_insert(insert_rb)
                log.debug(u"已经插入佣金,插入佣金表sql为:%s" % (insert_rb))
            insert_rbd = """INSERT INTO rbd(ud,gi,de,am) VALUES ("%s", "%s", "%s","%s")""" % (i_uuid, game_id, timestamp_rbd, commission)
            log.debug(u"插入佣金操作记录sql为:%s" %(insert_rbd))
            mysql_operation_rdb_obj = lpsystem_operation('lpsystem')
            mysql_operation_rdb_obj.mysql_insert(insert_rbd)
            if commission > 1000:
                log.debug(u"佣金值大于1000进行post操作")
                msgcontent = u"你推荐的玩家给您带来了佣金%s元，请您在佣金界面查收。加油多多推广，躺着也收钱。" % (commission)
                data = {'msgtitle': u"佣金到账提醒", 'msgcontent': msgcontent,'uuid': uuid}
                url = self.config.get("main", 'MailUrl')
                try:
                    r = requests.post(url, data=json.dumps(data))
                except Exception as e:
                    log.debug(u"post错误为:%s" % (str(e)))
            uuid = i_uuid
            rate_id += rate_id

    def statistics(self):
        totalbet_fields = self.handle_field_obj.type_serverid("Totalbet")
        totalbetcount_fields = self.handle_field_obj.type_serverid("TotalbetCount")
        servicerevenue_fields = self.handle_field_obj.type_serverid("ServiceRevenue")
        servicerevenuecount_fields = self.handle_field_obj.type_serverid("ServiceRevenueCount")
        statis_key_list = self.handle_key_obj.base_key_list("STATISTICS")
        log.debug("统计的key为:%s" % (statis_key_list))
        kindld = str(self.tup_value['gameKindId'])
        type_name = self.config.get(kindld, 'Type')
        if 'BET' in type_name:
            log.debug("类型为下注对应的field为:%s和%s" % (totalbet_fields,totalbetcount_fields))
            totalbet = self.tup_value['totalbet']
            log.debug("下注值为:%s" % (totalbet))
            for statis_key in statis_key_list:
                for totalbet_field in totalbet_fields:
                    self.redis_obj.hincrbyfloat(statis_key, totalbet_field, totalbet)
                for totalbetcount_field in totalbetcount_fields:
                    self.redis_obj.hincrby(statis_key, totalbetcount_field)
        else:
            log.debug("类型为付费对应的field为:%s和%s" % (servicerevenue_fields,servicerevenuecount_fields))
            servicerevenue = self.tup_value['servicerevenue']
            log.debug("付费值为:%s" % (servicerevenue))
            for statis_key in statis_key_list:
                for servicerevenue_field in servicerevenue_fields:
                    self.redis_obj.hincrbyfloat(statis_key, servicerevenue_field, servicerevenue)
                for servicerevenuecount_field in servicerevenuecount_fields:
                    self.redis_obj.hincrby(statis_key, servicerevenuecount_field)
    
