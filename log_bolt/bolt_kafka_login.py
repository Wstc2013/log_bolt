#-*-coding:utf8-*-
#!/usr/bin/env python
from __future__ import absolute_import
from pyleus.storm import SimpleBolt
from module.loginhandlelogic import LoginHandleLogic
import logging
import time
log = logging.getLogger('test_kafka')
import configparser
config = configparser.ConfigParser()
config.read("config/config.ini",encoding='utf-8')
logdir = config.get("log", "dir")


class LoginBolt(SimpleBolt):
    
    
    def process_tuple(self,tup):
         value = tup.values
         if value != [''] and value != ['\x03']:
         	log.debug("kafka获取到的数据为:%s" % (value))
                try:
         	    login_handle_logic_obj = LoginHandleLogic(value)
                except Exception as e:
                    return
         	if login_handle_logic_obj.tup_value.has_key('UUID'):
                        #login_handle_logic_obj.insertMysqlLogin()
         		log.debug("开始用户首登处理!!!!")
         		login_handle_logic_obj.firstlogin('FIRSTLOGIN', 'UUID')
                        log.debug("开始os用户首登处理!!!!")
                        login_handle_logic_obj.loginosfirstlogin()
         		log.debug("开始用户行为处理!!!!")
         		login_handle_logic_obj.behavior()
         		log.debug("开始用户登陆账号处理!!!!")
         		login_handle_logic_obj.account('LOGINACCOUNT', 'UUID')
                        log.debug("开始用户os登陆账号处理!!!!")
                        login_handle_logic_obj.loginosaccount()
         		log.debug("开始用户登陆次数处理!!!!")
         		login_handle_logic_obj.count('LOGINCOUNT')
         		log.debug("开始用户登陆人数处理!!!!")
         		login_handle_logic_obj.num('LOGINNUM', 'UUID')
         		log.debug("开始用户登陆留存处理!!!!")
         		login_handle_logic_obj.retained('RETAINED', 'UUID')
                        log.debug("开始os用户登陆次数处理!!!!")
                        login_handle_logic_obj.loginoscount()
                        log.debug("开始os用户登陆人数处理!!!!")
                        login_handle_logic_obj.loginosnum()
                        log.debug("开始os用户登陆留存处理!!!!")
                        login_handle_logic_obj.loginosretained()
         	if login_handle_logic_obj.tup_value['DeviceInfo'].has_key('IMEI'):
	 		log.debug("开始设备首登处理!!!!")
         		login_handle_logic_obj.firstlogin('FIRSTLOGINDEVICE', 'IMEI')
         		log.debug("开始设备登陆账号处理!!!!")
         		login_handle_logic_obj.account('ACCOUNTDEVICE', 'IMEI')
         		log.debug("开始设备登陆次数处理!!!!")
         		login_handle_logic_obj.count('COUNTDEVICE')
         		log.debug("开始设备登陆个数处理!!!!")
         		login_handle_logic_obj.num('NUMDEVICE', 'IMEI')
         		log.debug("开始设备留存处理!!!!")
         		login_handle_logic_obj.retained('RETAINEDDEVICE', 'IMEI')




if __name__ == '__main__':
    log_time = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_filename = '%s/test_login_%s.log' % (logdir,log_time)
    logging.basicConfig(
                level=logging.DEBUG,
                filename=log_filename,
                format="%(asctime)s[%(levelname)s][%(process)d][%(thread)d][%(lineno)d]%(message)s",
                filemode='a',
    )
    LoginBolt().run()
