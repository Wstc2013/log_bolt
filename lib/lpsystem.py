#-*-coding:utf8-*-
import MySQLdb
import logging
import configparser

log = logging.getLogger('test_kafka')

class lpsystem_operation(object):
    config = configparser.ConfigParser()
    config.read("config/config.ini",encoding='utf-8')
    hostname = config.get("lpsystem", "hostname")
    user = config.get("lpsystem", "username")
    password = config.get("lpsystem", "password")
    port = int(config.get("lpsystem", "port"))

    def __init__(self, db):
        self.db_obj = MySQLdb.connect(host=self.hostname, user=self.user, passwd=self.password, db=db, port=self.port,charset="utf8")
        self.cur = self.db_obj.cursor()

    def mysql_insert(self, sql):
        try:
            self.cur.execute(sql)
            self.db_obj.commit()
        except Exception as e:
            log.debug("数据库插入报错:%s" % (str(e)))
            self.db_obj.rollback()
        self.db_obj.close()

    def payset_insert(self,sql):
        self.cur.execute(sql)
        self.db_obj.commit()
        self.db_obj.close()

    def mysql_select_single(self, sql):
        try:
            self.cur.execute(sql)
            ret = self.cur.fetchone()
            log.debug("数据库查询值为:%s" % (ret))
        except Exception as e:
            self.db_obj.close()
            log.debug("数据库查询报错:%s" % (str(e)))
            return ''
        if ret is None:
           ret = ''
        else:
           ret = ret[0]
        self.db_obj.close()
        return ret

    def mysql_updata(self, sql):
        try:
            self.cur.execute(sql)
            self.db_obj.commit()
        except Exception as e:
            log.debug("数据库更新报错:%s" % (str(e)))
            self.db_obj.rollback()
        self.db_obj.close()

    def mysql_insert_updata(self, select_sql,insert_sql, updata_sql):
        self.cur.execute(select_sql)
        select_ret = self.cur.fetchone()[0]
        log.debug(select_ret)
        if select_ret:
                self.cur.execute(updata_sql)
        else:
            self.cur.execute(insert_sql)
        self.db_obj.commit()
        self.db_obj.close()
    
