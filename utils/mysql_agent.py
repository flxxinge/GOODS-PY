#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import MySQLdb
class MysqlAgent(object):
    def __init__(self, config):
        self.connections = {}
        self.config = config 
        
    def __get_connection(self, dbname, master = False):
        str = 'slave'
        if master:
            str = 'master'
        if self.connections.has_key(dbname) and self.connections[dbname].has_key(str):
            self.connections[dbname][str]['cursor'].close()
        
        self.connections[dbname]  = {}
        self.connections[dbname][str] = {}
        dbinfo = self.config[dbname][str]
        self.connections[dbname][str]['conn'] = MySQLdb.connect(
                    host = dbinfo['host'], 
                    user = dbinfo['user'], 
                    passwd = dbinfo['passwd'], 
                    db = dbname,
                    port = dbinfo['port'],
                    charset = 'utf8',
                    )
        self.connections[dbname][str]['cursor'] = self.connections[dbname][str]['conn'].cursor(MySQLdb.cursors.DictCursor)
        self.connections[dbname][str]['cursor'].execute("set names 'utf8'")
        return self.connections[dbname][str]['cursor']

    def read(self, db, sql, params=None):
        cursor_slave = self.__get_connection(db, False)
        if not params:
            cursor_slave.execute(sql)
        else:
            cursor_slave.execute(sql, params)
        res = cursor_slave.fetchall()
        return res

    def write(self, db, sql, params=None):
        cursor_master = self.__get_connection(db, True)
        if not params:
            cursor_master.execute(sql)
        else:
            cursor_master.execute(sql, params)
        self.connections[db]['master']['conn'].commit()
        return True
