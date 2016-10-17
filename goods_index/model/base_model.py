#encoding: utf-8
from utils.mysql_agent import MysqlAgent

class BaseModel(object):

    def initialize(self, conf):
        self._conf = conf
        #库名 @str
        self.db_name = ''
        #表名 @str
        self.table = ''
        #存入es表字段 @dict key为字段名，value为是否需要重命名，重命名规则为表名+'_'+字段名
        self.fields = {}
        #表关联字段 @str 
        self.join_key = ''
        #唯一确定一条记录的一组表字段组合 @list 用于数据一对一关联
        self.uniq_com_keys = []
        #表更新字段 @str
        self.incr_update_key = ''
        #mysql实例
        self._mysql = MysqlAgent(self._conf['db_conf'])
        self.__logger = conf['__logger']

    def get_join_key_alias(self):
        """
        获取join_key在es里的别名

        :return str
        """
        return self.get_field_alias_name(self.join_key)

    def get_incr_update_key_alias(self):
        """
        获取self.incr_update_key在es里的别名

        :return str
        """
        return self.get_field_alias_name(self.incr_update_key)

    def get_uniq_com_keys_alias(self):
        """
        获取uniq_com_keys在es里的别名

        :return list
        """
        keys = []
        for key in self.uniq_com_keys:
            keys.append(self.get_field_alias_name(key))
        return keys

    def get_uniq_com_keys_by_row(self, row):
        """
        根据uniq_com_keys与row里对应的值，拼接关联key值
        
        :param1 一行记录，@dict, 包含uniq_com_keys
        :return str
        """
        key = ''
        for k in self.uniq_com_keys:
            k = self.get_field_alias_name(k)
            key += str(row[k]) + '_'
        key = key.strip('_')
        return key

    def _build_sql_fields(self):
        """
        根据self.fields 构造sql查询field
        
        :return str
        """
        fields = []
        for field, alias in self.fields.items():
            if alias:
                fields.append(field + ' as ' + self._build_alias_field(field))
            else:
                fields.append(field)
        return ','.join(fields)

    def _build_alias_field(self, field):
        """
        构造field别名

        :param1 field @str
        :return @str
        """
        return self.table + '_' + field

    def get_field_alias_name(self, field):
        """
        获取field别名

        :param1 field @str
        :return @str
        """
        return self._build_alias_field(field) if self.fields[field] else field

    def get_rows_by_join_key(self, ids = [], wheres = []):
        """
        根据ids 获取相应数据行 

        :param1 ids @list
        :param2 wheres @list
        :return @dict
        """
        where = self._build_where(ids, wheres)
        sql = 'select ' + self._build_sql_fields() + ' from ' + self.table + where 
        self.__logger.info(sql)
        rows = self._mysql.read(self.db_name, sql)
        return self._build_uniq_key_dict(rows)

    def _build_uniq_key_dict(self, rows):
        """
        构造唯一key字典

        :param1 rows @list
        :return @dict
        """
        map = {}
        for row in rows:
            key = self.get_uniq_com_keys_by_row(row)
            map[key] = row
        return map

    def get_incr_rows_by_update_key(self, cur_max_value, offset, limit):
        """
        获取self.incr_update_key > cur_max_value 的行

        :param1 cur_max_value 当前es表记录最大更新时间 @mix
        :return @dict
        """
        sql = ' select %s from %s where %s > %s ' % (self._build_sql_fields(), \
                                                     self.table, self.incr_update_key,cur_max_value)
        sql += ' limit %s,%s'%(offset, limit)
        self.__logger.info(sql)
        rows = self._mysql.read(self.db_name, sql)
        return self._build_uniq_key_dict(rows)

    def _build_where(self, ids = [], wheres = []):
        """
        构造join key查询sql where

        :param1 ids @list
        :param1 wheres @list
        :return @str
        """
        conds = []
        ids_s = ''
        for id in ids:
            if id:
                ids_s += str(id) + ','
        ids_s = ids_s.strip(',')
        if ids_s:
            conds.append(self.join_key + ' in (%s) ' % (ids_s))
        if wheres:
            conds.extend(wheres)

        return '' if not conds else ' where ' + ' and '.join(conds)

