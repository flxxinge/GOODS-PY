#encoding: utf-8
from data_dispatcher import DataDispatcher
from utils.es_helper import EsHelper

class BaseIndex(object):
    def initialize(self, conf):
        #配置
        self._conf = conf
        self.__logger = conf['__logger']
        #数据请求分发器实例
        self._DT = DataDispatcher(conf) 
        #es实例
        self._ES = EsHelper(conf['es_hosts'], self.__logger)
        #业务真正使用的索引名，对应es里是一个别名
        self._alias_index_name = self.get_index_alias_name()
        #批量提交文档大小
        self._bulk_capacity = self.get_bulk_capacity()
        #地域配置信息
        self._zone_setting = self.get_vkorg_kunnr_setting()
        #根据规则生成work_index new_index
        self._work_index, self._new_index = self.__construct_work_new_index()

    def get_indice_conf_key(self):
        """
        索引信息在配置文件indices下key名 业务子类需实现
        """
        raise NotImplementedError

    def get_mapping(self):
        """
        索引mapping 业务子类需实现
        """
        raise NotImplementedError

    def read_next(self, i, incr = False, model = 'item_sale'):
        """
        分区读取多表联合数据 业务子类需实现
        """
        raise NotImplementedError

    def read_local(self, i, model):
        """
        分区读取单表更新数据 业务子类需实现
        """
        raise NotImplementedError

    def set_index_name(self, index_name):
        """
        build与update应当设置当前索引名
        """
        self._index_name = index_name

    def get_index_name(self):
        """
        获取当前实例索引名
        """
        return self._index_name

    def get_index_alias_name(self):
        """
        获取配置内业务具体使用的索引名
        """
        if not self.get_indice_conf_key() or self.get_indice_conf_key() not in self._conf['indices']:
            return None
        return self._conf['indices'][self.get_indice_conf_key()]['index_name']

    def get_index_type(self):
        """
        获取配置索引类型
        """
        if not self.get_indice_conf_key() or self.get_indice_conf_key() not in self._conf['indices']:
            return None
        return self._conf['indices'][self.get_indice_conf_key()]['index_type']

    def get_bulk_capacity(self):
        """
        获取配置bulk大小
        """
        return int(self._conf['index']['bulk_capacity'])

    def get_index_analyzer(self):
        """
        获取配置index_analyzer
        """
        return self._conf['index']['index_analyzer']

    def get_search_analyzer(self):
        """
        获取配置search_analyzer
        """
        return self._conf['index']['search_analyzer']

    def get_default_analyzer(self):
        """
        获取配置default_analyzer
        """
        return self._conf['index']['default_analyzer']

    def get_number_of_shards(self):
        """
        获取配置分片数
        """
        return self._conf['index']['number_of_shards']

    def get_number_of_replicas(self):
        """
        获取配置副本数
        """
        return self._conf['index']['number_of_replicas']

    def get_settings(self):
        """
        获取索引建立settings
        """
        settings = {
            "settings":{
                "number_of_shards": self.get_number_of_shards(),
                "number_of_replicas": self.get_number_of_replicas()
            }
        }
        return settings

    def get_vkorg_kunnr_setting(self):
        """
        获取zone 进货价 库存 区域id map

        :return @dict
        """
        rids_map = self._DT.dispatch('market_zone', 'get_zone_rids_map')
        return { 
            '1000' : {
                'vkorg' : 'S007',
                'kunnr' : '0000151001',
                'rid' : rids_map['1000'],
                'AREA_ID': '1'
            },
            '1001' : {
                'vkorg' : 'S038',
                'kunnr' : '0000151002',
                'rid' : rids_map['1001'],
                'AREA_ID' : '2'
            },
        }

    def create_index(self):
        """
        创建索引
        1.删除new_index 
        2.创建new_index 
        3.将业务真正使用的别名索引链接到new_index 
        4.删除与work_index的链接
        """

        self._ES.delete(self._new_index)
        self._ES.create(self._new_index, self.get_settings())
        self._ES.put_mapping(self._new_index, 
                             self.get_index_type(),
                             self.get_mapping())
        i = 0
        self.set_index_name(self._new_index)
        while True:
            actions  = self.read_next(i)
            self._ES.bulk(actions)
            if len(actions) < self._bulk_capacity:
                break
            i += 1
        if not self._ES.count(self._new_index):
            self.__logger.error('create new index fail ...........')
            return
        self._ES.put_alias(self._new_index, self._alias_index_name)
        self._ES.delete_alias(self._work_index, self._alias_index_name)

    def update_index(self, model, incr = False): 
        """
        增量更新索引
        如果work_index不存在 则新建work_index
        """

        if not self._ES.exists(self._work_index):
            self.create_index()
            return True
        self.set_index_name(self._work_index)
        i = 0
        while True:
            actions  = self.read_next(i, True) if incr else self.read_local(i, model)
            self._ES.bulk(actions)
            self.__logger.info('incr_actions ---------------- ' + str(actions))
            self.__logger.info('len incr_actions ----------------%s' % len(actions))
            if len(actions) < self._bulk_capacity:
                break
            i += 1

    def __construct_work_new_index(self):
        """
        根据业务使用的索引名（实际为es里的别名）,获取work_index,new_index
        work_index为当前业务使用的索引指向的es内索引名
        重建索引时需新建new_index
        通过后缀 '.1' 与 '.2' 不断切换work_index,new_index

        """
        work_index = self._ES.get_current_index_name_by_alias(self._alias_index_name)
        if not work_index:
            work_index = self._alias_index_name + '.1'
            new_index = self._alias_index_name + '.2'
        elif work_index[-2:] == '.1':
            new_index = self._alias_index_name + '.2'
        else: 
            new_index = self._alias_index_name + '.1'
        return work_index, new_index

