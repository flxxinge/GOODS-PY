#encoding: utf-8
from base_index import BaseIndex
from utils.common_funcs import *

class TestIndex(BaseIndex):

    """
    商品库索引数据组织类
    """

    def __init__(self, conf):
        super(TestIndex, self).initialize(conf)

    def get_indice_conf_key(self):
        return 'test'

    def get_mapping(self):
        return {
            "%s"%self.get_index_type(): {
                "_all": {
                    "analyzer": "ik_max_word"
                },
                "properties": {
                    "name": {
                        "type": "string",
                        "index": "not_analyzed",
                    },
                }
            }
        }

    def read_next(self, i, incr = False, model = 'item_sale'):
        rows = []
        ts = int(time.time() * 1000)
        if incr:
            if i >= 1: 
                return []
            begin = 93000
            for j in xrange(self._bulk_capacity):
                j += begin
                rows.append({'id' : j, 'ts': ts})
        else:
            begin = i * self._bulk_capacity
            print begin
            if begin > 50000:
                return []
            for j in xrange(self._bulk_capacity):
                j += begin
                rows.append({'id' : j, 'ts': ts})

        return self.compose_actions(rows)

    def compose_actions(self, rows):
        actions = []
        for row in rows:
            action = {
                "_source": row,
                "_id" : row['id'], #根据这个id更新文档,所以需保证sku_id唯一
                "_index" : self.get_index_name(),
                "_type" : self.get_index_type()
            }
            actions.append(action)
        return actions
