#encoding: utf-8
from base_index import BaseIndex
from utils.common_funcs import *
import yaml
import time

class GoodsIndex(BaseIndex):

    """
    商品库索引数据组织类
    """

    def __init__(self, conf):
        super(GoodsIndex, self).initialize(conf)

    def get_indice_conf_key(self):
        return 'goods'

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
        """
        return {
            "%s"%self.get_index_type(): {
                "_all": {
                    "analyzer": "ik_max_word"
                },
                "properties": {
                    "name": {
                        "type": "string",
                        "analyzer": "ik_smart",
                        "include_in_all": True,
                        "search_analyzer": "ik_syno_smart",
                    },
                }
            }
        }
        """

    def read_next(self, i, incr = False, model = 'item_sale'):
        actions = []
        max_update_time = 0
        if incr:
            incr_update_key = self._DT.dispatch(model, 
                                                'get_incr_update_key_alias')
            #获取当前表es内最大更新时间
            max_update_time = self._ES.get_max_value(self.get_index_name(), 
                                                     self.get_index_type(),
                                                     incr_update_key)
        rows = self._DT.dispatch(model, 
                                 'get_rows', 
                                 i * self._bulk_capacity, 
                                 self._bulk_capacity,
                                 max_update_time)
        if rows:
            actions = self._build_actions_by_skus(rows)
        return actions

    def read_local(self, i, model):
        uniq_com_keys_alias = self._DT.dispatch(model, 
                                                'get_uniq_com_keys_alias')
        incr_update_key = self._DT.dispatch(model, 
                                            'get_incr_update_key_alias')
        #获取当前表es内最大更新时间
        max_update_time = self._ES.get_max_value(self.get_index_name(), 
                                              self.get_index_type(),
                                              incr_update_key)
        #获取当前修改过的数据行
        incr_rows = self._DT.dispatch(model, 
                                      'get_incr_rows_by_update_key',
                                      max_update_time,
                                      i * self._bulk_capacity, 
                                      self._bulk_capacity)

        if not incr_rows:
            return []

        """
        根据uniq_com_keys_alias获取es里对应的文档
        todo: 后期更新数据量大时应分区取es文档，避免单次取的文档过多
        """
        should_terms = []
        for v in incr_rows.values():
            terms = {
                'bool': {
                    'must':[]
                }
            }
            for alias_key in uniq_com_keys_alias:
                terms['bool']['must'].append({
                    'term': {alias_key: v[alias_key]}
                })
            should_terms.append(terms)

        count = self._ES.get_count_by_key_terms(self.get_index_name(),
                                              self.get_index_type(),
                                              should_terms)

        docs = self._ES.get_doc_by_key_terms(self.get_index_name(),
                                           self.get_index_type(),
                                           should_terms,
                                           0,
                                           count)
        #update的表数据覆盖es里相应字段
        sources = []
        for doc in docs:
            key = self._DT.dispatch(model, 
                                    'get_uniq_com_keys_by_row',
                                    doc)
            incr_info = {} if not incr_rows.has_key(key) else incr_rows[key]
            doc.update(incr_info)
            sources.append(doc)
        actions = self.compose_actions(sources)
        return actions

    def _get_rows_by_join_key(self, scheme, ids = [] , wheres = []) :
        return self._DT.dispatch(scheme,
                                 'get_rows_by_join_key',
                                 ids = ids,
                                 wheres = wheres)

    def _build_actions_by_skus(self, rows):
        """
        根据主表item_sale 拼商品索引文档

        :param1 表记录rows @list
        :return @list
        """
        sku_ids = data2set(rows, 'sku_id')
        item_ids = data2set(rows, 'item_id')

        #获取商品基本信息
        item_infos = self._get_rows_by_join_key('item_sku', 
                                                item_ids,
                                                ['is_valid=1','status=2'])

        #获取商品类目、仓储
        category_ids = data2set(item_infos.values(), 'category_id')
        category_infos = self._get_rows_by_join_key('item_category',
                                                    category_ids)

        #获取商品等级
        level_infos = self._get_rows_by_join_key('item_level', 
                                                 sku_ids)

        #获取supply id(物美码)
        supply_infos = self._get_rows_by_join_key('item_supply', 
                                                  item_ids)
        codes = data2set(supply_infos.values(), 'code')

        #商品扩展属性、运营设置排序权重
        extend_infos = self._get_rows_by_join_key('item_extend', 
                                                  sku_ids)

        #商品促销信息
        now = int(time.time())
        promo_infos =  self._get_rows_by_join_key('item_promotion', 
                                          sku_ids,
                                         ['begin_at < %s and end_at > %s'%(now, now)])

        #supply info 供货价等
        base_infos = self._get_rows_by_join_key('item_cusar', 
                                                codes)

        #库存信息
        apt_infos = self._get_rows_by_join_key('inventory', 
                                               item_ids)

        sources = []
        for row in rows:
            sku_id = str(row['sku_id'])
            item_id = str(row['item_id'])

            item_info = {} if not item_infos.has_key(item_id) else item_infos[item_id]
            if item_info:
                #合并商品基本信息
                row.update(item_info)

                #合并商品类目、仓储信息
                category_id = str(item_info['category_id'])
                category_info = category_infos[category_id]
                row.update(category_info)

                #合并商品等级
                level_info = {} if not level_infos.has_key(sku_id) else level_infos[sku_id]
                row.update(level_info)

                #合并supply id(物美码)
                supply_info = supply_infos[item_id]
                row.update(supply_info)

                #合并扩展属性
                extend_info = {} if not extend_infos.has_key(sku_id) else extend_infos[sku_id]
                row.update(extend_info)

                #合并促销信息
                promo_info = {} if not promo_infos.has_key(sku_id) else promo_infos[sku_id]
                row.update(promo_info)

                #合并供货价
                zone_conf =  self._zone_setting[str(row['zone_id'])]
                base_key = '%s_%s_%s'%(supply_info['code'], zone_conf['vkorg'], zone_conf['kunnr'])
                base_info = {} if not base_infos.has_key(base_key) else base_infos[base_key]
                row.update(base_info)

                #合并库存
                apt_key = '%s_%s'%(row['item_id'], zone_conf['AREA_ID'])
                apt_info = {} if not apt_infos.has_key(apt_key) else apt_infos[apt_key]
                apt_info['inventory_num'] = 0 if 'INVENTORY_QUANTITY' not in apt_info else apt_info['INVENTORY_QUANTITY']
                row.update(apt_info)

                #合并异常信息
                exception_status = self.__get_goods_exception_status(row)
                row.update(exception_status)
                row['inventory_sort'] = 0
                if row['sale_unit'] * row['moq'] > 0 and \
                    row['inventory_num'] / (row['sale_unit'] * row['moq']) >= 1:
                    row['inventory_sort'] = 1
            sources.append(row)

        return self.compose_actions(sources)

    def compose_actions(self, rows):
        actions = []
        for row in rows:
            action = {
                "_source": row,
                "_id" : row['sku_id'], #根据这个id更新文档,所以需保证sku_id唯一
                "_index" : self.get_index_name(),
                "_type" : self.get_index_type()
            }
            actions.append(action)
        return actions

    def __get_goods_exception_status(self, row):
        """
        获取商品异常状态

        1.A档售罄商品
        2.B档售罄商品
        3.C档售罄商品
        4.供货价为0商品
        5.负毛利商品
        6.供货价异常 表item_sale exception_status字段1位置位
        7.销售价异常
        8.促销价异常 表item_sale exception_status字段2位置位
        9.箱规异常 表item_sale exception_status字段3位置位 (当前没做监控)

        :return @dict
        """
        exception_status = {}
        status_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        for i in status_list:
            exception_status['exception_status_%s' % (str(i))] = 0

        #A,B,C档售罄商品
        try:
            if row['level'] == 1 and row['inventory_num'] / row['sale_unit'] < row['moq']:
                exception_status['exception_status_1'] = 1
            elif row['level'] ==2 and row['inventory_num'] / row['sale_unit'] < row['moq']:
                exception_status['exception_status_2'] = 1
            elif row['level'] == 3 and row['inventory_num'] / row['sale_unit'] < row['moq']:
                exception_status['exception_status_3'] = 1
        except Exception, e:
            pass

        #供货价为0商品
        if 'kbetr' not in row or row['kbetr'] == 0 :
            exception_status['exception_status_4'] = 1
        #负毛利商品
        if 'kbetr' in row and row['kbetr'] * row['sale_unit'] > row['sale_price']:
            exception_status['exception_status_5'] = 1
        #供货价异常 
        if row['exception_status'] & 1 == 1:
            exception_status['exception_status_6'] = 1
        #销售价异常
        if row['price_status'] == 1:
            exception_status['exception_status_7'] = 1
        #促销价异常
        if row['exception_status'] & 2 == 2:
            exception_status['exception_status_8'] = 1
        #箱规异常
        if row['exception_status'] & 4 == 4:
            exception_status['exception_status_9'] = 1
        return exception_status

def main():
    stream = open('../conf/indexer.yml', 'r')
    conf = yaml.load(stream)
    sch = GoodsIndex(conf)
    sch.read_next(0, 10000)

if __name__ == '__main__':
    main()
