#encoding: utf-8
from base_model import BaseModel 

class ItemSale(BaseModel):

    """
    商品基本销售属性信息表
    """

    def __init__(self, conf):
        super(ItemSale, self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'item_sale'
        self.fields = { 
            'id': 0,
            'sku_id': 0,
            'status': 0,
            'op_status': 0,
            'created_at': 0,
            'updated_at': 0,
            'exception_status': 0,
            'promo_status': 0,
            'sale_type': 0,
            'zone_id': 0,
            'sale_price': 0,
            'need_effect_price': 0,
            'import_price': 0,
            'price_status': 0,
            'sale_unit': 0,
            'sale_unit_name': 0,
            'moq': 0,
            'order_limit': 0,
            'day_limit': 0,
            'item_id': 0
        }
        self.join_key = 'sku_id'
        self.uniq_com_keys = ['sku_id']
        self.incr_update_key = 'updated_at'

    def get_rows(self, offset = 0, limit = 1000, max_update_time = 0):
        where = ''
        if max_update_time > 0:
            where = ' where ' + self.incr_update_key + ' > %s' % (max_update_time)
        sql = 'select ' + self._build_sql_fields() + ' from ' + self.table \
                + where + ' order by id limit %s,%s'%(offset, limit)
        return self._mysql.read(self.db_name, sql)
