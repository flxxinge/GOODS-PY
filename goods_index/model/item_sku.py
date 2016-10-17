#encoding: utf-8
from base_model import BaseModel 

class ItemSku(BaseModel):

    """
    商品基本属性信息表
    """

    def __init__(self, conf):
        super(ItemSku, self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'item_sku'
        self.fields = {
            'status': 1, 
            'is_valid': 0,
            'name': 0,
            'category_id': 0,
            'top_cat': 0,
            'second_cat': 0,
            'third_cat': 0,
            'barcode': 0,
            'brand': 0,
            'properties': 1,
            'created_at': 1, 
            'updated_at': 1,
            'item_id': 0,
            'img_list': 0,
        }
        self.join_key = 'item_id'
        self.uniq_com_keys = ['item_id']
        self.incr_update_key = 'updated_at'
