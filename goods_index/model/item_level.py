#encoding: utf-8
from base_model import BaseModel 

class ItemLevel(BaseModel):

    """
    商品等级信息表
    """

    def __init__(self, conf):
        super(ItemLevel, self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'item_level'
        self.fields = {
            'level': 0, 
            'created_at': 1,
            'updated_at': 1,
            'sku_id': 0
        }
        self.join_key = 'sku_id'
        self.uniq_com_keys = ['sku_id']
        self.incr_update_key = 'updated_at'
