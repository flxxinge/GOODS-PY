#encoding: utf-8
from base_model import BaseModel 

class ItemExtend(BaseModel):

    """
    商品扩展属性信息表
    """

    def __init__(self, conf):
        super(ItemExtend,  self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'item_extend'
        self.fields = {
            'weight': 0, 
            'created_at': 1,
            'updated_at': 1,
            'sku_id': 0
        }
        self.join_key = 'sku_id'
        self.uniq_com_keys = ['sku_id']
        self.incr_update_key = 'updated_at'
