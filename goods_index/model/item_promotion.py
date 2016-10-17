#encoding: utf-8
from base_model import BaseModel 

class ItemPromotion(BaseModel):

    """
    商品促销信息表
    """

    def __init__(self, conf):
        super(ItemPromotion,  self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'item_promotion'
        self.fields = {
            'sku_id': 0,
            'promo_type': 0,
            'promo_detail': 0,
            'begin_at': 0,
            'end_at': 0,
            'status': 1,
            'created_at': 1, 
            'updated_at': 1,
            'inventory_num': 1,
            'is_recom': 1,
            'weight': 1,
            'recom_pos': 1,
            'activity_id': 0 
        }
        self.join_key = 'sku_id'
        self.uniq_com_keys = ['sku_id']
        self.incr_update_key = 'updated_at'

