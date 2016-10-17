#encoding: utf-8
from base_model import BaseModel 

class ItemCategory(BaseModel):

    """
    类目信息表
    """

    def __init__(self, conf):
        super(ItemCategory, self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'item_category'
        self.fields = {
            'storage_type_id': 0, 
            'id':1,
            'created_at': 1,
            'updated_at': 1
        }
        self.join_key = 'id'
        self.uniq_com_keys = ['id']
        self.incr_update_key = 'updated_at'
