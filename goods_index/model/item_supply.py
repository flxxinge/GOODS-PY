#encoding: utf-8
from base_model import BaseModel 

class ItemSupply(BaseModel):

    """
    lsh supply 映射表
    """

    def __init__(self, conf):
        super(ItemSupply,  self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'item_supply'
        self.fields = {
            'code': 0, 
            'item_id':0
        }
        self.join_key = 'item_id'
        self.uniq_com_keys = ['item_id']
