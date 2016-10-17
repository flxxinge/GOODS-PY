#encoding: utf-8
from base_model import BaseModel 

class ItemCusar(BaseModel):

    """
    供货价信息表
    """

    def __init__(self, conf):
        super(ItemCusar, self).initialize(conf)
        self.db_name = conf['db_lsh_base']
        self.table = 'item_cusar'
        self.fields = { 
            'sku_id': 1 ,
            'vkorg': 0, 
            'kunnr': 0, 
            'kbetr': 0, 
            'mmsta':0, 
            'start_time': 0, 
            'end_time': 0 
        }
        self.join_key = 'sku_id'
        self.uniq_com_keys = ['sku_id', 'vkorg', 'kunnr']
