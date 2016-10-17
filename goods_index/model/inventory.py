#encoding: utf-8
from base_model import BaseModel 

class Inventory(BaseModel):

    """
    库存信息表
    """

    def __init__(self, conf):
        super(Inventory, self).initialize(conf)
        self.db_name = conf['db_lsh_apt']
        self.table = 'INVENTORY_LOGIC'
        self.fields = { 
            'INVENTORY_QUANTITY': 0,
            'CREATED_AT': 1,
            'UPDATED_AT': 1,
            'AREA_ID': 0,
            'STATUS': 1,
            'SKU_ID': 0
        }
        self.join_key = 'SKU_ID'
        self.uniq_com_keys = ['SKU_ID', 'AREA_ID']
        self.incr_update_key = 'UPDATED_AT'
