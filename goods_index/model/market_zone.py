#encoding: utf-8
from base_model import BaseModel 

class MarketZone(BaseModel):

    def __init__(self, conf):
        super(MarketZone, self).initialize(conf)
        self.db_name = conf['db_lsh_market']
        self.table = 'market_zone'
        self.fields = {}
        self.join_key = ''
        self.uniq_com_keys = []

    def get_zone_rids_map(self):
        sql = 'select * from market_zone where is_valid = 1 '
        res = self._mysql.read(self.db_name, sql)
        ret = {}
        for row in res:
            ret[str(row['zone_id'])] = row['rids'].strip(',').split(',')[0]
        return ret
