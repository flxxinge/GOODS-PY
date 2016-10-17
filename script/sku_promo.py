#encoding:utf-8
import argparse
import sys
sys.path.append('..')
from utils.mysql_agent import MysqlAgent
from utils.common_funcs import *
import time
import yaml
import redis
import json
from collections import defaultdict
reload(sys)
sys.setdefaultencoding('utf8')

class SkuPromo(object):
    def __init__(self, conf):
        #配置
        self._conf = conf
        #mysql实例
        self._mysql = MysqlAgent(self._conf['db_conf'])
        #lsh_market
        self._db_lsh_market = conf['db_lsh_market']
        #lsh_base
        self._db_lsh_base = conf['db_lsh_base']
        #lsh_apt
        self._db_lsh_apt = conf['db_lsh_apt']
        #redis client
        self.__redis_client = redis.StrictRedis(host=conf['redis']['host'],
                                                port=conf['redis']['port'])
        #sku促销信息hash map
        self.__redis_sku_promo_info_hmap = 'lsh_market::search_rank::sku_promo_info'
        #销售信息key
        self.__redis_sale_key = 'lsh_market::search_rank::recent_month_sale'
        #销售信息过期时间
        self.__redis_sale_key_expire = 6 * 60 * 60
        #加载各种信息用于计算
        self.__load_data()

    def _get_sku_rows(self):
        """
        获取当前所有sku信息

        :return @dict
        """

        sql = '''
            select distinct a.sku_id, a.status, a.item_id, a.zone_id, a.sale_type, a.sale_unit, a.moq,
                 b.name, b.brand, c.name as second_cat_name, d.name as third_cat_name 
            from item_sale as a 
            left join item_sku as b on a.item_id = b.item_id and b.is_valid = 1 and b.status=2
            left join item_category as c on b.second_cat = c.id 
            left join item_category as d on b.third_cat = d.id 
            '''
        rows = self._mysql.read(self._db_lsh_market, sql)
        print len(rows)
        return rows

    def _get_inventory(self, ids = []):
        """
        获取当前库存信息
        
        :param1 ids => item_id  @list
        :return @dict
        """
        ids_s = join_int_ids(ids)
        sql = '''select INVENTORY_QUANTITY, AREA_ID, STATUS, SKU_ID 
                from INVENTORY_LOGIC where SKU_ID in (%s)'''%(ids_s)
        rows = self._mysql.read(self._db_lsh_apt, sql)
        map = {}
        for row in rows:
            key = '%s_%s'%(row['SKU_ID'], row['AREA_ID'])
            map[key] = row
        return map

    def _get_activitys_promotion(self):
        """
        获取当前所有有效活动促销信息

        :return @dict
        """
        now = int(time.time())
        sql = '''select t1.type, t1.promo_type,t1.promo_detail,t2.activity_id,t2.sku_id 
                from promotion_activitys t1 
                left join promotion_sku t2 on t1.id = t2.activity_id 
                where begin_at < %s and end_at > %s and t2.status=0 and t1.status=0'''%(now, now)
        rows = self._mysql.read(self._db_lsh_market, sql)
        map = {}
        for row in rows:
            key = '%s_%s'%(row['sku_id'], row['promo_type'])
            map[key] = row
        return map

    def _get_item_promotion(self, ids = []):
        """
        获取当前所有有效的表item_promotio促销信息

        :return @dict
        """

        now = int(time.time())
        ids_s = join_int_ids(ids)
        sql = '''select * from item_promotion 
                 where sku_id in (%s) and begin_at < %s and end_at > %s''' % (ids_s, now, now)
        rows = self._mysql.read(self._db_lsh_market, sql)
        map = {}
        for row in rows:
            key = '%s_%s'%(row['sku_id'], row['promo_type'])
            map[key] = row
        return map

    def _get_package_info(self):
        """
        获取当前所有有效的套餐信息

        :return @dict
        """
        sql = 'select * from package a left join package_detail b on a.package_id=b.package_id where a.status = 2 and b.status=2'
        rows = self._mysql.read(self._db_lsh_market, sql)
        map = defaultdict(list)
        for row in rows:
            key = '%s'%(row['package_id'])
            map[key].append(row)
        return map

    def _get_vkorg_kunnr_setting(self):
        """
        获取zone 进货价 库存 区域id map

        :return @dict
        """
        sql = 'select * from market_zone where is_valid = 1 '
        res = self._mysql.read(self._db_lsh_market, sql)
        rids_map = {}
        for row in res:
            rids_map[str(row['zone_id'])] = row['rids'].strip(',').split(',')[0]

        return { 
            '1000' : {
                'vkorg' : 'S007',
                'kunnr' : '0000151001',
                'rid' : rids_map['1000'],
                'AREA_ID': '1'
            },
            '1001' : {
                'vkorg' : 'S038',
                'kunnr' : '0000151002',
                'rid' : rids_map['1001'],
                'AREA_ID' : '2'
            },
        }

    def _get_sku_sale_infos(self):
        """
        从缓存里获取销量信息

        :return @dict
        """
        info = self.__redis_client.get(self.__redis_sale_key)
        if not info:
            info = self.stat_sku_sale()
        else: 
            info = json.loads(info)
        return info

    def __load_data(self):
        self._sku_rows = self._get_sku_rows()
        self._zone_setting = self._get_vkorg_kunnr_setting()
        sku_ids = data2set(self._sku_rows, 'sku_id')
        item_ids = data2set(self._sku_rows, 'item_id')

        self._sku_infos =  rows_to_dict_uniq(self._sku_rows, 'sku_id', True)
        self._apt_infos = self._get_inventory(item_ids)
        self._item_promo_infos = self._get_item_promotion(sku_ids)
        self._act_promon_infos = self._get_activitys_promotion()
        self._package_infos = self._get_package_info()
        #系统拆分后拿不到销量信息不在商城维护
        #self._sku_sale_infos = self._get_sku_sale_infos()

    def _is_tj(self, sku_id):
        """
        判断sku是否是特价，与活动无关，item_promotion有效即可

        :param1 sku_id
        :return 0 or 1
        """
        promo_status = 1
        pro_key = '%s_%s'%(sku_id, promo_status)
        if self._item_promo_infos.has_key(pro_key):
            return 1
        return 0

    def _is_mj(self, sku_id):
        """
        判断sku是否是满减，必须挂活动，activitys_promotion

        :param1 sku_id
        :return 0 or 1
        """
        promo_status = 2 
        pro_key = '%s_%s'%(sku_id, promo_status)
        if self._act_promon_infos.has_key(pro_key) : 
            return 1
        return 0

    def _is_mz(self, sku_id):
        """
        判断sku是否是买赠，与活动无关，item_promotion有效即可

        :param1 sku_id
        :return 0 or 1
        """
        promo_status = 3
        pro_key = '%s_%s'%(sku_id, promo_status)
        if self._item_promo_infos.has_key(pro_key):
            return 1
        return 0

    def _is_ms(self, sku_id, sale_type):
        """
        判断sku是否是套餐秒杀，必须挂活动，item_promotion有效即可与activitys_promotion有效

        :param1 sku_id
        :param2 sale_type
        :return 0 or 1
        """
        promo_status = 5
        pro_key = '%s_%s'%(sku_id, promo_status)
        if sale_type == 2 and self._item_promo_infos.has_key(pro_key) and self._act_promon_infos.has_key(pro_key) : 
            return 1
        return 0

    def _is_tc(self, sku_id, sale_type):
        """
        判断是否是套餐

        :param1 sku_id
        :param2 sale_type
        :return 0 or 1
        """
        if sale_type == 3 and self._package_infos.has_key(sku_id):
            return 1
        return 0

    def _cal_tc_inventory(self, package_id):
        """
        根据套餐id计算套餐库存

        :param1 package_id
        :return int
        """
        tc_inventory = None
        if self._package_infos.has_key(package_id):
            skus = self._package_infos[package_id]
            for row in skus:
                num = 1 if not row['num'] else row['num']
                sku_inventory = int (self._cal_sku_inventory(row['sku_id'], row['zone_id']) / num)
                if tc_inventory == None or tc_inventory > sku_inventory:
                    tc_inventory = sku_inventory
        return 0 if tc_inventory == None else tc_inventory

    def _cal_ms_inventory(self, sku_id):
        """
        根据sku_id计算秒杀库存

        :param1 sku_id
        :return int
        """
        promo_status = 5
        pro_key = '%s_%s'%(sku_id, promo_status)
        if self._item_promo_infos.has_key(pro_key):
            return self._item_promo_infos[pro_key]['inventory_num']
        return 0

    def _cal_sku_inventory(self, sku_id, zone_id):
        """
        根据sku_id计算sku库存

        :param1 sku_id
        :param2 zone_id
        :return int
        """
        zone_conf =  self._zone_setting[str(zone_id)]
        row = self._sku_infos[str(sku_id)]
        item_id = row['item_id']
        apt_key = '%s_%s'%(item_id, zone_conf['AREA_ID'])
        if self._apt_infos.has_key(apt_key):
            inventory_num = self._apt_infos[apt_key]['INVENTORY_QUANTITY']
            sale_unit = 1 if not row['sale_unit'] else row['sale_unit']
            return int(inventory_num / sale_unit)
        return 0

    def _get_promo_status_info(self, sku_id, sale_type):
        """
        获取sku各种促销活动，如果秒杀和套餐有效，则重新判断有效库存

        :param1 sku_id
        :param2 sale_type
        :return @dict
        """
        status_info = {
            'is_tj' : self._is_tj(sku_id),
            'is_mj' : self._is_mj(sku_id),
            'is_mz' : self._is_mz(sku_id),
            'is_ms' : self._is_ms(sku_id, sale_type),
            'is_tc' : self._is_tc(sku_id, sale_type)
        }

        if status_info['is_mj'] == 1 and self._cal_ms_inventory(sku_id) == 0:
            status_info['is_sold_out'] = 0

        if status_info['is_tc'] == 1 and self._cal_tc_inventory(sku_id) == 0:
            status_info['is_sold_out'] = 0
        return status_info

    def process(self):
        '''
        获取 sku_id,name,二级类目,三级类目,是否在架,是否售罄,当前是否参加特价促销,
        当前是否参加满减活动,当前是否参加买赠促销,当前是否参加秒杀活动,
        当前是参加套餐促销,和最近一个月销售信息,存入缓存
        '''
        sku_promo_infos = []
        for row in self._sku_rows:
            row['sku_id'] = str(row['sku_id'])
            promo_info = {
                'sku_id': row['sku_id'],
                'name': row['name'],
                'sec_cat': row['second_cat_name'],
                'third_cat': row['third_cat_name'],
                'is_on_sale': 1 if row['status'] == 2 else 0, #是否在架
                'is_sold_out':0, #售罄
                'is_tj':0, #特价 item_promotion
                'is_mj':0, #满减 promotion_sku promotion_activitys
                'is_mz':0, #买赠 item_promotion
                'is_ms':0, #秒杀 item_promotion promotion_sku promotion_activitys
                'is_tc':0, #套餐
                'recent_month_sale':0 #最近一个月销量
            }
            #获取售罄信息
            promo_info['is_sold_out'] = 1 if self._cal_sku_inventory(row['sku_id'], row['zone_id']) >= row['moq'] else 0
            #获取销售信息 系统拆分后拿不到销量信息不在商城维护
            #if self._sku_sale_infos.has_key(row['sku_id']):
            #    promo_info['recent_month_sale'] = self._sku_sale_infos[row['sku_id']]['count'] 
            #获取促销状态
            promo_status_info = self._get_promo_status_info(row['sku_id'], row['sale_type'])
            promo_info.update(promo_status_info)
            sku_promo_infos.append(promo_info)

        #存入缓存
        count = 0
        for row in sku_promo_infos:
            if row['is_tj'] or row['is_mj'] or row['is_mz'] or row['is_ms'] or row['is_tc']: 
                print row['sku_id'], row
            self.__redis_client.hset(self.__redis_sku_promo_info_hmap, str(row['sku_id']), json.dumps(row))
            count += 1
        print count

    def stat_sku_sale(self):
        """
        获取sku最近一个月销售信息,更新缓存

        :return @dict
        """
        ts = int(time.time()) - 30 * 24 * 60 * 60
        sql = """select b.sku_id, sum(1) as count from order_head as a
                inner join order_detail as b on a.order_id = b.order_id
                where a.status != 5 and a.created_at > %s group by b.sku_id """ % (ts)
        print sql
        rows = self._mysql.read(self._db_lsh_market, sql)
        sale_infos ={}
        for row in rows:
            sale_infos[str(row['sku_id'])] = {'count':int(row['count'])}
        self.__redis_client.set(self.__redis_sale_key, json.dumps(sale_infos))
        self.__redis_client.expire(self.__redis_sale_key, self.__redis_sale_key_expire)
        return sale_infos

def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('-c', '--conf', dest='config',
                       help='specify config file path', nargs='?',
                       default='../conf/sku_promo_conf.yml')
    parser.add_argument('-a', '--action', dest='action',
                       help='action name stat or info', nargs='?',
                       default='info')
    args = parser.parse_args()
    action = args.action
    conf_file = args.config
    stream = open(conf_file, 'r')
    conf = yaml.load(stream)
    processor = SkuPromo(conf)
    if action == 'info':
        processor.process()
    elif action == 'stat':
        processor.stat_sku_sale()
    else:
        print 'unsupported action ............'
    end_time = time.time()
    print "build sku promo info consumed %s seconds" % (end_time - start_time)

if __name__ == '__main__':
    main()
