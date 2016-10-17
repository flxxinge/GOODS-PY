import json
import time
import requests
import hashlib
class InventoryHelper(object):

    def __init__(self):
        self._gateway_dev = 'http://atp.lsh123.com/api/atp/java'
        self._gateway = 'http://api.atp.lsh123.com:9100/api/atp/java'
        self._api_version = 'v1.0';
        self._platform = 2;
        self._channel = 1;

    def query(self, rid, item_ids):
        items = []
        for id in item_ids:
            items.append({'item_id' : id})
        uri = '/v1/inventory/query'
        params = { 
            'area_code' : rid,
            'items' : items,
        }
        ret = self._request(uri, params);
        ids_map = {}
        for item in ret['items']:
            ids_map[str(item['item_id'])] = item['qty']
        return ids_map

    def _request(self, uri, params):
        #if (Utils_Common::getEnv() == 'dev'):
        if True:
            url = self._gateway_dev + uri
        else:
            url = self._gateway + uri

        params['channel'] = self._channel
        params = json.dumps(params)
        #timeout = 30
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'api-version': self._api_version,
            'platform': self._platform ,
            'random': self.__md5(str(time.time()) + str(params)),
            'secret-type' : 0,
        }
        
        #begin = time.time()
        ret = requests.post(url, headers=headers, data=params)
        #end = time.time()
        #consume_time = end - begin
        return json.loads(ret.content)

    def __md5(self, s):
        m= hashlib.md5()
        m.update(s)
        return m.hexdigest()

def main():
    it = InventoryHelper()
    rid = 13
    items = ['107177','100025']
    print it.query(rid, items)

if __name__ == '__main__':
    main()
