#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch, helpers

class EsHelper(object):
    def __init__(self, es_hosts, logger):
        self.__es = Elasticsearch(es_hosts)
        self.__logger = logger

    def count(self, index_name):
        result = self.__es.count(index=index_name)
        return result['count']

    def delete(self, index, ignore=[400, 404]):
        return self.__es.indices.delete(index=index, ignore=ignore, timeout='30s')

    def create(self, index, setting):
        self.__es.indices.create(index=index, body=setting, timeout='30s')

    def put_mapping(self, index, type, mapping):
        return self.__es.indices.put_mapping(index=index,
                                             doc_type=type,
                                             body=mapping,
                                             timeout='30s')

    def put_alias(self ,index, name):
        self.__es.indices.put_alias(index=index, name=name, timeout='30s')

    def delete_alias(self, index, name, ignore=[400, 404]):
        return self.__es.indices.delete_alias(index=index, name=name, ignore=ignore, timeout='30s')

    def exists(self, index):
        return self.__es.indices.exists(index=index)

    #批量提交
    def bulk(self, actions):
        if actions:
            helpers.bulk(self.__es, actions, request_timeout=30, timeout='30s')
        return True

    def build_bool_term(self, terms):
        body = {
            'bool':{
                'must':[]
            }
        }
        for i in terms:
            body['bool']['must'].append({'term':i})
        return body

    #根据别名获取es真正索引
    def get_current_index_name_by_alias(self, alias):
        try:
            alias_info = self.__es.indices.get_alias(name=alias)
        except Exception, e:
            return None
        if not alias_info:
            return None
        return alias_info.keys()[0]

    #获取最大更新时间
    def get_max_value(self, index_name, type_name, field):
        if not self.count(index_name):
            return 0
        body = '''
               {
                   "fields": ["%s"],
                   "query": {"match_all": {} },
                   "sort": {"%s": "desc"},
                   "size": 1
               }
               '''%(field, field)
        result = self.__es.search(index=index_name, doc_type=type_name, body=body, timeout='30s')
        if not result:
            return 0
        return int(result['hits']['hits'][0]['fields'][field][0])

    def get_doc_by_key_terms(self, index_name, type_name, terms, offset = 0, size = 1):
        result = self.query_by_key_terms(index_name, type_name, terms, offset, size)
        rows = []
        for item in result['hits']['hits']:
            rows.append(item['_source'])
        return rows

    def get_count_by_key_terms(self, index_name, type_name, terms):
        result = self.query_by_key_terms(index_name, type_name, terms, 0, 1)
        return result['hits']['total']

    def query_by_key_terms(self, index_name, type_name, terms, offset, size):
        body = {
            'query': {
                'bool': {
                    'should': [],
                    'minimum_should_match': 1
                }
            },
            'from': offset,
            'size': size
        }
        for term in terms:
            body['query']['bool']['should'].append(term)
        self.__logger.info(body)
        result = self.__es.search(index=index_name, doc_type=type_name, body=body, timeout='30s')
        return result

def main():
    es = Elasticsearch([{'host': '192.168.60.59', 'port': 9200}])
    body = {
        'query': {
            'bool': {
                'should': [
                    {'term': {'sku_id': '1004411'}},
                    {'term': {'sku_id': '1005231'}},
                ],
                'minimum_should_match': 1
            }
        },
        'sort': [
            {'sku_id': 'desc'}
        ],
        'size': 1
    }
    data = helpers.scan(es, query = body, index = 'lsh_market_goods_v1', doc_type = 'sku', scroll='1m', size = 1000)
    count = 0
    for d in data:
        print d
        print '-------------------'
        count += 1
    print count

if __name__ == '__main__':
    main()

