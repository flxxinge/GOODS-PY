#encoding:utf-8
import argparse
import sys
sys.path.append('..')
import yaml
import logging
import utils
import json
import time
from scheme.goods_index import GoodsIndex
from scheme.test_index import TestIndex
#from kafka import KafkaClient, SimpleProducer, SimpleConsumer
from pykafka import KafkaClient
import traceback
reload(sys)
sys.setdefaultencoding('utf8')

class Indexer:
    def __init__(self, conf):
        self.__config = conf
        utils.logger.init_logger(self.__config['log_conf'], True)
        self.__logger = logging.getLogger('indexer')
        self.__config['__logger'] = self.__logger
        self._kafka_client = KafkaClient(self.__config['kafka']['hosts'])
        self._zookeeper_hosts = self.__config['zookeeper']['hosts']
    
    def listen(self):
        print 'listen kafka queue .... %s'%(self.__config['kafka']['hosts'])
        topic = self._kafka_client.topics['MARKET-GOODS']
        balanced_consumer = topic.get_balanced_consumer(
            consumer_group='group.id',
            auto_commit_enable=True,
            zookeeper_connect=self._zookeeper_hosts
        )
        for message in balanced_consumer:
            if message is not None:
                self.__logger.info('offset => %s, msg => %s' % (message.offset, message.value))
                try:
                    json_msg = json.loads(message.value)
                    index_name = json_msg['index_name']
                    action = json_msg['action']
                    model = json_msg['model']
                    update_mode = json_msg['update_mode']
                    self.process(index_name, action, model, update_mode)
                except Exception, e:
                    self.__logger.error(traceback.print_exc())

    def process(self, index_name, action, model, update_mode):
        #time.sleep(1)
        start_time = time.time()
        index_scheme = None
        if index_name == 'goods':
            index_scheme = GoodsIndex(self.__config)
        elif index_name == 'test':
            index_scheme = TestIndex(self.__config)
        if not index_scheme:
            return False
        if action == 'build':
            ret = index_scheme.create_index()
        elif action == 'update':
            incr = False
            if update_mode == 'incr':
                incr = True
            ret = index_scheme.update_index(model, incr)
        else:
            raise NotImplementedError
        end_time = time.time()
        self.__logger.info("indexer process consumed %s seconds" % (end_time - start_time))

def main():
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('-c', '--conf', dest='config',
                       help='specify config file path', nargs='?',
                       default='../conf/indexer_conf.yml')
    parser.add_argument('-t', '--type', dest='type',
                       help='run type , trigger or listen', nargs='?',
                       default='trigger')
    parser.add_argument('-i', '--index', dest='index',
                       help='index name', nargs='?',
                       default='')
    parser.add_argument('-a', '--action', dest='action',
                       help='update or build', nargs='?',
                       default='update')
    parser.add_argument('-m', '--model', dest='model',
                       help='model', nargs='?',
                       default='')
    parser.add_argument('-u', '--update_mode', dest='update_mode',
                       help='local or incr', nargs='?',
                       default='incr')

    args = parser.parse_args()
    conf_file = args.config
    run_type = args.type
    index_name = args.index
    action = args.action
    model = args.model
    update_mode = args.update_mode

    stream = open(conf_file, 'r')
    conf = yaml.load(stream)
    processor = Indexer(conf)
    if run_type == 'listen':
        processor.listen()
    elif run_type == 'trigger':
        processor.process(index_name, action, model, update_mode)
    else:
        print 'unsupported run type ...'

if __name__ == '__main__':
    main()
