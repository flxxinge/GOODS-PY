# encoding=utf-8

import os.path
import logging
import logging.handlers
from mlogging import TimedRotatingFileHandler_MP

logger_lock = None

class MultiProcessFileHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, delay=False, utc=False):
        logging.handlers.TimedRotatingFileHandler.__init__(self, filename, when, interval, backupCount, encoding, delay, utc)

    def emit(self, record):
        """
        Emit a record.

        Output the record to the file, catering for rollover as described
        in doRollover().
        """
        global logger_lock
        logger_lock.acquire()
        try:
            if self.shouldRollover(record):
                self.doRollover()
            logging.FileHandler.emit(self, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
        finally:
            logger_lock.release()


def init_logger(log_conf_items, multi_process = False):
    """ 初始化logger.

    Args:
      log_conf_items: 配置项list.
    """
    LOGGER_LEVEL = {
            'DEBUG': logging.DEBUG,
            'INFO' : logging.INFO,
            'WARNING' : logging.WARNING,
            'ERROR' : logging.ERROR,
            'CRITICAL':logging.CRITICAL
            }
    for log_item in log_conf_items:
        logger = logging.getLogger(log_item['name'])
        path = os.path.expanduser(log_item['file'])
        dir = os.path.dirname(path)
        if dir and not os.path.exists(dir):
            os.makedirs(dir)
        handler = None
        if multi_process is False:
            handler = logging.handlers.TimedRotatingFileHandler( path, 'D', 1, 0 )
        else:
            handler = TimedRotatingFileHandler_MP(path, 'D', 1, 0)
        handler.suffix='%Y%m%d%H'
        formatter = logging.Formatter(log_item['format'])
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(LOGGER_LEVEL[log_item['level']])

if __name__ == '__main__':
    log_items = [
            { 'name':'indexer', 'file':'a.log', 'level':'DEBUG', 'format':'%(asctime)s %(levelname)s %(message)s' },
            { 'name':'goods_id_dist', 'file':'b.log', 'level':'DEBUG', 'format':'%(asctime)s %(levelname)s %(message)s'},
            ]
    init_logger(log_items,True)
    logger = logging.getLogger('indexer')
    logger.info('haha')
