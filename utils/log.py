""" 
logging facility
"""
import sys
import logging
import warnings

from twisted.python import log

def str_to_unicode(text, encoding=None, errors='strict'):
    """Return the unicode representation of text in the given encoding. Unlike
    .encode(encoding) this function can be applied directly to a unicode
    object without the risk of double-decoding problems (which can happen if
    you don't use the default 'ascii' encoding)
    """

    if encoding is None:
        encoding = 'utf-8'
    if isinstance(text, str):
        return text.decode(encoding, errors)
    elif isinstance(text, unicode):
        return text
    else:
        raise TypeError('str_to_unicode must receive a str or unicode object, got %s' % type(text).__name__)

def unicode_to_str(text, encoding=None, errors='strict'):
    """Return the str representation of text in the given encoding. Unlike
    .encode(encoding) this function can be applied directly to a str
    object without the risk of double-decoding problems (which can happen if
    you don't use the default 'ascii' encoding)
    """

    if encoding is None:
        encoding = 'utf-8'
    if isinstance(text, unicode):
        return text.encode(encoding, errors)
    elif isinstance(text, str):
        return text
    else:
        raise TypeError('unicode_to_str must receive a unicode or str object, got %s' % type(text).__name__)

# Logging levels
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL
SILENT = CRITICAL + 1

level_names = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARNING",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "CRITICAL",
    SILENT: "SILENT",
}

class FileLogObserver(log.FileLogObserver):

    def __init__(self, f, level=INFO, encoding='utf-8'):
        self.level = level
        self.encoding = encoding
        self.emit = self._emit
        log.FileLogObserver.__init__(self, f)

    def _emit(self, eventDict):
        ev = _adapt_eventdict(eventDict, self.level, self.encoding)
        if ev is not None:
            log.FileLogObserver.emit(self, ev)
        return ev

    def _emit_with_crawler(self, eventDict):
        ev = self._emit(eventDict)
        if ev:
            level = ev['logLevel']
            sname = 'log_count/%s' % level_names.get(level, level)

def _adapt_eventdict(eventDict, log_level=INFO, encoding='utf-8', prepend_level=True):
    """Adapt Twisted log eventDict making it suitable for logging with a 
    log observer. It may return None to indicate that the event should be
    ignored by a log observer.

    `log_level` is the minimum level being logged, and `encoding` is the log
    encoding.
    """
    ev = eventDict.copy()
    if ev['isError']:
        ev.setdefault('logLevel', ERROR)
        return 

    level = ev.get('logLevel')
    if level < log_level:
        return

    lvlname = level_names.get(level, 'NOLEVEL')
    message = ev.get('message')
    if message:
        message = [unicode_to_str(x, encoding) for x in message]
        if prepend_level:
            message[0] = "%s: %s" % (lvlname, message[0])
        ev['message'] = message

    why = ev.get('why')
    if why:
        why = unicode_to_str(why, encoding)
        if prepend_level:
            why = "%s: %s" % (lvlname, why)
        ev['why'] = why

    fmt = ev.get('format')
    if fmt:
        fmt = unicode_to_str(fmt, encoding)
        if prepend_level:
            fmt = "%s: %s" % (lvlname, fmt)
        ev['format'] = fmt

    return ev

def _get_log_level(level_name_or_id):
    if isinstance(level_name_or_id, int):
        return level_name_or_id
    elif isinstance(level_name_or_id, basestring):
        return globals()[level_name_or_id]
    else:
        raise ValueError("Unknown log level: %r" % level_name_or_id)

def start(logfile=None, loglevel='INFO', logstdout=True, logencoding='utf-8'):
    loglevel = _get_log_level(loglevel)
    file = open(logfile, 'a') if logfile else sys.stderr
    sflo = FileLogObserver(file, loglevel, logencoding)
    _oldshowwarning = warnings.showwarning
    log.startLoggingWithObserver(sflo.emit, setStdout=logstdout)
    # restore warnings, wrongly silenced by Twisted
    warnings.showwarning = _oldshowwarning
    return sflo

def msg(message=None, _level=INFO, service=None, **kw):
    kw['logLevel'] = kw.pop('level', _level)
    if service:
        kw.setdefault('system', service)
    if message is None:
        log.msg(**kw)
    else:
        log.msg(message, **kw)

def err(_stuff=None, _why=None, **kw):
    kw['logLevel'] = kw.pop('level', ERROR)
    log.err(_stuff, _why, **kw)

if __name__ == "__main__":
    start('test.log', loglevel='INFO')
    msg('test log error', level=ERROR, service='abc')
    msg('test log info', level=INFO, service='def')
    msg('test log warnings', level=WARNING)
