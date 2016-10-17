#coding=utf8
import os
import time
def get_values_from_seq_container(s,key,l):
    """从一个dict tuple中取一个key的值.
    ({"a":,"c":2
    """
    for item in s:
        if(item[key]!= None):
            l.append(item[key])

def initDict(path):
    rDict={}
    if(not os.path.exists(path)):
        return rDict
    fin=open(path)
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        if(len(ll)==2):
            rDict[ll[0]]=ll[1]
        else:
            rDict[ll[0]]=1
    fin.close()
    return rDict

def join_seq(s,seq):
    result = ''
    try:
        result += str(seq[0])
        for item in seq[1:]:
            result += s + str(item)
    except:
        pass
    return result

def join_int_ids(ids, con = ','):
    ids_s = ''
    for id in ids:
        if id:
            ids_s += str(id) + ','
    ids_s = ids_s.strip(',')
    return ids_s

def rows_to_dict_uniq(rows,key,key2str = False):
    result = {}
    for row in rows:
        new_key = row[key]
        if key2str:
            new_key = str(new_key)
        result[new_key] = row
    return result
    
def data2set(data, key):
    ls = set()
    for item in data:
        ls.add(item[key])
    return ls

def get_set_from_dicts(s, key):
    my_set = set()
    for item in s:
        my_set.add(item[key])
    return my_set

def rows_to_dict_multi(rows, key):
    result = {}
    for row in rows:
        d = result.get(row[key],{})
        it = row.iteritems()
        if  d:
            for field in it:
                d[field[0]].append(field[1])
        else:
            for field in it:
                d[ field[0] ] = [ field[1] ]
        result[ row[key] ] = d
    return result

def data2array(data, key):
    ls = []
    for item in data:
        ls.append(item[key])
    return ls

time_record_list = {}
def time_record(func):
    def WrappedFunc(*args, **kwargs):
        global time_record_list
        start = time.time()
        name = func.__name__
        ret = func(*args, **kwargs)
        run_time = time.time() - start
        if time_record_list.has_key(name):
            time_record_list[name] += run_time
        else:
            time_record_list[name] = run_time
        #print name,run_time
        return ret
    return WrappedFunc

def url_normalize(url):
    url = url.strip(' \t\r\n')
    url_http = "http://"
    len_url_http = len(url_http)
    if url[:len_url_http] != url_http:
        url = url_http + url
    return url

def fileToRecord(path):
    retList=[]
    fin=open(path)

if __name__ == "__main__":
    rows = ({'a':1, 'b':2, 'c':3}, {'a':4, 'b':5, 'c':6})
    rows = ({'a':1, 'b':2, 'c':3},)
    key = 'a'
    print rows_to_dict_uniq(rows, key)
