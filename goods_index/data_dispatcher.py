#encoding:utf-8
import sys
import yaml

class DataDispatcher(object):
    """
    数据请求分发器
    model内应遵循模块名为下划线命名,模块内类名为对应驼峰命名规则
    如模块名为item_sale.py,对应类名为class ItemSale

    todo: 分发器每次请求都会new一个对象,model可以考虑单例模式
    """
    def __init__(self, conf):
        self.__conf = conf

    def dispatch(self, module, func_name, *args, **kwargs):
        obj = self.__generate_model_obj(module)
        if not obj:
            return None
        func = getattr(obj, func_name)
        return func(*args, **kwargs)

    def __generate_model_obj(self, module):
        model_class_name = self.__generate_class_name(module)
        module_name = 'model.%s' % (module)
        try:
            if module_name not in locals():
                command = "from %s import *" % (module_name)
                #print command
                exec command
            m = sys.modules[module_name]
            model_class = getattr(m, model_class_name)
            obj = model_class(self.__conf)
            return obj
        except Exception, e:
            print e
            return None

    def __generate_class_name(self, scheme):
        items = []
        for item in scheme.split('_'):
            items.append(item.capitalize())
        return ''.join(items)

def main():
    stream = open('../conf/conf.yml', 'r')
    conf = yaml.load(stream)
    d = DataDispatcher(conf)
    print d.dispatch('item_sale', 'get_rows', 0, 2)

if __name__ == "__main__":
    main()
