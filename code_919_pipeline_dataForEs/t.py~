#! -*- coding:utf-8 -*-

cc={1:'1',2:'2'}



def dict_result_cache(cache_ref):#compare objectId cacheId  ->return objectIdAllContent
    print 'cache_ref',cache_ref

    def inner(object_ids, core_func):
        print 'inner'
        ret = {}
        miss = []
        for oid in object_ids:
            if oid in cache_ref:
                ret[oid] = cache_ref[oid]
            else:
                miss.append(oid)
        if miss:
            more = core_func(miss)
            cache_ref.update(more)
            ret.update(more)
        print 'cc in inner',cc
        return ret

    def decorator(func):
        print 'd1'
        def decorated(object_ids):
            print 'd2'
            return inner(object_ids, func)
        return decorated

    return decorator
    



@dict_result_cache(cc)#dict_result_cache(func,dictCache)  use get_clues() and cc as parameters execute inner() decorator()
def get_clues(clue_ids):

    return dict(zip(clue_ids,[0]*len(clue_ids)))
