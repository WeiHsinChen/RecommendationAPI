# data process methods
#
import string

FULLPATH_CONCAT_CHAR='$'

def dtf(d):
    return time.strftime("%Y/%m/%d %H:%M", d.timetuple())

def delete_coll(data, not_in=None):
    if type(data) is list:
        for i in xrange(len(data)):
            #print 'delete idx: %d' % i
            if not not_in or data[i] not in not_in:
                delete_coll(data[i])
        del data[:]
    elif type(data) is dict:
        l_keys = tuple(data.keys())
        for k in l_keys:
            #print 'delete key: %s' % k
            delete_coll(data[k])
        data.clear()

def fill_dict(src, ref, incl=None, force=False):
    # from utils.py 
    if type(incl) is list or type(incl) is tuple:
        for k in incl:
            if not src.has_key(k):
                if ref.has_key(k):
                    src[k] = ref[k]
                else:
                    src[k] = None
    else:
        for k in ref.keys():
            if not src.has_key(k) or force:
                src[k] = ref[k]

    return src

def compare_dict(src, dst, incl=None, excl=None):
    """src: source dict
    dst: destination dict
    incl: included key list
    excl: excluded key list

    return: {'add':...,'del':...,'dif':...,'eql':...}"""

    assert type(src) is dict
    assert type(dst) is dict

    s_src = set(src.keys())
    s_dst = set(dst.keys())

    if type(incl) is list and len(incl)>0: 
        s_src = s_src.intersection(set(incl))
        s_dst = s_dst.intersection(set(incl))
    if (type(excl) is list or type(excl) is tuple) and len(excl)>0:        
        s_src = s_src - set(excl)
        s_dst = s_dst - set(excl)

    s_isc = s_dst.intersection(s_src)

    s_add = s_dst - s_isc
    s_del = s_src - s_isc
    s_dif = set(o for o in s_isc if src[o] != dst[o])
    s_eql = s_isc - s_dif

    #print 'add: ',s_add
    #print 'del: ',s_del
    #print 'dif: ',s_dif
    #print 'eql: ',s_eql
    return {'add':s_add,'del':s_del,'dif':s_dif,'eql':s_eql}

def change_dict(src, dst, incl=None, excl=None):
    """src: source dict
    dst: destination dict
    incl: included key list
    excl: excluded key list

    return: {'chg':...,'del':...,'eql':...}"""

    d = compare_dict(src, dst, incl, excl)
    return {'chg':d['add'].union(d['dif']),'del':d['del'],'eql':d['eql']}

def update_dict(src, dst, incl=None, excl=[], pk=['id'], ignore_del=False):
    """preparation dict dataset for SQL's update"""

    tm = compare_dict(src, dst, incl=incl, excl=excl+pk)
    ud = {}
    if not ignore_del:
        for o in tm['del']: 
            if src[o] is not None: ud[o]=None

    for o in tm['add'].union(tm['dif']): ud[o]=dst[o]
    if len(ud)>0:
        for o in pk:
            if o in src.keys(): ud[o]=src[o]

    return ud

def get_row_dict(k, i_dict, rows):
    if i_dict.has_key(k) and i_dict[k] < len(rows):
        return rows[i_dict[k]]
    return None

def rows_diff_by_indictor(i_src, i_dst, r_src, r_dst, incl=None, excl=[], pk=['id']):
    """return {'add':...,'del':...,'mod':...}"""
    assert type(i_src) is dict
    assert type(i_dst) is dict
    assert type(r_src) is list
    assert type(r_dst) is list

    cmpd = compare_dict(i_src, i_dst, incl=None, excl=[])

    d_add = []
    d_del = []
    d_mod = []

    #print cmpd
    for k in cmpd['add']:
        d_add.append(get_row_dict(k, i_dst, r_dst))

    for k in cmpd['del']:
        d_del.append(get_row_dict(k, i_src, r_src))

    # inictor dict ignore diff of row#
    # they're same case ... compare data in rows
    for k in cmpd['dif'].union(cmpd['eql']): 
        s = get_row_dict(k, i_src, r_src)
        d = get_row_dict(k, i_dst, r_dst)
        ud = update_dict(s, d, incl=incl, excl=excl, pk=pk)
        if len(ud)>0:
            d_mod.append(ud)

    return {'add':d_add,'del':d_del,'mod':d_mod}

def rows_diff_by_seq(i_src, i_dst, r_src, r_dst, incl=None, excl=[], pk=['id'],
                     ind=None,ordr=None,rplc=False,fillback=False):
    """return {'add':...,'del':...,'mod':...}"""
    assert type(i_src) is list or type(i_src) is xrange
    assert type(i_dst) is list or type(i_dst) is xrange
    assert type(r_src) is list
    assert type(r_dst) is list
    d_mod = []
    d_eql_idx = {}
    d_mod_idx = {}

    for o in i_src: r_src[o]['_index_']=o
    for o in i_dst: r_dst[o]['_index_']=o

    if type(ordr) is list:
        sorted_func = lambda dd: string.join([dd[o] for o in ordr],"::")
        l_src = sorted([r_src[o] for o in i_src], key=sorted_func)
        l_dst = sorted([r_dst[o] for o in i_dst], key=sorted_func)
    else:
        l_src = [r_src[o] for o in i_src]
        l_dst = [r_dst[o] for o in i_dst]

    ti_src = [o for o in xrange(0,len(l_src))]
    ti_dst = [o for o in xrange(0,len(l_dst))]
    ti_src_t = []
    ti_dst_t = []

    if type(ind) is list:
        rplc = False # change option

        ind_src = get_ind_from_tbl(l_src, ind)
        ind_dst = get_ind_from_tbl(l_dst, ind)
        cmpd = compare_dict(ind_src, ind_dst, incl=None, excl=None)
        for k in cmpd['dif'].union(cmpd['eql']):
            i_s = ind_src[k]
            i_d = ind_dst[k]
            t_src = l_src[i_s]
            t_dst = l_dst[i_d]
            ud = update_dict(t_src, t_dst, incl=incl, excl=excl+ind, pk=pk)

            ti_src_t.append(i_s)
            ti_dst_t.append(i_d)

            if len(ud)==0:
                d_eql_idx[i_s]=i_d
            else:
                if incl:
                    fill_dict(ud, t_src, incl)
                elif fillback: 
                    fill_dict(ud, t_src)
                    ud['_old_']=t_src
                    ud['_new_']=t_dst
                d_mod.append(ud)
                d_mod_idx[i_s]=i_d
    else:
        for i_s in ti_src:
            for i_d in ti_dst:
                if i_d not in ti_dst_t:
                    t_src = l_src[i_s]
                    t_dst = l_dst[i_d]
                    cmpd = compare_dict(t_src, t_dst, incl=incl, excl=excl+pk)
                    if len(cmpd['add'])==0 and len(cmpd['del'])==0 and len(cmpd['dif'])==0:
                        ti_src_t.append(i_s)
                        ti_dst_t.append(i_d)
                        d_eql_idx[i_s]=i_d
                        break

    for o in ti_src_t: ti_src.remove(o)
    for o in ti_dst_t: ti_dst.remove(o)

    if rplc is True:
        ti_src_t = []
        ti_dst_t = []

        ti_src.reverse() # for ti_src.pop()

        for i_d in ti_dst:
            if len(ti_src)>0:
                i_s = ti_src.pop()
                ti_src_t.append(i_s)
                ti_dst_t.append(i_d)
                t_src = l_src[i_s]
                t_dst = l_dst[i_d]
                ud = update_dict(t_src, t_dst, incl=incl, excl=excl, pk=pk)
                if len(ud)>0:
                    if fillback: 
                        fill_dict(ud, t_src)
                        ud['_old_']=t_src
                        ud['_new_']=t_dst
                    d_mod.append(ud)
                    d_mod_idx[i_s]=i_d


        ti_src.reverse()
        for o in ti_dst_t: ti_dst.remove(o)

    d_add = [fill_attributes(l_dst[o],incl) for o in ti_dst]
    d_del = [l_src[o] for o in ti_src]

    return {'add':d_add, 'del':d_del, 'mod':d_mod, 
                'eql_idx':d_eql_idx, 'mod_idx':d_mod_idx}

def get_ind_from_tbl(dt, keys):
    dct = {}
    row = 0
    for di in dt:
        vals=[]
        for k in keys:            
            if di.has_key(k):
                vals.append(str(di[k] and di[k] or 'None'))
            else:
                vals.append('X')
        dct['$'.join(vals)]=row
        row += 1
    return dct

def fill_attributes(dt, keys):
    if keys:
        for k in keys:
            if not dt.has_key(k): 
                dt[k]=None

    return dt
