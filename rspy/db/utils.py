import sys
sys.path.append("../..")

from sqlalchemy import sql
from sqlalchemy.sql import and_
from sqlalchemy.sql.expression import bindparam

from rspy.utils.data import update_dict

import server

seq = lambda x: xrange(len(x))

def stmt_to_dct(stmt_, conn_):
	rets=[]
	for row in conn_.execute(stmt_):
		row_data = row2dict(row, has_none=True)
		rets.append(row_data)
	return rets

def updates_mon_for_changed(self, ud, old, new):
	ud['changed']=datetime.now()

def fill_dict(src, ref, force=False):
	for k in ref.keys():
		if not src.has_key(k) or force:
			src[k] = ref[k]

	return src

def row2dict(row, has_none=False):
	ret = {}
	if row is None: return ret

	for k,v in row.items():
		if not has_none and v is None: continue
		ret[k]=v
	return ret

def getconnection(conn_=None, db=None):
	if conn_ is None:
		if db is None or db=='rs':
			db = server.get_sqlite_db()
		return db.connect()
	else:
		return conn_

def generate_simple_where_clause(tbl, cols_lst, namedata):

	u_where_params = []
	for col in cols_lst:
		if namedata.has_key(col):
			u_where_params.append(tbl.c[col]==namedata[col])
	u_where_clause = and_(*u_where_params)

	return u_where_clause

def generate_column_data(cols_whr, namedata):

	dct = {}
	if type(namedata) in (tuple, list):
		for idx in xrange(0, len(cols_whr)):
			if idx<len(namedata): dct[cols_whr[idx]]=namedata[idx]
	elif type(namedata) is dict:
		for k in cols_whr:
			if namedata.has_key(k): dct[k]=namedata[k]
			#else: dct[k]= None
	else:
		if len(cols_whr)>0:
			dct[cols_whr[0]]=namedata

	return dct


def db_operates(action, conn, tbl, rows, pk=['id']):
	if rows is None or len(rows)==0: return 0
	cnt = 0
	if action in ('del', 'mod'):
		# generate where clause
		u_where_params = []
		for o in pk: 
			if action=='mod': u_where_params.append(tbl.c[o]==bindparam('_'+o))
			else: u_where_params.append(tbl.c[o]==bindparam(o))
		u_where_clause = and_(*u_where_params)

	if action=='add':
		if len(rows)==-1:
			respxy = conn.execute(tbl.insert(), rows[0])
			for idx in xrange(0, len(pk)):
				rows[0][pk[idx]]=respxy.inserted_primary_key[idx]
		else:
			respxy = conn.execute(tbl.insert(), rows)

		cnt = respxy.rowcount
	elif action=='mod':
		# generate values params
		u_value_keys = {}
		def prepare_columns(t_k, row_):
			for k in row_.keys():
				if tbl.columns.has_key(k) and not k in pk: 
					if u_value_keys.has_key(k):
						t_k[k] = u_value_keys[k]
					else:
						t_k[k] = u_value_keys[k] = bindparam(k)

		# preparation for key=id
		t_u_value_keys = {}
		for row in rows:
			prepare_columns(t_u_value_keys, row)
			for k in row.keys(): 
				if k in pk: row['_'+k]=row[k]
			st = tbl.update().where(u_where_clause).values(**t_u_value_keys)
			respxy = conn.execute(st, [row])
			cnt += respxy.rowcount
			t_u_value_keys.clear()
			del st
		# reset for key=id
		for row in rows:
			for k in row.keys():
				if k in pk: del row['_'+k]
	elif action=='del':
		respxy = conn.execute(tbl.delete().where(u_where_clause), rows)
		cnt = respxy.rowcount

	return cnt

def db_operate_dict(conn, tbl, dict, pk=['id']):
	"""dict={'add':.,'del':.,'mod':.}"""

	rows = 0

	if dict is None or len(dict)==0: return 0

	rows = rows + db_operates('add', conn, tbl, dict['add'], pk)
	rows = rows + db_operates('del', conn, tbl, dict['del'], pk)
	rows = rows + db_operates('mod', conn, tbl, dict['mod'], pk)

	return rows

def call_intercept_method(obj, methodname, args):
	if hasattr(obj,methodname) and callable(getattr(obj,methodname)):
		getattr(obj,methodname)(*args)


def read_many(tblobj, namedata, whr_cols=None, conn=None, one=False):

	_conn = getconnection(conn, db=tblobj.dbname)

	if whr_cols is None: whr_cols=tblobj.cols_whr
	if whr_cols is None: whr_cols=tblobj.cols
	#print "whr_cols:",whr_cols
	dct_namedata = generate_column_data(whr_cols, namedata)
	if len(dct_namedata)==0: return {}
	#print '\tdct_namedata:',dct_namedata
	whrcls = generate_simple_where_clause(tblobj.tbl,whr_cols,dct_namedata)
	stmt = sql.select([tblobj.tbl]).where(whrcls)
	#print '\tstmt:',stmt
	if one: ret = row2dict(_conn.execute(stmt).first())
	else: ret = [row2dict(row) for row in _conn.execute(stmt)]

	if conn is None: _conn.close()

	return ret

class TableObject(object):

	def __init__(self, tbl, cols_whr=(), cols_excl=(), dbname=None):
		self.tbl = tbl
		self.cols_excl = cols_excl
		self.dbname = dbname

		self.cols = tuple(tbl.columns.keys())
		self.pk = tuple(tbl.primary_key.columns.keys())

		if cols_whr is not None and len(cols_whr)==0: self.cols_whr = self.pk
		else: self.cols_whr = cols_whr

	def to_row(self, namedata, kw, old=None):
		dd = {}
		# values from kw
		for k in self.cols:
			if k in kw.keys(): dd[k]=kw[k]

		# fills-up other columns
		if old is None or len(old)==0:
			# values for default
			whr_cols = self.cols_whr
			if whr_cols is None: whr_cols=self.cols
			dct_namedata = generate_column_data(whr_cols, namedata)
			call_intercept_method(self,'to_row_new',(dct_namedata,dd))
		else:
			# values from old
			for k in [k for k in old.keys() if k not in dd.keys()]: dd[k]=old[k]

		return dd

	def read(self, namedata, conn=None, cols=None):
		return read_many(self, namedata, cols, conn, one=True)
	
	def updates(self, name, kw, conn_=None):

		_conn = getconnection(conn_, db=self.dbname)

		old = self.read(name, _conn)

		action = None
		if len(old)==0:
			action = 'add'
		else:
			if kw is None: action='del'
			else: action = 'mod'

		if action in ('add','mod'):
			new = self.to_row(name, kw, old=old)

		rows = 0

		if action=='add':
			rows=rows+db_operates('add', _conn, self.tbl, [new], pk=self.pk)
		elif action=='mod':
			ud = update_dict(old, new, excl=self.cols_excl, pk=self.pk)
			#print 'ud:',ud
			if len(ud)>0:
				call_intercept_method(self,'updates_mon',(ud,old,new))
				rows=rows+db_operates('mod', _conn, self.tbl, [ud], pk=self.pk)
		elif action=='del':
			rows=rows+db_operates('del', _conn, self.tbl, [old], pk=self.pk)

		if conn_ is None: _conn.close()

		return rows

	def pkdata(self, kw):
		t_data=[]
		for k in self.pk:
			if kw.has_key(k): t_data.append(kw[k])
			else: t_data.append(None)
		return tuple(t_data)

	def create(self, kw, conn_=None):

		_conn = getconnection(conn_, db=self.dbname)

		namedata={}
		for k in [k for k in self.cols_whr if k in kw.keys()]: namedata[k]=kw[k]
		if len(namedata)==0 and self.cols_whr!=self.pk: 
			return {'_result_':'no valid name data'}

		old = self.read(namedata, _conn)
		if len(old)>0: 
			old['_result_']='existed'
			return old

		call_intercept_method(self,'creates_mon',(kw,None,None))
		new = self.to_row(namedata, kw)
		rows = db_operates('add', _conn, self.tbl, [new], pk=self.pk)
		if rows==0: return {'_result_':'insert error'}
		
		if len(namedata)==0:
			# reset namedata from pk's data
			for k in [k for k in self.pk if k in new.keys()]: namedata[k]=new[k]
		ret = self.read(namedata, _conn)
		ret['_result_']='ok'

		if conn_ is None: _conn.close()

		return ret


	def update(self, kw, conn_=None, old_=None, new_=None):

		_conn = getconnection(conn_, db=self.dbname)

		if old_ is None and new_ is None: is_fresh = True
		else: is_fresh = False

		namedata={}
		for k in [k for k in self.cols_whr if k in kw.keys()]: namedata[k]=kw[k]
		if len(namedata)==0: return {'_result_':'no valid name data'}

		if is_fresh: old = self.read(namedata, _conn)
		else: old = old_
		if len(old)==0: return {'_result_':'doesnot existed'}

		if is_fresh: new = self.to_row(namedata, kw)
		else: new = new_

		if is_fresh: ud = update_dict(old, new, excl=self.cols_excl, pk=self.pk)
		else: ud = kw
		if len(ud)>0:
			call_intercept_method(self,'updates_mon',(ud,old,new))
			rows=db_operates('mod', _conn, self.tbl, [ud], pk=self.pk)
			if rows==0: return {'_result_':'update error'}
		
		ret = self.read(namedata, _conn)
		ret['_result_']='ok'

		if conn_ is None: _conn.close()

		return ret

	def delete(self, name, conn_=None):

		_conn = getconnection(conn_, db=self.dbname)

		old = self.read(name, _conn)
		if len(old)==0: return {'_result_':'doesnot existed'}

		rows = db_operates('del', _conn, self.tbl, [old], pk=self.pk)
		if rows==0: return {'_result_':'delete error'}

		if conn_ is None: _conn.close()

		old['_result_']='ok'
		return old


def tblobj_operates(action, conn, tblobj, rows, fillback=False):
	if rows is None or len(rows)==0: return 0

	if action=='add': act_func = tblobj.create
	elif action=='mod': act_func = tblobj.update
	elif action=='del': act_func = tblobj.delete

	results={'ok':0}
	for row in rows:
		_old = _new = None
		if row.has_key('_old_') and row.has_key('_new_'): 
			_old=row['_old_']
			_new=row['_new_']
		if action=='mod': ret = act_func(row, conn, _old, _new)
		else: ret = act_func(row, conn)

		if fillback and ret['_result_']=='ok':
			if _new is not None: fill_dict(_new, ret, force=False)
			else: fill_dict(row, ret, force=False)

		if results.has_key(ret['_result_']): results[ret['_result_']]+=1
		else: results[ret['_result_']]=1

	return results['ok']

def tblobj_operate_dict(conn, tblobj, dict, fillback=False):
	"""dict={'add':.,'del':.,'mod':.}"""

	rows = 0

	if dict is None or len(dict)==0: return 0
	rows = rows + tblobj_operates('add', conn, tblobj, dict['add'], fillback)
	rows = rows + tblobj_operates('del', conn, tblobj, dict['del'], fillback)
	rows = rows + tblobj_operates('mod', conn, tblobj, dict['mod'], fillback)

	return rows

