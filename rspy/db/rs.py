from datetime import datetime
import random
import string
import types
import urllib
import time


from sqlalchemy import sql
from sqlalchemy.sql import and_, or_, not_, func


from rspy.metadata.rs import *
from rspy.utils.data import compare_dict, \
	update_dict, \
	rows_diff_by_indictor, rows_diff_by_seq, \
	get_ind_from_tbl

import server
import utils as db_utils


def dtf(d):
	return time.strftime("%Y/%m/%d %H:%M", d.timetuple())


def updates_mon_for_changed(self, ud, old, new):
	ud['changed'] = datetime.now()

#####################################################################
# Table(projexps)
tblobj_customer = db_utils.TableObject(customer, cols_whr=None, dbname='rs')
# Table(expsmstr)
tblobj_goods = db_utils.TableObject(goods, cols_whr=None, dbname='rs')
# Table(projinfo)
tblobj_rate = db_utils.TableObject(rate, cols_whr=None, dbname='rs')


def stmt_to_dct(stmt_, conn_):
	rets = []
	for row in conn_.execute(stmt_):
		row_data = db_utils.row2dict(row, has_none=True)
		rets.append(row_data)
	return rets


def reset_disable(tbl_, version_=None, conn_=None):

	_conn = db_utils.getconnection(conn_, db='rs')

	stmt = tbl_.update().\
		values(disable=1).\
		where(tbl_.c.version == version_)
	_conn.execute(stmt)

	if conn_ is None:
		_conn.close()

seq = lambda x: xrange(len(x))

# RS 
# customer
def add_customers(rows_, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')
	old_rows = read_customer()

	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['NAME'],
						  ind=['NAME'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, customer, dd, pk=['ID'])

	# get goods dict
	cus_dict = get_customers_dict(conn_=_conn)

	if conn_ is None:
		_conn.close()
	return cus_dict

def add_a_customer(rows_, cus_id=None, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')

	if cus_id:
		old_rows = read_customer(cus_id=cus_id,conn_=_conn)
	else:
		old_rows = []
	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['ID','NAME'],
						  ind=['ID'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, customer, dd, pk=['ID'])

	# get goods dict
	cus_dict = get_customers_dict(conn_=_conn)

	if conn_ is None:
		_conn.close()
	return cus_dict

def read_customer(cus_id=None, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')

	if not cus_id:
		stmt = sql.select([customer]).\
			select_from(customer)
	else:
		stmt = sql.select([customer]).\
			select_from(customer).\
			where(customer.c.ID == cus_id)

	rets = []
	for row in _conn.execute(stmt):
		row_data = db_utils.row2dict(row, has_none=True)
		rets.append(row_data)

	if conn_ is None:
		_conn.close()
	return rets

def get_customers_dict(reverse=None, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')
	cus_list = read_customer(conn_=_conn)
	cus_dict = {}
	for c in cus_list:
		if not reverse:
			cus_dict[c['NAME']] = c['ID']
		else:
			cus_dict[c['ID']] = c['NAME']

	return cus_dict

# goods
def add_goods(rows_, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')
	old_rows = read_goods()

	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['NAME'],
						  ind=['NAME'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, goods, dd, pk=['ID'])

	# get goods dict
	goods_dict = get_goods_dict(conn_=_conn)

	if conn_ is None:
		_conn.close()
	return goods_dict

def add_a_good(rows_, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')

	old_rows = []
	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['NAME'],
						  ind=['NAME'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, goods, dd, pk=['ID'])

	# get goods dict
	goods_dict = get_goods_dict(conn_=_conn)

	if conn_ is None:
		_conn.close()
	return goods_dict

def read_goods(conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')
	stmt = sql.select([goods]).\
		select_from(goods)

	rets = []
	for row in _conn.execute(stmt):
		row_data = db_utils.row2dict(row, has_none=True)
		rets.append(row_data)

	if conn_ is None:
		_conn.close()
	return rets

def get_goods_dict(reverse=None, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')
	goods_list = read_goods(conn_=_conn)
	goods_dict = {}
	for g in goods_list:
		if not reverse:
			goods_dict[g['NAME']] = g['ID']
		else:
			goods_dict[g['ID']] = g['NAME']

	return goods_dict

# rates
def add_rates(rows_, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')
	old_rows = read_rate(conn_=_conn)    
	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['CID','GID', 'RATE', 'REAL'],
						  ind=['CID','GID'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, rate, dd, pk=['CID','GID'])

	if conn_ is None:
		_conn.close()
	return rows

def add_a_rate(rows_, CID=None, GID=None, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')

	if CID==None and GID==None:
		old_rows = []
	else:
		old_rows = read_rate(CID=CID, GID=GID, conn_=_conn)
	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['CID','GID', 'RATE', 'REAL'],
						  ind=['CID','GID'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, rate, dd, pk=['CID','GID'])

	if conn_ is None:
		_conn.close()
	return rows

def update_all_pred_rate(rows_, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')

	stmt = sql.select([rate]).\
		select_from(rate).\
		where(rate.c.REAL == 0)

	old_rows = []
	for row in _conn.execute(stmt):
		row_data = db_utils.row2dict(row, has_none=True)
		old_rows.append(row_data)

	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['CID','GID','RATE', 'REAL'],
						  ind=['CID','GID'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, rate, dd, pk=['CID','GID'])

	if conn_ is None:
		_conn.close()
	return rows

def update_a_pred_rate(rows_, CID=None, GID=None, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')

	if CID != None and GID == None:
		stmt = sql.select([rate]).\
			select_from(rate).\
			where(and_(rate.c.REAL == 0, rate.c.CID == CID))
	elif CID == None and GID != None:
		stmt = sql.select([rate]).\
			select_from(rate).\
			where(and_(rate.c.REAL == 0, rate.c.GID == GID))

	old_rows = []
	for row in _conn.execute(stmt):
		row_data = db_utils.row2dict(row, has_none=True)
		old_rows.append(row_data)
	dd = rows_diff_by_seq(seq(old_rows), seq(rows_), old_rows, rows_,
						  incl=['CID','GID','RATE', 'REAL'],
						  ind=['CID','GID'], rplc=False,  fillback=False)
	rows = db_utils.db_operate_dict(_conn, rate, dd, pk=['CID','GID'])

	if conn_ is None:
		_conn.close()
	return rows

def read_rate(CID=None, GID=None, conn_=None):
	_conn = db_utils.getconnection(conn_, db='rs')

	if CID == None and GID == None:
		stmt = sql.select([rate]).\
			select_from(rate).\
			order_by(rate.c.CID, rate.c.GID)
	elif CID != None and GID == None:
		stmt = sql.select([rate]).\
			select_from(rate).\
			where(rate.c.CID == CID).\
			order_by(rate.c.CID, rate.c.GID)
	elif CID == None and GID != None:
		stmt = sql.select([rate]).\
			select_from(rate).\
			where(rate.c.GID == GID).\
			order_by(rate.c.CID, rate.c.GID)

	rets = []
	for row in _conn.execute(stmt):
		row_data = db_utils.row2dict(row, has_none=True)
		rets.append(row_data)

	if conn_ is None:
		_conn.close()
	return rets

def get_rate_matrix():
	import numpy as np
	rates = read_rate()
	num_cus = rates[len(rates)-1]['CID']
	num_good = rates[len(rates)-1]['GID']

	cus_id = 0
	rate_record = ""
	real_record = ""
	for record in rates:
		if record['CID'] != cus_id:
			cus_id = record['CID']
			rate_record += ";"+str(record['RATE'])
			real_record += ";"+str(record['REAL'])
		else:
			rate_record += " "+str(record['RATE'])
			real_record += " "+str(record['REAL'])

	rate_matrix = np.matrix(rate_record[1: len(rate_record)])
	real_matrix = np.matrix(real_record[1: len(real_record)])

	return {'rates':rate_matrix, 'real':real_matrix}

