

def train(num_neuron=8, num_loop=1000000):
	# my code here
	import os
	import time
	import numpy as np
	from RS import Matrix_Factorization as MF
	from rspy.db import rs as db_rs

	# variables
	step_size = 0.005

	# recoro start time
	tStart = time.time()

	# get rate from db
	temp = db_rs.get_rate_matrix()
	Rec = MF(rates = temp['rates'], real = temp['real'])

	# start training
	result = Rec.whole_by_SGD(num_neuron, num_loop, step_size)

	# record end time
	tEnd = time.time() 
	ExeTime = (tEnd - tStart)

	# get result
	V = result["V"]
	W = result["W"]
	Ein = result["Ein"]
	# EinTable = result["EinTable"]

	# save V and W
	np.savetxt('../data/VTable.txt', V)	
	np.savetxt('../data/WTable.txt', W)

	# save predicted rate into database
	pred_rate = []
	R = V.T * W
	num_cus = int(np.shape(temp['real'])[0])
	num_goods = int(np.shape(temp['real'])[1])
	for i in xrange(num_cus):
		for j in xrange(num_goods):
			if temp['real'][i, j] == 0:
				pred_rate.append({'CID':i+1, 'GID': j+1, 'RATE': R[i, j], 'REAL':0})
	db_rs.update_all_pred_rate(pred_rate)

	# print Ein
	print "Time taken: ", round(ExeTime,2), " seconds."
	print 'Ein: ', round(Ein,5)
	print ''
	print 'Train data successfully'

def save_raw_data(raw_data):
	import math
	from rspy.db import rs as db_rs

	# update goods
	goods_list = []
	goods_raw = raw_data[0].split('	')
	for i in xrange(2, len(goods_raw)-2):
		goods_list.append({'NAME':goods_raw[i]})
	goods_dict = db_rs.add_goods(goods_list)

	# update customers
	cus_list = []
	cus_raw = {}
	for i in xrange(1, len(raw_data)):
		temp = raw_data[i].split('	')
		cus_list.append({'NAME':temp[1]})
		cus_raw[temp[1]] = {}
		for j in xrange(2, len(temp)-2):
			if is_int(temp[j]):
				cus_raw[temp[1]][goods_raw[j]] = temp[j]
			else:
				cus_raw[temp[1]][goods_raw[j]] = None
	cus_dict = db_rs.add_customers(cus_list)

	# save rate record
	record_list = []
	for cus, record in cus_raw.items():
		for good, rate in record.items():
			if rate != None:
				record_list.append({'CID': cus_dict[cus],'GID': goods_dict[good],'RATE': int(rate), 'REAL':1})
			else:
				record_list.append({'CID': cus_dict[cus],'GID': goods_dict[good],'RATE': 0, 'REAL':0})
	db_rs.add_rates(record_list)

	print ''
	print 'Save raw data successfully'

def add_a_customer(raw_data, name, cus_id=None):
	import math
	import numpy as np
	from RS import Matrix_Factorization as MF
	from rspy.db import rs as db_rs

	# raw_data = [{'GID': 12, 'RATE': 5}, {'GID': 2, 'RATE': 3}, {'GID': 9, 'RATE': 9}]
	# add a new customer
	cus_list = []
	if cus_id == None:
		cus_list.append({'NAME':name})	
		cus_id_upd = db_rs.add_a_customer(cus_list)
	else:
		cus_list.append({'ID': int(cus_id), 'NAME':name})
		cus_id_upd = db_rs.add_a_customer(cus_list, cus_id = int(cus_id))
		update = True

	# transform raw_data to m_data = {12:5, 2:3, 9:9}
	m_data = {}
	for item in raw_data:
		m_data[item['GID']] = item['RATE']

	# record rate and real vector
	goods_list = db_rs.max_good_id()
	max_good_no = max(goods_list)
	rated_goods_list = db_rs.rated_goods(cus_id) if cus_id != None else {}
	record_list = []
	rates = ""
	real = ""
	for i in xrange(1,max_good_no+1):
		if i in m_data:
			rates += " "+ str(m_data[i])
			real += " 1"
			record_list.append({'CID': cus_id_upd,'GID': i,'RATE': m_data[i], 'REAL':1})
		elif i in rated_goods_list:
			rates += " "+ str(rated_goods_list[i])
			real += " 1"
			record_list.append({'CID': cus_id_upd,'GID': i,'RATE': rated_goods_list[i], 'REAL':1})
		else:
			rates += " 0"
			real += " 0"
			record_list.append({'CID': cus_id_upd,'GID': i,'RATE': 0, 'REAL':0})

	# parse rates and real record for this customer 
	rates = np.matrix(rates)
	real = np.matrix(real)

	# save rate record
	if cus_id == None:
		db_rs.add_a_rate(record_list)
	else:
		db_rs.add_a_rate(record_list, CID = int(cus_id))

	# changing V matrix
	V = np.matrix(np.loadtxt('../data/VTable.txt'))
	W = np.matrix(np.loadtxt('../data/WTable.txt'))

	if cus_id == None:
		num_neuron = int(np.shape(V)[0])
		V = np.concatenate((V, np.matrix(np.random.rand(num_neuron, 1))), 1)

	# train single data
	# variables
	step_size = 0.005
	Rec = MF(rates = rates, real = real, V = V, W = W)

	# start train
	result = Rec.single_by_SGD(cus_id_upd, step_size)

	# save V and W
	V = result['V']
	W = result['W']
	np.savetxt('../data/VTable.txt', V)	
	np.savetxt('../data/WTable.txt', W)	

	# save predicted rate into TABLE(RATE)
	pred_rate = []
	num_goods = int(np.shape(real)[1])
	for j in xrange(num_goods):
		if real[0, j] == 0:
			pred_rate.append({'CID':cus_id_upd, 'GID': j+1, 'RATE': Rec.predict_rate(cus_id_upd, j), 'REAL':0})
	db_rs.update_a_pred_rate(pred_rate, CID = cus_id_upd)

	print ''
	if cus_id != None:
		print 'Update a new customer successfully'
	else:
		print 'Add a new customer successfully'

	return cus_id_upd

def add_goods(goods):
	import math
	import numpy as np
	from rspy.db import rs as db_rs

	id_list = []
	for i in xrange(len(goods)):
		# add a good into TABLE(GOODS)
		goods_list = []
		good_name = goods[i].encode('utf-8')
		goods_list.append({'NAME': good_name})
		good_id = db_rs.add_a_good(goods_list)
		id_list.append(good_id)

		# add blank rate into TABLE(RATE)
		pred_rate = []
		cus_dict = db_rs.get_customers_dict(reverse=True)
		for cus_id, cus_name in cus_dict.items():
			# default rate is 5 if adding a new commodity
			pred_rate.append({'CID':cus_id, 'GID': good_id, 'RATE': 5, 'REAL':0}) 
		db_rs.update_a_pred_rate(pred_rate, GID = good_id)

		# changing W matrix
		W = np.matrix(np.loadtxt('../data/WTable.txt'))
		num_neuron = int(np.shape(W)[0])
		W = np.concatenate((W, np.matrix(np.random.rand(num_neuron, 1))), 1)
		np.savetxt('../data/WTable.txt', W)	

	print ''
	print 'Add a new good successfully'
	return id_list

def rec_for_a_cus(cus_id, num_rec=3):
	from Queue import PriorityQueue
	from rspy.db import rs as db_rs

	q = PriorityQueue()
	rates = db_rs.read_rate(CID = cus_id)
	goods_dict = db_rs.get_goods_dict(reverse=True)
	for record in rates:
		if record['REAL'] == 0:
			q.put(Movie(record['GID'], goods_dict[record['GID']], record['RATE']))

	temp = ""
	if not q.empty():
		s = q.qsize()
		for i in xrange(num_rec if num_rec < s else s):
			temp += "\n"+q.get().__str__()
		print temp[1:len(temp)]
	else:
		print ''
		print 'There is no such customer!'

def test():
	from rspy.db import rs as db_rs
	print db_rs.max_good_id()

def is_int(s):
	try:
		int(s)
		return True
	except ValueError:
		return False

class Movie(object):
	def __init__(self, id, name, rate):
		self.id = id
		self.name = name
		self.rate = rate
	def __cmp__(self, other):
		if self.rate >= other.rate:
			return -1
		else:
			return 1
	def __str__(self):
		# IMPORTANT: encode to Big5 in order to display on cmp
		return '(GID: '+str(self.id)+', NAME: '+self.name.decode('utf-8').encode('Big5')+', RATE: '+str(round(self.rate,2))+')'

if __name__ == '__main__':
	import sys
	import codecs
	import json
	if len(sys.argv) > 1:
		# file_location = sys.argv[1].strip()
		if sys.argv[1] == 'save_raw_data':
			file_location = sys.argv[2].strip()
			# input_data_file = open(file_location, 'r')
			# raw_data = input_data_file.readlines()
			# input_data_file.close()
			with codecs.open(file_location, mode='r') as f:
				raw_data = f.readlines()
			save_raw_data(raw_data)
		elif sys.argv[1] == 'train':
			if len(sys.argv) == 4:
				train(num_neuron=int(sys.argv[2]), num_loop=int(sys.argv[3]))
			elif len(sys.argv) == 2:
				train()
			else:
				print 'Please type the correct format'
		elif sys.argv[1] in ('add_a_cus', 'update_a_cus'):
			file_location = sys.argv[2].strip()
			with codecs.open(file_location, mode='r') as f:
				raw_data = f.readlines()
			raw_data = json.loads(raw_data[0].decode('Big5').encode('utf-8'))
			add_a_customer(raw_data['DATA'], raw_data['NAME'].encode('utf-8'), cus_id=raw_data['ID'] if 'ID' in raw_data else None)
		elif sys.argv[1] == 'add_goods':
			file_location = sys.argv[2].strip()
			with codecs.open(file_location, mode='r') as f:
				raw_data = f.readlines()
			goods = json.loads(raw_data[0].decode('Big5').encode('utf-8'))
			add_goods(goods)
		elif sys.argv[1] == 'rec_for_a_cus':
			if len(sys.argv) == 4:
				rec_for_a_cus(int(sys.argv[2]), num_rec=int(sys.argv[3]))
			elif len(sys.argv) == 3:
				rec_for_a_cus(int(sys.argv[2]))
			else:
				print 'Please type the correct format'
		elif sys.argv[1] == 'test':
			file_location = sys.argv[2].strip()
			with codecs.open(file_location, mode='r') as f:
				raw_data = f.readlines()
			raw_data = json.loads(raw_data[0].decode('Big5').encode('utf-8'))
			add_a_customer(raw_data['DATA'], raw_data['NAME'], cus_id=raw_data['ID'] if 'ID' in raw_data else None)
	else:
		print 'Please type the correct format'

