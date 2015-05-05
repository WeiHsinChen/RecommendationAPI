

def train(num_neuron=8, num_loop=10):
	# my code here
	import os
	import codecs
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
	print "Time taken: ", ExeTime, " seconds."
	print 'Ein'
	print Ein
	print ''
	print 'Train data successfully'
	# 

	# return result
	# print V
	# print W
	# print Rec.predict_rate(1, 2)

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

	print 'Save raw data successfully'

def add_a_customer(raw_data, cus_id=None):
	import math
	import numpy as np
	from RS import Matrix_Factorization as MF
	from rspy.db import rs as db_rs


	# add a new customer
	goods_raw = raw_data[0].split('	')
	goods_dict = db_rs.get_goods_dict()
	temp = raw_data[1].split('	')
	cus_list = []
	cus_raw = {}
	if cus_id == None:
		cus_list.append({'NAME':temp[1]})	
	else:
		cus_list.append({'ID': int(cus_id), 'NAME':temp[1]})
	cus_raw[temp[1]] = {}
	rates = ""
	real = ""
	for j in xrange(2, len(temp)-2):
		if is_int(temp[j]):
			cus_raw[temp[1]][goods_raw[j]] = temp[j]
			rates += " "+ temp[j]
			real += " 1"
		else:
			cus_raw[temp[1]][goods_raw[j]] = None
			rates += " 0"
			real += " 0"

	if cus_id == None:
		cus_dict = db_rs.add_a_customer(cus_list)
	else:
		cus_dict = db_rs.add_a_customer(cus_list, cus_id = int(cus_id))

	# parse rates and real record for this customer 
	rates = np.matrix(rates)
	real = np.matrix(real)

	# save rate record
	record_list = []
	for cus, record in cus_raw.items():
		for good, rate in record.items():
			if rate != None:
				record_list.append({'CID': cus_dict[cus],'GID': goods_dict[good],'RATE': int(rate), 'REAL':1})
			else:
				record_list.append({'CID': cus_dict[cus],'GID': goods_dict[good],'RATE': 0, 'REAL':0})
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
	cus_id = cus_dict[temp[1]]
	result = Rec.single_by_SGD(cus_id, step_size)

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
			pred_rate.append({'CID':cus_id, 'GID': j+1, 'RATE': Rec.predict_rate(cus_id, j), 'REAL':0})
	db_rs.update_a_pred_rate(pred_rate, CID = cus_id)

	print 'Add a new customer successfully'

def add_a_good(good_name):
	import math
	import numpy as np
	from rspy.db import rs as db_rs

	# add a good into TABLE(GOODS)
	goods_list = []
	goods_list.append({'NAME': good_name})
	goods_dict = db_rs.add_a_good(goods_list)
	good_id = goods_dict[good_name]

	# add blank rate into TABLE(RATE)
	pred_rate = []
	cus_dict = db_rs.get_customers_dict()
	for cus_name, cus_id in cus_dict.items():
		# default rate is 5 if adding a new commodity
		pred_rate.append({'CID':cus_id, 'GID': good_id, 'RATE': 5, 'REAL':0}) 
	db_rs.update_a_pred_rate(pred_rate, GID = good_id)

	# changing W matrix
	W = np.matrix(np.loadtxt('../data/WTable.txt'))
	num_neuron = int(np.shape(W)[0])
	W = np.concatenate((W, np.matrix(np.random.rand(num_neuron, 1))), 1)
	np.savetxt('../data/WTable.txt', W)	

	print 'Add a new good successfully'

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
		print 'you already rated all commodities'


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
		return '(id: '+str(self.id)+', name: '+self.name.decode('utf-8').encode('Big5')+', rate: '+str(round(self.rate,2))+')'

import sys
import codecs

if __name__ == '__main__':
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
		elif sys.argv[1] == 'add_a_cus':
			file_location = sys.argv[2].strip()
			with codecs.open(file_location, mode='r') as f:
				raw_data = f.readlines()
			add_a_customer(raw_data)
		elif sys.argv[1] == 'update_a_cus':
			file_location = sys.argv[3].strip()
			with codecs.open(file_location, mode='r') as f:
				raw_data = f.readlines()
			add_a_customer(raw_data, cus_id = int(sys.argv[2]))
		elif sys.argv[1] == 'add_a_good':
			good_name = sys.argv[2].decode('Big5').encode('utf-8')
			add_a_good(good_name)
		elif sys.argv[1] == 'rec_for_a_cus':
			if len(sys.argv) == 4:
				rec_for_a_cus(int(sys.argv[2]), num_rec=int(sys.argv[3]))
			elif len(sys.argv) == 3:
				rec_for_a_cus(int(sys.argv[2]))
			else:
				print 'Please type the correct format'
	else:
		print 'Please type the correct format'

