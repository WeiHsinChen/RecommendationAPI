
class Matrix_Factorization:
	def __init__(self, rates = None, real = None, V = None, W = None):
		self.rates = rates
		self.real = real
		self.V = V
		self.W = W

	def whole_by_SGD(self, num_neuron, num_loop, step_size):
		if hasattr(self, 'rates'):
			import math
			import numpy as np
			from random import randint

			# variables
			num_viewer = int(np.shape(self.rates)[0])
			num_movie = int(np.shape(self.rates)[1])

			# initialize feature matrix
			V = np.matrix(np.random.rand(num_neuron, num_viewer))
			W = np.matrix(np.random.rand(num_neuron, num_movie))

			# start to training
			for i in xrange(num_loop):
				rated = False
				# random pick one viewer and one movie he/she rate
				while not rated:
					viewer_no = randint(0, num_viewer-1)
					movie_no = randint(0, num_movie-1)
					if int(self.real[viewer_no, movie_no]) == 1:
						rated = True

				# calculate old vn and wm vector
				vn_old = V[:, viewer_no]
				wm_old = W[:, movie_no]

				# calculate residual
				rate = int(self.rates[viewer_no, movie_no])
				residual = rate - (wm_old.T * vn_old)[0,0]

				# SGD-update
				# avoid reach breaking point
				if math.isnan(residual) or abs(wm_old.T * vn_old) > 15:
					print 'viewer_no: '+str(viewer_no)
					print 'movie_no: '+str(movie_no)
					break

				V[:, viewer_no] = vn_old + step_size * residual * wm_old
				W[:, movie_no] = wm_old + step_size * residual * vn_old

				if i%10000 == 0:
					print "Step: "+str(i) 

			error_table = np.matrix(np.zeros(shape = (num_viewer, num_movie)))
			error = 0
			for i in xrange(num_viewer):
				for j in xrange(num_movie):
					if int(self.rates[i, j]) != 0:
						e = math.pow(float(self.rates[i, j]) - W[:, j].T * V[:, i], 2)
						error += e
						error_table[i, j] = e

			return {"V": V, "W": W, "Ein": error, "EinTable": error_table}

		return False

	def single_by_SGD(self, cus_id, step_size):
		import math
		import numpy as np
		from random import randint

		# variables
		num_movie = int(np.shape(self.rates)[1])

		# start to training
		for i in xrange(num_movie):
			if int(self.real[0, i]) == 1:
				# variables
				viewer_no = cus_id - 1
				movie_no = i

				# calculate old vn and wm vector
				vn_old = self.V[:, viewer_no]
				wm_old = self.W[:, movie_no]

				# calculate residual
				rate = int(self.rates[0, movie_no])
				residual = rate - (wm_old.T * vn_old)[0,0]

				# SGD-update
				# avoid reach breaking point
				if math.isnan(residual) or abs(wm_old.T * vn_old) > 15:
					print 'viewer_no: '+str(viewer_no)
					print 'movie_no: '+str(movie_no)
					break

				self.V[:, viewer_no] = vn_old + step_size * residual * wm_old
				self.W[:, movie_no] = wm_old + step_size * residual * vn_old

		return {"V": self.V, "W": self.W}

	def predict_rate(self, viewer_id, movie_id):
		return (self.V[:, viewer_id-1].T * self.W[:, movie_id-1])[0,0]

	def parse_txt(self, resp_raw):
		import numpy as np
		rates_from_xlsx = []
		lines = resp_raw.split('\n')

		for i in xrange(1, len(lines)-1):
			temp = lines[i].split('\t')
			person_rate = []
			# TODO: find out why need to -2
			for j in xrange(2, len(temp)-2):
				r = str(temp[j])
				if self.is_number(r):
					person_rate.append(float(r))
				else:
					person_rate.append(0)
			rates_from_xlsx.append(person_rate)

		self.rates = np.matrix(rates_from_xlsx)

	def parse_VorW_txt(self, V_txt):
		import numpy as np
		V_raw = []
		lines = V_txt.split('\n')

		for i in xrange(len(lines)-1):
			temp = lines[i].split(' ')
			cell = []
			# TODO: find out why need to -2
			for j in xrange(len(temp)):
				cell.append(float(temp[j]))

			V_raw.append(cell)
			V = np.matrix(V_raw)
		return V

	def sign(x):
		if x > 0:
			return 1
		else:
			return -1

	def is_number(self, s):
	    try:
	        float(s)
	        return True
	    except ValueError:
	        pass
	 
	    try:
	        import unicodedata
	        unicodedata.numeric(s)
	        return True
	    except (TypeError, ValueError):
	        pass
	 	return False



