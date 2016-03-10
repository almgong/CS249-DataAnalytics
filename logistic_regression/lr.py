from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import *
from pyspark.mllib.linalg import SparseVector

import time
import gc
'''
Module that we will use as an API to run spark's LR algorithm
'''

# userUserLoc = 'subalg/user_user/'
# userUserLoc = 'subalg/item_item/output/item_item_result_2.txt'
# itemItemLoc = 'subalg/item_item/output/item_item_results.txt'
# trainingLoc = 'data/rec_log_train.txt'	#relative to sc
# currDir = 'logistic_regression'

# toy data
userUserLoc = 'data/toy/item_item_toy.txt'
itemItemLoc = 'data/toy/item_item_toy_1.txt'
trainingLoc = 'data/toy/rec_log_train_100000.txt'	#relative to sc
currDir = 'logistic_regression'

userItemIndex = {} # user: {item: [ratings]}
def parsePoint(line):
	'''Given a line in a txt file, return the formatted LabeledPoint'''

	values = [float(x) for x in line.split(' ')]
	# return LabeledPoint(1, values)	#for now, label all as 1
	return LabeledPoint(values[0], values[1:])

def runLogisticRegression():
	'''Wrapper function that formats output of subalgs to run LR'''
	global userItemIndex
	start = time.time()
	#open all output files and generate in memory dictionary with .1's filled in for missing intersects
	#with open...
	print "open file 1"
	with open(itemItemLoc) as f:
		for line in f:
			line = line.split()
			user = line[0]
			item = line[1]
			rating = line[2]

			if not user in userItemIndex:
				userItemIndex[user] = {}
				userItemIndex[user][item] = {
					0: -1, #for label
					1: rating,
					2: 0.1,
					3: 0.1
				}
			else:
				userItemIndex[user][item] = {
					0: -1, #for label
					1: rating,
					2: 0.1,
					3: 0.1
				}
	print "Done with file 1"
	print "Total runtime: %s sec"%(time.time() - start)
	f.close()
	print "open file 2"
	with open(userUserLoc) as f:
		for line in f:
			line = line.split()
			user = line[0]
			item = line[1]
			rating = line[2]
			if not user in userItemIndex: # this user does not appears in sub1
				userItemIndex[user] = {}
				userItemIndex[user][item] = {
					0: -1, #for label
					1: 0.1,
					2: rating,
					# 3: 0.1
				}
			elif not item in userItemIndex[user]: # this user-item pair does not appear in sub1
				userItemIndex[user][item] = {
					0: -1, #for label
					1: 0.1,
					2: rating,
					# 3: 0.1
				}
			else: # this user-item pair appears in sub1
				userItemIndex[user][item][2] = rating
	print "Done with file 2"
	print "Total runtime: %s sec"%(time.time() - start)
	f.close()
	
	# open training file
	print "Open training file"
	with open(trainingLoc) as f:
		for line in f:
			line = line.split()
			user = line[0]
			item = line[1]
			if line[2] == '-1':
				label = 0
			else:
				label = 1
			if not user in userItemIndex:
				continue
			elif not item in userItemIndex[user]:
				continue
			else: #this user-item pair appears in our subalg result
				userItemIndex[user][item][0] = label
	print "Done with training file"
	print "Total runtime: %s sec"%(time.time() - start)
	f.close()

	print "Start writing input file for logistic regression"
	#write resultant dictionary to final output file to pass into logistic regression
	with open(currDir+"/output/input_for_lr.txt", 'w') as f:
		for user in userItemIndex:
			for item in userItemIndex[user]:
				if userItemIndex[user][item][0] == -1: # does not appear in training data
					continue
				f.write(str(userItemIndex[user][item][0])+" "+str(userItemIndex[user][item][1])+" "+str(userItemIndex[user][item][2])+" "+str(userItemIndex[user][item][3])+"\n")
	print "Done with writing input file for logistic regression"
	print "Total runtime: %s sec"%(time.time() - start)
	f.close()
	
	#open final output file and run logistic regression
	# itemItemFile = sc.textFile('subalg/item_item/output/item_item_results.txt')	#sc looks at dir of execution
	# itemItemData = itemItemFile.map(parsePoint)

	lrInputFile = sc.textFile(currDir+"/output/input_for_lr.txt")
	lrInputData = lrInputFile.map(parsePoint)

	#build the LR LogisticRegressionModel
	model = LogisticRegressionWithLBFGS.train(lrInputData)

	# get weights of this model, and then apply to in memory dictionary to get final recommendations to pass to evaluation
	weights = model.weights
	w1 = float(weights[0])
	w2 = float(weights[1])

	print "Start writing final output file"
	with open(currDir+"/output/final_output.txt", 'w') as f:
		for user in userItemIndex:
			for item in userItemIndex[user]:
				userItem = userItemIndex[user][item]
				value = float(userItem[0])*w1 + float(userItem[1])*w2
				userItem['value'] = value
			# print userItemIndex[user].items()
			userItemIndex[user] = sorted(userItemIndex[user].items(),
				key = lambda x: x[1]['value'], reverse=True)[:3]
			# write to file
			f.write(user)
			for i in userItemIndex[user]:
				f.write(" "+i[0])
			f.write("\n")
	print "Done with writing final output"
	f.close()
	userItemIndex = None
	gc.collect()
#run
# runLogisticRegression()