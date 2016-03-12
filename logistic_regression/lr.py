from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import *
from pyspark.mllib.linalg import SparseVector

import time
import gc
'''
Module that we will use as an API to run spark's LR algorithm
'''

userUserLoc = 'subalg/user_user/output/user_user_results.txt'
itemItemLoc = 'subalg/item_item/output/item_item_results.txt'
userItemLoc = 'subalg/user_item/output/user_item_results.txt'
newUserItemLoc = 'subalg/user_item/output/test_user_item_results.txt'
trainingLoc = 'data/train2.txt'	#relative to sc
# trainingLoc = 'data/rec_log_train.txt'
testLoc = 'data/rec_log_test.txt'
currDir = 'logistic_regression'

# toy data
# userUserLoc = 'data/toy/item_item_toy.txt'
# itemItemLoc = 'data/toy/item_item_toy_1.txt'
# userItemLoc = 'data/toy/item_item_toy_2.txt'
# trainingLoc = 'data/toy/rec_log_train_100000.txt'	#relative to sc
# currDir = 'logistic_regression'

userItemIndex = {} # user: {item: [ratings]} dictionary to LR
newUserItemIndex = {} # dictionary for test
def parsePoint(line):
	'''Given a line in a txt file, return the formatted LabeledPoint'''

	values = [float(x) for x in line.split(' ')]
	# return LabeledPoint(1, values)	#for now, label all as 1
	return LabeledPoint(values[0], values[1:])

def runLogisticRegression(sc):
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
					2: 0,
					3: 0
				}
			else:
				userItemIndex[user][item] = {
					0: -1, #for label
					1: rating,
					2: 0,
					3: 0
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
			if not user in userItemIndex: # this user does not appears before
				userItemIndex[user] = {}
				userItemIndex[user][item] = {
					0: -1, #for label
					1: 0,
					2: rating,
					3: 0
				}
			elif not item in userItemIndex[user]: # this user-item pair does not appear before
				userItemIndex[user][item] = {
					0: -1, #for label
					1: 0,
					2: rating,
					3: 0
				}
			else: # this user-item pair appears in sub1
				userItemIndex[user][item][2] = rating
	print "Done with file 2"
	print "Total runtime: %s sec"%(time.time() - start)
	f.close()

	newUserItemIndex = userItemIndex # here two dictionary differ

	print "open file 3"
	with open(userItemLoc) as f:
		for line in f:
			line = line.strip('(')
			line = line.split(',')
			user = line[0]
			item = line[1]
			rating = line[2].split(')')[0]
			if not user in userItemIndex: # this user does not appears before
				userItemIndex[user] = {}
				userItemIndex[user][item] = {
					0: -1, #for label
					1: 0,
					2: 0,
					3: rating
				}
			elif not item in userItemIndex[user]: # this user-item pair does not appear before
				userItemIndex[user][item] = {
					0: -1, #for label
					1: 0,
					2: 0,
					3: rating
				}
			else: # this user-item pair appears in sub1
				userItemIndex[user][item][3] = rating
	print "Done with file 3"
	print "Total runtime: %s sec"%(time.time() - start)
	print "Length of userItemIndex = " + str(len(userItemIndex)) #1,816,664
	f.close()	
	
	print "open file 4"
	with open(newUserItemLoc) as f:
		for line in f:
			line = line.strip('(')
			line = line.split(',')
			user = line[0]
			item = line[1]
			rating = line[2].split(')')[0]
			if not user in newUserItemLoc: # this user does not appears before
				newUserItemLoc[user] = {}
				newUserItemLoc[user][item] = {
					0: -1, #for label
					1: 0,
					2: 0,
					3: rating
				}
			elif not item in newUserItemLoc[user]: # this user-item pair does not appear before
				newUserItemLoc[user][item] = {
					0: -1, #for label
					1: 0,
					2: 0,
					3: rating
				}
			else: # this user-item pair appears in sub1
				newUserItemLoc[user][item][3] = rating
	print "Done with file 3"
	print "Total runtime: %s sec"%(time.time() - start)
	print "Length of newUserItemLoc = " + str(len(newUserItemLoc)) #1,816,664
	f.close()	

	# open training file
	print "Open training file"
	userInTrainingButNotInOutput = 0
	itemInTrainingButNotInOutput = 0
	with open(trainingLoc) as f:
		for line in f:
			line = line.split()
			user = line[0]
			item = line[1]
			label = line[2]

			if not user in newUserItemIndex:
				userInTrainingButNotInOutput += 1
				continue
			elif not item in newUserItemIndex[user]:
				itemInTrainingButNotInOutput += 1
				continue
			else: #this user-item pair appears in our subalg result
				newUserItemIndex[user][item][0] = label
	print "Done with training file"
	print "userInTrainingButNotInOutput: " + str(userInTrainingButNotInOutput)
	print "itemInTrainingButNotInOutput: " + str(itemInTrainingButNotInOutput)
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

	# start logistic regression	
	print "Start Logistic Regression..."
	lrInputFile = sc.textFile(currDir+"/output/input_for_lr.txt")
	lrInputData = lrInputFile.map(parsePoint)

	#build the LR LogisticRegressionModel
	model = LogisticRegressionWithLBFGS.train(lrInputData)

	# get weights of this model, and then apply to in memory dictionary to get final recommendations to pass to evaluation
	weights = model.weights
	w1 = float(weights[0])
	w2 = float(weights[1])
	w3 = float(weights[2])
	print "Total runtime after Logistic Regression: %s sec"%(time.time() - start)

	# For testing, look at the users that appear in test file only
	print "Start looking at test data..."
	testUsers = {} # users that appear in test data
	with open(testLoc) as test:
		for line in test:
			line = line.split()
			testUser = line[0]
			if testUser not in testUsers:
				testUsers[testUser] = 0

	print "Start writing final output file"
	skipperUsers = 0
	with open(currDir+"/output/final_output.txt", 'w') as f:
		for user in newUserItemIndex:
			if user not in testUsers:
				skipperUsers += 1
				continue
			else: # only generate recommendation list for users in test dataset
				for item in newUserItemIndex[user]:
					userItem = newUserItemIndex[user][item]
					value = float(userItem[1])*w1 + float(userItem[2])*w2 + float(userItem[3])*w3
					userItem['value'] = value
				newUserItemIndex[user] = sorted(newUserItemIndex[user].items(),
					key = lambda x: x[1]['value'], reverse=True)[:3]
				# write to file
				f.write(user)
				for i in newUserItemIndex[user]:
					f.write(" "+i[0])
				f.write("\n")
	print "Done with writing final output"
	print "Skipped Users: " + str(skipperUsers)
	print "w1 = " + str(w1) + "w2 = " + str(w2) + "w3 = " + str(w3)
	print "Total runtime: %s sec"%(time.time() - start)
	f.close()
	newUserItemIndex = None
	testUsers = None
	gc.collect()
#run
# runLogisticRegression()