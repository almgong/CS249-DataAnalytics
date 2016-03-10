from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import *
from pyspark.mllib.linalg import SparseVector

'''
Module that we will use as an API to run spark's LR algorithm
'''

userUserLoc = 'subalg/user_user/'
itemItemLoc = 'subalg/item_item/output/item_item_results.txt'	#relative to sc

def parsePoint(line):
	'''Given a line in a txt file, return the formatted LabeledPoint'''

	values = [float(x) for x in line.split(' ')]
	return LabeledPoint(1, values)	#for now, label all as 1

def runLogisticRegression(sc):
	'''Wrapper function that formats output of subalgs to run LR'''

	#open all output files and generate in memory dictionary with .1's filled in for missing intersects
	#with open...

	#write resultant dictionary to final output file to pass into logistic regression


	#open final output file and run logistic regression
	itemItemFile = sc.textFile('subalg/item_item/output/item_item_results.txt')	#sc looks at dir of execution
	itemItemData = itemItemFile.map(parsePoint)

	#build the LR LogisticRegressionModel
	model = LogisticRegressionWithLBFGS.train(itemItemData)

	#get weights of this model, and then apply to in memory dictionary to get final recommendations to pass to evaluation