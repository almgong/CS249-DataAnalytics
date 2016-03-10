from pyspark.mllib.recommendation import ALS
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from collections import Counter

# train to build CF model with full info (user, product, following)
file_train1 = "/Users/hanwang/Desktop/249/CS249_data/train1.txt"

# predict result of train data (user, product) with model
file_train2 = "/Users/hanwang/Desktop/249/CS249_data/train2.txt"

def generateCandidatesWithWeights(sc):
	# load training and test data into (user, product, rating) tuples
	def parseRating(line):	
		fields = line.split()	
		return (int(fields[0]), int(fields[1]), float(fields[2]))   
	train1 = sc.textFile(file_train1).map(parseRating).cache()
	# test data for CF model part but train data for later logistic regression model
	test = sc.textFile(file_train2).map(parseRating).cache()

	# train a recommendation model
	model = ALS.train(training, rank = 10, iterations = 3)
	# make predictions on (user, product) pairs from the test data
	test_data = test.map(lambda x: (x[0], x[1]))
	#predictions are the input for logistic regression model
	predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
	return predictions