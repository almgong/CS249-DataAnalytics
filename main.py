from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import *

###preprocess###
from preprocesser import Dataformatter
from trendfinder import Trends 

'''
Main driver for CS249 classification problem.
Designed to run in the interactive pyspark shell.
'''

### Code to obtain data ###
print "Obtaining and Formatting data..."


### Run preprocessing ###
print "Preprocessing..."
userDict = {}
userFV = {} 	#feature vectors per user
Dataformatter.parseKDDData(userDict,
	userFV, 
	"data/user_profile.txt", 
	"data/item.txt",
	"data/user_sns.txt",
	"data/user_key_word.txt")

userDict = None 						#null to free up memory
userFV = [userFV[i] for i in userFV]	#convert to list of SparseVectors()
bcFV = sc.broadcast(userFV)
userFV = None
model = KMeans.train(sc.parallelize(bcFV.value), 20, initializationMode="k-means||",seed=50, initializationSteps=5, epsilon=1e-4)

"""t = Trends.numFollowersBasedOnBirthYear(f)
t = Trends.numDistinctTagIds(f)
t = Trends.numDistinctKeywords(f)"""

### Run main algorithm ###
#lrm = LogisticRegressionWithLBFGS.train(sc.parallelize(data), iterations=10)

### Post Processing ###
print "Postprocessing..."

### Final output ###
print "Predicting..."
#lrm.predict([.5,.4]) #should be 1
