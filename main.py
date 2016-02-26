from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

###preprocess###
from preprocesser import Dataformatter
from trendfinder import Trends 

'''
Main driver for CS249 classification problem.
Designed to run in the interactive pyspark shell.
'''

### Code to obtain data ###
print "Obtaining and Formatting data..."
data = [
	LabeledPoint(0.0, [0.0, 1.0]),
	LabeledPoint(1.0, [1.0, 0.0])
]

### Run preprocessing ###
print "Preprocessing..."
f = Dataformatter.generateUserDict("data/user_profile.txt", 
	"data/item.txt",
	"data/user_sns.txt")

t = Trends.numFollowersBasedOnBirthYear(f)

### Run main algorithm ###
#lrm = LogisticRegressionWithLBFGS.train(sc.parallelize(data), iterations=10)

### Post Processing ###
print "Postprocessing..."

### Final output ###
print "Predicting..."
#lrm.predict([.5,.4]) #should be 1
