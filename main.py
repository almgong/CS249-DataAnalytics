from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import *
from pyspark.mllib.linalg import SparseVector

###preprocess###
#from preprocesser import Dataformatter
#from trendfinder import Trends 

from subalg.item_item import  *
from subalg.user_user import *

'''
Main driver for CS249 classification problem.
Designed to run in the interactive pyspark shell.
'''

### Run preprocessing ### NOT USED
print "Preprocessing..."
'''
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
'''

"""t = Trends.numFollowersBasedOnBirthYear(f)
t = Trends.numDistinctTagIds(f)
t = Trends.numDistinctKeywords(f)"""



'''
	Each algorithm will output its own list of items with ratings greater than
	some lower bound, then as input to LR, we need to automatically fill in
	gaps with some default low number (0.1)

'''



### Any preprocessing needed (may be none)
print "Preprocessing - Preparing data"



### Main logic, run 3 sub algorithms ###
print "Runing main logic"



### Postprocessing - may be none ###
print "Postprocessing - Cleaning up"







