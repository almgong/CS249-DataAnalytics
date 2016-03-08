###preprocess###
#from preprocesser import Dataformatter
#from trendfinder import Trends 

# from subalg.user_user import user_user
from subalg.item_item import item_item
from subalg.user_item import user_item
from subalg.user_user import *
from logistic_regression import lr

import gc
import os #for deleting temp files

'''
Main driver for CS249 classification problem.
Designed to run in the interactive pyspark shell.
'''

### Run preprocessing ### NOT USED
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


################################## new ################################


### Any preprocessing needed (may be none)
print "Preprocessing..."



### Main logic, run 3 sub algorithms ###
print "Runing main logic"

print "\nStarting user-user logic"
user_user.generateCandidatesWithWeights()
gc.collect()

results_user_item = user_item.generateCandidatesWithWeights(sc)

print "\nStarting item-item logic"
item_item.generateCandidatesWithWeights(sc) #pass in sc, expects a file to have been writter
gc.collect()

print "\n\nDone processing data, begin Logistic Regression..."
lr.runLogisticRegression(sc)

### Postprocessing - may be none ###
print "Postprocessing - Cleaning up"
try:
	os.remove('subalg/item_item/output/item_item_result.txt')

except:
	print "Something went wrong with removing temporary files, you may need to manually delete them."

print "Exiting spark..."




