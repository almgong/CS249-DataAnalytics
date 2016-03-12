###preprocess###
#from preprocesser import Dataformatter
#from trendfinder import Trends 

from subalg.user_user import user_user
from subalg.item_item import item_item
from subalg.user_item import user_item
from logistic_regression import lr
import evaluation

import gc
import os #for deleting temp files
import time

'''
Main driver for CS249 classification problem.
Designed to run in the interactive pyspark shell.
'''
start = time.time()

### Any preprocessing needed (may be none)
print "Preprocessing..."
print "Splitting training file..."
execfile('splitTrainFile.py')
os.rename("data/train1/part-00000","data/train1.txt")
os.rename("data/train2/part-00000","data/train2.txt")

#os.rename("data/train3/part-00000","data/train3.txt")

### Main logic, run 3 sub algorithms ###
print "Runing main logic"

print "\nStarting user-user logic"
user_user.generateCandidatesWithWeights()
gc.collect()

print "\nStarting user-item logic"
user_item.generateCandidatesWithWeights(sc)
gc.collect()
os.rename("subalg/user_item/output/part-00000","subalg/user_item/output/user_item_results.txt")

print "\nStarting item-item logic"
item_item.generateCandidatesWithWeights(sc) #pass in sc, expects a file to have been writter
gc.collect()

print "\n\nDone processing data, begin Logistic Regression..."
lr.runLogisticRegression(sc)

print "Starting evaluation..."
resultLoc = 'logistic_regression/output/final_output.txt'
solLoc = 'data/solution.csv'

print evaluation.evaluation(resultLoc, solLoc)

### Postprocessing - Mainly should be to delete files created on disk ###
print "Postprocessing - Cleaning up"
try:
	os.remove('subalg/item_item/output/item_item_results.txt')
	os.remove('subalg/user_user/output/user_user_results.txt')
	os.remove('subalg/item_item/output/user_item_results.txt')
	os.remove('logistic_regression/output/input_for_lr.txt')

except:
	print "Something went wrong with removing temporary files, you may need to manually delete them."

print "Exiting spark..."


