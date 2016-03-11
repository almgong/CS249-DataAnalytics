from pyspark.mllib.fpm import FPGrowth #for frequent pattern mining
from pyspark.mllib.feature import Normalizer #normalize data ratings
import gc
import os
import time
import math

''''
Module that will compute a list of user-most likely to follow
items, considering similarity items followed by the user.
'''

### list of file locations ###
itemLoc = 'data/item.txt'
userActionLoc = 'data/user_action.txt'
userSNSLoc = 'data/user_sns.txt'
currDir = 'subalg/item_item'

### Indexes used in logic, please dereference after usage! ###
categoryIndex = {} 	#{3-level-cat: [list of item ids], ... }
keywordIndex = {}	#{keywordID:[list of item ids], ...} - might not be needed!
itemIndex = {}		#{itemID: {keywords:[], category:'x.x.x'},... }
sharedKeywordsIndex = {} 	#{itemID:{itemID2:value,...},... }

userActionIndex = {}	#{userID:{itemID:numActionsToThisItem, ...},...}

### Mapping functions for preprocessing data ###
def mapLineToItemIndexes(line):
	'''Given a line from item.txt, store to appropriate indexes'''
	
	line = line.split()
	itemID = line[0]
	category = line[1][:5] 			#to get x.x.x|<truncated>
	keywords = line[2].split(";")	#list of keywords


	#add entry to category index
	if category in categoryIndex:
		categoryIndex[category].append(itemID)
	else:
		categoryIndex[category] = [itemID]

	#add entries to keyword index
	for kw in keywords:
		if kw in keywordIndex:
			keywordIndex[kw].append(itemID)
		else:
			keywordIndex[kw] = [itemID]

	#Lastly, add entry to itemIndex
	itemIndex[itemID] = {
		'keywords': keywords,
		'category': category
	}

def mapLineToUserSNSIndex(line):
	'''Given a line in the user_sns.txt file, store info to appropriate index'''

	line = line.split()	#should be 2 element array [userID, followedItemID]
	if line[0] in userActionIndex:
		userActionIndex[line[0]][line[1]] = 1 		#def 1 because we multiply later
	else:
		userActionIndex[line[0]] = {line[1]:1}


def mapLineToUserActionIndex(line):
	'''Given line in user_action.txt, store info to appropriate index'''

	line = line.split() #always in form [user, target, numRetweet, num@, numComment]

	#will store in same struct as userSNSIndex, for memory efficiency 
	# - also we only care about	items the user actually follows!
	num = int(line[2]) + int(line[3]) + int(line[4])
	if line[0] in userActionIndex:
		if line[1] in userActionIndex[line[0]]: #if the user follows this target
			userActionIndex[line[0]][line[1]] = num

### Actual Logic ###

def mergeDictionaries(d1, d2):
	'''Merges 2 dictionaries and returns the result'''

	ret = d1.copy()
	ret.update(d2)
	return ret

def getNumOrPropShared(list1, list2, returnNum=False):
	'''
	Gets either the number of items shared between 2 lists, or the proportion, 
	using the first list as the reference list (e.g. proportion/num of items in
	list2 that is in list1).

	By Default returns the proportion, if you pass in getNumOrPropShared(x,y,True),
	it will instead just return the number shared between the 2 lists and NOT 
	the proportion shared.
	'''

	#ensure uniqueness in elements lists
	list1 = set(list1)
	list2 = set(list2)

	#variables 
	startingLength = len(list1)	#denominator for proportion
	numShared = startingLength - len(list1-list2)	#numerator

	if returnNum:
		return numShared

	return (numShared*1.0)/startingLength	#returns the proportion

def applyUserActionWeights(sim10List, numActionsOnSimilarItem):
	'''Applies weights of number of actions onto expected tuples'''

	ret = {}	#to return
	for tup in sim10List:
		if not numActionsOnSimilarItem:	#if num actions is 0
			numActionsOnSimilarItem = 1
		ret[tup[0]] = tup[1]*(math.log(numActionsOnSimilarItem, 10)+0.25)  #itemID:newValue

	return ret

def filterAlreadyFollowed(user, items):
	'''
	Given the input dictionary, filter out the items that are already 
	followed by this user 
	'''
	toRemove = []
	for item in items:
		if item in userActionIndex[user]:	#if user already follows this item
			toRemove.append(item)

	for item in toRemove:
		del items[item]

def normalizeData(sc, fileToNormalize="subalg/item_item/output/item_item_results_unnormalized.txt", 
	fileToCreate="subalg/item_item/output/item_item_results.txt"):
	'''Normalizes values in a subalg output file and normalizes the values'''


	def parseLine(line):
		'''Inner helper for getting just the rating value'''
		return float(line.split(' ')[2]) 	#just the ratings

	n2 = Normalizer()
	finalOutputFile = currDir+fileToCreate
	ratings = sc.textFile(fileToNormalize).map(parseLine)
	#rdd = sc.parallelize(ratings,2)
	results = n2.transform(ratings.collect())

	#open to read and write simaltaneously, update the weights
	i = 0
	with open(fileToNormalize) as f:
		with open(fileToCreate, "a+") as fToCreate:

			#for each line in file to norm
			for line in f:
				line = line.split()
				fToCreate.write(line[0] + " " + line[1] + " " + str(results[i])+"\n")
				i+=1

	os.remove('subalg/item_item/output/item_item_results_unnormalized.txt')


def generateCandidatesWithWeights(sc):
	'''
	Wrapper function for generating candidates

	Notes: for items without other items in the same cat, no value is outputted.
	Only users who follow items are considered (users are drawn form user_sns).
	Also, only those users who actually perform some action towards a followed
	item are considered, roughly 1 million users are outputted with some 
	recommendations - this is the biggest filtering that occurs for item-item.
	'''

	#use of globals, need to explicitly state since we will None them later
	global categoryIndex 
	global keywordIndex
	global itemIndex
	global userActionIndex
	global sharedKeywordsIndex
	global resultIndex

	start = time.time()

	#proactively open file and index what we need
	print "Opening item.txt file..."
	with open(itemLoc) as itemFile:
		for line in itemFile:
			mapLineToItemIndexes(line)			#generate indexes to use

	print "Done with reading file and generating indexes..."
	print "Starting main logic to generate candidates..."
	
	#for each item, populate the sharedKeywordsIndex
	for item in itemIndex:
		#first lets look only at items within the same category
		sameCatItems = categoryIndex[itemIndex[item]['category']]

		#if there are no items within the same cat, skip
		if not len(sameCatItems):
			sharedKeywordsIndex[item] ={}
			continue

		#for each item in the same category, compute the proportion of shared keywords - store in sharedKeywordsIndex
		for itemWithSameCat in sameCatItems:
			if itemWithSameCat==item:
				continue
			
			if not item in sharedKeywordsIndex:
				sharedKeywordsIndex[item] = {
					itemWithSameCat : getNumOrPropShared(itemIndex[item]['keywords'], 
						itemIndex[itemWithSameCat]['keywords'])
				} 

			else:
				sharedKeywordsIndex[item][itemWithSameCat] = getNumOrPropShared(itemIndex[item]['keywords'],
					itemIndex[itemWithSameCat]['keywords'])

	#last thing, sort each resultant entry in sharedKeywordsIndex, limit to up to top 10 for memory efficiency
	for entry in sharedKeywordsIndex:
		sharedKeywordsIndex[entry] = sorted(sharedKeywordsIndex[entry].items(), 
			key=lambda x: x[1], reverse=True)[:2]

	print "Finished Candidate Generation, took %s secs"%(time.time()-start)

	#Can now free some memory - recall global statement above
	categoryIndex = None
	keywordIndex = None
	itemIndex = None
	gc.collect()

	#read in new data, user_sns and user_action
	print "Opening user_sns.txt, preprocess"
	with open(userSNSLoc) as userSNSFile:
		for line in userSNSFile:
			mapLineToUserSNSIndex(line)

	print "Finished with user_sns preprocessing..."
	print "Opening user_action.txt, preprocess..."
	with open(userActionLoc) as userActionFile:
		for line in userActionFile:
			mapLineToUserActionIndex(line)	#generate user-action table

	print "Finished with user_action preprocessing..."
	print "Generating final return, total runtime so far: %s..."%(time.time()-start)
	countSkipped = 0

	#apply weights to sharedKeywordsIndex based on number of user actions (simply multiplying)
	with open(currDir+"/output/item_item_results_unnormalized.txt", 'a+') as f:
		for user in userActionIndex:
			followsSorted = sorted(userActionIndex[user].items(),
				key=lambda x: x[1], reverse=True)
			followsSorted = followsSorted[:2]	#only look up to top 2 interested items, limits return to up to 2*^ per user
			if not len(followsSorted):
				countSkipped+=1
			#for each tuple, get 10 most similar items, and apply interest weight to get rating
			for tup in followsSorted:
				if not tup[0] in sharedKeywordsIndex:
					countSkipped+=1
					continue

				topMostSimilar = sharedKeywordsIndex[tup[0]]
				topWithRatings = applyUserActionWeights(topMostSimilar, tup[1])
				filterAlreadyFollowed(user, topWithRatings)

				#instead of merging and storing in memory, write results to disk
				if not len(topWithRatings):
					countSkipped+=1

				for item in topWithRatings:
					if topWithRatings[item] > 0:
						f.write(user+" "+item+" "+str(topWithRatings[item])+"\n")



	print "Finished writing, clearing up some memory..."
	#free memory!!
	sharedKeywordsIndex = None
	userActionIndex = None 	
	gc.collect()			

	print "Skipped %s items - no other items in same category or no actions"%(countSkipped)
	print "Current runtime: %s sec"%(time.time() - start)

	print "Normalizing data - Last step..."
	normalizeData(sc)

	print "Total runtime: %s sec"%(time.time() - start)
