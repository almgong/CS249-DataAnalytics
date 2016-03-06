from pyspark.mllib.fpm import FPGrowth #for frequent pattern mining
import gc
import time

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

resultIndex = {}	#{userID:{itemID:value}} //FINAL RESULTS!

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
		ret[tup[0]] = tup[1]*numActionsOnSimilarItem  #itemID:newValue

	return ret

def generateCandidatesWithWeights(sparkContext):
	'''Wrapper function for generating candidates'''

	#use of globals, need to explicitly state since we will None them later
	global categoryIndex 
	global keywordIndex
	global itemIndex
	global userActionIndex
	global sharedKeywordsIndex
	global resultIndex

	start = time.time()

	sc = sparkContext						#abbreviation for easier use
	'''itemFile = sc.textFile(itemLoc)			#open item file
	itemFile.foreach(mapLineToItemIndexes)	#preprocess..., count is a dummy action call''' #lazy eval sucks

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
			key=lambda x: x[1], reverse=True)[:10]

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

	#apply weights to sharedKeywordsIndex based on number of user actions (simply multiplying)
	for user in userActionIndex:
		followsSorted = sorted(userActionIndex[user].items(),
			key=lambda x: x[1], reverse=True)
		followsSorted = followsSorted[:10]	#only look up to top 10 interest items

		#for each tuple, get 10 most similar items, and apply interest weight to get rating
		for tup in followsSorted:
			tenMostSimilar = sharedKeywordsIndex[tup[0]]
			top10WithRatings = applyUserActionWeights(tenMostSimilar, tup[1])
			
			#now we have a "chunk" of 10 item recommendtions, merge with result
			#resultIndex = mergeDictionaries(resultIndex, top10WithRatings)

			with open(currDir+"/output/item_item_results.txt", 'a+') as f:
				for item in top10WithRatings:
					f.write(user+" "+item+" "+str(top10WithRatings[item])+"\n")



	#free memory!!
	sharedKeywordsIndex = None
	userActionIndex = None 	
	gc.collect()			

	print "Total runtime: %s sec"%(time.time() - start)
	return resultIndex
