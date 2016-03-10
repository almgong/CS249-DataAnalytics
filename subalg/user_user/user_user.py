import time
import gc

### list of file locations ###
userLoc = "data/user_profile.txt"
snsLoc = "data/user_sns.txt"
keywordsLoc = "data/user_key_word.txt"
currDir = "subalg/user_user"

### list of toy file locations ###
# userLoc = "data/toy/user_profile.txt"
# snsLoc = "data/toy/user_sns.txt"
# keywordsLoc = "data/toy/user_key_word.txt"
# currDir = "subalg/user_user"


### Indexes used in logic, please dereference after usage! ###
userIndex = {} #{userID: {keywords:[], tagIds:[], follow:[list of itemIds]},... }
followedByIndex = {} #{userID1: [list of userIds that follows userID1] }
userSharedIndex = {}#{userID: {userID2: value, userID3: value,...},...}

### Mapping functions for preprocessing data ###
def mapLineToUserIndexesFromProfile(line):
	'''Given a line from user_profile.txt, store to appropriate indexes, depends on mapLineToUserIndexesFromSns to have been ran first'''
	
	line = line.split()
	userID = line[0]
	tags = line[4].split(";") #list of tags

	if userID in userIndex: # only look at users in user_sns
		if not tags[0] == '0':
			userIndex[userID]['tagIds'] = tags
			
		else:
			userIndex.pop(userID)

def mapLineToUserIndexesFromSns(line):
	'''Given a line from user_sns.txt, store to userIndex'''
	line = line.split()
	userID = line[0]
	follows = line[1]
	
	if userID in userIndex:
		userIndex[userID]['follows'].append(follows)
	else:
		userIndex[userID] = {
			'follows': [follows],
			'keywords': []
		}

	if follows in followedByIndex:
		followedByIndex[follows].append(userID)
	else:
		followedByIndex[follows] = [userID]

def mapLineToUserIndexesFromKeywords(line):
	'''Given a line from user_key_word.txt, store to userIndex, depends on mapLineToUserIndexesFromSns to have been ran first'''
	line = line.split()
	userID = line[0]
	
	if userID in userIndex:
		keywords = line[1].split(";") #[list of "keywordID:weights"]
		for kw in keywords:
			k = kw.split(":")[0]
			userIndex[userID]['keywords'].append(k)

def getNumOrPropShared(list1, list2, returnNum=False):
	'''
	Gets either the number of items shared between 2 lists, or the proportion, 
	using the first list as the reference list (e.g. proportion/num of items in
	list2 that is in list1).

	By Default returns the proportion, if you pass in getNumOrPropShared(x,y,True),
	it will instead just return the number shared between the 2 lists and NOT 
	the proportion shared.
	'''
	if len(list1) == 0: # avoid zero division
		return 0

	#ensure uniqueness in elements lists
	list1 = set(list1)
	list2 = set(list2)

	#variables 
	startingLength = len(list1)	#denominator for proportion
	numShared = startingLength - len(list1-list2)	#numerator

	if returnNum:
		return numShared

	return (numShared*1.0)/startingLength	#returns the proportion


def generateCandidatesWithWeights():
	global userIndex
	global followedByIndex
	global userSharedIndex
	
	start = time.time()
	
	#proactively open file and index what we need	
	print "Opening user_sns.txt file..."
	with open(snsLoc) as snsFile:
		for line in snsFile:
			mapLineToUserIndexesFromSns(line)
	print "Done with reading user_sns..."

	print "Opening user_profile.txt file..."
	with open(userLoc) as userFile:
		for line in userFile:
			mapLineToUserIndexesFromProfile(line)
	print "Done with reading user_profile..."

	print "Opening user_key_word.txt file..."
	with open(keywordsLoc) as keywordsFile:
		for line in keywordsFile:
			mapLineToUserIndexesFromKeywords(line)
	print "Done with reading user_key_word..."
	print "Done with reading file and generating indexes..."

	print "Length of UserIndex: " + str(len(userIndex))
	print "Length of followedByIndex: " + str(len(followedByIndex))
	# Length of UserIndex: 1892059
	# Length of followedByIndex: 920110


	print "Starting main logic to generate candidates..."
	print "Total runtime: %s sec"%(time.time() - start)

	#for each user, generate a list of candidate userIds to compute similarities
	count = 0
	for user in userIndex:
		if (count % 5) == 0:
			print "Processing number " + str(count)
			print "Already took %s secs"%(time.time()-start)
		count += 1
		
		userSharedIndex[user] = {}
		userSharedEntry = {} # {userID: {sharedTags: 1, sharedKeywords: 2, sharedFollows:5},...}
		tags = userIndex[user]['tagIds']
		follows = userIndex[user]['follows']
		keywords = userIndex[user]['keywords']

		# First, look at user that share same followers
		if not follows == []:
			for follow in follows:
				for userWithSameFollows in followedByIndex[follow]: #list of users that follows this item
					if userWithSameFollows not in userIndex:
						continue
					if user == userWithSameFollows:
						continue
					if userWithSameFollows in userSharedIndex[user]:
						continue
					
					if len(keywords) == 0:
						keywordValue = 0
					else:
						keywordValue = getNumOrPropShared(keywords,userIndex[userWithSameFollows]['keywords'])
					
					if len(tags) == 0:
						tagValue = 0
					else:
						tagValue = getNumOrPropShared(tags,userIndex[userWithSameFollows]['tagIds'])					
					
					if len(follows) == 0:
						followValue = 0
					else:
						followValue = getNumOrPropShared(follows,userIndex[userWithSameFollows]['follows'])
					value = .25*tagValue + .25*keywordValue + .5*followValue
					userSharedIndex[user][userWithSameFollows] = value
		
		# sort each resultant entry in userSharedIndex, limit to up to top 50 for memory efficiency
		userSharedIndex[user] =  sorted(userSharedIndex[user].items(),
				key=lambda x: x[1], reverse=True)[:20]

	# print userSharedIndex
	print "Finished Candidate Generation, took %s secs"%(time.time()-start)

	#Can now free some memory - recall global statement above
	followedByIndex = None
	gc.collect()

	# generate the items from similar users for frequent pattern mining
	print "Start writing to result file"
	with open(currDir+"/output/user_user_results.txt", 'w') as f:
		for user in userSharedIndex:
			itemList = {} 
			userFollowedItems = userIndex[user]['follows']
			for similarUser in userSharedIndex[user]:
				followedItems = userIndex[similarUser[0]]['follows'] # list of items followed by similar users
				# print "similarUser: " + similarUser[0] + " " + str(similarUser[1])
				for item in followedItems:
					if not item in userFollowedItems: # not already followed by the user
						if not item in itemList:
							itemList[item] = similarUser[1] # add up the value from userSharedIndex as support
						else:
							itemList[item] += similarUser[1]
			itemList = sorted(itemList.items(),
				key=lambda x: x[1], reverse=True)[:20]

			#write result to file
			for item in itemList:
				f.write(user+" "+item[0]+" "+str(item[1])+"\n")

	print "Finished writing to result file"
	#free memory
	userIndex = None
	userSharedIndex = None
	gc.collect()
	print "Total runtime: %s sec"%(time.time() - start)

# run
# generateCandidatesWithWeights()
