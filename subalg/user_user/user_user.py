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
keywordIndex = {} #{keyword: [list of userIds],...}
tagIndex = {} #{tagId: [list of userIds],...}
followedByIndex = {} #{userID1: [list of userIds that follows userID1] }
userSharedIndex = {}#{userID: {userID2: value, userID3: value,...},...}

### Mapping functions for preprocessing data ###
def mapLineToUserIndexes(line):
	'''Given a line from user_profile.txt, store to appropriate indexes'''
	
	line = line.split()
	userID = line[0]
	tags = line[4].split(";") #list of tags

	userIndex[userID] = {
		'tagIds': tags,
		'follows': [],
		'keywords': []
	}
	#add entries to tag index
	if tags != '0': 
		for tag in tags:
			if tag in tagIndex:
				tagIndex[tag].append(userID)
			else:
				tagIndex[tag] = [userID]

def mapLineToUserIndexesFromSns(line):
	'''Given a line from user_sns.txt, store to userIndex, depends on mapLineToUserIndexes to have been ran first'''
	line = line.split()
	userID = line[0]
	follows = line[1]
	userIndex[userID]['follows'].append(follows)
	if follows in followedByIndex:
		followedByIndex[follows].append(userID)
	else:
		followedByIndex[follows] = [userID]

def mapLineToUserIndexesFromKeywords(line):
	'''Given a line from user_key_word.txt, store to userIndex, depends on mapLineToUserIndexes to have been ran first'''
	line = line.split()
	userID = line[0]
	keywords = line[1].split(";") #[list of "keywordID:weights"]
	for kw in keywords:
		k = kw.split(":")[0]
		if not keywordIndex.has_key(k):
			keywordIndex[k] = []
		userIndex[userID]['keywords'].append(k)
		keywordIndex[k].append(userID)


def generateCandidatesWithWeights():
	global userIndex
	global keywordIndex
	global tagIndex
	global followedByIndex
	global userSharedIndex
	
	start = time.time()
	
	#proactively open file and index what we need
	print "Opening user_profile.txt file..."
	with open(userLoc) as userFile:
		for line in userFile:
			mapLineToUserIndexes(line)
	print "Done with reading user_profile..."
	
	print "Opening user_sns.txt file..."
	with open(snsLoc) as snsFile:
		for line in snsFile:
			mapLineToUserIndexesFromSns(line)
	print "Done with reading user_sns..."

	print "Opening user_key_word.txt file..."
	with open(keywordsLoc) as keywordsFile:
		for line in keywordsFile:
			mapLineToUserIndexesFromKeywords(line)
	print "Done with reading user_key_word..."
	print "Done with reading file and generating indexes..."

	# print userIndex
	print "Starting main logic to generate candidates..."

	#for each user, generate a list of candidate userIds to compute similarities
	count = 0
	for user in userIndex:
		if (count % 100000) == 0:
			print "Processing number " + str(count)
			print "Already took %s secs"%(time.time()-start)
		count += 1
		userSharedIndex[user] = {}
		userSharedEntry = {} # {userID: {sharedTags: 1, sharedKeywords: 2, sharedFollows:5},...}
		tags = userIndex[user]['tagIds']
		follows = userIndex[user]['follows']
		keywords = userIndex[user]['keywords']
		
		# look at users that share same tags
		if not tags[0] == '0':
			for tag in tags:
				for userWithSameTag in tagIndex[tag]: #list of users that share this tag
					if user == userWithSameTag:
						continue
					if not userWithSameTag in userSharedEntry:
						userSharedEntry[userWithSameTag] = {
							'sharedTags': 1,
							'sharedKeywords': 0,
							'sharedFollows': 0
						}
						# userSharedEntry[userWithSameTag]['sharedTags'] = 1
						# userSharedEntry[userWithSameTag]['sharedKeywords'] = 0
						# userSharedEntry[userWithSameTag]['sharedFollows'] = 0
					else:	
						userSharedEntry[userWithSameTag]['sharedTags'] += 1

		# look at user that share same keywords
		if not keywords == []:
			for keyword in keywords:
				for userWithSameKeyword in keywordIndex[keyword]: #list of users that share this keyword
					if user == userWithSameKeyword:
						continue
					if not userWithSameKeyword in userSharedEntry:
						userSharedEntry[userWithSameKeyword] = {
							'sharedTags': 0, #ensure no shared tags
							'sharedKeywords': 1,
							'sharedFollows': 0
						}
						# userSharedEntry[userWithSameKeyword]['sharedTags'] = 0 #ensure no shared tags
						# userSharedEntry[userWithSameKeyword]['sharedKeywords'] = 1
						# userSharedEntry[userWithSameKeyword]['sharedFollows'] = 0
					else:
						userSharedEntry[userWithSameKeyword]['sharedKeywords'] += 1

		# look at user that share same followers
		if not follows == []:
			for follow in follows:
				for userWithSameFollows in followedByIndex[follow]: #list of users that follows this item
					if user == userWithSameFollows:
						continue
					if not userWithSameFollows in userSharedEntry:
						userSharedEntry[userWithSameFollows] = {
							'sharedTags': 0, #ensure no shared tags
							'sharedKeywords': 0, #ensure no shared keywords
							'sharedFollows': 1
						}
						# userSharedEntry[userWithSameFollows]['sharedTags'] = 0 #ensure no shared tags
						# userSharedEntry[userWithSameFollows]['sharedKeywords'] = 0 #ensure no shared keywords
						# userSharedEntry[userWithSameFollows]['sharedFollows'] = 1
					else:
						userSharedEntry[userWithSameFollows]['sharedFollows'] += 1

		# compute the value for userSharedIndex
		for candidateUser in userSharedEntry:
			if not len(tags) == 0:
				tagValue = (userSharedEntry[candidateUser]['sharedTags']*1.0)/len(tags)
			else:
				tagValue = 0
			
			if not len(keywords) == 0:
				keywordValue = (userSharedEntry[candidateUser]['sharedKeywords']*1.0)/len(keywords)
			else:
				keywordValue = 0
			
			if not len(follows) == 0:
				followValue = (userSharedEntry[candidateUser]['sharedFollows']*1.0)/len(follows)
			else:
				followValue = 0
			value = .25*tagValue + .25*keywordValue + .5*followValue

			userSharedIndex[user][candidateUser] = value
			# userSharedIndex[user] = {
			# 	'candidateUser': value
			# }
		
		# sort each resultant entry in userSharedIndex, limit to up to top 50 for memory efficiency
		userSharedIndex[user] =  sorted(userSharedIndex[user].items(),
				key=lambda x: x[1], reverse=True)[:50]

	# print "userSharedIndex: " + str(userSharedIndex)
	print "Finished Candidate Generation, took %s secs"%(time.time()-start)

	#Can now free some memory - recall global statement above
	tagIndex = None
	keywordIndex = None
	followedByIndex = None
	gc.collect()

	# generate the items from similar users for frequent pattern mining
	
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
			key=lambda x: x[1], reverse=True)[:50]

		#write result to file
		with open(currDir+"/output/user_user_results.txt", 'a+') as f:
			f.write(user)
			for item in itemList:
				f.write(" "+item[0])
			f.write("\n")

	#free memory
	userIndex = None
	userSharedIndex = None
	gc.collect()
	print "Total runtime: %s sec"%(time.time() - start)

# run
# generateCandidatesWithWeights()
