'''
Module that exposes functions to retrieve and format data for use in 
preprocessing.
'''

def readInUserProfile(filename, arr, uniqueTags):
	'''
	Reads the input file creates a user record in the input associative array
	'''

	with open(filename) as f:
		for line in f:
			curr = line.split()	#split by all whitespace
			arr[curr[0]] = {
				'birthYear': curr[1], 
				'gender':curr[2], 
				'numTweets':curr[3],
				'tagIds': curr[4],
				'follows':[]
			}

			tags = curr[4].split(";")
			for tag in tags:
				uniqueTags[tag] = 0


def readInItem(filename, arr, uniqueKW):
	'''Must be called AFTER user data read in, assumes every item is a user'''

	with open(filename) as f:
		for line in f:
			curr = line.split()
			arr[curr[0]]['categories'] = curr[1]
			arr[curr[0]]['itemKeywords'] = curr[2]
			kws = curr[2].split(";")
			for kw in kws:
				uniqueKW[kw] = 0


def readInSNS(filename, arr):
	'''
	Reads into the array info on which user follows which user(s), depends on
	readInUserProfile() to have been ran first
	'''

	with open(filename) as f:
		for line in f:
			curr = line.split()
			arr[curr[0]]['follows'].append(curr[1])


def readInUserKeyword(filename, arr, uniqueKW):
	'''Reads User keyword data, depends on readInUserProfile()'''

	with open(filename) as f:
		for line in f:
			curr = line.split()
			
			kwWeightPairs = {}
			kws = curr[1].split(";")
			for kw in kws:
				k = kw.split(":")[0]
				uniqueKW[k] = 0
				kwWeightPairs[k] = kw.split(":")[1]

			arr[curr[0]]['keywords'] = kwWeightPairs

def parseKDDData(userDict, userFV, userFileLoc, itemFileLoc, snsLoc, kwLoc):
	'''Generates and returns a data structure to hold user/item information'''

	uniqueKW = {}	#repr all keywords possible from user_key_word and item
	uniqueTags = {} #repr all unique tags possible profile

	### logic to read in data from file to structure
	readInUserProfile(userFileLoc, userDict, uniqueTags)
	readInItem(itemFileLoc, userDict, uniqueKW)
	readInSNS(snsLoc, userDict)
	readInUserKeyword(kwLoc, userDict, uniqueKW)

	### creating feature vectors for each user
	sortedKWVector = uniqueKW.keys().sort()
	sKWVLen = len(sortedKWVector)

	sortedTagVector = uniqueTags.keys().sort()

	countSkipped = 0
	for user in userDict:
		if (not 'keywords' in userDict[user] && 
			not 'itemKeywords' in userDict[user] %%
			not userDict[user]['tagIds']=='0'):

			countSkipped+=1
			continue #skip because not enough information

		userFV[user] = [] 
		keysVisited = 0 	#number of keys visited in the sortedKWVector

		#generate keyword part of FV
		for uKW in sortedKWVector:
			currKeywords = userDict[user]['keywords']
			if 'itemKeywords' in userDict[user]:
				 
			visitsLeft = len(currKeywords)				#max number of visits/checks we need on user keyword
			keysVisited+=1			

			if uKW in currKeywords: 	#if the user has this keyword
				userFV[user].append(currKeywords[uKW])
				visitsLeft-=1
			else:
				userFV[user].append(0)




			### early termination of current user if all possible keywords marked
			if visitsLeft == 0:
				filler = [0 for i in range(sKWVLen - keysVisited)]
				userFV[user] + filler
				break



			


	print len(uniqueKW)
