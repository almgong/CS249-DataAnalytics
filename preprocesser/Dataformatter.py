from pyspark.mllib.linalg import SparseVector

import time

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

	start = time.time()

	uniqueKW = {}	#repr all keywords possible from user_key_word and item
	uniqueTags = {} #repr all unique tags possible from user profile

	### logic to read in data from file to structure
	readInUserProfile(userFileLoc, userDict, uniqueTags)
	readInItem(itemFileLoc, userDict, uniqueKW)
	readInSNS(snsLoc, userDict)
	readInUserKeyword(kwLoc, userDict, uniqueKW)

	now = time.time()
	print "Done with reading and creating Data structure. Took %s secs"%(now-start)

	initFVforUsers(userDict, userFV, uniqueKW, uniqueTags)	#generate FVs

	now = time.time()
	print "Created FV, took: %s seconds"%(now-start)


	print "Finished job!!"

def initFVforUsers(userDict, userFV, uniqueKW, uniqueTags):
	'''Logic to create feature vectos for each user, stored in userFV param'''

	### creating feature vectors for each user
	sortedKWVector = uniqueKW.keys()
	sortedKWVector.sort()
	sKWVLen = len(sortedKWVector)		#length of keywords (num unique)
	uKeywords = arrayToIndexedDict(sortedKWVector)	#convert arr to appropriate dictionary
	sortedKWVector = None				#no longer needed, null to GC()

	sortedTagVector = uniqueTags.keys()
	sortedTagVector.sort()
	sTVLen = len(sortedTagVector)		#length of tag ids (num unique)
	uTags = arrayToIndexedDict(sortedTagVector)
	sortedTagVector = None				#no longer needed, remove reference

	fvSize = sKWVLen + sTVLen 			#total size of each feature vector

	countSkipped = 0
	for user in userDict:
		#only consider users with either keywords or tags (ignore ones with neither)
		if ((not 'keywords' in userDict[user]) and (not 'itemKeywords' in userDict[user]) and (not userDict[user]['tagIds']=='0')):

			countSkipped+=1
			continue #skip because not enough information

		nonzeroElesInFV = {}	#holds indices of nonzero values with their values
		
		#collect all keywords of user, set to currKeywords
		currKeywords = userDict[user]['keywords']
		if 'itemKeywords' in userDict[user]:
				 iKWs = userDict[user]['itemKeywords'].split(';')
				 for iKW in iKWs:
				 	if not iKW in currKeywords:
				 		currKeywords[iKW] = 1

		visitsLeft = len(currKeywords) #max number of visits/checks we need on user keyword

		#generate keyword part of FV
		for kw in currKeywords:
			index = uKeywords[kw]	#get correct index location for this keyword
			nonzeroElesInFV[index] = currKeywords[kw]	#{index:weight,...}

		#generate tag part of FV
		currTags = userDict[user]['tagIds'].split(';')
		offset = sKWVLen	#offset of indices for tags, since tag portion appears after kw
		for tag in currTags:
			if(currTags[0]=='0'):
				break	#no tags for this user
			
			index = uTags[tag] + offset	#apply offset
			nonzeroElesInFV[index] = 1



		#construct and set the sparse vector as fv
		userFV[user] = SparseVector(fvSize, nonzeroElesInFV)



#### HElPER FUNCTIONS ####
def arrayToIndexedDict(arr):
	'''converts an array to a dictionary with array value as key and index as value'''

	index = 0
	result = {}
	for ele in arr:
		result[ele] = index
		index+=1

	return result
