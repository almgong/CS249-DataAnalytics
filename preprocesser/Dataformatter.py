'''
Module that exposes functions to retrieve and format data for use in 
preprocessing.
'''

def readInUserProfile(filename, arr):
	'''
	Reads the input file creates a user record in the input associative array
	'''

	with open(filename) as f:
		for line in f:
			curr = line.split()	#split by all whitespace
			arr[curr[0]] = {'user_data':curr[1:], 'follows':[]}


def readInItem(filename, arr):
	'''Must be called AFTER user data read in, assumes every item is a user'''

	with open(filename) as f:
		for line in f:
			curr = line.split()
			arr[curr[0]]['item_data'] = curr[1:]


def readInSNS(filename, arr):
	'''
	Reads into the array info on which user follows which user(s), depends on
	readInUserProfile() to have been ran first
	'''

	with open(filename) as f:
		for line in f:
			curr = line.split()
			arr[curr[0]]['follows'].append(curr[1])



def generateUserDict(userFileLoc, itemFileLoc, snsLoc):
	'''Generates and returns a data structure to hold user/item information'''

	arr = {}
	readInUserProfile(userFileLoc, arr)
	readInItem(itemFileLoc, arr)
	readInSNS(snsLoc, arr)
	return arr
