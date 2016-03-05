''''
Module that will compute a list of user-most likely to follow
items, considering similarity items followed by the user.
'''

### list of file locations ###
itemLoc = 'data/item.txt'

### Indexes used in logic, please dereference after usage! ###
categoryIndex = {} 	#{3-level-cat: [list of item ids], ... }
keywordIndex = {}	#{keywordID:[list of item ids], ...}
itemIndex = {}		#{itemID: {keywords:[], category:'x.x.x'},... }

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


### Actual Logic ###

def generateCandidatesWithWeights(sparkContext):
	'''Wrapper function for generating candidates'''

	'''sc = sparkContext						#abbreviation for easier use
	itemFile = sc.textFile(itemLoc)			#open item file
	itemFile.foreach(mapLineToItemIndexes)	#preprocess..., count is a dummy action call''' #lazy eval sucks

	#proactive open file and index what we need
	print "Open file..."
	with open(itemLoc) as itemFile:
		for line in itemFile:
			mapLineToItemIndexes(line)

	print "Done with reading file and generating indexes"

	

	print len(itemIndex)
	#return keywordIndex

