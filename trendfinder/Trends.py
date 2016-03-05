'''Deprecated, no longer used'''

'''
Module that generates trends and statistics based on input (format of input)
based on the return(s) of Dataformatter.py functions.
'''

referenceYear = 2012 #year of KDD cup challenge

def numFollowersBasedOnBirthYear(data):	#should probably do more than counting only number of followers, to be more efficient
	ageRanges = { #interpreted as "10 and under(0-10)", "20 and under(10-20)" ...
		10:{'count':0, 'numUsers':0}, 
		20:{'count':0, 'numUsers':0},
		30:{'count':0, 'numUsers':0},
		-1:{'count':0, 'numUsers':0}
	}
	invalidCount = 0
	for user in data:
		try:
			userAge = referenceYear - int(data[user]['user_data'][0])
		except:
			invalidCount+=1
			continue

		numFollowsForUser = len(data[user]['follows'])
		if(userAge < 10):
			ageRanges[10]['numUsers']+=1
			ageRanges[10]['count']+=numFollowsForUser
		elif userAge < 20:
			ageRanges[20]['numUsers']+=1
			ageRanges[20]['count']+=numFollowsForUser
		elif userAge < 30:
			ageRanges[30]['numUsers']+=1
			ageRanges[30]['count']+=numFollowsForUser
		else:
			ageRanges[-1]['numUsers']+=1
			ageRanges[-1]['count']+=numFollowsForUser

	print "could not count: " + str(invalidCount)

	print "result: " 
	print ageRanges
	print
	print "Average follows - Age (0-10): ", ageRanges[10]['count']*1.0/ageRanges[10]['numUsers']
	print "Average follows - Age (10-20): ", ageRanges[20]['count']*1.0/ageRanges[20]['numUsers'] 
	print "Average follows - Age (20-30): ", ageRanges[30]['count']*1.0/ageRanges[30]['numUsers'] 
	print "Average follows - Age (30+): ", ageRanges[-1]['count']*1.0/ageRanges[-1]['numUsers']  

def numDistinctTagIds(data):
	'''Return num of distinct tag ids in user profile'''

	temp = {}
	maximum = -1
	for user in data:
		taglist = data[user]['user_data'][-1].split(";")
		if(len(taglist) > 1):
			for tag in taglist:
				if(int(tag)> maximum):
					maximum = int(tag)
				temp[tag] = 0

	print "num ", len(temp)
	return maximum

def numDistinctKeywords(data):
	'''return num of distinct keywords for users'''
	temp = {}
	for user in data:
		keywords = data[user]['keywords'].split(";")
		for kw in keywords:
			curr = kw.split(':')[0] #keyword without weight
			temp[curr] = 0

	return len(temp)

