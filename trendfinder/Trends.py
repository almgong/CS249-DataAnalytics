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

	