with open("CS249_DataAnalytics/subalg/user_item/prediction.txt") as f:
	with open("CS249_DataAnalytics/subalg/user_item/newFormatedPred.txt", 'w') as wf:
		for line in f:
			line = line.strip('(')
			line = line.split(',')
			wf.write(line[0] + line[1] + line[2].split(')')[0] + "\n")
