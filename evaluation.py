# -*- coding: utf-8 -*-
"""
Created on Mon Mar 07 21:00:25 2016

@author: Xiuqi
"""

def precision_compute(stat, aver_precision):    
    aver_precision[1] += 1   # the total number of user
    sum = list(stat)
    for i in range(1, len(stat)):
        sum[i] = sum[i-1] + stat[i]
        
    top_prec = 0
    for i in range(0, len(stat)):
        if stat[i] != 0:
            top_prec += float(sum[i]) / (i+1)
                    
    if sum[-1] > 0:
        aver_precision[0] += top_prec / sum[-1]   # the sum of precision
        #print top_prec / sum[-1]
    
def readRecom(filename, user):
    with open(filename) as f:
        for line in f:
            curr = line.split()
            user[curr[0]] = {}
            user[curr[0]]['recommendation'] = curr[1:]   # top 3 recommendation
            user[curr[0]]['stat'] = [0] * len(curr[1:])
            # 0 if no click, 1 if click
                
                
                
def readSolution(filename, user):
    with open(filename) as f:
        for line in f:
            curr = line.split(',')
            click_item = curr[1].split()   # obtain the list of click item
            
            
            if curr[0] in user:
                for ele in set(user[curr[0]]['recommendation']) & set(click_item):
                    user[curr[0]]['stat'][user[curr[0]]['recommendation'].index(ele)] = 1
            
                    
                    
def stat_compute(user, aver_precision):
    for userId in user:
        precision_compute(user[userId]['stat'], aver_precision)
        

# filename1 is for recommendation file, filename2 is for solution file.
def evaluation(filename1, filename2):   # wrapped function \
    user = {}
    readRecom(filename1, user)
    readSolution(filename2, user)
    
    aver_precision = [0, 0]
    stat_compute(user, aver_precision)
    return aver_precision[0] / aver_precision[1]
        

print evaluation('recom.txt', 'solu.txt')