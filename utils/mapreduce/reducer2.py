#!/usr/bin/env python

from operator import itemgetter
import sys

current_word = None
prev_doc = None
current_count = 0
word = None
total=0
df={}
l1=[]


for line in sys.stdin:
    line = line.strip()
    l1.append(line)
    doc,word_count = line.split('\t', 1)
    word,count = word_count.split(' ', 1)
    count=int(count)
    if prev_doc == doc:
        # total number of words is the sum of each word count
        total=total+count
    else:
       if prev_doc != None:
            df[prev_doc]=total
       total=0
       prev_doc = doc
df[prev_doc]=total


for h in l1:
    doc,word_count = h.split('\t', 1)
    word,count = word_count.split(' ', 1) 
    for k in df:
        if doc == k:
           word_doc=word+' '+doc
           count_total=count+' '+str(df[k])
           print '%s\t%s' % (word_doc,count_total)
    
