#!/usr/bin/env python

from operator import itemgetter
import sys

prev_word = None
doccount = 1 
word = None
df={}
l1=[]
# input comes from STDIN
for line in sys.stdin:
    line = line.strip()
    word,z= line.split('\t', 1)
    doc,count_total_c = z.split(' ',1)
    count,total_c=count_total_c.split(' ',1)
    total,c=total_c.split(' ',1)
    if prev_word == word:
        doccount = doccount+int(c)
    else:
        if prev_word != None:
            q=count+' '+total+' '+str(doccount)
            df[prev_word]=q
            j=prev_word+' '+doc
            l1.append(j)
        doccount=1
        prev_word = word

       
q=count+' '+total+' '+str(doccount)
df[prev_word]=q
j=prev_word+' '+doc
l1.append(j)
# h contains word & doc
for h in l1:
   word,doc=h.split(' ',1)
    # df[d] contains count, total per doc and number of docs word appears in
   for d in df:
       if word == d:
          print '%s\t%s' % (h,df[d])
