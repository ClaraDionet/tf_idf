#!/usr/bin/env python

import sys
import os
from math import log10,sqrt

# total number of documents
D=5.0
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    word_doc,count_total_doccount=line.split('\t',1)
    count,total,doccount=count_total_doccount.split(' ',2)
    count=float(count)
    total=float(total)
    doccount=float(doccount)
    tfidf= (count/total)*log10(D/doccount)
    print '%s\t%s' % (word_doc,tfidf)

        
