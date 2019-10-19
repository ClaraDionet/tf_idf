#!/usr/bin/env python

import sys
import os


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    word_doc,count_total=line.split('\t',1)
    word,doc=word_doc.split(' ',1)
    z=doc+' '+count_total+' '+str(1)
    print '%s\t%s' % (word,z)

        
