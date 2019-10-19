#!/bin/sh

T0=`date +%s` 

##Command to execute first map-reduce job
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/hadoop/wc/input \
-output /user/hadoop/wc/output_interim \
-file /home/hadoop/mapper1.py \
-mapper /home/hadoop/mapper1.py \
-file /home/hadoop/reducer1.py \
-reducer /home/hadoop/reducer1.py

T1=`date +%s`

##Command to execute second map-reduce job
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/hadoop/wc/output_interim \
-output /user/hadoop/wc/output_interim2 \
-file /home/hadoop/mapper2.py \
-mapper /home/hadoop/mapper2.py \
-file /home/hadoop/reducer2.py \
-reducer /home/hadoop/reducer2.py

T2=`date +%s`

##Command to execute third map-reduce job
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/hadoop/wc/output_interim2 \
-output /user/hadoop/wc/output_interim3 \
-file /home/hadoop/mapper3.py \
-mapper /home/hadoop/mapper3.py \
-file /home/hadoop/reducer3.py \
-reducer /home/hadoop/reducer3.py

T3=`date +%s`

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-numReduceTasks 0 \
-input /user/hadoop/wc/output_interim3 \
-output /user/hadoop/wc/output \
-file /home/hadoop/mapper4.py \
-mapper /home/hadoop/mapper4.py

T4=`date +%s`

echo $T0
echo $T1
echo $T2
echo $T3
echo $T4