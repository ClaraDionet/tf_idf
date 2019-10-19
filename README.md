## Description

This project compares two implementations of the [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) (Term Frequency Inverse Document Frequency) metric calculation: the first using Map-reduce written in python and the Second using Spark-python. We first explain what the TF-IDF metric is, and cover in detail how each implementation works. We also run experiments on how efficient each implementation is for different document sizes. 

The full written report is found [here](https://github.com/raeray/tf_idf/blob/master/TF-IDF%20Analysis.pdf)

## Project Content

[TF-IDF Analysis.pdf](tf_idf/TF-IDF%20Analysis.pdf): Full written report    
[utils/input/doc_generator.py](utils/input/doc_generator.py): Python program that generates word documents of various sizes for input into tf-idf calculations  
[utils/mapreduce/commands_TFIDF.sh](tf_idf/utils/mapreduce/commands_TFIDF.sh): Shell script to execute Hadoop code    
[utils/mapreduce/Mapper1.py](tf_idf/utils/mapreduce/mapper1.py), [utils/mapreduce/Reducer1.py](tf_idf/utils/mapreduce/reducer1.py): Step 1 of Hadoop implementation: computes word count/ word-doc  
[utils/mapreduce/Mapper2.py](tf_idf/utils/mapreduce/mapper2.py), [utils/mapreduce/Reducer2.py](tf_idf/utils/mapreduce/reducer2.py): Step 2 of Hadoop implementation: computes total word count / doc   
[utils/mapreduce/Mapper3.py](tf_idf/utils/mapreduce/mapper3.py), [utils/mapreduce/Reducer3.py](tf_idf/utils/mapreduce/reducer3.py): Step 3 of Hadoop implementation: computes doc count / word  
[utils/mapreduce/Mapper4.py](tf_idf/utils/mapreduce/mapper4.py): Step 4 of Hadoop implementation: computes TF-IDF / word-doc  
[Spark.py](tf_idf/utils/spark/spark.py): Main Spark implementation code  
[Spark_bc_accum.py](tf_idf/utils/spark/spark_bc_accum.py): Spark implementation with Broadcast and accumulator variables  
[Spark_best_implementation.py](tf_idf/utils/spark/spark_best_implementation.py): Optimised implementation of Spark code   

## Authors

[Mirae Kim](https://github.com/raeray)  
[Clara Dionet](https://github.com/ClaraDionet) 

