############################
#   Spark Implementation   #
############################



import pyspark
import pyspark.sql.functions as F
import math
import re
import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
total_docs_count_accum = spark.sparkContext.accumulator(0)

def clean_characters(input_str):
    new_str = re.sub('[^a-zA-Z0-9 ]', '', input_str)
    return new_str.lower()

def create_tf_idf(input_dir, result_file):
    start = time.time()
    #instantiate text object
    text_things = spark.sparkContext.wholeTextFiles(input_dir)

    # output(RDD): (file_name, list_of_words_in_file)
    split_word = text_things.map(lambda x: (x[0], clean_characters(x[1]).split(' ')))

    # create a broadcast variable 
    total_words_dict = dict(split_word.map(lambda x: (x[0], len(x[1]))).collect()) 
    bc = spark.sparkContext.broadcast(total_words_dict)

    total_docs_count = len(bc.value)

    # output(RDD): (k: (word, document), v: word_count_in_document (wc))
    word_all_count = split_word.flatMap(lambda x: ([((e,x[0]),1)for e in x[1]])).reduceByKey(lambda x,y: x+y)
    
    # output(DF): cols: word, document, word_count_per_document
    word_and_file_count = word_all_count.map(lambda x:(x[0][0], (x[0][1], x[1])))


    # RDD JOIN output(DF): cols: word, number_of_docs_with_word (doc_count)
    document_word_count = word_all_count.map(lambda x: (x[0][0],1)).reduceByKey(lambda x,y:x+y)
    word_join = word_and_file_count.join(document_word_count)
  
    # BROADCAST VARIABLE output(: cols: word, number_of_docs_with_word (doc_count)
    # document_word_count_dict = dict(word_all_count.map(lambda x: (x[0][0],1)).reduceByKey(lambda x,y:x+y).collect())
    # document_word_count_bc = spark.sparkContext.broadcast(document_word_count_dict)

    
    #output: doc, word, word_count, doc_count_per_word, total_word_per_doc
    tf_df = word_join.map(lambda x: (x[1][0][0], x[0], x[1][0][1], x[1][1], bc.value[x[1][0][0]]))
    tf_df_result = tf_df.map(lambda x: (x[0], x[1], (x[2]/x[4])*math.log(total_docs_count/x[3])))



    spark.stop()
    return tf_idf_result_df


if __name__ == "__main__":

    final_result1 = create_tf_idf('hdfs:/user/spark/wc/input/file_05mb'
    ,'hdfs:/user/spark/wc/input/file_05mb/output_file_bc.csv')



