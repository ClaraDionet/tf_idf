############################
#   Spark Implementation   #
############################

list_1 = []
list_2 = []
list_3 = []
list_4 = []
list_5 = []
list_6 = []
list_7 = []

import pyspark
import pyspark.sql.functions as F
import math
import re
import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()

def clean_characters(input_str):
    new_str = re.sub('[^a-zA-Z0-9 ]', '', input_str)
    return new_str.lower()

def create_tf_idf(input_dir, result_file):
    start = time.time()
    #instantiate text object
    text_things = spark.sparkContext.wholeTextFiles(input_dir)

    # output(RDD): (file_name, list_of_words_in_file)
    split_word = text_things.map(lambda x: (x[0], clean_characters(x[1]).split(' ')))

    # output(DF): (file_name, total_word_count_in_file (twc))
    total_words_df = split_word.map(lambda x: (x[0], len(x[1]))).toDF(['document', 'twc'])

    break_point0 = time.time()
    print('split up words', break_point0- start)
    list_1.append(break_point0- start)

    # output(int): total number of documents
    total_docs_count = total_words_df.count()
    break_point1 = time.time()
    print('doc count aggregate', break_point1 - break_point0)
    list_2.append(break_point1 - break_point0)

    # output(RDD): (k: (word, document), v: word_count_in_document (wc))
    word_all_count = split_word.flatMap(lambda x: ([((e,x[0]),1)for e in x[1]])).reduceByKey(lambda x,y: x+y)
    break_point2 = time.time()    
    print('word, doc aggregate', break_point2 - break_point1)
    list_3.append(break_point2 - break_point1)

    # output(DF): cols: word, document, word_count_per_document
    word_and_file_count_df = word_all_count.map(lambda x:(x[0][0], x[0][1], x[1])).toDF(['word', 'document', 'wc'])

    # output(DF): cols: word, number_of_docs_with_word (doc_count)
    document_word_count_df = word_all_count.map(lambda x: (x[0][0],1)).reduceByKey(lambda x,y:x+y).toDF(['word', 'doc_count'])
    
    break_point3 = time.time()    
    print('word aggregate', break_point3 - break_point2)
    list_4.append(break_point3 - break_point2)
    
    # output(DF): cols: document, word, word_count_per_document, total_word_count_in_doc
    tf_df = word_and_file_count_df.join(total_words_df,'document','inner')
    break_point4 = time.time()
    print('document join', break_point4 - break_point3)
    list_5.append(break_point4 - break_point3)

    # add column tf to tf_df
    # output(DF): cols: document, word, word_count_per_document, total_word_count_in_doc, tf
    tf_df_result_df = tf_df.withColumn('tf', F.col("wc")/F.col("twc"))

    # output(DF): cols: word, document, word_count_per_document (wc), total_word_count_in_doc (twc), tf, number_of_docs_with_word (doc_count)
    idf_df = tf_df_result_df.join(document_word_count_df, 'word', 'inner')
    break_point5 = time.time()
    print('word join', break_point5 - break_point4)
    list_6.append(break_point5 - break_point4)

    #add column idf
    # output(DF):  word, document, wc, twc, tf, doc_count, idf
    idf_df_result_df = idf_df.withColumn('idf',F.log(total_docs_count/F.col('doc_count')))

    # add column tf_idf
    # output (DF): word, document, wc, twc, tf, doc_count, idf, tf_idf
    tf_idf_result_df = idf_df_result_df.withColumn('tf_idf', F.col('tf')*F.col('idf'))
    break_point6 = time.time()
    print('finish', break_point6 - break_point5)
    list_7.append( break_point6 - break_point5)
    
    #print time results to the console to look at them
    print('*%*%*%'*100)
    print(list_1)
    print(list_2)
    print(list_3)
    print(list_4)
    print(list_5)
    print(list_6)
    print(list_7)
    print('*%*%*%'*100)
    print(tf_idf_result_df.show())
    
    spark.stop()
    return tf_idf_result_df


if __name__ == "__main__":

    final_result1 = create_tf_idf('hdfs:/user/spark/wc/input/file_40mb'
    ,'hdfs:/user/spark/wc/input/file_40mb/output_file.csv')

