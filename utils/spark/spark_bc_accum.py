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

    def file_num(x): 
        global total_docs_count_accum
        total_docs_count_accum.add(1)

    # output(RDD): (file_name, list_of_words_in_file)
    split_word = text_things.map(lambda x: (x[0], clean_characters(x[1]).split(' ')))
    split_word.foreach(file_num) #count the num of docs

    # dictionary witih {doc, total_word_in_doc}
    total_words_dict = dict(split_word.map(lambda x: (x[0], len(x[1]))).collect()) #create a broadcast variable
    bc = spark.sparkContext.broadcast(total_words_dict)


    # output(RDD): (k: (word, document), v: word_count_in_document (wc))
    word_all_count = split_word.flatMap(lambda x: ([((e,x[0]),1)for e in x[1]])).reduceByKey(lambda x,y: x+y)

    #output(dict): {word: doc_count}
    document_word_count_dict = dict(word_all_count.map(lambda x: (x[0][0],1)).reduceByKey(lambda x,y:x+y).collect())
    document_word_count_bc = spark.sparkContext.broadcast(document_word_count_dict)

    # output(DF): cols: word, document, count (wc), word_count_per_document (twc), doc_count
    word_and_file_count_df = word_all_count.map(lambda x:(x[0][0], x[0][1], x[1], bc.value[x[0][1]], document_word_count_bc.value[x[0][0]])).toDF(['word', 'document', 'wc', 'twc', 'doc_count'])

    #calculate final tf_idf values
    tf_idf_result_df = word_and_file_count_df.withColumn('tf', F.col("wc")/F.col("twc"))
    tf_idf_result_df = tf_idf_result_df.withColumn('idf',F.log(total_docs_count_accum.value/F.col('doc_count')))
    tf_idf_result_df = tf_idf_result_df.withColumn('tf_idf', F.col('tf')*F.col('idf'))


    print('finish', break_point6 - start)
    
    tf_idf_result_df.show()
    
    tf_idf_result_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").csv(result_file)

    
    spark.stop()
    return tf_idf_result_df


if __name__ == "__main__":

    final_result1 = create_tf_idf('hdfs:/user/spark/wc/input/file_05mb'
    ,'hdfs:/user/spark/wc/input/file_05mb/output_file_bc.csv')
    


