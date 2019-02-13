'''
Instructions
Get word count for the input data using space as delimiter (for each word, we need to get how many times it is repeated in the entire input data set)

Data Description
Data is available in HDFS /public/randomtextwriter

word count data information:

Number of executors should be 10
executor memory should be 3 GB
Executor cores should be 20 in total (2 per executor)
Number of output files should be 8
Avro dependency details: groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
Output Requirements
Output File format: Avro
Output fields: word, count
Compression: Uncompressed
Place the customer files in the HDFS directory
/user/`whoami`/problem5/solution/
Replace `whoami` with your OS user name
End of Problem
'''

pyspark --master yarn \
--conf spark.ui.port=14563 \
--total-executor-cores 20 \
--executor-memory 3GB \
--num-executors 10 \
--packages com.databricks:spark-avro_2.10:2.0.1

files  = sc.sequenceFile("/public/randomtextwriter")

filesRDD = files.map(lambda r: r[0]+' '+r[1])

from operator import add 

words = filesRDD.flatMap(lambda w: w.split(" ")).map(lambda word: (word, 1)).reduceByKey(add)

from pyspark.sql import Row

wordsDF = words.map(lambda r : Row(word = r[0],count = r[1])).toDF()

wordsDF.coalesce(8).write.format("com.databricks.spark.avro").save("/user/smakired/problem5/solution/")


#validation 

wordLoad = sqlContext.read.format("com.databricks.spark.avro").load("/user/smakired/problem5/solution/")

