'''
Exercise 05 - Develop word count program

Details - Duration 20 minutes
Data is available in HDFS /public/randomtextwriter
Get word count for the input data using space as delimiter (for each word, we need to get how many types it is repeated in the entire input data set)
Number of executors should be 10
Executor memory should be 3 GB
Executor cores should be 20 in total (2 per executor)
Number of output files should be 8
Avro dependency details: groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
Target Directory: /user/<YOUR_USER_ID>/solutions/solution05/wordcount
Target File Format: Avro
Target fields: word, count
Compression: N/A or default
Validation
Solution

sequenceFile


pyspark --master yarn \
	--conf spark.ui.port=12459 \
	--num-executors 3 \
	--executor-memory 1GB \
	--executor-cores 2 \
	--packages com.databricks:spark-avro_2.10:2.0.1

pyspark --master yarn \
	--conf spark.ui.port=12459 \
	--packages com.databricks:spark-avro_2.10:2.0.1

'''

seqData=sc.sequenceFile("/public/randomtextwriter/part-m-00000")

from operator import add

lines = seqData.map(lambda r: r[0]+" "+r[1])

counts = lines. \
flatMap(lambda line: line.split(" ")). \
filter(lambda word :word!=''). \
map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)




