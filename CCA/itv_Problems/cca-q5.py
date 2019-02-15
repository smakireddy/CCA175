'''
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
'''

pyspark --master yarn \
--conf 

files = sc.sequenceFile("/public/randomtextwriter")

lines = files.map(lambda r: r[0]+" "+r[1])

words = lines.flatMap(lambda line: line.split(" ")). \
filter(lambda word: word <> ''). \
map(lambda word: (str(word), 1))

from operator import add

wordCountDF = words.reduceByKey(add).toDF(["word","count"])

wordCountDF.coalesce(8).write. \
format("com.databricks.spark.avro").save("/user/smakired/practice-1/q5/wordcount")


http://rm01.itversity.com:19288/proxy/application_1540458187951_8294/


#validation 

avroFiles = sqlContext.read.format("com.databricks.spark.avro").load("/user/smakired/practice-1/q5/wordcount")

avroFile.show() 

avroFile.count()