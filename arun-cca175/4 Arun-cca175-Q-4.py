'''
1. Import orders table from mysql as text file to the destination /user/smakired/arun-cca175/problem5/text. Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n"). 
2. Import orders table from mysql  into hdfs to the destination /user/smakired/arun-cca175/problem5/avro. File should be stored as avro file.
3. Import orders table from mysql  into hdfs  to folders /user/smakired/arun-cca175/problem5/parquet. File should be stored as parquet file.
4. Transform/Convert data-files at /user/smakired/arun-cca175/problem5/avro and store the converted file at the following locations and file formats
 ->save the data to hdfs using snappy compression as parquet file at /user/smakired/arun-cca175/problem5/parquet-snappy-compress
 ->save the data to hdfs using gzip compression as text file at /user/smakired/arun-cca175/problem5/text-gzip-compress
 ->save the data to hdfs using no compression as sequence file at /user/smakired/arun-cca175/problem5/sequence
 ->save the data to hdfs using snappy compression as text file at /user/smakired/arun-cca175/problem5/text-snappy-compress
5. Transform/Convert data-files at /user/smakired/arun-cca175/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
 ->save the data to hdfs using no compression as parquet file at /user/smakired/arun-cca175/problem5/parquet-no-compress
 ->save the data to hdfs using snappy compression as avro file at /user/smakired/arun-cca175/problem5/avro-snappy
6.Transform/Convert data-files at /user/smakired/arun-cca175/problem5/avro-snappy and store the converted file at the following locations and file formats
 ->save the data to hdfs using no compression as json file at /user/smakired/arun-cca175/problem5/json-no-compress
 ->save the data to hdfs using gzip compression as json file at /user/smakired/arun-cca175/problem5/json-gzip
7.Transform/Convert data-files at  /user/smakired/arun-cca175/problem5/json-gzip and store the converted file at the following locations and file formats
 ->save the data to as comma separated text using gzip compression at   /user/smakired/arun-cca175/problem5/csv-gzip
8.Using spark access data at /user/smakired/arun-cca175/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/smakired/arun-cca175/problem5/orc 
'''

#1

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/smakired/arun-cca175/problem5/text \
--fields-terminated-by '\t' \
--as-textfile

#2

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/smakired/arun-cca175/problem5/avro \
--as-avrodatafile

#3

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/smakired/arun-cca175/problem5/parquet \
--as-parquetfile

#4

pyspark --master yarn \
--packages com.databricks:spark-avro_2.10:2.0.1 


orderDF=sqlContext.read.format("com.databricks.spark.avro").load("/user/smakired/arun-cca175/problem5/avro/*")

#a

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
orderDF.coalesce(1).write.parquet("/user/smakired/arun-cca175/problem5/parquet-snappy-compress")

#b


result = orderDF.rdd.map(list)
result.map(lambda r : '\t'.join([str(i) for i in r])). \
	coalesce(1). \
	saveAsTextFile("/user/smakired/arun-cca175/problem5/text-gzip-compress","org.apache.hadoop.io.compress.GzipCodec")

#c 

result.map(lambda r : (None, ','.join([str(i) for i in r]))). \
coalesce(1). \
saveAsSequenceFile("/user/smakired/arun-cca175/problem5/sequence")


#d

result.map(lambda r : '\t'.join([str(i) for i in r])). \
	coalesce(1). \
	saveAsTextFile("/user/smakired/arun-cca175/problem5/text-snappy-compress","org.apache.hadoop.io.compress.SnappyCodec")
