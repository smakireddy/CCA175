'''
Problem 1:
1.Using sqoop, import orders table into hdfs to folders /user/smakired/arun-cca175/problem1/orders. File should be loaded as Avro File and use snappy compression
2.Using sqoop, import order_items  table into hdfs to folders /user/smakired/arun-cca175/problem1/order-items. Files should be loaded as avro file and use snappy compression
3.Using Spark Scala load data at /user/smakired/arun-cca175/problem1/orders and /user/smakired/arun-cca175/problem1/orders-items items as dataframes. 
4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day. The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending. Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
	a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
	b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
	c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
5.Store the result as parquet file into hdfs using gzip compression under folder
	/user/smakired/arun-cca175/problem1/result4a-gzip
	/user/smakired/arun-cca175/problem1/result4b-gzip
	/user/smakired/arun-cca175/problem1/result4c-gzip
6.Store the result as parquet file into hdfs using snappy compression under folder
	/user/smakired/arun-cca175/problem1/result4a-snappy
	/user/smakired/arun-cca175/problem1/result4b-snappy
	/user/smakired/arun-cca175/problem1/result4c-snappy
7.Store the result as CSV file into hdfs using No compression under folder
/user/smakired/arun-cca175/problem1/result4a-csv
/user/smakired/arun-cca175/problem1/result4b-csv
/user/smakired/arun-cca175/problem1/result4c-csv
8.create a mysql table named result and load data from /user/smakired/arun-cca175/problem1/result4a-csv to mysql table named result 
'''

#1

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/smakired/arun-cca175/problem1/orders \
--delete-target-dir \
--as-avrodatafile \
--compress \
--compression-codec 'org.apache.hadoop.io.compress.SnappyCodec' \
--m 1

#'org.apache.hadoop.io.compress.SnappyCodec'


#2
 
 sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/smakired/arun-cca175/problem1/order_items \
--delete-target-dir \
--as-avrodatafile \
--compress \
--compression-codec 'org.apache.hadoop.io.compress.SnappyCodec' \
--m 1

#3 


pyspark --master yarn \
--conf spark.ui.port=14356 \
--packages com.databricks:spark-avro_2.10:2.0.1

ordersDF = sqlContext.read.format("com.databricks.spark.avro").load("/user/smakired/arun-cca175/problem1/orders")

orderItemsDF = sqlContext.read.format("com.databricks.spark.avro").load("/user/smakired/arun-cca175/problem1/order_items")

#4 

ordersDF.registerTempTable("orders_temp")
orderItemsDF.registerTempTable("order_items_temp")


resultDF = sqlContext.sql('''select from_unixtime(cast(order_date/1000 as bigint),'yyyy-MM-dd') order_date , Order_status, 
count(order_id) total_orders, sum(order_item_subtotal) as total_amount
from orders_temp a join order_items_temp b 
on a.order_id= b.order_item_order_id 
group by from_unixtime(cast(order_date/1000 as bigint),'yyyy-MM-dd') ,Order_status 
order by order_date desc,order_status, total_amount desc , total_orders''')


#5 

sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")

resultDF.coalesce(1).write.parquet("/user/smakired/arun-cca175/problem1/result4b-gzip")


#validate
sqlContext.read.parquet("/user/smakired/arun-cca175/problem1/result4b-gzip").show()

#6 

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

resultDF.coalesce(1).write.parquet("/user/smakired/arun-cca175/problem1/result4b-snappy")

#validate
sqlContext.read.parquet("/user/smakired/arun-cca175/problem1/result4b-snappy").show()

#7 

resultMap = resultDF.rdd.map(list)

resultMap.map(lambda r: ','.join([str(i) for i in r])). \
coalesce(1). \
saveAsTextFile("/user/smakired/arun-cca175/problem1/result4b-csv")


for i in sc.textFile("/user/smakired/arun-cca175/problem1/result4b-csv").take(10):print(i)


#8 

create table result_smakired_01
(
	order_date date,
	order_status varchar(500),
	total_orders int,
	total_amount float)

sqoop export \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--table result_smakired_01 \
--export-dir /user/smakired/arun-cca175/problem1/result4b-csv \
--input-fields-terminated-by ','