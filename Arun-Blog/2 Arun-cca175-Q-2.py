'''
Problem 2:

1. Using sqoop copy data available in mysql products table to folder /user/smakired/arun-cca175/products on hdfs as text file. columns should be delimited by pipe '|'
2. move all the files from /user/smakired/arun-cca175/products folder to /user/smakired/arun-cca175/problem2/products folder
3. Change permissions of all the files under /user/smakired/arun-cca175/problem2/products such that owner has read,write and execute permissions, group has read and write permissions whereas others have just read and execute permissions
4. read data in /user/smakired/arun-cca175/problem2/products and do the following operations using a) dataframes api b) spark sql c) RDDs aggregateByKey method. Your solution should have three sets of steps. Sort the resultant dataset by category id

-filter such that your RDD\DF has products whose price is lesser than 100 USD
-on the filtered data set find out the higest value in the product_price column under each category
-on the filtered data set also find out total products under each category
-on the filtered data set also find out the average price of the product under each category
-on the filtered data set also find out the minimum price of the product under each category

5. store the result in avro file using snappy compression under these folders respectively
/user/smakired/arun-cca175/problem2/products/result-df
/user/smakired/arun-cca175/problem2/products/result-sql
/user/smakired/arun-cca175/problem2/products/result-rdd

'''
#1

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table products \
--target-dir /user/smakired/arun-cca175/products \
--fields-terminated-by '|' \
--as-textfile 


#2 

hadoop fs -mkdir /user/smakired/arun-cca175/problem2/products

hadoop fs -mv /user/smakired/arun-cca175/products/* /user/smakired/arun-cca175/problem2/products/

#3 

r w e 
4 2 1

hadoop fs -chmod 764 /user/smakired/arun-cca175/problem2/products/*

#4

pyspark --master yarn \
--packages com.databricks:spark-avro_2.10:2.0.1 


products = sc.textFile("/user/smakired/arun-cca175/problem2/products/")
prodRDD = products.filter(lambda r : float(r.split("|")[4]) < 100)

productDF= prodRDD.map(lambda r: (int(r.split("|")[0]),float(r.split("|")[4]))). \
toDF(schema=["product_category_id","product_price"])


productDF.registerTempTable("product_temp")

resultDF = sqlContext.sql('''select product_category_id,
	max(product_price) max_price, sum(product_price) total_price,avg(product_price) avg_price,min(product_price) min_price
				from product_temp
				group by product_category_id''')

sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

resultDF.coalesce(1).write.mode("overwrite").format("com.databricks.spark.avro").save("/user/smakired/arun-cca175/problem2/products/result-sql")


