#Ignore this file
#read 4 , write 2 , execute 1 
#owner , group , others 
#7,6,4
#gzip will not work on avro files - sqoop

sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
sqlContext.setConf("spark.sql.avro.compression.codec","gzip")

pyspark --master yarn \
--packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.11:1.5.0

MyNewRDD = resultDF.rdd.map(list) -> avro -> csv 

resultsDF.coalesce(1). \
write.format("com.databricks.spark.avro"). \
save("/user/smakireddy/arun/problem2/products/result-sql-snappy")

from_unixtime(cast(order_date/1000 as bigint),'yyyy-MM-dd')

from pyspark.sql import Row
from operator import add

avro-tools getschema part-m-00000.avro>orders.avsc

create external table orders_sqoop
stored as avro
location '/user/hive/warehouse/retail_test.db/orders'
tblproperties ('avro.schema.url'='/user/smakireddy/arun/schema/orders.avsc')


