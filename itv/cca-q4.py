'''
Details - Duration 10 minutes
Data is available in local file system under /data/nyse (ls -ltr /data/nyse)
Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)
Convert file format to parquet
Save it /user/<YOUR_USER_ID>/nyse_parquet
Validation
Solution
'''

hadoop fs -copyFromLocal /data/nyse /user/smakired/data/nyse

from pyspark.sql import Row
nyseDF = sc.textFile("/user/smakired/data/nyse/nyse/"). \
map(lambda r : Row(stockticker = r.split(",")[0], \
transactiondate = r.split(",")[1], \
openprice = float(r.split(",")[2]), \
highprice = float(r.split(",")[3]), \
lowprice = float(r.split(",")[4]), \
closeprice = float(r.split(",")[5]))). \
toDF()

nyseDF.write.parquet("/user/smakired/practice-1/q4/nyse_parquet")


#validation
sqlContext.read.format("parquet").load("/user/smakired/practice-1/q4/nyse_parquet").show()