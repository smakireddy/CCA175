'''
Details - Duration 10 minutes
Data is available in local file system under /data/nyse (ls -ltr /data/nyse)
Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)
Convert file format to parquet
Save it /user/<YOUR_USER_ID>/nyse_parquet
Validation
'''


hadoop fs -put /data/nyse/NYSE* /user/smakireddy/somu/nyse/

nyseData = sc.textFile("/user/smakireddy/somu/nyse/nyse")

from pyspark.sql import Row

nyseDF = nyseData. \
map(lambda r: Row( \
	stockticker=str(r.split(",")[0]), \
	transactiondate=str(r.split(",")[1]), \
	openprice=float(r.split(",")[2]), \
	highprice=float(r.split(",")[3]), \
	lowprice=float(r.split(",")[4]), \
	closeprice=float(r.split(",")[5]), \
	volume=int(r.split(",")[6]))). \
toDF()

nyseDF.write. \
coalesce(1). \
parquet("/user/smakireddy/somu/solution3/nyse_parquet")


	



