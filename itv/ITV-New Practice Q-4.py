'''
Instructions
Convert NYSE data into parquet

NYSE data Description
Data is available in local file system under /data/NYSE (ls -ltr /data/NYSE)

NYSE Data information:

Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)
Output Requirements
Column Names: stockticker, transactiondate, openprice, highprice, lowprice, closeprice, volume
Convert file format to parquet
Place the output file in the HDFS directory
/user/`whoami`/problem4/solution/
Replace `whoami` with your OS user name
End of Problem

'''


nyseData = sc.textFile("/user/smakired/data/nyse/nyse/")
from pyspark.sql import Row
nyseDataDF = nyseData. \
map(lambda r: \
	Row(stockticker = r.split(",")[0], \
		transactiondate = r.split(",")[1], \
		openprice = float(r.split(",")[2]), \
		highprice = float(r.split(",")[3]), \
		lowprice = float(r.split(",")[4]), \
		closeprice = float(r.split(",")[5]), \
		volume = int(r.split(",")[6])) ).toDF()

nyseDataDF.write.parquet("/user/smakired/problem4/solution/")
