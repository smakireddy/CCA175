'''
Instructions
Get NYSE data in ascending order by date and descending order by volume

Data Description
NYSE data with "," as delimiter is available in HDFS

NYSE data information:

HDFS location: /public/nyse
There is no header in the data
Output Requirements
Save data back to HDFS
Column order: stockticker, transactiondate, openprice, highprice, lowprice, closeprice, volume
File Format: text
Delimiter: :
Place the sorted NYSE data in the HDFS directory
/user/`whoami`/problem16/solution/
Replace `whoami` with your OS user name
End of Problem

'''

nyse = sc.textFile("/public/nyse")
nyseDF = nyse.map(lambda r: (r.split(",")[0], \
r.split(",")[1], \
r.split(",")[2], \
r.split(",")[3], \
r.split(",")[4], \
r.split(",")[5], \
r.split(",")[6])). \
toDF(schema=["stockticker", "transactiondate", "openprice", "highprice", "lowprice", "closeprice", "volume"])

from pyspark.sql.functions import *
nyseDF1 = nyseDF.sort(asc("transactiondate"),desc("volume"))

nyseDF1.map(lambda r : (":".join([i for i in r]))). \
coalesce(1). \
saveAsTextFile("/user/smakired/problem16/solution/")

