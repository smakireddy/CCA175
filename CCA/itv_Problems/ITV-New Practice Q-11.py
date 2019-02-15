'''

Instructions
Get number of LCAs by status for the year 2016

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data
Ignore first record which is header of the data
YEAR is 8th field in the data
STATUS is 2nd field in the data
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: json
Output Field Names: year, status, count
Place the output files in the HDFS directory
/user/`whoami`/problem11/solution/
Replace `whoami` with your OS user name
End of Problem

'''


h1b = sc.textFile("/public/h1b/h1b_data")

header = h1b.first()

h1bRDD = h1b.filter(lambda r : r!= header)

from pyspark.sql import Row

h1bDF = h1bRDD.map(lambda r : Row(year = (r.split("\0")[7]), status = r.split("\0")[1])).toDF()

h1bDF.registerTempTable("h1b")

result = sqlContext.sql("select cast(year as bigint) , status , count(1) count from h1b where year <> 'NA' group by year, status")

result. \
coalesce(1). \
write.format("json"). \
save("/user/smakired/problem11/solution/")