'''

Instructions
Get top 5 employers for year 2016 where the status is WITHDRAWN or CERTIFIED-WITHDRAWN or DENIED

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data
Ignore first record which is header of the data
YEAR is 7th field in the data
STATUS is 2nd field in the data
EMPLOYER is 3rd field in the data
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: parquet
Output Fields: employer_name, lca_count
Data needs to be in descending order by count
Place the output files in the HDFS directory
/user/`whoami`/problem12/solution/
Replace `whoami` with your OS user name
End of Problem

'''

h1b = sc.textFile ("/public/h1b/h1b_data")

header = h1b.first()

h1bRDD = h1b.filter(lambda r : r <> header)

h1bFinalRDD = h1bRDD.filter(lambda r : ((r.split("\0")[7]) <> 'NA' and (r.split("\0")[7]) == '2016'))
from pyspark.sql import Row
h1bDF = h1bFinalRDD.map(lambda r : Row(year = int(r.split("\0")[7]), status = r.split("\0")[1], employer = r.split("\0")[2] )).toDF()

h1bDF.registerTempTable("h1bdf")

result=sqlContext.sql('''select employer, count(1) lca_count  from h1bdf 
	where status in ('WITHDRAWN','CERTIFIED-WITHDRAWN','DENIED') 
	group by employer
	order by lca_count desc
	limit 5 ''')

result. \
coalesce(1). \
write. \
parquet ("/user/smakired/problem12/solution/")