'''

Instructions
Get number of companies who filed LCAs for each year

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data_noheader
Fields: 
ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
Use EMPLOYER_NAME as the criteria to identify the company name to get number of companies
YEAR is 8th field
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: text
Delimiter: tab character "\t"
Output Field Order: year, lca_count
Place the output files in the HDFS directory
/user/`whoami`/problem19/solution/
Replace `whoami` with your OS user name
End of Problem


'''

h1b = sc.textFile("/public/h1b/h1b_data_noheader")

from pyspark.sql import Row

h1bDF= h1b.map(lambda r : Row(year = r.split("\0")[7])).toDF()

h1bDF.registerTempTable("h1b_data")

resultDF = sqlContext.sql('''select year,count(1) lca_count from h1b_data where year != 'NA' group by year''')

resultDF.map(lambda r:'\t'.join([str(i) for i in r])). \
coalesce(1). \
saveAsTextFile("/user/smakired/problem19/solution/")

