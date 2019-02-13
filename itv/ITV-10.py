'''
Instructions
Get number of LCAs filed for each year

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data
Ignore first record which is header of the data
YEAR is 8th field in the data
There are some LCAs for which YEAR is NA, ignore those records
Output Requirements
File Format: text
Output Fields: YEAR, NUMBER_OF_LCAS
Delimiter: Ascii null "\0"
Place the output files in the HDFS directory
/user/`whoami`/problem10/solution/
Replace `whoami` with your OS user name
End of Problem

'''

h1b = sc.textFile("/public/h1b/h1b_data")
header = h1b.first()
h1bRDD = h1b.filter(lambda r : r != header)
h1bData = h1bRDD.filter(lambda r: (r.split("\0")[7]) != 'NA' )
from  operator import add 
h1bReduce = h1bData.map(lambda r : (int(r.split("\0")[7]), 1)).reduceByKey(add).sortByKey()

h1bReduce.map(lambda r : str(r[0])+'\0'+str(r[1])). \
coalesce(1). \
saveAsTextFile("/user/smakired/problem10/solution/")
