
'''
Instructions
Remove header from h1b data

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS location: /public/h1b/h1b_data
First record is the header for the data
Output Requirements
Remove the header from the data and save rest of the data as is
Data should be compressed using snappy algorithm
Place the H1B data in the HDFS directory
/user/`whoami`/problem9/solution/
Replace `whoami` with your OS user name
End of Problem

'''

h1b_data = sc.textFile("/public/h1b/h1b_data")
header = h1b_data.first()
h1bRDD = h1b_data.filter(lambda r : r != header)
h1bRDD.saveAsTextFile()


codec = "org.apache.hadoop.io.compress.SnappyCodec"
h1bRDD.saveAsTextFile("/user/smakired/problem9/solution/", codec)


