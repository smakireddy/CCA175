'''
Instructions
Get the stock tickers from NYSE data for which full name is missing in NYSE symbols data

Data Description
NYSE data with "," as delimiter is available in HDFS

NYSE data information:

HDFS location: /public/nyse
There is no header in the data
NYSE Symbols data with " " as delimiter is available in HDFS

NYSE Symbols data information:

HDFS location: /public/nyse_symbols
First line is header and it should be included
Output Requirements
Get unique stock ticker for which corresponding names are missing in NYSE symbols data
Save data back to HDFS
File Format: avro
Avro dependency details: 
groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
Place the sorted NYSE data in the HDFS directory
/user/`whoami`/problem17/solution/
Replace `whoami` with your OS user name
End of Problem

'''

pyspark --master yarn \
--conf spark.ui.port=14535 \
--executor-memory 1GB \
--total-executor-cores 3 \
--num-executors 6 \
--packages com.databricks:spark-avro_2.10:2.0.1 

nyse = sc.textFile("/public/nyse")

nyse_sym = sc.textFile("/public/nyse_symbols")

from pyspark.sql import Row

nyseDF = nyse.map(lambda r: Row(stockticker= r.split(",")[0])).toDF()
nyse_sym_df = nyse_sym.map(lambda r : Row(stock_sym = r.split("\t")[0])).toDF()

nyseDF.registerTempTable("nyse")
nyse_sym_df.registerTempTable("nyse_sym")


resultDF = sqlContext.sql('''select distinct stockticker as symbol from nyse n left outer join nyse_sym ns 
	on (n.stockticker = ns.stock_sym) where ns.stock_sym is NULL
	order by symbol''')

resultDF.write.format("com.databricks.spark.avro").save("/user/smakired/problem17/solution/")


#validation

sqlContext.read.format("com.databricks.spark.avro").load("/user/smakired/problem17/solution/").show()


