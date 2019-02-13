'''
Instructions
Get the name of stocks displayed along with other information

Data Description
NYSE data with "," as delimiter is available in HDFS

NYSE data information:

HDFS location: /public/nyse
There is no header in the data
NYSE Symbols data with tab character (\t) as delimiter is available in HDFS

NYSE Symbols data information:

HDFS location: /public/nyse_symbols
First line is header and it should be included
Output Requirements
Get all NYSE details along with stock name if exists, if not stockname should be empty
Column Order: stockticker, stockname, transactiondate, openprice, highprice, lowprice, closeprice, volume
Delimiter: ,
File Format: text
Place the data in the HDFS directory
/user/`whoami`/problem18/solution/
Replace `whoami` with your OS user name
End of Problem

'''

pyspark --master yarn \
--conf spark.ui.port=14536 \
--total-executor-cores 6 \
--num-executors 3 \
--executor-memory 1GB 


nyse = sc.textFile("/public/nyse")

nyse_sym = sc.textFile("/public/nyse_symbols")
header = nyse_sym.first()
nyse_sym_woh = nyse_sym.filter(lambda r : r <> header)

nyseDF = nyse. \
map(lambda r : (r.split(",")[0],r.split(",")[1],r.split(",")[2],r.split(",")[3],r.split(",")[4],r.split(",")[5],r.split(",")[6])). \
toDF(schema=["stockticker","transactiondate","openprice", "highprice", "lowprice", "closeprice", "volume"])

nyse_sym_df = nyse_sym_woh.map(lambda r : (r.split("\t")[0],r.split("\t")[1])).toDF(schema=["symbol","stockname"])

nyseDF.registerTempTable("nyse")
nyse_sym_df.registerTempTable("nyse_sym")

resultDF = sqlContext.sql('''select stockticker, nvl(stockname,''), 
				  transactiondate, openprice, highprice, lowprice, closeprice, volume
				  from nyse n left outer join nyse_sym ns
				  on (n.stockticker = ns.symbol)
			   ''')

resultDF.map(lambda r : ','.join([str(i) for i in r])). \
coalesce(1). \
saveAsTextFile("/user/smakired/problem18/solution/")




