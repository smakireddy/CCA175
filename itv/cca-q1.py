'''
Details - Duration 40 minutes
Data set URL 416
Choose language of your choice Python or Scala
Data is available in HDFS file system under /public/crime/csv
You can check properties of files using hadoop fs -ls -h /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,”
Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
Output File Format: TEXT
Output Columns: Month in YYYYMM format, crime count, crime type
Output Delimiter: \t (tab delimited)
Output Compression: gzip
'''

pyspark --master yarn \
--conf spark.ui.port=14356 \
--executor-memory 1GB \
--num-executors 3 \
--total-executor-cores 2 


crimeRaw = sc.textFile("/public/crime/csv")
first = crimeRaw.first()
crimeRDD = crimeRaw.filter(lambda r: r!= first)

def fetchCrimeTypeAndMonth(rec):
	items = rec.split(",")
	date = items[2]
	month = date[:2]
	year = date[6:10]
	crimetype = items[5]
	monthyear = int(year+month)
	return (monthyear,crimetype)

crimeDF = crimeRDD.map(lambda r : fetchCrimeTypeAndMonth(r)).\
	toDF(["crime_month","crime_type"])

crimeDF.registerTempTable("crime_data")

resultDF = sqlContext.sql('''select crime_month, count(1) crime_count, crime_type
	from crime_data
	group by crime_month, crime_type
	order by crime_month, crime_count desc''')

resultDF.map(lambda r: '\t'.join([str(i) for i in r])).\
	coalesce(1). \
	saveAsTextFile('/user/smakired/practice-1/q1/crimes_by_type_by_month',compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")