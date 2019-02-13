'''
Details - Duration 40 minutes
	Data set URL 284
	- Choose language of your choice Python or Scala
	- Data is available in HDFS file system under /public/crime/csv
	- You can check properties of files using hadoop fs -ls -h /public/crime/csv
	- Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
	- File format - text file
	- Delimiter - “,”
	- Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order
	- Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
	- Output File Format: TEXT
 	- Output Columns: Month in YYYYMM format, crime count, crime type
	- Output Delimiter: \t (tab delimited)
	- Output Compression: gzip
'''




crimeData = sc.textFile("/public/crime/csv")
crimeDataH = crimeData.first()
crimeDataWOH = crimeData.filter(lambda r: r!= crimeDataH)

def fetchDateAndType(r):
	recItems = r.split(",")
	dateString = recItems[2]
	year = dateString[6:10]
	month = dateString[0:2]
	date = int(year+month)
	crimeType = str(recItems[5])
	return date,crimeType

crimeDF = crimeDataWOH. \
map(lambda r : fetchDateAndType(r)). \
toDF(schema=["crime_date","crime_type"])


crimeDF.registerTempTable("crime_data")

resultDF = sqlContext. \
sql("select crime_date, count(crime_type) crime_type_count, \
crime_type from crime_data group by crime_date,crime_type \
order by crime_date,crime_type_count desc")

resultDF.map(lambda r: "\t".join([str(x) for x in r])). \
coalesce(1). \
saveAsTextFile("/user/smakireddy/somu/sol1/crimesbytypebymonth", \
compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")


