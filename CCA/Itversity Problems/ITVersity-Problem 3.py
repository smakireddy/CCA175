'''
Details - Duration 15 to 20 minutes
Data is available in HDFS file system under /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,” (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.
Get top 3 crime types based on number of incidents in RESIDENCE area using “Location Description”
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA
Output Fields: Crime Type, Number of Incidents
Output File Format: JSON
Output Delimiter: N/A
Output Compression: No
'''


8:17

pyspark --master yarn \
--conf spark.ui.port=14568 \
--num-executors 6 \
--executor-memory 1GB \
--executor-cores 2



crimeData = sc.textFile("/public/crime/csv")

crimeMap = crimeData.filter(lambda r: r.split(",")[7]=='RESIDENCE'). \
map(lambda c: (c.split(",")[5], 1)). \
reduceByKey(lambda a,b:a+b)

crimeData = crimeMap.takeOrdered(3, key=lambda x: -x[1])

sc.parallelize(crimeData). \
toDF(schema=(["Crime Type","Number of Incidents"])). \
coalesce(1). \
write.json("/user/smakireddy/somu/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA")




