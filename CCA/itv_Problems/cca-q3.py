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



crimeData = sc.textFile("/public/crime/csv")
first = crimeData.first()
crimeDataWOH = crimeData.filter(lambda r: r!=first)

import re 

regex = re.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

from pyspark.sql import Row 

crimeDataDF = crimeDataWOH. \
map(lambda r: Row(crime_type = regex.split(r)[5], location_desc = regex.split(r)[7])). \
toDF()


crimeDataDF.registerTempTable("crime_data")

result = sqlContext.sql('''select crime_type, count(1) crime_count from crime_data where location_desc = 'RESIDENCE'
group by crime_type 
order by crime_count desc
limit 3''') 

result.write. \
coalesce(1).json("/user/smakired/practice-1/q3/RESIDENCE_AREA_CRIMINAL_TYPE_DATA/")


