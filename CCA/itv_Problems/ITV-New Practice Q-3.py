'''

Instructions
Get top 3 crime types based on number of incidents in RESIDENCE area using "Location Description"

Data Description
Data is available in HDFS under /public/crime/csv

crime data information:

Structure of data: (ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrst, Domestic, Beat, District, Ward, Community Area, FBI Code, X Coordinate, Y Coordinate, Year, Updated on, Latitude, Longitude, Location)
File format - text file
Delimiter - "," (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.
Output Requirements
Output Fields: crime_type, incident_count
Output File Format: JSON
Delimiter: N/A
Compression: No
Place the output file in the HDFS directory
/user/`whoami`/problem3/solution/
Replace `whoami` with your OS user name
End of Problem
'''

crimeData = sc.textFile("/public/crime/csv/crime_data.csv")

header = crimeData.first()

crimeDataRDD = crimeData.filter(lambda r : r!=header)

import re

rexp = re.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

crimeDataDF = crimeDataRDD. \
map(lambda r: (str(rexp.split(r)[5]),str(rexp.split(r)[7]))). \
toDF(["crime_type","location_desc"])

crimeDataDF.registerTempTable("crime_data")

result = sqlContext.sql('''select crime_type, count(1) incident_count from crime_data
	where location_desc = 'RESIDENCE' 
	group by crime_type 
	order by incident_count desc
	limit 3''')

result.write.format("json").save("/user/smakired/problem3/solution/")


result.explaine
