'''

Instructions
Export h1b data from hdfs to MySQL Database

Data Description
h1b data with ascii character "\001" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data_to_be_exported
Fields: 
ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
Number of records: 3002373
Output Requirements
Export data to MySQL Database
MySQL database is running on ms.itversity.com
User: h1b_user
Password: itversity
Database Name: h1b_export
Table Name: h1b_data_`whoami`
Nulls are represented as: NA
After export nulls should not be stored as NA in database. It should be represented as database null
Create table command:

CREATE TABLE h1b_data_smakired(
  ID                 INT, 
  CASE_STATUS        VARCHAR(50), 
  EMPLOYER_NAME      VARCHAR(100), 
  SOC_NAME           VARCHAR(100), 
  JOB_TITLE          VARCHAR(100), 
  FULL_TIME_POSITION VARCHAR(50), 
  PREVAILING_WAGE    FLOAT, 
  YEAR               INT, 
  WORKSITE           VARCHAR(50), 
  LONGITUDE          VARCHAR(50), 
  LATITUDE           VARCHAR(50));
                
Replace `whoami` with your OS user name
Above create table command can be run using
Login using mysql -u h1b_user -h ms.itversity.com -p
When prompted enter password itversity
Switch to database using use h1b_export
Run above create table command by replacing `whoami` with your OS user name
End of Problem

'''

sqoop export \
--connect jdbc:mysql://ms.itversity.com:3306/h1b_export \
--username h1b_user \
--password itversity \
--export-dir /public/h1b/h1b_data_to_be_exported \
--table h1b_data_smakired \
--input-fields-terminated-by '\001' \
--input-null-string 'NA'



'''
h1b = sc.textFile("/public/h1b/h1b_data_to_be_exported")

from pyspark.sql import Row

h1bDF = h1b. \
map(lambda r : (r.split("\001")[0],r.split("\001")[1],r.split("\001")[2], r.split("\001")[3],r.split("\001")[4],r.split("\001")[5], r.split("\001")[6],r.split("\001")[7], r.split("\001")[8], r.split("\001")[9],r.split("\001")[10])). \
toDF(schema = ["ID", "CASE_STATUS", "EMPLOYER_NAME", "SOC_NAME", "JOB_TITLE", "FULL_TIME_POSITION","PREVAILING_WAGE", "YEAR", "WORKSITE", "LONGITUDE", "LATITUDE"])

h1bDF.registerTempTable("h1b")

sqlContext.sql("select count(1) from h1b where SOC_NAME ='NA'").show()
'''
