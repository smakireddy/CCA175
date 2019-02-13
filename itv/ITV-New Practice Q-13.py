'''
Instructions
Copy all h1b data from HDFS to Hive table excluding those where year is NA or prevailing_wage is NA

Data Description
h1b data with ascii null "\0" as delimiter is available in HDFS

h1b data information:

HDFS Location: /public/h1b/h1b_data_noheader
Fields: 
ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
Ignore data where PREVAILING_WAGE is NA or YEAR is NA
PREVAILING_WAGE is 7th field
YEAR is 8th field
Number of records matching criteria: 3002373
Output Requirements
Save it in Hive Database
Create Database: CREATE DATABASE IF NOT EXISTS `whoami`
Switch Database: USE `whoami`
Save data to hive table h1b_data
Create table command:

CREATE TABLE h1b_data (
  ID                 INT,
  CASE_STATUS        STRING,
  EMPLOYER_NAME      STRING,
  SOC_NAME           STRING,
  JOB_TITLE          STRING,
  FULL_TIME_POSITION STRING,
  PREVAILING_WAGE    DOUBLE,
  YEAR               INT,
  WORKSITE           STRING,
  LONGITUDE          STRING,
  LATITUDE           STRING
)
                
Replace `whoami` with your OS user name
End of Problem
'''
create database if not exists smakired_1;

use smakired_1;

CREATE TABLE h1b_data (
  ID                 INT,
  CASE_STATUS        STRING,
  EMPLOYER_NAME      STRING,
  SOC_NAME           STRING,
  JOB_TITLE          STRING,
  FULL_TIME_POSITION STRING,
  PREVAILING_WAGE    DOUBLE,
  YEAR               INT,
  WORKSITE           STRING,
  LONGITUDE          STRING,
  LATITUDE           STRING
); 

pyspark --master yarn \
--conf spark.ui.port=14365 \
--total-executor-cores 2 \
--executor-memory 2GB \
--num-executors 5


h1b = sc.textFile("/public/h1b/h1b_data_noheader")

h1bDF = h1b.map(lambda r: (r.split("\0")[0], r.split("\0")[1],r.split("\0")[2],r.split("\0")[3],r.split("\0")[4],r.split("\0")[5],r.split("\0")[6],r.split("\0")[7],r.split("\0")[8],r.split("\0")[9],r.split("\0")[10])).toDF(schema=["ID", "CASE_STATUS", "EMPLOYER_NAME", "SOC_NAME", "JOB_TITLE", "FULL_TIME_POSITION", "PREVAILING_WAGE", "YEAR", "WORKSITE", "LONGITUDE", "LATITUDE"])

h1bDF.registerTempTable("h1b")

sqlContext.sql('''
insert into smakired_1.h1b_data 
select * from h1b 
where YEAR != 'NA' and PREVAILING_WAGE != 'NA'
''')


