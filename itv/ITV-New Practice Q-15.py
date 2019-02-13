'''
Instructions
Connect to the MySQL database on the itversity labs using sqoop and import data with case_status as CERTIFIED

Data Description
A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 3002373 rows of h1b data

MySQL database information:

Installation on the node ms.itversity.com
Database name is h1b_db
Username: h1b_user
Password: itversity
Table name h1b_data
Output Requirements
Place the h1b related data in files in HDFS directory
/user/`whoami`/problem15/solution/
Replace `whoami` with your OS user name
Use avro file format
Load only those records which have case_status as CERTIFIED completely
There are 2615623 such records
End of Problem

'''

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/h1b_db \
--username h1b_user \
--password itversity \
--table h1b_data \
--target-dir /user/smakired/problem15/solution/ \
--where "case_status = 'CERTIFIED'" \
--as-avrodatafile