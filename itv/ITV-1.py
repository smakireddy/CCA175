'''

Instructions
Connect to the MySQL database on the itversity labs using sqoop and import all of the data from the orders table into HDFS

Data Description
A MySQL instance is running on a remote node ms.itversity.com in the instance. You will find a table that contains 68883 rows of orders data

MySQL database information:

Installation on the node ms.itversity.com
Database name is retail_db
Username: retail_user
Password: itversity
Table name orders
Output Requirements
Place the customer files in the HDFS directory
/user/`whoami`/problem1/solution/
Replace `whoami` with your OS user name
Use a text format with comma as the columnar delimiter
Load every order record completely
End of Problem
'''

sqoop list-tables \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/smakired/problem1/solution/
--as-textfile 

