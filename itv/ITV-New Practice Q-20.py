'''

Instructions
Connect to the MySQL database on the itversity labs using sqoop and import data with employer_name, case_status and count. Make sure data is sorted by employer_name in ascending order and by count in descending order

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
/user/`whoami`/problem20/solution/
Replace `whoami` with your OS user name
Use text file format and tab (\t) as delimiter
Hint: You can use Spark with JDBC or Sqoop import with query
You might not get such hints in actual exam
Output should contain employer name, case status and count
End of Problem

''' 

sqoop import \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:mysql://ms.itversity.com:3306/h1b_db \
--username h1b_user \
--password itversity \
--query 'select employer_name,case_status, count(1) as count from h1b_data where $CONDITIONS group by employer_name,case_status order by employer_name , count desc' \
--target-dir '/user/smakired/problem20/solution/' \
--fields-terminated-by '\t' \
--split-by case_status \
--as-textfile