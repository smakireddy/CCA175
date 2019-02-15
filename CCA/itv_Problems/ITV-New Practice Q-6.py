'''

Instructions
Get total number of orders for each customer where the cutomer_state = 'TX'

Data Description
retail_db data is available in HDFS at /public/retail_db

retail_db data information:

Source directories: /public/retail_db/orders and /public/retail_db/customers
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname, customer_state (8th column) and many more
delimiter: (",")
Output Requirements
Output Fields: customer_fname, customer_lname, order_count
File Format: text
Delimiter: Tab character (\t)
Place the result file in the HDFS directory
/user/`whoami`/problem6/solution/
Replace `whoami` with your OS user name
End of Problem

'''


orders = sc.textFile("/public/retail_db/orders")
customers = sc.textFile("/public/retail_db/customers")

from pyspark.sql import Row 

ordersDF = orders.map(lambda r: Row(order_id = int(r.split(",")[0]),order_customer_id = int(r.split(",")[2]))).toDF()

ordersDF.registerTempTable("orders")

customersDF = customers. \
map(lambda c: Row(customer_id= int(c.split(",")[0]), \
	customer_fname = c.split(",")[1], \
	customer_lname= c.split(",")[2] , customer_state=c.split(",")[7])).toDF()

customersDF.registerTempTable("customers")

results = sqlContext.sql('''select customer_fname, customer_lname, count(order_id) order_count
				  from orders o join customers c
				  on o.order_customer_id= c.customer_id
				  where c.customer_state = 'TX'
				  group by customer_fname, customer_lname
				  ''')

results.map(lambda r: '\t'.join([str(i) for i in r])). \
coalesce(1). \
saveAsTextFile("/user/smakired/problem6/solution/")