'''
Instructions
Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname

Data Description
Data is available in local file system /data/retail_db

retail_db information:

Source directories: /data/retail_db/orders and /data/retail_db/customers
Source delimiter: comma(",")
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
Output Requirements
Target Columns: customer_lname, customer_fname
Number of Files: 1
Place the output file in the HDFS directory
/user/`whoami`/problem2/solution/
Replace `whoami` with your OS user name
File format should be text
delimiter is (",")
Compression: Uncompressed
End of Problem

'''

ordersRaw = open("/data/retail_db/orders/part-00000").read().splitlines()
orders = sc.parallelize(ordersRaw)
ordersDF = orders.map(lambda r:  (int(r.split(",")[0]) ,int(r.split(",")[2]))).toDF(["order_id","order_customer_id"])
ordersDF.registerTempTable("orders")


customersRaw = open("/data/retail_db/customers/part-00000").read().splitlines()
customers = sc.parallelize(customersRaw)
customerDF = customers. \
map(lambda c:(int(c.split(",")[0]) ,c.split(",")[1],c.split(",")[2])). \
toDF(["customer_id","customer_fname","customer_lname"])
customerDF.registerTempTable("customers")


results = sqlContext.sql('''
	select distinct customer_lname, customer_fname 
	from customers c left outer join orders o
	on c.customer_id = o.order_customer_id
	where o.order_customer_id is null
	order by customer_lname, customer_fname
	''')

results.map(lambda r : ",".join([str(i) for i in r])). \
coalesce(1). \
saveAsTextFile("/user/smakired/problem2/solution/")

sqlContext.sql('''select * from orders where order_customer_id= 10925''').show()


sqlContext.sql('''
	select o.*, c.* 
	from customers c left outer join orders o
	on c.customer_id = o.order_customer_id
	where o.order_customer_id is not null 
	order by customer_lname, customer_fname ''').show()

