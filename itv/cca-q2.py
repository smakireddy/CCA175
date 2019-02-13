'''
Details - Duration 15 to 20 minutes
Data is available in local file system /data/retail_db
Source directories: /data/retail_db/orders and /data/retail_db/customers
Source delimiter: comma (“,”)
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname
Target Columns: customer_lname, customer_fname
Number of files - 1
Target Directory: /user/<YOUR_USER_ID>/solutions/solutions02/inactive_customers
Target File Format: TEXT
Target Delimiter: comma (“, ”)
Compression: N/A
'''

pyspark --master yarn \
--conf spark.ui.port=14563 \
--num-executors 3 \
--executor-memory 1GB \
--total-executor-cores 2 



ordersRaw = open("/data/retail_db/orders/part-00000").read().splitlines()
ordersRDD = sc.parallelize(ordersRaw)
ordersDF = ordersRDD.map(lambda r : (int(r.split(",")[0]), int(r.split(",")[2]))).toDF(["order_id","order_customer_id"])
ordersDF.registerTempTable("orders")

customersRaw = open("/data/retail_db/customers/part-00000").read().splitlines()
customersRDD = sc.parallelize(customersRaw)
customersDF = customersRDD.map(lambda r : (int(r.split(",")[0]), r.split(",")[1],r.split(",")[2])).toDF(["customer_id","customer_fname","customer_lname"])
customersDF.registerTempTable("customers")

result = sqlContext.sql('''select distinct customer_lname, customer_fname 
	from orders o right outer join customers 
	on order_customer_id = customer_id 
	and order_customer_id is null
	order by customer_lname,customer_fname''')

result.map(lambda r: ', '.join([x for x in r])). \
	coalesce(1). \
	saveAsTextFile("/user/smakired/practice-1/q2/inactive_customers")

