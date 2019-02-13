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

ordersRaw = open("/data/retail_db/orders/part-00000").read().splitlines()
ordersRDD = sc.parallelize(ordersRaw)

customersRaw= open("/data/retail_db/customers/part-00000").read().splitlines()
customersRDD = sc.parallelize(customersRaw)

ordersDF = ordersRDD.map(lambda r: (int(r.split(",")[0]),str(r.split(",")[2]))). \
toDF(schema=["order_id","order_customer_id"])

ordersDF.registerTempTable("orders")

customerDF = customersRDD. \
map(lambda c: (int(c.split(",")[0]),c.split(",")[2],c.split(",")[1])). \
toDF(schema =(["customer_id","customer_lname","customer_fname"]))


customerDF.registerTempTable("customers")

resultDF=sqlContext.sql("select distinct customer_lname , customer_fname \
	from customers c left outer join orders o \
	on order_customer_id = customer_id \
	where order_customer_id is NULL \
	order by customer_lname,customer_fname")


resultDF.map(lambda r: r[0]+", "+r[1]).coalesce(1). \
saveAsTextFile("/user/smakireddy/somu/solutions02/inactive_customers")

