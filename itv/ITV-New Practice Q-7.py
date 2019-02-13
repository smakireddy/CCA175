'''
Instructions
List the names of the Top 5 products by revenue ordered on '2013-07-26'. Revenue is considered only for COMPLETE and CLOSED orders.

Data Description
retail_db data is available in HDFS at /public/retail_db

retail_db data information:

Source directories: 
/public/retail_db/orders 
/public/retail_db/order_items 
/public/retail_db/products
Source delimiter: comma(",")
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - order_itemss - order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price
Source Columns - products - product_id, product_category_id, product_name, product_description, product_price, product_image
Output Requirements
Target Columns: order_date, order_revenue, product_name, product_category_id
Data has to be sorted in descending order by order_revenue
File Format: text
Delimiter: colon (:)
Place the output file in the HDFS directory
/user/`whoami`/problem7/solution/
Replace `whoami` with your OS user name
End of Problem

'''


orders = sc.textFile("/public/retail_db/orders")
order_items = sc.textFile("/public/retail_db/order_items")
products = sc.textFile("/public/retail_db/products")

from pyspark.sql import Row

ordersDF = orders. \
map(lambda r: Row(order_id = int(r.split(",")[0]), \
	order_date=r.split(",")[1], \
	order_customer_id = int(r.split(",")[2]), \
	order_status = r.split(",")[3])).toDF()

orderItemsDF = order_items. \
map(lambda oi : Row(order_item_id = int(oi.split(",")[0]), \
	order_item_order_id = int(oi.split(",")[1]), \
	order_item_product_id = int(oi.split(",")[2]), \
	order_item_subtotal = float(oi.split(",")[4])
	)).toDF()

productsDF = products.map(lambda p: Row(product_id=int(p.split(",")[0]), \
	product_category_id = int(p.split(",")[1]),product_name = p.split(",")[2])).toDF()


ordersDF.registerTempTable("orders")
orderItemsDF.registerTempTable("order_items")
productsDF.registerTempTable("products")


results = sqlContext.sql('''select to_date(order_date) , order_revenue, product_name , product_category_id 
	from (select order_date,order_item_product_id , sum(order_item_subtotal) order_revenue 
	from orders o join order_items oi 
	on o.order_id = oi.order_item_order_id 
	where to_date(order_date) = '2013-07-26'
	and order_status in ('CLOSED','COMPLTE')
	group by order_date, order_item_product_id) a
	join products p 
	on p.product_id = a.order_item_product_id 
	order by order_revenue desc 
	limit 5''')


results.map(lambda r: ":".join([str(i) for i in r])). \
coalesce(1). \
saveAsTextFile("/user/smakired/problem7/solution/")
