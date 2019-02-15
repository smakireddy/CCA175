'''
Instructions
List the order Items where the order_status = 'PENDING PAYMENT' order by order_id

Data Description
Data is available in HDFS location

retail_db data information:

Source directories: /data/retail_db/orders
Source delimiter: comma(",")
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Output Requirements
Target columns: order_id, order_date, order_customer_id, order_status
File Format: orc
Place the output files in the HDFS directory
/user/`whoami`/problem8/solution/
Replace `whoami` with your OS user name
End of Problem

'''

ordersRaw = open("/data/retail_db/orders/part-00000").read().splitlines()
orders = sc.parallelize(ordersRaw)
ordersMap = orders.filter(lambda r : r.split(",")[3] == 'PENDING_PAYMENT')
from pyspar.sql import Row
ordersDF = ordersMap. \
map(lambda r : Row(order_id = int(r.split(",")[0]), \
	order_date = r.split(",")[1], \
	order_customer_id = int(r.split(",")[2]), \
	order_status = r.split(",")[3])).toDF()

ordersDF.write.orc("/user/smakired/problem8/solution/")


)
