'''
Duration: 20 to 30 minutes

Tables should be in hive database - <YOUR_USER_ID>_retail_db_txt
orders
order_items
customers
Time to create database and tables need not be counted. Make sure to go back to Spark SQL module and create tables and load data
Get details of top 5 customers by revenue for each month
We need to get all the details of the customer along with month and revenue per month
Data need to be sorted by month in ascending order and revenue per month in descending order
Create table top5_customers_per_month in <YOUR_USER_ID>_retail_db_txt
Insert the output into the newly created table

'''


pyspark --master yarn \
--conf spark.ui.port=14536 \
--total-executor-cores 10 \
--executor-memory 1GB \
--num-executors 5 


orders = sc.textFile("/apps/hive/warehouse/smakired_retail_db_txt.db/orders")
order_items = sc.textFile("/apps/hive/warehouse/smakired_retail_db_txt.db/order_items")
customers = sc.textFile("/apps/hive/warehouse/smakired_retail_db_txt.db/customers")


orders.createOrReplaceTempView("orders")

select total_revenue, order_month , revenue_rank , c.* from 
(select a.total_revenue,a.order_month, a.order_customer_id , dense_rank() over(partition by a.order_month order by a.total_revenue desc) revenue_rank 
from 
(select round(sum(oi.order_item_subtotal), 2) total_revenue,cast(concat(substr(order_date,1,4),substr(order_date,6,2)) as bigint) order_month , o.order_customer_id
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
group by cast(concat(substr(order_date,1,4),substr(order_date,6,2)) as bigint),o.order_customer_id ) a ) b 
join customers c
on b.order_customer_id = c.customer_id
where b.order_revenue <= 5 
order by b.order_month , b.total_revenue desc

ordersDF.createOrReplaceTempView("orders")