import configparser as cp, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, dense_rank
from pyspark.sql.window import *

props = cp.RawConfigParser()
props.read('src/main/resources/application.properties')
env = sys.argv[1]
topN = int(sys.argv[2])

spark = SparkSession. \
  builder. \
  master(props.get(env, 'execution.mode')). \
  appName('Get TopN Daily Products using Data Frame Operations'). \
  getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '2')
spark.sparkContext.setLogLevel('ERROR')

inputBaseDir = props.get(env, 'input.base.dir')
orders = spark.read. \
  format('csv'). \
  schema('order_id int, order_date string, order_customer_id int, order_status string'). \
  load(inputBaseDir + '/orders')

orderItems = spark.read. \
  format('csv'). \
  schema('''order_item_id int, 
            order_item_order_id int, 
            order_item_product_id int, 
            order_item_quantity int,
            order_item_subtotal float,
            order_item_product_price float
         '''). \
  load(inputBaseDir + '/order_items')

dailyProductRevenue = orders. \
    where('order_status in ("COMPLETE", "CLOSED")'). \
    join(orderItems, orders.order_id == orderItems.order_item_order_id). \
    groupBy('order_date', 'order_item_product_id'). \
    agg(round(sum('order_item_subtotal'), 2).alias('revenue'))

spec = Window. \
partitionBy('order_date'). \
orderBy(dailyProductRevenue.revenue.desc())

dailyProductRevenueRanked = dailyProductRevenue. \
withColumn("rnk", dense_rank().over(spec))

topNDailyProducts = dailyProductRevenueRanked. \
where(dailyProductRevenueRanked.rnk <= topN). \
drop('rnk'). \
orderBy('order_date', dailyProductRevenueRanked.revenue.desc())

outputBaseDir = props.get(env, 'output.base.dir')
topNDailyProducts. \
    write. \
    mode('overwrite'). \
    csv(outputBaseDir + '/topn_daily_products')