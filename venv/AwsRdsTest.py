from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp

#from pyspark.sql.column import
spark = SparkSession.builder.getOrCreate()
#def expr(str):
#uber_read.withColumn("date",expr("from_unixtime(unix_timestamp(date, 'dd/MMM/yyyy'), 'yyyy-MMM-dd')"))
uber_data = spark.read.format("csv").option("header", "true").load("file:///F:/Hadoop_practice//uber")
#uber_data.show()
#uber_data_df = uber_data.withColumn(expr("from_unixtime(unix_timestamp(date, 'MM/dd/yyyy')),'yyyy-MM-dd')"))
uber_data_df = uber_data.withColumn('date', from_unixtime(unix_timestamp('date', 'MM/dd/yyyy')))
#uber_data_df = uber_data.withColumn('date', expr("from_unixtime(unix_timestamp('date', 'MM/dd/yyyy'),'dd-MM-yyyy')"))
#uber_data_df = uber_data.withColumn('date',date_format('date', 'MM/dd/yyyy'))
#uber_data_df.show()
#uber_data_df = uber_data.withColumn('date',date_format('date','MM-dd-yyyy'))
uber_data_day = uber_data_df.withColumn('day',date_format('date','EEE'))
#uber_data_day.show()
#uber_data_grp = uber_data_day.groupBy('dispatching_base_number',"day").agg({'trips':'sum'}).withColumnRenamed("sum(trips)")
uber_data_grp = uber_data_day.groupBy("dispatching_base_number","day").agg({"trips":"sum"}).withColumnRenamed("sum(trips)","sum_trips")
#withColumnRenamed("sum(trips)","sum_trips")
#uber_data.withColumn('date' ,date_format(func(col('date')) , 'YYYY-MM-DD')).show()
uber_data_grp.write.format("jdbc").option("url", "jdbc:mysql://database-1.cuc9djnhvu2j.ap-south-1.rds.amazonaws.com:3306/uberwrite").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Rakesh_Uber").option("user", "root").option("password", "Adityausa908").mode("append").save()
#uber_read = spark.read.format("jdbc").option("url", "jdbc:mysql://database-1.cuc9djnhvu2j.ap-south-1.rds.amazonaws.com:3306/uberwrite").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Rakesh_Uber").option("user", "root").option("password", "Adityausa908").load()
#uber_read.show()