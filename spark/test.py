import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import Row,functions as F
from pyspark.sql.functions import desc,  from_unixtime
import datetime
import time
start_time =time.time()
os.environ['JAVA_HOME'] = '/root/jdk1.8.0_141'
spark = SparkSession \
    .builder \
    .appName('my_first_app_name') \
    .getOrCreate()
file = r'/root/20170928.txt'
df = spark.read.csv(file,header=True,inferSchema=True)
df = df.drop('direction','HDOP')
speed = df.groupBy('num').avg('speed')
df = df.join(speed, df.num == speed.num, "left").drop(speed.num)
df = df.filter(df['avg(speed)']>0).filter(df['avg(speed)']<16.7)
df = df.drop(df['avg(speed)'])
start_result = df.withColumn("rn", F.row_number().over(Window.partitionBy("num").orderBy("time")))
start_result=start_result.where(start_result.rn<=1)
start_result = start_result.withColumnRenamed('longitude', 'start_long').withColumnRenamed('latitude', 'start_lati')
end_result = df.withColumn("rn", F.row_number().over(Window.partitionBy("num").orderBy(desc("time"))))
end_result = end_result.where(end_result.rn<=1)
end_result = end_result.withColumnRenamed('longitude', 'end_long').withColumnRenamed('latitude', 'end_lati')
result = start_result.join(end_result, start_result.num == end_result.num).drop(start_result.num)
result.createOrReplaceTempView("result")
a = spark.sql("select * from result where result.start_long!=result.end_long or  result.start_lati!=result.end_lati")
a.createOrReplaceTempView("aa")
aa = spark.sql("select num from aa")
df = df.join(aa, df.num == aa.num).drop(aa.num).drop('rn').drop('speed')
start_result1 = df.withColumn("rn", F.row_number().over(Window.partitionBy("num").orderBy("time")))
start_result1=start_result1.where(start_result1.rn<=1).drop('rn').orderBy("time")
start_result1.coalesce(1).write.format('com.databricks.spark.csv').options(header="true").mode('append').save('/root/start')
# head = start_result1.head(1)[0]['time']
# head = datetime.datetime.fromtimestamp(head)
# for i in range(24):
#     tail = head + datetime.timedelta(hours=1)
#     head = tail
#     part_start = start_result1.filter(start_result1['time']>= int(time.mktime((head-datetime.timedelta(hours=1)).timetuple())))\
#         .filter(start_result1['time']<=int(time.mktime(tail.timetuple()))).drop('time').drop('num')
#     print(part_start.count())
#     part_start.coalesce(1).write.format('com.databricks.spark.csv').options(header="true").mode('append').save('/root/start'+str(i))
end_result1 = df.withColumn("rn", F.row_number().over(Window.partitionBy("num").orderBy(desc("time"))))
end_result1 = end_result1.where(end_result1.rn<=1).drop('rn').orderBy("time")
end_result1.coalesce(1).write.format('com.databricks.spark.csv').options(header="true").mode('append').save('/root/end')
# head1 = end_result1.head(1)[0]['time']
# head1 = datetime.datetime.fromtimestamp(head1)
# for i in range(24):
#     tail1 = head1 + datetime.timedelta(hours=1)
#     head1 = tail1
#     part_end = end_result1.filter(end_result1['time']>= int(time.mktime((head1-datetime.timedelta(hours=1)).timetuple())))\
#         .filter(end_result1['time']<=int(time.mktime(tail1.timetuple()))).drop('time').drop('num')
#     part_end.show()
#     part_end.coalesce(1).write.format('com.databricks.spark.csv').options(header="true").mode('append').save('/root/end'+str(i))
end_time = time.time()
print(end_time-start_time)

