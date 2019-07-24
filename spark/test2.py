import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import Row,functions as F
from pyspark.sql.functions import desc,  from_unixtime
import datetime
import time
from pyspark import  SparkContext
start_time =time.time()
os.environ['JAVA_HOME'] = '/root/jdk1.8.0_141'
spark = SparkSession \
    .builder \
    .appName('my_second_app_name') \
    .getOrCreate()
file = r'/root/20170928.txt'

df = spark.read.csv(file,header=True,inferSchema=True)
df['time'] = df['time'].apply(lambda x:time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')))
df.withColumn("new_time", lambda x:time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')))
df.show()
df.coalesce(1).write.format('com.databricks.spark.csv').options(header="true").mode('append').save('start')
df.show()