from pyspark.sql import *
from pyspark.sql.functions import *
# to use the cast funtion we use below line
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data="D:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load(data)

import re

cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]

ndf=df.toDF(*cols)
ndf.show(10)
ndf.printSchema()
host="jdbc:mysql://database-1.cf4kz4gafarc.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","thousandrecs").option("driver","com.mysql.jdbc.Driver").save()


