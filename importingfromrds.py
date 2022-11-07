from pyspark.sql import *
from pyspark.sql.functions import *
# to use the cast funtion we use below line
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
host="jdbc:mysql://database-1.cf4kz4gafarc.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").load()
# df.show()

'''in order to replace the null values in the data with some value we use following method'''
# res=df.na.fill(0)             #here na is nothing but null all
# res=df.na.fill(0,["comm"]).withColumn("comm",col("comm").cast(IntegerType()))
'''we can mention the column in which want to do the operation of replacing the nulls and also cast that value'''

'''here we want to change the date format as per client requirement'''
res=df.withColumn("hiredate",date_format(col("hiredate"),"yyyy/MMM/dd"))


res.printSchema()
res.show()

'''in order to store this cleaned data into a new staging table into RDS we use write operation as shown 
below '''

'''we cant perform both read and write operation at same time with the same table we have to use staging table
in ETL we have to use staging table as because of deadlock
A deadlock is a situation where different transactions are unable to proceed because each holds a lock that the 
other needs. Because both transactions are waiting for a resource to become available, neither ever release the 
locks it holds. thats why staging table came into picture
if we use the read and write operation at the same time then entire data gets deleted'''

res.write.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","empcleaned").option("driver","com.mysql.jdbc.Driver").save()

