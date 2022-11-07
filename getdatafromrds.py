from pyspark.sql import *
from pyspark.sql.functions import *
# to use the cast funtion we use below line
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false"
df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").load()
df.show()