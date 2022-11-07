from pyspark.sql import *
from pyspark.sql.functions import *
# to use the cast funtion we use below line
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
host="jdbc:mysql://database-1.cf4kz4gafarc.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"

'''here we want to fetch the data on the basis of a sql query here t is a temporary 
variable which is compulsory'''

query="(select * from emp where sal>2000 )t"

df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable",query).option("driver","com.mysql.jdbc.Driver").load()
df.show()
