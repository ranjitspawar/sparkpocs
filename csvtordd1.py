from pyspark.sql import *
from pyspark.sql.functions import *
# to use the cast funtion we use below line
from pyspark.sql.types import *
import configparser
from configparser import ConfigParser
conf=ConfigParser()

'''our requirement is to hide all the credentials like host,user,password and which data we are using
hence we created a file named encryptcreds.txt in the location "D:\\data\\encryptcreds.txt"
 and given all the details which we want to hide in that file and saved that file 
 and by using and to do so we have to import certain things those are as follows
import configparser
from configparser import ConfigParser
conf=ConfigParser()
then we have to read that file as shown below and use the credentials as shown and we can also hide the data
we are using same method that is also shown'''



conf.read(r"D:\\data\\encryptcreds.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# data="D:\\bigdata\\datasets\\us500.csv"
data=conf.get("input","data")

ndf=spark.read.format("csv").option("header","true").option("sep","|").option("inferschema","true").load(data)


ndf.show(10)
ndf.printSchema()
# host="jdbc:mysql://database-1.cf4kz4gafarc.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable","us500recs2").option("driver","com.mysql.jdbc.Driver").save()


