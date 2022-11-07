from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
# data=[1,2,3,4,5]
# nrdd=spark.sparkContext.parallelize(data)
# res=nrdd.map(lambda x:x*x)
# for i in res.collect():
#     print(i)


sc=spark.sparkContext
data="D:/bigdata/datasets/asl.csv"
aslrdd=sc.textFile(data)
# res=aslrdd.map(lambda x:x.split(",")).filter(lambda x:x[2]=='hyd')
# res=aslrdd.map(lambda x:x.split(",")).filter(lambda x:"hyd" in x[2])
# res=aslrdd.map(lambda x:x.split(",")).filter(lambda x:x[2]=='blr')
'''
in below query we can see that as there was a header in the data so we have to remove that header by using 
the first filter statement because the age column in the data is a string column and we cant apply operations 
like less than or more than  on string so we removed that header line '''

# res=aslrdd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).filter(lambda x:int(x[1])>=30)

# for i in res.collect():
#     print(i)

'''in below query we can see that we created a dataframe using .toDF() method and then we created a temporary view table that is tab1 
as RDD is only programming model hence we created dataframe and then we can make that as sql friendly to use sql commands '''

# res=aslrdd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).toDF(["name","age","city"])
'''here in below command we grouped the 2nd column in the data and fetching the cities only group
by is used and hence it is appied on top of only any perticular column'''
'''if we want to group the value then first its mandatory to use reduceByKey its used to group the values'''
'''reduceByKey anything function/method that ends with key data must be key-value format'''

res=aslrdd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
for i in res.collect():
    print(i)
# res.createOrReplaceTempView("tab1")
# this view is used to only generate the temporary view and then run sql queries on top of it
# result=spark.sql("select * from tab1 where city ='blr' and age < 30")
# result.show()

# result=res.where((col("city")=="blr")&(col("age")>30))
# result.show()
'''its just that dataframe makes it sql friendly 
'''
