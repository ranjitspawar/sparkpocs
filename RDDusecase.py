from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data="D:\\bigdata\\datasets\\donations.csv"
nrdd=sc.textFile(data)

'''here in below query we fetched the records in descending order of person who donated the most donations
first we removed the header then we split the data using "," then we only fetched two records those are column 0 and 
column 2 then we used reducebykey 
so that we can aggregate the all the donations of a perticular key and group that  aggregation and then sorted the data 
in ascending order '''

# process=nrdd.filter(lambda x:"name" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).\
#     reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)

'''disinct method is used to get unique records only from the data as shown in below query'''

process=nrdd.filter(lambda x:"name" not in x).map(lambda x:x.split(",")).map(lambda x:x[0]).distinct()

'''in below two queries .map() and .flatmap() methods are shown'''

# process=nrdd.filter(lambda x:"name" not in x).flatMap(lambda x:x.split(","))

# process=nrdd.filter(lambda x:"name" not in x).map(lambda x:x.split(","))

for i in process.collect():
    print(i)