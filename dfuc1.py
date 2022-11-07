from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("sparkdataframe").getOrCreate()

'''in order to read any data we use following way,header is by default false hence we mentioned header true to take header 
seperately,if header true is mentioned then first line of the data is considered as column names '''


# data="D:\\bigdata\\datasets\\donations.csv"
# df=spark.read.format("csv").option("header","true").load(data)
# df.show()

# data="D:\\bigdata\\datasets\\donations1.txt"
# df=spark.read.format("csv").option("header","true").load(data)
# df.show()
'''in above query we do not get expected o/p as there is extra line in the data at the top therefore 
there is different o/p'''
'''if we have some mal data then we do below procedure'''
'''hence we use below approach i.e. first we convert the data into rdd textfile then we skip the header using first()
then we take only those lines from the data other than first line i.e. using filter and lambda and then we create a 
dataframe 
printschema is used to show the schema of the table i.e. columns name and their datatypes'''

# nrdd=spark.sparkContext.textFile(data)
# skip=nrdd.first()
# odata=nrdd.filter(lambda x:x!= skip)
# df=spark.read.csv(odata,header=True,inferSchema=True)
# '''by default inferschema is false mentioning inferschema true means automatically it converts
# the data into appropriate datatypes,here first it was string and now its integer'''
# df.printSchema()
# df.show()


# another usecase
'''sep option is used to specify the delimiter as in data there was ; as delimiter therefore we have to 
explicitly mention that using sep option default seperator is , if the delimiter is different in the data then we have 
to mention that explicitly '''


data="D:\\bigdata\\datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").load(data)

# df.printSchema()
# df.show()

'''on top of this dataframe we can run sql queries '''
# res = df.where((col("age")>50) & (col("marital")!="married")

# in order to get only few or required columns details use select statement
# res = df.select(col("age"),col("job"),col("loan")).where((col("age")>50)\
#       & (col("marital")!="married"))

# this is python friendly groupby method

# res =df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy("smb")

#process sql friendly
df.createOrReplaceTempView("tab")
# #createOrReplaceTempView .. register this dataframe as a table. its very useful to run sql queries.
# # res=spark.sql("select * from tab where age>60 and balance>50000")

#this is sql frienly groupby method

res=spark.sql("select marital, sum(balance) sumbal from tab group by(marital)")
'''here we sorted by marital status and used sum function to aggregate the balance column'''
res.show()
res.printSchema()
