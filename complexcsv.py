from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\bigdata\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
'''in order to show all the records we use a variable and gives that a count funcion and calls that variable 
in the show function that shows all the records available'''




'''there are some spaces and special characters are present in the schema so in order to remove that we are importing 
regular expression package
and then we are removing all the characters other than a-z A-Z 0-9 we are replacing them with 
"" empty i.e. we are joining them
and to make all the fields name as lower 
we use .lower method 
toDF used to rename all columns ,and convert rdd to dataframe ... at that time use toDF'''

import re
# cols=[re.sub(' ',"",c.lower()) for c in df.columns]

cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]

ndf=df.toDF(*cols)
res=ndf.groupBy(col("gender")).agg(count(col("*")).alias("cnt"))
res.show()

# num = int(df.count())
# df.show(num,truncate=True)
# df.show(num,5,truncate=False)

'''show function by default shows only 20 rows and it only shows fields having less than 20 characters only if the 
character size increases more than 20 then it show ...in this fashion so in order to show all the character 
which are even more than 20 characters then we use truncate = True'''
# ndf.printSchema()
res.printSchema()
