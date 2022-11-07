from pyspark.sql import *
from pyspark.sql.functions import *
# to use the cast funtion we use below line
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\bigdata\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

'''how to show min max values
	describe() its used to give the all values of the min max mean and some more
	df.describe().show'''

# df.describe().show()
# we can give a perticular column also explicitely shown below
# df.describe("amount").show()

'''we want to showcase like 1 year 2 months 12 days have been completed like this from the dtdiff column
so we created a udf then we divided the number of days of dtdiff with 365 days of a year
then we got number of years then we have to get the number of months completed
so we took the remainder of nums and 365 and then divided that with 30 days of a month and then we find out the remaining days
'''
def daystoyrmndays(dtdiff):
    yrs = int(dtdiff / 365)
    mon = int((dtdiff % 365) / 30)
    days = int((dtdiff % 365) % 30)
    result = yrs, "years" , mon , "months" , days, "days"

    st = ' '.join(map(str,result))
    return st

udffunc = udf(daystoyrmndays)


ndf=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today_date",current_date())\
    .withColumn("timestamp",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today_date"),col("dt")))\
    .withColumn("daystoyrmon", udffunc(col("dtdiff")))

ndf.show(truncate=False)


