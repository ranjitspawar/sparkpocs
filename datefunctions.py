from pyspark.sql import *
from pyspark.sql.functions import *
# to use the cast funtion we use below line
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone","EST").getOrCreate()

'''TO GET THE USA DATE IN CURRENT DATE FUNCTION WE HAVE TO CONFIG THAT IN THE SPARK SESSION 
.config("spark.sql.session.timeZone","EST") in this fashion so that it will show current date from USA timezone'''


data="D:\\bigdata\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
'''datefunctions:
spark only understands by default date format that is yyyy-MM-dd
but in the data date format is different and datatype is string hence we used to_date function to convert 
the date format and datatype
to_date converts the input date format to yyyy-MM-dd date format that is spark date format
'''
'''current_date function is used to get the current date from the system '''
'''current_timestamp is used to get the current time in minutes and seconds and hours'''
'''date_diff is a function which is used to get the difference between two dates'''
'''date_add is a function which is used to add certain amount of days into a perticular
date columns therefore we can see that in the example below we added the 10 days into the 
dt column and there is change in the added_days with 10 days'''
'''date_sub is used to substract the certain amount of days same as of date_add
note:date_add -100 and date_sub 100 both gives the same result as both means the same'''
'''last_day it return last_day of the month is a function which is used to get the last day of the month for that perticular mentioned
date column,here we can see that in results that the january has last date 31 so it it 
showing 31 jan in last_date column and in same manner for the other date as well'''
'''next_day it is used to get the perticular next day here we want to find out the next sunday from todays 
date column so it gives the next sunday'''

'''date_format() is used to get the in our required format default format is yyyy-mm-dd bu we want the date 
format to be dd/MM/yy then at that time we use the date_format() as shown below 
use MM for numerical months and use MMMM for alphabetical months as shown
use E for day of the week like sunday monday 
use VV to show the time-zone'''

'''in order to find the last friday of the month we used multiple functions those are as follows
first we got the last_day function that gives us the last day of the month then we then we used the date_sub
that subtracts the 7 day out of last day of the month then we used the next_day function to get the next friday
in this manner we do the adjustment we dont have any function to find the last day of the month so we join
multiple function to get the required the o/p see 'monlastfri' '''

'''dayofweek gives the day number from the sunday means the sunday is 1 and the saturday is 7,
dayofmonth is used to get the number of days fromm the 1 st of date of that month means the if the date is 
18 then the dayofmonth willl give result as 18,
dayofyear is used to give the date from the 1st of jan means it counts the days from 1st of jan of that year'''
'''months_between its used to get the months in between the two dates'''
'''floor is used to get lower limit of any floating number as there is in monthsbetw column
no matter waht value is there it take lower limit only whether its .1 or .9'''
'''ceil is used to get the upper value same as floor'''
'''round is used to get the round value if the number is less than .5 then it takes lower limit if the 
value is more than 0.5 then it takes the upper limit'''
'''here we used cast to convert the double to integer'''
'''datetruncate is used to truncate the date and we have to mention that what we have to truncate 
 year,month,day if we truncate year then date gets to the 1 of jan,
 if we truncate the month then date comes to that months first date i.e 1 st of the month, and if we truncate 
 day then it comes to 1 st hour of that day '''
'''weekofyear its used to get the week number from that entire year'''




ndf=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today_date",current_date())\
    .withColumn("timestamp",current_timestamp())\
    .withColumn("datedifference",datediff(col("today_date"),col("dt")))\
    .withColumn("added_days",date_add(col("dt"),10))\
    .withColumn("subtracted_days",date_sub(col("dt"),10))\
    .withColumn("lastdate",last_day(col("dt")))\
    .withColumn("next_day",next_day(col("today_date"),"Sunday"))\
    .withColumn("new_date",date_format(col("dt"),"dd/MM/yy"))\
    .withColumn("new_date1",date_format(col("dt"),"dd/MMMM/yy"))\
    .withColumn("new_date2",date_format(col("dt"),"dd/MMMM/yy/E/VV"))\
    .withColumn("monlastfri",next_day(date_sub(last_day(col("today_date")),7),"friday"))\
    .withColumn("dayofweek",dayofweek(col("dt")))\
    .withColumn("dayofmonth",dayofmonth(col("dt")))\
    .withColumn("dayofyear",dayofyear(col("dt")))\
    .withColumn("monthsbetw",months_between(col("today_date"),col("dt")))\
    .withColumn("floor",floor(col("monthsbetw")))\
    .withColumn("ceil",ceil(col("monthsbetw")))\
    .withColumn("round",round(col("monthsbetw")).cast(IntegerType()))\
    .withColumn("datetrunc",date_trunc("year",col("dt")))\
    .withColumn("datetrunc1",date_trunc("month",col("dt")))\
    .withColumn("datetrunc2",date_trunc("day",col("dt")))\
    .withColumn("weekofyear",weekofyear(col("dt")))








# rows=ndf.count()
# print(f"the total number of rows are: {rows}")

ndf.printSchema()
ndf.show(truncate=False)
