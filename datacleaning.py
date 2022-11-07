from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data="D:\\bigdata\\datasets\\us-500.csv"

df=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)
# df.show()
# df.printSchema()


#in below query we  grouped the state column and also used count method on it

# ndf=df.groupBy(col("state")).agg((count("*")).alias("cnt")).orderBy(col("cnt").desc())
# ndf.show()

'''wihtcolumn is used when we have to add a new column or updae a exitsting column if the column is not present
in the data then the column will be created and if the column exits already then it will be updated 
and lit() is used to give a dummy value to all the rows in tha tperticular column'''
# ndf=df.withColumn("fullname",lit("mr/ms"))

'''now we are using the concat_ws() for concatenating purpose,here in concat_ws there is " " that is separator
mentioned and then the first name for concatenation and then there is second name for concatenation'''

# ndf=df.withColumn("fullname",concat_ws(" ",df.first_name,df.last_name))

'''there were some - in the phone1 column so in order to remove those we used regexp_replace and 
removed "-"with "" empty that is that meaning'''

# ndf=df.withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType()))
'''here if we notice that if we dont use cast function then there is datatype of the phone1 and phone2 
column is string and hence we have to change that to long hence we used cast function'''

'''now we want to show only few columns in the display rather than showing all the columnsfor that we use 
drop function as given below,here we are droping email,zip,and web'''

# ndf=df.withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType())).drop("email","zip","web")

'''in order to rename some existing columns with the new name then we use below command'''
# ndf=df.withColumn("phone1",regexp_replace(col("phone1"),"-","")).withColumnRenamed("first_name","fname")

'''collect_list(Column e)
Aggregate function: returns a list of objects with duplicates
here we get the result in the form of list if we jsut dont want the count but also we want the names as well
then we use collect_list funtion'''
# ndf=df.groupBy(col("state")).agg((count("*")),collect_list(df.first_name))

'''as collect_list gives the list with all the duplicates as well hence to get only the unique records 
we use collect_set() with this duplicated records will be eliminated'''

# ndf=df.groupBy(col("state")).agg(count(col("city")),collect_set(df.city).alias("unique_cities"))

'''when condition
    here we use when condition to filter out the results when this then give this value otherwise give this 
    value in this fashion'''
# ndf=df.withColumn("state",when(col("state")=="NY","NewYork").when(col("state")=="CA","Cali").otherwise(col("state"))).drop("address").drop("country").drop("email").drop("web").drop("zip").drop("phone2").drop("phone1")

# ndf=df.withColumn("first_name",when(col("first_name")=="Josephine","bhausaheb").when(col("first_name")=="James","dadasaheb").otherwise(col("first_name"))).drop("address").drop("country").drop("email").drop("web").drop("zip").drop("phone2").drop("phone1")

'''regexp_replace only repalces the perticular element from the data but if you see closely
when and contains removes the total element containg the perticular element means a word conatins 
a perticular letter and we want to remove that then with the help of regexp_replace we can do that 
but if we have to remove that total word/field detail then we use when and contain statement as shown 
in below query'''

# ndf=df.withColumn("address1",when(col("address").contains("#"),"****").otherwise(col("address")))\
#       .withColumn("address2",regexp_replace(col("address"),"#","_"))\
#       .drop("country").drop("email")\
#       .drop("web").drop("zip").drop("phone2").drop("phone1").drop("city").drop("county")\
#       .drop("last_name").drop("company_name").drop("state")



'''substring is a function which is used to extract something from the given field and hence here we mwntioned that we want the 
element from 0th position to 5th position that means it will exclude the 5th position and will return the elements
from 0th [position to 4th position 
but we have to extract a perticular field with the help of delimiter then we use substring_index which helps in doing so
here we mentioned the delimiter as @ and 1 as count hence we can see that the we are getting result in the form of username
means we are geing all the elements which are to the left of delimiter @ 
and if we use count as -1 then we get the result in the form of mail means we get all the elements which are to the right
of delimiter @ as shown below'''

# ndf1=df.withColumn("substr",substring(col("email"),0,5)).withColumn("username",substring_index(col("email"),"@",1))\
#     .withColumn("mail",substring_index(col("email"),"@",-1))
'''here in below query we can see that using groupby and count we get that how many
people are using which mail engines'''
# ndf=ndf1.groupBy("mail").count().orderBy(col("count").desc())







'''in order to show all the records we use num in this fashion as by default spark only 
show upto 20 records only'''
# num=int(ndf.count())
# ndf.show(num)

'''spark does not understand the python functions spark only understands udf and if there is any requirement 
 to create any function in spark then we have to create python function according to requirement then
 we have to convert that into udf and then we can use it'''

def funct(st):
    if(st=="NY"):
        return "30% off"
    elif(st=="OH"):
        return "20% off"
    elif(st=="CA"):
        return "10% off"
    else:
        return "5% off"
'''here we are converting the pyhton function into pyspark udf'''
uf=udf(funct)
ndf=df.withColumn("offer",uf(col("state")))

ndf.show(truncate=False)
ndf.printSchema()
