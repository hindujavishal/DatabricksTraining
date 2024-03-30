# Databricks notebook source
data=[["1","2022-01-01"],["2","2021-02-01"],["3","2020-03-01"]]
df=spark.createDataFrame(data,["id","input"])

# COMMAND ----------

df.printSchema()
display(df)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(current_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ####current_date()
# MAGIC Returns the current date at the start of query evaluation as a DateType column.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select("*",current_date()).show()

# COMMAND ----------

df.select(current_date()).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####current_timestamp
# MAGIC Returns the current timestamp at the start of query evaluation as a TimestampType column.

# COMMAND ----------

df.select("*",current_timestamp().alias("current_time")).show(truncate=False)

# COMMAND ----------

display(df.withColumn("timestamp",current_date()))

# COMMAND ----------

df.withColumn("current time", current_timestamp()).show(truncate= False)

# COMMAND ----------

help(date_format)

# COMMAND ----------

# MAGIC %md
# MAGIC ####date_format()
# MAGIC The below example uses date_format() to parses the date and converts from yyyy-dd-mm to MM-dd-yyyy format.

# COMMAND ----------

df3=df.select(col("input"),date_format(col("input"), "MMMM/dd/yy").alias("New_Date"))
df3.printSchema()
display(df3)

# COMMAND ----------

help(to_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ###to_date()
# MAGIC Below example converts string in date format yyyy-MM-dd to a DateType yyyy-MM-dd using to_date(). You can also use this to convert into any specific format.

# COMMAND ----------

df4=df.select("id", col("input"), to_date(col("input"),'dd-MM-yyyy').alias("datetype"))
display(df4)
df4.printSchema()

# COMMAND ----------

emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
    (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
    (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
    (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
    (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
    (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
    (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
    (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
    (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
    (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])
display(empdf)
empdf.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ###add_months
# MAGIC This function adds months to a date. It will return a new date, however many months from the start date. In the below statement we add 1 month to the column “date” and generated a new column as “next_month”.

# COMMAND ----------

df = (empdf
      .select("date")
    .withColumn("next_month", add_months("date", 2)))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###current_date
# MAGIC This function returns the current date.

# COMMAND ----------

df = (empdf
    .withColumn("current_date", current_date())
    .select("id", "current_date"))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###current_timestamp
# MAGIC This function returns the current timestamp.

# COMMAND ----------

df = (empdf
    .withColumn("current_timestamp", current_timestamp())
    .select("id", "current_timestamp"))
df.show(2,False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_add
# MAGIC This function returns a date x days after the start date passed to the function. In the example below, it returns a date 5 days after “date” in a new column as “next_date”. E.g. for date: 1st Feb 2019 it returns 6th Feb 2019.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("next_date", date_add("date", 5)))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_format
# MAGIC This function will convert the date to the specified format. For example, we can convert the date from “yyyy-MM-dd” to “dd/MM/yyyy” format.

# COMMAND ----------



# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("new_date", date_format("date", "dd-MMMM-yy")))
df.show(2)

# COMMAND ----------

df.display()

# COMMAND ----------

df2=df.withColumn("new_Date2",to_date("date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_sub
# MAGIC This function returns a date some number of the days before the date passed to it. It is the opposite of date_add. In the example below, it returns a date that is 5 days earlier in a column as “new_date”. For example, date 1st Feb 2019 returns 27th Jan 2019.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("new_date", date_sub("date", 5)))
df.show(2)

# COMMAND ----------

help(date_trunc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_trunc
# MAGIC This function returns a timestamp truncated to the specified unit. It could be a year, month, day, hour, minute, second, week or quarter.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("new_date", date_trunc("day", "date")))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### datediff
# MAGIC This function returns the difference between dates in terms of days. Let’s add another column as the current date and then take the difference between “current_date” and “date”.

# COMMAND ----------

df = (empdf.select("date")
        # Add another date column as current date.
        .withColumn("current_date", current_date()) 
        # Take the difference between current_date and date column.
        .withColumn("date_diff", months_between("current_date", "date"))) 
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### dayofmonth
# MAGIC This function returns the day of the month. For 5th Jan 2019 (2019–01–05) it will return 5.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("dayofmonth", dayofmonth("date")))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dayofweek
# MAGIC This function returns the day of the week as an integer. It will consider Sunday as 1st and Saturday as 7th. For 1st Feb 2019 (2019–02–01) which is Friday, it will return 6. Similarly, for 1st April 2018 (2018–04–01) which is Sunday, it will return 1.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("dayofweek", dayofweek("date")))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### dayofyear
# MAGIC This function returns the day of the year as an integer. For 1st Feb it will return 32 (31 days of Jan +1 day of Feb). For 1st April 2018, it will return 91 (31 days of Jan + 28 days of Feb (2018 is a non-leap year) + 31 days of Mar + 1 day of April).

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("dayofyear", dayofyear("date")))
df.show()

# COMMAND ----------

help(from_utc_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### from_utc_timestamp
# MAGIC This function converts UTC timestamps to timestamps of any specified timezone. By default, it assumes the date is a UTC timestamp.
# MAGIC Let's convert a UTC timestamp to “PST” time.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("pst_timestamp", from_utc_timestamp("date", "PST")))
df.show(2)

# COMMAND ----------

help(unix_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### unix_timestamp
# MAGIC This function converts timestamp strings of the given format to Unix timestamps (in seconds). The default format is “yyyy-MM-dd HH:mm:ss”. (Note: You can use spark property: “spark.sql.session.timeZone” to set the timezone.)

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
time_df = spark.createDataFrame([('2015-04-08',)], ['dt'])
time_df.select(unix_timestamp('dt', 'yyyy-MM-dd').alias('unix_time')).collect()


# COMMAND ----------

spark.conf.unset("spark.sql.session.timeZone")

# COMMAND ----------

empdf.select(unix_timestamp("date", "yyyy-MM-dd")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###from_unixtime
# MAGIC This function converts the number of seconds from Unix epoch (1970–01–01 00:00:00 UTC) to a given string format. You can set the timezone and format as well. (Note: you can use spark property: “spark.sql.session.timeZone” to set the timezone). For demonstration purposes, we have converted the timestamp to Unix timestamp and converted it back to timestamp.

# COMMAND ----------

df = (empdf
    .select("date")
    # Convert timestamp to unix timestamp.
    .withColumn("unix_timestamp", unix_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
    # Convert unix timestamp to timestamp.
    .withColumn("date_from_unixtime", from_unixtime("unix_timestamp")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####hour
# MAGIC This function will return the hour part of the date.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("hour", hour("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### last_day
# MAGIC This function will return the last date of the month for a given date. For 5th Jan 2019, it will return 31st Jan 2019, since this is the last date for the month.

# COMMAND ----------

df = empdf.select("date").withColumn("last_date", last_day("date")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### month
# MAGIC This function will return the month part of the date.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("month", month("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### months_between
# MAGIC This function returns the difference between dates in terms of months. If the first date is greater than the second one, the result will be positive else negative. For example, between 6th Feb 2019 and 5th Jan 2019, it will return 1.

# COMMAND ----------

df = (empdf
    .select("date")
    # Add another date column as current date.        
    .withColumn("current_date", current_date()) 
    # Take the difference between current_date and date column in terms of months.
    .withColumn("months_between", months_between("current_date", "date"))) 
df.show(2)

# COMMAND ----------

help(next_day)

# COMMAND ----------

# MAGIC %md
# MAGIC ### next_day
# MAGIC This function will return the next day based on the dayOfWeek specified in the next argument. For e.g. for 1st Feb 2019 (Friday) if we ask for next_day as Sunday, it will return 3rd Feb 2019.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("next_day", next_day("date", "sun")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### quarter
# MAGIC This function will return a quarter of the given date as an integer.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("quarter", quarter("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### second
# MAGIC This function will return the second part of the date.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("second", second("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### to_date
# MAGIC This function will convert the String or TimeStamp to Date.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("to_date", to_date("date")))
df.show(2)

# COMMAND ----------

Note: Check the data type of column “date” and “to-date”.

If the string format is ‘yyyy-MM-dd HH:mm:ss’ then we need not specify the format. Otherwise, specify the format as the second arg in to_date function.

# COMMAND ----------

Here we convert a string of format ‘dd/MM/yyyy HH:mm:ss’ to “date” data type. Note the default format is ‘yyyy-MM-dd`.

# COMMAND ----------

df1 = spark.createDataFrame([('15/02/2019 10:30:00',)], ['date'])
df2 = (df1
    .withColumn("new_date", to_date("date", 'dd/MM/yyyy HH:mm:ss')))    
df2.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### to_timestamp
# MAGIC This function converts String to TimeStamp. Here we convert a string of format ‘dd/MM/yyyy HH:mm:ss’ to the “timestamp” data type. The default format is ‘yyyy-MM-dd HH:mm:ss’

# COMMAND ----------

df1 = spark.createDataFrame([('15/02/2019 10:30:00',)], ['date'])
df2 = (df1
    .withColumn("new_date", to_timestamp("date", 'dd/MM/yyyy HH:mm:ss')))
df2.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### to_utc_timestamp
# MAGIC This function converts given timestamp to UTC timestamp. Let's convert a “PST” timestamp to “UTC” timestamp.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("utc_timestamp", to_utc_timestamp("date", "PST")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### weekofyear
# MAGIC This function will return the weekofyear for the given date.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("weekofyear", weekofyear("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### year
# MAGIC This function will return the year part of the date.

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("year", year("date")))
df.show(2)
