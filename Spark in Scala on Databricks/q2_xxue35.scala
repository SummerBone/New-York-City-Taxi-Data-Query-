// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.

// ENTER THE CODE BELOW

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df_zone = spark.read.option("header",true).csv("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with
df_zone.show(5)

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
var df1=df_filter
df1=df1.groupBy("DOLocationID").count().orderBy(desc("count"),asc("DOLocationID")).withColumnRenamed("count","number_of_dropoffs")
display(df1.limit(5))


// COMMAND ----------

61// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
var dfa2=df_filter
dfa2=dfa2.groupBy("PULocationID").count().orderBy(desc("count"),asc("PULocationID")).withColumnRenamed("count","number_of_pickups")
display(dfa2.limit(5))


// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW
var dfb=df_filter
dfb=df1.join(dfa2,$"PULocationID"===$"DOLocationID","inner").drop($"PULocationID")
dfb=dfb.withColumn("number_activites",$"number_of_dropoffs"+$"number_of_pickups").orderBy(desc("number_activites"),asc("DOLocationID")).drop($"number_of_dropoffs")
dfb=dfb.drop($"number_of_pickups").withColumnRenamed("DOLocationID","LocationID")
display(dfb.limit(3))

// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW
var dfc=df_zone
dfc=dfc.as("dfc").join(dfb.as("dfb"),$"dfc.LocationID"===$"dfb.LocationID","left").select($"dfc.LocationID",$"Borough",$"number_activites").groupBy("Borough").sum("number_activites").withColumnRenamed("sum(number_activites)","total_number_activities").orderBy(desc("total_number_activities"))
display(dfc)


// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW
var df4=df_filter
//get count for each sigle date
df4=df4.select(date_format($"pickup_datetime","yyyy MM dd E").as("date"),$"PULocationID").groupBy("date").count()
//convert string date to datetime 
df4=df4.select(to_date($"date","yyyy MM dd E").as("to_date"),$"count")
//convert datetime to day of week and groupby weekday
df4=df4.select(date_format($"to_date","EEEE").as("day_of_week"),$"count").groupBy("day_of_week").avg("count").withColumnRenamed("avg(count)","avg_count").orderBy(desc("avg_count"))
df4.show(2)

// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW
var df5=df_zone
df5=df5.as("df5").join(df_filter.as("df_filter"),$"df5.LocationID"===$"df_filter.PULocationID","right").select($"Zone",(hour($"pickup_datetime").as("hour_of_day"))).filter($"Borough"==="Brooklyn").groupBy("hour_of_day","Zone").count()
var par=Window.partitionBy('hour_of_day)
var result= df5.select('*,max('count) over par as "max_count").orderBy('hour_of_day).filter('count==='max_count).drop('count)
display(result)



// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float

// Hint: You might need to use lag function, over a window ordered by day of month.


// ENTER THE CODE BELOW

val par = Window.orderBy("day")

val df6 = df_filter.join(df_zone, $"PULocationID"===$"LocationID", "left").withColumn("day", dayofmonth($"pickup_datetime")).
 withColumn("month", date_format($"pickup_datetime", "MM")).filter(($"Borough" ==="Manhattan") && ($"month" === 1))
  .groupBy("day").count().withColumn("lag",lag("count", 1, 0).over(par))

result=df6.withColumn("percent_change", ($"count"-$"lag")*100/$"lag")
  .select("day","percent_change")
  .withColumn("percent_change", round($"percent_change", 2)).orderBy(desc("percent_change"))
 

display(result.limit(3))

