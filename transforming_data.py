##import required libraries
import pyspark.sql
from pyspark.sql.functions import round
from connector_variables import DB_USER, MYSQL_DRIVER, MYSQL_JAR, MYSQL_URL, DB_PASSWORD

##create spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.driver.extraClassPath', MYSQL_JAR) \
    .getOrCreate()


##read table from db using spark jdbc
movies_df = spark.read \
    .format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", "movies") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", MYSQL_DRIVER) \
    .load()

##add code below
users_df = spark.read \
    .format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", "users") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", MYSQL_DRIVER) \
    .load()


## transforming tables
avg_rating = users_df.groupBy("movie_id").mean("rating")

##join the movies_df and avg_ratings table on id
df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)

##change decimal place
df = df.withColumn("avg(rating)", round(df["avg(rating)"], 1))

##print all the tables/dataframes
print(movies_df.show())
print(users_df.show())
print(df.show())
print(df.dtypes)





