##import required libraries
import pyspark
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
user_df = spark.read \
   .format("jdbc") \
   .option("url", MYSQL_URL) \
   .option("dbtable", "users") \
   .option("user", DB_USER) \
   .option("password", DB_PASSWORD) \
   .option("driver", MYSQL_DRIVER) \
   .load()

##print the users dataframe
print(user_df.show())




