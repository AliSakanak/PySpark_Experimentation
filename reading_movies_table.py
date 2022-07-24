##import required libraries
import pyspark
from PySpark_Experimentation.connector_variables import DB_PASSWORD, DB_USER, MYSQL_DRIVER, MYSQL_JAR, MYSQL_URL

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

##print the movies_df
print(movies_df.show())




