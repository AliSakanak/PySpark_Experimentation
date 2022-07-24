##import required libraries
import pyspark
from connector_variables import MYSQL_JAR, MYSQL_DRIVER, MYSQL_URL

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
   .option("user", "root") \
   .option("password", "kelebek18") \
   .option("driver", MYSQL_DRIVER) \
   .load()

##print the movies_df
print(movies_df.show())




