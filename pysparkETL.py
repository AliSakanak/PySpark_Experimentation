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


def extract_movies_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", "movies") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", MYSQL_DRIVER) \
        .load()
    return movies_df

def extract_users_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", "users") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", MYSQL_DRIVER) \
        .load()
    return users_df


def transform_avg_ratings(movies_df, users_df):
    avg_rating = users_df.groupBy("movie_id").mean("rating")

    ##join the movies_df and avg_ratings table on id
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)

    ##change decimal place
    df = df.withColumn("avg(rating)", round(df["avg(rating)"], 1))
    df = df.drop('movie_id')
    return df

def load_df_to_db(df):
    mode = "overwrite"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": MYSQL_DRIVER
    }
    df.write.jdbc(
        url = MYSQL_URL,
        table = "avg_ratings",
        mode = mode,
        properties = properties
        )

if __name__ == "__main__":
    movies_df = extract_movies_df()
    users_df = extract_users_df()
    ratings_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(ratings_df)






