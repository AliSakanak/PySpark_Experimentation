import pymysql
import pandas as pd
from connector_variables import DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT, DB_USER


def connect_to_db():
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
        print("Connected to DB")
        return connection
    except pymysql.DatabaseError as e:
            print(f"\nERROR: Unable to successfully connect to Database.\n{e}")
            print("Please check database connection. Quitting application...\n")
            quit()

def extract_csv_data(csv):
    df = pd.read_csv(csv)
    return df


def create_tables():
    sql_tables = (
        """
        CREATE TABLE IF NOT EXISTS movies (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(55) NOT NULL,
            description VARCHAR(255) NOT NULL,
            category VARCHAR(55) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            movie_id INTEGER NOT NULL,
            rating decimal(2,1),
            CONSTRAINT fk_movies
                FOREIGN KEY(movie_id)
                    REFERENCES movies(id)
        )
        """
    )
    connection = None
    try:
        connection = connect_to_db()
        cursor = connection.cursor()

        for q in sql_tables:
            cursor.execute(q)
        for row in movies_df.itertuples():
            cursor.execute(f"INSERT INTO movies (name, description, category) VALUES (\'{row.name}\', \'{row.description}\', \'{row.category}\')")
        for row in users_df.itertuples():
            cursor.execute(f"INSERT INTO users (movie_id, rating) VALUES (\'{row.movie_id}\', \'{row.rating}\')")
    
        cursor.close()

        connection.commit()

    # except Exception as e:
    #     print(e)
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    movies_df = extract_csv_data("movies_data.csv")
    users_df = extract_csv_data("users_data.csv")
    print("extraction done")
    create_tables()
    print("end")
    


