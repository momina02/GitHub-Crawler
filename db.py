# Importing necessary libraries
import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

# Loads environment variables from a .env file into the program.
load_dotenv()

DB_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB_NAME = os.getenv("MYSQL_DATABASE", "github_data")

# Creates and returns a MySQL connection object.
def get_conn():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
    )

# Creates and initializes the database and required tables if they don’t exist.
def initialize_database():
    # Connect to MySQL server
    try:
        cnx = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD
        )
        # Creates cursor object to execute SQL.
        cursor = cnx.cursor()
        # Creates the database only if it doesn’t exist.
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`;")
        # Closes cursor and connection afterward.
        cursor.close()
        cnx.close()
    # Incase any mysql error occurs
    except mysql.connector.Error as err:
        print("Error creating database:", err)
        raise

    # Create tables
    # Now that the database exists, it connects to the specific database using get_conn().
    # Creates a new cursor for executing SQL statements.
    conn = get_conn()
    cur = conn.cursor()

    # Create the "repos" table
    create_repos = """
    CREATE TABLE IF NOT EXISTS repos (
        repo_id VARCHAR(50) NOT NULL,
        repo_name VARCHAR(512),
        full_name VARCHAR(512),
        html_url VARCHAR(1024),
        description TEXT,
        stars INT,
        forks INT,
        language VARCHAR(128),
        updated_at DATETIME,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (repo_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    # Execute SQL to create the table.
    cur.execute(create_repos)
    
    # Close the cursor and connection.
    cur.close()
    conn.close()
    print("Database initialized.")

if __name__ == "__main__":
    # This is called to ensure database has been created and tables exist.
    initialize_database()
