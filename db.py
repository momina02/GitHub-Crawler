"""
db.py
Database utilities for MySQL: create tables, get connection.
Run `python db.py` from workflow to initialize the DB schema.
"""

import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB_NAME = os.getenv("MYSQL_DATABASE", "github_data")


def get_conn():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
    )


def initialize_database():
    # Connect to MySQL server (may need to create database first)
    try:
        cnx = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD
        )
        cursor = cnx.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`;")
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        print("Error creating database:", err)
        raise

    # Create tables
    conn = get_conn()
    cur = conn.cursor()
    create_repos = """
    CREATE TABLE IF NOT EXISTS repos (
        repo_id BIGINT NOT NULL,
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
    cur.execute(create_repos)
    cur.close()
    conn.close()
    print("Database initialized.")


if __name__ == "__main__":
    initialize_database()
