# Importing necessary libraries
from github_api import GitHubGraphQLCrawler
from db import get_conn, initialize_database
from tqdm import tqdm
import os
from dotenv import load_dotenv
import time
from datetime import datetime

# Load environment variables
load_dotenv()

# Number of repos to fetch, default 100000
TARGET_COUNT = int(os.getenv("TARGET_COUNT", 100000))
# Number of records to insert in db at a time
BATCH_SIZE = 1000

# Ensure DB and table exist
initialize_database()

# Initialize crawler and DB connection
crawler = GitHubGraphQLCrawler(token=os.getenv("GITHUB_TOKEN"))
conn = get_conn()
cur = conn.cursor()

# Sql query for inserting or updating repo data
insert_sql = """
INSERT INTO repos (repo_id, repo_name, full_name, html_url, description, stars, forks, language, updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  stars=VALUES(stars),
  forks=VALUES(forks),
  updated_at=VALUES(updated_at),
  crawled_at=CURRENT_TIMESTAMP
"""

# Start fetching repos
print(f"🚀 Starting to collect up to {TARGET_COUNT} popular repositories...")

# temporary list to save repos before inserting in bulk.
batch = []
# Tracks total repositories inserted so far.
count = 0

# Loops through the repositories fetched from GitHub using your crawler.
for repo in tqdm(crawler.fetch_popular_repos(TARGET_COUNT)):
    # ✅ Fix timestamp format
    updated_at = repo.get("updated_at")
    if updated_at:
        updated_at = updated_at.replace("T", " ").replace("Z", "")
    else:
        updated_at = None

    # Adds the repository data as tuple to the batch list.
    batch.append((
        repo["repo_id"],
        repo["repo_name"],
        repo["full_name"],
        repo["html_url"],
        repo["description"],
        repo["stars"],
        repo["forks"],
        repo["language"],
        updated_at,
    ))

    # Insert after each batch of 1000
    if len(batch) >= BATCH_SIZE:
        cur.executemany(insert_sql, batch)
        conn.commit()
        count += len(batch)
        print(f"✅ Inserted {count} repos so far...")
        batch.clear()
        time.sleep(30)  # short pause to respect API rate limits

# Insert any remaining repos
if batch:
    cur.executemany(insert_sql, batch)
    conn.commit()
    count += len(batch)

# Close cursor and connection
cur.close()
conn.close()
print(f"🎯 All {count} repositories inserted successfully into the database!")
