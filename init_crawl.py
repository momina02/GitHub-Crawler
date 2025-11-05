# from github_api import GitHubRestCrawler
# from db import get_conn
# from tqdm import tqdm
# import pandas as pd
# import os
# from dotenv import load_dotenv

# load_dotenv()

# TARGET_COUNT = int(os.getenv("TARGET_COUNT", 10000))
# crawler = GitHubRestCrawler(token=os.getenv("GITHUB_TOKEN"))
# conn = get_conn()

# rows = []
# # for repo_data in tqdm(crawler.iter_repos(TARGET_COUNT)):
# # for repo_data in tqdm(crawler.list_public_repos(TARGET_COUNT)):

# popular_repos = crawler.fetch_popular_repos(TARGET_COUNT)
# for repo_data in tqdm(popular_repos):
#     rows.append(repo_data)


# # Save initial data to MySQL
# cur = conn.cursor()
# insert_sql = """
# INSERT INTO repos (repo_id, repo_name, full_name, html_url, description, stars, forks, language, updated_at)
# VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
# ON DUPLICATE KEY UPDATE
#   stars=VALUES(stars),
#   forks=VALUES(forks),
#   updated_at=VALUES(updated_at),
#   crawled_at=CURRENT_TIMESTAMP
# """
# data = [
#     (
#         r.get("repo_id"),
#         r.get("repo_name"),
#         r.get("full_name"),
#         r.get("html_url"),
#         r.get("description"),
#         r.get("stars"),
#         r.get("forks"),
#         r.get("language"),
#         r.get("updated_at"),
#     )
#     for r in rows
# ]
# cur.executemany(insert_sql, data)
# conn.commit()
# cur.close()
# print(f"✅ Inserted {len(rows)} initial repos into database.")


from github_api import GitHubRestCrawler
from db import get_conn, initialize_database
from tqdm import tqdm
import os
from dotenv import load_dotenv
import time
from datetime import datetime

# Load environment variables
load_dotenv()

TARGET_COUNT = int(os.getenv("TARGET_COUNT", 100000))
BATCH_SIZE = 1000

# Ensure DB and table exist
initialize_database()

# Initialize crawler and DB connection
crawler = GitHubRestCrawler(token=os.getenv("GITHUB_TOKEN"))
conn = get_conn()
cur = conn.cursor()

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
batch = []
count = 0

for repo in tqdm(crawler.fetch_popular_repos(TARGET_COUNT)):
    # ✅ Fix timestamp format
    updated_at = repo.get("updated_at")
    if updated_at:
        updated_at = updated_at.replace("T", " ").replace("Z", "")
        # Or use datetime for better handling:
        # updated_at = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%SZ")
    else:
        updated_at = None

    batch.append((
        repo["repo_id"],
        repo["repo_name"],
        repo["full_name"],
        repo["html_url"],
        repo["description"],
        repo["stars"],
        repo["forks"],
        repo["language"],
        updated_at,   # 👈 use the cleaned timestamp here
    ))

    # Insert after each batch of 1000
    if len(batch) >= BATCH_SIZE:
        cur.executemany(insert_sql, batch)
        conn.commit()
        count += len(batch)
        print(f"✅ Inserted {count} repos so far...")
        batch.clear()
        time.sleep(1)  # short pause to respect API rate limits

# Insert any remaining repos
if batch:
    cur.executemany(insert_sql, batch)
    conn.commit()
    count += len(batch)

cur.close()
conn.close()
print(f"🎯 All {count} repositories inserted successfully into the database!")
