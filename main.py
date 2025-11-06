import os
import time
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
from github_api import GitHubRestCrawler
from db import get_conn, initialize_database
from datetime import datetime
import csv

load_dotenv()

TARGET_COUNT = int(os.getenv("TARGET_COUNT", 100000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))  # number of upserts per DB transaction

def upsert_batch(conn, rows):
    if not rows:
        return

    # Convert updated_at to MySQL DATETIME format
    for r in rows:
        updated_at = r.get("updated_at")
        if updated_at:
            try:
                dt = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%SZ")
                r["updated_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                r["updated_at"] = updated_at.replace("T", " ").replace("Z", "")

    cur = conn.cursor()
    insert_sql = """
    INSERT INTO repos
      (repo_id, repo_name, full_name, html_url, description, stars, forks, language, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      repo_name=VALUES(repo_name),
      full_name=VALUES(full_name),
      html_url=VALUES(html_url),
      description=VALUES(description),
      stars=VALUES(stars),
      forks=VALUES(forks),
      language=VALUES(language),
      updated_at=VALUES(updated_at),
      crawled_at=CURRENT_TIMESTAMP
    """
    data = [
        (
            r.get("repo_id"),
            r.get("repo_name"),
            r.get("full_name"),
            r.get("html_url"),
            r.get("description"),
            r.get("stars"),
            r.get("forks"),
            r.get("language"),
            r.get("updated_at"),
        )
        for r in rows
    ]
    cur.executemany(insert_sql, data)
    conn.commit()
    cur.close()

def main():
    token = os.getenv("GITHUB_TOKEN")
    crawler = GitHubRestCrawler(token=token)
    conn = get_conn()
    cur = conn.cursor()
    initialize_database()

    # Check last fetched repo_id to resume if needed
    cur.execute("SELECT MAX(repo_id) FROM repos")
    last_repo_id = cur.fetchone()[0]

    rows_batch = []
    collected = 0

    for repo in tqdm(crawler.list_public_repos(TARGET_COUNT)):
        if last_repo_id and repo["repo_id"] <= last_repo_id:
            continue  # skip already fetched repos

        # Optional: skip repos with zero stars
        if repo.get("stars", 0) == 0:
            continue

        rows_batch.append(repo)
        collected += 1

        if len(rows_batch) >= BATCH_SIZE:
            upsert_batch(conn, rows_batch)
            print(f"Inserted {collected} repos so far...")
            rows_batch.clear()
            time.sleep(0.5)  # polite pause

        if collected >= TARGET_COUNT:
            break

    # Insert remaining
    if rows_batch:
        upsert_batch(conn, rows_batch)

    print(f"✅ Finished fetching {collected} repositories.")

    # Export to comma-separated CSV
    df = pd.read_sql("SELECT * FROM repos", conn)
    df["description"] = df["description"].fillna("").str.replace("\n", " ").str.replace("\r", " ").str.strip()
    df.to_csv("repos.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"📦 Exported repos.csv with {len(df)} records.")

if __name__ == "__main__":
    main()






# import os
# import time
# from dotenv import load_dotenv
# from tqdm import tqdm
# import pandas as pd
# from github_api import GitHubRestCrawler
# from db import get_conn, initialize_database
# from datetime import datetime
# import csv

# load_dotenv()

# TARGET_COUNT = int(os.getenv("TARGET_COUNT", 100000))
# BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))  # number of upserts per DB transaction

# from datetime import datetime

# def upsert_batch(conn, rows):
#     if not rows:
#         return

#     # Convert updated_at to MySQL DATETIME format
#     for r in rows:
#         updated_at = r.get("updated_at")
#         if updated_at:
#             try:
#                 # Convert ISO 8601 to MySQL DATETIME
#                 dt = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%SZ")
#                 r["updated_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
#             except ValueError:
#                 # Fallback: if already converted
#                 r["updated_at"] = updated_at.replace("T", " ").replace("Z", "")

#     cur = conn.cursor()
#     insert_sql = """
#     INSERT INTO repos
#       (repo_id, repo_name, full_name, html_url, description, stars, forks, language, updated_at)
#     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#     ON DUPLICATE KEY UPDATE
#       repo_name=VALUES(repo_name),
#       full_name=VALUES(full_name),
#       html_url=VALUES(html_url),
#       description=VALUES(description),
#       stars=VALUES(stars),
#       forks=VALUES(forks),
#       language=VALUES(language),
#       updated_at=VALUES(updated_at),
#       crawled_at=CURRENT_TIMESTAMP
#     """
#     data = [
#         (
#             r.get("repo_id"),
#             r.get("repo_name"),
#             r.get("full_name"),
#             r.get("html_url"),
#             r.get("description"),
#             r.get("stars"),
#             r.get("forks"),
#             r.get("language"),
#             r.get("updated_at"),
#         )
#         for r in rows
#     ]
#     cur.executemany(insert_sql, data)
#     conn.commit()
#     cur.close()


# def main():
#     token = os.getenv("GITHUB_TOKEN")
#     crawler = GitHubRestCrawler(token=token)
#     conn = get_conn()
#     cur = conn.cursor()
#     cur.execute("SELECT full_name FROM repos")
#     all_repos = [row[0] for row in cur.fetchall()]

#     print(f"Refreshing {len(all_repos)} existing repos...")

#     # If database is empty, perform an initial seed of popular repositories
#     if len(all_repos) == 0:
#         print("No existing repos found — performing initial seed of popular repositories...")
#         # Ensure DB schema exists (harmless if already created)
#         try:
#             initialize_database()
#         except Exception:
#             # initialize_database is best-effort; continue even if it fails here
#             pass

#         seed_batch = []
#         inserted = 0
#         for repo in tqdm(crawler.fetch_popular_repos(TARGET_COUNT)):
#             updated_at = repo.get("updated_at")
#             if updated_at:
#                 updated_at = updated_at.replace("T", " ").replace("Z", "")
#             seed_batch.append({
#                 "repo_id": repo.get("repo_id"),
#                 "repo_name": repo.get("repo_name"),
#                 "full_name": repo.get("full_name"),
#                 "html_url": repo.get("html_url"),
#                 "description": repo.get("description"),
#                 "stars": repo.get("stars"),
#                 "forks": repo.get("forks"),
#                 "language": repo.get("language"),
#                 "updated_at": updated_at,
#             })

#             if len(seed_batch) >= BATCH_SIZE:
#                 upsert_batch(conn, seed_batch)
#                 inserted += len(seed_batch)
#                 print(f"Inserted {inserted} repos so far...")
#                 seed_batch.clear()
#                 time.sleep(1)

#         if seed_batch:
#             upsert_batch(conn, seed_batch)
#             inserted += len(seed_batch)
#         print(f"✅ Initial seeding complete — inserted {inserted} repositories.")

#         # Re-query the table for subsequent refresh step
#         cur.execute("SELECT full_name FROM repos")
#         all_repos = [row[0] for row in cur.fetchall()]

#     # Refresh existing repos
#     rows_batch = []
#     for full_name in tqdm(all_repos):
#         data = crawler.fetch_repo_details(full_name)
#         if data:
#             rows_batch.append(data)
#         if len(rows_batch) >= 100:
#             upsert_batch(conn, rows_batch)
#             rows_batch = []
#     if rows_batch:
#         upsert_batch(conn, rows_batch)

#     print("✅ All repos updated successfully.")
    
#     # Export updated data to CSV for GitHub Actions artifact
    
#     # df = pd.read_sql("SELECT * FROM repos", conn)
#     # df.to_csv("repos.csv", index=False)
#     # print("📦 Exported repos.csv with", len(df), "records.")
    
#     df = pd.read_sql("SELECT * FROM repos", conn)

#     # Optional: clean description/newlines
#     df["description"] = df["description"].fillna("").str.replace("\n", " ").str.replace("\r", " ").str.strip()

#     # Write CSV with proper quoting
#     df.to_csv("repos.csv", index=False, quoting=csv.QUOTE_ALL, escapechar='\\')

#     print("📦 Exported repos.csv with", len(df), "records.")


# if __name__ == "__main__":
#     main()
