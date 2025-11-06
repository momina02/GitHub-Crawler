import os
import time
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
from github_api import GitHubGraphQLCrawler
# from github_api import GitHubRestCrawler
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
    crawler = GitHubGraphQLCrawler(token=token)
    # crawler = GitHubRestCrawler(token=token)
    conn = get_conn()
    cur = conn.cursor()

    # Ensure DB exists
    initialize_database()

    # Check if DB already has repos
    cur.execute("SELECT COUNT(*) FROM repos")
    count = cur.fetchone()[0]

    if count == 0:
        print(f"DB empty — fetching initial {TARGET_COUNT} popular repositories...")
        rows_batch = []
        collected = 0
        for repo in tqdm(crawler.fetch_popular_repos(TARGET_COUNT)):
            rows_batch.append(repo)
            collected += 1

            if len(rows_batch) >= BATCH_SIZE:
                upsert_batch(conn, rows_batch)
                print(f"Inserted {collected} repos so far...")
                rows_batch.clear()
                time.sleep(0.5)  # polite pause

            if collected >= TARGET_COUNT:
                break

        # Insert any remaining repos
        if rows_batch:
            upsert_batch(conn, rows_batch)

        print(f"✅ Initial fetch complete — inserted {collected} repositories.")

    # Refresh existing repos
    print("Refreshing existing repositories...")
    cur.execute("SELECT full_name FROM repos")
    all_repos = [row[0] for row in cur.fetchall()]

    rows_batch = []
    refreshed = 0
    for full_name in tqdm(all_repos):
        repo = crawler.fetch_repo_details(full_name)
        if repo:
            rows_batch.append(repo)
            refreshed += 1

        if len(rows_batch) >= BATCH_SIZE:
            upsert_batch(conn, rows_batch)
            rows_batch.clear()
            time.sleep(0.5)

    # Insert any remaining updates
    if rows_batch:
        upsert_batch(conn, rows_batch)

    print(f"✅ Refreshed {refreshed} existing repositories.")

    # Export to CSV
    df = pd.read_sql("SELECT * FROM repos", conn)
    df["description"] = df["description"].fillna("").str.replace("\n", " ").str.replace("\r", " ").str.strip()
    df.to_csv("repos.csv", index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
    print(f"📦 Exported repos.csv with {len(df)} records.")


if __name__ == "__main__":
    main()
