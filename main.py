import os
import time
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
from github_api import GitHubRestCrawler
from db import get_conn, initialize_database

load_dotenv()

TARGET_COUNT = int(os.getenv("TARGET_COUNT", 100000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))  # number of upserts per DB transaction

def upsert_batch(conn, rows):
    """
    rows: list of dicts with keys matching table columns.
    Uses ON DUPLICATE KEY UPDATE (primary key repo_id).
    """
    if not rows:
        return
    placeholders = ", ".join(["(%s,%s,%s,%s,%s,%s,%s,%s)"] * len(rows))
    sql = """
    INSERT INTO repos
      (repo_id, repo_name, full_name, html_url, description, stars, forks, language, updated_at)
    VALUES
      {}
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
    """.format(placeholders)

    # Flatten values
    val_list = []
    for r in rows:
        val_list.extend([
            r.get("repo_id"),
            r.get("repo_name"),
            r.get("full_name"),
            r.get("html_url"),
            r.get("description"),
            r.get("stars"),
            r.get("forks"),
            r.get("language"),
            r.get("updated_at"),
        ])
    # Note: Because we used 8 placeholders, ensure they match insertion columns: (adjust)
    # To avoid confusion, use executemany instead:
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
    data = []
    for r in rows:
        data.append((
            r.get("repo_id"),
            r.get("repo_name"),
            r.get("full_name"),
            r.get("html_url"),
            r.get("description"),
            r.get("stars"),
            r.get("forks"),
            r.get("language"),
            r.get("updated_at"),
        ))
    cur.executemany(insert_sql, data)
    conn.commit()
    cur.close()


def main():
    token = os.getenv("GITHUB_TOKEN")
    crawler = GitHubRestCrawler(token=token)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT full_name FROM repos")
    all_repos = [row[0] for row in cur.fetchall()]

    print(f"Refreshing {len(all_repos)} existing repos...")

    # If database is empty, perform an initial seed of popular repositories
    if len(all_repos) == 0:
        print("No existing repos found — performing initial seed of popular repositories...")
        # Ensure DB schema exists (harmless if already created)
        try:
            initialize_database()
        except Exception:
            # initialize_database is best-effort; continue even if it fails here
            pass

        seed_batch = []
        inserted = 0
        for repo in tqdm(crawler.fetch_popular_repos(TARGET_COUNT)):
            updated_at = repo.get("updated_at")
            if updated_at:
                updated_at = updated_at.replace("T", " ").replace("Z", "")
            seed_batch.append({
                "repo_id": repo.get("repo_id"),
                "repo_name": repo.get("repo_name"),
                "full_name": repo.get("full_name"),
                "html_url": repo.get("html_url"),
                "description": repo.get("description"),
                "stars": repo.get("stars"),
                "forks": repo.get("forks"),
                "language": repo.get("language"),
                "updated_at": updated_at,
            })

            if len(seed_batch) >= BATCH_SIZE:
                upsert_batch(conn, seed_batch)
                inserted += len(seed_batch)
                print(f"Inserted {inserted} repos so far...")
                seed_batch.clear()
                time.sleep(1)

        if seed_batch:
            upsert_batch(conn, seed_batch)
            inserted += len(seed_batch)
        print(f"✅ Initial seeding complete — inserted {inserted} repositories.")

        # Re-query the table for subsequent refresh step
        cur.execute("SELECT full_name FROM repos")
        all_repos = [row[0] for row in cur.fetchall()]

    # Refresh existing repos
    rows_batch = []
    for full_name in tqdm(all_repos):
        data = crawler.fetch_repo_details(full_name)
        if data:
            rows_batch.append(data)
        if len(rows_batch) >= 100:
            upsert_batch(conn, rows_batch)
            rows_batch = []
    if rows_batch:
        upsert_batch(conn, rows_batch)

    print("✅ All repos updated successfully.")
    
    # Export updated data to CSV for GitHub Actions artifact
    df = pd.read_sql("SELECT * FROM repos", conn)
    df.to_csv("repos.csv", index=False)
    print("📦 Exported repos.csv with", len(df), "records.")


if __name__ == "__main__":
    main()
