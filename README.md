# GitHub Repositories Crawler

## Overall workflow

1. **Setup .env**

   - `.env` contains MySQL and crawler settings.
   - GitHub Actions runner starts MySQL container.

2. **Setup Database**

   - `db.py` ensures the `github_data` database and `repos` table exist.

3. **Fetching**

   - `main.py` (or `init_crawl.py`) calls `GitHubGraphQLCrawler`.
   - Fetches popular repos in batches.
   - Handles pagination with GraphQL cursors.
   - Inserts or updates records in MySQL in batches.

4. **Refreshing**

   - Updates stars, forks, and other fields for existing repos.
   - Keeps `crawled_at` timestamp updated.

5. **Exporting**

   - After fetching/updating, `repos` table exported to CSV (`repos.csv`) via MySQL.
   - Uploaded as artifact in GitHub Actions.

6. **Automation**

   - Workflow runs daily, ensuring the DB and CSV are always up-to-date with GitHub stars info.

### **Visual workflow (simplified)**

```
GitHub Actions Trigger
        |
        v
   MySQL container
        |
        v
   Load .env -> initialize DB (db.py)
        |
        v
   Run main.py -> GitHub GraphQL crawler
        |       -> Fetch popular repos
        |       -> Insert/update MySQL (batch insert)
        |
        v
   Refresh existing repos
        |
        v
   Export MySQL table -> repos.csv
        |
        v
   Upload CSV artifact
```

---

## File Level Workflow
