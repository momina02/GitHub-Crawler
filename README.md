# 🕸️ GitHub Repositories Crawler 

A fully automated system that crawls **popular GitHub repositories**, stores them in **MySQL**, refreshes them daily, and exports a clean **CSV snapshot** — all powered by **GitHub Actions + GraphQL API**.

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![GitHub API](https://img.shields.io/badge/GitHub%20API-181717?style=for-the-badge&logo=github&logoColor=white)
![Requests](https://img.shields.io/badge/Requests-000000?style=for-the-badge&logo=python&logoColor=white)
![JSON](https://img.shields.io/badge/JSON-333333?style=for-the-badge&logo=json&logoColor=white)
![Automation](https://img.shields.io/badge/Automation-FF6F00?style=for-the-badge)
![Data%20Engineering](https://img.shields.io/badge/Data%20Engineering-4CAF50?style=for-the-badge)

## 🔥 Tech Stack

**Languages & Tools:**
`Python` • `GitHub GraphQL API` • `MySQL` • `GitHub Actions` • `Automation` • `Data Engineering` • `Docker` • `CSV Export`

## 🎯 What This Project Does

This crawler automatically:

* Fetches **popular repositories** from GitHub using the **GraphQL API**
* Stores & updates repo metadata in **MySQL**
* Handles **pagination**, **batch inserts**, and **refresh cycles**
* Exports the final dataset as **repos.csv**
* Runs **daily** with full automation via **GitHub Actions**

# ⚙️ Overall Workflow

1. **🧩 Setup `.env`**

   * Contains MySQL credentials + crawler configs
   * GitHub Actions runner initializes MySQL Docker container

2. **🛢️ Database Setup**

   * `db.py` ensures `github_data` DB + `repos` table exists
   * Auto-creates DB & schema if missing

3. **🕷️ Fetching Repos**

   * `main.py` / `init_crawler.py` triggers `GitHubGraphQLCrawler`
   * Fetches repos in **6-month intervals** (star-based search)
   * Uses **GraphQL cursors** for pagination
   * Inserts/updates rows in **batch mode**

4. **🔄 Refreshing Existing Data**

   * Updates stars, forks, timestamps
   * Keeps `crawled_at` always fresh

5. **📤 Exporting**

   * Full `repos` table exported to `repos.csv`
   * CSV uploaded as Actions **artifact**

6. **🤖 Automation**

   * Daily GitHub Actions run
   * Ensures fresh DB + updated CSV snapshot

# 🧭 Visual Workflow (Simplified)

```
GitHub Actions Trigger
        |
        v
   MySQL Container
        |
        v
Load .env -> initialize DB (db.py)
        |
        v
Run main.py -> GitHub GraphQL Crawler
        |       -> Fetch popular repos
        |       -> Batch insert/update MySQL
        |
        v
Refresh existing repos
        |
        v
Export MySQL table -> repos.csv
        |
        v
Upload CSV Artifact
```

---

# 📁 File-Level Workflow

### **📌 db.py**

* Loads env variables (host, port, user, password, DB)
* Creates the MySQL DB + `repos` table
* Provides `get_conn()` for other modules
* Ensures clean schema creation

### **📌 github_api.py**

* Contains `GitHubGraphQLCrawler`
* Handles **authentication**, **GraphQL queries**, **pagination**
* `fetch_popular_repos()` retrieves repos in 6-month intervals
* `fetch_repo_details()` updates a single repo
* Yields structured repository metadata

### **📌 init_crawler.py**

* Bulk fetch + batch insert logic
* Builds formatted MySQL rows
* Handles API rate limiting
* Ideal for initial large-scale data ingestion

### **📌 main.py**

* Main automation runner
* Detects if DB is empty → performs initial crawl
* Refreshes existing repos (stars, forks, updated_at)
* Cleans & exports `repos.csv`

### **📌 crawler.yaml (GitHub Actions)**

* Runs daily or manually
* Starts MySQL container
* Creates `.env` automatically
* Initializes DB + runs crawler
* Exports & uploads CSV artifact

# 📊 Summary

This system:

* 🛢️ Creates & manages a MySQL repo database
* 🔗 Fetches GitHub repos via **GraphQL**
* 🧱 Stores + updates records in batches
* 🕒 Refreshes daily using **GitHub Actions**
* 📤 Outputs a clean CSV snapshot of  popular repos

Fully automated. Zero manual effort.

# 🚀 Future Enhancements

### **📌 Scaling to 500,000+ Repositories**

* Parallel scripts for different:

  * date ranges
  * languages
  * star ranges
* Split workloads to accelerate crawling
* Add async tasks + queue-based operations

### **📌 Future Database Schema**

* Break into multiple tables:

  * `repos`
  * `issues`
  * `comments`
  * `stars`
  * `contributors`
* Cleaner, normalized, analytics-friendly schema
* All linked via `repo_id`
