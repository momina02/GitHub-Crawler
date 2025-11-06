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

**db.py** : This file manages MySQL database. It loads enviornment variables host, port, username, password, and database name from .env file. The `get_conn()` function creates a connection to MySQL so the other arts of project can read or write in it similarly `initialize_database()` function ensures that the database has th table of `repos`. So it first connects to the MySQL, creates the database if it’s not there, and then creates the `repos` table with columns like `repo_id`, `repo_name`, `stars`, `forks`, `updated_at`, and a timestamp for when the record was crawled. Essentially, this file sets up the database structure and provides a way to connect to it for storing GitHub repository data.

**github_api.py** : This file contains the `GitHubGraphQLCrawler` class, which handles fetching repository data from GitHub using the GraphQL API. When the crawler is started, it sets up the API, including GitHub token for authentication. The `fetch_popular_repos()` function is the main part it fetches repositories with stars, splitting the time range into 6-month intervals, and pages through results using cursors to handle GitHub’s pagination. It keeps track of already fetched repositories to avoid duplicates and yields each repository’s details. The `fetch_repo_details()` function allows fetching the details of a single repository by its full name, which is used for refreshing data in the database. In short, this file handles all communication with GitHub and organizes repository data for storage.

**init_crawler.py** : This file organize fetching repositories from GitHub and saving them into the database in bulk. It starts by loading environment variables, initializes the database, and creates a connection. It sets up a SQL query for inserting or updating repository data so that duplicates are updated rather than inserted multiple times. The script loops through repositories from the `GitHubGraphQLCrawler`, formats the timestamp, adds the data to a batch, and inserts it into the database in chunks (default 1000 at a time). It also respects API rate limits by pausing between inserts. Essentially, this file is for an initial or large-scale collection of GitHub repositories into your MySQL database.

**main.py** : This is your main automation script that handles both the initial collection and the periodic update of repository data. It uses a batch-upsert function `upsert_batch()` to insert or update multiple records at once, converting timestamps to MySQL format. The script first checks if the database is empty. If yes, it fetches repositories from GitHub and inserts them. Then, it refreshes the existing records by fetching updated details for each repository already in the database. After updates, it exports the entire `repos` table to a CSV file, cleaning up descriptions by removing newlines and extra spaces. So, this file is the main script for maintaining an up-to-date snapshot of popular GitHub repositories.

**crawler.yaml** : This is a GitHub Actions workflow file that automates the running of your crawler. It is set to run daily and can also be triggered manually. It sets up a MySQL service, installs Python dependencies, waits for MySQL to be ready, creates a `.env` file with database and crawler configuration, initializes the database schema, and then runs your `main.py` crawler. After crawling, it exports the database table to a CSV and uploads it as an artifact so you can download it. Essentially, this file automates the entire process in a cloud environment, ensuring your database and CSV are always up to date without manual intervention.

**In short, it is a complete system that: sets up a MySQL database, fetches popular GitHub repositories using the GraphQL API, stores them in batches, refreshes them periodically, and exports the results to a CSV, all fully automatable via GitHub Actions.**

---

## **Approach for fetching 500,000 repos**:

Instead of running a single script, multiple scripts will run in parallel, reducing time. Also, I will add a batch process to make it more efficient. I can also split the data between multiple scripts, so instead of a single script fetching the complete data of a single repo, multiple script will fetch multiple parts of repositories data parallely.

## **Approach for future schema**:

I will make different tables for different types, like there will be a separate table for issues, separate for comments, stars, and so on... it will give a better overview and a clean structure which can be accessed through repo id.
