import os
import time
import requests
from typing import Iterator, Dict, Any, Optional

GITHUB_API_BASE = "https://api.github.com"
PER_PAGE = 100  # /repositories doesn't accept per_page>100; 100 is standard max for REST lists

class RateLimitExceeded(Exception):
    pass

class GitHubRestCrawler:
    def __init__(self, token: Optional[str] = None, per_page: int = PER_PAGE, session: Optional[requests.Session] = None):
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.per_page = per_page
        self.session = session or requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"token {self.token}"})
        self.session.headers.update({"Accept": "application/vnd.github.v3+json", "User-Agent": "github-crawler"})

    def fetch_repo_details(self, repo_full_name):
        """Fetch up-to-date details for a given repo"""
        url = f"{GITHUB_API_BASE}/repos/{repo_full_name}"
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 403:
            self._check_rate_limit_and_maybe_sleep(resp)
            return self.fetch_repo_details(repo_full_name)
        if resp.status_code != 200:
            print(f"Failed to fetch {repo_full_name}: {resp.status_code}")
            return None
        repo = resp.json()
        return {
            "repo_id": repo["id"],
            "repo_name": repo["name"],
            "full_name": repo["full_name"],
            "html_url": repo["html_url"],
            "description": repo["description"],
            "stars": repo["stargazers_count"],
            "forks": repo["forks_count"],
            "language": repo["language"],
            "updated_at": repo["updated_at"],
        }


    def _check_rate_limit_and_maybe_sleep(self, resp: requests.Response):
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None:
            remaining = int(remaining)
            if remaining <= 1:
                if reset is None:
                    # conservative pause
                    print("Rate limit near exhaustion, sleeping 60s")
                    time.sleep(60)
                    return
                reset_ts = int(reset)
                sleep_for = max(0, reset_ts - int(time.time()) + 2)
                print(f"Rate limit exhausted. Sleeping for {sleep_for} seconds until reset.")
                time.sleep(sleep_for)
                return

    def list_public_repos(self, max_repos: int) -> Iterator[Dict[str, Any]]:
        collected = 0
        since = None  # repository id to start after
        while collected < max_repos:
            params = {"per_page": self.per_page}
            if since:
                params["since"] = since
            url = f"{GITHUB_API_BASE}/repositories"
            print(f"Requesting {url} params={params}")
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 403:
                # maybe rate-limited
                print("403 received. Inspecting headers for rate limit info.")
                self._check_rate_limit_and_maybe_sleep(resp)
                # after sleeping, retry this loop
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"GitHub API returned {resp.status_code}: {resp.text}")

            self._check_rate_limit_and_maybe_sleep(resp)
            page = resp.json()
            if not page:
                print("Empty page returned by /repositories — stopping.")
                break

            for repo in page:
                # Map fields we care about
                yield {
                    "repo_id": repo.get("id"),
                    "repo_name": repo.get("name"),
                    "full_name": repo.get("full_name"),
                    "html_url": repo.get("html_url"),
                    "description": repo.get("description"),
                    "stars": repo.get("stargazers_count", 0),
                    "forks": repo.get("forks_count", 0),
                    "language": repo.get("language"),
                    "updated_at": repo.get("updated_at"),
                }
                collected += 1
                if collected >= max_repos:
                    break

            # prepare next 'since' as the last repo id on this page
            last_repo = page[-1]
            since = last_repo.get("id")
            # small sleep to be polite
            time.sleep(0.5)
    
    def fetch_popular_repos(self, limit=100000):
        import datetime

        total_collected = 0
        seen = set()

        # Split by 6-month intervals (adjust as needed)
        start_date = datetime.date(2008, 1, 1)
        end_date = datetime.date.today()
        delta = datetime.timedelta(days=180)

        while start_date < end_date and total_collected < limit:
            range_end = min(start_date + delta, end_date)

            page = 1
            while total_collected < limit:
                query = f"stars:>0 created:{start_date}..{range_end}"
                params = {
                    "q": query,
                    "sort": "stars",
                    "order": "desc",
                    "per_page": min(self.per_page, 100),
                    "page": page,
                }
                resp = self.session.get(f"{GITHUB_API_BASE}/search/repositories", params=params)
                if resp.status_code == 403:
                    self._check_rate_limit_and_maybe_sleep(resp)
                    continue
                if resp.status_code != 200:
                    print(f"Failed: {resp.status_code} for {query} page {page}")
                    break

                items = resp.json().get("items", [])
                if not items:
                    break

                for repo in items:
                    if repo["id"] in seen:
                        continue
                    seen.add(repo["id"])
                    total_collected += 1
                    yield {
                        "repo_id": repo["id"],
                        "repo_name": repo["name"],
                        "full_name": repo["full_name"],
                        "html_url": repo["html_url"],
                        "description": repo.get("description"),
                        "stars": repo.get("stargazers_count", 0),
                        "forks": repo.get("forks_count", 0),
                        "language": repo.get("language"),
                        "updated_at": repo.get("updated_at"),
                    }
                    if total_collected >= limit:
                        return

                page += 1
                time.sleep(1)  # polite sleep

            start_date += delta

