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
        """
        Iterate public repos from /repositories endpoint until we yield max_repos items.
        This endpoint paginates using the `since` parameter (repo id).
        """
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
        """
        Fetch up to 100,000 popular repositories using GitHub's Search API.
        Streams repositories page-by-page using yield for continuous insertion.
        """
        url = f"{GITHUB_API_BASE}/search/repositories"
        per_page = min(self.per_page, 100)
        seen = set()
        collected = 0

        star_ranges = [
            "stars:>50000",
            "stars:10000..50000",
            "stars:5000..10000",
            "stars:1000..5000",
            "stars:500..1000",
            "stars:200..500",
            "stars:100..200",
            "stars:50..100",
            "stars:10..50",
            "stars:1..10",
        ]

        for star_query in star_ranges:
            for page in range(1, 11):
                if collected >= limit:
                    return

                params = {
                    "q": star_query,
                    "sort": "stars",
                    "order": "desc",
                    "per_page": per_page,
                    "page": page,
                }

                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 403:
                    self._check_rate_limit_and_maybe_sleep(resp)
                    continue
                if resp.status_code != 200:
                    print(f"❌ Failed {star_query} page {page}: {resp.status_code}")
                    break

                items = resp.json().get("items", [])
                if not items:
                    break

                for repo in items:
                    if repo["id"] in seen:
                        continue
                    seen.add(repo["id"])
                    collected += 1
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

                    if collected >= limit:
                        return

                print(f"✅ Collected {collected} repos so far... ({star_query} p{page})")
                time.sleep(1)

        print(f"🎯 Finished streaming {collected} repositories.")


            
    # def fetch_popular_repos(self, limit=1000):
    #     """
    #     Fetch repositories sorted by stars using the Search API.
    #     This returns older, popular repos instead of new empty ones.
    #     """
    #     url = f"{GITHUB_API_BASE}/search/repositories"
    #     per_page = min(self.per_page, 100)
    #     repos = []
    #     page = 1

    #     while len(repos) < limit:
    #         params = {
    #             "q": "stars:>100",  # only repos with >100 stars
    #             "sort": "stars",
    #             "order": "desc",
    #             "per_page": per_page,
    #             "page": page,
    #         }
    #         resp = self.session.get(url, params=params, timeout=30)
    #         if resp.status_code == 403:
    #             self._check_rate_limit_and_maybe_sleep(resp)
    #             continue
    #         if resp.status_code != 200:
    #             print("Failed:", resp.status_code, resp.text)
    #             break

    #         items = resp.json().get("items", [])
    #         if not items:
    #             break

    #         for repo in items:
    #             repos.append({
    #                 "repo_id": repo["id"],
    #                 "repo_name": repo["name"],
    #                 "full_name": repo["full_name"],
    #                 "html_url": repo["html_url"],
    #                 "description": repo["description"],
    #                 "stars": repo["stargazers_count"],
    #                 "forks": repo["forks_count"],
    #                 "language": repo["language"],
    #                 "updated_at": repo["updated_at"],
    #             })
    #             if len(repos) >= limit:
    #                 break

    #         page += 1
    #         time.sleep(1)  # polite delay

    #     return repos
    
