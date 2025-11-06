import datetime
import os
import time
import requests
from typing import Iterator, Dict, Any, Optional

class GitHubGraphQLCrawler:
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.url = "https://api.github.com/graphql"
        self.headers = {
            "Authorization": f"bearer {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "github-graphql-crawler"
        }

    def fetch_popular_repos(self, limit: int = 100000) -> Iterator[Dict[str, Any]]:
        
        fetched = 0
        seen = set()

        start_date = datetime.date(2008, 1, 1)
        end_date = datetime.date.today()
        delta = datetime.timedelta(days=180)  # 6-month intervals

        while start_date < end_date and fetched < limit:
            range_end = min(start_date + delta, end_date)
            cursor = None
            has_next_page = True

            while has_next_page and fetched < limit:
                query = """
                query ($cursor: String) {
                search(query: "stars:>0 created:{start}..{end}", type: REPOSITORY, first: 100, after: $cursor) {
                    edges {
                    node {
                        ... on Repository {
                        id
                        name
                        nameWithOwner
                        url
                        description
                        stargazerCount
                        forkCount
                        primaryLanguage { name }
                        updatedAt
                        }
                    }
                    }
                    pageInfo { hasNextPage endCursor }
                }
                }
                """.format(start=start_date, end=range_end)

                variables = {"cursor": cursor}
                resp = requests.post(self.url, json={"query": query, "variables": variables}, headers=self.headers)
                if resp.status_code != 200:
                    print(f"Error {resp.status_code}: {resp.text}")
                    time.sleep(30)
                    continue

                data = resp.json()["data"]["search"]
                edges = data["edges"]
                if not edges:
                    break

                for edge in edges:
                    node = edge["node"]
                    repo_id = node["id"]
                    if repo_id in seen:
                        continue
                    seen.add(repo_id)
                    fetched += 1
                    yield {
                        "repo_id": repo_id,
                        "repo_name": node["name"],
                        "full_name": node["nameWithOwner"],
                        "html_url": node["url"],
                        "description": node["description"],
                        "stars": node["stargazerCount"],
                        "forks": node["forkCount"],
                        "language": node["primaryLanguage"]["name"] if node["primaryLanguage"] else None,
                        "updated_at": node["updatedAt"],
                    }
                    if fetched >= limit:
                        break

                page_info = data["pageInfo"]
                has_next_page = page_info["hasNextPage"]
                cursor = page_info["endCursor"]
                time.sleep(1)  # polite sleep to avoid rate limits

            start_date += delta


    def fetch_repo_details(self, full_name: str) -> Optional[Dict[str, Any]]:
        """Fetch single repo details by full_name using GraphQL"""
        query = """
        query ($name: String!) {
          repository(owner: $owner, name: $repo) {
            id
            name
            nameWithOwner
            url
            description
            stargazerCount
            forkCount
            primaryLanguage { name }
            updatedAt
          }
        }
        """

        owner, repo = full_name.split("/")
        variables = {"owner": owner, "repo": repo}
        resp = requests.post(self.url, json={"query": query, "variables": variables}, headers=self.headers)
        if resp.status_code != 200:
            print(f"Failed to fetch {full_name}: {resp.status_code}")
            return None

        node = resp.json().get("data", {}).get("repository")
        if not node:
            return None

        return {
            "repo_id": node["id"],
            "repo_name": node["name"],
            "full_name": node["nameWithOwner"],
            "html_url": node["url"],
            "description": node["description"],
            "stars": node["stargazerCount"],
            "forks": node["forkCount"],
            "language": node["primaryLanguage"]["name"] if node["primaryLanguage"] else None,
            "updated_at": node["updatedAt"],
        }
