# github_api.py
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
        """Fetch popular repos using GraphQL with pagination"""
        fetched = 0
        cursor = None
        batch_size = 100  # max items per GraphQL query

        while fetched < limit:
            query = """
            query ($cursor: String) {
              search(query: "stars:>0", type: REPOSITORY, first: 100, after: $cursor) {
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
            """

            variables = {"cursor": cursor}
            resp = requests.post(self.url, json={"query": query, "variables": variables}, headers=self.headers)
            if resp.status_code != 200:
                raise RuntimeError(f"GraphQL request failed: {resp.status_code} {resp.text}")

            data = resp.json()
            edges = data["data"]["search"]["edges"]
            if not edges:
                break

            for edge in edges:
                node = edge["node"]
                yield {
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
                fetched += 1
                if fetched >= limit:
                    break

            page_info = data["data"]["search"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]
            time.sleep(30)  # polite pause to respect rate limits

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
