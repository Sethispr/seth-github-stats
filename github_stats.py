#!/usr/bin/python3

import asyncio
import os
from typing import Dict, List, Optional, Set, Tuple, Any, cast

import aiohttp

###############################################################################
# Main Classes
###############################################################################


class Queries(object):
    """
    Class with functions to query the GitHub GraphQL (v4) API and the REST (v3)
    API. Also includes functions to dynamically generate GraphQL queries.
    """

    def __init__(
        self,
        username: str,
        access_token: str,
        session: aiohttp.ClientSession,
        max_connections: int = 10,
    ):
        self.username = username
        self.access_token = access_token
        self.session = session
        self.semaphore = asyncio.Semaphore(max_connections)

    async def query(self, generated_query: str) -> Dict:
        """
        Make a request to the GraphQL API using the authentication token from
        the environment
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        try:
            async with self.semaphore:
                async with self.session.post(
                    "https://api.github.com/graphql",
                    headers=headers,
                    json={"query": generated_query},
                ) as response:
                    result = await response.json()
                    if result is not None:
                        return result
        except Exception as e:
            print(f"aiohttp failed for GraphQL query: {e}")
        
        return dict()

    async def query_rest(self, path: str, params: Optional[Dict] = None) -> Any:
        """
        Make a request to the REST API
        """
        # Reduced retries to prevent infinite hangs
        for _ in range(67):
            headers = {
                "Authorization": f"token {self.access_token}",
            }
            if params is None:
                params = dict()
            if path.startswith("/"):
                path = path[1:]
            
            try:
                async with self.semaphore:
                    async with self.session.get(
                        f"https://api.github.com/{path}",
                        headers=headers,
                        params=tuple(params.items()),
                    ) as response:
                        
                        if response.status == 202:
                            print(f"Path {path} returned 202 (Processing). Retrying...")
                            await asyncio.sleep(2)
                            continue
                        
                        result = await response.json()
                        if result is not None:
                            return result
                            
            except Exception as e:
                print(f"aiohttp failed for rest query {path}: {e}")
                return dict()

        print(f"There were too many 202s. Data for {path} will be incomplete.")
        return dict()

    @staticmethod
    def repos_overview(
        contrib_cursor: Optional[str] = None, owned_cursor: Optional[str] = None
    ) -> str:
        return f"""{{
  viewer {{
    login,
    name,
    repositories(
        first: 100,
        orderBy: {{
            field: UPDATED_AT,
            direction: DESC
        }},
        isFork: false,
        after: {"null" if owned_cursor is None else '"'+ owned_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
    repositoriesContributedTo(
        first: 100,
        includeUserRepositories: false,
        orderBy: {{
            field: UPDATED_AT,
            direction: DESC
        }},
        contributionTypes: [
            COMMIT,
            PULL_REQUEST,
            REPOSITORY,
            PULL_REQUEST_REVIEW
        ]
        after: {"null" if contrib_cursor is None else '"'+ contrib_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

    @staticmethod
    def contrib_years() -> str:
        return """
query {
  viewer {
    contributionsCollection {
      contributionYears
    }
  }
}
"""

    @staticmethod
    def contribs_by_year(year: str) -> str:
        return f"""
    year{year}: contributionsCollection(
        from: "{year}-01-01T00:00:00Z",
        to: "{int(year) + 1}-01-01T00:00:00Z"
    ) {{
      contributionCalendar {{
        totalContributions
      }}
    }}
"""

    @classmethod
    def all_contribs(cls, years: List[str]) -> str:
        by_years = "\n".join(map(cls.contribs_by_year, years))
        return f"""
query {{
  viewer {{
    {by_years}
  }}
}}
"""


class Stats(object):
    """
    Retrieve and store statistics about GitHub usage.
    """

    def __init__(
        self,
        username: str,
        access_token: str,
        session: aiohttp.ClientSession,
        exclude_repos: Optional[Set] = None,
        exclude_langs: Optional[Set] = None,
        ignore_forked_repos: bool = False,
    ):
        self.username = username
        self._ignore_forked_repos = ignore_forked_repos
        self._exclude_repos = set() if exclude_repos is None else exclude_repos
        self._exclude_langs = set() if exclude_langs is None else exclude_langs
        self.queries = Queries(username, access_token, session)
        
        # Lock to prevent race conditions during parallel fetching
        self._stats_lock = asyncio.Lock()

        self._name: Optional[str] = None
        self._stargazers: Optional[int] = None
        self._forks: Optional[int] = None
        self._total_contributions: Optional[int] = None
        self._languages: Optional[Dict[str, Any]] = None
        self._repos: Optional[Set[str]] = None
        self._lines_changed: Optional[Tuple[int, int]] = None
        self._views: Optional[int] = None

    async def to_str(self) -> str:
        """
        :return: summary of all available statistics
        """
        languages = await self.languages_proportional
        formatted_languages = "\n  - ".join(
            [f"{k}: {v:0.4f}%" for k, v in languages.items()]
        )
        lines_changed = await self.lines_changed
        return f"""Name: {await self.name}
Stargazers: {await self.stargazers:,}
Forks: {await self.forks:,}
All-time contributions: {await self.total_contributions:,}
Repositories with contributions: {len(await self.repos)}
Lines of code added: {lines_changed[0]:,}
Lines of code deleted: {lines_changed[1]:,}
Lines of code changed: {lines_changed[0] + lines_changed[1]:,}
Project page views: {await self.views:,}
Languages:
  - {formatted_languages}"""

    async def get_stats(self) -> None:
        """
        Get lots of summary statistics using one big query. Sets many attributes.
        Protected by a lock to ensure we don't run this expensive query twice
        if accessed concurrently.
        """
        if self._name is not None:
            return

        async with self._stats_lock:
            # Double check inside lock
            if self._name is not None:
                return

            self._stargazers = 0
            self._forks = 0
            self._languages = dict()
            self._repos = set()

            exclude_langs_lower = {x.lower() for x in self._exclude_langs}

            next_owned = None
            next_contrib = None
            while True:
                raw_results = await self.queries.query(
                    Queries.repos_overview(
                        owned_cursor=next_owned, contrib_cursor=next_contrib
                    )
                )
                raw_results = raw_results if raw_results is not None else {}

                self._name = raw_results.get("data", {}).get("viewer", {}).get("name", None)
                if self._name is None:
                    self._name = (
                        raw_results.get("data", {})
                        .get("viewer", {})
                        .get("login", "No Name")
                    )

                contrib_repos = (
                    raw_results.get("data", {})
                    .get("viewer", {})
                    .get("repositoriesContributedTo", {})
                )
                owned_repos = (
                    raw_results.get("data", {}).get("viewer", {}).get("repositories", {})
                )

                repos = owned_repos.get("nodes", [])
                if not self._ignore_forked_repos:
                    repos += contrib_repos.get("nodes", [])

                for repo in repos:
                    if repo is None:
                        continue
                    name = repo.get("nameWithOwner")
                    if name in self._repos or name in self._exclude_repos:
                        continue
                    self._repos.add(name)
                    self._stargazers += repo.get("stargazers").get("totalCount", 0)
                    self._forks += repo.get("forkCount", 0)

                    for lang in repo.get("languages", {}).get("edges", []):
                        name = lang.get("node", {}).get("name", "Other")
                        languages = await self.languages
                        if name.lower() in exclude_langs_lower:
                            continue
                        if name in languages:
                            languages[name]["size"] += lang.get("size", 0)
                            languages[name]["occurrences"] += 1
                        else:
                            languages[name] = {
                                "size": lang.get("size", 0),
                                "occurrences": 1,
                                "color": lang.get("node", {}).get("color"),
                            }

                if owned_repos.get("pageInfo", {}).get(
                    "hasNextPage", False
                ) or contrib_repos.get("pageInfo", {}).get("hasNextPage", False):
                    next_owned = owned_repos.get("pageInfo", {}).get(
                        "endCursor", next_owned
                    )
                    next_contrib = contrib_repos.get("pageInfo", {}).get(
                        "endCursor", next_contrib
                    )
                else:
                    break

            langs_total = sum([v.get("size", 0) for v in self._languages.values()])
            for k, v in self._languages.items():
                v["prop"] = 100 * (v.get("size", 0) / langs_total) if langs_total > 0 else 0

    @property
    async def name(self) -> str:
        if self._name is not None:
            return self._name
        await self.get_stats()
        assert self._name is not None
        return self._name

    @property
    async def stargazers(self) -> int:
        if self._stargazers is not None:
            return self._stargazers
        await self.get_stats()
        assert self._stargazers is not None
        return self._stargazers

    @property
    async def forks(self) -> int:
        if self._forks is not None:
            return self._forks
        await self.get_stats()
        assert self._forks is not None
        return self._forks

    @property
    async def languages(self) -> Dict:
        if self._languages is not None:
            return self._languages
        await self.get_stats()
        assert self._languages is not None
        return self._languages

    @property
    async def languages_proportional(self) -> Dict:
        if self._languages is None:
            await self.get_stats()
            assert self._languages is not None

        return {k: v.get("prop", 0) for (k, v) in self._languages.items()}

    @property
    async def repos(self) -> Set[str]:
        if self._repos is not None:
            return self._repos
        await self.get_stats()
        assert self._repos is not None
        return self._repos

    @property
    async def total_contributions(self) -> int:
        if self._total_contributions is not None:
            return self._total_contributions

        self._total_contributions = 0
        years = (
            (await self.queries.query(Queries.contrib_years()))
            .get("data", {})
            .get("viewer", {})
            .get("contributionsCollection", {})
            .get("contributionYears", [])
        )
        
        # FIX: Correctly access the data dictionary
        response = await self.queries.query(Queries.all_contribs(years))
        viewer_data = response.get("data", {}).get("viewer", {})
        
        # iterate over keys like year2020, year2021...
        for year_data in viewer_data.values():
            self._total_contributions += year_data.get("contributionCalendar", {}).get(
                "totalContributions", 0
            )
        return cast(int, self._total_contributions)

    @property
    async def lines_changed(self) -> Tuple[int, int]:
        if self._lines_changed is not None:
            return self._lines_changed
        
        additions = 0
        deletions = 0
        
        repo_list = list(await self.repos)
        tasks = [
            self.queries.query_rest(f"/repos/{repo}/stats/contributors") 
            for repo in repo_list
        ]
        
        results = await asyncio.gather(*tasks)

        for response_obj in results:
            if not isinstance(response_obj, list):
                continue
            
            for author_obj in response_obj:
                if not isinstance(author_obj, dict) or not isinstance(
                    author_obj.get("author", {}), dict
                ):
                    continue
                author = author_obj.get("author", {}).get("login", "")
                if author != self.username:
                    continue

                for week in author_obj.get("weeks", []):
                    additions += week.get("a", 0)
                    deletions += week.get("d", 0)

        self._lines_changed = (additions, deletions)
        return self._lines_changed

    @property
    async def views(self) -> int:
        if self._views is not None:
            return self._views

        total = 0
        
        repo_list = list(await self.repos)
        tasks = [
            self.queries.query_rest(f"/repos/{repo}/traffic/views") 
            for repo in repo_list
        ]
        
        results = await asyncio.gather(*tasks)

        for result in results:
            if not isinstance(result, dict):
                continue
            for view in result.get("views", []):
                total += view.get("count", 0)

        self._views = total
        return total
        
