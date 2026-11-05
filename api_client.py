"""Shared GitHub HTTP layer used by Task 1 (extract).

- RateLimiter: one per GitHub budget (REST "core" and GraphQL are separate 5,000/hour
  budgets). Every thread that uses the same token shares the same limiter, so the
  three repos fetched in parallel can never jointly overspend the quota.
- Odometer:   counts every HTTP request sent to GitHub during a flow run (retries
  included), broken down by API, repo and endpoint, plus GraphQL points spent.
- rest_get:   one REST GET with rate-limit handling and transient-error retries.
"""
import threading
import time
from collections import Counter

import httpx


class RateLimiter:
    """Thread-safe view of one GitHub rate-limit budget, shared by all fetching threads.

    Before each request a thread calls wait(). If the last known budget is at or below
    `buffer`, the thread sleeps until the reset time while HOLDING the lock, so every
    other thread queues behind it instead of spending the last few calls. Each wait()
    also reserves one unit, so several threads that pass at once don't all assume the
    same remaining budget. update() records what GitHub reported on each response.
    """

    def __init__(self, name, buffer):
        self.name = name
        self.buffer = buffer
        self.remaining = None        # unknown until the first response
        self.reset_at = 0.0          # unix time the budget refills
        self._lock = threading.Lock()

    def wait(self, logger=None):
        with self._lock:
            if self.remaining is not None and self.remaining <= self.buffer:
                pause = self.reset_at - time.time() + 5
                if pause > 0:
                    if logger:
                        logger.warning(f"{self.name} budget at {self.remaining} (buffer "
                                       f"{self.buffer}): all fetchers sleeping {pause:.0f}s "
                                       f"until reset")
                    time.sleep(pause)
                self.remaining = None            # fresh window; next response tells us the truth
            if self.remaining is not None:
                self.remaining -= 1              # reserve a unit for the request about to go out

    def update(self, remaining, reset_at):
        """Record the budget from a response. Responses from parallel threads can arrive
        out of order, so within one window keep the LOWEST remaining value seen."""
        with self._lock:
            if reset_at > self.reset_at + 1 or self.remaining is None:
                self.remaining, self.reset_at = remaining, reset_at   # new window
            else:
                self.remaining = min(self.remaining, remaining)


class Odometer:
    """Counts every request to GitHub in this flow run. Thread-safe."""

    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self):
        with self._lock:
            self.requests = Counter()    # (api, repo, endpoint) -> HTTP requests sent
            self.points = Counter()      # (api, repo, endpoint) -> GraphQL points spent

    def add(self, api, repo, endpoint):
        """Count one HTTP request (called just before it is sent, so retries count too)."""
        with self._lock:
            self.requests[(api, repo, endpoint)] += 1

    def spend(self, api, repo, endpoint, points):
        """Record GraphQL points a query cost (from the rateLimit field of its response)."""
        with self._lock:
            self.points[(api, repo, endpoint)] += points

    def total(self, api=None, repo=None):
        with self._lock:
            return sum(n for (a, r, _), n in self.requests.items()
                       if (api is None or a == api) and (repo is None or r == repo))

    def rows(self):
        """One dict per (api, repo, endpoint), sorted, for logging and the api_odometer table."""
        with self._lock:
            return [{"api": a, "repo": r, "endpoint": e, "requests": n,
                     "graphql_points": self.points[(a, r, e)]}
                    for (a, r, e), n in sorted(self.requests.items())]


REST_LIMITER = RateLimiter("REST core", buffer=10)
GRAPHQL_LIMITER = RateLimiter("GraphQL", buffer=50)     # points; one PR page costs ~1-2
ODOMETER = Odometer()


def rest_get(url, headers, repo, endpoint, logger=None, max_tries=5):
    """GET one REST page. Retries network errors and 5xx with backoff, honors secondary
    rate limits (403/429 + Retry-After), and sleeps before the primary budget hits zero.
    Anything still failing is raised so the Prefect task's own retries take over."""
    tries = 0
    while True:
        REST_LIMITER.wait(logger)
        ODOMETER.add("rest", repo, endpoint)
        try:
            response = httpx.get(url, headers=headers, timeout=60)
        except httpx.TransportError as e:
            tries += 1
            if tries >= max_tries:
                raise RuntimeError(f"{repo} {endpoint}: network error after {tries} tries "
                                   f"on {url}: {e}") from e
            time.sleep(2 ** tries)
            continue

        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        if remaining is not None and reset is not None:
            REST_LIMITER.update(int(remaining), int(reset))

        if response.status_code in (403, 429):
            if remaining == "0" and reset:                       # primary limit exhausted
                wait = max(1, int(reset) - int(time.time())) + 5
            elif response.headers.get("Retry-After", "").isdigit():   # secondary limit
                wait = int(response.headers["Retry-After"])
            else:
                raise RuntimeError(f"{repo} {endpoint}: HTTP {response.status_code} on {url}: "
                                   f"{response.text[:300]}")
            if logger:
                logger.warning(f"{repo} {endpoint}: HTTP {response.status_code}, "
                               f"sleeping {wait}s before retrying")
            time.sleep(wait)
            continue

        if response.status_code >= 500:
            tries += 1
            if tries >= max_tries:
                raise RuntimeError(f"{repo} {endpoint}: HTTP {response.status_code} after "
                                   f"{tries} tries on {url}")
            time.sleep(2 ** tries)
            continue

        if response.is_error:
            raise RuntimeError(f"{repo} {endpoint}: HTTP {response.status_code} on {url}: "
                               f"{response.text[:300]}")
        return response
