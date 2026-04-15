"""
Reddit Sentiment Collector
==========================
Tracks mention velocity, sentiment shifts, and account quality scoring.
Uses Reddit's public JSON API (no auth needed for basic access).
For higher rate limits, set REDDIT_CLIENT_ID/SECRET in config.
"""
import re
import logging
from datetime import datetime, timezone, timedelta
from collections import Counter
from typing import Dict, List, Optional

from stock_oracle.collectors.base import BaseCollector, SignalResult
from stock_oracle.config import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT, REDDIT_SUBREDDITS,
)

logger = logging.getLogger("stock_oracle")

# Simple sentiment lexicon for financial text
BULLISH_WORDS = {
    "moon", "rocket", "calls", "bull", "buy", "long", "breakout", "squeeze",
    "undervalued", "oversold", "dip", "cheap", "upside", "growth", "beat",
    "strong", "surge", "rally", "soar", "gain", "profit", "bullish",
    "tendies", "diamond", "hands", "yolo", "printing", "🚀", "💎",
}
BEARISH_WORDS = {
    "puts", "short", "bear", "sell", "crash", "dump", "overvalued",
    "overbought", "bubble", "fraud", "scam", "bankrupt", "debt", "loss",
    "down", "tank", "plunge", "drop", "bearish", "bag", "holding",
    "drill", "rug", "pull", "dead", "cave", "📉", "🐻",
}


class RedditSentimentCollector(BaseCollector):
    """
    Tracks Reddit sentiment with:
    - Mention velocity (acceleration of mentions)
    - Weighted sentiment (account quality scoring)
    - Subreddit diversity (mentioned across how many subs)
    """

    @property
    def name(self) -> str:
        return "reddit_sentiment"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "reddit")
        if cached:
            return SignalResult.from_dict(cached)

        posts = self._fetch_mentions(ticker)

        if not posts:
            return self._neutral_signal(ticker, "No Reddit mentions found")

        # Detect if we're using Google News proxy (not actual Reddit data)
        is_proxy = any(p.get("subreddit") == "google_proxy" for p in posts)

        # Analyze
        sentiment_score = self._compute_sentiment(posts)
        velocity = self._compute_velocity(posts)
        diversity = self._compute_subreddit_diversity(posts)
        quality = self._compute_account_quality(posts)

        # Composite signal
        # High velocity + positive sentiment = strong bullish
        # High velocity + negative sentiment = strong bearish
        # Low velocity = low confidence regardless
        raw_signal = sentiment_score * (1 + velocity * 0.5) * quality
        signal = max(-0.5, min(0.5, raw_signal))  # Cap at ±0.5 — social sentiment is noisy

        confidence = min(1.0, (len(posts) / 50) * diversity * quality)

        # Google News proxy is NOT Reddit data — heavily penalize confidence
        if is_proxy:
            confidence *= 0.3  # 70% confidence penalty for proxy data
            signal *= 0.5      # Halve the signal too

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data={
                "post_count": len(posts),
                "sentiment_score": sentiment_score,
                "velocity": velocity,
                "subreddit_diversity": diversity,
                "quality_score": quality,
                "top_posts": [p.get("title", "")[:100] for p in posts[:5]],
            },
            details=(
                f"{len(posts)} mentions | sent={sentiment_score:+.2f} | "
                f"vel={velocity:.1f}x | qual={quality:.0%}"
            ),
        )

        self._set_cache(result.to_dict(), ticker, "reddit")
        return result

    def _fetch_mentions(self, ticker: str) -> List[Dict]:
        """Fetch recent posts mentioning ticker across subreddits."""
        # Try PullPush API first (public Pushshift replacement)
        pullpush_posts = self._fetch_pullpush(ticker)
        if pullpush_posts:
            return pullpush_posts

        # Fallback 1: Reddit OAuth API (if credentials configured)
        if REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET:
            oauth_posts = self._fetch_reddit_oauth(ticker)
            if oauth_posts:
                return oauth_posts

        # Fallback 2: Google News RSS for Reddit mentions (always works, no rate limits)
        # Skip www/old.reddit.com — they 429 constantly and add 15-30s per scan
        return self._fetch_reddit_via_google(ticker)

    def _fetch_reddit_oauth(self, ticker: str) -> List[Dict]:
        """Use Reddit OAuth API with configured credentials."""
        try:
            import requests as req
            auth = req.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)
            token_resp = req.post(
                "https://www.reddit.com/api/v1/access_token",
                data={"grant_type": "client_credentials"},
                auth=auth,
                headers={"User-Agent": REDDIT_USER_AGENT},
                timeout=8,
            )
            if token_resp.status_code != 200:
                return []

            token = token_resp.json().get("access_token")
            if not token:
                return []

            headers = {
                "Authorization": f"Bearer {token}",
                "User-Agent": REDDIT_USER_AGENT,
            }

            all_posts = []
            for sub in REDDIT_SUBREDDITS[:4]:
                resp = req.get(
                    f"https://oauth.reddit.com/r/{sub}/search",
                    params={"q": ticker, "sort": "new", "t": "week",
                            "limit": 25, "restrict_sr": "true"},
                    headers=headers,
                    timeout=8,
                )
                if resp.status_code == 200:
                    for child in resp.json().get("data", {}).get("children", []):
                        post = child.get("data", {})
                        all_posts.append({
                            "title": post.get("title", ""),
                            "selftext": post.get("selftext", "")[:500],
                            "score": post.get("score", 0),
                            "num_comments": post.get("num_comments", 0),
                            "created_utc": post.get("created_utc", 0),
                            "subreddit": sub,
                            "author": post.get("author", ""),
                            "upvote_ratio": post.get("upvote_ratio", 0.5),
                        })

            return all_posts
        except Exception:
            return []

    def _fetch_reddit_via_google(self, ticker: str) -> List[Dict]:
        """Last resort: find Reddit discussion via Google News RSS."""
        url = f"https://news.google.com/rss/search?q={ticker}+reddit+stock&hl=en-US&gl=US&ceid=US:en"
        resp = self._request(url)
        if not resp or resp.status_code != 200:
            return []

        try:
            import re as regex
            titles = regex.findall(r'<title>(.*?)</title>', resp.text)

            posts = []
            for title in titles[1:15]:  # Skip feed title
                # Skip generic titles
                if title.strip() in ("Google News", "") or len(title) < 10:
                    continue
                posts.append({
                    "title": title,
                    "selftext": "",
                    "score": 10,  # Assume moderate engagement
                    "num_comments": 5,
                    "created_utc": datetime.now(timezone.utc).timestamp(),
                    "subreddit": "google_proxy",
                    "author": "unknown",
                    "upvote_ratio": 0.7,
                })
            return posts
        except Exception:
            return []

    def _fetch_pullpush(self, ticker: str) -> List[Dict]:
        """Use PullPush.io API (public Pushshift replacement)."""
        url = "https://api.pullpush.io/reddit/search/submission/"
        params = {
            "q": ticker,
            "subreddit": ",".join(REDDIT_SUBREDDITS[:5]),
            "size": 50,
            "sort": "desc",
            "sort_type": "created_utc",
            "after": int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp()),
        }
        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json().get("data", [])
                return [
                    {
                        "title": p.get("title", ""),
                        "selftext": p.get("selftext", "")[:500],
                        "score": p.get("score", 0),
                        "num_comments": p.get("num_comments", 0),
                        "created_utc": p.get("created_utc", 0),
                        "subreddit": p.get("subreddit", ""),
                        "author": p.get("author", ""),
                        "upvote_ratio": p.get("upvote_ratio", 0.5),
                    }
                    for p in data
                ]
            except Exception:
                pass
        return []

    def _compute_sentiment(self, posts: List[Dict]) -> float:
        """Weighted sentiment: high-engagement posts count more."""
        total_weight = 0
        weighted_sentiment = 0

        for post in posts:
            text = f"{post['title']} {post.get('selftext', '')}".lower()
            words = set(re.findall(r'\w+', text)) | set(re.findall(r'[🚀💎📉🐻]', text))

            bull = len(words & BULLISH_WORDS)
            bear = len(words & BEARISH_WORDS)

            if bull + bear == 0:
                continue

            post_sentiment = (bull - bear) / (bull + bear)

            # Weight by engagement
            weight = 1 + (post["score"] * 0.01) + (post["num_comments"] * 0.02)
            weight *= post.get("upvote_ratio", 0.5)

            weighted_sentiment += post_sentiment * weight
            total_weight += weight

        if total_weight == 0:
            return 0.0

        return weighted_sentiment / total_weight

    def _compute_velocity(self, posts: List[Dict]) -> float:
        """
        Mention velocity: ratio of recent mentions to older mentions.
        A score > 1 means accelerating interest.
        """
        now = datetime.now(timezone.utc).timestamp()
        recent_cutoff = now - (24 * 3600)  # Last 24h
        older_cutoff = now - (7 * 24 * 3600)  # Last week

        recent = sum(1 for p in posts if p["created_utc"] > recent_cutoff)
        older = sum(1 for p in posts if older_cutoff < p["created_utc"] <= recent_cutoff)

        if older == 0:
            return 2.0 if recent > 3 else 1.0

        # Normalize: daily rate vs weekly daily average
        daily_avg = older / 6
        if daily_avg == 0:
            return 2.0 if recent > 0 else 1.0

        return recent / daily_avg

    def _compute_subreddit_diversity(self, posts: List[Dict]) -> float:
        """More subreddits = more credible signal (0.0 to 1.0)."""
        subs = set(p["subreddit"] for p in posts)
        return min(1.0, len(subs) / 4)

    def _compute_account_quality(self, posts: List[Dict]) -> float:
        """
        Rough proxy for bot filtering.
        Posts from accounts with low upvote ratios get discounted.
        """
        if not posts:
            return 0.5
        avg_ratio = sum(p.get("upvote_ratio", 0.5) for p in posts) / len(posts)
        return avg_ratio
