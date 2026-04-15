"""
Ollama NLP Engine
=================
Local AI-powered analysis using James's Ollama setup.
Runs on the local network (192.168.8.32:11434) using qwen2.5:14b.

Features:
- Earnings call transcript analysis
- News article sentiment deep-dive
- SEC filing language complexity scoring
- Custom financial text analysis

No data leaves your network — everything runs locally.
"""
import json
import logging
import time
from typing import Dict, List, Optional

import requests

logger = logging.getLogger("stock_oracle")


class OllamaNLP:
    """
    Local LLM-powered NLP for financial text analysis.

    Usage:
        nlp = OllamaNLP()  # Auto-detects local Ollama
        result = nlp.analyze_earnings("AAPL", transcript_text)
        result = nlp.analyze_news_batch(articles)
        result = nlp.detect_hedging(filing_text)
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "qwen2.5:14b",
        fallback_model: str = "qwen2.5:7b",
        timeout: int = 120,
    ):
        self.base_url = base_url
        self.model = model
        self.fallback_model = fallback_model
        self.timeout = timeout
        self._available = None

    @property
    def available(self) -> bool:
        if self._available is None:
            self._available = self._check_connection()
        return self._available

    def _check_connection(self) -> bool:
        """Check if Ollama is running and model is available."""
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                if self.model in models or any(self.model.split(":")[0] in m for m in models):
                    logger.info(f"Ollama connected: {self.model} available")
                    return True
                elif self.fallback_model in models or any(self.fallback_model.split(":")[0] in m for m in models):
                    logger.info(f"Using fallback model: {self.fallback_model}")
                    self.model = self.fallback_model
                    return True
                else:
                    logger.warning(f"Ollama running but {self.model} not found. Available: {models}")
                    if models:
                        self.model = models[0]
                        logger.info(f"Auto-selected: {self.model}")
                        return True
            return False
        except Exception:
            logger.warning("Ollama not reachable — NLP features disabled")
            return False

    def _generate(self, prompt: str, system: str = "", temperature: float = 0.1) -> Optional[str]:
        """Send prompt to Ollama and get response."""
        if not self.available:
            return None

        try:
            resp = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "system": system,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": 2000,
                    },
                },
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                return resp.json().get("response", "")
        except Exception as e:
            logger.error(f"Ollama generation error: {e}")
        return None

    def _generate_json(self, prompt: str, system: str = "") -> Optional[Dict]:
        """Generate and parse JSON response."""
        result = self._generate(prompt, system)
        if not result:
            return None

        # Try to extract JSON from response
        try:
            # Handle markdown code blocks
            if "```json" in result:
                result = result.split("```json")[1].split("```")[0]
            elif "```" in result:
                result = result.split("```")[1].split("```")[0]

            return json.loads(result.strip())
        except json.JSONDecodeError:
            # Try to find JSON object in response
            import re
            match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', result)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    pass
            logger.warning(f"Failed to parse JSON from Ollama response")
            return None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # EARNINGS CALL ANALYSIS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def analyze_earnings(self, ticker: str, transcript: str) -> Dict:
        """
        Deep analysis of earnings call transcript.
        Returns sentiment, confidence indicators, key topics, and red flags.
        """
        # Chunk transcript if too long (keep under ~3000 tokens for context)
        max_chars = 8000
        if len(transcript) > max_chars:
            # Take intro, middle, and Q&A sections
            chunk = transcript[:3000] + "\n...\n" + transcript[-5000:]
        else:
            chunk = transcript

        system = """You are a financial analyst specializing in earnings call analysis.
Analyze the transcript and return ONLY a JSON object with these exact keys:
{
  "overall_sentiment": float (-1.0 bearish to 1.0 bullish),
  "ceo_confidence": float (0.0 to 1.0, based on language certainty),
  "hedge_word_count": int (may, might, could, potentially, etc.),
  "forward_guidance": "positive" | "neutral" | "cautious" | "negative",
  "key_topics": [list of 3-5 main topics discussed],
  "red_flags": [list of any concerning statements or omissions],
  "bullish_signals": [list of positive indicators],
  "bearish_signals": [list of negative indicators],
  "surprise_factor": float (-1.0 to 1.0, how surprising vs expectations),
  "summary": "2-3 sentence summary of the call"
}
Return ONLY valid JSON, no other text."""

        prompt = f"Analyze this {ticker} earnings call transcript:\n\n{chunk}"

        result = self._generate_json(prompt, system)

        if not result:
            return {
                "error": "Ollama analysis failed",
                "signal": 0.0,
                "confidence": 0.0,
            }

        # Convert to signal
        sentiment = result.get("overall_sentiment", 0)
        confidence = result.get("ceo_confidence", 0.5)
        guidance_map = {"positive": 0.3, "neutral": 0, "cautious": -0.2, "negative": -0.4}
        guidance_adj = guidance_map.get(result.get("forward_guidance", "neutral"), 0)

        signal = (sentiment * 0.5) + (guidance_adj * 0.3) + (result.get("surprise_factor", 0) * 0.2)

        return {
            "signal": max(-1.0, min(1.0, signal)),
            "confidence": confidence,
            "analysis": result,
            "model": self.model,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # NEWS SENTIMENT DEEP DIVE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def analyze_news_batch(self, articles: List[Dict]) -> Dict:
        """
        Analyze a batch of news articles for nuanced sentiment.
        Goes beyond keyword matching to understand context and implications.
        """
        if not articles:
            return {"signal": 0.0, "confidence": 0.0, "error": "No articles"}

        # Format articles for analysis
        article_text = "\n".join([
            f"[{i+1}] {a.get('title', 'No title')} — {a.get('source', 'Unknown')}"
            for i, a in enumerate(articles[:15])
        ])

        system = """You are a financial news analyst. Analyze these headlines and return ONLY JSON:
{
  "overall_sentiment": float (-1.0 to 1.0),
  "confidence": float (0.0 to 1.0),
  "dominant_narrative": "brief description of the main story",
  "sentiment_by_article": [{"index": 1, "sentiment": float, "impact": "high|medium|low"}],
  "market_impact_prediction": "strong_positive|positive|neutral|negative|strong_negative",
  "contrarian_note": "any reason the obvious interpretation might be wrong"
}
Return ONLY valid JSON."""

        result = self._generate_json(
            f"Analyze these financial news headlines:\n\n{article_text}",
            system,
        )

        if not result:
            return {"signal": 0.0, "confidence": 0.0, "error": "Analysis failed"}

        return {
            "signal": result.get("overall_sentiment", 0),
            "confidence": result.get("confidence", 0.5),
            "analysis": result,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SEC FILING LANGUAGE ANALYSIS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def analyze_filing(self, ticker: str, filing_text: str) -> Dict:
        """
        Analyze SEC filing language for hidden signals.
        Detects hedging, risk language changes, and unusual disclosures.
        """
        max_chars = 8000
        chunk = filing_text[:max_chars] if len(filing_text) > max_chars else filing_text

        system = """You are an SEC filing analyst. Analyze this filing excerpt and return ONLY JSON:
{
  "risk_level": float (0.0 low risk to 1.0 high risk),
  "hedging_intensity": float (0.0 direct to 1.0 very hedged),
  "legal_exposure": float (0.0 to 1.0),
  "new_risk_factors": [list of any newly disclosed risks],
  "language_changes": "description of notable language shifts vs typical filings",
  "red_flags": [list of concerning items],
  "signal": float (-1.0 bearish to 1.0 bullish),
  "summary": "2-3 sentence summary"
}
Return ONLY valid JSON."""

        result = self._generate_json(
            f"Analyze this {ticker} SEC filing:\n\n{chunk}",
            system,
        )

        if not result:
            return {"signal": 0.0, "confidence": 0.0}

        return {
            "signal": result.get("signal", 0),
            "confidence": 1.0 - result.get("hedging_intensity", 0.5),
            "analysis": result,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CUSTOM ANALYSIS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def custom_analysis(self, text: str, question: str) -> Optional[str]:
        """
        Ask any custom question about financial text.
        Returns free-form analysis.
        """
        system = (
            "You are a financial analyst. Provide concise, actionable analysis. "
            "Focus on implications for stock price and trading decisions."
        )
        return self._generate(f"{question}\n\nText:\n{text}", system)

    def compare_filings(self, current: str, previous: str, ticker: str) -> Dict:
        """
        Compare two filing periods to detect changes.
        """
        system = """Compare these two filing periods and return ONLY JSON:
{
  "tone_shift": float (-1.0 more negative to 1.0 more positive),
  "new_risks": [newly mentioned risks],
  "removed_risks": [risks no longer mentioned],
  "language_confidence_change": float (-1.0 less confident to 1.0 more confident),
  "key_changes": [list of significant differences],
  "signal": float (-1.0 bearish to 1.0 bullish),
  "summary": "2-3 sentence comparison"
}
Return ONLY valid JSON."""

        prompt = (
            f"{ticker} Filing Comparison\n\n"
            f"CURRENT PERIOD:\n{current[:4000]}\n\n"
            f"PREVIOUS PERIOD:\n{previous[:4000]}"
        )

        return self._generate_json(prompt, system) or {"signal": 0, "error": "Comparison failed"}

    def get_status(self) -> Dict:
        """Get Ollama connection status and model info."""
        status = {
            "available": self.available,
            "base_url": self.base_url,
            "model": self.model,
        }

        if self.available:
            try:
                resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
                if resp.status_code == 200:
                    models = resp.json().get("models", [])
                    for m in models:
                        if m["name"] == self.model or self.model.split(":")[0] in m["name"]:
                            status["model_size"] = m.get("size", 0)
                            status["model_family"] = m.get("details", {}).get("family", "unknown")
                            break
                    status["all_models"] = [m["name"] for m in models]
            except Exception:
                pass

        return status
