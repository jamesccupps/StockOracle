"""
Scrub credentials out of text before it is logged or shown.

requests puts the full URL, query string included, into exception messages
("Max retries exceeded with url: /api/v1/quote?symbol=AAPL&token=..."). Some
APIs (FRED) only accept the key as a query parameter, so anything that logs
a requests exception should pass it through redact() first.
"""
import re

_SECRET_PARAM = re.compile(
    r"(?P<k>\b(?:token|api_?key|apikey|access_token|secret|password)=)[^&\s'\"<>)]+",
    re.IGNORECASE,
)


def redact(text) -> str:
    return _SECRET_PARAM.sub(r"\g<k>***", str(text))
