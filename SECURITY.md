# Security policy

Stock Oracle keeps your API keys and runs a web terminal on your PC, so a security problem in it matters. Please report one privately, not in a public issue.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting:
**Security → Report a vulnerability** on this repository
(<https://github.com/jamesccupps/StockOracle/security/advisories/new>). Only the maintainer sees it.

Include:
- what an attacker could do with it;
- the steps to reproduce;
- the version (the release, or the commit if you run from source), and whether it's the desktop app or the terminal.

You should hear back within a week. Fixes are credited in the release notes unless you'd rather not be named.

## What counts

- **The terminal answering without its token**, whether from another computer, another web page (CSRF or DNS rebinding) or another local port, or giving away the token or anything in `.env`.
- **A key where it shouldn't be:** in a URL, a log, an error message, the Settings dialog or a build.
- **Placing a trade.** The broker connectors are read-only by design, and a test enforces it. Any code path that places, changes or cancels an order is in scope.
- **Spending past the Claude cap** (`CLAUDE_MONTHLY_CAP`), which the desktop app and the terminal share.
- **A link that opens something other than a web page.** News and filing links should open only http(s) addresses.
- **Hostile data:** a response from a data source, or from something posing as one, that runs code in the app or puts script into the terminal page.

## What doesn't

These are by design, and the README describes them:

- **Keys in plain text.** `.env` holds them unencrypted (`%APPDATA%\StockOracle\.env` in the installed app). Anyone signed in to your Windows account can read it, so treat it like a password.
- **`--lan` serves plain HTTP.** It's meant for Tailscale or another encrypted link.
- **Finnhub's websocket** needs the key in its URL. That's Finnhub's design; REST calls send it in a header.
- **Predictions being wrong.** Accuracy isn't a security issue, and Stock Oracle isn't financial advice.

## Supported versions

The latest release and `main`.
