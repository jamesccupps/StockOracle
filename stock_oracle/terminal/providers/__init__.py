"""Market data providers. Use DataRouter; the modules below are its backends."""
from stock_oracle.terminal.providers.router import DataRouter, ProviderError

__all__ = ["DataRouter", "ProviderError"]
