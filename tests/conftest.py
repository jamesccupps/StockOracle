"""Shared test setup."""
import gc

import pytest


@pytest.fixture(autouse=True)
def _collect_on_main_thread():
    """Free each test's garbage on the main thread.

    Tests that build Tk windows (wizard, GUI threading) leave the dead
    interpreters to the garbage collector. If the next collection happens to
    run on another thread, such as the event-loop thread a terminal
    TestClient starts, Tcl aborts the process ("Tcl_AsyncDelete: async
    handler deleted by the wrong thread", Windows code 0x80000003). That was
    the intermittent CI crash on every Python version since the wizard tests
    landed (reproduced in a 12-line script; see the commit message).
    """
    yield
    gc.collect()


@pytest.fixture(autouse=True)
def _no_live_quote_polling(monkeypatch):
    """Keep unit tests off the network. Every create_app() lifespan started
    the real QuoteHub, which polls Yahoo from worker threads on every
    terminal test: slow, rate-limited from CI runners, and no test reads the
    result. register() still pushes the status message the websocket tests
    read."""
    import asyncio
    from stock_oracle.terminal import quotes

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._tasks = []

    async def stop(self):
        pass

    monkeypatch.setattr(quotes.QuoteHub, "start", start)
    monkeypatch.setattr(quotes.QuoteHub, "stop", stop)
