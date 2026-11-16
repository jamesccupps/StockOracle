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
