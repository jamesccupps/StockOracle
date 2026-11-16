"""Worker threads reach Tk only through the main thread's queues."""
import inspect
import queue
import re
import threading
import types

import pytest

from stock_oracle import gui, setup_wizard


class Untouchable:
    """A widget stand-in that fails the test if a worker thread touches it."""
    def __getattr__(self, name):
        raise AssertionError(f"widget .{name} touched off the Tk thread")


def _in_thread(fn, *args):
    err = []

    def run():
        try:
            fn(*args)
        except BaseException as e:      # surface assertion errors from the worker
            err.append(e)
    t = threading.Thread(target=run)
    t.start()
    t.join()
    if err:
        raise err[0]


def test_log_from_worker_is_queued():
    fake = types.SimpleNamespace(msg_queue=queue.Queue(), log_text=Untouchable())
    _in_thread(gui.StockOracleGUI._log, fake, "from the monitor thread")
    assert fake.msg_queue.get_nowait() == ("log", "from the monitor thread")


def test_advisor_status_from_worker_is_queued():
    fake = types.SimpleNamespace(msg_queue=queue.Queue(), advisor_status_label=Untouchable())
    fake._update_advisor_status = lambda: None
    _in_thread(gui.StockOracleGUI._update_advisor_status, fake)
    kind, fn = fake.msg_queue.get_nowait()
    assert kind == "call" and fn is fake._update_advisor_status


def test_one_failing_message_does_not_drop_the_rest():
    logged = []
    fake = types.SimpleNamespace(msg_queue=queue.Queue(), _log=logged.append,
                                 root=types.SimpleNamespace(after=lambda ms, fn: None),
                                 _process_queue=lambda: None)

    def boom():
        raise RuntimeError("bad draw")
    fake.msg_queue.put(("call", boom))
    fake.msg_queue.put(("log", "still delivered"))
    gui.StockOracleGUI._process_queue(fake)
    assert "still delivered" in logged
    assert any("Display error (call): bad draw" in m for m in logged)


def test_worker_paths_do_not_call_tk_directly():
    src = inspect.getsource(gui.StockOracleGUI)
    assert "self.root.after(100, self._check_ml_status)" not in src
    assert not re.search(r"def on_\w+\(icon, item\):\s*\n\s*self\.root\.after", src)


def test_wizard_training_thread_routes_widget_updates():
    src = inspect.getsource(setup_wizard.SetupWizard._run_training)
    worker = src[src.index("        def run():"):]
    for direct in ("self.train_progress.configure(", "self.train_log.insert(",
                   "self.next_btn.configure(", "self.back_btn.configure(", "update_idletasks"):
        assert direct not in worker, direct


def test_wizard_pump_applies_queued_calls():
    import tkinter as tk
    try:
        root = tk.Tk()
    except tk.TclError:
        pytest.skip("no display for Tk")
    try:
        fake = types.SimpleNamespace(_ui_queue=queue.Queue(), root=root, _pump_ui=lambda: None)
        label = tk.Label(root, text="before")
        fake._ui_queue.put((label.configure, (), {"text": "after"}))
        gone = tk.Label(root)
        gone.destroy()
        fake._ui_queue.put((gone.configure, (), {"text": "x"}))   # TclError: ignored
        setup_wizard.SetupWizard._pump_ui(fake)
        assert label.cget("text") == "after"
    finally:
        root.destroy()
