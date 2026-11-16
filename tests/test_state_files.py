"""Shared state files: atomic writes, no lost appends, bounded log."""
import json
from pathlib import Path

import pytest

from stock_oracle.utils.atomic import write_text_atomic

PKG = Path(__file__).resolve().parent.parent / "stock_oracle"


def test_atomic_write_keeps_old_file_when_write_fails(tmp_path, monkeypatch):
    target = tmp_path / "state.json"
    target.write_text('{"good": true}')

    def broken(self, data):
        raise OSError("disk full")
    monkeypatch.setattr(Path, "write_bytes", broken)
    with pytest.raises(OSError):
        write_text_atomic(target, '{"new": true}')
    monkeypatch.undo()
    assert json.loads(target.read_text()) == {"good": True}
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]   # no temp file left


def test_prediction_appended_during_verification_survives(tmp_path, monkeypatch):
    from stock_oracle import prediction_tracker as pt
    monkeypatch.setattr(pt, "PREDICTIONS_DIR", tmp_path)
    monkeypatch.setattr(pt, "PENDING_LOCK", tmp_path / ".pending.lock")
    monkeypatch.setattr(pt, "VERIFIED_FILE", tmp_path / "verified.jsonl")
    monkeypatch.setattr(pt, "ACCURACY_FILE", tmp_path / "accuracy_log.json")

    pending = tmp_path / "pending_20261101.jsonl"
    due = {"ticker": "AAPL", "prediction": "BULLISH", "signal": 0.3, "confidence": 0.6,
           "price_at_prediction": 100.0, "verify_after": "2026-01-01T00:00:00+00:00",
           "timestamp": "2025-12-27T15:00:00+00:00"}
    later = dict(due, ticker="MSFT", verify_after="2099-01-01T00:00:00+00:00")
    pending.write_text(json.dumps(due) + "\n" + json.dumps(later) + "\n")
    appended = dict(due, ticker="NVDA", verify_after="2099-01-02T00:00:00+00:00")

    tracker = pt.PredictionTracker()

    def price_while_other_process_appends(ticker, yf):
        # The terminal/GUI records a new prediction mid-verification
        with open(pending, "a", encoding="utf-8") as f:
            f.write(json.dumps(appended) + "\n")
        return 105.0
    monkeypatch.setattr(tracker, "_get_current_price", price_while_other_process_appends)

    stats = tracker.verify_pending()
    assert stats["verified"] == 1
    left = [json.loads(l)["ticker"] for l in pending.read_text().splitlines()]
    assert left == ["MSFT", "NVDA"]          # AAPL verified and dropped; NVDA not lost
    assert json.loads((tmp_path / "verified.jsonl").read_text().splitlines()[0])["ticker"] == "AAPL"


@pytest.mark.parametrize("module, needle", [
    ("ml/pipeline.py", "write_bytes_atomic(save_path"),
    ("prediction_tracker.py", "write_text_atomic(ACCURACY_FILE"),
    ("signal_intelligence.py", "write_text_atomic(INTELLIGENCE_FILE"),
    ("utils/ticker_resolver.py", "write_text_atomic(NAMES_CACHE_FILE"),
    ("gui.py", 'write_text_atomic(DATA_DIR / "gui_watchlist.json"'),
])
def test_shared_state_files_are_written_atomically(module, needle):
    assert needle in (PKG / module).read_text(encoding="utf-8")


def test_log_is_rotated_and_in_the_data_folder():
    import stock_oracle.config as cfg
    from stock_oracle import oracle
    src = (PKG / "oracle.py").read_text(encoding="utf-8")
    assert "RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3" in src
    assert 'FileHandler("stock_oracle.log"' not in src
    assert oracle.LOG_FILE.parent == Path(cfg.DATA_DIR)
