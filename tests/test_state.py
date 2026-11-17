import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from signaldesk_goal_runner.state import EventStore, StateError, _atomic_replace, _local_lock


class EventStoreTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="goal-runner-test-")) / "state"
        self.store = EventStore(self.root, lambda: True)
        self.store.initialize()

    def test_restart_and_duplicate_are_idempotent(self):
        self.store.append("one", {"value": 1}, 0)
        reopened = EventStore(self.root, lambda: True)
        self.assertEqual(reopened.read()["sequence"], 1)
        self.assertEqual(reopened.append("one", {"value": 1}, 0)["sequence"], 1)
        with self.assertRaises(StateError):
            reopened.append("one", {"value": 2}, 1)

    def test_stale_write_is_rejected(self):
        self.store.append("one", {"value": 1}, 0)
        with self.assertRaises(StateError):
            self.store.append("two", {"value": 2}, 0)
        self.assertEqual(self.store.read()["sequence"], 1)

    def test_approval_revoked_before_write(self):
        before = self.store.checkpoint.read_bytes()
        denied = EventStore(self.root, lambda: False)
        with self.assertRaises(StateError):
            denied.append("one", {"value": 1}, 0)
        self.assertEqual(before, self.store.checkpoint.read_bytes())

    def test_interrupted_checkpoint_fails_closed(self):
        def inject(path, data):
            if Path(path).name == "checkpoint.json":
                raise OSError("simulated interruption")
            return _atomic_replace(path, data)
        with patch("signaldesk_goal_runner.state._atomic_replace", side_effect=inject):
            with self.assertRaises(OSError):
                self.store.append("one", {"value": 1}, 0)
        with self.assertRaises(StateError):
            self.store.read()

    def test_tampered_event_is_rejected(self):
        self.store.append("one", {"value": 1}, 0)
        event = self.store.events / "00000001.json"
        value = json.loads(event.read_text())
        value["payload"]["value"] = 2
        event.write_text(json.dumps(value))
        with self.assertRaises(StateError):
            self.store.read()

    def test_local_lock_excludes_another_process(self):
        script = ("import sys; from signaldesk_goal_runner.state import _local_lock,StateError\n"
                  "try:\n with _local_lock(sys.argv[1]): pass\nexcept StateError: sys.exit(23)\n")
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
        with _local_lock(self.store.lock):
            result = subprocess.run([sys.executable, "-B", "-c", script, str(self.store.lock)],
                                    env=env, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 23, result.stderr)


if __name__ == "__main__":
    unittest.main()