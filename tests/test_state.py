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

    def test_conflict_names_block_reads_without_modification(self):
        for name in ("checkpoint sync conflict.json", "checkpoint conflicted copy.json"):
            with self.subTest(name=name):
                path = self.store.root / name
                path.write_text("preserve")
                before = self.store.checkpoint.read_bytes()
                with self.assertRaises(StateError):
                    self.store.read()
                self.assertEqual(before, self.store.checkpoint.read_bytes())
                self.assertEqual(path.read_text(), "preserve")
                path.unlink()

    def test_initialization_rejects_existing_content(self):
        root = self.root.parent / "dirty"
        root.mkdir()
        existing = root / "orphan.tmp"
        existing.write_text("preserve")
        with self.assertRaises(StateError):
            EventStore(root, lambda: True).initialize()
        self.assertEqual(existing.read_text(), "preserve")
        self.assertEqual({p.name for p in root.iterdir()}, {"orphan.tmp"})

    def test_wrong_json_shapes_raise_state_error(self):
        self.store.checkpoint.write_text("[]")
        with self.assertRaises(StateError):
            self.store.read()
        other = self.root.parent / "other"
        store = EventStore(other, lambda: True)
        store.initialize()
        store.append("one", {"value": 1}, 0)
        (store.events / "00000001.json").write_text("[]")
        with self.assertRaises(StateError):
            store.read()

    def test_existing_lock_symlink_is_rejected(self):
        target = self.root.parent / "target"
        target.write_bytes(b"")
        link = self.root.parent / "lock-link"
        try:
            link.symlink_to(target)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"symlink creation unavailable: {exc}")
        with self.assertRaises(StateError):
            with _local_lock(link):
                self.fail("symlink lock was accepted")
        self.assertEqual(target.read_bytes(), b"")

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