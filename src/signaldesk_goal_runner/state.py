"""Experimental local state primitives. No task execution or distributed fencing."""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable


class StateError(RuntimeError):
    """The state or its authority cannot be established safely."""


def _encode(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_encode(value)).hexdigest()


def _atomic_replace(path: Path, data: bytes) -> None:
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    if os.name != "nt":
        directory = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


@contextmanager
def _local_lock(path: Path):
    """Serialize cooperating processes on one host, never independent replicas."""
    with Path(path).open("a+b") as stream:
        if os.name == "nt":
            import msvcrt
            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise StateError("state is busy") from exc
        else:
            import fcntl
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise StateError("state is busy") from exc
        try:
            yield
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class EventStore:
    """A local synthetic event store. Approval integration is the caller's responsibility."""

    def __init__(self, root: Path, approve: Callable[[], bool]):
        self.root = Path(root)
        self.events = self.root / "events"
        self.checkpoint = self.root / "checkpoint.json"
        self.lock = self.root / "state.lock"
        self.approve = approve

    def _check(self) -> None:
        if self.approve() is not True:
            raise StateError("approval unavailable")

    def initialize(self) -> None:
        self._check()
        self.root.mkdir(parents=True, exist_ok=True)
        with _local_lock(self.lock):
            self._check()
            if self.events.exists() or self.checkpoint.exists():
                raise StateError("state already exists")
            self.events.mkdir()
            _atomic_replace(self.checkpoint, _encode({"sequence": 0, "head": None, "events": {}}))

    def _read_locked(self) -> dict:
        self._check()
        if not self.events.is_dir() or not self.checkpoint.is_file():
            raise StateError("state absent")
        if any(p.is_symlink() or p.name.endswith(".tmp") or "conflicted copy" in p.name.casefold()
               for p in self.root.rglob("*")):
            raise StateError("conflict or interrupted save")
        try:
            saved = json.loads(self.checkpoint.read_text(encoding="utf-8"))
            files = sorted(self.events.glob("*.json"))
            rebuilt = {"sequence": 0, "head": None, "events": {}}
            if len(files) != saved.get("sequence"):
                raise StateError("event/checkpoint mismatch")
            for number, path in enumerate(files, 1):
                event = json.loads(path.read_text(encoding="utf-8"))
                body = {key: event.get(key) for key in ("sequence", "id", "previous", "payload")}
                if (path.name != f"{number:08}.json" or body["sequence"] != number
                    or body["previous"] != rebuilt["head"] or body["id"] in rebuilt["events"]
                    or event.get("hash") != _digest(body)):
                    raise StateError("invalid event chain")
                rebuilt["events"][body["id"]] = body["payload"]
                rebuilt["sequence"], rebuilt["head"] = number, event["hash"]
            if saved != rebuilt:
                raise StateError("checkpoint differs from replay")
            return rebuilt
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise StateError("state unreadable") from exc

    def read(self) -> dict:
        self._check()
        if not self.root.is_dir():
            raise StateError("state absent")
        with _local_lock(self.lock):
            return self._read_locked()

    def append(self, event_id: str, payload: dict, expected_sequence: int) -> dict:
        self._check()
        if not self.root.is_dir():
            raise StateError("state absent")
        if not isinstance(event_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", event_id):
            raise StateError("invalid event id")
        if not isinstance(payload, dict):
            raise StateError("invalid payload")
        with _local_lock(self.lock):
            state = self._read_locked()
            if event_id in state["events"]:
                if state["events"][event_id] != payload:
                    raise StateError("event id reused with different payload")
                return state
            if state["sequence"] != expected_sequence:
                raise StateError("stale checkpoint")
            number = state["sequence"] + 1
            body = {"sequence": number, "id": event_id, "previous": state["head"], "payload": payload}
            event = dict(body, hash=_digest(body))
            target = self.events / f"{number:08}.json"
            if target.exists():
                raise StateError("event already exists")
            _atomic_replace(target, _encode(event))
            state["sequence"], state["head"] = number, event["hash"]
            state["events"][event_id] = payload
            _atomic_replace(self.checkpoint, _encode(state))
            return state