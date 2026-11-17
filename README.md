# SignalDesk Goal Runner

**Experimental and under active development. This project is not operational and is not ready for production use.**

SignalDesk Goal Runner explores portable, resumable execution of approved tasks. This repository currently contains a small, no-dispatch state prototype and tests. It does not start model workers, install a scheduler, coordinate independent cloud replicas, or implement independent task acceptance.

## Current prototype

- JSON event records and a checkpoint with hash-chain verification.
- Local process locking and fail-closed detection of interrupted saves.
- An explicit approval callback and a disabled-dispatch boundary.
- Deterministic tests that do not use external services or model calls.

The caller must route all writes to one qualified coordination endpoint. A local file lock is not a distributed lock. Do not use the prototype to control production tasks or synchronized replicas.

## Run the tests

Requires Python 3.11 or later; no third-party packages are required.

```sh
python -m unittest discover -s tests -v
```

## Status and safety

This is development code, not a deployment package. Recovery, remote execution, independent verification, budget controls, endpoint migration, and scheduler integration remain unimplemented or unverified. Tests provide evidence for specific local behavior, not an operational guarantee. See [Design notes](docs/design.md) for the boundaries and known gaps.