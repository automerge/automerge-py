# Changelog

## Unreleased

### Added

- An asynchronous repository API in `automerge.repo` for managing documents,
  analogous to and interoperable with the `@automerge/automerge-repo`
  JavaScript library. 
- Collaborative text proxies with string-like reads and `insert`, `delete`, and
  `splice` operations. Assigning a plain Python string through the high-level API
  creates collaborative text. Use `ImmutableString` for non-collaborative scalar
  strings.
- `automerge.core.Document.get_last_local_change()` to retrieve the most recent
  local change.
- `automerge.core.Document.load_incremental()` to import encoded changes, change
  bundles, or saved documents, and `get_missing_deps()` to inspect missing change
  dependencies. The `Change` type is now exported from `automerge.core`.

### Changed

- **Python 3.10 or newer is now required**, up from Python 3.8.
- Updated the Rust Automerge dependency to 0.12.0
- Source builds now require Rust 1.90 or newer.
- Enabled Python's stable ABI (`abi3`) for CPython wheels, targeting Python 3.10
  and newer.
- Updated minimum optional dependencies to `websockets>=16.0` and
  `aiobotocore>=3.7.0`; source builds now require `maturin>=1.13,<2.0`.

### Fixed

- Document lookups wait for storage loading or peer synchronization to finish,
  and return `None` when the document is currently unavailable.
- Boolean values assigned through the high-level API now round-trip as
  `True`/`False` instead of integers.
- Updated Intel macOS wheel builds to use the `macos-15-intel` runner, and aligned
  release builds with the Python 3.10 minimum.
- Pinned Ruff in CI, formatted README Python examples, and explicitly preserved
  the previous default lint rules to prevent unexpected tooling upgrades from
  breaking checks.

## 1.0.0rc1

- Complete rewrite to support Automerge v2.x.
- Introduced `automerge.core`, a low-level wrapper around the Rust
  implementation of Automerge.

## 0.1.0

This was the first release of automerge-py.
