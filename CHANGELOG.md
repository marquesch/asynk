# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `asynk.__version__`, read from the installed distribution's metadata.
- Package classifiers, keywords and project URLs.

### Changed

- **Breaking:** `delay` returns a `TaskHandle`; the producer's receipt is at
  `handle.receipt`.
- **Breaking:** `App.get_task(name)` was renamed `App.registered_task(name)`, and
  `get_task` takes a task id.
- **Breaking:** `Codec` is now a generic value serializer with `validate(value)`,
  `encode(value)` and `decode(data)`, owned by the app through `App(codec=...)`.
- **Breaking:** `Producer.publish(envelope, body)` takes the encoded body and
  `Delivery.body` carries it; transports no longer take a codec argument.
- The worker decodes each message and rejects, without requeue, one it cannot decode.
- **Breaking:** hooks are deduped by object identity and equality instead of
  `module.qualname`, so partials, lambdas, factory-made hooks and callable instances
  register; registering the same hook twice is still a no-op.
- **Breaking:** the `Store` protocol gains `delete(key) -> bool`; third-party stores must
  implement it.

### Removed

- **Breaking:** `Producer.validate` and `Delivery.envelope`.
- **Breaking:** `MemoryProducer.publish_raw`.
- **Breaking:** the unix socket transport (`asynk.transport.unix`, `unix_transport`) and
  `SqliteStore`, with their examples.

### Fixed

- The scheduler releases its dedupe claim when the publish fails or is cancelled.
- `MemoryStore` purges expired keys and frees wait events instead of keeping them
  for the life of the store.
