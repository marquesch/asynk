# Changelog

## [0.4.0](https://github.com/marquesch/asynk/compare/v0.3.0...v0.4.0) (2026-11-09)


### ⚠ BREAKING CHANGES

* let a task opt out of the app-wide timeout ([#103](https://github.com/marquesch/asynk/issues/103))

### Bug Fixes

* let a task opt out of the app-wide timeout ([#103](https://github.com/marquesch/asynk/issues/103)) ([5961214](https://github.com/marquesch/asynk/commit/59612145486f781427e8a4dcd5c89f7ce69e0dd2))


### Refactoring

* flatten Scheduler._fire and drop its bool return ([#105](https://github.com/marquesch/asynk/issues/105)) ([73b09b8](https://github.com/marquesch/asynk/commit/73b09b84f138a6a978489dddabd681bd5e5e2448))

## [0.3.0](https://github.com/marquesch/asynk/compare/v0.2.0...v0.3.0) (2026-11-06)


### ⚠ BREAKING CHANGES

* typed task results with TaskHandle[T] ([#90](https://github.com/marquesch/asynk/issues/90))
* move the shared worker and scheduler plumbing into one Runner base ([#83](https://github.com/marquesch/asynk/issues/83))

### Features

* OpenTelemetry tracing and metrics, activated when opentelemetry-api is installed ([#86](https://github.com/marquesch/asynk/issues/86)) ([7379e91](https://github.com/marquesch/asynk/commit/7379e91e4c7473a68f5bb4d530690dffaed33899))
* per-task timeout ([#85](https://github.com/marquesch/asynk/issues/85)) ([7176e38](https://github.com/marquesch/asynk/commit/7176e380ddce6c62f954ab8fc4e11fbfe624da86))
* typed task results with TaskHandle[T] ([#90](https://github.com/marquesch/asynk/issues/90)) ([b6e9268](https://github.com/marquesch/asynk/commit/b6e9268458767fd00de3412efffd207b41fd8af3))


### Bug Fixes

* end RedisConsumer.deliveries() when the Redis client swallows a cancellation ([#92](https://github.com/marquesch/asynk/issues/92)) ([e5f1120](https://github.com/marquesch/asynk/commit/e5f11204b30728aec61fc20d12e209e38cbb5629))


### Refactoring

* move the shared worker and scheduler plumbing into one Runner base ([#83](https://github.com/marquesch/asynk/issues/83)) ([5a3babf](https://github.com/marquesch/asynk/commit/5a3babf52cbd771714e0d5d55aa6bed397f5f54c))

## [0.2.0](https://github.com/marquesch/asynk/compare/v0.1.0...v0.2.0) (2026-11-06)


### ⚠ BREAKING CHANGES

* publish through an injected callable so Task no longer holds the App ([#80](https://github.com/marquesch/asynk/issues/80))
* tasks must be async def ([#77](https://github.com/marquesch/asynk/issues/77))

### Refactoring

* publish through an injected callable so Task no longer holds the App ([#80](https://github.com/marquesch/asynk/issues/80)) ([5f765e6](https://github.com/marquesch/asynk/commit/5f765e685c00e8b7a25bba20ad275fc4f9280297))
* tasks must be async def ([#77](https://github.com/marquesch/asynk/issues/77)) ([6107a2a](https://github.com/marquesch/asynk/commit/6107a2aaa57e8c6768fb3aa126d25f8f73e3fc80))


### Documentation

* drop the hand-written release notes ([#73](https://github.com/marquesch/asynk/issues/73)) ([e030264](https://github.com/marquesch/asynk/commit/e0302646dbcbdf262a667e8e64bb1302fa64168b))
* keep the release notes in a single file ([#69](https://github.com/marquesch/asynk/issues/69)) ([76567ae](https://github.com/marquesch/asynk/commit/76567ae08e672326831fa4ae6ff03d1d2ac321e7))
* simplify the release notes steps for a single maintainer ([#71](https://github.com/marquesch/asynk/issues/71)) ([e051d64](https://github.com/marquesch/asynk/commit/e051d64f3192b8f50e72cd4154d8505ce44ab086))

## 0.1.0 (2026-11-06)


### ⚠ BREAKING CHANGES

* Producer.publish takes a third parameter, options: PublishOptions | None = None; custom producers must accept it (they may ignore it).
* the store is no longer registered in app.hooks (before_startup and after_shutdown start empty), after_shutdown hooks now run before the store stops, and they no longer run when startup fails. Runner subclasses must implement _serve().
* every asynk exception now lives in asynk.errors, is exported from asynk and derives from AsynkError only; NotConfiguredError is no longer a RuntimeError, TaskNotFoundError no longer a LookupError, and CronError, DecodeError, EncodeError and InvalidEnvelopeError no longer ValueErrors. The old module paths (asynk.app, asynk.codec, asynk.hooks, ...) no longer provide them. TaskFailedError is renamed DeliveryFailedError.
* before_task, after_task, BeforeTaskHook, AfterTaskHook and SkipTask are removed; use @app.middleware and TaskCall.skip(). A layer that raises after the task ran is logged and no longer redelivers.
* require Python 3.11
* **hooks:** dedupe hooks by object instead of module.qualname
* remove the unix socket transport and SqliteStore
* **scheduler:** release the slot claim when the publish fails, and add Store.delete ([#44](https://github.com/marquesch/asynk/issues/44))

### Features

* add 3.10+ compatibility and improving some aspects ([abd40fe](https://github.com/marquesch/asynk/commit/abd40fe0c75e309284a211afb1ec668b32bf4e8b))
* add a Redis store (asynk[redis]) ([#54](https://github.com/marquesch/asynk/issues/54)) ([fbc6935](https://github.com/marquesch/asynk/commit/fbc6935a83fa572eccdbe76efb87e628dd749032))
* add a Redis Streams transport (asynk[redis]) ([#59](https://github.com/marquesch/asynk/issues/59)) ([1e1e64a](https://github.com/marquesch/asynk/commit/1e1e64a03931173f30c83b3d280475d9ef2c3976))
* built-in task retries with backoff ([#55](https://github.com/marquesch/asynk/issues/55)) ([6635ca1](https://github.com/marquesch/asynk/commit/6635ca19d0e8ea4dd502e546559cd0806103244c))
* hooks ([#5](https://github.com/marquesch/asynk/issues/5)) ([83ad790](https://github.com/marquesch/asynk/commit/83ad790cf37cb6e80b2150c479ed0e62bdeb45be))
* pluggable store for task results and scheduler dedupe ([#16](https://github.com/marquesch/asynk/issues/16)) ([85743d7](https://github.com/marquesch/asynk/commit/85743d7728effd4d79595869faf1ac00578fc46d))
* scheduler (cron-scheduled tasks) ([#9](https://github.com/marquesch/asynk/issues/9)) ([89145cd](https://github.com/marquesch/asynk/commit/89145cd77781b89761250f369315d39ca15ab8fb))
* task middleware, replacing before_task and after_task ([#47](https://github.com/marquesch/asynk/issues/47)) ([0981b9f](https://github.com/marquesch/asynk/commit/0981b9fc41eca3f7747663d9d1716ef414a16020))
* unix transport ([#4](https://github.com/marquesch/asynk/issues/4)) ([1df1633](https://github.com/marquesch/asynk/commit/1df163384eb584e87b9adeaa9c5ace4b0c7bc969))
* unix transport ([#6](https://github.com/marquesch/asynk/issues/6)) ([ce7a126](https://github.com/marquesch/asynk/commit/ce7a126145aad561ca9dfd72ae3ec6207e736f40))


### Bug Fixes

* **hooks:** dedupe hooks by object instead of module.qualname ([af32a4a](https://github.com/marquesch/asynk/commit/af32a4a915ec6cda8588a4d44564cd8a31d2265e))
* **scheduler:** release the slot claim when the publish fails, and add Store.delete ([#44](https://github.com/marquesch/asynk/issues/44)) ([4c3ff71](https://github.com/marquesch/asynk/commit/4c3ff71e108f94f285e4dcb81051db5b32431e16))
* **store:** purge expired MemoryStore keys and free wait events ([08cb4b6](https://github.com/marquesch/asynk/commit/08cb4b6fbada72708697b1a8c1a95cf58d6c449e))
* unwind the store when startup fails, and stop it after the shutdown hooks ([#53](https://github.com/marquesch/asynk/issues/53)) ([f28af37](https://github.com/marquesch/asynk/commit/f28af3792b6580af0ac3078391e9bda8b82121bd))


### Refactoring

* add AsynkError and consolidate the exceptions in asynk.errors ([#51](https://github.com/marquesch/asynk/issues/51)) ([b9f42b2](https://github.com/marquesch/asynk/commit/b9f42b2198d97e106c5e818a1344d7578c251f82))
* add logging module to centralize logging configuration ([#13](https://github.com/marquesch/asynk/issues/13)) ([efeda02](https://github.com/marquesch/asynk/commit/efeda023651e62e8b5ab1dcdf0b61454947a8c7f))
* improved task naming ([6f09550](https://github.com/marquesch/asynk/commit/6f0955083944e7c2b80f5b5cf541a740ef24875e))
* make Codec a generic value serializer owned by the app ([#18](https://github.com/marquesch/asynk/issues/18)) ([cc48608](https://github.com/marquesch/asynk/commit/cc486086f12b2ba4fc244fcb2a04ae868c0c1594))
* move JSON validation from Envelope to the codec ([#14](https://github.com/marquesch/asynk/issues/14)) ([d28c787](https://github.com/marquesch/asynk/commit/d28c787d5ba43d37a3528c9773278ce2d4b92e6f))
* remove the unix socket transport and SqliteStore ([0dbeab9](https://github.com/marquesch/asynk/commit/0dbeab9e7521716626a49a042fb0c5eb82bb874a))
* split codec and transport into interface/implementation packages ([a7cb614](https://github.com/marquesch/asynk/commit/a7cb614a73ca761ece08952c46e01663d1a4581e))


### Documentation

* add dark mode to the documentation site ([#52](https://github.com/marquesch/asynk/issues/52)) ([e77d036](https://github.com/marquesch/asynk/commit/e77d036ee372da7b2c7965dbc30ae6e1c712a528))
* add the advanced middleware page with a metrics example ([#50](https://github.com/marquesch/asynk/issues/50)) ([0b61118](https://github.com/marquesch/asynk/commit/0b611180bef7d34034b3b05d85699ac050321d09))
* add Zensical docs site with API reference and getting started ([#46](https://github.com/marquesch/asynk/issues/46)) ([6488932](https://github.com/marquesch/asynk/commit/6488932d3025587346d859515477aec6cd9216ad))
* bring docstrings in line with the docstring rules ([#48](https://github.com/marquesch/asynk/issues/48)) ([12f4fc4](https://github.com/marquesch/asynk/commit/12f4fc464ee6db746e6ff6b4caf0c66fa72e4b49))
* fix python version in readme ([#3](https://github.com/marquesch/asynk/issues/3)) ([a98aa3c](https://github.com/marquesch/asynk/commit/a98aa3c64c4084e885d397558865c7a6be1f8888))
* slim the README and move the details to the documentation site ([#49](https://github.com/marquesch/asynk/issues/49)) ([eac0cf6](https://github.com/marquesch/asynk/commit/eac0cf62f95610c0c1e893bce2ecf6336c6149d6))


### Miscellaneous

* require Python 3.11 ([dd5f044](https://github.com/marquesch/asynk/commit/dd5f0441020788a4495d583d2384a3504938505c))

## Changelog
