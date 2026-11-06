# Publishing with metadata

Metadata is free-form data carried alongside a call, in `envelope.metadata`,
for example a trace id or the source of the request. Attach it with
`with_metadata`:

``` { .python }
--8<-- "examples/guides/publish_metadata.py"
```

`with_metadata` returns an immutable bound task, so calls can be chained and
later keys override earlier ones; the task itself is unchanged. The bound task
has the same type-checked `delay` as the task. Metadata values go through the
app's codec with the rest of the envelope, so they must be values it can
encode.

The [scheduler](scheduled-tasks.md) uses metadata too: each scheduled publish
carries `idempotency_key` and `scheduled_at`.
