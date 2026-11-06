# Delivery guarantees and shutdown

## At-least-once delivery

Delivery is at-least-once, so tasks should be idempotent. `envelope.id` stays
stable across `envelope.retry()`, which makes it usable as an idempotency key.

## Failed tasks

A failing task is retried only when it has a retry policy; see
[Retries](retries.md). A failure that is not retried yields a `TaskResult` with
status `FAILED`, or `RETRIES_EXCEEDED` once its retries are used up, and
reaches the consumer's `process()` as a `DeliveryFailedError`. The transport
settles the delivery: the memory transport rejects it without requeue, and a
broker transport typically dead-letters it.
See [Writing a custom transport](../advanced/custom-transport.md#the-consumer-contract).

A message the worker cannot decode, or that is not a valid envelope, is logged
and rejected without requeue.

## Graceful shutdown

On SIGTERM or SIGINT the worker stops fetching, then waits up to
`shutdown_timeout` (default 10 s, `--shutdown-timeout`) for running tasks. Tasks
still running afterwards are cancelled, and their deliveries are rejected with
requeue, so the broker redelivers them.

On Kubernetes keep `terminationGracePeriodSeconds` above `shutdown_timeout`. The
default `shutdown_timeout` of 10 seconds is below the Kubernetes default grace
period of 30 seconds; if you raise it, raise the grace period too.

A retry published with a delay waits in the broker. The memory transport keeps
it in the process, so a delayed retry that is not yet due is lost when the
process exits.

A scheduler stops on the same signals: it cancels a publish in flight and does
not wait for slots that are not yet due.
