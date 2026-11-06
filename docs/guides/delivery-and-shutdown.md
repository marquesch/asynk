# Delivery guarantees and shutdown

## At-least-once delivery

Delivery is at-least-once, so tasks should be idempotent. `envelope.id` stays
stable across `envelope.retry()`, which makes it usable as an idempotency key.

## Failed tasks

The core does not retry a failing task. A failure yields a `TaskResult` with
status `FAILED`, or `RETRIES_EXCEEDED` once `envelope.attempt` reaches the
task's `max_retries`, and the failure reaches the consumer's `process()` as a
`DeliveryFailedError`. The transport settles the delivery: the memory transport
rejects it without requeue, and a broker transport typically dead-letters it.
See [Writing a custom transport](../advanced/custom-transport.md#the-consumer-contract).

A message the worker cannot decode, or that is not a valid envelope, is logged
and rejected without requeue.

## Graceful shutdown

On SIGTERM or SIGINT the worker stops fetching, then waits up to
`shutdown_timeout` (default 30 s, `--shutdown-timeout`) for running tasks:

- async tasks still running afterwards are cancelled, and their deliveries are
  rejected with requeue, so the broker redelivers them;
- sync tasks (plain `def`, run in a thread) cannot be interrupted: after the
  timeout their delivery is requeued but the thread keeps running, so the task
  may run twice, and process exit waits until the thread finishes.

On Kubernetes set `terminationGracePeriodSeconds` above `shutdown_timeout` plus
the longest sync task. The default `shutdown_timeout` of 30 seconds equals the
Kubernetes default grace period of 30 seconds, so raise one of them.

A scheduler stops on the same signals: it cancels a publish in flight and does
not wait for slots that are not yet due.
