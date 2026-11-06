# API reference

## App and tasks

::: asynk.App

::: asynk.Task

::: asynk.ScheduledTask

::: asynk.TaskOptions

::: asynk.Scheduler
    options:
      inherited_members: true

## Running

::: asynk.Worker
    options:
      inherited_members: true

::: asynk.Runner

## Middleware

::: asynk.TaskCall

::: asynk.Next

## Retries

::: asynk.RetryPolicy

::: asynk.Exponential

::: asynk.Backoff

## Transport protocols

::: asynk.Producer

::: asynk.PublishOptions

::: asynk.Consumer

::: asynk.Delivery

::: asynk.transport.redis.RedisProducer

::: asynk.transport.redis.RedisConsumer

## Codec

::: asynk.Codec

::: asynk.JsonCodec

::: asynk.Envelope

## Store

::: asynk.Store

::: asynk.MemoryStore

::: asynk.NullStore

::: asynk.store.redis.RedisStore

::: asynk.store.polling.poll

## Results

::: asynk.TaskHandle

::: asynk.TaskState

::: asynk.TaskResult

::: asynk.TaskStatus

## Errors

::: asynk.AsynkError

::: asynk.NotConfiguredError

::: asynk.EncodeError

::: asynk.DecodeError

::: asynk.InvalidEnvelopeError

::: asynk.CronError

::: asynk.TaskError

::: asynk.TaskNotFoundError

::: asynk.TaskTimeoutError

::: asynk.DeliveryFailedError
