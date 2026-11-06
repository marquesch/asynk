"""Publishing a task call with metadata attached to its envelope."""

import asyncio

from asynk import App, Envelope


class PrintingProducer:
    """A producer that prints each envelope's metadata instead of sending it."""

    async def publish(self, envelope: Envelope, body: bytes) -> str:
        print(envelope.task, envelope.metadata)
        return envelope.id


app = App("metadata", producer=PrintingProducer())


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    await add.with_metadata(source="api").delay(1, b=2)

    # Each call returns a new bound task; later keys override earlier ones.
    traced = add.with_metadata(source="api", trace="x")
    await traced.with_metadata(trace="y").delay(1, b=2)


if __name__ == "__main__":
    asyncio.run(main())
