"""Static-only checks, verified by mypy (not collected by pytest)."""

from asynk.app import App

app = App("typing")


@app.task()
async def add(a: int, b: int = 0) -> int:
    return a + b


async def _checks() -> None:
    await add.delay(1, b=2)
    await add.delay("x")  # type: ignore[arg-type]
    await add.delay()  # type: ignore[call-arg]
    await add.delay(1, c=3)  # type: ignore[call-arg]
