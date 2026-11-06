import ast
import importlib
import inspect
import pkgutil
from types import ModuleType

import asynk
import asynk.errors
from asynk import AsynkError, DeliveryFailedError, Envelope, TaskResult, TaskStatus

PUBLIC_ERRORS = {
    "AsynkError",
    "NotConfiguredError",
    "EncodeError",
    "DecodeError",
    "InvalidEnvelopeError",
    "CronError",
    "TaskError",
    "TaskNotFoundError",
    "DeliveryFailedError",
}
# Private exceptions allowed outside errors.py, as module.qualname.
PRIVATE_OUTSIDE = {"asynk.cli.CliError"}


def _modules() -> list[ModuleType]:
    names = [m.name for m in pkgutil.walk_packages(asynk.__path__, "asynk.")]
    return [asynk, *(importlib.import_module(name) for name in names)]


def _defined_exceptions(module: ModuleType) -> list[type[BaseException]]:
    return [
        obj
        for obj in vars(module).values()
        if inspect.isclass(obj)
        and issubclass(obj, BaseException)
        and obj.__module__ == module.__name__
    ]


def test_errors_module_defines_the_public_errors() -> None:
    assert {c.__name__ for c in _defined_exceptions(asynk.errors)} == PUBLIC_ERRORS


def test_public_errors_are_exported() -> None:
    for name in PUBLIC_ERRORS:
        assert name in asynk.__all__
        assert getattr(asynk, name) is getattr(asynk.errors, name)
    assert "TaskFailedError" not in asynk.__all__


def test_every_asynk_exception_derives_from_asynk_error() -> None:
    assert AsynkError.__bases__ == (Exception,)
    for module in _modules():
        for cls in _defined_exceptions(module):
            assert issubclass(cls, AsynkError), cls
            assert not issubclass(cls, (ValueError, LookupError, RuntimeError)), cls


def test_no_exception_is_defined_outside_errors() -> None:
    outside = {
        f"{c.__module__}.{c.__qualname__}"
        for m in _modules()
        if m is not asynk.errors
        for c in _defined_exceptions(m)
    }
    assert outside == PRIVATE_OUTSIDE


def test_old_locations_are_gone() -> None:
    import asynk.app
    import asynk.codec
    import asynk.hooks

    assert not hasattr(asynk.hooks, "TaskFailedError")
    assert not hasattr(asynk.codec, "DecodeError")
    assert not hasattr(asynk.codec, "EncodeError")
    assert not hasattr(asynk.app, "NotConfiguredError")


def _runtime_imports(tree: ast.Module) -> list[str]:
    modules: list[str] = []

    def visit(nodes: list[ast.stmt]) -> None:
        for node in nodes:
            if isinstance(node, ast.If) and "TYPE_CHECKING" in ast.unparse(node.test):
                continue
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                modules.append(node.module or "")
            else:
                for field in ("body", "orelse", "finalbody"):
                    visit(getattr(node, field, []))

    visit(tree.body)
    return modules


def test_errors_module_imports_nothing_from_asynk() -> None:
    tree = ast.parse(inspect.getsource(asynk.errors))
    assert not [m for m in _runtime_imports(tree) if m.startswith("asynk")]


def test_delivery_failed_error_carries_the_result() -> None:
    result = TaskResult(Envelope.create("job", (), {}), TaskStatus.FAILED)
    exc = DeliveryFailedError(result)
    assert exc.result is result
    assert str(exc) == "task 'job' ended with FAILED"
