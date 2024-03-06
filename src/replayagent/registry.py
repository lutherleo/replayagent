"""Workflow and activity registration.

Workflows and activities are plain Python functions. Decorate them with
:func:`workflow` / :func:`activity` to attach a name and default options, then collect
them in an :class:`App`, which is what workers, clients and the CLI load.
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from replayagent.errors import UnknownActivity, UnknownWorkflowType
from replayagent.retry import RetryPolicy

if TYPE_CHECKING:
    from replayagent.replay import WorkflowContext

Delivery = Literal["at_least_once", "at_most_once"]

WorkflowFn = Callable[["WorkflowContext", Any], Any]

_ACTIVITY_ATTR = "__replayagent_activity__"
_WORKFLOW_ATTR = "__replayagent_workflow__"


@dataclass(frozen=True)
class ActivityOptions:
    """Per-activity execution options. ``None`` fields inherit from the next level up
    (call site -> decorator -> engine defaults)."""

    retry: RetryPolicy | None = None
    timeout: float | None = None
    delivery: Delivery | None = None

    def __post_init__(self) -> None:
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be > 0 seconds")
        if self.delivery not in (None, "at_least_once", "at_most_once"):
            raise ValueError("delivery must be 'at_least_once' or 'at_most_once'")

    def merged(self, override: ActivityOptions | None) -> ActivityOptions:
        if override is None:
            return self
        return replace(
            self,
            retry=override.retry if override.retry is not None else self.retry,
            timeout=override.timeout if override.timeout is not None else self.timeout,
            delivery=override.delivery if override.delivery is not None else self.delivery,
        )

    def to_history(self) -> dict[str, Any]:
        """Fully resolved options, as recorded in ``ActivityScheduled``."""
        return {
            "retry": (self.retry or RetryPolicy()).to_dict(),
            "timeout": self.timeout,
            "delivery": self.delivery or "at_least_once",
        }


@dataclass(frozen=True)
class ActivityDefinition:
    name: str
    fn: Callable[..., Any]
    options: ActivityOptions


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    fn: WorkflowFn
    code_hash: str


def code_fingerprint(fn: Callable[..., Any]) -> str:
    """Hash of a function's source; recorded at start so ``show`` can flag code drift."""
    try:
        source = inspect.getsource(fn)
    except (OSError, TypeError):
        source = f"{getattr(fn, '__module__', '?')}.{getattr(fn, '__qualname__', repr(fn))}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:12]


@overload
def activity[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def activity[F: Callable[..., Any]](
    *,
    name: str | None = None,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
    delivery: Delivery = "at_least_once",
) -> Callable[[F], F]: ...
def activity[F: Callable[..., Any]](
    fn: F | None = None,
    /,
    *,
    name: str | None = None,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
    delivery: Delivery = "at_least_once",
) -> F | Callable[[F], F]:
    """Mark a function as an activity (a step whose result is recorded in history)."""

    def wrap(func: F) -> F:
        defn = ActivityDefinition(
            name=name or func.__name__,
            fn=func,
            options=ActivityOptions(retry=retry, timeout=timeout, delivery=delivery),
        )
        setattr(func, _ACTIVITY_ATTR, defn)
        return func

    return wrap(fn) if fn is not None else wrap


@overload
def workflow[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def workflow[F: Callable[..., Any]](*, name: str | None = None) -> Callable[[F], F]: ...
def workflow[F: Callable[..., Any]](
    fn: F | None = None, /, *, name: str | None = None
) -> F | Callable[[F], F]:
    """Mark a function ``fn(ctx, input) -> result`` as a workflow."""

    def wrap(func: F) -> F:
        defn = WorkflowDefinition(
            name=name or func.__name__,
            fn=cast(WorkflowFn, func),
            code_hash=code_fingerprint(func),
        )
        setattr(func, _WORKFLOW_ATTR, defn)
        return func

    return wrap(fn) if fn is not None else wrap


def activity_name_of(act: str | Callable[..., Any]) -> str:
    """Resolve the registered name for an activity reference (name or function)."""
    if isinstance(act, str):
        return act
    defn = getattr(act, _ACTIVITY_ATTR, None)
    if isinstance(defn, ActivityDefinition):
        return defn.name
    return str(getattr(act, "__name__", repr(act)))


class App:
    """A registry of workflows and activities."""

    def __init__(
        self,
        *,
        workflows: Iterable[Callable[..., Any]] = (),
        activities: Iterable[Callable[..., Any]] = (),
    ) -> None:
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._activities: dict[str, ActivityDefinition] = {}
        for wf in workflows:
            self.register_workflow(wf)
        for act in activities:
            self.register_activity(act)

    def register_workflow(self, fn: Callable[..., Any], *, name: str | None = None) -> str:
        defn = getattr(fn, _WORKFLOW_ATTR, None)
        if not isinstance(defn, WorkflowDefinition):
            defn = WorkflowDefinition(
                name=fn.__name__, fn=cast(WorkflowFn, fn), code_hash=code_fingerprint(fn)
            )
        if name:
            defn = replace(defn, name=name)
        if defn.name in self._workflows and self._workflows[defn.name].fn is not defn.fn:
            raise ValueError(f"workflow {defn.name!r} is already registered")
        self._workflows[defn.name] = defn
        return defn.name

    def register_activity(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        options: ActivityOptions | None = None,
    ) -> str:
        defn = getattr(fn, _ACTIVITY_ATTR, None)
        if not isinstance(defn, ActivityDefinition):
            defn = ActivityDefinition(name=fn.__name__, fn=fn, options=ActivityOptions())
        if name:
            defn = replace(defn, name=name)
        if options is not None:
            defn = replace(defn, options=defn.options.merged(options))
        if defn.name in self._activities and self._activities[defn.name].fn is not defn.fn:
            raise ValueError(f"activity {defn.name!r} is already registered")
        self._activities[defn.name] = defn
        return defn.name

    def get_workflow(self, name: str) -> WorkflowDefinition:
        try:
            return self._workflows[name]
        except KeyError:
            raise UnknownWorkflowType(f"workflow type {name!r} is not registered") from None

    def get_activity(self, name: str) -> ActivityDefinition:
        try:
            return self._activities[name]
        except KeyError:
            raise UnknownActivity(f"activity {name!r} is not registered") from None

    def has_activity(self, name: str) -> bool:
        return name in self._activities

    @property
    def workflow_names(self) -> list[str]:
        return sorted(self._workflows)

    @property
    def activity_names(self) -> list[str]:
        return sorted(self._activities)

    def resolve_workflow(self, wf: str | Callable[..., Any]) -> WorkflowDefinition:
        if isinstance(wf, str):
            return self.get_workflow(wf)
        defn = getattr(wf, _WORKFLOW_ATTR, None)
        return self.get_workflow(defn.name if isinstance(defn, WorkflowDefinition) else wf.__name__)
