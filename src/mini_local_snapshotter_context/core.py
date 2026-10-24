"""Context-local key-value snapshotter with serialized token propagation.

Design decisions (the non-obvious ones):

- We intentionally use `contextvars.ContextVar` rather than `threading.local`.
  asyncio copies the context per task, so a snapshot taken in one task does not
  leak into sibling tasks. `threading.local` would silently share state across
  tasks scheduled on the same thread, which is a classic source of corruption
  in async code.

- Tokens are self-contained: the serialized string embeds everything needed to
  restore the snapshot in a fresh process. We do not ship a side-table.

- Serialization is `base64(json)` for a reason: json round-trips the basic types
  we support (str, int, float, bool, None) deterministically, and base64 makes
  the token safe to pass through shells, headers, and environments without
  quoting games. We are NOT trying to be compact; if you have 10 MB of context,
  this is the wrong library.

- Bytes and other types are rejected explicitly at set time rather than failing
  later during serialization. Fail close to the user's mistake.

- On restore, unknown keys do not raise. The whole point of the token is to be
  robust across versions of the producer; a strict checker would break every
  deploy. We accept the trade that a typo silently passes.
"""

from __future__ import annotations

import base64
import contextvars
import json
from typing import Any

__all__ = ["Snapshotter", "SnapshotError"]

# The set of JSON-native scalar types we allow as values. We deliberately do
# not accept list/dict because nested mutability across a process boundary is
# a bug factory: the producer mutates after capture, the consumer sees stale
# state, nobody knows who owns the object.
_ALLOWED_VALUE_TYPES: tuple = (str, int, float, bool, type(None))


class SnapshotError(ValueError):
    """Raised when a value or token cannot be handled by this snapshotter."""


class Snapshotter:
    """Captures a context-local dict and round-trips it through a token string.

    The snapshot lives in a `contextvars.ContextVar`, so each asyncio task (and
    each thread) has its own isolated view. Use `set` to store a value in the
    current context, `capture` to snapshot everything set so far into a token,
    and `restore` to load a token into a (possibly different) context.
    """

    def __init__(self, name: str = "mini_local_snapshotter_context") -> None:
        # A fresh ContextVar per Snapshotter instance keeps unrelated instances
        # from sharing storage. We derive the var name from the user-provided
        # name purely for debuggability.
        self._var: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
            f"snapshotter:{name}", default={}
        )

    def set(self, key: str, value: Any) -> None:
        """Store `value` under `key` in the current context.

        Raises:
            SnapshotError: if `key` is not a str or `value` is not a scalar
                type we can serialize.
        """
        if not isinstance(key, str):
            raise SnapshotError(f"key must be str, got {type(key).__name__}")
        if not isinstance(value, _ALLOWED_VALUE_TYPES):
            raise SnapshotError(
                f"value for {key!r} must be one of "
                f"{', '.join(t.__name__ for t in _ALLOWED_VALUE_TYPES)}, "
                f"got {type(value).__name__}"
            )
        current = dict(self._var.get())
        current[key] = value
        # Re-assign a fresh dict so we never hand out a reference that a caller
        # could mutate behind our back.
        self._var.set(current)

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for `key` in the current context, or `default`."""
        return self._var.get().get(key, default)

    def current(self) -> dict[str, Any]:
        """Return a copy of the full current snapshot."""
        return dict(self._var.get())

    def clear(self) -> None:
        """Reset the current context's snapshot to empty."""
        self._var.set({})

    def capture(self) -> str:
        """Serialize the current context's snapshot into a propagation token."""
        payload = json.dumps(
            self._var.get(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")

    def restore(self, token: str) -> dict[str, Any]:
        """Load `token` into the current context and return the restored dict.

        Raises:
            SnapshotError: if the token is malformed or contains values we
                cannot accept.
        """
        if not isinstance(token, str):
            raise SnapshotError(f"token must be str, got {type(token).__name__}")
        try:
            raw = base64.urlsafe_b64decode(token.encode("ascii"))
            decoded = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SnapshotError(f"invalid snapshot token: {exc}") from exc
        if not isinstance(decoded, dict):
            raise SnapshotError(
                f"token payload must decode to a JSON object, got {type(decoded).__name__}"
            )
        # Validate keys and values before committing to the context, so a
        # half-restored state on failure is impossible.
        normalized: dict[str, Any] = {}
        for key, value in decoded.items():
            if not isinstance(key, str):
                raise SnapshotError(f"token key must be str, got {type(key).__name__}")
            if not isinstance(value, _ALLOWED_VALUE_TYPES):
                raise SnapshotError(
                    f"token value for {key!r} is not a supported scalar type "
                    f"(got {type(value).__name__})"
                )
            normalized[key] = value
        self._var.set(dict(normalized))
        return dict(normalized)
