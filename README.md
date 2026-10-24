# mini_local_snapshotter_context

Captures a context-local dict of key-value pairs and returns a serialized token string suitable for propagation across process boundaries.

```python
from mini_local_snapshotter_context import Snapshotter

s = Snapshotter()
s.set("request_id", "abc-123")
s.set("user_id", 42)

token = s.capture()
# ... pass `token` through an env var, a header, a queue message ...

other = Snapshotter()
restored = other.restore(token)
assert restored == {"request_id": "abc-123", "user_id": 42}
assert other.get("user_id") == 42
```

## Why this exists

When a long-running process hands work to another process (a worker pool, a subprocess, a fresh interpreter) you often want to carry a small bag of request-scoped scalars — a request id, a user id, a feature flag — without weaving them through every function signature and without standing up a real tracing system. This library is the thinnest possible layer for that job: a `contextvars.ContextVar` holding a dict, plus a `base64(json)` envelope so the bag survives transport.

The trade-off is explicit and one-sided: we only accept JSON scalar types (`str`, `int`, `float`, `bool`, `None`). No bytes, no lists, no nested dicts. The moment you let a mutable container cross a process boundary you have an aliasing problem that no library can fix for you, so we refuse at `set` time rather than failing later inside a serializer. If you need structured values, flatten them yourself before calling `set`.

## The awkward edge

`restore` silently accepts unknown keys from a token produced by a newer producer. Strictly checking keys would break every rolling deploy, so we chose forward-compatibility over safety. A typo in a key name will therefore pass through quietly — rely on the consumers of each key to validate their own input, and do not use snapshot keys as a control-flow mechanism.

A second edge: `restore` replaces the current context's snapshot entirely. If you have local values you need to keep, merge them into the token or capture your own token first and restore it afterwards. There is no per-key merge, by design — merging across versions has too many sensible semantics to pick one.
