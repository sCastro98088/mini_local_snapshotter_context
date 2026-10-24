import asyncio
import base64
import unittest

from mini_local_snapshotter_context import Snapshotter, SnapshotError


class TestSnapshotterBasics(unittest.TestCase):
    def test_set_and_get_roundtrip(self):
        s = Snapshotter()
        s.set("request_id", "abc-123")
        self.assertEqual(s.get("request_id"), "abc-123")
        self.assertIsNone(s.get("missing"))

    def test_get_with_default(self):
        s = Snapshotter()
        self.assertEqual(s.get("missing", "fallback"), "fallback")

    def test_set_overwrites(self):
        s = Snapshotter()
        s.set("status", "pending")
        s.set("status", "done")
        self.assertEqual(s.get("status"), "done")

    def test_current_returns_copy(self):
        s = Snapshotter()
        s.set("a", 1)
        snap = s.current()
        snap["a"] = 999
        self.assertEqual(s.get("a"), 1)

    def test_clear(self):
        s = Snapshotter()
        s.set("x", 1)
        s.clear()
        self.assertEqual(s.current(), {})


class TestSerialization(unittest.TestCase):
    def test_capture_then_restore_preserves_all_scalars(self):
        s = Snapshotter()
        s.set("s", "text")
        s.set("i", 42)
        s.set("f", 3.25)
        s.set("b", True)
        s.set("n", None)
        s.set("neg", -7)
        s.set("zero", 0)
        s.set("empty", "")
        token = s.capture()

        other = Snapshotter()
        result = other.restore(token)
        self.assertEqual(result, {
            "s": "text",
            "i": 42,
            "f": 3.25,
            "b": True,
            "n": None,
            "neg": -7,
            "zero": 0,
            "empty": "",
        })

    def test_restore_to_context_and_back(self):
        s = Snapshotter()
        s.set("user", "alice")
        s.set("count", 3)
        token = s.capture()

        other = Snapshotter()
        other.restore(token)
        self.assertEqual(other.get("user"), "alice")
        self.assertEqual(other.get("count"), 3)
        self.assertEqual(other.get("user", default="default"), "alice")

    def test_capture_empty_context(self):
        s = Snapshotter()
        token = s.capture()
        other = Snapshotter()
        self.assertEqual(other.restore(token), {})

    def test_token_is_ascii_str(self):
        s = Snapshotter()
        s.set("k", "v")
        token = s.capture()
        self.assertIsInstance(token, str)
        token.encode("ascii")

    def test_token_is_url_safe(self):
        s = Snapshotter()
        # Unicode value exercises encoding; token must be url-safe.
        s.set("name", "café")
        token = s.capture()
        self.assertNotIn("+", token)
        self.assertNotIn("/", token)

    def test_restore_into_existing_context_replaces(self):
        s = Snapshotter()
        s.set("old", 1)
        s.set("kept", 2)
        token = s.capture()

        other = Snapshotter()
        other.set("old", 999)
        other.set("extra", 5)
        other.restore(token)
        self.assertEqual(other.get("old"), 1)
        self.assertEqual(other.get("kept"), 2)
        self.assertIsNone(other.get("extra"))


class TestValidation(unittest.TestCase):
    def test_non_string_key_rejected(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.set(123, "v")

    def test_bytes_value_rejected_at_set(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.set("k", b"bytes")

    def test_list_value_rejected_at_set(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.set("k", [1, 2, 3])

    def test_dict_value_rejected_at_set(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.set("k", {"nested": True})

    def test_malformed_token_raises(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.restore("not-a-real-token!!")

    def test_non_string_token_rejected(self):
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.restore(b"not-a-string")  # type: ignore[arg-type]

    def test_valid_base64_non_object_payload_rejected(self):
        import json
        payload = json.dumps([1, 2, 3]).encode("utf-8")
        token = base64.urlsafe_b64encode(payload).decode("ascii")
        s = Snapshotter()
        with self.assertRaises(SnapshotError):
            s.restore(token)

    def test_restore_does_not_partially_apply_on_failure(self):
        s = Snapshotter()
        s.set("pre", "exists")
        import json
        # Build a payload that decodes to a dict but has an invalid value type
        # so validation fails AFTER parsing.
        payload = json.dumps({"bad": [1, 2]}).encode("utf-8")
        token = base64.urlsafe_b64encode(payload).decode("ascii")
        with self.assertRaises(SnapshotError):
            s.restore(token)
        # Pre-existing value must be untouched.
        self.assertEqual(s.get("pre"), "exists")
        self.assertIsNone(s.get("bad"))


class TestFloats(unittest.TestCase):
    def test_float_roundtrip_no_equality_compare(self):
        # We do NOT compare floats with ==. We compare their repr, which for
        # JSON-safe floats (those that survive json round-trip) is stable.
        s = Snapshotter()
        s.set("f", 0.5)
        token = s.capture()
        other = Snapshotter()
        result = other.restore(token)
        self.assertEqual(repr(result["f"]), repr(0.5))


class TestIsolation(unittest.TestCase):
    def test_separate_instances_isolated(self):
        a = Snapshotter()
        b = Snapshotter()
        a.set("k", "a")
        b.set("k", "b")
        self.assertEqual(a.get("k"), "a")
        self.assertEqual(b.get("k"), "b")

    def test_async_task_isolation(self):
        # The defining feature of contextvars: each task gets its own copy.
        # A snapshot taken inside one task must not bleed into another.
        s = Snapshotter()

        async def writer():
            s.set("task_key", "writer")
            return s.capture()

        async def reader():
            # No set has happened in THIS task's context; the writer's set
            # must not be visible here.
            return s.get("task_key")

        async def main():
            token = await asyncio.create_task(writer())
            seen_in_reader = await asyncio.create_task(reader())
            seen_in_main = s.get("task_key")
            return token, seen_in_reader, seen_in_main

        token, seen_in_reader, seen_in_main = asyncio.run(main())
        self.assertIsNotNone(token)
        self.assertIsNone(seen_in_reader)
        self.assertIsNone(seen_in_main)

    def test_restore_does_not_leak_into_sibling_task(self):
        s = Snapshotter()

        async def restorer():
            s.restore(_make_token({"x": 1}))
            return s.get("x")

        async def observer():
            return s.get("x")

        async def main():
            await asyncio.create_task(restorer())
            return await asyncio.create_task(observer())

        result = asyncio.run(main())
        self.assertIsNone(result)


def _make_token(payload_dict):
    import json
    payload = json.dumps(payload_dict, sort_keys=True, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")


if __name__ == "__main__":
    unittest.main()
