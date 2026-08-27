#!/usr/bin/env python3
"""Fail if a `go test -json` stream skipped tests, or ran none at all.

Infra-gated tests (test/e2e) call t.Skip when their env vars are unset, so a
suite that ran nothing exits 0 — indistinguishable from a suite that passed.
That is exactly how test/e2e rotted: CI never ran it, and nothing said so.

Usage:  go test -json ./... | scripts/assert-tests-ran.py
"""
import json
import sys

passed = skipped = failed = 0
skipped_names: list[str] = []

for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        ev = json.loads(line)
    except ValueError:
        continue
    # Package-level events carry no "Test"; only count individual tests.
    if not ev.get("Test"):
        continue
    action = ev.get("Action")
    if action == "pass":
        passed += 1
    elif action == "skip":
        skipped += 1
        skipped_names.append(f'{ev.get("Package", "?")}.{ev["Test"]}')
    elif action == "fail":
        failed += 1

print(f"tests: {passed} passed, {skipped} skipped, {failed} failed")

if failed:
    print(f"::error::{failed} test(s) failed")
    sys.exit(1)

if skipped:
    for name in skipped_names[:20]:
        print(f"  skipped: {name}")
    print(
        f"::error::{skipped} test(s) skipped. These are infra-gated: a skip "
        "means the required services or env vars did not reach them, so this "
        "job would report green having verified nothing."
    )
    sys.exit(1)

if passed == 0:
    print("::error::no tests ran at all")
    sys.exit(1)
