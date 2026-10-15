#!/usr/bin/env python3
"""Summarize retained timing logs without recording or revealing typed characters."""
import argparse
import collections
import datetime
import json
import pathlib
import re


def summarize(lines, since_us=0):
    stages = collections.defaultdict(collections.Counter)
    held = collections.defaultdict(list)
    pressed = {}
    controls = collections.Counter()
    samples = []
    native_wait = []
    source_holds = []
    native_pressed = {}
    versions = []
    for line in lines:
        if line.startswith("schema="):
            version = dict(re.findall(r"(\w+)=([^\s]+)", line))
            if not versions or versions[-1] != version:
                versions.append(version)
                pressed.clear()
                native_pressed.clear()
            continue
        stamp = re.search(r"utc_us=(\d+)", line)
        if not stamp or int(stamp[1]) < since_us:
            continue
        utc_us = int(stamp[1])
        sample = re.search(r"sample_ms=(\d+) dropped=(\d+)", line)
        if sample:
            item = {"utc_us": utc_us, "interval_ms": int(sample[1]), "dropped": int(sample[2])}
            for field in ("motion", "motion_gap_max_us", "operation_max_us"):
                value = re.search(field + r"=(\[[\d, ]+\])", line)
                if value:
                    item[field] = json.loads(value[1])
            value = re.search(r"native_pointer_wait_max_ms=(\d+)", line)
            if value:
                item["native_pointer_wait_max_ms"] = int(value[1])
            samples.append(item)
            continue
        location = re.search(r"stage=(\w+) peer=(\w+)", line)
        if not location:
            continue
        stage, peer = location.groups()
        control = re.search(r"control kind=(\w+)", line)
        if control:
            controls[control[1]] += 1
            if stage == "Capture" and control[1] == "Release":
                # Native key-up may belong to the local machine after capture ends.
                pressed = {k: v for k, v in pressed.items() if k[0] not in ("Native", "Capture")}
                native_pressed.clear()
            if stage == "Receive" and control[1] == "Leave":
                pressed = {k: v for k, v in pressed.items() if k[:2] != (stage, peer)}
            continue
        native = re.search(r"native_key token=(\w+) state=(\d+) message_ms=(\d+) observed_ms=(\d+) wait_ms=(\d+) injected=(true|false) modifier=(true|false)", line)
        if native:
            token, state, message, _, wait, injected, modifier = native.groups()
            if injected == "false":
                native_wait.append(int(wait))
            else:
                continue
            key = (peer, token)
            if state == "1":
                native_pressed.setdefault(key, (int(message), modifier))
            elif state == "0" and key in native_pressed:
                down, role = native_pressed.pop(key)
                if role == "false":
                    source_holds.append((int(message) - down) & 0xffffffff)
            continue
        event = re.search(r"\b(key|button) token=(\w+) state=(\d+)", line)
        if not event:
            continue
        kind, token, state = event.groups()
        elapsed_us = int(re.search(r"elapsed_us=(\d+)", line)[1])
        group = stage + ":" + kind
        stages[group]["down" if state == "1" else "up"] += 1
        if stage == "Suppressed":
            continue
        key = (stage, peer, kind, token)
        role = re.search(r"modifier=(true|false)", line)
        modifier = role[1] == "true" if role else (False if kind == "button" else None)
        if state == "1":
            if key in pressed:
                stages[group]["duplicate_down"] += 1
            else:
                pressed[key] = (elapsed_us, modifier)
        elif key in pressed:
            down, role = pressed.pop(key)
            if role is False:
                held[group].append((elapsed_us - down) / 1000)
        else:
            stages[group]["unmatched_up"] += 1
    for key in pressed:
        stages[key[0] + ":" + key[2]]["open_at_end"] += 1
    return {
        "builds": versions,
        "stages": dict(stages),
        "max_non_modifier_held_ms": {k: max(v) for k, v in held.items() if v},
        "native_key_wait_max_ms": max(native_wait, default=None),
        "native_non_modifier_hold_max_ms": max(source_holds, default=None),
        "control_reasons": dict(controls),
        "record_drops": sum(s["dropped"] for s in samples),
        "sample_interval_max_ms": max((s["interval_ms"] for s in samples), default=None),
        "native_pointer_wait_max_ms": max((s.get("native_pointer_wait_max_ms", 0) for s in samples), default=None),
        "motion_total": [sum(s.get("motion", [0] * 8)[i] for s in samples) for i in range(8)],
        "motion_gap_max_us": [max((s.get("motion_gap_max_us", [0] * 8)[i] for s in samples), default=0) for i in range(8)],
        "operation_max_us": [max((s.get("operation_max_us", [0] * 8)[i] for s in samples), default=0) for i in range(8)],
        "limits": ["Open transitions and unmatched releases can cross the selected time window or retained file boundary.",
                   "Movement gaps also include intentional pauses. Sent is not receipt; Applied is not screen rendering.",
                   "Compare host-relative durations; wall clocks may differ. A long key hold alone is not proof of a fault."],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=pathlib.Path, help="Oldest retained file first, then current file")
    parser.add_argument("--since", help="ISO time with timezone, e.g. 2026-10-15T13:36:00+08:00")
    args = parser.parse_args()
    since = datetime.datetime.fromisoformat(args.since) if args.since else None
    if since is not None and since.tzinfo is None:
        parser.error("--since must include a timezone")
    lines = []
    for path in args.logs:
        lines.extend(path.read_text(encoding="utf-8-sig").splitlines())
    print(json.dumps(summarize(lines, int(since.timestamp() * 1e6) if since else 0), indent=2))


if __name__ == "__main__":
    main()
