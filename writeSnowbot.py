#!/usr/bin/env python3
"""
writeSnowbot_fixed.py

Publish a Snowbot app command to an MQTT broker and listen for responses.

Key behavior aligned to request:
- Only print a line when the **payload JSON** contains a field `"topic"` that
  **case-insensitively equals** the issued *command*.
- If `--req-id` is provided, a response counts as a match **only if** both the
  JSON `"topic"` matches the command **and** JSON `"req_id"` equals the provided
  value.
- By default, **non-matching** messages are **suppressed**.
- If `--outputAll` (or legacy `--showAllOutput` / `--showAllOuput`) is set, 
  non-matching responses are printed as `[INFO ]`.
- As soon as the **first MATCH** is printed, the program stops listening and
  disconnects (early-exit; it does not wait for the entire `--wait` window).

Typical usage:
  python writeSnowbot_fixed.py --host 192.168.50.85 --serial 25070102ATHDG219 \
      read_gps_ref '{}'

This will subscribe to default Snowbot response topics and print only the
matching response(s). It exits right after the first match.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List

try:
    import paho.mqtt.client as mqtt
except Exception as e:
    print("Error: paho-mqtt is required. Install with: pip install paho-mqtt", file=sys.stderr)
    raise

# --------------------------- Utilities ------------------------------------

def pretty_json_or_text(b: bytes) -> str:
    try:
        return json.dumps(json.loads(b.decode("utf-8")), ensure_ascii=False, indent=2)
    except Exception:
        try:
            return b.decode("utf-8", errors="replace")
        except Exception:
            return repr(b)

def load_json_arg(maybe_json: Optional[str]) -> Optional[dict]:
    if maybe_json is None:
        return {}
    s = maybe_json.strip()
    if s == "":
        return {}
    try:
        return json.loads(s)
    except Exception:
        # Not valid JSON → treat as raw string payload by returning None
        return None

# Default response topics for a Snowbot device by serial

def default_response_topics(serial: str, command: str) -> List[str]:
    base = f"snowbot/{serial}"
    return [
        f"{base}/device/data_feedback",   # observed primary data feedback
        f"{base}/app/+/reply",            # generic app replies
        f"{base}/app/+/ack",              # ack topics if used
        f"{base}/msg/#",                  # misc messages
    ]

# --------------------------- Collector ------------------------------------

class Collector:
    def __init__(
        self,
        expected_topic_value: Optional[str],
        expect_req_id: Optional[str] = None,
        show_all: bool = False,
    ) -> None:
        self.messages: List[tuple] = []
        self.expect_topic_value = (expected_topic_value or None)
        self.expect_req_id = expect_req_id
        self.show_all = show_all
        self.got_match = False

    def on_message(self, client, userdata, msg):
        # Filter out heartbeat & DeviceMSG unless show_all is on
        if not self.show_all:
            t = msg.topic or ""
            if "heart_beat" in t or "DeviceMSG" in t:
                return

        now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
        body = pretty_json_or_text(msg.payload)

        matched = False
        try:
            obj = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            obj = None

        if isinstance(obj, dict):
            topic_val = obj.get("topic")
            topic_ok = (
                isinstance(topic_val, str)
                and isinstance(self.expect_topic_value, str)
                and topic_val.strip().lower() == self.expect_topic_value.strip().lower()
            )
            if topic_ok:
                if self.expect_req_id is not None:
                    matched = str(obj.get("req_id")) == str(self.expect_req_id)
                else:
                    matched = True

        self.messages.append((now, msg.topic, body, matched))

        if matched:
            self.got_match = True
            print(f"[MATCH] {now}  {msg.topic}\n{body}\n")
        elif self.show_all:
            print(f"[INFO ] {now}  {msg.topic}\n{body}\n")

# --------------------------- Main -----------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Publish a Snowbot app command via MQTT and listen for responses.")

    # Connection / transport
    ap.add_argument("--host", required=True, help="MQTT broker host or IP")
    ap.add_argument("--port", type=int, default=None,
                    help="MQTT port: default 1883 (TCP) or 8083 (WebSockets)")
    ap.add_argument("--ws", action="store_true", help="Use WebSockets transport (default: TCP)")
    ap.add_argument("--tls", action="store_true", help="Enable TLS (for TCP or WebSockets)")
    ap.add_argument("-u", "--username", help="MQTT username")
    ap.add_argument("-p", "--password", help="MQTT password")
    ap.add_argument("--client-id", default="snowbot-writer", help="MQTT client id")
    ap.add_argument("--keepalive", type=int, default=30, help="Keepalive seconds (default 30)")

    # Publish
    ap.add_argument("--qos", type=int, default=0, choices=[0, 1, 2], help="Publish QoS (default 0)")
    ap.add_argument("--retain", action="store_true", help="Publish with retain flag")

    # Target
    ap.add_argument("--serial", required=True, help="Snowbot device serial")

    # Response listening
    ap.add_argument("--wait", type=float, default=3.0,
                    help="Max seconds to wait; exits early on first match (default 3.0)")
    ap.add_argument("--resp-topic", action="append", default=None,
                    help="Extra/override response topic(s); repeatable. If set, replaces defaults.")
    ap.add_argument("--req-id", help="Inject req_id into JSON payload and require it to match in responses.")

    # Output controls
    ap.add_argument("--outputAll", action="store_true",
                    help="Also print non-matching response messages (as [INFO ])")
    ap.add_argument("--showAllOutput", action="store_true",
                    help="Legacy alias of --outputAll")
    ap.add_argument("--showAllOuput", action="store_true",
                    help=argparse.SUPPRESS)  # legacy misspelling

    # Utilities
    ap.add_argument("--dry-run", action="store_true",
                    help="Print planned publish + subscriptions and exit.")

    # Positional: command and optional JSON payload string
    ap.add_argument("command", help="App command to publish (e.g., read_gps_ref)")
    ap.add_argument("payload", nargs="?", default="{}",
                    help="JSON string payload (default '{}'). If not valid JSON, sent as raw text.")

    return ap


def main() -> None:
    ap = build_arg_parser()
    args = ap.parse_args()

    # Determine transport + port
    transport = "websockets" if args.ws else "tcp"
    port = args.port
    if port is None:
        port = 8083 if args.ws else 1883

    # Build publish topic & payload
    pub_topic = f"snowbot/{args.serial}/app/{args.command}"

    # Try to interpret payload as JSON
    json_payload = load_json_arg(args.payload)
    if json_payload is not None and args.req_id:
        # Inject req_id for correlation if using JSON payloads
        json_payload = dict(json_payload)  # shallow copy
        json_payload["req_id"] = args.req_id

    if args.dry_run:
        # Determine response topics
        resp_topics = args.resp_topic if args.resp_topic else default_response_topics(args.serial, args.command)
        print("DRY RUN — will publish & listen:")
        print(f"  Host:     {args.host}:{port}  (transport={transport}, tls={args.tls})")
        print(f"  ClientID: {args.client_id}")
        print(f"  Publish:  {pub_topic}  qos={args.qos} retain={args.retain}")
        print(f"  Payload:  {json.dumps(json_payload) if json_payload is not None else args.payload}")
        print("  Subscribe:")
        for rt in resp_topics:
            print(f"    - {rt}")
        print(f"  Wait:     {args.wait} s (early-exit on first match)")
        print(f"  Show all: {bool(args.outputAll or args.showAllOutput or args.showAllOuput)}")
        return

    # Prepare client
    client = mqtt.Client(client_id=args.client_id, transport=transport, protocol=mqtt.MQTTv311)
    if args.username:
        client.username_pw_set(args.username, args.password or "")
    if args.tls:
        try:
            client.tls_set()
        except Exception:
            # Leave as-is if TLS defaults are fine; surface errors if connect fails
            pass

    # Collector and callback
    show_all = bool(getattr(args, "outputAll", False) or getattr(args, "showAllOutput", False) or getattr(args, "showAllOuput", False))
    collector = Collector(expected_topic_value=args.command, expect_req_id=args.req_id, show_all=show_all)
    client.on_message = collector.on_message

    # Connect
    try:
        client.connect(args.host, port, args.keepalive)
    except Exception as e:
        print(f"ERROR: connect failed: {e}", file=sys.stderr)
        sys.exit(2)

    # Subscribe response topics
    resp_topics = args.resp_topic if args.resp_topic else default_response_topics(args.serial, args.command)
    for rt in resp_topics:
        try:
            client.subscribe(rt, qos=0)
            # Only show subscription list if outputAll for visibility
            if show_all:
                print(f"Subscribed: {rt}")
        except Exception as e:
            print(f"WARN: subscribe failed for {rt}: {e}", file=sys.stderr)

    # Publish
    try:
        if json_payload is not None:
            payload_bytes = json.dumps(json_payload, ensure_ascii=False).encode("utf-8")
        else:
            payload_bytes = str(args.payload).encode("utf-8")
        client.publish(pub_topic, payload=payload_bytes, qos=args.qos, retain=args.retain)
    except Exception as e:
        print(f"ERROR: publish failed: {e}", file=sys.stderr)
        try:
            client.disconnect()
        finally:
            pass
        sys.exit(2)

    # Pump network until first match or timeout
    client.loop_start()
    start = time.time()
    deadline = start + (args.wait or 0)
    try:
        while True:
            if collector.got_match:
                break
            if args.wait is not None and time.time() >= deadline:
                break
            time.sleep(0.05)
    finally:
        client.loop_stop()
        client.disconnect()

    # Exit status: if --req-id was provided and we saw >=1 MATCH, return 0; else 1
    if args.req_id:
        matched = any(m[3] for m in collector.messages)
        sys.exit(0 if matched else 1)
    else:
        # No correlation requested; success if we reached here
        sys.exit(0)


if __name__ == "__main__":
    main()
