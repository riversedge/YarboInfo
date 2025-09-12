#!/usr/bin/env python3
"""
writeSnowbot.py

Publish arbitrary Snowbot app commands to an MQTT broker and listen for responses.

It constructs request topics like:
  snowbot/<SERIAL>/app/<COMMAND>

…and sends a JSON payload you provide (or {} if omitted).
Then it subscribes to one or more response topics and prints anything received
within a wait window.

By default, noisy topics containing 'heart_beat' or 'DeviceMSG' are suppressed
from output unless you pass --showAllOutput (or the alias --showAllOuput).

Examples
--------
# Minimal (defaults to TCP :1883, QoS 0). Listen 3s on default response topics.
python writeSnowbot.py --host 192.168.50.85 --serial 25070102ATHDG219 resume

# With JSON string and longer wait
python writeSnowbot.py --host 192.168.50.85 --serial 25070102ATHDG219 cmd_vel '{"vel":0.0,"rev":0.0}' --wait 5

# JSON from file
python writeSnowbot.py --host 192.168.50.85 --serial 25070102ATHDG219 light_ctrl @payloads/led_off.json

# WebSockets & auth, custom response topics (you can repeat --resp-topic)
python writeSnowbot.py --host 192.168.50.85 --ws -u mqtt -p pass \
  --serial 25070102ATHDG219 set_working_state '{"state":1}' \
  --resp-topic 'snowbot/+/device/#' --resp-topic 'snowbot/+/app/ack'

# Show all incoming messages (don't suppress heartbeats/DeviceMSG)
python writeSnowbot.py --host 192.168.50.85 --serial 25070102ATHDG219 --showAllOutput get_map

# Dry run: show what would be sent and what topics would be listened to
python writeSnowbot.py --host 192.168.50.85 --serial 25070102ATHDG219 --dry-run cmd_recharge '{"cmd":2}'

Notes
-----
- You can publish ANY command; --list only prints commonly observed ones.
- If no JSON is provided, {} is sent.
- Default response subscriptions (can be overridden):
    snowbot/<SERIAL>/device/#
    snowbot/<SERIAL>/app/ack
    snowbot/<SERIAL>/app/+/reply
    snowbot/<SERIAL>/msg/#

- Use --req-id to inject a correlation field (req_id) into your JSON if you want
  to grep for it in responses; many devices won’t echo it, but it can help when they do.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional, List

import paho.mqtt.client as mqtt


KNOWN_COMMANDS = [
    # Observed in your logs (for convenience/help only)
    "push_rod_cmd",
    "cmd_roller",
    "cmd_vel",
    "set_working_state",
    "resume",
    "read_all_plan",
    "get_map",
    "cmd_recharge",
    "get_connect_wifi_name",
    "restart_container",
    "shutdown",
    "pause",
    "light_ctrl",
    "cmd_motor_protect",
    "mower_speed_cmd",
    "mower_target_cmd",
    "stop",
    # Previously used / plausible:
    "check_map_connectivity",
    "start_plan",
]


def load_json_arg(maybe_json: Optional[str]) -> dict:
    """
    Accepts one of:
      - None  -> returns {}
      - JSON string (e.g., '{"vel":0,"rev":0}')
      - File reference starting with '@' (e.g., '@payload.json')
    """
    if not maybe_json:
        return {}

    if maybe_json.startswith("@"):
        path = maybe_json[1:]
        if not os.path.isfile(path):
            raise FileNotFoundError(f"JSON file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    try:
        return json.loads(maybe_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON argument: {e}") from e


def build_topic(serial: str, command: str) -> str:
    serial = serial.strip()
    command = command.strip()
    if not serial:
        raise ValueError("Serial must be non-empty.")
    if not command:
        raise ValueError("Command must be non-empty.")
    return f"snowbot/{serial}/app/{command}"


def default_response_topics(serial: str) -> List[str]:
    s = serial.strip()
    return [
        f"snowbot/{s}/device/#",
        f"snowbot/{s}/app/ack",
        f"snowbot/{s}/app/+/reply",
        f"snowbot/{s}/msg/#",
    ]


def pretty_json_or_text(payload: bytes) -> str:
    txt = payload.decode("utf-8", errors="replace").strip()
    # Try JSON, else fall back to raw text
    try:
        obj = json.loads(txt)
        # Bubble up 'note' if present for quick scanning
        if isinstance(obj, dict) and "note" in obj:
            note = obj.get("note")
            return json.dumps(obj, ensure_ascii=False, indent=2) + f"\n(note: {note})"
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return txt


class Collector:
    def __init__(self, expect_req_id: Optional[str] = None, show_all: bool = False):
        self.messages = []
        self.expect_req_id = expect_req_id
        self.show_all = show_all

    def on_message(self, client, userdata, msg):
        # Filter out heartbeat & DeviceMSG* unless show_all is on
        if not self.show_all:
            t = msg.topic
            if "heart_beat" in t or "DeviceMSG" in t:
                return

        now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
        body = pretty_json_or_text(msg.payload)

        matched = True
        if self.expect_req_id:
            try:
                obj = json.loads(msg.payload.decode("utf-8"))
                if isinstance(obj, dict):
                    matched = str(obj.get("req_id")) == str(self.expect_req_id)
                else:
                    matched = False
            except Exception:
                matched = False

        self.messages.append((now, msg.topic, body, matched))
        tag = "MATCH" if matched else "INFO "
        print(f"[{tag}] {now}  {msg.topic}\n{body}\n")


def main():
    ap = argparse.ArgumentParser(
        description="Publish arbitrary Snowbot app commands via MQTT and listen for responses."
    )
    # Connection / transport
    ap.add_argument("--host", required=False, help="MQTT broker host/IP (required unless --list).")
    ap.add_argument("--port", type=int, help="MQTT broker port (default 1883 for TCP, 8083 for WS).")
    ap.add_argument("--ws", action="store_true", help="Use WebSockets transport (default TCP).")
    ap.add_argument("--tls", action="store_true", help="Enable TLS (use with TCP or WS).")
    ap.add_argument("-u", "--username", help="MQTT username.")
    ap.add_argument("-p", "--password", help="MQTT password.")
    ap.add_argument("--client-id", default="snowbot-writer", help="MQTT client id.")
    ap.add_argument("--keepalive", type=int, default=30, help="MQTT keepalive seconds (default 30).")
    # Publish knobs
    ap.add_argument("--qos", type=int, default=0, choices=[0, 1, 2], help="Publish QoS (default 0).")
    ap.add_argument("--retain", action="store_true", help="Publish with retain flag.")
    # Target addressing
    ap.add_argument("--serial", help="Snowbot serial (e.g., 25070102ATHDG219).")
    # Response listening
    ap.add_argument("--wait", type=float, default=3.0, help="Seconds to wait for responses after publish (default 3.0).")
    ap.add_argument("--resp-topic", action="append", default=None,
                    help="Additional/override response topic(s) to subscribe. May be repeated. "
                         "If provided, replaces defaults.")
    ap.add_argument("--req-id", help="Inject req_id into JSON payload for ad-hoc correlation and prefer matching responses.")
    # Output controls
    ap.add_argument("--showAllOutput", action="store_true",
                    help="If set, also print heart_beat and DeviceMSG messages (normally suppressed).")
    # Backward-compatible alias for the misspelling (--showAllOuput)
    ap.add_argument("--showAllOuput", action="store_true",
                    help=argparse.SUPPRESS)
    # Utilities
    ap.add_argument("--dry-run", action="store_true", help="Show topic/payload and intended subscriptions, then exit.")
    ap.add_argument("--timeout", type=float, default=6.0, help="Seconds to wait for publish ack (default 6.0).")
    ap.add_argument("--list", action="store_true", help="List commonly observed commands and exit.")
    # Positional
    ap.add_argument("command", nargs="?", help="App command to append after snowbot/<serial>/app/ (e.g., cmd_vel).")
    ap.add_argument("json_payload", nargs="?", help="JSON string OR @file.json. Defaults to {} if omitted.")

    args = ap.parse_args()

    if args.list:
        print("Commonly observed commands (you can publish ANY command name):")
        for c in KNOWN_COMMANDS:
            print(f"  - {c}")
        sys.exit(0)

    # Validate requireds
    if not args.host:
        ap.error("--host is required (unless using --list).")
    if not args.serial:
        ap.error("--serial is required.")
    if not args.command:
        ap.error("command is required (e.g., cmd_vel, get_map, light_ctrl, etc.).")

    # Transport and port defaults
    transport = "websockets" if args.ws else "tcp"
    port = args.port if args.port is not None else (8083 if args.ws else 1883)

    # Build request topic & payload
    topic = build_topic(args.serial, args.command)
    try:
        payload_obj = load_json_arg(args.json_payload)
    except (ValueError, FileNotFoundError) as e:
        print(str(e))
        sys.exit(2)

    # Optionally inject a req_id for rough correlation
    if args.req_id:
        if not isinstance(payload_obj, dict):
            print("Warning: --req-id set, but payload is not a JSON object; cannot inject req_id.")
        else:
            payload_obj.setdefault("req_id", args.req_id)

    payload_str = json.dumps(payload_obj, separators=(",", ":"))

    # Determine response topics
    resp_topics = args.resp_topic if args.resp_topic else default_response_topics(args.serial)

    # Effective show_all flag (accept either correct or misspelled option)
    show_all = args.showAllOutput or args.showAllOuput

    # Dry run mode
    if args.dry_run:
        print("DRY RUN — would publish and listen as follows:")
        print(f"  Host:     {args.host}:{port} ({'WS' if args.ws else 'TCP'}{' + TLS' if args.tls else ''})")
        print(f"  ClientID: {args.client_id}")
        print(f"  Publish:  {topic}   QoS={args.qos}  Retain={args.retain}")
        print(f"  Payload:  {payload_str}")
        print("  Subscribe on:")
        for rt in resp_topics:
            print(f"    - {rt}")
        print(f"  Wait:     {args.wait} seconds for responses")
        print(f"  Show all: {show_all}")
        sys.exit(0)

    # MQTT client
    client = mqtt.Client(client_id=args.client_id, transport=transport, protocol=mqtt.MQTTv311)
    if args.username:
        client.username_pw_set(args.username, args.password or "")
    if args.tls:
        client.tls_set()

    # Hooks
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected to MQTT broker.")
        else:
            print(f"MQTT connect failed (rc={rc}).")

    def on_publish(client, userdata, mid):
        print(f"Publish acknowledged (mid={mid}).")

    collector = Collector(expect_req_id=args.req_id, show_all=show_all)

    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_message = collector.on_message

    # Connect
    try:
        client.connect(args.host, port, keepalive=args.keepalive)
    except Exception as e:
        print(f"Failed to connect to MQTT broker at {args.host}:{port} ({transport}) — {e}")
        sys.exit(3)

    client.loop_start()
    time.sleep(0.2)

    # Subscribe to response topics first (so we don't miss quick acks)
    for rt in resp_topics:
        try:
            client.subscribe(rt, qos=0)
            print(f"Subscribed: {rt}")
        except Exception as e:
            print(f"Subscribe failed for {rt}: {e}")

    # Publish
    print(f"Publishing to {topic} with QoS={args.qos}, retain={args.retain}")
    print(f"Payload: {payload_str}")
    res = client.publish(topic, payload=payload_str.encode("utf-8"), qos=args.qos, retain=args.retain)

    # Wait for completion / or timeout
    res.wait_for_publish(timeout=args.timeout)
    if not res.is_published():
        print("Warning: publish may not have completed before timeout.")

    # Keep listening for responses
    if args.wait > 0:
        print(f"Listening for responses ({args.wait:.1f}s)...")
        time.sleep(args.wait)

    client.loop_stop()
    client.disconnect()

    # Exit status: if --req-id was provided and we saw at least one MATCH, return 0; else 1
    if args.req_id:
        matched = any(m[3] for m in collector.messages)
        sys.exit(0 if matched else 1)
    else:
        # No correlation requested; always success if we got this far
        sys.exit(0)


if __name__ == "__main__":
    main()

