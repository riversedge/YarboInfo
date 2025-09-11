#!/usr/bin/env python3
"""
writeSnowbot.py
Publish control commands to a Snowbot/Yarbo device over MQTT (WebSockets).

Defaults:
- WSS on port 8084.
- TLS used, but certificate verification is DISABLED by default (self-signed OK).
  Use --secure_only (and optionally --cafile) to enforce strict validation.

Subscriptions:
- snowbot/<device_id>/device/data_feedback  (printed)
- snowbot/<device_id>/device/plan_feedback  (printed; disable with --data-feedback-only)
- snowbot/<device_id>/device/heart_beat     (NOT printed; used to guard writes)

Commands (zlib JSON payloads):
  * check_map_connectivity [ids_csv] → {} or {"ids":[...]}
  * get_map                         → {}
  * read_all_plan                   → {}
  * set_working_state <state:int>   → {"state":N}
  * start_plan <id:int>             → {"id":ID,"percent":0}
"""

import argparse, binascii, sys, threading, time, json, zlib
from typing import Optional
import paho.mqtt.client as mqtt

try:
    import ssl
except ImportError:
    ssl = None

# ---------- encoders ----------
def zlib_json(obj: dict) -> bytes:
    # Match app behavior (0x78 0x01 header): deflate level 0
    return zlib.compress(json.dumps(obj, separators=(",", ":")).encode("utf-8"), level=0)

def enc_check_map_connectivity(param: Optional[str]) -> bytes:
    if param:
        ids = [int(x) for x in str(param).split(",") if x.strip() != ""]
        return zlib_json({"ids": ids})
    return zlib_json({})

def enc_get_map(_: Optional[int]) -> bytes:
    return zlib_json({})

def enc_read_all_plan(_: Optional[int]) -> bytes:
    return zlib_json({})

def enc_set_working_state(param: Optional[int]) -> bytes:
    if param is None:
        raise ValueError("set_working_state requires an integer parameter (e.g., 0 or 1).")
    return zlib_json({"state": int(param)})

def enc_start_plan(param: Optional[int]) -> bytes:
    if param is None:
        raise ValueError("start_plan requires an integer plan id parameter.")
    return zlib_json({"id": int(param), "percent": 0})

COMMANDS = {
    "check_map_connectivity": enc_check_map_connectivity,
    "get_map": enc_get_map,
    "read_all_plan": enc_read_all_plan,
    "set_working_state": enc_set_working_state,
    "start_plan": enc_start_plan,
}

# ---------- helpers ----------
def build_topic(device_id, cmd): return f"snowbot/{device_id}/app/{cmd}"

def pretty_hex(b: bytes, limit=256):
    hx = binascii.hexlify(b).decode()
    return hx if len(hx) <= limit else hx[:limit] + "..."

def try_decode(payload: bytes) -> str:
    # 1) try utf-8 (pretty-print JSON if possible)
    try:
        txt = payload.decode("utf-8")
        try:
            return json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
        except Exception:
            return txt
    except UnicodeDecodeError:
        pass
    # 2) try zlib -> utf-8 (pretty JSON if possible)
    try:
        dec = zlib.decompress(payload)
        try:
            txt = dec.decode("utf-8")
            try:
                return json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
            except Exception:
                return txt
        except UnicodeDecodeError:
            return "(zlib) " + pretty_hex(dec)
    except Exception:
        pass
    # 3) hex fallback
    return "(hex) " + pretty_hex(payload)

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Publish Snowbot/Yarbo commands over MQTT (WSS/WS/TCP).")
    ap.add_argument("--host", required=True, help="Broker hostname or IP (no scheme).")
    ap.add_argument("--port", type=int, default=8084, help="8084=WSS (default), 8083=WS.")
    ap.add_argument("--ws-path", default="/mqtt", help="WebSocket path (ignored with --no-ws).")
    ap.add_argument("--no-ws", action="store_true", help="Use TCP MQTT instead of WebSockets.")
    # TLS opts
    ap.add_argument("--secure_only", action="store_true",
                    help="Strict cert verification (default accepts self-signed).")
    ap.add_argument("--cafile", help="Custom CA PEM (use with --secure_only).")
    # Auth
    ap.add_argument("--username"); ap.add_argument("--password")
    ap.add_argument("--client-id", default="snowbot-writer")
    ap.add_argument("--keepalive", type=int, default=60)
    ap.add_argument("--qos", type=int, default=0, choices=[0, 1, 2])
    ap.add_argument("--retain", action="store_true")
    ap.add_argument("--listen-seconds", type=float, default=12.0)
    ap.add_argument("--data-feedback-only", action="store_true",
                    help="Subscribe only to data_feedback (omit plan_feedback).")
    # Target & command
    ap.add_argument("--device-id", required=True)
    ap.add_argument("command", choices=sorted(COMMANDS.keys()))
    ap.add_argument("param", nargs="?",
                    help='For check_map_connectivity, a comma list of IDs (e.g., "739,760,782"); '
                         'for others, an integer (state or plan id).')
    args = ap.parse_args()

    # Build payload
    encoder = COMMANDS[args.command]
    try:
        payload = encoder(args.param)
    except ValueError as e:
        print(f"Parameter error: {e}", file=sys.stderr); sys.exit(2)

    topic_pub = build_topic(args.device_id, args.command)
    topic_feedback = f"snowbot/{args.device_id}/device/data_feedback"
    topic_planfb  = f"snowbot/{args.device_id}/device/plan_feedback"
    topic_heartbeat = f"snowbot/{args.device_id}/device/heart_beat"

    # Client
    transport = "websockets" if not args.no_ws else "tcp"
    client = mqtt.Client(
        client_id=args.client_id,
        transport=transport,
        protocol=mqtt.MQTTv311,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    if transport == "websockets":
        client.ws_set_options(path=args.ws_path)

    # TLS
    if ssl is None:
        print("[error] Python ssl module not available", file=sys.stderr); sys.exit(1)
    if args.secure_only:
        ctx = ssl.create_default_context()
        if args.cafile:
            ctx.load_verify_locations(args.cafile)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
    else:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    client.tls_set_context(ctx)
    try:
        client.tls_insecure_set(not args.secure_only)
    except Exception:
        pass

    if args.username:
        client.username_pw_set(args.username, args.password or "")

    done = threading.Event()

    # Gate: if working_state==1 and not sending set_working_state 0 → abort
    def should_block():
        return not (args.command == "set_working_state" and args.param == "0")

    # v2 callback signatures
    def on_connect(c, u, flags, rc, properties=None):
        if rc == 0:
            scheme = "wss" if transport == "websockets" else "mqtts"
            print(f"[connected] {scheme}://{args.host}:{args.port}  ws_path={args.ws_path if transport=='websockets' else '-'}  secure_only={args.secure_only}")
            # Silent heartbeat for gating
            c.subscribe(topic_heartbeat, qos=0)
            # Feedback topics
            c.subscribe(topic_feedback, qos=0)
            if not args.data_feedback_only:
                c.subscribe(topic_planfb, qos=0)
            print(f"[subscribed] {topic_feedback}" + ("" if args.data_feedback_only else " + plan_feedback"))
            # Publish AFTER subscribing to catch immediate replies
            mid = c.publish(topic_pub, payload=payload, qos=args.qos, retain=args.retain).mid
            print(f"[published] {topic_pub}  bytes={len(payload)}  hex={pretty_hex(payload)}  mid={mid}")
        else:
            print(f"[connect failed] rc={rc}", file=sys.stderr)

    def on_message(c, u, m):
        # Silent gating on heartbeat
        if m.topic == topic_heartbeat:
            try:
                hb = json.loads(m.payload.decode("utf-8"))
                if isinstance(hb, dict) and hb.get("working_state") == 1 and should_block():
                    print("WARNING: In incorrect state for write (working_state=1). Disconnecting.")
                    c.disconnect()
                    return
            except Exception:
                pass
            return  # do not print heartbeat

        # Print feedback topics
        print(f"\n[message] {m.topic}  len={len(m.payload)}")
        print(try_decode(m.payload))

    def on_disconnect(c, u, flags, rc, properties=None):
        print(f"[disconnected] rc={rc}")
        done.set()

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(args.host, args.port, keepalive=args.keepalive)
    except Exception as e:
        print(f"[connect error] {e}", file=sys.stderr); sys.exit(1)

    client.loop_start()
    try:
        time.sleep(max(0.1, args.listen_seconds))
    finally:
        client.loop_stop()
        client.disconnect()
        done.wait(2)

if __name__ == "__main__":
    main()

