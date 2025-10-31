#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import json, zlib, gzip, io, ssl
import argparse
import sys

def on_connect(client, userdata, flags, reason_code, properties=None):
    print("Connected rc=", reason_code)

def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
    print("Subscribed:", reason_code_list)

def on_message(client, userdata, msg):
    try:
        data = msg.payload
        try:
            if data[:2] == b"\x1f\x8b":  # gzip
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                    data = f.read()
            else:  # zlib
                data = zlib.decompress(data)
        except Exception:
            pass

        try:
            j = json.loads(data)
            print(f"\n{msg.topic}:")
            print(json.dumps(j, indent=2))
        except Exception:
            print(f"\n{msg.topic}: {data}")
            print(data)
    except Exception as e:
        print("Parse error:", e)

def build_client(args):
    client = mqtt.Client(
        protocol=mqtt.MQTTv311,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        transport=("websockets" if args.ws else "tcp"),
    )
    if args.ws and args.path:
        client.ws_set_options(path=args.path)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    return client

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=None, help="Default 8883 with TLS, 1883 with --noTLS")
    ap.add_argument("--topic", default="+/#")
    ap.add_argument("--ws", action="store_true", help="Use WebSockets transport")
    ap.add_argument("--path", default="/mqtt", help="WebSocket path (if --ws)")

    # TLS behavior options
    ap.add_argument("--noTLS", action="store_true", help="Disable TLS entirely (plaintext, port 1883)")
    ap.add_argument("--secure", action="store_true", help="Verify TLS certificates (default is insecure/no verify)")
    ap.add_argument("--cafile", help="CA bundle/CA cert for server verification (required if --secure)")
    ap.add_argument("--certfile", help="Client certificate (optional)")
    ap.add_argument("--keyfile", help="Client private key (optional)")

    args = ap.parse_args()

    # Determine port
    if args.port is None:
        args.port = 1883 if args.noTLS else 8883

    client = build_client(args)

    if not args.noTLS:
        if args.secure:
            if not args.cafile:
                print("ERROR: --secure requires --cafile to verify certificates.")
                sys.exit(1)
            # Secure mode: verify server using provided CA
            client.tls_set(
                ca_certs=args.cafile,
                certfile=args.certfile if args.certfile else None,
                keyfile=args.keyfile if args.keyfile else None,
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )
            client.tls_insecure_set(False)
        else:
            # Insecure default: disable verification explicitly via context
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            # Optional client certs even in insecure mode
            if args.certfile and args.keyfile:
                ctx.load_cert_chain(certfile=args.certfile, keyfile=args.keyfile)
            client.tls_set_context(ctx)

    client.connect(args.host, args.port, 60)
    client.subscribe(args.topic)
    client.loop_forever()

if __name__ == "__main__":
    main()
