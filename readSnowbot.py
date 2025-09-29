import paho.mqtt.client as mqtt
import json, zlib, gzip, io
import argparse

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected rc=", reason_code)
    client.subscribe(("+/bridge/#", 0))
    client.subscribe(("mqtt_bridge/#", 0))
    client.subscribe(("ros/#", 0))
    client.subscribe(("snowbot/+/app/#", 0))
    client.subscribe(("snowbot/+/device/#", 0))

def on_subscribe(client, userdata, mid, granted_qos, properties):
    print("SUBACK mid=", mid, "granted=", granted_qos)

def on_message(client, userdata, msg):
    def try_json(b):
        for f in (lambda x: x, lambda x: zlib.decompress(x),
                  lambda x: gzip.GzipFile(fileobj=io.BytesIO(x)).read()):
            try:
                return "json", json.loads(f(msg.payload).decode("utf-8"))
            except Exception:
                pass
        return "binary", msg.payload[:64].hex()

    kind, data = try_json(msg.payload)
    if kind == "json":
        print(f"\n{msg.topic}\n{json.dumps(data, indent=2, ensure_ascii=False)}")
    else:
        print(f"\n{msg.topic} [binary] len={len(msg.payload)} head64={data}")

def main():
    parser = argparse.ArgumentParser(description="MQTT JSON sniffer")
    parser.add_argument("--host", required=True, help="MQTT broker host/IP")
    parser.add_argument("--port", type=int, default=8083, help="MQTT broker port")
    parser.add_argument("--path", default="/mqtt", help="MQTT websocket path")
    args = parser.parse_args()

    client = mqtt.Client(
        client_id="test-json",
        transport="websockets",
        protocol=mqtt.MQTTv311,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.ws_set_options(path=args.path)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    client.connect(args.host, args.port, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()

