import json
import base64
from pathlib import Path
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256


def canonical_json(obj):
    if isinstance(obj, dict):
        items = []
        for k in sorted(obj.keys()):
            items.append(f'"{k}":{canonical_json(obj[k])}')
        return "{" + ",".join(items) + "}"
    elif isinstance(obj, list):
        return "[" + ",".join(canonical_json(v) for v in obj) + "]"
    else:
        return json.dumps(obj, separators=(",", ":"))


# 1) Load your private key (PEM with BEGIN/END PRIVATE KEY)
priv_key = RSA.import_key(Path("rsa_private_key.pem").read_bytes())

data = {
    "deviceList": [
        {
            "serialNum": "25070102ATHDG219",
            "headType": 5,
            "master": 1,
            "masterUsername": "wewright@wesleyacoustics.com",
            "deviceNickname": "Wright Yarbo",
            "gmtCreate": "2025-07-16 22:47:10",
            "masterNickname": "Res",
        }
    ]
}

# 2) Canonical JSON and bytes
data_json_str = canonical_json(data)
data_bytes = data_json_str.encode("utf-8")

# 3) Sign
h = SHA256.new(data_bytes)
signature = pkcs1_15.new(priv_key).sign(h)
sign_b64 = base64.b64encode(signature).decode("ascii")

print("canonical data:", data_json_str)
print("\nsign:", sign_b64)

# 4) Write exact files for openssl
Path("data_from_py.json").write_bytes(data_bytes)
Path("sig_from_py.bin").write_bytes(signature)
print("\nWrote data_from_py.json and sig_from_py.bin")

