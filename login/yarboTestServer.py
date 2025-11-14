#!/usr/bin/env python3
"""
yarboTestServer.py – Local TLS test server for Yarbo mobile app

Features:
- RSA signs only response["data"]
- RS256 JWTs (kid: yarbo-test-key)
- All original + new endpoints (getUserRobotBindVos, questionnaire → 403)
- TLS 1.2/1.3
- --rsa-dir <path> (default: ./custom/)
- --showRequests / --showResponses
"""

import argparse
import base64
import json
import os
import secrets
import socket
import socketserver
import ssl
import sys
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, Tuple

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


# ----------------------------------------------------------------------
# RSA key loader (configurable via --rsa-dir)
# ----------------------------------------------------------------------
_RSA_PRIVATE: rsa.RSAPrivateKey | None = None
_RSA_PUBLIC: rsa.RSAPublicKey | None = None
_RSA_DIR: str = "custom"


def _load_rsa_keys() -> Tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]:
    """Load private (standard) and public (custom-wrapped OK) keys."""
    global _RSA_PRIVATE, _RSA_PUBLIC
    if _RSA_PRIVATE is not None and _RSA_PUBLIC is not None:
        return _RSA_PRIVATE, _RSA_PUBLIC

    priv_path = os.path.join(_RSA_DIR, "rsa_private_key.pem")
    pub_path = os.path.join(_RSA_DIR, "rsa_public_key.pem")

    # --- Private key ---
    if not os.path.isfile(priv_path):
        print(f"ERROR: RSA private key not found: {priv_path}")
        sys.exit(1)
    with open(priv_path, "rb") as f:
        priv_pem = f.read()
    try:
        _RSA_PRIVATE = serialization.load_pem_private_key(priv_pem, password=None)
    except Exception as e:
        print(f"ERROR: Failed to load private key: {e}")
        sys.exit(1)

    # --- Public key (handles custom YARBO wrapper) ---
    if not os.path.isfile(pub_path):
        print(f"ERROR: RSA public key not found: {pub_path}")
        sys.exit(1)
    with open(pub_path, "rb") as f:
        raw_pub = f.read()

    try:
        pub_str = raw_pub.decode("utf-8", errors="ignore")
        lines = [l.strip() for l in pub_str.splitlines() if l.strip() and not l.startswith("-----")]
        b64_str = "".join(lines)
        der_bytes = base64.b64decode(b64_str)
        temp_key = serialization.load_der_public_key(der_bytes)
        _RSA_PUBLIC = rsa.RSAPublicNumbers(
            e=temp_key.public_numbers().e,
            n=temp_key.public_numbers().n,
        ).public_key()
    except Exception as e:
        print(f"ERROR: Failed to parse public key: {e}")
        print("HINT: Check rsa_public_key.pem – base64 between -----BEGIN PUBLIC KEY----- and -----END PUBLIC KEY-----")
        sys.exit(1)

    return _RSA_PRIVATE, _RSA_PUBLIC


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def add_sign_to_response(response: Dict[str, Any]) -> Dict[str, Any]:
    if not response.get("success", False) or "data" not in response:
        return response

    try:
        priv, _ = _load_rsa_keys()
    except Exception as e:
        print(f"WARNING: Failed to load RSA key for signing: {e}")
        return response

    canon = _canonical_json(response["data"]).encode("utf-8")
    signature = priv.sign(canon, padding.PKCS1v15(), hashes.SHA256())
    response["sign"] = base64.b64encode(signature).decode("ascii")
    return response


def _urlsafe_b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def create_jwt(payload: Dict[str, Any]) -> str:
    header = {"alg": "RS256", "typ": "JWT", "kid": "yarbo-test-key"}
    header_b64 = _urlsafe_b64(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _urlsafe_b64(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header_b64}.{payload_b64}".encode()

    priv, _ = _load_rsa_keys()
    sig = priv.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = _urlsafe_b64(sig)
    return f"{header_b64}.{payload_b64}.{sig_b64}"


# ----------------------------------------------------------------------
# Dummy data
# ----------------------------------------------------------------------
_PLAY_STORE_JSON = {
    "code": "00000",
    "data": {
        "version": "3.16.4",
        "packageName": "com.hanyang.yarbo",
        "updateAvailable": False,
    },
    "message": "ok",
    "success": True,
    "timestamp": 0,
}

_agora_tokens: Dict[str, Dict[str, Any]] = {}


def generate_agora_token(app_id: str, app_cert: str, channel: str, uid: str) -> Dict[str, Any]:
    now = int(time.time())
    expire = now + 3600
    return {
        "appId": app_id,
        "channelName": channel,
        "uid": uid,
        "token": f"dummy-agora-{channel}-{uid}-{expire}",
        "expireTs": expire,
    }


# ----------------------------------------------------------------------
# Request handler
# ----------------------------------------------------------------------
class YarboRequestHandler(BaseHTTPRequestHandler):
    show_requests: bool = False
    show_responses: bool = False
    access_tokens: Dict[str, str] = {}
    user_ids: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _ts(self) -> int:
        return int(time.time() * 1000)

    def _week_ago(self) -> int:
        dt = datetime.now(timezone.utc) - timedelta(days=7)
        return int(dt.timestamp())

    def _send_json(self, status: int, body: Dict[str, Any]):
        signed = add_sign_to_response(body)
        if self.show_responses:
            print(f"RESPONSE {status} {self.path}")
            print(json.dumps(signed, ensure_ascii=False, indent=2))
        data = json.dumps(signed, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _load_info(self) -> Dict[str, Any]:
        try:
            with open("testServerInfo.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            print(f"load testServerInfo.json error: {e}")
            return {}

    def _auth_check(self, client_ip: str, auth_header: str | None) -> Tuple[bool, str | None]:
        stored = self.access_tokens.get(client_ip)
        token = None
        if auth_header and auth_header.lower().startswith("bearer "):
            token = auth_header.split(None, 1)[1].strip()

        if token and stored is None:
            self.access_tokens[client_ip] = token
            print(f"Adopted token for {client_ip}: {token[:40]}...")
            return True, token

        if token and stored:
            return token == stored, stored

        return bool(stored), stored

    def reject_unauthorized(self):
        self._send_json(
            401,
            {
                "code": "401",
                "data": None,
                "message": "Invalid token",
                "success": False,
                "timestamp": self._ts(),
            },
        )

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------
    def do_GET(self):
        path = self.path.lstrip("/")
        client_ip = self.client_address[0]

        if self.show_requests:
            print(f"GET {self.path} from {client_ip}")
            for k, v in self.headers.items():
                print(f"  {k}: {v}")

        # Play Store – no auth
        if path.startswith("store/apps/details?id=com.hanyang.yarbo"):
            resp = json.loads(json.dumps(_PLAY_STORE_JSON))
            resp["timestamp"] = self._ts()
            self._send_json(200, resp)
            return

        # Auth check
        auth_ok, _ = self._auth_check(client_ip, self.headers.get("Authorization"))
        if not auth_ok:
            self.reject_unauthorized()
            return

        server_info = self._load_info()
        one_week_ago = self._week_ago()

        # Known endpoints
        if path == "Stage/app/getPolicyKey":
            self._send_json(
                200,
                {
                    "code": "00000",
                    "success": True,
                    "timestamp": self._ts(),
                    "data": {"policyKey": "test-policy-key"},
                    "message": "ok",
                },
            )
            return

        if path == "Stage/yarbo/robot-service/robot/commonUser/getUesrInfo":
            if not server_info:
                self._send_json(
                    500,
                    {
                        "code": "500",
                        "data": None,
                        "message": "Server configuration error",
                        "success": False,
                        "timestamp": self._ts(),
                    },
                )
                return

            data = {
                "userId": server_info.get("userId", "test-user"),
                "nickname": server_info.get("nickName", "Test User"),
                "avatar": "",
                "phone": "",
                "email": server_info.get("userId", "test-user@example.com"),
                "country": "",
                "state": "",
                "city": "",
                "address": "",
                "zipCode": "",
                "gmtCreate": one_week_ago * 1000,
                "gmtModified": one_week_ago * 1000,
            }
            self._send_json(
                200,
                {
                    "code": "00000",
                    "success": True,
                    "timestamp": self._ts(),
                    "data": data,
                    "message": "ok",
                },
            )
            return

        # NEW: getUserRobotBindVos
        if path == "Stage/yarbo/robot-service/commonUser/userRobotBind/getUserRobotBindVos":
            if not server_info:
                self._send_json(
                    500,
                    {
                        "code": "500",
                        "data": None,
                        "message": "Server configuration error",
                        "success": False,
                        "timestamp": self._ts(),
                    },
                )
                return

            device_list = [
                {
                    "master": 1,
                    "masterUsername": server_info.get("username", ""),
                    "masterNickname": server_info.get("nickName", ""),
                    "serialNum": device["serialNum"],
                    "headType": device["headType"],
                    "deviceNickname": device.get("deviceNickname", ""),
                    "gmtCreate": one_week_ago * 1000,
                    "gmtModified": one_week_ago * 1000,
                }
                for device in server_info.get("deviceList", [])
            ]

            response = {
                "code": "00000",
                "data": {"deviceList": device_list},
                "message": "ok",
                "success": True,
                "timestamp": self._ts(),
            }
            self._send_json(200, response)
            return

        # questionnaire → 403
        if path == "Stage/yarbo/commonUser/questionnaire":
            self._send_json(
                403,
                {
                    "code": "403",
                    "data": None,
                    "message": "Invalid authorization header",
                    "success": False,
                    "timestamp": self._ts(),
                },
            )
            return

        # Other known (empty data)
        known_get = {
            "Stage/yarbo/dict/getCommonDictVos": {"data": [], "message": "ok"},
            "Stage/yarbo/robot-service/commonUser/getCountryList": {"data": [], "message": "ok"},
            "Stage/yarbo/commonUser/getLatestPubVersion": {
                "data": {"id": 0, "version": "", "gmtCreate": one_week_ago * 1000, "gmtModified": one_week_ago * 1000},
                "message": "ok",
            },
            "Stage/yarbo/robot-service/robot/commonUser/downloadUserAvatar": {"data": {}, "message": "ok"},
            "Stage/yarbo/robot/rasterBackground/get": {"data": {}, "message": "ok"},
            "Stage/yarbo/robot-service/robot/commonUser/getUserDeviceList": {
                "data": server_info.get("deviceList", []) if server_info else [],
                "message": "ok",
            },
            "Stage/yarbo/robot-service/commonUser/getBleDeviceService": {"data": [], "message": "ok"},
            "Stage/yarbo/robot-service/robot/commonUser/getDeviceOnlineStatusBySn": {"data": [], "message": "ok"},
            "Stage/yarbo/robot-service/robot/commonUser/getDeviceFlowBySn": {"data": [], "message": "ok"},
            "Stage/yarbo/robot-service/robot/commonUser/getDeviceBatteryBySn": {"data": [], "message": "ok"},
            "Stage/admin/getUsedFlowBySn": {
                "data": {"usedFlow": {"roverUsedFlow": "0", "baseUsedFlow": None}},
                "message": "ok",
            },
        }

        if path in known_get:
            base = {"code": "00000", "success": True, "timestamp": self._ts()}
            base.update(known_get[path])
            self._send_json(200, base)
            return

        print(f"Unsupported GET: {path}")
        self._send_json(
            404,
            {
                "code": "404",
                "data": None,
                "message": "Not Found",
                "success": False,
                "timestamp": self._ts(),
            },
        )

    # ------------------------------------------------------------------
    # POST
    # ------------------------------------------------------------------
    def do_POST(self):
        path = self.path.lstrip("/")
        client_ip = self.client_address[0]
        length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(length) if length else b""
        body_str = body_bytes.decode("utf-8", errors="replace")
        try:
            payload = json.loads(body_str) if body_str else {}
        except json.JSONDecodeError:
            payload = {}

        if self.show_requests:
            print(f"POST {self.path} from {client_ip}")
            for k, v in self.headers.items():
                print(f"  {k}: {v}")
            print(f"  Body: {body_str}")

        auth_ok, _ = self._auth_check(client_ip, self.headers.get("Authorization"))

        # /dev/app – no auth
        if path == "dev/app":
            resp = {
                "code": "00000",
                "data": {"appId": "com.hanyang.yarbo", "channel": "test", "config": {}},
                "message": "ok",
                "success": True,
                "timestamp": self._ts(),
            }
            self._send_json(200, resp)
            return

        # login → local JWT
        if path == "Stage/yarbo/robot-service/robot/commonUser/login":
            username = payload.get("username") or "test-user"
            password = payload.get("password")
            if not username or not password:
                self._send_json(
                    400,
                    {
                        "code": "400",
                        "data": None,
                        "message": "Missing username or password",
                        "success": False,
                        "timestamp": self._ts(),
                    },
                )
                return

            iat = int(time.time())
            exp = iat + 30 * 24 * 60 * 60
            jwt_payload = {
                "sub": username,
                "iat": iat,
                "exp": exp,
                "scope": "openid profile offline_access",
            }
            access_token = create_jwt(jwt_payload)
            refresh_token = secrets.token_hex(32)

            self.access_tokens[client_ip] = access_token
            self.user_ids[client_ip] = username

            resp = {
                "code": "00000",
                "data": {
                    "accessToken": access_token,
                    "refreshToken": refresh_token,
                    "expiresIn": 2592000,
                    "jti": "",
                    "snList": [],
                    "userId": username,
                },
                "message": "ok",
                "success": True,
                "timestamp": self._ts(),
            }
            self._send_json(200, resp)
            return

        # refreshToken
        if path == "Stage/yarbo/robot-service/robot/commonUser/refreshToken":
            username = self.user_ids.get(client_ip, "test-user")
            iat = int(time.time())
            exp = iat + 30 * 24 * 60 * 60
            jwt_payload = {
                "sub": username,
                "iat": iat,
                "exp": exp,
                "scope": "openid profile offline_access",
            }
            access_token = create_jwt(jwt_payload)
            self.access_tokens[client_ip] = access_token

            resp = {
                "code": "00000",
                "data": {
                    "accessToken": access_token,
                    "refreshToken": payload.get("refreshToken") or secrets.token_hex(32),
                    "expiresIn": 2592000,
                    "jti": "",
                    "snList": [],
                    "userId": username,
                },
                "message": "ok",
                "success": True,
                "timestamp": self._ts(),
            }
            self._send_json(200, resp)
            return

        # getAgoraToken
        if path == "Stage/yarbo/robot-service/robot/commonUser/getAgoraToken":
            if not auth_ok:
                self.reject_unauthorized()
                return
            if not all(k in payload for k in ("uid", "channel_name", "sn")):
                self._send_json(
                    400,
                    {
                        "code": "400",
                        "data": None,
                        "message": "Missing uid/channel_name/sn",
                        "success": False,
                        "timestamp": self._ts(),
                    },
                )
                return

            sn = payload["sn"]
            uid = str(payload["uid"])
            channel = payload["channel_name"]
            update = payload.get("update_key", False)

            info = self._load_info()
            if not any(d.get("serialNum") == sn for d in info.get("deviceList", [])):
                self._send_json(
                    400,
                    {
                        "code": "400",
                        "data": None,
                        "message": f"Invalid serialNum: {sn}",
                        "success": False,
                        "timestamp": self._ts(),
                    },
                )
                return

            if sn in _agora_tokens and not update:
                agora = _agora_tokens[sn]
            else:
                agora = generate_agora_token(
                    "4zx17x5q7l",
                    "0123456789abcdef0123456789abcdef",
                    channel,
                    uid,
                )
                _agora_tokens[sn] = agora

            resp = {
                "code": "00000",
                "data": agora,
                "message": "success",
                "success": True,
                "timestamp": self._ts(),
            }
            self._send_json(200, resp)
            return

        # Known POSTs
        known = {
            "Stage/yarbo/robot-service/commonUser/notification/getNotificationVos": {
                "data": {},
                "message": "ok",
            },
            "Stage/yarbo/robot-service/commonUser/userRobotBind/getUserRobotBindVos": {
                "data": {"records": [], "total": 0},
                "message": "ok",
            },
            "Stage/yarbo/robot-service/robot/commonUser/logout": {
                "data": None,
                "message": "ok",
            },
            "Stage/admin/listPlanHistoryBySn": {
                "data": {"planHistory": []},
                "message": "ok",
            },
            "dev/iot": {"data": None, "message": "ok"},
        }

        if path in known:
            if path != "dev/iot" and not auth_ok:
                self.reject_unauthorized()
                return
            if path == "Stage/yarbo/robot-service/robot/commonUser/logout":
                self.access_tokens.pop(client_ip, None)
            resp = {"code": "00000", "success": True, "timestamp": self._ts()}
            resp.update(known[path])
            self._send_json(200, resp)
            return

        print(f"Unsupported POST: {path}")
        self._send_json(
            404,
            {
                "code": "404",
                "data": None,
                "message": "Not Found",
                "success": False,
                "timestamp": self._ts(),
            },
        )


# ----------------------------------------------------------------------
# TLS Server
# ----------------------------------------------------------------------
class TLSServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, server_address, handler):
        super().__init__(server_address, handler)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def get_request(self):
        sock, addr = self.socket.accept()
        print(f"TLS attempt {addr[0]}:{addr[1]}")
        try:
            ssl_sock = self.ssl_context.wrap_socket(sock, server_side=True)
            ver = ssl_sock.version() or "?"
            cip = ssl_sock.cipher()[0] if ssl_sock.cipher() else "?"
            print(f"TLS OK – {ver} {cip}")
            return ssl_sock, addr
        except ssl.SSLEOFError:
            sock.close()
            raise
        except Exception as e:
            print(f"TLS failed: {e}")
            sock.close()
            raise


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Yarbo local TLS test server – RSA-signed + local JWTs"
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--cert", default="CA/server.crt", help="TLS server certificate")
    parser.add_argument("--key", default="CA/server.key", help="TLS server private key")
    parser.add_argument(
        "--tls-version",
        default="TLSv1.3",
        choices=["TLSv1.2", "TLSv1.3"],
        help="Minimum TLS version",
    )
    parser.add_argument(
        "--rsa-dir",
        default="custom",
        help="Directory containing rsa_private_key.pem and rsa_public_key.pem",
    )
    parser.add_argument("--showRequests", action="store_true")
    parser.add_argument("--showResponses", action="store_true")

    args = parser.parse_args()

    global _RSA_DIR
    _RSA_DIR = args.rsa_dir

    YarboRequestHandler.show_requests = args.showRequests
    YarboRequestHandler.show_responses = args.showResponses

    # Validate TLS files
    for f in (args.cert, args.key):
        if not os.path.isfile(f):
            print(f"ERROR: Missing TLS file: {f}")
            sys.exit(1)

    # Validate RSA keys
    try:
        _load_rsa_keys()
        print(f"RSA keys loaded from: {_RSA_DIR}")
    except Exception as e:
        print(f"ERROR: Failed to load RSA keys: {e}")
        sys.exit(1)

    # TLS context
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = (
        ssl.TLSVersion.TLSv1_3 if args.tls_version == "TLSv1.3" else ssl.TLSVersion.TLSv1_2
    )
    ctx.load_cert_chain(certfile=args.cert, keyfile=args.key)

    # Start server
    httpd = TLSServer((args.host, args.port), YarboRequestHandler)
    httpd.ssl_context = ctx

    print(f"\nYarbo test server → https://{args.host}:{args.port}")
    print(f"  TLS cert: {args.cert}")
    print(f"  TLS key : {args.key}")
    print(f"  RSA dir : {_RSA_DIR}")
    print(f"  Logging : requests={'ON' if args.showRequests else 'off'}, responses={'ON' if args.showResponses else 'off'}\n")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
