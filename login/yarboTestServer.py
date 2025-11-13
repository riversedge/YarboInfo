#!/usr/bin/env python3
"""
yarboTestServer.py – Complete test server for Yarbo mobile app
Supports login, refreshToken, Play Store, /dev/app, and more.
"""

import argparse
import http.server
import ssl
import json
import secrets
import uuid
import datetime
import time
import socketserver
import os
import sys
import socket
import base64
import hashlib
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from agora_token_builder import RtcTokenBuilder

# ----------------------------------------------------------------------
# Global caches / constants
# ----------------------------------------------------------------------
_rsa_private_key = None
_rsa_public_pem = None
_agora_tokens = {}
SIGN_SECRET = "Yarbo@2023"

# ----------------------------------------------------------------------
# JWT helpers
# ----------------------------------------------------------------------
def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

def _get_rsa_key():
    global _rsa_private_key, _rsa_public_pem
    if _rsa_private_key is not None:
        return _rsa_private_key, _rsa_public_pem
    _rsa_private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    public_key = _rsa_private_key.public_key()
    _rsa_public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    return _rsa_private_key, _rsa_public_pem

def create_jwt(payload: dict, iat: int, exp: int) -> str:
    header = {
        "alg": "RS256",
        "typ": "JWT",
        "kid": "xAZNn4-ZW-t4hBO8p3YDO"
    }
    header_enc = _base64url_encode(json.dumps(header, separators=(',', ':')).encode())
    payload_enc = _base64url_encode(json.dumps(payload, separators=(',', ':')).encode())
    signing_input = f"{header_enc}.{payload_enc}".encode()
    priv_key, _ = _get_rsa_key()
    signature = priv_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    sig_enc = _base64url_encode(signature)
    return f"{header_enc}.{payload_enc}.{sig_enc}"

# ----------------------------------------------------------------------
# Agora token helper
# ----------------------------------------------------------------------
def generate_agora_token(app_id: str, app_cert: str, channel: str, uid: str, expire: int = 3600) -> dict:
    uid_int = int(uid) if uid and uid.isdigit() else 0
    role = 1
    privilege_expired_ts = int(time.time()) + expire
    token = RtcTokenBuilder.buildTokenWithUid(
        appId=app_id,
        appCertificate=app_cert,
        channelName=channel,
        uid=uid_int,
        role=role,
        privilegeExpiredTs=privilege_expired_ts,
    )
    combined = f"{channel}{uid}"
    key = hashlib.sha256(combined.encode()).hexdigest()
    salt = base64.b64encode(combined.encode()).decode()
    return {"token": token, "key": key, "salt": salt}

# ----------------------------------------------------------------------
# Response signing
# ----------------------------------------------------------------------
def add_sign_to_response(response: dict) -> dict:
    if not response.get("success", False):
        return response
    resp_copy = json.loads(json.dumps(response))
    resp_copy.pop("sign", None)
    body_str = json.dumps(resp_copy, sort_keys=True, separators=(',', ':'))
    sign_input = body_str + SIGN_SECRET
    signature = base64.b64encode(hashlib.sha256(sign_input.encode()).digest()).decode()
    response["sign"] = signature
    return response

# ----------------------------------------------------------------------
# Play-Store static response
# ----------------------------------------------------------------------
_PLAY_STORE_JSON = {
    "code": "00000",
    "data": {
        "appId": "com.hanyang.yarbo",
        "title": "Yarbo",
        "summary": "Smart lawn mower & snow blower control",
        "developer": "Yarbo Inc.",
        "icon": "https://play-lh.googleusercontent.com/...",
        "screenshots": [],
        "version": "1.2.3",
        "updated": "2025-10-01",
        "size": "45M",
        "installs": "10,000+",
        "rating": 4.6,
        "reviews": 321,
        "contentRating": "Everyone",
        "containsAds": False,
        "offersIAP": False,
        "androidVersion": "7.0",
        "androidVersionText": "7.0 and up",
        "developerEmail": "support@yarbo.com",
        "developerWebsite": "https://www.yarbo.com",
        "privacyPolicy": "https://www.yarbo.com/privacy"
    },
    "message": "ok",
    "success": True,
    "timestamp": 0
}

# ----------------------------------------------------------------------
# HTTP request handler
# ----------------------------------------------------------------------
class YarboRequestHandler(http.server.BaseHTTPRequestHandler):
    show_requests = False
    show_responses = False
    access_tokens = {}

    def auth_matches_or_adopt(self, client_ip, stored_token, auth_header):
        if not isinstance(auth_header, str) or not auth_header.startswith("Bearer "):
            return False, stored_token, "Bearer None"
        incoming = auth_header.split(" ", 1)[1].strip()
        if not incoming:
            return False, stored_token, "Bearer None"
        if stored_token is None or str(stored_token).strip().lower() in ("none", "", "null"):
            self.access_tokens[client_ip] = incoming
            print(f"Adopted new token for {client_ip}: {incoming[:10]}...")
            return True, incoming, f"Bearer {incoming}"
        if stored_token != incoming:
            print(f"Token mismatch for {client_ip} – updating")
            self.access_tokens[client_ip] = incoming
            return True, incoming, f"Bearer {incoming}"
        return True, stored_token, f"Bearer {stored_token}"

    def load_test_server_info(self):
        try:
            with open('testServerInfo.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading testServerInfo.json: {e}")
            return None

    def get_timestamp(self):
        return int(time.time() * 1000)

    def send_json_response(self, status, data):
        try:
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            if status == 200:
                self.send_header('Connection', 'keep-alive')
                self.send_header('Keep-Alive', 'timeout=5, max=100')
            else:
                self.send_header('Connection', 'close')
            payload = json.dumps(data, indent=2 if self.show_responses else None)
            self.send_header('Content-Length', len(payload.encode('utf-8')))
            self.end_headers()
            time.sleep(0.07)
            if self.show_responses:
                compact = json.dumps(data, separators=(',', ':'))
                print(f"SENDING [{status}] {self.path} → {compact}")
            self.wfile.write(payload.encode('utf-8'))
            self.wfile.flush()
            print(f"Sent {status} for {self.path} (IP: {self.client_address[0]})")
        except Exception as e:
            print(f"Error sending response: {e}")
            self.close_connection = True

    def do_GET(self):
        path = self.path.lstrip('/')
        client_ip = self.client_address[0]
        stored_token = self.access_tokens.get(client_ip)

        if path.startswith('store/apps/details?id=com.hanyang.yarbo'):
            print(f"GET Play-Store lookup from {client_ip}")
            resp = json.loads(json.dumps(_PLAY_STORE_JSON))
            resp["timestamp"] = self.get_timestamp()
            self.send_json_response(200, add_sign_to_response(resp))
            return

        auth_ok, _, _ = self.auth_matches_or_adopt(
            client_ip, stored_token, self.headers.get('authorization')
        )

        def reject_unauthorized():
            self.send_json_response(401, add_sign_to_response({
                'code': '401', 'data': None, 'message': 'Invalid token',
                'success': False, 'timestamp': self.get_timestamp()
            }))

        if not auth_ok:
            reject_unauthorized()
            return

        server_info = self.load_test_server_info()
        if path == 'Stage/app/getPolicyKey':
            resp = {
                'code': '00000',
                'data': {'privacyKey': '7feb6023c5704f2e9d1c6b8a1f3e5d7c9b0a2e4f6d8c1a3b5e7f9d0c2a4b6e8f'},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        self.send_json_response(404, {
            'code': '404', 'data': None, 'message': 'Not Found',
            'success': False, 'timestamp': self.get_timestamp()
        })

    def do_POST(self):
        path = self.path.lstrip('/')
        client_ip = self.client_address[0]
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length else b''
        body_str = body.decode('utf-8', errors='ignore')
        parsed_json = {}
        try:
            parsed_json = json.loads(body_str) if body_str.strip() else {}
        except json.JSONDecodeError:
            pass

        if self.show_requests:
            print(f"POST {path} from {client_ip} → {body_str}")

        stored_token = self.access_tokens.get(client_ip)

        # ------------------- /dev/app ---------------------------
        if path == 'dev/app':
            print(f"POST /dev/app from {client_ip}")
            resp = {
                "code": "00000",
                "data": {},
                "message": "Data inserted/updated successfully!",
                "success": True,
                "timestamp": self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------- /login ---------------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/login':
            print(f"POST login: {client_ip}")
            try:
                data = parsed_json or json.loads(body_str or "{}")
            except Exception:
                data = {}
            username = data.get("username", "wewright@wesleyacoustics.com")
            # Backdate iat by 1 minute to avoid clock skew
            iat = int(time.time()) - 60
            exp = iat + 30 * 24 * 60 * 60  # 30 days
            payload = {
                "userId": username,
                "permissionGroup": "",
                "https://auth0.yarbo.com/roles": [],
                "https://auth0.yarbo.com/email": username,
                "iss": "https://dev-6ubfuqym1d3m0mq1.us.auth0.com/",
                "sub": "auth0|67e9930075b689b7db2688df",
                "aud": [
                    "https://auth0-jwt-authorizer",
                    "https://dev-6ubfuqym1d3m0mq1.us.auth0.com/userinfo"
                ],
                "iat": iat,
                "exp": exp,
                "scope": "openid profile offline_access",
                "gty": "password",
                "azp": "SL1GSNy3VmCLTMl01qPkwqjY4xm66i0",
                "permissions": []
            }
            access_token = create_jwt(payload, iat, exp)
            refresh_token = secrets.token_hex(91)
            self.access_tokens[client_ip] = access_token
            resp = {
                "code": "00000",
                "data": {
                    "accessToken": access_token,
                    "refreshToken": refresh_token,
                    "expiresIn": 2592000,
                    "jti": str(uuid.uuid4()),
                    "snList": [],
                    "userId": username
                },
                "message": "ok",
                "success": True,
                "timestamp": self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------- /refreshToken ---------------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/refreshToken':
            print(f"POST refreshToken: {client_ip}")
            try:
                data = parsed_json or json.loads(body_str or "{}")
            except Exception:
                data = {}
            incoming_refresh = (
                data.get('refresh_token') or
                data.get('refreshToken') or
                secrets.token_hex(91)
            )
            iat = int(time.time()) - 60
            exp = iat + 30 * 24 * 60 * 60
            payload = {
                "userId": "wewright@wesleyacoustics.com",
                "permissionGroup": "",
                "https://auth0.yarbo.com/roles": [],
                "https://auth0.yarbo.com/email": "wewright@wesleyacoustics.com",
                "iss": "https://dev-6ubfuqym1d3m0mq1.us.auth0.com/",
                "sub": "auth0|67e9930075b689b7db2688df",
                "aud": [
                    "https://auth0-jwt-authorizer",
                    "https://dev-6ubfuqym1d3m0mq1.us.auth0.com/userinfo"
                ],
                "iat": iat,
                "exp": exp,
                "scope": "openid profile offline_access",
                "gty": "refresh_token",
                "azp": "SL1GSNy3VmCLTMl01qPkwqjY4xm66i0",
                "permissions": []
            }
            access_token = create_jwt(payload, iat, exp)
            self.access_tokens[client_ip] = access_token
            resp = {
                "code": "00000",
                "data": {
                    "accessToken": access_token,
                    "refreshToken": incoming_refresh,
                    "expiresIn": 2592000,
                    "jti": str(uuid.uuid4()),
                    "snList": [],
                    "userId": "wewright@wesleyacoustics.com"
                },
                "message": "ok",
                "success": True,
                "timestamp": self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------- Default Auth Check ---------------------------
        auth_ok, _, _ = self.auth_matches_or_adopt(
            client_ip, stored_token, self.headers.get('authorization')
        )

        def reject_unauthorized():
            self.send_json_response(401, add_sign_to_response({
                'code': '401', 'data': None, 'message': 'Invalid token',
                'success': False, 'timestamp': self.get_timestamp()
            }))

        if not auth_ok:
            reject_unauthorized()
            return

        # ------------------- Other Endpoints ---------------------------
        if path == 'Stage/yarbo/robot-service/robot/device/getList':
            resp = {
                "code": "00000",
                "data": [],
                "message": "ok",
                "success": True,
                "timestamp": self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        self.send_json_response(404, {
            'code': '404', 'data': None, 'message': 'Not Found',
            'success': False, 'timestamp': self.get_timestamp()
        })

# ----------------------------------------------------------------------
# TLS Server with clean EOF handling
# ----------------------------------------------------------------------
class TLSServer(socketserver.TCPServer):
    def get_request(self):
        client_socket, client_address = self.socket.accept()
        try:
            ssl_socket = self.ssl_context.wrap_socket(client_socket, server_side=True)
            ver = ssl_socket.version() or "?"
            cip = ssl_socket.cipher()[0] if ssl_socket.cipher() else "?"
            print(f"TLS OK – {ver} {cip} from {client_address[0]}:{client_address[1]}")
            return ssl_socket, client_address
        except Exception as e:
            if "UNEXPECTED_EOF" not in str(e):
                print(f"TLS failed: {e}")
            client_socket.close()
            raise

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Yarbo Test Server")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--cert", default="CA/server.crt")
    parser.add_argument("--key", default="CA/server.key")
    parser.add_argument("--tls-version", default="TLSv1.3", choices=["TLSv1.2", "TLSv1.3"])
    parser.add_argument("--showRequests", action="store_true", help="Log incoming request bodies")
    parser.add_argument("--showResponses", action="store_true", help="Log outgoing JSON responses")
    args = parser.parse_args()

    YarboRequestHandler.show_requests = args.showRequests
    YarboRequestHandler.show_responses = args.showResponses

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    if args.tls_version == "TLSv1.2":
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.maximum_version = ssl.TLSVersion.TLSv1_2
    else:
        context.minimum_version = ssl.TLSVersion.TLSv1_3

    context.load_cert_chain(certfile=args.cert, keyfile=args.key)

    server = TLSServer((args.host, args.port), YarboRequestHandler)
    server.ssl_context = context

    print(f"Yarbo test server listening on https://{args.host}:{args.port}")
    print(f"   • GET  /store/apps/details?id=com.hanyang.yarbo → 200 OK")
    print(f"   • POST /dev/app → 200 OK (no auth)")
    print(f"   • POST /Stage/yarbo/robot-service/robot/commonUser/login → 200 OK")
    print(f"   • POST /Stage/yarbo/robot-service/robot/commonUser/refreshToken → 200 OK")
    print(f"   • All successful JSON responses contain a `sign` field")
    print(f"   • JWT login valid for 30 days (iat backdated 1 min)")
    print(f"   • Use --showRequests to log incoming bodies")
    print(f"   • Use --showResponses to log outgoing JSON")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()

if __name__ == "__main__":
    main()
