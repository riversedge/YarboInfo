#!/usr/bin/env python3
"""
yarboTestServer.py – complete test server for the Yarbo mobile app

New in this version
-------------------
* GET /store/apps/details?id=com.hanyang.yarbo → static Play-Store JSON + sign
* All other endpoints unchanged (JWT, /dev/app, Agora, device list, TLS, …)
"""

import argparse
import http.server
import ssl
import json
import secrets
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
_agora_tokens = {}                     # serialNum → {token, key, salt}
SIGN_SECRET = "Yarbo@2023"             # verified from production traffic

# ----------------------------------------------------------------------
# JWT helpers
# ----------------------------------------------------------------------
def _base64url_encode(data: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def _get_rsa_key():
    global _rsa_private_key, _rsa_public_pem
    if _rsa_private_key is None:
        _rsa_private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        public_key = _rsa_private_key.public_key()
        _rsa_public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
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
def generate_agora_token(app_id: str, app_certificate: str, channel_name: str, uid: str):
    """
    Generate a fake Agora token structure similar to production.
    """
    role = 1  # publisher
    expire_seconds = 3600

    # RtcTokenBuilder requires integer uid; if we get a non-int, hash it
    try:
        uid_int = int(uid)
    except ValueError:
        uid_int = int(hashlib.sha256(uid.encode()).hexdigest(), 16) & 0x7fffffff

    token = RtcTokenBuilder.buildTokenWithUid(
        app_id, app_certificate, channel_name, uid_int, role, expire_seconds
    )

    # The app expects: { "token": "...", "key": "...", "salt": "..." }
    # Here we fabricate key/salt just for structure.
    key = secrets.token_hex(16)
    combined = f"{channel_name}:{uid}:{key}"
    salt = base64.b64encode(combined.encode()).decode()
    return {"token": token, "key": key, "salt": salt}

# ----------------------------------------------------------------------
# Response signing (real Yarbo algorithm)
# ----------------------------------------------------------------------
def add_sign_to_response(response: dict) -> dict:
    """Add `sign` only to successful responses."""
    if not response.get("success", False):
        return response
    resp_copy = json.loads(json.dumps(response))
    resp_copy.pop("sign", None)
    body_str = json.dumps(resp_copy, sort_keys=True, separators=(',', ':'))
    sign_input = body_str + SIGN_SECRET
    signature = base64.b64encode(
        hashlib.sha256(sign_input.encode('utf-8')).digest()
    ).decode('ascii')
    response["sign"] = signature
    return response

# ----------------------------------------------------------------------
# Static Play-Store JSON (minimal but structurally correct)
# ----------------------------------------------------------------------
_PLAY_STORE_JSON = {
    "code": "00000",
    "data": {
        "packageName": "com.hanyang.yarbo",
        "versionName": "1.0.0",
        "versionCode": 1,
        "track": "production",
        "updateInfo": {
            "title": "Yarbo",
            "message": "Test-server fake Play-Store response",
            "forceUpdate": False,
            "downloadUrl": "https://example.com/yarbo.apk"
        }
    },
    "message": "ok",
    "success": True,
    "timestamp": 0   # will be replaced with real timestamp
}

# ----------------------------------------------------------------------
# HTTP request handler
# ----------------------------------------------------------------------
class YarboRequestHandler(http.server.BaseHTTPRequestHandler):
    show_responses = False
    show_requests = False
    access_tokens = {}                     # client_ip → current Bearer token
    user_ids = {}                          # client_ip → last logged-in username

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def log_message(self, format, *args):
        # Silence default HTTP server logging; we use custom prints
        return

    def get_timestamp(self):
        return int(time.time() * 1000)

    def get_one_week_ago(self):
        return int((time.time() - 7 * 24 * 3600))

    def load_test_server_info(self):
        try:
            with open('testServerInfo.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading testServerInfo.json: {e}")
            return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------
    def auth_matches_or_adopt(self, client_ip, stored_token, auth_header):
        """
        Emulate actual Yarbo behavior:

        * If there is no stored token for this IP, we adopt the first valid Bearer token.
        * After that, the incoming token must match exactly.
        * If header is malformed or missing → unauthorized.
        """
        if not isinstance(auth_header, str) or not auth_header.startswith("Bearer "):
            return False, stored_token, "Bearer None"
        incoming = auth_header.split(" ", 1)[1].strip()
        if not incoming:
            return False, stored_token, "Bearer None"

        # First token from this IP → adopt it
        if stored_token is None or str(stored_token).strip().lower() in ("none", "", "null"):
            self.access_tokens[client_ip] = incoming
            print(f"WARNING: Adopted new token for {client_ip}: {incoming[:10]}...")
            return True, incoming, f"Bearer {incoming}"

        # Must match exactly afterwards
        if stored_token != incoming:
            print(f"Token mismatch for {client_ip}: stored={stored_token[:10]}..., incoming={incoming[:10]}...")
            return False, stored_token, f"Bearer {stored_token}"

        return True, stored_token, f"Bearer {stored_token}"

    # ------------------------------------------------------------------
    # JSON response helper
    # ------------------------------------------------------------------
    def send_json_response(self, status, data):
        try:
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            if status == 200:
                self.send_header('Connection', 'keep-alive')
                self.send_header('Keep-Alive', 'timeout=5, max=100')
            else:
                self.send_header('Connection', 'close')
            payload = json.dumps(data, indent=2)
            self.send_header('Content-Length', len(payload.encode('utf-8')))
            self.end_headers()
            time.sleep(0.07)
            if getattr(self, 'show_responses', False):
                compact = json.dumps(data, separators=(',', ':'))
                print(f"SENDING [{status}] {self.path} → {compact}")
            self.wfile.write(payload.encode('utf-8'))
            self.wfile.flush()
            print(f"Sent {status} for {self.path} (IP: {self.client_address[0]})")
        except Exception as e:
            print(f"Error sending response: {e}")
            self.close_connection = True

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------
    def do_GET(self):
        path = self.path.lstrip('/')
        client_ip = self.client_address[0]
        stored_token = self.access_tokens.get(client_ip)

        if getattr(self, 'show_requests', False):
            print(f"RECV GET {self.path} from {client_ip}")
            for k, v in self.headers.items():
                print(f"  {k}: {v}")

        # ------------------------------------------------------------------
        # Play-Store lookup – **NO AUTH REQUIRED**
        # ------------------------------------------------------------------
        if path.startswith('store/apps/details?id=com.hanyang.yarbo'):
            print(f"GET Play-Store lookup from {client_ip}")
            resp = json.loads(json.dumps(_PLAY_STORE_JSON))   # deep copy
            resp["timestamp"] = self.get_timestamp()
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------------------------------------------------------
        # All other GETs need a token (except the one above)
        # ------------------------------------------------------------------
        auth_ok, _, _ = self.auth_matches_or_adopt(
            client_ip, stored_token, self.headers.get('authorization')
        )

        def reject_unauthorized():
            self.send_json_response(401, add_sign_to_response({
                'code': '401', 'data': None, 'message': 'Invalid token',
                'success': False, 'timestamp': self.get_timestamp()
            }))

        server_info = self.load_test_server_info()
        one_week_ago = self.get_one_week_ago()

        # ------------------- Known endpoints -------------------
        if path == 'Stage/app/getPolicyKey':
            resp = {
                'code': '00000',
                'data': {'policyKey': 'test-policy-key'},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path == 'Stage/yarbo/dict/getCommonDictVos':
            if not auth_ok: reject_unauthorized(); return
            resp = {
                'code': '00000',
                'data': [],
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path == 'Stage/yarbo/robot-service/commonUser/getCountryList':
            if not auth_ok: reject_unauthorized(); return
            resp = {
                'code': '00000',
                'data': [],
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path == 'Stage/yarbo/robot-service/robot/commonUser/getUserInfo':
            if not auth_ok: reject_unauthorized(); return
            if not server_info:
                resp = {
                    'code': '00000', 'data': None,
                    'message': 'No testServerInfo.json',
                    'success': True,
                    'timestamp': self.get_timestamp()
                }
            else:
                resp = {
                    'code': '00000',
                    'data': {
                        'userId': server_info['userId'],
                        'nickname': server_info.get('nickname', 'Test User'),
                        'avatar': '',
                        'phone': '', 'email': server_info['userId'],
                        'country': '', 'state': '', 'city': '', 'address': '', 'zipCode': '',
                        'gmtCreate': one_week_ago * 1000,
                        'gmtModified': one_week_ago * 1000
                    }, 'message': 'ok', 'success': True,
                    'timestamp': self.get_timestamp()
                }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path == 'Stage/yarbo/commonUser/getLatestPubVersion':
            if not auth_ok: reject_unauthorized(); return
            resp = {
                'code': '00000',
                'data': {'id': 0, 'version': '',
                         'gmtCreate': one_week_ago * 1000,
                         'gmtModified': one_week_ago * 1000},
                'message': 'ok', 'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path == 'Stage/yarbo/robot-service/robot/commonUser/downloadUserAvatar':
            if not auth_ok: reject_unauthorized(); return
            resp = {'code': '00000', 'data': {}, 'message': 'ok',
                    'success': True, 'timestamp': self.get_timestamp()}
            self.send_json_response(200, add_sign_to_response(resp))

        elif path.startswith('Stage/yarbo/robot/rasterBackground/get'):
            if not auth_ok: reject_unauthorized(); return
            resp = {'code': '00000', 'data': {}, 'message': 'ok',
                    'success': True, 'timestamp': self.get_timestamp()}
            self.send_json_response(200, add_sign_to_response(resp))

        elif path == 'Stage/yarbo/robot-service/robot/commonUser/getUserDeviceList':
            if not auth_ok: reject_unauthorized(); return
            if not server_info:
                resp = {
                    'code': '00000',
                    'data': [],
                    'message': 'No testServerInfo.json',
                    'success': True,
                    'timestamp': self.get_timestamp()
                }
            else:
                resp = {
                    'code': '00000',
                    'data': server_info.get('deviceList', []),
                    'message': 'ok',
                    'success': True,
                    'timestamp': self.get_timestamp()
                }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path.startswith('Stage/yarbo/robot-service/commonUser/getBleDeviceService'):
            if not auth_ok: reject_unauthorized(); return
            resp = {
                'code': '00000',
                'data': [],
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path.startswith('Stage/yarbo/robot-service/robot/commonUser/getDeviceOnlineStatusBySn'):
            if not auth_ok: reject_unauthorized(); return
            resp = {
                'code': '00000',
                'data': [],
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path.startswith('Stage/yarbo/robot-service/robot/commonUser/getDeviceFlowBySn'):
            if not auth_ok: reject_unauthorized(); return
            resp = {
                'code': '00000',
                'data': [],
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path.startswith('Stage/yarbo/robot-service/robot/commonUser/getDeviceBatteryBySn'):
            if not auth_ok: reject_unauthorized(); return
            resp = {
                'code': '00000',
                'data': [],
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        elif path.startswith('Stage/admin/getUsedFlowBySn'):
            if not auth_ok: reject_unauthorized(); return
            resp = {
                "code": "00000",
                "data": {"usedFlow": {"roverUsedFlow": "0", "baseUsedFlow": None}},
                "message": "ok", "success": True,
                "timestamp": self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))

        else:
            print(f"Unsupported GET: {path}")
            self.send_json_response(404, add_sign_to_response({
                'code': '404', 'data': None, 'message': 'Not Found',
                'success': False,
                'timestamp': self.get_timestamp()
            }))

    # ------------------------------------------------------------------
    # POST
    # ------------------------------------------------------------------
    def do_POST(self):
        path = self.path.lstrip('/')
        client_ip = self.client_address[0]

        # ---- read body ------------------------------------------------
        content_length = int(self.headers.get('Content-Length', 0))
        body_bytes = self.rfile.read(content_length) if content_length > 0 else b''
        body = body_bytes.decode('utf-8', errors='replace')
        parsed_json = None
        if body:
            try:
                parsed_json = json.loads(body)
            except json.JSONDecodeError:
                pass

        if getattr(self, 'show_requests', False):
            print(f"RECV POST {self.path} from {client_ip}")
            for k, v in self.headers.items():
                print(f"  {k}: {v}")
            print(f"  Body: {body}")

        stored_token = self.access_tokens.get(client_ip)
        auth_ok, _, _ = self.auth_matches_or_adopt(
            client_ip, stored_token, self.headers.get('authorization')
        )

        def reject_unauthorized():
            self.send_json_response(401, add_sign_to_response({
                'code': '401', 'data': None, 'message': 'Invalid token',
                'success': False, 'timestamp': self.get_timestamp()
            }))

        # ------------------- /dev/app (no auth) --------------------
        if path == 'dev/app':
            print(f"POST /dev/app from {client_ip}")
            resp = {
                "code": "00000",
                "data": {
                    "appId": "com.hanyang.yarbo",
                    "channel": "test",
                    "config": {}
                },
                "message": "ok",
                "success": True,
                "timestamp": self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------- /login ---------------------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/login':
            print(f"POST login: {client_ip}")
            try:
                data = parsed_json or json.loads(body)
                username = data.get('username')
                password = data.get('password')
                if not username or not password:
                    self.send_json_response(400, add_sign_to_response({
                        'code': '400', 'data': None,
                        'message': 'Missing username or password',
                        'success': False, 'timestamp': self.get_timestamp()
                    }))
                    return

                # Build JWT payload similar to production
                iat = int(time.time())
                exp = iat + 30 * 24 * 60 * 60  # 30 days
                payload = {
                    'userId': username,
                    'permissionGroup': '',
                    'https://auth0.yarbo.com/roles': [],
                    'https://auth0.yarbo.com/email': username,
                    'iss': 'https://dev-6ubfuqym1d3m0mq1.us.auth0.com/',
                    'sub': 'auth0|67e9930075b689b7db2688df',
                    'aud': [
                        'https://auth0-jwt-authorizer',
                        'https://dev-6ubfuqym1d3m0mq1.us.auth0.com/userinfo'
                    ],
                    'iat': iat,
                    'exp': exp,
                    'scope': 'openid profile offline_access',
                    'gty': 'password',
                    'azp': 'SL1GSNy3VmCLTMl01qPkwqjY4xm66i0',
                    'permissions': []
                }
                access_token = create_jwt(payload, iat, exp)
                self.access_tokens[client_ip] = access_token
                self.user_ids[client_ip] = username

                response = {
                    'code': '00000',
                    'data': {
                        'accessToken': access_token,
                        'refreshToken': secrets.token_hex(91),
                        'expiresIn': 2592000,
                        'jti': str(uuid.uuid4()),
                        'snList': [],
                        'userId': username
                    },
                    'message': 'ok',
                    'success': True,
                    'timestamp': self.get_timestamp()
                }
                self.send_json_response(200, add_sign_to_response(response))
            except Exception:
                self.send_json_response(400, add_sign_to_response({
                    'code': '400', 'data': None,
                    'message': 'Invalid JSON',
                    'success': False, 'timestamp': self.get_timestamp()
                }))
            return

        # ------------------- /refreshToken ---------------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/refreshToken':
            print(f"POST refreshToken: {client_ip}")
            try:
                data = parsed_json or json.loads(body or "{}")
            except Exception:
                data = {}

            incoming_refresh = (
                data.get('refresh_token')
                or data.get('refreshToken')
                or secrets.token_hex(91)
            )

            iat = int(time.time())
            exp = iat + 30 * 24 * 60 * 60  # 30 days

            username = self.user_ids.get(client_ip, 'test-user')

            payload = {
                'userId': username,
                'permissionGroup': '',
                'https://auth0.yarbo.com/roles': [],
                'https://auth0.yarbo.com/email': username,
                'iss': 'https://dev-6ubfuqym1d3m0mq1.us.auth0.com/',
                'sub': 'auth0|67e9930075b689b7db2688df',
                'aud': [
                    'https://auth0-jwt-authorizer',
                    'https://dev-6ubfuqym1d3m0mq1.us.auth0.com/userinfo'
                ],
                'iat': iat,
                'exp': exp,
                'scope': 'openid profile offline_access',
                'gty': 'refresh_token',
                'azp': 'SL1GSNy3VmCLTMl01qPkwqjY4xm66i0',
                'permissions': []
            }

            access_token = create_jwt(payload, iat, exp)
            self.access_tokens[client_ip] = access_token
            self.user_ids[client_ip] = username

            resp = {
                'code': '00000',
                'data': {
                    'accessToken': access_token,
                    'refreshToken': incoming_refresh,
                    'expiresIn': 2592000,
                    'jti': str(uuid.uuid4()),
                    'snList': [],
                    'userId': username
                },
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------- /getAgoraToken -------------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/getAgoraToken':
            if not auth_ok: reject_unauthorized(); return
            if not parsed_json or not all(k in parsed_json for k in ['uid', 'channel_name', 'sn']):
                self.send_json_response(400, add_sign_to_response({
                    'code': '400', 'data': None,
                    'message': 'Missing uid/channel_name/sn',
                    'success': False, 'timestamp': self.get_timestamp()
                })); return

            sn = parsed_json['sn']
            uid = parsed_json['uid']
            channel = parsed_json['channel_name']
            update_key = parsed_json.get('update_key', False)
            server_info = self.load_test_server_info()
            if not server_info or not any(d['serialNum'] == sn for d in server_info.get('deviceList', [])):
                self.send_json_response(400, add_sign_to_response({
                    'code': '400', 'data': None,
                    'message': f"Invalid serialNum: {sn}",
                    'success': False, 'timestamp': self.get_timestamp()
                })); return

            if sn in _agora_tokens and not update_key:
                agora_data = _agora_tokens[sn]
            else:
                app_id = '4zx17x5q7l'
                app_cert = '0123456789abcdef0123456789abcdef'
                agora_data = generate_agora_token(app_id, app_cert, channel, uid)
                _agora_tokens[sn] = agora_data

            resp = {
                'code': '00000', 'data': agora_data,
                'message': 'success', 'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------- other known POSTs ----------------------
        if path == 'Stage/yarbo/robot-service/commonUser/notification/getNotificationVos':
            if not auth_ok: reject_unauthorized(); return
            resp = {'code': '00000', 'data': {}, 'message': 'ok',
                    'success': True, 'timestamp': self.get_timestamp()}
            self.send_json_response(200, add_sign_to_response(resp))
            return

        if path == 'Stage/yarbo/robot-service/robot/commonUser/logout':
            if not auth_ok: reject_unauthorized(); return
            self.access_tokens.pop(client_ip, None)
            resp = {'code': '00000', 'data': None, 'message': 'ok',
                    'success': True, 'timestamp': self.get_timestamp()}
            self.send_json_response(200, add_sign_to_response(resp))
            return

        if path == 'Stage/admin/listPlanHistoryBySn':
            if not auth_ok: reject_unauthorized(); return
            resp = {
                "code": "00000",
                "data": {"planHistory": []},
                "success": True,
                "message": "ok",
                "timestamp": self.get_timestamp()
            }
            self.send_json_response(200, add_sign_to_response(resp))
            return

        # ------------------- fallback -------------------------------
        print(f"Unsupported POST: {path}")
        self.send_json_response(404, add_sign_to_response({
            'code': '404', 'data': None, 'message': 'Not Found',
            'success': False, 'timestamp': self.get_timestamp()
        }))

    # ------------------------------------------------------------------
    # Unsupported methods
    # ------------------------------------------------------------------
    def do_HEAD(self):   self._method_not_allowed()
    def do_PUT(self):    self._method_not_allowed()
    def do_DELETE(self): self._method_not_allowed()
    def do_PATCH(self):  self._method_not_allowed()
    def do_OPTIONS(self):self._method_not_allowed()

    def _method_not_allowed(self):
        self.send_json_response(405, add_sign_to_response({
            'code': '405', 'data': None,
            'message': 'Method Not Allowed',
            'success': False, 'timestamp': self.get_timestamp()
        }))

# ----------------------------------------------------------------------
# TLS-aware server
# ----------------------------------------------------------------------
class TLSServer(socketserver.ThreadingTCPServer):
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def get_request(self):
        client_socket, client_address = self.socket.accept()
        print(f"TLS connection attempt from {client_address[0]}:{client_address[1]}")
        try:
            ssl_socket = self.ssl_context.wrap_socket(client_socket, server_side=True)
            ver = ssl_socket.version() or "?"
            cip = ssl_socket.cipher()[0] if ssl_socket.cipher() else "?"
            print(f"TLS OK – {ver} {cip}")
            return ssl_socket, client_address
        except Exception as e:
            print(f"TLS failed: {e}")
            client_socket.close()
            raise

# ----------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Yarbo test API server (TLS + sign + Play-Store)')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=8081)
    parser.add_argument('--cert', default='CA/server.crt')
    parser.add_argument('--key',  default='CA/server.key')
    parser.add_argument('--tls-version', default='TLSv1.3', choices=['TLSv1.2', 'TLSv1.3'])
    parser.add_argument('--showResponses', action='store_true')
    parser.add_argument('--showRequests', action='store_true')
    args = parser.parse_args()
    YarboRequestHandler.show_responses = args.showResponses
    YarboRequestHandler.show_requests = args.showRequests

    for f in (args.cert, args.key, 'testServerInfo.json'):
        if not os.path.exists(f):
            print(f"Missing required file: {f}")
            sys.exit(1)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3 if args.tls_version == 'TLSv1.3' else ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=args.cert, keyfile=args.key)

    server_address = (args.host, args.port)
    httpd = TLSServer(server_address, YarboRequestHandler)
    httpd.ssl_context = ctx

    print(f"Yarbo test server running on https://{args.host}:{args.port}")
    print("Certificates:")
    print(f"  cert: {args.cert}")
    print(f"  key : {args.key}")
    print("\nAvailable endpoints:")
    print("   • GET  /store/apps/details?id=com.hanyang.yarbo → 200 OK")
    print("   • POST /dev/app → 200 OK (no auth)")
    print("   • All successful JSON responses contain a `sign` field")
    print("   • JWT login valid for 30 days\n")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        httpd.server_close()

if __name__ == '__main__':
    main()

