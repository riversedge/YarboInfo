#!/usr/bin/env python3
"""yarboTestServer.py

Test server that mimics the real Yarbo API.
- Handles the two extra GET endpoints with empty JSON.
- Accepts any Bearer token, updates the stored one and prints a warning on mismatch.
- Returns a **real-looking JWT** on login (same claims, RSA-4096 signed, 2158-hex length).
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
from cryptography.hazmat.primitives import serialization

# ----------------------------------------------------------------------
# RSA key generation (cached on first login)
# ----------------------------------------------------------------------
_rsa_private_key = None
_rsa_public_pem = None

def _get_rsa_key():
    """Generate (or reuse) a 4096-bit RSA key pair and return (priv, pub_pem)."""
    global _rsa_private_key, _rsa_public_pem
    if _rsa_private_key is not None:
        return _rsa_private_key, _rsa_public_pem

    # Generate a fresh 4096-bit key – this mimics the real server.
    _rsa_private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    pem = _rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    # Also keep the public PEM for debugging (not used by the server)
    public_key = _rsa_private_key.public_key()
    _rsa_public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return _rsa_private_key, _rsa_public_pem

# ----------------------------------------------------------------------
# JWT helper
# ----------------------------------------------------------------------
def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

def _base64url_decode(s: str) -> bytes:
    pad = '=' * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s + pad)

def create_jwt(payload: dict, iat: int, exp: int) -> str:
    """Create a RS256 JWT that looks exactly like the real server."""
    header = {
        "alg": "RS256",
        "typ": "JWT",
        # The real server uses a kid that points to its public key – we keep the same.
        "kid": "xAZNn4-ZW-t4hBO8p3YDO"
    }

    header_enc = _base64url_encode(json.dumps(header, separators=(',', ':')).encode())
    payload_enc = _base64url_encode(json.dumps(payload, separators=(',', ':')).encode())
    signing_input = f"{header_enc}.{payload_enc}".encode()

    priv_key, _ = _get_rsa_key()
    signature = priv_key.sign(
        signing_input,
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    sig_enc = _base64url_encode(signature)
    return f"{header_enc}.{payload_enc}.{sig_enc}"

# ----------------------------------------------------------------------
# Request handler
# ----------------------------------------------------------------------
class YarboRequestHandler(http.server.BaseHTTPRequestHandler):
    show_responses = False

    # ------------------------------------------------------------------
    # Auth – accept any Bearer, update on mismatch, warn
    # ------------------------------------------------------------------
    def auth_matches_or_adopt(self, client_ip, stored_token, auth_header):
        if not isinstance(auth_header, str) or not auth_header.startswith("Bearer "):
            return False, stored_token, "Bearer None"

        incoming = auth_header.split(" ", 1)[1].strip()
        if not incoming:
            return False, stored_token, "Bearer None"

        # First request for this IP → adopt
        if stored_token is None or str(stored_token).strip().lower() in ("none", "", "null"):
            self.access_tokens[client_ip] = incoming
            print(f"WARNING: Updating access token for Client IP: {client_ip} to new token {incoming[:10]}... (adopted)")
            return True, incoming, f"Bearer {incoming}"

        # Mismatch → update + warn
        if stored_token != incoming:
            print(f"WARNING: Updating access token mismatch for Client IP: {client_ip}. "
                  f"Old: {stored_token[:10]}..., New: {incoming[:10]}...")
            self.access_tokens[client_ip] = incoming
            return True, incoming, f"Bearer {incoming}"

        return True, stored_token, f"Bearer {stored_token}"

    # Shared dict: client_ip → current token
    access_tokens = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def load_test_server_info(self):
        try:
            with open('testServerInfo.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading testServerInfo.json: {e}")
            return None

    def get_timestamp(self):
        return int(time.time() * 1000)

    def get_one_week_ago(self):
        return int((datetime.datetime.now() - datetime.timedelta(days=7)).timestamp())

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
            time.sleep(0.1)                     # mimic real latency
            if getattr(self, 'show_responses', False):
                compact = json.dumps(data, separators=(',', ':'))
                print(f"SENDING [{status}] {self.path} -> {compact}")
            self.wfile.write(payload.encode('utf-8'))
            self.wfile.flush()
            print(f"Sent response for {self.path}: Status {status}, Client IP: {self.client_address[0]}")
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
        one_week_ago = self.get_one_week_ago()

        # ------------------------------------------------------------------
        # Unauthenticated endpoints
        # ------------------------------------------------------------------
        if path == 'Stage/app/getPolicyKey':
            self.send_json_response(200, {
                "code": "00000",
                "data": {"privacyKey": "7feb6023c570477d8b5ae66dc6e0cd5d"},
                "message": "ok",
                "success": True,
                "timestamp": self.get_timestamp()
            })
            return

        # ------------------------------------------------------------------
        # Authenticated endpoints
        # ------------------------------------------------------------------
        auth_ok, token_after, _ = self.auth_matches_or_adopt(client_ip, stored_token,
                                                            self.headers.get('authorization'))

        # ------------------------------------------------------------------
        # Helper to reject on bad auth
        # ------------------------------------------------------------------
        def reject():
            self.send_json_response(401, {
                "code": "401",
                "data": None,
                "message": "Invalid token",
                "success": False,
                "timestamp": self.get_timestamp()
            })

        # ------------------------------------------------------------------
        # Existing authenticated GETs
        # ------------------------------------------------------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/getUesrInfo':
            if not auth_ok: reject(); return
            info = self.load_test_server_info()
            if not info:
                self.send_json_response(500, {"code":"500","data":None,"message":"Server config error","success":False,"timestamp":self.get_timestamp()})
                return
            self.send_json_response(200, {
                "code": "00000",
                "data": {
                    "userId": info['userId'],
                    "username": info['username'],
                    "nickName": info['nickName'],
                    "phone": "", "email": info['userId'],
                    "country": "", "state": "", "city": "", "address": "", "zipCode": "",
                    "gmtCreate": one_week_ago * 1000,
                    "gmtModified": one_week_ago * 1000
                },
                "message": "ok", "success": True,
                "timestamp": self.get_timestamp()
            })
            return

        if path == 'Stage/yarbo/commonUser/getLatestPubVersion':
            if not auth_ok: reject(); return
            self.send_json_response(200, {
                "code":"00000","data":{"id":0,"version":"","gmtCreate":one_week_ago*1000,"gmtModified":one_week_ago*1000},
                "message":"ok","success":True,"timestamp":self.get_timestamp()
            })
            return

        # ------------------------------------------------------------------
        # NEW EMPTY ENDPOINTS
        # ------------------------------------------------------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/downloadUserAvatar':
            if not auth_ok: reject(); return
            self.send_json_response(200, {
                "code":"00000","data":{},"message":"ok","success":True,
                "timestamp":self.get_timestamp()
            })
            return

        if path.startswith('Stage/yarbo/robot/rasterBackground/get'):
            if not auth_ok: reject(); return
            self.send_json_response(200, {
                "code":"00000","data":{},"message":"ok","success":True,
                "timestamp":self.get_timestamp()
            })
            return

        # ------------------------------------------------------------------
        # Existing bind list
        # ------------------------------------------------------------------
        if path == 'Stage/yarbo/robot-service/commonUser/userRobotBind/getUserRobotBindVos':
            if not auth_ok: reject(); return
            info = self.load_test_server_info()
            if not info:
                self.send_json_response(500, {"code":"500","data":None,"message":"Server config error","success":False,"timestamp":self.get_timestamp()})
                return
            devices = [
                {
                    "master":1,
                    "masterUsername":info['username'],
                    "masterNickname":info['nickName'],
                    "serialNum":d['serialNum'],
                    "headType":d['headType'],
                    "deviceNickname":d.get('deviceNickname',''),
                    "gmtCreate":one_week_ago*1000,
                    "gmtModified":one_week_ago*1000
                } for d in info['deviceList']
            ]
            self.send_json_response(200, {
                "code":"00000","data":{"deviceList":devices},
                "message":"ok","success":True,"timestamp":self.get_timestamp()
            })
            return

        # ------------------------------------------------------------------
        # Misc
        # ------------------------------------------------------------------
        if path == 'Stage/yarbo/commonUser/questionnaire':
            self.send_json_response(403, {
                "code":"403","data":None,"message":"Invalid authorization header",
                "success":False,"timestamp":self.get_timestamp()
            })
            return

        # ------------------------------------------------------------------
        # 404
        # ------------------------------------------------------------------
        print(f"Unsupported GET: {self.path} from {client_ip}")
        self.send_json_response(404, {
            "code":"404","data":None,"message":"Not Found",
            "success":False,"timestamp":self.get_timestamp()
        })

    # ------------------------------------------------------------------
    # POST
    # ------------------------------------------------------------------
    def do_POST(self):
        path = self.path.lstrip('/')
        client_ip = self.client_address[0]
        one_week_ago = self.get_one_week_ago()

        # ------------------- read body -------------------
        try:
            cl = int(self.headers.get('Content-Length', 0))
        except Exception:
            cl = 0
        body_bytes = self.rfile.read(cl) if cl else b''
        body = body_bytes.decode('utf-8', errors='replace')
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None

        # ------------------- /login -------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/login':
            print(f"POST login: Client IP: {client_ip}, Body: {body[:200]}...")
            if not parsed or 'username' not in parsed or 'password' not in parsed:
                self.send_json_response(400, {
                    "code":"400","data":None,"message":"Missing username or password",
                    "success":False,"timestamp":self.get_timestamp()
                })
                return

            username = parsed['username']

            # ---- build realistic JWT ----
            iat = int(time.time())
            exp = iat + 30 * 24 * 60 * 60          # 30 days (real server uses 30d)
            payload = {
                "userId": username,
                "permissionGroup": "",
                "https://auth0.yarbo.com/roles": [],
                "https://auth0.yarbo.com/email": username,
                "iss": "https://dev-6ubfuqym1d3m0mq1.us.auth0.com/",
                "sub": f"auth0|67e9930075b689b7db2688df",   # placeholder sub
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
            access_token_jwt = create_jwt(payload, iat, exp)

            # ---- refresh token (random hex, 182 chars) ----
            refresh_token = secrets.token_hex(91)

            # ---- store the JWT for this client (so subsequent calls can reuse it) ----
            self.access_tokens[client_ip] = access_token_jwt
            print(f"POST login: Generated JWT (len={len(access_token_jwt)}) for {client_ip}")

            self.send_json_response(200, {
                "code": "00000",
                "data": {
                    "accessToken": access_token_jwt,
                    "expiresIn": 2592000,                 # 30 days in seconds (real server)
                    "jti": "",
                    "refreshToken": refresh_token,
                    "snList": [],
                    "userId": username
                },
                "message": "ok",
                "success": True,
                "timestamp": self.get_timestamp()
            })
            return

        # ------------------- notification -------------------
        if path == 'Stage/yarbo/robot-service/commonUser/notification/getNotificationVos':
            stored = self.access_tokens.get(client_ip)
            ok, _, _ = self.auth_matches_or_adopt(client_ip, stored, self.headers.get('authorization'))
            if not ok:
                self.send_json_response(401, {"code":"401","data":None,"message":"Invalid token","success":False,"timestamp":self.get_timestamp()})
                return
            self.send_json_response(200, {
                "code":"00000","data":{},"message":"ok","success":True,
                "timestamp":self.get_timestamp()
            })
            return

        # ------------------- logout -------------------
        if path == 'Stage/yarbo/robot-service/robot/commonUser/logout':
            stored = self.access_tokens.get(client_ip)
            ok, _, _ = self.auth_matches_or_adopt(client_ip, stored, self.headers.get('authorization'))
            if not ok:
                self.send_json_response(401, {"code":"401","data":None,"message":"Invalid token","success":False,"timestamp":self.get_timestamp()})
                return
            self.access_tokens.pop(client_ip, None)
            self.send_json_response(200, {
                "code":"00000","data":None,"message":"ok","success":True,
                "timestamp":self.get_timestamp()
            })
            return

        # ------------------- 404 -------------------
        print(f"Unsupported POST: {self.path} from {client_ip}")
        self.send_json_response(404, {
            "code":"404","data":None,"message":"Not Found",
            "success":False,"timestamp":self.get_timestamp()
        })

    # ------------------------------------------------------------------
    # Other HTTP verbs – all 405
    # ------------------------------------------------------------------
    def do_HEAD(self):  self._method_not_allowed()
    def do_PUT(self):   self._method_not_allowed()
    def do_DELETE(self):self._method_not_allowed()
    def do_PATCH(self): self._method_not_allowed()
    def do_OPTIONS(self):self._method_not_allowed()

    def _method_not_allowed(self):
        print(f"Unsupported {self.command} {self.path} from {self.client_address[0]}")
        self.send_json_response(405, {
            "code":"405","data":None,"message":"Method Not Allowed",
            "success":False,"timestamp":self.get_timestamp()
        })

# ----------------------------------------------------------------------
# TLS server with detailed handshake logging
# ----------------------------------------------------------------------
class TLSServer(socketserver.ThreadingTCPServer):
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def get_request(self):
        client_sock, addr = self.socket.accept()
        print(f"TLS connection attempt from {addr[0]}:{addr[1]}")
        try:
            ssl_sock = self.ssl_context.wrap_socket(client_sock, server_side=True,
                                                   do_handshake_on_connect=True)
            ver = ssl_sock.version() or "Unknown"
            cipher = ssl_sock.cipher() or ("Unknown", "", 0)
            cert = ssl_sock.getpeercert() or "None"
            print(f"TLS OK {addr[0]}:{addr[1]} | {ver} | {cipher[0]} | cert:{cert}")
            return ssl_sock, addr
        except ssl.SSLError as e:
            print(f"TLS FAILED {addr[0]}:{addr[1]} | {e} | reason:{getattr(e,'reason','N/A')}")
            client_sock.close()
            raise
        except Exception as e:
            print(f"Connection error {addr[0]}:{addr[1]} | {e}")
            client_sock.close()
            raise

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Yarbo test server (TLS + realistic JWT)')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=8081)
    parser.add_argument('--cert', default='CA/server.crt')
    parser.add_argument('--key',  default='CA/server.key')
    parser.add_argument('--tls-version', default='TLSv1.3', choices=['TLSv1.2','TLSv1.3'])
    parser.add_argument('--showResponses', action='store_true')
    args = parser.parse_args()
    YarboRequestHandler.show_responses = args.showResponses

    for p in (args.cert, args.key, 'testServerInfo.json'):
        if not os.path.exists(p):
            print(f"ERROR: {p} missing.")
            sys.exit(1)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3 if args.tls_version == 'TLSv1.3' else ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=args.cert, keyfile=args.key)

    server = TLSServer((args.host, args.port), YarboRequestHandler)
    server.ssl_context = ctx

    print(f"Starting Yarbo test server https://{args.host}:{args.port} ({args.tls_version})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.server_close()

if __name__ == '__main__':
    main()
