#!/usr/bin/env python3
"""yarboTestServer.py

Test server mimicking the Yarbo API server.
- Handles GET /downloadUserAvatar and /rasterBackground/get with empty JSON responses.
- Accepts any non-empty Bearer token, updates on mismatch with warning.
- Generates realistic RS256 JWT for /login (2158 hex chars, same claims as real server).
- Supports POST /getAgoraToken with consistent Agora token/key/salt per core based on sn.
- Requires testServerInfo.json and TLS cert/key (CA/server.crt, CA/server.key).

Requirements:
- Python 3.7+ with cryptography (pip install cryptography)
- OpenSSL for cert generation
- testServerInfo.json with userId, username, nickName, deviceList (serialNum, headType)

Example usage:
  python3 yarboTestServer.py --port 8081 --cert CA/server.crt --key CA/server.key

Generate self-signed certificate:
  openssl req -x509 -newkey rsa:4096 -keyout CA/server.key -out CA/server.crt -days 365 -nodes -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost"
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
import hmac
import hashlib
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from agora_token_builder import RtcTokenBuilder

# Cache for RSA key (JWT signing) and Agora tokens (per core)
_rsa_private_key = None
_rsa_public_pem = None
_agora_tokens = {}  # serialNum -> {token, key, salt}

# JWT helper functions
def _base64url_encode(data: bytes) -> str:
    """Base64url encode without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

def _base64url_decode(s: str) -> bytes:
    """Base64url decode with padding."""
    pad = '=' * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s + pad)

def _get_rsa_key():
    """Generate or reuse a 4096-bit RSA key pair."""
    global _rsa_private_key, _rsa_public_pem
    if _rsa_private_key is not None:
        return _rsa_private_key, _rsa_public_pem
    _rsa_private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    pem = _rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    public_key = _rsa_private_key.public_key()
    _rsa_public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return _rsa_private_key, _rsa_public_pem

def create_jwt(payload: dict, iat: int, exp: int) -> str:
    """Create RS256 JWT matching real server's structure."""
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

def generate_agora_token(
    app_id: str,
    app_cert: str,
    channel: str,
    uid: str,
    expire: int = 3600,
) -> dict:
    """
    Generate valid Agora RTC token using official builder.
    Returns {"token": "...", "key": "...", "salt": "..."}
    """
    # Convert uid to int; 0 = dynamic UID
    uid_int = int(uid) if uid and uid.isdigit() else 0

    # Role = 1 (Attendee: full publish/subscribe)
    role = 1

    # Token expires in `expire` seconds
    privilege_expired_ts = int(time.time()) + expire

    # Build token
    token = RtcTokenBuilder.buildTokenWithUid(
        appId=app_id,
        appCertificate=app_cert,
        channelName=channel,
        uid=uid_int,
        role=role,
        privilegeExpiredTs=privilege_expired_ts,
    )

    # Consistent key & salt (matches real server)
    combined = f"{channel}{uid}"
    key = hashlib.sha256(combined.encode()).hexdigest()
    salt = base64.b64encode(combined.encode()).decode()

    return {"token": token, "key": key, "salt": salt}

class YarboRequestHandler(http.server.BaseHTTPRequestHandler):
    show_responses = False

    def auth_matches_or_adopt(self, client_ip, stored_token, auth_header):
        """Accept any non-empty Bearer token, update on mismatch with warning."""
        if not isinstance(auth_header, str) or not auth_header.startswith("Bearer "):
            return False, stored_token, "Bearer None"
        incoming = auth_header.split(" ", 1)[1].strip()
        if not incoming:
            return False, stored_token, "Bearer None"
        if stored_token is None or str(stored_token).strip().lower() in ("none", "", "null"):
            self.access_tokens[client_ip] = incoming
            print(f"WARNING: Updating access token for Client IP: {client_ip} to new token {incoming[:10]}... (adopted)")
            return True, incoming, f"Bearer {incoming}"
        if stored_token != incoming:
            print(f"WARNING: Updating access token mismatch for Client IP: {client_ip}. "
                  f"Old: {stored_token[:10]}..., New: {incoming[:10]}...")
            self.access_tokens[client_ip] = incoming
            return True, incoming, f"Bearer {incoming}"
        return True, stored_token, f"Bearer {stored_token}"

    access_tokens = {}  # client_ip -> current token

    def load_test_server_info(self):
        """Load testServerInfo.json."""
        try:
            with open('testServerInfo.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading testServerInfo.json: {e}")
            return None

    def get_timestamp(self):
        """Current Unix timestamp in milliseconds."""
        return int(time.time() * 1000)

    def get_one_week_ago(self):
        """Unix timestamp for one week ago in seconds."""
        return int((datetime.datetime.now() - datetime.timedelta(days=7)).timestamp())

    def send_json_response(self, status, data):
        """Send JSON response with status code."""
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
            time.sleep(0.1)  # Mimic server latency
            if getattr(self, 'show_responses', False):
                compact = json.dumps(data, separators=(',', ':'))
                print(f"SENDING [{status}] {self.path} -> {compact}")
            self.wfile.write(payload.encode('utf-8'))
            self.wfile.flush()
            print(f"Sent response for {self.path}: Status {status}, Client IP: {self.client_address[0]}")
        except ssl.SSLEOFError as e:
            print(f"TLS error sending response for {self.path}: {e}, Client IP: {self.client_address[0]}")
            self.close_connection = True
        except Exception as e:
            print(f"Error sending response for {self.path}: {e}, Client IP: {self.client_address[0]}")
            self.close_connection = True

    def do_GET(self):
        """Handle GET requests."""
        path = self.path.lstrip('/')
        client_ip = self.client_address[0]
        stored_token = self.access_tokens.get(client_ip)
        one_week_ago = self.get_one_week_ago()
        server_info = self.load_test_server_info()

        if path == 'Stage/app/getPolicyKey':
            response = {
                'code': '00000',
                'data': {'privacyKey': '7feb6023c570477d8b5ae66dc6e0cd5d'},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)
            return

        auth_ok, _, expected_header = self.auth_matches_or_adopt(client_ip, stored_token, self.headers.get('authorization'))

        def reject_unauthorized():
            print(f"GET {path}: Client IP: {client_ip}, Received authorization: {self.headers.get('authorization')}, Expected: {expected_header}...")
            self.send_json_response(401, {
                'code': '401',
                'data': None,
                'message': 'Invalid token',
                'success': False,
                'timestamp': self.get_timestamp()
            })

        if path == 'Stage/yarbo/robot-service/robot/commonUser/getUesrInfo':
            if not auth_ok:
                reject_unauthorized()
                return
            if not server_info:
                self.send_json_response(500, {
                    'code': '500',
                    'data': None,
                    'message': 'Server configuration error',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return
            response = {
                'code': '00000',
                'data': {
                    'userId': server_info['userId'],
                    'username': server_info['username'],
                    'nickName': server_info['nickName'],
                    'phone': '',
                    'email': server_info['userId'],
                    'country': '',
                    'state': '',
                    'city': '',
                    'address': '',
                    'zipCode': '',
                    'gmtCreate': one_week_ago * 1000,
                    'gmtModified': one_week_ago * 1000
                },
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path == 'Stage/yarbo/commonUser/getLatestPubVersion':
            if not auth_ok:
                reject_unauthorized()
                return
            response = {
                'code': '00000',
                'data': {
                    'id': 0,
                    'version': '',
                    'gmtCreate': one_week_ago * 1000,
                    'gmtModified': one_week_ago * 1000
                },
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path == 'Stage/yarbo/robot-service/robot/commonUser/downloadUserAvatar':
            if not auth_ok:
                reject_unauthorized()
                return
            response = {
                'code': '00000',
                'data': {},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path.startswith('Stage/yarbo/robot/rasterBackground/get'):
            if not auth_ok:
                reject_unauthorized()
                return
            response = {
                'code': '00000',
                'data': {},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path == 'Stage/yarbo/robot-service/commonUser/userRobotBind/getUserRobotBindVos':
            if not auth_ok:
                reject_unauthorized()
                return
            if not server_info:
                self.send_json_response(500, {
                    'code': '500',
                    'data': None,
                    'message': 'Server configuration error',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return
            device_list = [
                {
                    'master': 1,
                    'masterUsername': server_info['username'],
                    'masterNickname': server_info['nickName'],
                    'serialNum': device['serialNum'],
                    'headType': device['headType'],
                    'deviceNickname': device.get('deviceNickname', ''),
                    'gmtCreate': one_week_ago * 1000,
                    'gmtModified': one_week_ago * 1000
                } for device in server_info['deviceList']
            ]
            response = {
                'code': '00000',
                'data': {'deviceList': device_list},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path == 'Stage/yarbo/commonUser/questionnaire':
            self.send_json_response(403, {
                'code': '403',
                'data': None,
                'message': 'Invalid authorization header',
                'success': False,
                'timestamp': self.get_timestamp()
            })

        else:
            print(f"Unsupported GET request: Path {self.path}, Client IP: {self.client_address[0]}")
            self.send_json_response(404, {
                'code': '404',
                'data': None,
                'message': 'Not Found',
                'success': False,
                'timestamp': self.get_timestamp()
            })

    def do_POST(self):
        """Handle POST requests."""
        path = self.path.lstrip('/')
        client_ip = self.client_address[0]
        one_week_ago = self.get_one_week_ago()

        # Read request body
        try:
            content_length = int(self.headers.get('Content-Length', 0))
        except (TypeError, ValueError):
            content_length = 0
        body_bytes = self.rfile.read(content_length) if content_length > 0 else b''
        body = body_bytes.decode('utf-8', errors='replace')
        parsed_json = None
        if body:
            try:
                parsed_json = json.loads(body)
            except json.JSONDecodeError:
                parsed_json = None

        if path == 'Stage/yarbo/robot-service/robot/commonUser/login':
            print(f"POST login: Client IP: {client_ip}, Body: {body[:200]}...")
            try:
                data = parsed_json if parsed_json is not None else json.loads(body)
                username = data.get('username')
                password = data.get('password')
                if not username or not password:
                    self.send_json_response(400, {
                        'code': '400',
                        'data': None,
                        'message': 'Missing username or password',
                        'success': False,
                        'timestamp': self.get_timestamp()
                    })
                    return
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
                print(f"POST login: Client IP: {client_ip}, Generated access_token {access_token[:10]}... (length: {len(access_token)})")
                response = {
                    'code': '00000',
                    'data': {
                        'accessToken': access_token,
                        'refreshToken': secrets.token_hex(91),
                        'expiresIn': 2592000,
                        'jti': '',
                        'snList': [],
                        'userId': username
                    },
                    'message': 'ok',
                    'success': True,
                    'timestamp': self.get_timestamp()
                }
                self.send_json_response(200, response)
            except json.JSONDecodeError:
                self.send_json_response(400, {
                    'code': '400',
                    'data': None,
                    'message': 'Invalid JSON in request body',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return

        elif path == 'Stage/yarbo/robot-service/robot/commonUser/getAgoraToken':
            print(f"POST getAgoraToken: Client IP: {client_ip}, Body: {body[:200]}...")
            stored_token = self.access_tokens.get(client_ip)
            auth_ok, _, expected_header = self.auth_matches_or_adopt(client_ip, stored_token, self.headers.get('authorization'))
            if not auth_ok:
                print(f"POST getAgoraToken: Client IP: {client_ip}, Received authorization: {self.headers.get('authorization')}, Expected: {expected_header}...")
                self.send_json_response(401, {
                    'code': '401',
                    'data': None,
                    'message': 'Invalid token',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return
            if not parsed_json or not all(key in parsed_json for key in ['uid', 'channel_name', 'sn']):
                self.send_json_response(400, {
                    'code': '400',
                    'data': None,
                    'message': 'Missing uid, channel_name, or sn in request body',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return
            serial_num = parsed_json['sn']
            uid = parsed_json['uid']
            channel_name = parsed_json['channel_name']
            update_key = parsed_json.get('update_key', False)
            server_info = self.load_test_server_info()
            if not server_info or not any(device['serialNum'] == serial_num for device in server_info.get('deviceList', [])):
                self.send_json_response(400, {
                    'code': '400',
                    'data': None,
                    'message': f"Invalid serialNum: {serial_num}",
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return
            # Check cache for consistent token/key/salt
            if serial_num in _agora_tokens and not update_key:
                agora_data = _agora_tokens[serial_num]
            else:
                # Use placeholder app_id and app_cert (replace with real ones if available)
                app_id = '4zx17x5q7l'
                app_cert = '0123456789abcdef0123456789abcdef'
                agora_data = generate_agora_token(app_id, app_cert, channel=channel_name, uid=uid)
                _agora_tokens[serial_num] = agora_data
            response = {
                'code': '00000',
                'data': agora_data,
                'message': 'success',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path == 'Stage/yarbo/robot-service/commonUser/notification/getNotificationVos':
            stored_token = self.access_tokens.get(client_ip)
            auth_ok, _, expected_header = self.auth_matches_or_adopt(client_ip, stored_token, self.headers.get('authorization'))
            if not auth_ok:
                print(f"POST getNotificationVos: Client IP: {client_ip}, Received authorization: {self.headers.get('authorization')}, Expected: {expected_header}...")
                self.send_json_response(401, {
                    'code': '401',
                    'data': None,
                    'message': 'Invalid token',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return
            response = {
                'code': '00000',
                'data': {},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path == 'Stage/yarbo/robot-service/robot/commonUser/logout':
            stored_token = self.access_tokens.get(client_ip)
            auth_ok, _, expected_header = self.auth_matches_or_adopt(client_ip, stored_token, self.headers.get('authorization'))
            if not auth_ok:
                print(f"POST logout: Client IP: {client_ip}, Received authorization: {self.headers.get('authorization')}, Expected: {expected_header}...")
                self.send_json_response(401, {
                    'code': '401',
                    'data': None,
                    'message': 'Invalid token',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
                return
            self.access_tokens.pop(client_ip, None)
            response = {
                'code': '00000',
                'data': None,
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        else:
            print(f"Unsupported POST request: Path {self.path}, Client IP: {self.client_address[0]}")
            self.send_json_response(404, {
                'code': '404',
                'data': None,
                'message': 'Not Found',
                'success': False,
                'timestamp': self.get_timestamp()
            })

    def do_HEAD(self):
        """Handle HEAD requests (unsupported)."""
        print(f"Unsupported HEAD request: Path {self.path}, Client IP: {self.client_address[0]}")
        self.send_json_response(405, {
            'code': '405',
            'data': None,
            'message': 'Method Not Allowed',
            'success': False,
            'timestamp': self.get_timestamp()
        })

    def do_PUT(self):
        """Handle PUT requests (unsupported)."""
        print(f"Unsupported PUT request: Path {self.path}, Client IP: {self.client_address[0]}")
        self.send_json_response(405, {
            'code': '405',
            'data': None,
            'message': 'Method Not Allowed',
            'success': False,
            'timestamp': self.get_timestamp()
        })

    def do_DELETE(self):
        """Handle DELETE requests (unsupported)."""
        print(f"Unsupported DELETE request: Path {self.path}, Client IP: {self.client_address[0]}")
        self.send_json_response(405, {
            'code': '405',
            'data': None,
            'message': 'Method Not Allowed',
            'success': False,
            'timestamp': self.get_timestamp()
        })

    def do_PATCH(self):
        """Handle PATCH requests (unsupported)."""
        print(f"Unsupported PATCH request: Path {self.path}, Client IP: {self.client_address[0]}")
        self.send_json_response(405, {
            'code': '405',
            'data': None,
            'message': 'Method Not Allowed',
            'success': False,
            'timestamp': self.get_timestamp()
        })

    def do_OPTIONS(self):
        """Handle OPTIONS requests (unsupported)."""
        print(f"Unsupported OPTIONS request: Path {self.path}, Client IP: {self.client_address[0]}")
        self.send_json_response(405, {
            'code': '405',
            'data': None,
            'message': 'Method Not Allowed',
            'success': False,
            'timestamp': self.get_timestamp()
        })

class TLSServer(socketserver.ThreadingTCPServer):
    """Custom TCPServer for TLS connection logging."""
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def get_request(self):
        """Log TLS connection attempts and handshake details."""
        client_socket, client_address = self.socket.accept()
        print(f"TLS connection attempt from Client IP: {client_address[0]}:{client_address[1]}")
        try:
            ssl_socket = self.ssl_context.wrap_socket(client_socket, server_side=True, do_handshake_on_connect=True)
            tls_version = ssl_socket.version() or "Unknown"
            cipher = ssl_socket.cipher() or ("Unknown", "Unknown", "Unknown")
            client_certs = ssl_socket.getpeercert() or "None"
            print(f"TLS connection successful from Client IP: {client_address[0]}:{client_address[1]}, TLS Version: {tls_version}, Cipher: {cipher[0]}, Client Cert: {client_certs}")
            return ssl_socket, client_address
        except ssl.SSLError as e:
            print(f"TLS connection failed from Client IP: {client_address[0]}:{client_address[1]}: {e}, Error Code: {getattr(e, 'errno', 'N/A')}, Reason: {getattr(e, 'reason', 'N/A')}")
            try:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ssl_context.minimum_version = self.ssl_context.minimum_version
                ssl_socket = ssl_context.wrap_socket(client_socket, server_side=True, do_handshake_on_connect=False)
                ssl_socket.do_handshake()
            except Exception as handshake_err:
                print(f"Client Hello debug failed: {handshake_err}, Offered Ciphers: Unavailable")
            client_socket.close()
            raise
        except Exception as e:
            print(f"Connection error from Client IP: {client_address[0]}:{client_address[1]}: {e}")
            client_socket.close()
            raise

def main():
    parser = argparse.ArgumentParser(description='Yarbo test server mimicking API endpoints with TLS.')
    parser.add_argument('--host', default='localhost', help='Host to listen on (default: localhost)')
    parser.add_argument('--port', type=int, default=8081, help='Port to listen on (default: 8081)')
    parser.add_argument('--cert', default='CA/server.crt', help='Path to SSL certificate file (default: CA/server.crt)')
    parser.add_argument('--key', default='CA/server.key', help='Path to SSL private key file (default: CA/server.key)')
    parser.add_argument('--tls-version', default='TLSv1.3', choices=['TLSv1.2', 'TLSv1.3'], help='TLS protocol version (default: TLSv1.3)')
    parser.add_argument('--showResponses', action='store_true', help='Print each JSON response sent')
    args = parser.parse_args()
    YarboRequestHandler.show_responses = bool(getattr(args, 'showResponses', False))

    if not os.path.exists(args.cert) or not os.path.exists(args.key):
        print("Error: Certificate (--cert) or key (--key) file not found. Generate with:")
        print("openssl req -x509 -newkey rsa:4096 -keyout CA/server.key -out CA/server.crt -days 365 -nodes -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost'")
        sys.exit(1)

    if not os.path.exists('testServerInfo.json'):
        print("Error: testServerInfo.json not found in current directory.")
        sys.exit(1)

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3 if args.tls_version == 'TLSv1.3' else ssl.TLSVersion.TLSv1_2
    try:
        ssl_context.load_cert_chain(certfile=args.cert, keyfile=args.key)
    except Exception as e:
        print(f"Error loading SSL certificate/key: {e}")
        sys.exit(1)

    server = TLSServer((args.host, args.port), YarboRequestHandler)
    server.ssl_context = ssl_context

    print(f"Starting Yarbo test server on https://{args.host}:{args.port} with {args.tls_version}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.server_close()
    except Exception as e:
        print(f"Server error: {e}")
        server.server_close()

if __name__ == '__main__':
    main()
