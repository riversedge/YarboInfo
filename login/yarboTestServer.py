#!/usr/bin/env python3
"""yarboTestServer.py

A test server that mimics the Yarbo API server, handling the same endpoints as yarboLoginAndListCores.py.
Listens on a specified port (default 8081) with TLS, using a self-signed certificate.
Reads user and device information from testServerInfo.json in the same directory.
Generates a random accessToken for login and uses a timestamp of one week ago for gmtCreate fields.
Stores access tokens in a shared dictionary keyed by client IP to persist across requests.
Uses keep-alive headers and response flushing to prevent ConnectionResetError.
Logs unsupported requests and detailed TLS connection events (success and failure).

Requirements:
- Python 3.7+ (uses stdlib)
- OpenSSL (to generate CA/server.crt and CA/server.key)
- testServerInfo.json in the same directory
- CA/server.crt and CA/server.key for TLS in CA/

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

class YarboRequestHandler(http.server.BaseHTTPRequestHandler):
    
    def auth_matches_or_adopt(self, client_ip, access_token, auth_header):
        """
        Accept any bearer token if expected token is "None" or not set.
        If expected is empty/None/"None", adopt the incoming bearer token for this client and pass auth.
        Returns (auth_ok: bool, access_token_after: str or None, expected_header: str).
        """
        expected_token = access_token
        # Normalize string forms of none
        if expected_token is None or (isinstance(expected_token, str) and expected_token.strip().lower() in ("none", "", "null" )):
            # If header present and looks like Bearer, adopt it
            if isinstance(auth_header, str) and auth_header.startswith("Bearer "):
                expected_token = auth_header.split(" ", 1)[1]
                # Persist adopted token for this client for subsequent calls
                try:
                    if not hasattr(self, "access_tokens") or self.access_tokens is None:
                        self.access_tokens = {}
                    self.access_tokens[client_ip] = expected_token
                except Exception:
                    pass
                return True, expected_token, f"Bearer {expected_token}"
            # No header to adopt -> not ok
            return False, expected_token, "Bearer None"
        # If we had a concrete expected token, compare exact header
        expected_header = f"Bearer {expected_token}"
        return (auth_header == expected_header), expected_token, expected_header
    # Shared dictionary to store access tokens by client IP
    access_tokens = {}

    def load_test_server_info(self):
        """Load testServerInfo.json from the same directory and return its contents."""
        try:
            with open('testServerInfo.json', 'r', encoding='utf-8') as f:
                return json.loads(f.read())
        except Exception as e:
            print(f"Error loading testServerInfo.json: {e}")
            return None

    def get_timestamp(self):
        """Return current Unix timestamp in milliseconds."""
        return int(time.time() * 1000)

    def get_one_week_ago(self):
        """Return Unix timestamp in seconds for one week ago from today."""
        one_week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
        return int(one_week_ago.timestamp())

    def send_json_response(self, status, data):
        """Send a JSON response with the given status code."""
        try:
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            if status == 200:
                self.send_header('Connection', 'keep-alive')
                self.send_header('Keep-Alive', 'timeout=5, max=100')
            else:
                self.send_header('Connection', 'close')
            response = json.dumps(data, indent=2)
            self.send_header('Content-Length', len(response.encode('utf-8')))
            self.end_headers()
            time.sleep(0.1)  # Small delay to mimic real server timing
            self.wfile.write(response.encode('utf-8'))
            self.wfile.flush()  # Ensure response is sent
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
        server_info = self.load_test_server_info()
        one_week_ago = self.get_one_week_ago()
        client_ip = self.client_address[0]

        # Get the access token for this client
        access_token = self.access_tokens.get(client_ip)

        if path == 'Stage/app/getPolicyKey':
            response = {
                'code': '00000',
                'data': {'privacyKey': '7feb6023c570477d8b5ae66dc6e0cd5d'},
                'message': 'ok',
                'success': True,
                'timestamp': self.get_timestamp()
            }
            self.send_json_response(200, response)

        elif path == 'Stage/yarbo/robot-service/robot/commonUser/getUesrInfo':
            auth_ok, access_token, expected_header = self.auth_matches_or_adopt(client_ip, access_token, self.headers.get('authorization'))
            if not auth_ok:
                print(f"GET getUesrInfo: Client IP: {client_ip}, Received authorization: {self.headers.get('authorization')}, Expected: {expected_header}...")
                self.send_json_response(401, {
                    'code': '401',
                    'data': None,
                    'message': 'Invalid token',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
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
            auth_ok, access_token, expected_header = self.auth_matches_or_adopt(client_ip, access_token, self.headers.get('authorization'))
            if not auth_ok:
                print(f"GET getLatestPubVersion: Client IP: {client_ip}, Received authorization: {self.headers.get('authorization')}, Expected: {expected_header}...")
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

        elif path == 'Stage/yarbo/robot-service/commonUser/userRobotBind/getUserRobotBindVos':
            auth_ok, access_token, expected_header = self.auth_matches_or_adopt(client_ip, access_token, self.headers.get('authorization'))
            if not auth_ok:
                print(f"GET getUserRobotBindVos: Client IP: {client_ip}, Received authorization: {self.headers.get('authorization')}, Expected: {expected_header}...")
                self.send_json_response(401, {
                    'code': '401',
                    'data': None,
                    'message': 'Invalid token',
                    'success': False,
                    'timestamp': self.get_timestamp()
                })
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
                'data': {'robotBindVoList': device_list},
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
        one_week_ago = self.get_one_week_ago()
        client_ip = self.client_address[0]

        # --- Begin: universal body read to avoid leftover bytes on socket ---
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
        # --- End: universal body read ---

        if path == 'Stage/yarbo/robot-service/robot/commonUser/login':
            print(f"POST login: Client IP: {client_ip}, Body: {body}")
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
                # Store access token for this client IP
                access_token = secrets.token_hex(1079)  # 1079 bytes = 2158 hex chars for JWT
                self.access_tokens[client_ip] = access_token
                print(f"POST login: Client IP: {client_ip}, Generated access_token {access_token}... (length: {len(access_token)})")
                response = {
                    'code': '00000',
                    'data': {
                        'accessToken': access_token,
                        'refreshToken': secrets.token_hex(91),  # 91 bytes = 182 hex chars
                        'expiresIn': 86400000,
                        'gmtCreate': one_week_ago * 1000,
                        'gmtModified': one_week_ago * 1000
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

        elif path == 'Stage/yarbo/robot-service/commonUser/notification/getNotificationVos':
            access_token = self.access_tokens.get(client_ip)
            auth_ok, access_token, expected_header = self.auth_matches_or_adopt(client_ip, access_token, self.headers.get('authorization'))
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
            access_token = self.access_tokens.get(client_ip)
            auth_ok, access_token, expected_header = self.auth_matches_or_adopt(client_ip, access_token, self.headers.get('authorization'))
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
            # Clear the token for this client
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
    """Custom TCPServer to log detailed TLS connection attempts."""
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def get_request(self):
        """Override to log connection attempts and TLS handshake with details."""
        client_socket, client_address = self.socket.accept()
        print(f"TLS connection attempt from Client IP: {client_address[0]}:{client_address[1]}")
        try:
            # Perform TLS handshake
            ssl_socket = self.ssl_context.wrap_socket(client_socket, server_side=True, do_handshake_on_connect=True)
            # Log TLS version, cipher, and client certificates (if any)
            tls_version = ssl_socket.version() or "Unknown"
            cipher = ssl_socket.cipher() or ("Unknown", "Unknown", "Unknown")
            client_certs = ssl_socket.getpeercert() or "None"
            print(f"TLS connection successful from Client IP: {client_address[0]}:{client_address[1]}, TLS Version: {tls_version}, Cipher: {cipher[0]}, Client Cert: {client_certs}")
            return ssl_socket, client_address
        except ssl.SSLError as e:
            print(f"TLS connection failed from Client IP: {client_address[0]}:{client_address[1]}: {e}, Error Code: {getattr(e, 'errno', 'N/A')}, Reason: {getattr(e, 'reason', 'N/A')}")
            try:
                # Attempt to capture client hello ciphers (requires manual handshake)
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
    parser.add_argument('--port', type=int, default=8081, help='Port to listen on (default: 8081)')
    parser.add_argument('--cert', default='CA/server.crt', help='Path to SSL certificate file (default: CA/server.crt)')
    parser.add_argument('--key', default='CA/server.key', help='Path to SSL private key file (default: CA/server.key)')
    parser.add_argument('--tls-version', default='TLSv1.3', choices=['TLSv1.2', 'TLSv1.3'], help='TLS protocol version (default: TLSv1.3)')
    args = parser.parse_args()

    if not os.path.exists(args.cert) or not os.path.exists(args.key):
        print("Error: Certificate (--cert) or key (--key) file not found. Generate with:")
        print("openssl req -x509 -newkey rsa:4096 -keyout CA/server.key -out CA/server.crt -days 365 -nodes -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost'")
        sys.exit(1)

    if not os.path.exists('testServerInfo.json'):
        print("Error: testServerInfo.json not found in current directory.")
        sys.exit(1)

    # Create SSL context
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3 if args.tls_version == 'TLSv1.3' else ssl.TLSVersion.TLSv1_2
    try:
        ssl_context.load_cert_chain(certfile=args.cert, keyfile=args.key)
    except Exception as e:
        print(f"Error loading SSL certificate/key: {e}")
        sys.exit(1)

    # Create server with custom TLSServer
    server = TLSServer(('localhost', args.port), YarboRequestHandler)
    server.ssl_context = ssl_context  # Attach SSL context to server

    print(f"Starting Yarbo test server on https://localhost:{args.port} with {args.tls_version}")
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
