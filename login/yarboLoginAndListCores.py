#!/usr/bin/env python3
"""yarboLoginAndListCores.py

Encrypt the provided password using the app's public key (assets/rsa_key/rsa_public_key.pem
format allowed: either full PEM with BEGIN/END lines or base64-only body), using RSA PKCS#1 v1.5 padding,
base64-encode the ciphertext, and POST as {"username":..., "password": "<base64>"} to the login endpoint.
Then, recreate the series of subsequent requests as seen in the output trace.

Requirements (on the machine where you run this):
- openssl (command-line) available in PATH
- Python 3.7+ (uses only stdlib)
- Network access to the endpoints (if not --dry-run)

Example usage:
  python3 yarboLoginAndListCores.py \
    --username wewright@wesleyacoustics.com \
    --password 'MySecretPassword' \
    --show-cipher
"""

import argparse
import subprocess
import tempfile
import os
import sys
import base64
import json
import urllib.request as _urllib_request
import urllib.parse
import urllib.error
import datetime
import gzip
import time

def ensure_pem(pubkey_path):
    """Return a path to a PEM file. If the provided file already contains a PEM header,
    return it; otherwise create a temporary PEM wrapper and return its path (caller should delete it)."""
    with open(pubkey_path, 'r', encoding='utf-8', errors='ignore') as f:
        data = f.read().strip()

    if '-----BEGIN' in data:
        return pubkey_path, None
    # assume the file is base64 body; wrap it
    tf = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
    with open(tf.name, 'w', encoding='utf-8') as out:
        out.write('-----BEGIN PUBLIC KEY-----\n')
        for i in range(0, len(data), 64):
            out.write(data[i:i+64] + '\n')
        out.write('-----END PUBLIC KEY-----\n')
    return tf.name, tf.name

def openssl_encrypt_pkcs1(pubkey_path, plaintext_bytes):
    wrapped, tmpwrap = ensure_pem(pubkey_path)
    try:
        with tempfile.NamedTemporaryFile(delete=False) as pt:
            pt.write(plaintext_bytes)
            pt.flush()
            inname = pt.name
        outname = inname + '.enc'
        cmd = ['openssl', 'pkeyutl', '-encrypt', '-pubin', '-inkey', wrapped, '-in', inname, '-out', outname, '-pkeyopt', 'rsa_padding_mode:pkcs1']
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            raise RuntimeError('openssl failed: ' + proc.stderr.decode('utf-8', errors='replace'))
        data = open(outname, 'rb').read()
        return data
    finally:
        for fn in (inname, outname):
            try:
                os.remove(fn)
            except Exception:
                pass
        if tmpwrap:
            try:
                os.remove(tmpwrap)
            except Exception:
                pass

def format_timestamp(timestamp):
    """Convert Unix timestamp (in seconds) to readable date string in local timezone."""
    dt = datetime.datetime.fromtimestamp(timestamp)
    # Get the local timezone name or abbreviation
    tz_name = time.tzname[time.localtime(timestamp).tm_isdst]
    return dt.strftime('%a, %d %b %Y %H:%M:%S ') + tz_name
    #dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    #return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')

def main():
    p = argparse.ArgumentParser(description='Encrypt password with RSA PKCS#1 v1.5, post login JSON, and recreate subsequent requests.')
    p.add_argument('--pubkey', default='rsa_public_key.pem', help='Path to rsa_public_key.pem (PEM or base64 body)')
    p.add_argument('--username', required=True, help='Username for login')
    p.add_argument('--password', required=True, help='Password for login')
    p.add_argument('--url', default='https://4zx17x5q7l.execute-api.us-east-1.amazonaws.com/Stage/yarbo/robot-service/robot/commonUser/login', help='Login endpoint URL')
    p.add_argument('--dry-run', action='store_true', help='Do not POST; just print the base64 ciphertext')
    p.add_argument('--show-cipher', action='store_true', help='Print base64 ciphertext')
    p.add_argument('--show-requests', action='store_true', help='Show full request headers')
    args = p.parse_args()

    # exact byte representation used by the client: UTF-8 encoding of the password string as-is
    plaintext_bytes = args.password.encode('utf-8')

    try:
        cipher_bytes = openssl_encrypt_pkcs1(args.pubkey, plaintext_bytes)
    except Exception as e:
        print('Encryption failed:', e, file=sys.stderr)
        sys.exit(2)

    b64 = base64.b64encode(cipher_bytes).decode('ascii')
    if args.show_cipher or args.dry_run:
        print('Base64 ciphertext:')
        print(b64)
    if args.dry_run:
        return

    # Parse the login URL to extract scheme and host
    parsed = urllib.parse.urlparse(args.url)
    scheme = parsed.scheme
    host = parsed.netloc

    # Define the series of actions
    actions = [
        {
            'method': 'POST',
            'endpoint': 'yarbo/robot-service/robot/commonUser/login',
            'body': json.dumps({'username': args.username, 'password': b64}),
            'auth': False
        },
        {
            'method': 'GET',
            'endpoint': 'app/getPolicyKey',
            'body': None,
            'auth': False
        },
        {
            'method': 'GET',
            'endpoint': 'yarbo/robot-service/robot/commonUser/getUesrInfo',
            'body': None,
            'auth': True
        },
        {
            'method': 'GET',
            'endpoint': 'yarbo/commonUser/getLatestPubVersion',
            'body': None,
            'auth': True
        },
        {
            'method': 'GET',
            'endpoint': 'yarbo/robot-service/commonUser/userRobotBind/getUserRobotBindVos',
            'body': None,
            'auth': True
        },
        {
            'method': 'POST',
            'endpoint': 'yarbo/robot-service/commonUser/notification/getNotificationVos',
            'body': json.dumps({"page": 1, "size": 10}),
            'auth': True
        },
        {
            'method': 'GET',
            'endpoint': 'app/getPolicyKey',
            'body': None,
            'auth': False
        },
        {
            'method': 'GET',
            'endpoint': 'yarbo/commonUser/questionnaire',
            'body': None,
            'auth': True
        },
        {
            'method': 'POST',
            'endpoint': 'yarbo/robot-service/robot/commonUser/logout',
            'body': '',
            'auth': True
        },
    ]

    # Verify the URL matches the first endpoint
    login_endpoint = actions[0]['endpoint']
    if not args.url.endswith(login_endpoint):
        print('Error: Provided URL does not end with expected login endpoint.', file=sys.stderr)
        sys.exit(1)

    # Base URL is everything before the login endpoint
    base_url = args.url[:-len(login_endpoint)]

    access_token = None

    for action in actions:
        full_url = base_url + action['endpoint']

        headers = {
            'User-Agent': 'Dart/3.4 (dart:io)',
            'Accept-Encoding': 'gzip'
        }

        data = None
        if action['body']:
            data = action['body'].encode('utf-8')
            headers['Content-Type'] = 'application/json'
            headers['Content-Length'] = str(len(data))

        if action['auth']:
            if access_token is None:
                print('Error: Authorization required but no access token available.', file=sys.stderr)
                sys.exit(3)
            headers['Authorization'] = 'Bearer ' + access_token

        req = _urllib_request.Request(full_url, data=data, headers=headers, method=action['method'])

        # Print method and URL
        print(f"{action['method']} {full_url}")

        # Print full headers only if --show-requests is specified
        if args.show_requests:
            path = parsed.path.rsplit('/', 1)[0] + '/' + action['endpoint']
            print(f"{action['method']} /{path.split('/Stage/')[1]} HTTP/1.1")
            header_list = [(k.lower(), v) for k, v in headers.items()]
            header_list.sort(key=lambda x: x[0])
            for k, v in header_list:
                print(f"{k}: {v}")
            print(f"host: {host}")
            if data:
                print()
                print(data.decode('utf-8'))
            print()

        # Send the request
        try:
            with _urllib_request.urlopen(req, timeout=30) as r:
                status = r.status
                reason = r.reason
                resp_headers = dict(r.getheaders())
                body = r.read()
                if resp_headers.get('Content-Encoding') == 'gzip':
                    body = gzip.decompress(body)
                body_str = body.decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            status = e.code
            reason = e.reason
            resp_headers = dict(e.headers)
            body = e.read()
            if resp_headers.get('Content-Encoding') == 'gzip':
                body = gzip.decompress(body)
            body_str = body.decode('utf-8', errors='replace')
        except Exception as e:
            print('Request failed:', e, file=sys.stderr)
            sys.exit(3)

        # Print response headers
        print(f"HTTP/1.1 {status} {reason}")
        for k, v in resp_headers.items():
            print(f"{k}: {v}")
        print()

        # Handle notification endpoint specially
        if action['endpoint'] == 'yarbo/robot-service/commonUser/notification/getNotificationVos':
            try:
                resp_data = json.loads(body_str)
                if resp_data.get('data', {}).get('msgList'):
                    for msg in resp_data['data']['msgList']:
                        print("--- Notification ---")
                        print(f"To: {msg['receiver']}")
                        print(f"From: {msg['sender']}")
                        print(f"Date: {format_timestamp(msg['gmtCreate'])}")
                        print(f"Subject: {msg['msgTitle']}")
                        print(msg['msgContent'])
                        print("--------------------")
                        print()
            except json.JSONDecodeError:
                print(body_str)
        else:
            # Pretty-print JSON response
            try:
                resp_data = json.loads(body_str)
                print(json.dumps(resp_data, indent=2))
            except json.JSONDecodeError:
                print(body_str)

        # After login, extract accessToken
        if action == actions[0] and status == 200:
            try:
                resp_data = json.loads(body_str)
                access_token = resp_data['data']['accessToken']
            except Exception as e:
                print('Failed to extract accessToken:', e, file=sys.stderr)
                sys.exit(4)

if __name__ == '__main__':
    main()
