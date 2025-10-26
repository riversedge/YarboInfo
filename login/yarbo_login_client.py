#!/usr/bin/env python3
"""yarbo_login_client.py

Encrypt the provided password using the app's public key (assets/rsa_key/rsa_public_key.pem
format allowed: either full PEM with BEGIN/END lines or base64-only body), using RSA PKCS#1 v1.5 padding,
base64-encode the ciphertext, and POST as {"username":..., "password": "<base64>"} to the login endpoint.

Requirements (on the machine where you run this):
- openssl (command-line) available in PATH
- Python 3.7+ (uses only stdlib)
- Network access to the login URL (if not --dry-run)

Example usage:
  python3 yarbo_login_client.py \
    --pubkey ./rsa_public_key.pem \
    --username wewright@wesleyacoustics.com \
    --password 'MySecretPassword' \
    --url 'https://4zx17x5q7l.execute-api.us-east-1.amazonaws.com/Stage/yarbo/robot-service/robot/commonUser/login' \
    --show-cipher

Notes:
- The script will wrap the provided public key with PEM headers if they are missing.
- It performs RSA encryption with PKCS#1 v1.5 (openssl pkeyutl with -pkeyopt rsa_padding_mode:pkcs1).
- Use --dry-run to only print the base64 ciphertext without posting.
"""

import argparse, subprocess, tempfile, os, sys, base64, json, urllib.request as _urllib_request, shutil, stat

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
        # write in 64-char chunks
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
        cmd = ['openssl','pkeyutl','-encrypt','-pubin','-inkey',wrapped,'-in',inname,'-out',outname,'-pkeyopt','rsa_padding_mode:pkcs1']
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            raise RuntimeError('openssl failed: ' + proc.stderr.decode('utf-8', errors='replace'))
        data = open(outname,'rb').read()
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

def post_login(url, username, b64cipher, extra_headers=None):
    payload = json.dumps({'username': username, 'password': b64cipher}).encode('utf-8')
    headers = {'Content-Type':'application/json', 'User-Agent':'Dart/3.4 (dart:io)'}
    if extra_headers:
        headers.update(extra_headers)
    req = _urllib_request.Request(url, data=payload, headers=headers, method='POST')
    with _urllib_request.urlopen(req, timeout=30) as r:
        status = r.status
        body = r.read().decode('utf-8', errors='replace')
        response_headers = dict(r.getheaders())
    return status, body, response_headers

def main():
    p = argparse.ArgumentParser(description='Encrypt password with RSA PKCS#1 v1.5 and post login JSON.')
    p.add_argument('--pubkey', required=True, default='rsa_public_key.pem', help='Path to rsa_public_key.pem (PEM or base64 body)')
    p.add_argument('--username', required=True)
    p.add_argument('--password', required=True)
    p.add_argument('--url', required=True, default='https://4zx17x5q7l.execute-api.us-east-1.amazonaws.com/Stage/yarbo/robot-service/robot/commonUser/login', help='Login endpoint URL')
    p.add_argument('--dry-run', action='store_true', help='Do not POST; just print the base64 ciphertext')
    p.add_argument('--show-cipher', action='store_true', help='Print base64 ciphertext')
    p.add_argument('--header', action='append', help='Extra HTTP header: Name:Value (can repeat)')
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

    headers = {}
    if args.header:
        for h in args.header:
            if ':' in h:
                k,v = h.split(':',1)
                headers[k.strip()] = v.strip()

    try:
        status, body, resp_headers = post_login(args.url, args.username, b64, extra_headers=headers)
    except Exception as e:
        print('HTTP request failed:', e, file=sys.stderr)
        sys.exit(3)

    print('HTTP status:', status)
    print('Response headers:', resp_headers)
    print('Response body:')
    print(body)

if __name__ == '__main__':
    main()
