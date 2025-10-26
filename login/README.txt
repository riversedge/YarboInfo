Included files (extracted from dart.zip):
- rsa_encrypt.dart      (login encryption glue code)
- encrypt.dart          (package:encrypt implementation used)
- oaep.dart, rsa.dart   (pointycastle asymmetric helpers)

Findings summary:
- The client loads assets/rsa_key/rsa_public_key.pem (base64-only content) and wraps it with:
  "-----BEGIN PUBLIC KEY-----\n" + content + "\n-----END PUBLIC KEY-----\n"
- The code uses package:encrypt RSAKeyParser::parse and Encrypter::encrypt which (per implementation)
  constructs a PointyCastle PKCS1Encoding (PKCS#1 v1.5) RSA encryptor by default.
- Therefore the client encrypts the plaintext (password) using RSA/PKCS1 v1.5 and Base64-encodes the result.
- The asset public key is the file assets/rsa_key/rsa_public_key.pem referenced in rsa_encrypt.dart.
- If your scripts didn't match the server, check that you used the same public key file (that one), PKCS#1 v1.5 padding,
  and that you base64-encode the raw RSA ciphertext (no extra headers). Also ensure the plaintext string matches exactly
  (no extra whitespace, unicode normalization, or character encoding differences).

