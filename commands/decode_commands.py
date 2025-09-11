#!/usr/bin/env python3
import argparse
import binascii
import json
import re
import sys
import zlib

BLOCK_RE = re.compile(r"^(Topic|Message|Note)\s*:\s*(.*)$", re.IGNORECASE)

def parse_entries(lines):
    """
    Parse blocks of:
      Topic: <topic>
      Message: <hex>
      [Note: <text>]   # optional, may appear or not

    Returns a list of dicts: [{"topic":..., "message":..., "note":...}, ...]
    """
    entries = []
    cur = {}
    for raw in lines:
        line = raw.strip()
        if not line:
            # Blank line ends a block if we have a message already
            if "topic" in cur and "message" in cur:
                entries.append(cur)
                cur = {}
            continue
        m = BLOCK_RE.match(line)
        if not m:
            # Non-matching lines are ignored
            continue
        key, val = m.group(1).lower(), m.group(2).strip()
        if key == "topic":
            # If a new topic appears and we already had one complete block, push it
            if "topic" in cur and "message" in cur:
                entries.append(cur)
                cur = {}
            cur["topic"] = val
        elif key == "message":
            cur["message"] = val
        elif key == "note":
            cur["note"] = val

    # Flush last block
    if "topic" in cur and "message" in cur:
        entries.append(cur)
    return entries

def _looks_like_json_text(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    if s[0] in '{["' and s[-1] in '}]"':
        try:
            json.loads(s)
            return True
        except Exception:
            return False
    return False

def decode_payload(hex_str):
    """
    Given a hex string, try to decode it in a few ways:
    - raw hex (fallback)
    - bytes -> utf-8 text (if mostly printable)
    - json (if parseable)
    - zlib-compressed bytes (try standard & nowrap)
      If zlib decode yields text/JSON, report appropriately.

    Returns (kind, text) where kind in {"hex","text","json","zlib-bytes"}.
    """
    hex_str = hex_str.strip()
    # Remove spaces if user pasted spaced hex
    hs = re.sub(r"\s+", "", hex_str)
    try:
        raw = binascii.unhexlify(hs)
    except binascii.Error:
        # Not valid hex; just return as-is
        return "hex", hex_str

    # Try zlib first (0x78 is common zlib header), else try anyway
    def try_zlib(b):
        try:
            return zlib.decompress(b)
        except Exception:
            try:
                return zlib.decompress(b, -zlib.MAX_WBITS)  # nowrap
            except Exception:
                return None

    z = None
    if raw and raw[0] == 0x78:
        z = try_zlib(raw)
    if z is None:
        z = try_zlib(raw)

    if z is not None:
        # If it looks like text/JSON, return that; else return decompressed hex
        try:
            txt = z.decode("utf-8", errors="replace")
            if _looks_like_json_text(txt):
                return "json", txt
            # If mostly printable, call it text
            printable_ratio = sum(32 <= c <= 126 or c in (9, 10, 13) for c in z) / max(1, len(z))
            if printable_ratio > 0.85:
                return "text", txt
            return "zlib-bytes", binascii.hexlify(z).decode()
        except Exception:
            return "zlib-bytes", binascii.hexlify(z).decode()

    # Not zlib; try direct text/JSON
    try:
        txt = raw.decode("utf-8")
        if _looks_like_json_text(txt):
            return "json", txt
        printable_ratio = sum(32 <= c <= 126 or c in (9, 10, 13) for c in raw) / max(1, len(raw))
        if printable_ratio > 0.85:
            return "text", txt
    except Exception:
        pass

    return "hex", hex_str

def main():
    ap = argparse.ArgumentParser(description="Decode Topic/Message[/Note] blocks with hex payloads.")
    ap.add_argument("file", nargs="?", help="Input file (default: stdin)")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON bodies")
    args = ap.parse_args()

    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()

    entries = parse_entries(lines)

    for e in entries:
        topic = e["topic"]
        hex_str = e["message"]
        note = e.get("note")

        kind, text = decode_payload(hex_str)

        print(f"Topic:   {topic}")
        print(f"Message: {hex_str}")
        if note is not None:
            print(f"Note:    {note}")
        if kind == "json" and args.pretty:
            try:
                print("JSON:    " + json.dumps(json.loads(text), indent=2, ensure_ascii=False))
            except Exception:
                print("JSON:    " + text)
        else:
            label = "Decoded" if kind in ("json", "text") else ("DecompressedHex" if kind == "zlib-bytes" else "Hex")
            print(f"{label}: {text}")
        print("-" * 60)

if __name__ == "__main__":
    main()

