#!/usr/bin/env python3
"""
Yarbo DC NMEA sniffer (Unicore UM960 stream on port 3000)

- Connects to a TCP host/port (default 3000)
- Immediately sends ephemeral NMEA enables on the same socket (default: GSV/GSA/GST/RMC at 1 Hz)
- Filters ASCII NMEA sentences from a mixed binary stream
- Verifies checksums
- Parses:
  * GGA: position, fix, sats, HDOP, altitude (with UM960 fix-quality text)
  * GSA: DOPs (PDOP/HDOP/VDOP) + "used" satellites
  * GSV: per-satellite C/N0, elevation, azimuth (summaries by constellation)
  * GST: pseudorange noise (if present)
- Optional logging of raw binary and GGA to CSV

Ephemeral commands do NOT persist; they apply only while this connection is open.
"""

import socket
import argparse
import time
import csv
import re
from datetime import datetime, timezone

# Human-readable fix quality (UM960 mapping)
FIX_QUALITY = {
    0: "Invalid",
    1: "Autonomous GNSS",
    2: "DGPS",
    3: "PPS",
    4: "RTK Fixed",
    5: "RTK Float",
    6: "Estimated/DR",
    7: "RTK Fixed (Heading/Moving Base)",
    8: "Simulation",
}

# Map NMEA talker to constellation label
TALKER_NAME = {
    "GP": "GPS",
    "GL": "GLONASS",
    "GA": "Galileo",
    "BD": "BeiDou",
    "GB": "BeiDou",   # some devices use GB
    "GN": "Mixed",    # combined
    "QZ": "QZSS",
    "IR": "IRNSS",
}

ASCII_LINE = re.compile(rb'\$[A-Z]{2,3}[A-Z]{2,3},.*\*[0-9A-F]{2}\r?\n')

def nmea_checksum(sentence: str) -> int:
    s = sentence.strip()
    if not s.startswith("$") or "*" not in s:
        return -1
    body = s[1:s.index("*")]
    csum = 0
    for ch in body:
        csum ^= ord(ch)
    return csum

def nmea_verify(sentence: str) -> bool:
    if "*" not in sentence:
        return False
    try:
        provided = int(sentence.strip().split("*")[-1], 16)
    except ValueError:
        return False
    return nmea_checksum(sentence) == provided

def ddmm_to_decimal(ddmm: str, hemi: str):
    if not ddmm:
        return None
    try:
        if "." in ddmm:
            whole, frac = ddmm.split(".", 1)
            deg = int(whole[:-2])
            minutes = float(whole[-2:] + "." + frac)
        else:
            deg = int(ddmm[:-2])
            minutes = float(ddmm[-2:])
        val = deg + minutes / 60.0
        if hemi in ("S", "W"):
            val = -val
        return val
    except Exception:
        return None

def parse_gga(fields: list[str]) -> dict:
    # $..GGA,time,lat,N,lon,E,fix,sats,HDOP,alt,M,geoid,M,age,ref*CS
    try:
        hhmmss = fields[1]
        utc_time = None
        if hhmmss and len(hhmmss) >= 6:
            hh = int(hhmmss[0:2]); mm = int(hhmmss[2:4]); ssf = float(hhmmss[4:])
            sec = int(ssf); msec = int(round((ssf - sec) * 1000))
            now = datetime.now(timezone.utc)
            utc_time = now.replace(hour=hh, minute=mm, second=sec, microsecond=msec*1000)
        lat = ddmm_to_decimal(fields[2], fields[3])
        lon = ddmm_to_decimal(fields[4], fields[5])
        fixq = int(fields[6]) if fields[6] else 0
        sats = int(fields[7]) if fields[7] else 0
        hdop = float(fields[8]) if fields[8] else None
        alt = float(fields[9]) if fields[9] else None
        geoid = float(fields[11]) if len(fields) > 11 and fields[11] else None
        return {"utc_time": utc_time, "lat": lat, "lon": lon, "fix": fixq, "sats": sats,
                "hdop": hdop, "alt_m": alt, "geoid_m": geoid}
    except Exception:
        return {}

def parse_gsa(fields: list[str]) -> dict:
    # $..GSA,mode,fix_type,s1,s2,...,s12,PDOP,HDOP,VDOP*CS
    try:
        used = []
        for i in range(3, 15):
            prn = fields[i]
            if prn:
                try:
                    used.append(int(prn))
                except ValueError:
                    pass
        pdop = float(fields[15]) if len(fields) > 15 and fields[15] else None
        hdop = float(fields[16]) if len(fields) > 16 and fields[16] else None
        vdop = float(fields[17].split("*")[0]) if len(fields) > 17 and fields[17] else None
        return {"used": used, "pdop": pdop, "hdop": hdop, "vdop": vdop}
    except Exception:
        return {}

def parse_gsv(fields: list[str]):
    # $..GSV,total_sent,sent_num,total_sats, (PRN,elev,az,cn0) x up to 4
    sats = []
    try:
        total_sent = int(fields[1]) if fields[1] else 0
        sent_num = int(fields[2]) if fields[2] else 0
        total_sats = int(fields[3]) if fields[3] else 0
        for i in range(4, len(fields)-3, 4):
            prn = fields[i]
            elev = fields[i+1]
            az = fields[i+2]
            cn0 = fields[i+3].split("*")[0]
            if prn:
                try:
                    sats.append({
                        "prn": int(prn),
                        "elev": int(elev) if elev else None,
                        "az": int(az) if az else None,
                        "cn0": int(cn0) if cn0 else None
                    })
                except ValueError:
                    pass
        return total_sent, sent_num, total_sats, sats
    except Exception:
        return 0, 0, 0, []

def parse_gst(fields: list[str]) -> dict:
    # $..GST,utc, rms,maj, min, orient, lat_sd, lon_sd, alt_sd*CS (varies by vendor)
    try:
        lat_sd = float(fields[-3]) if len(fields) >= 3 and fields[-3] else None
        lon_sd = float(fields[-2]) if len(fields) >= 2 and fields[-2] else None
        alt_sd = float(fields[-1].split("*")[0]) if fields[-1] else None
        return {"lat_sd": lat_sd, "lon_sd": lon_sd, "alt_sd": alt_sd}
    except Exception:
        return {}

def is_printable_ascii_line(line: bytes) -> bool:
    if not line.startswith(b"$") or b"*" not in line:
        return False
    try:
        line.decode("ascii")
        return True
    except UnicodeDecodeError:
        return False

def send_ephemeral_enables(sock, cmds, verbose: bool):
    """Send one-shot NMEA enable commands over the current TCP socket (CRLF)."""
    for cmd in cmds:
        line = (cmd + "\r\n").encode("ascii", errors="ignore")
        try:
            sock.sendall(line)
            if verbose:
                print(f"[EPHEMERAL->] {cmd}")
            # tiny pause helps some receivers parse back-to-back lines
            time.sleep(0.05)
        except Exception as e:
            print(f"[WARN] Failed sending '{cmd}': {e}")

def run(host: str, port: int, log_path: str, csv_path: str, min_cn0: int,
        show_all_nmea: bool, use_ephemeral: bool, ephemeral_cmds: list[str], verbose_cmds: bool):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    sock.connect((host, port))
    sock.settimeout(1.0)

    print(f"Connected to {host}:{port}. Press Ctrl+C to stop.")

    # Send ephemeral enables on this socket (session-only; non-persistent)
    if use_ephemeral and ephemeral_cmds:
        send_ephemeral_enables(sock, ephemeral_cmds, verbose_cmds)

    raw_log = open(log_path, "ab") if log_path else None
    csv_file = open(csv_path, "a", newline="") if csv_path else None
    csv_writer = None
    if csv_file:
        csv_writer = csv.writer(csv_file)
        if csv_file.tell() == 0:
            csv_writer.writerow(["utc_iso", "lat", "lon", "fix_text", "fix_code", "sats", "HDOP", "alt_m"])

    buffer = b""
    last_gga_print = 0.0

    # GSV grouping per talker & epoch-ish second
    gsv_groups = {}  # key=(talker,total_sats,epoch_sec) -> {"seen": set(), "sats": []}

    try:
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    time.sleep(0.05)
                    continue
                if raw_log:
                    raw_log.write(chunk)
                buffer += chunk

                while True:
                    start = buffer.find(b"$")
                    if start == -1:
                        buffer = buffer[-4096:]
                        break
                    end_r = buffer.find(b"\r", start)
                    end_n = buffer.find(b"\n", start)
                    ends = [x for x in (end_r, end_n) if x != -1]
                    if not ends:
                        buffer = buffer[start:]
                        break
                    end = min(ends)
                    line = buffer[start:end+1]
                    buffer = buffer[end+1:]

                    if not is_printable_ascii_line(line):
                        continue
                    try:
                        text = line.decode("ascii").strip()
                    except UnicodeDecodeError:
                        continue

                    if not nmea_verify(text):
                        continue

                    if show_all_nmea:
                        print(text)

                    sentence_type = text[1:6]  # e.g., 'GNGGA','GPGSV'
                    talker = text[1:3]
                    fields = text.split(",")

                    # --- GGA ---
                    if sentence_type.endswith("GGA"):
                        g = parse_gga(fields)
                        if g and (time.time() - last_gga_print) > 0.5:
                            last_gga_print = time.time()
                            iso = g["utc_time"].isoformat() if g.get("utc_time") else "unknown"
                            fix_text = FIX_QUALITY.get(g["fix"], f"Unknown({g['fix']})")
                            print(f"[GGA] {iso} lat={g['lat']:.8f} lon={g['lon']:.8f} "
                                  f"fix={fix_text} (code={g['fix']}) sats={g['sats']} HDOP={g['hdop']} alt={g['alt_m']}m")
                            if csv_writer:
                                csv_writer.writerow([iso, g["lat"], g["lon"], fix_text, g["fix"], g["sats"], g["hdop"], g["alt_m"]])

                    # --- GSA ---
                    elif sentence_type.endswith("GSA"):
                        gsa = parse_gsa(fields)
                        if gsa:
                            used_ct = len(gsa["used"]) if gsa.get("used") else 0
                            print(f"[GSA] used={used_ct} PDOP={gsa.get('pdop')} "
                                  f"HDOP={gsa.get('hdop')} VDOP={gsa.get('vdop')}")

                    # --- GST (optional) ---
                    elif sentence_type.endswith("GST"):
                        gst = parse_gst(fields)
                        if gst and any(v is not None for v in gst.values()):
                            print(f"[GST] σ_lat={gst.get('lat_sd')} σ_lon={gst.get('lon_sd')} σ_alt={gst.get('alt_sd')} (m)")

                    # --- GSV ---
                    elif sentence_type.endswith("GSV"):
                        total, num, total_sats, sats = parse_gsv(fields)
                        if total == 0:
                            continue
                        # Group by talker & epoch-ish second to assemble multi-part GSVs
                        key = (talker, total_sats, int(time.time()))
                        group = gsv_groups.get(key, {"seen": set(), "sats": []})
                        group["seen"].add(num)
                        group["sats"].extend(sats)
                        gsv_groups[key] = group

                        # When all sentences for this GSV group have been seen, summarize
                        if len(group["seen"]) >= total:
                            label = TALKER_NAME.get(talker, talker)
                            cn0_vals = [s["cn0"] for s in group["sats"] if s.get("cn0") is not None]
                            mean_cn0 = (sum(cn0_vals) / len(cn0_vals)) if cn0_vals else None
                            max_cn0 = max(cn0_vals) if cn0_vals else None
                            strong = [s for s in group["sats"] if s.get("cn0") is not None and s["cn0"] >= min_cn0]

                            print(f"[{label} GSV] in_view={total_sats} tracked={len(group['sats'])} "
                                  f"strong(≥{min_cn0})={len(strong)}"
                                  + (f" mean C/N0={mean_cn0:.1f}" if mean_cn0 is not None else "")
                                  + (f" max C/N0={max_cn0}" if max_cn0 is not None else ""))

                            if strong:
                                strong_sorted = sorted(strong, key=lambda s: s["cn0"], reverse=True)
                                pairs = " ".join(f"{s['prn']}:{s['cn0']}" for s in strong_sorted[:12])
                                print(f"   strongest: {pairs}")

                            # cleanup stale groups
                            for k in list(gsv_groups.keys()):
                                if k[2] < key[2] - 2:
                                    gsv_groups.pop(k, None)

            except socket.timeout:
                continue
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        if raw_log:
            raw_log.close()
        if csv_file:
            csv_file.close()
        try:
            sock.close()
        except Exception:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Yarbo DC NMEA sniffer (GGA/GSA/GSV/GST parser) with ephemeral enables")
    parser.add_argument("--host", required=True, help="Device IP (e.g., 192.168.1.2)")
    parser.add_argument("--port", type=int, default=3000, help="TCP port (default 3000)")
    parser.add_argument("--log", help="Optional raw binary log path (appends)")
    parser.add_argument("--csv", help="Optional CSV to append GGA rows")
    parser.add_argument("--min-cn0", type=int, default=30, help="Threshold for 'strong' satellites in dB-Hz")
    parser.add_argument("--all", action="store_true", help="Print every valid NMEA line")
    parser.add_argument("--no-ephemeral", action="store_true", help="Do not send ephemeral NMEA enables on connect")
    parser.add_argument("--ephemeral-cmds", default="GPGSV 1;GPGSA 1;GPGST 1;GPRMC 1",
                        help="Semicolon-separated list of commands to send after connect (CRLF appended)")
    parser.add_argument("--verbose-cmds", action="store_true", help="Print commands as they are sent")
    args = parser.parse_args()

    use_ephemeral = not args.no_ephemeral
    cmds = [c.strip() for c in args.ephemeral_cmds.split(";") if c.strip()]

    run(args.host, args.port, args.log, args.csv, args.min_cn0, args.all, use_ephemeral, cmds, args.verbose_cmds)

