#!/usr/bin/env python3
"""Generate deterministic, offline v1/v2/hybrid transfer fixtures."""
import argparse
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LINE = b"ariax BitTorrent fixture\n"
PIECE = 16384


def encode(value):
    if isinstance(value, dict):
        return b"d" + b"".join(encode(k) + encode(v) for k, v in sorted(value.items())) + b"e"
    if isinstance(value, int):
        return b"i" + str(value).encode() + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(encode(item) for item in value) + b"e"
    return str(len(value)).encode() + b":" + value


def torrent(payload, version):
    blocks = [payload[start:start + PIECE] for start in range(0, len(payload), PIECE)]
    info = {b"name": b"payload.bin", b"piece length": PIECE}
    result = {b"info": info}
    if version != "v2":
        info.update({b"length": len(payload),
                     b"pieces": b"".join(hashlib.sha1(block).digest() for block in blocks)})
    if version != "v1":
        hashes = [hashlib.sha256(block).digest() for block in blocks]
        level = hashes + [bytes(32)] * ((1 << (len(hashes) - 1).bit_length()) - len(hashes))
        while len(level) > 1:
            level = [hashlib.sha256(level[i] + level[i + 1]).digest() for i in range(0, len(level), 2)]
        root = level[0]
        info.update({b"meta version": 2,
                     b"file tree": {b"payload.bin": {b"": {b"length": len(payload), b"pieces root": root}}}})
        if len(hashes) > 1:
            result[b"piece layers"] = {root: b"".join(hashes)}
    return encode(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    payload = LINE * 200
    outputs = {"payload.bin": payload}
    for version in ("v1", "v2", "hybrid"):
        outputs[version + ".torrent"] = torrent(payload, version)
    large = (LINE * (70000 // len(LINE) + 1))[:70000]
    for version in ("v2", "hybrid"):
        outputs["multi-piece-" + version + ".torrent"] = torrent(large, version)
    outputs["selection-v1.torrent"] = encode({b"info": {
        b"name": b"collection", b"piece length": PIECE,
        b"files": [{b"length": PIECE, b"path": [name]} for name in (b"A?.bin", b"A*.bin")],
        b"pieces": b"".join(hashlib.sha1(byte * PIECE).digest() for byte in (b"a", b"b")),
    }})
    outputs["unsafe-path-v1.torrent"] = encode({b"info": {
        b"name": b"../escape", b"length": 1, b"piece length": PIECE,
        b"pieces": hashlib.sha1(b"x").digest(),
    }})
    outputs["symlink-v1.torrent"] = encode({b"info": {
        b"name": b"bundle", b"piece length": PIECE,
        b"pieces": hashlib.sha1(b"x").digest(),
        b"files": [{b"length": 1, b"path": [b"payload.bin"]},
                   {b"length": 0, b"path": [b"link"], b"attr": b"l",
                    b"symlink path": [b"payload.bin"]}],
    }})
    for name, content in outputs.items():
        path = ROOT / name
        if args.check:
            if path.read_bytes() != content:
                raise RuntimeError("fixture mismatch: " + name)
        else:
            path.write_bytes(content)
    print("Verified fixtures." if args.check else "Generated fixtures.")


if __name__ == "__main__":
    main()
