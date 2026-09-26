# BitTorrent Fixtures

`payload.bin` is 200 repetitions of `ariax BitTorrent fixture` followed by LF.
The v1, v2 and hybrid torrents describe this one file with 16 KiB pieces.
The v1 piece is its SHA-1 digest; the single v2 block root is its SHA-256 digest.
All bencoded dictionaries use canonical byte ordering. Fixtures have no
trackers, web seeds, credentials or external network dependencies.

The multi-piece v2 and hybrid fixtures describe 70,000 bytes of the same repeated
line, with five 16 KiB pieces and the corresponding BEP 52 piece layer. The
native tests reconstruct those payloads and restart from the info dictionary
alone plus checkpoint data. `python3 generate.py --check` verifies all fixture
hashes and byte encodings; omit `--check` to regenerate them.

`selection-v1.torrent` contains two 16 KiB files under `collection`, filled with
`a` and `b` respectively. `A?.bin` and `A*.bin` collide after portable path
sanitization. Their pieces do not overlap, so selecting the second file must
preserve its deterministic renamed path without creating the first file.

`unsafe-path-v1.torrent` and `symlink-v1.torrent` are rejection inputs. A minimal
metadata peer serves their unchanged info dictionaries to magnets so the engine
must reject traversal and symlink entries before creating any payload files.
