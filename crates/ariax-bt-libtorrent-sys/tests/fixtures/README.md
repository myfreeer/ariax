# BitTorrent Fixtures

`payload.bin` is 200 repetitions of `ariax BitTorrent fixture` followed by LF.
The v1, v2 and hybrid torrents describe this one file with 16 KiB pieces.
The v1 piece is its SHA-1 digest; the single v2 block root is its SHA-256 digest.
All bencoded dictionaries use canonical byte ordering. Fixtures have no
trackers, web seeds, credentials or external network dependencies.
