// Bounded framing for the benchmark's deliberately small BitTorrent peer.

use std::collections::VecDeque;

pub const PEERS: usize = 1_000;
pub const PIECES: usize = PEERS * 8;
pub const PIECE_BYTES: usize = 1024 * 1024;
pub const BLOCK_BYTES: usize = 16 * 1024;
const MAX_MESSAGE: usize = 64 * 1024;
const MAX_REQUESTS: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRequest {
    pub piece: u32,
    pub offset: u32,
}

pub struct PeerWire {
    peer: usize,
    bytes: Vec<u8>,
    requests: VecDeque<BlockRequest>,
}

impl PeerWire {
    pub fn new(peer: usize) -> Self {
        assert!(peer < PEERS);
        Self {
            peer,
            bytes: Vec::new(),
            requests: VecDeque::new(),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<(), &'static str> {
        // The socket loop reads at most 8 KiB at a time. Keep incomplete frames
        // across cancellation of read(), and reject lengths before allocating.
        if self.bytes.len().saturating_add(bytes.len()) > MAX_MESSAGE + 4 + 8192 {
            return Err("peer receive buffer exceeded its limit");
        }
        self.bytes.extend_from_slice(bytes);
        let mut consumed = 0;
        while self.bytes.len() - consumed >= 4 {
            let frame = &self.bytes[consumed..];
            let length = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
            if length > MAX_MESSAGE {
                return Err("peer message exceeded its limit");
            }
            if frame.len() < 4 + length {
                break;
            }
            if length > 0 && matches!(frame[4], 6 | 8) {
                if length != 13 {
                    return Err("invalid peer request length");
                }
                let request = BlockRequest {
                    piece: u32::from_be_bytes(frame[5..9].try_into().unwrap()),
                    offset: u32::from_be_bytes(frame[9..13].try_into().unwrap()),
                };
                let size = u32::from_be_bytes(frame[13..17].try_into().unwrap()) as usize;
                if request.piece as usize >= PIECES
                    || request.piece as usize % PEERS != self.peer
                    || size != BLOCK_BYTES
                    || !(request.offset as usize).is_multiple_of(BLOCK_BYTES)
                    || request.offset as usize > PIECE_BYTES - BLOCK_BYTES
                {
                    return Err("peer requested an unavailable block");
                }
                if frame[4] == 8 {
                    self.requests.retain(|queued| *queued != request);
                } else if !self.requests.contains(&request) {
                    if self.requests.len() == MAX_REQUESTS {
                        return Err("peer request queue exceeded its limit");
                    }
                    self.requests.push_back(request);
                }
            }
            consumed += 4 + length;
        }
        self.bytes.drain(..consumed);
        Ok(())
    }

    pub fn next_request(&mut self) -> Option<BlockRequest> {
        self.requests.pop_front()
    }
}
