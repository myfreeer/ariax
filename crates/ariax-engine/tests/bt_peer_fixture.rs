#![forbid(unsafe_code)]

// The custom benchmark has no test harness; execute these regressions here.
mod peer_wire {
    include!("../benches/rpc_active_profile/peer_wire.rs");

    #[cfg(test)]
    mod tests {
        use super::*;

        fn message(kind: u8, piece: usize, offset: usize, size: usize) -> Vec<u8> {
            let mut bytes = 13_u32.to_be_bytes().to_vec();
            bytes.push(kind);
            for number in [piece, offset, size] {
                bytes.extend_from_slice(&(number as u32).to_be_bytes());
            }
            bytes
        }

        #[test]
        fn fragmented_frames_survive_every_read_boundary_and_cancellation() {
            let request = message(6, 7, BLOCK_BYTES, BLOCK_BYTES);
            for boundary in 0..=request.len() {
                let mut wire = PeerWire::new(7);
                wire.push(&[0, 0, 0, 0]).unwrap(); // keepalive
                wire.push(&request[..boundary]).unwrap();
                wire.push(&request[boundary..]).unwrap();
                assert_eq!(
                    wire.next_request(),
                    Some(BlockRequest {
                        piece: 7,
                        offset: BLOCK_BYTES as u32
                    })
                );
                assert_eq!(wire.next_request(), None);
            }
            let mut wire = PeerWire::new(7);
            wire.push(&request).unwrap();
            wire.push(&request).unwrap(); // duplicate requests do not grow the queue
            wire.push(&message(8, 7, BLOCK_BYTES, BLOCK_BYTES)).unwrap();
            assert_eq!(wire.next_request(), None);
        }

        #[test]
        fn malformed_lengths_unadvertised_pieces_and_unbounded_queues_fail() {
            for bytes in [
                (MAX_MESSAGE as u32 + 1).to_be_bytes().to_vec(),
                vec![0, 0, 0, 1, 6],
                message(6, 1, 0, BLOCK_BYTES),
                message(6, PIECES, 0, BLOCK_BYTES),
                message(6, 0, PIECE_BYTES, BLOCK_BYTES),
                message(6, 0, 1, BLOCK_BYTES),
                message(6, 0, 0, BLOCK_BYTES + 1),
                vec![0; MAX_MESSAGE + 8197],
            ] {
                assert!(PeerWire::new(0).push(&bytes).is_err());
            }
            let mut wire = PeerWire::new(0);
            // Each advertised piece has 64 blocks, for exactly 512 bounded slots.
            for piece in (0..PIECES).step_by(PEERS) {
                for offset in (0..PIECE_BYTES).step_by(BLOCK_BYTES) {
                    wire.push(&message(6, piece, offset, BLOCK_BYTES)).unwrap();
                }
            }
            assert_eq!(wire.requests.len(), MAX_REQUESTS);
            // Even a full queue can process cancellation and a replacement request.
            wire.push(&message(8, 0, 0, BLOCK_BYTES)).unwrap();
            wire.push(&message(6, 0, 0, BLOCK_BYTES)).unwrap();
            assert_eq!(wire.requests.len(), MAX_REQUESTS);
        }
    }
}
