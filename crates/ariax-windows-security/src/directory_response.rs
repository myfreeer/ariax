//! Bounds-check variable NT directory records before interpreting their names.
#![forbid(unsafe_code)]

use std::io;

pub(super) fn visit_names(
    data: &[u8],
    mut visit: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<()> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid NT directory response");
    let mut offset = 0_usize;
    loop {
        let header = data
            .get(offset..offset.checked_add(12).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        let next = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        if length == 0 || !length.is_multiple_of(2) {
            return Err(invalid());
        }
        let start = offset + 12;
        let end = start.checked_add(length).ok_or_else(invalid)?;
        let name = data.get(start..end).ok_or_else(invalid)?;
        if next != 0 && (!next.is_multiple_of(4) || next < 12 + length) {
            return Err(invalid());
        }
        visit(name)?;
        if next == 0 {
            return Ok(());
        }
        offset = offset.checked_add(next).ok_or_else(invalid)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(next: u32, name: &[u16]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&next.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&(name.len() as u32 * 2).to_le_bytes());
        for unit in name {
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn names_decode_without_padding_reads_and_every_truncation_rejects() {
        let one = record(0, &[b'a' as u16]);
        let mut count = 0;
        visit_names(&one, |name| {
            assert_eq!(name, [97, 0]);
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        for end in 0..one.len() {
            assert!(visit_names(&one[..end], |_| Ok(())).is_err());
        }
        let mut multiple = record(16, &[97]);
        multiple.extend_from_slice(&[0, 0]);
        multiple.extend_from_slice(&record(0, &[98, 99]));
        count = 0;
        visit_names(&multiple, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn malformed_offsets_lengths_and_visitor_rejections_fail_closed() {
        for next in [1, 4, 12, 15, 16, u32::MAX] {
            assert!(visit_names(&record(next, &[97]), |_| Ok(())).is_err());
        }
        for length in [0_u32, 1, 3, u32::MAX] {
            let mut data = record(0, &[97]);
            data[8..12].copy_from_slice(&length.to_le_bytes());
            assert!(visit_names(&data, |_| Ok(())).is_err());
        }
        assert!(
            visit_names(&record(0, &[97]), |_| Err(io::Error::other(
                "budget exhausted"
            )))
            .is_err()
        );
    }
}
