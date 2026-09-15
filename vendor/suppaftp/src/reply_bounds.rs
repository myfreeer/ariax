//! Shared bounded RFC 959 control framing. No server text is logged.
use crate::{FtpError, Status};
use crate::types::Response;

pub const ARIAX_BOUNDED_PROTOCOL_REVISION: u32 = 1;
pub const MAX_CONTROL_LINE: usize = 64 * 1024;
pub const MAX_CONTROL_REPLY: usize = 1024 * 1024;
pub const MAX_CONTROL_LINES: usize = 4096;

#[derive(Default)]
pub(crate) struct ReplyBuilder {
    code: Option<[u8; 3]>,
    body: Vec<u8>,
    lines: usize,
}

impl ReplyBuilder {
    pub(crate) fn push(&mut self, line: &[u8]) -> Result<Option<Response>, FtpError> {
        if line.len() > MAX_CONTROL_LINE
            || self.lines >= MAX_CONTROL_LINES
            || line.len() > MAX_CONTROL_REPLY.saturating_sub(self.body.len())
        {
            return Err(FtpError::ControlLimit);
        }
        if !line.ends_with(b"\r\n") || line[..line.len() - 2].contains(&b'\r') {
            return Err(FtpError::BadResponse);
        }
        let header = line.len() >= 6 && line[..3].iter().all(u8::is_ascii_digit);
        let terminal = if let Some(code) = self.code {
            if header && line[3] == b' ' {
                if line[..3] != code { return Err(FtpError::BadResponse); }
                true
            } else {
                false
            }
        } else {
            if !header || !matches!(line[3], b' ' | b'-') {
                return Err(FtpError::BadResponse);
            }
            self.code = Some([line[0], line[1], line[2]]);
            line[3] == b' '
        };
        self.lines += 1;
        self.body.extend_from_slice(line);
        if terminal {
            let code = self.code.expect("validated opener");
            let number = u32::from(code[0] - b'0') * 100
                + u32::from(code[1] - b'0') * 10 + u32::from(code[2] - b'0');
            trace!("FTP reply status={} bytes={} lines={}", number, self.body.len(), self.lines);
            Ok(Some(Response::new(Status::from(number), std::mem::take(&mut self.body))))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_require_exact_terminators_and_bounded_aggregate() {
        let mut reply = ReplyBuilder::default();
        assert!(reply.push(b"211-features\r\n").unwrap().is_none());
        assert!(reply.push(b" SIZE\r\n").unwrap().is_none());
        assert_eq!(reply.push(b"211 done\r\n").unwrap().unwrap().status, Status::System);
        let mut reply = ReplyBuilder::default();
        reply.push(b"220-greeting\r\n").unwrap();
        assert!(matches!(reply.push(b"221 wrong\r\n"), Err(FtpError::BadResponse)));
        assert!(ReplyBuilder::default().push(b"220 LF only\n").is_err());
        let mut reply = ReplyBuilder::default();
        reply.push(b"220-long\r\n").unwrap();
        let line = [vec![b' '; MAX_CONTROL_LINE - 2], b"\r\n".to_vec()].concat();
        for _ in 0..15 { reply.push(&line).unwrap(); }
        assert!(matches!(reply.push(&line), Err(FtpError::ControlLimit)));
    }

    #[test]
    fn line_count_and_line_bytes_are_hard_bounds() {
        let mut reply = ReplyBuilder::default();
        reply.push(b"220-many\r\n").unwrap();
        for _ in 1..MAX_CONTROL_LINES { reply.push(b" x\r\n").unwrap(); }
        assert!(matches!(reply.push(b"220 done\r\n"), Err(FtpError::ControlLimit)));
        assert!(matches!(ReplyBuilder::default().push(&vec![b'x'; MAX_CONTROL_LINE + 1]), Err(FtpError::ControlLimit)));
    }
}
